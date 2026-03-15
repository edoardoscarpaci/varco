"""
fastrest_core.query.parser
============================
``QueryParser`` — converts a raw query string into a typed ``TransformerNode``
AST using the Lark grammar defined in ``grammar.lark``.

This is the entry point for string-based queries (e.g. from HTTP query params).
``QueryBuilder`` is preferred when queries are constructed in code; use this
module only when the query comes in as a string that must be parsed at runtime.

Thread safety:  ✅ ``QueryParser`` is stateless after construction.  The
                   internal ``Lark`` parser is thread-safe for concurrent
                   ``parse()`` calls (Lark does not mutate shared state
                   during parsing).
Async safety:   ✅ ``parse()`` is synchronous and safe to call from
                   async contexts — it does no I/O.

Example::

    from fastrest_core.query.parser import QueryParser
    from fastrest_core.query.params import QueryParams

    ast = QueryParser().parse("name = 'Alice' AND age > 18")
    params = QueryParams(node=ast, limit=20)
    results = await repo.find_by_query(params)
"""

from __future__ import annotations

from functools import cached_property
from pathlib import Path
from typing import ClassVar

from lark import Lark

from fastrest_core.query.transformer import QueryTransformer
from fastrest_core.query.type import TransformerNode

# ── Grammar source ─────────────────────────────────────────────────────────────

# Grammar file lives next to this module — read once at import time.
# Using a ClassVar on the parser avoids re-reading the file on each
# QueryParser() construction while still being overridable in tests.
_GRAMMAR_PATH: Path = Path(__file__).parent / "grammar.lark"


class QueryParser:
    """
    Parses a query string into a ``TransformerNode`` AST.

    Uses the Lark LALR(1) parser with the ``grammar.lark`` grammar and the
    ``QueryTransformer`` to produce typed AST nodes.

    DESIGN: LALR(1) over Earley parser
      ✅ LALR(1) is O(n) — predictable performance for user-facing queries.
      ✅ Grammar is simple enough that LALR(1) handles it without ambiguity.
      ❌ Earley handles ambiguous grammars more gracefully, but is O(n³)
         worst-case — inappropriate for queries coming from HTTP requests.

    DESIGN: ``cached_property`` for ``_parser``
      ✅ Lark parser construction is expensive (grammar compilation) — do it
         once per ``QueryParser`` instance, not on every ``parse()`` call.
      ❌ First ``parse()`` call is slower than subsequent ones — negligible
         in practice.

    Thread safety:  ✅ ``parse()`` does not mutate any shared state.
    Async safety:   ✅ ``parse()`` is synchronous.

    Edge cases:
        - Empty string → ``lark.UnexpectedEOF`` (propagated to caller).
        - Unrecognised operator → ``OperationNotFound`` from ``QueryTransformer``.
        - Malformed expression → ``lark.UnexpectedToken`` (propagated to caller).
    """

    # ── Class-level grammar source — read once per interpreter ────────────────
    # DESIGN: class variable over instance variable so the file is read only
    # once even when many QueryParser instances are created.
    _grammar_text: ClassVar[str] = _GRAMMAR_PATH.read_text(encoding="utf-8")

    @cached_property
    def _parser(self) -> Lark:
        """
        Lazily constructed and cached LALR(1) Lark parser.

        ``cached_property`` means the Lark instance is created on the first
        call to ``parse()`` and reused for all subsequent calls on the same
        ``QueryParser`` instance.

        Returns:
            Configured ``Lark`` parser ready to parse query strings.
        """
        # Use a fresh QueryTransformer so the parser owns its transformer
        # — avoids any accidental state sharing between parser instances.
        return Lark(
            self._grammar_text,
            parser="lalr",
            transformer=QueryTransformer(),
        )

    def parse(self, query: str) -> TransformerNode:
        """
        Parse ``query`` into a ``TransformerNode`` AST.

        The transformer runs inline with the parser (``transformer=`` kwarg),
        so the returned value is already the final AST node — no separate
        ``.transform()`` call needed.

        Args:
            query: Raw filter expression string, e.g.::

                "name = 'Alice' AND age > 18"
                "status IN ('active', 'pending')"
                "deleted_at IS NULL OR (created_at > '2024-01-01')"

        Returns:
            Root ``TransformerNode`` representing the parsed expression.

        Raises:
            lark.UnexpectedEOF:    ``query`` is empty or ends unexpectedly.
            lark.UnexpectedToken:  Syntax error in ``query``.
            OperationNotFound:     Operator in ``query`` not in ``Operation``.

        Example::

            ast = QueryParser().parse("active = True AND age >= 18")
            params = QueryParams(node=ast, limit=50)
        """
        # The Lark transformer is set inline — parse() returns the AST directly.
        return self._parser.parse(query)  # type: ignore[return-value]
