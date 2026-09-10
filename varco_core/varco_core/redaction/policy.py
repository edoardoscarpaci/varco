"""
varco_core.redaction.policy
=============================
``RedactionPolicy`` — the frozen configuration object, and ``is_sensitive_key``
— the leaf predicate every ``Redactor`` implementation ultimately answers
(Plan 040 / S21, §D-S21-shape, §D-S21-falsepos, §D-S21-perf).

DESIGN: two match modes, one default, byte-identical to the incumbent
    ``match_mode="substring"`` (the default) is exactly the incumbent
    matcher moved from ``varco_core.observability.params`` — case-insensitive
    substring on the parameter/key name. Applied to *domain-chosen* payload
    keys (an audit diff) rather than *developer-chosen* parameter names, it
    has real false positives: ``"pin" in "shipping"`` and ``"auth" in
    "author"`` are both ``True`` (§D-S21-falsepos, found and evidenced, not
    fixed here — fixing the incumbent default would be a security-relevant
    behaviour change belonging to its own row).

    ✅ Default stays the incumbent — nobody's span-capture behaviour changes.
    ✅ ``match_mode="word"`` is documented and recommended for audit
       payloads — it tokenises on ``_``/``-``/camelCase boundaries so
       ``pin`` matches ``pin``/``user_pin``/``userPin`` but not ``shipping``.
    ❌ Two match modes is a knob to learn. Mitigated: the default requires no
       new knowledge, and the false-positive examples are the same two used
       throughout the plan's docs.

DESIGN: precomputed lowercase + a bounded ``functools.lru_cache`` (§D-S21-perf)
    ✅ An audit diff walks every field of two DTO dumps on every write — the
       *same* field names, every request. Caching turns repeat lookups into
       a dict hit after the first sighting.
    ✅ The cache key includes the policy itself (patterns + match_mode), so
       ``set_default_redactor()`` or a per-service policy cannot read a
       stale decision — a different policy is a different key.
    ✅ ``maxsize=4096`` bounds the cache against an attacker-influenced key
       space — the same ``InMemoryRateLimiter`` unbounded-keyspace lesson
       CLAUDE.md/``plans/035-http-edge-hardening.md`` already names.
    ❌ Another piece of module state to reset in tests — covered by
       ``reset_redaction_state()`` (``redactor.py``), which clears it.

Thread safety:  ✅ ``RedactionPolicy`` is frozen and hashable — safe to share
                   across threads/tasks. The ``lru_cache`` is CPython's own
                   thread-safe implementation.
Async safety:   ✅ Pure, synchronous, no I/O.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from functools import lru_cache
from typing import Literal

from varco_core.redaction.patterns import DEFAULT_REDACT_PATTERNS

__all__ = ["RedactionPolicy", "is_sensitive_key"]

# Splits a key into word-boundary tokens: non-alnum separators (``_``/``-``/
# ``.``/space/...) AND camelCase/PascalCase transitions. Stdlib `re` only.
_WORD_TOKEN_RE = re.compile(r"[A-Z]+(?=[A-Z][a-z0-9])|[A-Z]?[a-z0-9]+|[A-Z0-9]+")


@dataclass(frozen=True)
class RedactionPolicy:
    """
    Immutable configuration for key-name-based redaction and (for the one
    nested surface, the audit diff) traversal guards.

    Args:
        patterns: Case-insensitive patterns matched against a key name
            (interpretation depends on ``match_mode``). Defaults to
            ``DEFAULT_REDACT_PATTERNS`` — byte-identical to span capture's
            incumbent list.
        match_mode: ``"substring"`` (default, the incumbent semantics) or
            ``"word"`` (token-boundary matching — recommended, not
            defaulted, for audit payloads; §D-S21-falsepos).
        max_depth: ``redact_mapping()``'s nested-walk depth cap. A subtree
            deeper than this is replaced by the string ``"<max-depth>"``.
        max_items: Per-container item cap for ``redact_mapping()``. Extra
            entries are dropped and a ``"<truncated>"`` marker entry is
            appended — never a silent drop.

    Edge cases:
        - ``patterns`` is normalised to lowercase once, in
          ``__post_init__``, via the frozen-dataclass ``object.__setattr__``
          idiom — so ``is_sensitive_key`` never re-lowercases per call.

    Thread safety:  ✅ Frozen — immutable, hashable, safe to share.
    """

    patterns: tuple[str, ...] = DEFAULT_REDACT_PATTERNS
    match_mode: Literal["substring", "word"] = "substring"
    max_depth: int = 6
    max_items: int = 1000
    render_non_json: bool = field(default=True)

    def __post_init__(self) -> None:
        # DESIGN: lowercase once here, not per-call in is_sensitive_key.
        # object.__setattr__ is the standard frozen-dataclass idiom for a
        # normalised-on-construction field (⚠️ ASSUMPTION in the plan's
        # Risks table — untested against mypy strict at plan-writing time;
        # if it ever fails the strict gate, the one-line fallback is
        # dropping normalisation and lowercasing inside the cached
        # predicate instead, at the same measured cost after first call).
        object.__setattr__(self, "patterns", tuple(p.lower() for p in self.patterns))


def _tokenize(name: str) -> tuple[str, ...]:
    """
    Split ``name`` into lowercase word-boundary tokens.

    Splits on any non-alphanumeric run (``_``, ``-``, ``.``, space, ...) and
    on camelCase/PascalCase transitions, e.g. ``"userPin"`` -> ``("user",
    "pin")``, ``"shipping_address"`` -> ``("shipping", "address")``.

    Args:
        name: The raw key/parameter name.

    Returns:
        A tuple of lowercase tokens. Never raises — an unmatched character
        run simply produces no token for that run.
    """
    return tuple(match.group(0).lower() for match in _WORD_TOKEN_RE.finditer(name))


@lru_cache(maxsize=4096)
def _is_sensitive_key_cached(key: str, patterns: tuple[str, ...], match_mode: str) -> bool:
    if match_mode == "word":
        # Tokenise the ORIGINAL-case key — camelCase boundaries (e.g.
        # "userPin" -> "user"/"Pin") are only detectable before lowering;
        # `_tokenize` lowercases each token itself once it has split them.
        tokens = _tokenize(key)
        return any(pattern in tokens for pattern in patterns)
    # "substring" — the incumbent semantics, verbatim.
    return any(pattern in key.lower() for pattern in patterns)


def is_sensitive_key(key: str, policy: RedactionPolicy) -> bool:
    """
    Decide whether ``key`` names something sensitive under ``policy``.

    The leaf predicate every ``Redactor`` implementation ultimately answers
    — not on the ``Redactor`` Protocol itself (§D-S21-shape: traversal and
    the leaf decision are free functions, not Protocol methods).

    Args:
        key: The key/parameter name to test. Stringified via ``str(key)``
            first, so a non-``str`` key (e.g. ``1`` from a hand-built dict)
            never raises here.
        policy: The ``RedactionPolicy`` to evaluate against.

    Returns:
        ``True`` when ``key`` matches one of ``policy.patterns`` under
        ``policy.match_mode``. Never raises.

    Edge cases:
        - ``match_mode="substring"``: case-insensitive substring match —
          fail-closed, the same incumbent semantics as
          ``varco_core.observability.params._is_redacted``.
        - ``match_mode="word"``: exact match against a tokenised key —
          ``"pin"`` matches ``"pin"``/``"user_pin"``/``"userPin"`` but not
          ``"shipping"``. Case boundaries are read from the *original*
          key, so this must not be pre-lowered before tokenising.

    Thread safety:  ✅ Pure function; the backing cache is CPython's own
                       thread-safe ``lru_cache``.
    """
    return _is_sensitive_key_cached(str(key), policy.patterns, policy.match_mode)
