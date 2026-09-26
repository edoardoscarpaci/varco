"""
Guard for §D-loose's allowlist (Plan 048 / Step 15).

Walks the ten source trees for loose ``VARCO_*`` environment reads and asserts
the hit set equals the frozen allowlist in
``varco_core.settings.inventory.LOOSE_ENV_READ_ALLOWLIST``.

Detection is **AST-based, not regex-based** (Plan 048 / repair round 2). The
original regex (``environ(?:\\.get\\(|\\[)\\s*["']VARCO_|getenv\\(...``) had two
defects that together made this guard's "the hit set equals the allowlist"
promise narrower than it advertised:

1. **Aliased reads were invisible.** ``varco_core/observability/attributes.py``
   does ``env = environ if environ is not None else os.environ`` and then reads
   ``env.get("VARCO_OTEL_GLOBAL_ATTRS")``. The literal token ``environ`` is
   absent from the reading line, so the regex never saw it — even though
   §D-loose's own table names those reads as allowlisted. An entire class of
   loose read could be added without this test noticing.
2. **Docstrings counted as code.** A triple-quoted ``Example::`` block
   containing ``environ[`` is indistinguishable from an executable read to a
   line-oriented regex, which forced a bogus allowlist row for
   ``di/select.py``'s docstring.

The AST walk fixes both: it resolves aliases of ``os.environ`` (including
through an ``IfExp``, which is the exact shape ``attributes.py`` uses) and it
only ever sees real expressions, never string literals.

DESIGN: AST over a widened regex.
    ✅ Catches aliased reads, which is the whole point — a widened regex would
       have to match a bare ``\\w+\\.get\\("VARCO_`` and would then flag every
       dict/settings ``.get("VARCO_...")`` in the tree as a false positive.
    ✅ Structurally cannot see docstrings or comments, so no bogus rows.
    ❌ Cannot follow an alias across a function boundary (an ``environ`` passed
       into a helper that stores it on ``self``). No such shape exists in the
       tree today; if one appears it is invisible here, same as any static check.
"""

from __future__ import annotations

import ast
from pathlib import Path

from varco_core.settings.inventory import LOOSE_ENV_READ_ALLOWLIST

_PACKAGES = (
    "varco_core",
    "varco_kafka",
    "varco_redis",
    "varco_nats",
    "varco_sa",
    "varco_beanie",
    "varco_memcached",
    "varco_ws",
    "varco_fastapi",
    "varco_casbin",
)

_PREFIX = "VARCO_"


def _is_os_environ(node: ast.expr, env_names: set[str]) -> bool:
    """Whether ``node`` evaluates to an environment mapping.

    Args:
        node: The expression to classify.
        env_names: Bare names currently known to hold an environment mapping —
            ``environ`` itself plus any alias assigned from it.

    Returns:
        ``True`` for ``os.environ``, for a bare name in ``env_names``, and for
        an ``IfExp`` whose either branch is one of those (the
        ``env = environ if environ is not None else os.environ`` shape).
    """
    if isinstance(node, ast.Attribute) and node.attr == "environ":
        return True
    if isinstance(node, ast.Name) and node.id in env_names:
        return True
    if isinstance(node, ast.IfExp):
        return _is_os_environ(node.body, env_names) or _is_os_environ(node.orelse, env_names)
    return False


def _env_alias_names(tree: ast.AST) -> set[str]:
    """Collect every name in ``tree`` that holds an environment mapping.

    Seeded with ``environ`` (the conventional parameter name for an injected
    mapping across this codebase) and grown by one fixed-point pass over every
    assignment whose value resolves to an environment mapping, so a chain
    (``a = os.environ`` then ``b = a``) is followed.
    """
    names = {"environ"}
    for _ in range(4):  # a fixed point in practice; bounded to stay cheap
        grew = False
        for node in ast.walk(tree):
            if not isinstance(node, ast.Assign):
                continue
            if not _is_os_environ(node.value, names):
                continue
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id not in names:
                    names.add(target.id)
                    grew = True
        if not grew:
            break
    return names


def _varco_literal(node: ast.expr | None) -> bool:
    return (
        isinstance(node, ast.Constant)
        and isinstance(node.value, str)
        and node.value.startswith(_PREFIX)
    )


def _loose_reads_in(text: str) -> set[int]:
    """Return the 1-indexed line numbers of loose ``VARCO_*`` reads in ``text``."""
    tree = ast.parse(text)
    env_names = _env_alias_names(tree)
    lines: set[int] = set()

    for node in ast.walk(tree):
        # <env>.get("VARCO_...")  /  os.getenv("VARCO_...")  /  getenv("VARCO_...")
        if isinstance(node, ast.Call):
            fn = node.func
            first = node.args[0] if node.args else None
            if (
                isinstance(fn, ast.Attribute)
                and fn.attr == "get"
                and _is_os_environ(fn.value, env_names)
                and _varco_literal(first)
            ):
                lines.add(node.lineno)
            elif (
                (isinstance(fn, ast.Attribute) and fn.attr == "getenv")
                or (isinstance(fn, ast.Name) and fn.id == "getenv")
            ) and _varco_literal(first):
                lines.add(node.lineno)
        # <env>["VARCO_..."]
        elif (
            isinstance(node, ast.Subscript)
            and _is_os_environ(node.value, env_names)
            and _varco_literal(node.slice)
        ):
            lines.add(node.lineno)

    return lines


def _find_loose_reads(root: Path) -> set[str]:
    hits: set[str] = set()
    for pkg in _PACKAGES:
        pkg_src = root / pkg / pkg
        if not pkg_src.is_dir():
            continue
        for path in pkg_src.rglob("*.py"):
            for lineno in _loose_reads_in(path.read_text()):
                hits.add(f"{path.relative_to(root)}:{lineno}")
    return hits


class TestNoLooseEnvReads:
    def test_loose_reads_equal_the_frozen_allowlist(self) -> None:
        repo_root = Path(__file__).resolve().parents[2]

        found = _find_loose_reads(repo_root)

        assert found == set(LOOSE_ENV_READ_ALLOWLIST)


class TestGuardDetectsWhatTheRegexMissed:
    """Regression tests for the two defects that motivated the AST rewrite."""

    def test_aliased_environ_read_is_detected(self) -> None:
        """The ``attributes.py`` shape: alias first, then read off the alias."""
        src = (
            "import os\n"
            "def f(environ=None):\n"
            "    env = environ if environ is not None else os.environ\n"
            '    return env.get("VARCO_OTEL_GLOBAL_ATTRS", "")\n'
        )

        assert _loose_reads_in(src) == {4}

    def test_chained_alias_read_is_detected(self) -> None:
        src = 'import os\na = os.environ\nb = a\nx = b["VARCO_THING"]\n'

        assert _loose_reads_in(src) == {4}

    def test_docstring_mentioning_environ_is_not_a_read(self) -> None:
        """A regex counted this as a hit; the AST cannot see it at all."""
        src = 'def f():\n    """Example::\n\n        os.environ["VARCO_CACHE_BACKEND"] = "redis"\n    """\n'

        assert _loose_reads_in(src) == set()

    def test_unrelated_get_on_a_plain_dict_is_not_a_read(self) -> None:
        """Guards against the false-positive a widened regex would have caused."""
        src = 'd = {"VARCO_X": 1}\nv = d.get("VARCO_X")\n'

        assert _loose_reads_in(src) == set()
