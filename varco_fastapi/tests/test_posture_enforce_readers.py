"""
Plan 054 / Phase A, Step 3 — reader guard for FLIP-2's sentinel-derived
``enforce`` (§D-flip2-default).

``SecurityPostureSettings.enforce`` is ``None`` by default since 4.0, so it is
no longer the value the lifecycle uses — every reader must go through
``effective_enforce`` (or ``enforce_is_derived``). A raw ``settings.enforce``
read outside the two properties silently treats the derived ``"refuse"`` as
"unset/warn", which is exactly the regression this guard exists to catch.
"""

from __future__ import annotations

import ast
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]

# Raw-field reads that are allowed: the two properties themselves read
# ``self.enforce`` (excluded structurally below, since the receiver is ``self``).
_ALLOWED: frozenset[tuple[str, int]] = frozenset()


def _receiver_name(node: ast.expr) -> str:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return node.attr
    return ""


def test_only_the_property_reads_the_raw_field() -> None:
    offenders: list[str] = []
    for src in sorted(_REPO_ROOT.glob("varco_*/varco_*/**/*.py")):
        tree = ast.parse(src.read_text(encoding="utf-8"), filename=str(src))
        for node in ast.walk(tree):
            if not (isinstance(node, ast.Attribute) and node.attr == "enforce"):
                continue
            receiver = _receiver_name(node.value)
            # `self.enforce` inside SecurityPostureSettings' own properties, and
            # unrelated `.enforce(...)` methods (casbin's PolicyEngine), are fine.
            if receiver == "self" or "settings" not in receiver.lower():
                continue
            rel = src.relative_to(_REPO_ROOT).as_posix()
            if (rel, node.lineno) in _ALLOWED:
                continue
            offenders.append(f"{rel}:{node.lineno}")

    assert not offenders, (
        "raw SecurityPostureSettings.enforce reads outside effective_enforce/"
        f"enforce_is_derived: {offenders}"
    )
