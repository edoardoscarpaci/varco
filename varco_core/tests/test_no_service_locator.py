"""
Plan 046 / Step 11 — DI-8 done gate: zero service-locator use in varco
itself (§D-locator).

RED MODE: this test is expected to FAIL today (not error) — 13 code sites
across `varco_kafka`, `varco_nats`, `varco_redis`, `varco_memcached`,
`varco_ws`, `varco_casbin`, `varco_sa`, `varco_beanie`, `varco_fastapi` still
call `DIContainer.current()`/`.acurrent()` until Steps 4-10 land. This is the
right failure mode: an assertion on real repo content, not an import error.
"""

from __future__ import annotations

from pathlib import Path

FORBIDDEN_TOKENS = (
    "DIContainer.current(",
    "DIContainer.acurrent(",
    "DIContainer.scoped(",
    "DIContainer.reset(",
)


def test_no_varco_source_file_uses_the_process_global_container() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    offenders: list[str] = []

    for pkg_dir in sorted(repo_root.glob("varco_*/varco_*")):
        if not pkg_dir.is_dir():
            continue
        for py_file in sorted(pkg_dir.rglob("*.py")):
            try:
                lines = py_file.read_text().splitlines()
            except UnicodeDecodeError:
                continue
            for lineno, line in enumerate(lines, start=1):
                if any(token in line for token in FORBIDDEN_TOKENS):
                    offenders.append(f"{py_file.relative_to(repo_root)}:{lineno}: {line.strip()}")

    assert offenders == [], (
        "varco source must never reach for the process-global DIContainer "
        "(applications may; providify's `di_global` pytest fixture exists "
        "for their tests) — offending lines:\n" + "\n".join(offenders)
    )
