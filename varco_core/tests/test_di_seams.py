"""
Plan 047 / Step 1 — `varco_core.di.seams` (§D-vocab).

`varco_core/varco_core/di/seams.py` holds the one seam registry: a `Seam`
frozen dataclass plus `SEAMS` (per §D-vocab's table), `seam_by_name()`, and
`interface_of(seam)` which resolves the dotted interface string lazily
(providify must never be imported at module scope — 046's import-budget
rule).

RED MODE: `varco_core/varco_core/di/seams.py` does not exist yet (the
module is still a bare `di.py`, not a package) — every test here is
expected to fail with `ModuleNotFoundError`/`ImportError`.
"""

from __future__ import annotations

import re

import pytest

EXPECTED_SEAMS = {
    "cache": ("VARCO_CACHE_BACKEND", "memory"),
    "dlq": ("VARCO_DLQ_BACKEND", "memory"),
    "idempotency": ("VARCO_IDEMPOTENCY_BACKEND", "memory"),
    "job_store": ("VARCO_JOB_STORE_BACKEND", "memory"),
    "event_bus": ("VARCO_EVENT_BUS_BACKEND", "memory"),
    "revocation": ("VARCO_REVOCATION_BACKEND", "null"),
}

VALUE_RE = re.compile(r"^[a-z][a-z0-9-]*$")


def test_seams_has_exactly_the_six_named_seams() -> None:
    from varco_core.di.seams import SEAMS

    assert {s.seam for s in SEAMS} == set(EXPECTED_SEAMS)


@pytest.mark.parametrize("seam_name", list(EXPECTED_SEAMS))
def test_each_seam_has_the_expected_env_name_and_default(seam_name: str) -> None:
    from varco_core.di.seams import SEAMS

    expected_env, expected_default = EXPECTED_SEAMS[seam_name]
    seam = next(s for s in SEAMS if s.seam == seam_name)
    assert seam.env == expected_env
    assert seam.default_value == expected_default


def test_seam_env_names_are_unique() -> None:
    from varco_core.di.seams import SEAMS

    envs = [s.env for s in SEAMS]
    assert len(envs) == len(set(envs))


def test_seam_by_name_returns_matching_seam() -> None:
    from varco_core.di.seams import seam_by_name

    seam = seam_by_name("cache")
    assert seam.seam == "cache"


def test_seam_by_name_raises_on_unknown_seam() -> None:
    from varco_core.di.seams import seam_by_name

    with pytest.raises(LookupError):
        seam_by_name("not-a-real-seam")


@pytest.mark.parametrize("seam_name", list(EXPECTED_SEAMS))
def test_interface_of_resolves_the_real_abc(seam_name: str) -> None:
    # interface_of() resolves the dotted interface string lazily, on
    # demand — it must return the actual ABC class, not the string.
    from varco_core.di.seams import interface_of, seam_by_name

    seam = seam_by_name(seam_name)
    interface = interface_of(seam)
    assert isinstance(interface, type)


def test_default_value_regex_accepts_expected_shapes() -> None:
    # Values are [a-z][a-z0-9-]* — the real validator selected_by() uses,
    # imported from production code so this test is red until it exists.
    from varco_core.di.select import selected_by

    for value in ("redis", "redis-streams", "in-memory2"):
        # Must not raise for a well-shaped, non-default value.
        selected_by("cache", value)


def test_default_value_regex_rejects_bad_shapes() -> None:
    from varco_core.di.select import selected_by

    for value in ("Redis", "REDIS", "1redis", "", "re dis"):
        with pytest.raises(ValueError):
            selected_by("cache", value)


def test_importing_seams_module_does_not_import_providify() -> None:
    import subprocess
    import sys

    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import varco_core.di.seams; import sys; "
            "print('providify' in sys.modules or "
            "any(m.startswith('providify.') for m in sys.modules))",
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    assert result.stdout.strip() == "False", "varco_core.di.seams must not import providify"
