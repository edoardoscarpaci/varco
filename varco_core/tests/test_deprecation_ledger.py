"""
Red-mode tests for Plan 052 Step 2 — the deprecation ledger
(``varco_core.deprecation``: ``DeprecationRecord``, ``fired_deprecations()``,
``reset_fired_deprecations()``, the ``_note()`` hook fired from ``deprecated()``
before each ``warnings.warn``).

None of these names exist yet — every test is expected to fail with an
``ImportError``/``AttributeError`` until Plan 052 Step 2 lands the ledger.
"""

from __future__ import annotations

import threading
import warnings

import pytest

# This import is the primary red-mode probe: DeprecationRecord/fired_deprecations/
# reset_fired_deprecations do not exist on varco_core.deprecation yet.
from varco_core.deprecation import (  # noqa: F401  (import is the red-mode probe)
    DeprecationRecord,
    deprecated,
    fired_deprecations,
    reset_fired_deprecations,
)


@pytest.fixture(autouse=True)
def _reset_ledger() -> None:
    reset_fired_deprecations()
    yield
    reset_fired_deprecations()


@deprecated(since="3.0.0", removed_in="4.0.0", replacement="new_thing")
def _old_thing() -> str:
    return "ok"


def test_calling_a_deprecated_function_twice_records_one_entry_with_count_two() -> None:
    # The ledger dedups on subject and accumulates a call count, not one row per call.
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        _old_thing()
        _old_thing()

    records = fired_deprecations()
    assert len(records) == 1
    record = records[0]
    assert record.count == 2
    assert record.since == "3.0.0"
    assert record.removed_in == "4.0.0"
    assert record.replacement == "new_thing"


def test_first_location_names_the_calling_test_file() -> None:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        _old_thing()

    (record,) = fired_deprecations()
    assert "test_deprecation_ledger.py" in record.first_location


def test_record_is_present_even_under_simplefilter_ignore() -> None:
    # The ledger records regardless of the warnings filter (§D-deprecations) —
    # this is the whole point: it survives a process-wide -W ignore.
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        _old_thing()

    assert len(fired_deprecations()) == 1


def test_reset_fired_deprecations_empties_the_ledger() -> None:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        _old_thing()
    assert len(fired_deprecations()) == 1

    reset_fired_deprecations()

    assert fired_deprecations() == ()


def test_ledger_is_thread_safe_under_concurrent_calls() -> None:
    def _hammer() -> None:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            for _ in range(100):
                _old_thing()

    threads = [threading.Thread(target=_hammer) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    (record,) = fired_deprecations()
    assert record.count == 200
