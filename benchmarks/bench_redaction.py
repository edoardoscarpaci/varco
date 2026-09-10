"""``redact_mapping()`` / ``is_sensitive_key()`` (Plan 040 / S21, §D-S21-perf).

Audit diffs are hot in a way span attributes are not: a span captures at most
32 developer-named parameters, but an audit diff walks every field of two
DTO dumps on every write. This benchmark exercises the nested walk over a
representative audit diff, and the cold-vs-cached cost of the leaf
predicate.

⛔ No time assertion, no container-backed import — this is collected by
``benchmarks/pytest.ini`` (``bench_*.py``) only, never by
``scripts/unit_tests.sh``, and ``make bench`` is never a required check.
"""

from __future__ import annotations

from typing import Any

from varco_core.redaction import PolicyRedactor, RedactionPolicy, is_sensitive_key, redact_mapping
from varco_core.redaction.policy import _is_sensitive_key_cached

# A representative nested audit diff: two ~10-field DTO dumps, one secret
# field each, mirroring a real `{"before": ..., "after": ...}` update diff
# (varco_core/varco_core/service/audit.py:646-650).
_ORDER_DTO: dict[str, Any] = {
    "id": "ord_1",
    "customer_name": "Ada Lovelace",
    "shipping_address": "1 Analytical Engine Way",
    "total_amount": 129.99,
    "currency": "USD",
    "status": "confirmed",
    "password": "hunter2",  # the sensitive field the benchmark must redact
    "notes": "handle with care",
    "created_at": "2026-01-01T00:00:00Z",
    "updated_at": "2026-01-02T00:00:00Z",
}

_AUDIT_DIFF: dict[str, Any] = {
    "before": {**_ORDER_DTO, "status": "pending"},
    "after": _ORDER_DTO,
}


def test_redact_mapping_nested_audit_diff(benchmark) -> None:  # type: ignore[no-untyped-def]
    redactor = PolicyRedactor()
    result = benchmark(redact_mapping, _AUDIT_DIFF, redactor)
    assert result["after"]["password"] == "[REDACTED]"
    assert result["after"]["status"] == "confirmed"


def test_is_sensitive_key_cold(benchmark) -> None:  # type: ignore[no-untyped-def]
    policy = RedactionPolicy()

    def _cold_lookup() -> bool:
        _is_sensitive_key_cached.cache_clear()
        return is_sensitive_key("customer_password_hash", policy)

    result = benchmark(_cold_lookup)
    assert result is True


def test_is_sensitive_key_cached(benchmark) -> None:  # type: ignore[no-untyped-def]
    policy = RedactionPolicy()
    is_sensitive_key("customer_password_hash", policy)  # warm the cache once

    result = benchmark(is_sensitive_key, "customer_password_hash", policy)
    assert result is True
