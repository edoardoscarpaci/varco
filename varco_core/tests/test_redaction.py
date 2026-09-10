"""
tests.test_redaction
=====================
Plan 040 / S21, Phase 1, Step 1 — the ``varco_core.redaction`` seam's own
contract, written failing-first against a package that does not exist yet.

Covers:
    RedactionPolicy      — frozen, defaults to DEFAULT_REDACT_PATTERNS
    is_sensitive_key     — case-insensitive substring (default) vs. word mode
    PolicyRedactor        — satisfies the Redactor Protocol
    redact_mapping        — nesting, cycles, depth cap, item cap
    fail-safe behaviour   — a raising redactor / a broken walk never passes
                             the original value through (§D-S21-failsafe)
    default_redactor / set_default_redactor / reset_redaction_state
    redact_query_string
    json_safe
    match_mode="word" vs. "substring" (§D-S21-falsepos)

All tests are synchronous — no I/O, no event loop needed.
"""

from __future__ import annotations

import dataclasses

import pytest

# ── Import guard: the whole package is new (Phase 1, Step 2) ──────────────────


def test_redaction_package_importable() -> None:
    # If this fails with ImportError, nothing below in this module can run
    # meaningfully — it is the single most informative failure in Phase 1.
    import varco_core.redaction  # noqa: F401


# ── RedactionPolicy ─────────────────────────────────────────────────────────


class TestRedactionPolicy:
    def test_is_frozen_dataclass(self) -> None:
        from varco_core.redaction import RedactionPolicy

        assert dataclasses.is_dataclass(RedactionPolicy)
        policy = RedactionPolicy()
        with pytest.raises(dataclasses.FrozenInstanceError):
            policy.patterns = ()  # type: ignore[misc]

    def test_default_patterns_are_default_redact_patterns(self) -> None:
        from varco_core.redaction import DEFAULT_REDACT_PATTERNS, RedactionPolicy

        policy = RedactionPolicy()
        assert policy.patterns == DEFAULT_REDACT_PATTERNS

    def test_default_match_mode_is_substring(self) -> None:
        from varco_core.redaction import RedactionPolicy

        assert RedactionPolicy().match_mode == "substring"


# ── DEFAULT_REDACT_PATTERNS — literal, so a future "completeness" edit
#    cannot silently change span redaction (§D-S21-patterns, Non-goals) ───────


def test_default_redact_patterns_are_the_verbatim_fifteen_literals() -> None:
    from varco_core.redaction import DEFAULT_REDACT_PATTERNS

    assert DEFAULT_REDACT_PATTERNS == (
        "password",
        "passwd",
        "secret",
        "token",
        "authorization",
        "auth",
        "api_key",
        "apikey",
        "credential",
        "private_key",
        "cookie",
        "session_id",
        "otp",
        "pin",
        "ssn",
    )


def test_signature_is_absent_from_default_patterns() -> None:
    # §D-S21-patterns: "signature" lives only in EXTENDED_REDACT_PATTERNS.
    from varco_core.redaction import DEFAULT_REDACT_PATTERNS

    assert "signature" not in DEFAULT_REDACT_PATTERNS


def test_extended_redact_patterns_contains_signature_and_is_opt_in() -> None:
    from varco_core.redaction import DEFAULT_REDACT_PATTERNS, EXTENDED_REDACT_PATTERNS

    assert EXTENDED_REDACT_PATTERNS == (
        "signature",
        "passphrase",
        "bearer",
        "jwt",
        "salt",
        "api-key",
    )
    # Never silently folded into the default.
    assert not set(EXTENDED_REDACT_PATTERNS) & set(DEFAULT_REDACT_PATTERNS)


def test_hyphenated_api_key_header_not_caught_by_default_alone() -> None:
    # Open Question 3: DEFAULT_REDACT_PATTERNS is underscore-shaped
    # ("api_key"), so a hyphenated HTTP header name like "x-api-key" is
    # NOT a substring match — this documents the incumbent gap rather than
    # silently changing DEFAULT (which must stay byte-identical).
    from varco_core.redaction import DEFAULT_REDACT_PATTERNS, RedactionPolicy, is_sensitive_key

    policy = RedactionPolicy(patterns=DEFAULT_REDACT_PATTERNS)
    assert is_sensitive_key("x-api-key", policy) is False
    assert is_sensitive_key("X-Api-Key", policy) is False


def test_hyphenated_api_key_header_caught_with_extended_patterns() -> None:
    # The fix: EXTENDED_REDACT_PATTERNS adds "api-key", opt-in only.
    from varco_core.redaction import (
        DEFAULT_REDACT_PATTERNS,
        EXTENDED_REDACT_PATTERNS,
        RedactionPolicy,
        is_sensitive_key,
    )

    policy = RedactionPolicy(patterns=DEFAULT_REDACT_PATTERNS + EXTENDED_REDACT_PATTERNS)
    assert is_sensitive_key("x-api-key", policy) is True
    assert is_sensitive_key("X-Api-Key", policy) is True


def test_pii_redact_patterns_never_in_any_default() -> None:
    from varco_core.redaction import (
        DEFAULT_REDACT_PATTERNS,
        EXTENDED_REDACT_PATTERNS,
        PII_REDACT_PATTERNS,
    )

    assert PII_REDACT_PATTERNS == (
        "email",
        "phone",
        "address",
        "iban",
        "card_number",
        "cvv",
        "birth",
        "national_id",
        "tax_id",
    )
    assert not set(PII_REDACT_PATTERNS) & set(DEFAULT_REDACT_PATTERNS)
    assert not set(PII_REDACT_PATTERNS) & set(EXTENDED_REDACT_PATTERNS)


# ── is_sensitive_key ────────────────────────────────────────────────────────


class TestIsSensitiveKey:
    def test_case_insensitive_substring_match_by_default(self) -> None:
        from varco_core.redaction import RedactionPolicy, is_sensitive_key

        policy = RedactionPolicy()
        assert is_sensitive_key("PASSWORD", policy) is True
        assert is_sensitive_key("user_password_hash", policy) is True
        assert is_sensitive_key("harmless_field", policy) is False

    def test_substring_mode_has_known_false_positives(self) -> None:
        # §D-S21-falsepos, asserted as a literal so the finding cannot be lost.
        from varco_core.redaction import RedactionPolicy, is_sensitive_key

        policy = RedactionPolicy(match_mode="substring")
        assert is_sensitive_key("shipping_address", policy) is True  # "pin" in "shipping"
        assert is_sensitive_key("author", policy) is True  # "auth" in "author"

    def test_word_mode_does_not_match_shipping_or_author(self) -> None:
        # §D-S21-falsepos: the documented, non-default fix.
        from varco_core.redaction import RedactionPolicy, is_sensitive_key

        policy = RedactionPolicy(match_mode="word")
        assert is_sensitive_key("shipping_address", policy) is False
        assert is_sensitive_key("author", policy) is False
        assert is_sensitive_key("authored_at", policy) is False

    def test_word_mode_still_matches_token_boundary_variants(self) -> None:
        from varco_core.redaction import RedactionPolicy, is_sensitive_key

        policy = RedactionPolicy(patterns=("pin",), match_mode="word")
        assert is_sensitive_key("pin", policy) is True
        assert is_sensitive_key("user_pin", policy) is True
        assert is_sensitive_key("userPin", policy) is True
        assert is_sensitive_key("shipping", policy) is False


# ── PolicyRedactor / Redactor Protocol ──────────────────────────────────────


class TestPolicyRedactor:
    def test_satisfies_redactor_protocol(self) -> None:
        from varco_core.redaction import PolicyRedactor, Redactor

        assert isinstance(PolicyRedactor(), Redactor)

    def test_redacts_sensitive_key_to_placeholder(self) -> None:
        from varco_core.redaction import PolicyRedactor

        redactor = PolicyRedactor()
        assert redactor.redact("password", "hunter2") == "[REDACTED]"

    def test_passes_through_non_sensitive_key(self) -> None:
        from varco_core.redaction import PolicyRedactor

        redactor = PolicyRedactor()
        assert redactor.redact("page", 2) == 2

    def test_is_frozen(self) -> None:
        from varco_core.redaction import PolicyRedactor

        redactor = PolicyRedactor()
        with pytest.raises(dataclasses.FrozenInstanceError):
            redactor.policy = None  # type: ignore[misc]


# ── redact_mapping ───────────────────────────────────────────────────────────


class TestRedactMapping:
    def test_empty_mapping_returns_empty_dict(self) -> None:
        from varco_core.redaction import redact_mapping

        assert redact_mapping({}) == {}

    def test_redacts_flat_sensitive_key(self) -> None:
        from varco_core.redaction import redact_mapping

        result = redact_mapping({"password": "hunter2", "page": 2})
        assert result == {"password": "[REDACTED]", "page": 2}

    def test_redacts_nested_sensitive_key(self) -> None:
        from varco_core.redaction import redact_mapping

        result = redact_mapping({"before": {"password": "x"}, "after": {"password": "y"}})
        assert result == {"before": {"password": "[REDACTED]"}, "after": {"password": "[REDACTED]"}}

    def test_non_str_key_is_stringified_for_predicate_but_preserved_in_output(self) -> None:
        from varco_core.redaction import redact_mapping

        result = redact_mapping({1: "x"})
        assert result == {1: "x"}

    def test_cycle_is_replaced_with_marker(self) -> None:
        from varco_core.redaction import redact_mapping

        cyclic: dict = {"a": 1}
        cyclic["self"] = cyclic
        result = redact_mapping(cyclic)
        assert result["a"] == 1
        assert result["self"] == "<cycle>"

    def test_depth_cap_replaces_over_deep_subtree(self) -> None:
        from varco_core.redaction import RedactionPolicy, redact_mapping

        # Build a structure 8 levels deep; default max_depth is 6.
        deepest: dict = {"leaf": "value"}
        nested = deepest
        for _ in range(8):
            nested = {"child": nested}

        result = redact_mapping(nested, policy=RedactionPolicy())
        # Walk down until we hit the marker instead of a dict.
        cursor = result
        depths_walked = 0
        while isinstance(cursor, dict) and "child" in cursor:
            cursor = cursor["child"]
            depths_walked += 1
        assert cursor == "<max-depth>"
        assert depths_walked <= 6

    def test_item_cap_truncates_large_container(self) -> None:
        from varco_core.redaction import RedactionPolicy, redact_mapping

        policy = RedactionPolicy(max_items=3)
        data = {f"k{i}": i for i in range(10)}
        result = redact_mapping(data, policy=policy)
        assert "<truncated>" in result
        # At most max_items real entries plus the marker.
        assert len(result) <= 4

    def test_redactor_and_policy_both_given_raises_value_error(self) -> None:
        from varco_core.redaction import PolicyRedactor, RedactionPolicy, redact_mapping

        with pytest.raises(ValueError):
            redact_mapping({"a": 1}, PolicyRedactor(), policy=RedactionPolicy())


# ── Fail-safe: a raising redactor / a broken walk never passes data through ──


class TestFailSafe:
    def test_leaf_raising_redactor_yields_placeholder_not_original_value(self) -> None:
        from varco_core.redaction import redact_mapping

        class _BoomRedactor:
            def redact(self, key: str, value: object) -> object:
                raise RuntimeError("boom")

        result = redact_mapping({"page": 2}, _BoomRedactor())
        assert result["page"] != 2
        assert result["page"] == "[REDACTED]"

    def test_walk_failure_redacts_every_top_level_value_never_passes_input(self) -> None:
        from varco_core.redaction import redact_mapping

        class _RaisesOnSecondCall:
            def __init__(self) -> None:
                self.calls = 0

            def redact(self, key: str, value: object) -> object:
                self.calls += 1
                if self.calls > 1:
                    raise RuntimeError("boom")
                return value

        data = {"a": 1, "b": 2, "c": 3}
        result = redact_mapping(data, _RaisesOnSecondCall())
        assert set(result.keys()) == set(data.keys())
        assert all(v == "[REDACTED]" for v in result.values())
        # Never the original data object or its unredacted values.
        assert result != data


# ── Process-wide default redactor ───────────────────────────────────────────


class TestDefaultRedactorState:
    def test_default_redactor_is_policy_redactor_by_default(self) -> None:
        from varco_core.redaction import PolicyRedactor, default_redactor, reset_redaction_state

        reset_redaction_state()
        assert isinstance(default_redactor(), PolicyRedactor)

    def test_set_default_redactor_round_trips(self) -> None:
        from varco_core.redaction import (
            PolicyRedactor,
            default_redactor,
            reset_redaction_state,
            set_default_redactor,
        )

        reset_redaction_state()
        custom = PolicyRedactor()
        set_default_redactor(custom)
        try:
            assert default_redactor() is custom
        finally:
            reset_redaction_state()

    def test_reset_redaction_state_restores_default_and_clears_cache(self) -> None:
        from varco_core.redaction import (
            PolicyRedactor,
            RedactionPolicy,
            default_redactor,
            is_sensitive_key,
            reset_redaction_state,
            set_default_redactor,
        )

        reset_redaction_state()
        set_default_redactor(PolicyRedactor(policy=RedactionPolicy(patterns=("zzz",))))
        # Warm the lru_cache with a decision under the custom policy.
        is_sensitive_key("zzz", RedactionPolicy(patterns=("zzz",)))
        reset_redaction_state()
        assert isinstance(default_redactor(), PolicyRedactor)
        assert default_redactor().policy.patterns == RedactionPolicy().patterns


# ── redact_query_string ──────────────────────────────────────────────────────


class TestRedactQueryString:
    def test_redacts_matching_key_preserves_others(self) -> None:
        from varco_core.redaction import redact_query_string

        result = redact_query_string("api_key=abc&page=2")
        assert result == "api_key=%5BREDACTED%5D&page=2"

    def test_repeated_key_all_occurrences_redacted_order_preserved(self) -> None:
        from varco_core.redaction import redact_query_string

        result = redact_query_string("token=a&token=b&page=1")
        assert result == "token=%5BREDACTED%5D&token=%5BREDACTED%5D&page=1"

    def test_malformed_query_string_returned_unchanged_never_raises(self) -> None:
        from varco_core.redaction import redact_query_string

        malformed = "%%%not-a-real-encoding%%%"
        # Must never raise (§D-S21-failsafe).
        result = redact_query_string(malformed)
        assert isinstance(result, str)


# ── json_safe ────────────────────────────────────────────────────────────────


class TestJsonSafe:
    @pytest.mark.parametrize(
        "value",
        [None, True, 1, 1.5, "a string", [1, 2, 3], {"a": 1}],
    )
    def test_json_scalars_and_containers_pass_through_untouched(self, value: object) -> None:
        from varco_core.redaction import json_safe

        assert json_safe(value) == value

    def test_long_string_is_not_truncated(self) -> None:
        # Contrast with sanitize_value's 256-char ceiling — json_safe never truncates.
        from varco_core.redaction import json_safe

        long_string = "x" * 5000
        assert json_safe(long_string) == long_string

    def test_arbitrary_object_renders_as_type_name_placeholder(self) -> None:
        from varco_core.redaction import json_safe

        class _Opaque:
            pass

        result = json_safe(_Opaque())
        assert result == "<_Opaque>"
