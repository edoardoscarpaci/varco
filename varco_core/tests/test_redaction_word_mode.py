"""
tests.test_redaction_word_mode
================================
Plan 057 / FLIP-040-1, §D-word — word-boundary matching becomes the
``RedactionPolicy`` default. Every row of the plan's contract table is a
test here, so an un-redaction in the unsafe direction is a red test, never
a discovery in production.

Pure functions — all tests are sync ``def`` (no async in this module).
"""

from __future__ import annotations

import pytest

# ── (a) tokenizer ───────────────────────────────────────────────────────────


class TestTokenizer:
    @pytest.mark.parametrize(
        ("name", "expected"),
        [
            ("x-api-key", ("x", "api", "key")),
            ("apiKey", ("api", "key")),
            ("HTTPAuthorization", ("http", "authorization")),
            # Digit runs are their own token (new in 4.0 — 3.2 glued them).
            ("password2", ("password", "2")),
            ("sha256", ("sha", "256")),
            ("X-Hub-Signature-256", ("x", "hub", "signature", "256")),
            ("shipping_address", ("shipping", "address")),
            ("Authorization", ("authorization",)),
            ("__dunder__", ("dunder",)),
            ("", ()),
        ],
    )
    def test_tokenize(self, name: str, expected: tuple[str, ...]) -> None:
        from varco_core.redaction.policy import _tokenize

        assert _tokenize(name) == expected


# ── (b) the default contract (fifteen defaults, word mode) ──────────────────

_REDACTED = [
    # token `password` / plural / digit token
    "password", "PASSWORD", "password_hash", "passwordHash", "new_password", "passwords",
    "password2",
    "passwd", "db_passwd",
    "secret", "client_secret", "clientSecret", "SECRET_KEY", "secrets",
    "token", "access_token", "refreshToken", "x-auth-token", "id_token", "tokens",
    "Authorization", "authorization", "X-Authorization", "HTTPAuthorization",
    "auth", "auth_token", "basic_auth", "authKey",
    # pattern `api_key` -> ("api","key") contiguous subsequence
    "api_key", "apiKey", "API_KEY", "x-api-key", "X-Api-Key", "api_keys",
    "apikey", "APIKEY",
    "credential", "credentials", "aws_credentials",
    "private_key", "privateKey", "PRIVATE_KEY",
    "cookie", "cookies", "set-cookie",
    "session_id", "sessionId",
    "otp", "otp_code", "pin", "user_pin", "userPin", "pin_code", "pins", "ssn",
    "oauth_token", "totp_secret",
]  # fmt: skip

_FALSE_POSITIVE_ROWS = [
    "shipping_address", "shipping", "mapping", "typing", "grouping", "stripping",
    "author", "authored_at", "authority", "authorship",
]  # fmt: skip

# "Narrower than substring" — documented in §Migration.
_NARROWER_ROWS = ["oauth", "oauth_client", "csrftoken", "csrfmiddlewaretoken", "secretkey",
                  "totp_code"]  # fmt: skip

_NOT_REDACTED = [
    *_FALSE_POSITIVE_ROWS,
    "page", "harmless_field", "primary_key", "cache_key", "idempotency_key", "partition_key",
    *_NARROWER_ROWS,
    "sessionid",  # never caught by either mode
]  # fmt: skip


class TestDefaultWordContract:
    """§D-word contract table against ``RedactionPolicy()`` — closes 040
    §D-S21-falsepos (``pin`` in ``shipping``, ``auth`` in ``author``)."""

    @pytest.mark.parametrize("key", _REDACTED)
    def test_redacted_by_default(self, key: str) -> None:
        from varco_core.redaction import RedactionPolicy, is_sensitive_key

        assert is_sensitive_key(key, RedactionPolicy()) is True

    @pytest.mark.parametrize("key", _NOT_REDACTED)
    def test_not_redacted_by_default(self, key: str) -> None:
        from varco_core.redaction import RedactionPolicy, is_sensitive_key

        assert is_sensitive_key(key, RedactionPolicy()) is False


# ── (c) substring escape hatch — byte-identical 3.2 semantics ───────────────


class TestSubstringEscapeHatch:
    # The four headline false-positive rows + the five narrower rows that
    # substring caught; proves the escape hatch rather than promising it.
    @pytest.mark.parametrize(
        "key",
        [
            "shipping_address", "author", "authored_at", "authority",
            "oauth", "csrftoken", "csrfmiddlewaretoken", "secretkey", "totp_code",
        ],
    )  # fmt: skip
    def test_substring_mode_matches(self, key: str) -> None:
        from varco_core.redaction import RedactionPolicy, is_sensitive_key

        assert is_sensitive_key(key, RedactionPolicy(match_mode="substring")) is True


# ── (d)-(f) ─────────────────────────────────────────────────────────────────


def test_empty_token_pattern_rejected_in_word_mode() -> None:
    # A pattern that tokenises to () can never match — a config error, fail loud.
    from varco_core.redaction import RedactionPolicy

    with pytest.raises(ValueError, match="-"):
        RedactionPolicy(patterns=("-",))
    RedactionPolicy(patterns=("-",), match_mode="substring")  # still constructs


def test_plural_rule_applies_to_last_token_only() -> None:
    from varco_core.redaction import RedactionPolicy, is_sensitive_key

    # Explicit word mode: under 3.2's substring default this would pass vacuously.
    policy = RedactionPolicy(match_mode="word")
    assert is_sensitive_key("api_keys", policy) is True
    assert is_sensitive_key("apis_key", policy) is False


def test_cache_key_includes_match_mode() -> None:
    # Same key, both modes, different answers — a stale cached decision
    # from one mode must never leak into the other.
    from varco_core.redaction import RedactionPolicy, is_sensitive_key, reset_redaction_state

    reset_redaction_state()
    assert is_sensitive_key("shipping_address", RedactionPolicy(match_mode="substring")) is True
    assert is_sensitive_key("shipping_address", RedactionPolicy()) is False
    assert is_sensitive_key("shipping_address", RedactionPolicy(match_mode="substring")) is True


# ── Repair (Plan 057 drift #2): camelCase patterns tokenise like keys ───────


@pytest.mark.parametrize("key", ["api_key", "apiKey", "x-api-key", "API_KEY"])
def test_camelcase_pattern_tokenised_like_a_key(key: str) -> None:
    # Edge case "a pattern given in camelCase ('apiKey') → ('api','key')".
    # Lowercasing before tokenising made it ('apikey',) and silently
    # un-redacted every one of these keys.
    from varco_core.redaction import RedactionPolicy, is_sensitive_key

    assert is_sensitive_key(key, RedactionPolicy(patterns=("apiKey",))) is True


@pytest.mark.parametrize(
    ("key", "expected"), [("api_key", True), ("apiKey", True), ("apikey", False)]
)
def test_acronym_pattern_follows_the_acronym_boundary_rule(key: str, expected: bool) -> None:
    # "APIKey" → acronym followed by a capitalised word → ('api','key').
    from varco_core.redaction import RedactionPolicy, is_sensitive_key

    assert is_sensitive_key(key, RedactionPolicy(patterns=("APIKey",))) is expected


def test_camelcase_pattern_survives_dataclasses_replace() -> None:
    # replace() re-runs __post_init__ on the stored patterns — the split
    # must not be lost on the second pass.
    from dataclasses import replace

    from varco_core.redaction import RedactionPolicy, is_sensitive_key

    policy = replace(RedactionPolicy(patterns=("apiKey",)), max_depth=3)
    assert is_sensitive_key("api_key", policy) is True


def test_lowercase_patterns_are_stored_unchanged() -> None:
    from varco_core.redaction import DEFAULT_REDACT_PATTERNS, RedactionPolicy

    assert RedactionPolicy().patterns == DEFAULT_REDACT_PATTERNS
    assert RedactionPolicy(patterns=("x-api-key",)).patterns == ("x-api-key",)
    assert RedactionPolicy(patterns=("apiKey",), match_mode="substring").patterns == ("apikey",)
