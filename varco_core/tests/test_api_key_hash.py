"""
Unit tests for varco_core.auth.api_key — Plan 034 / S14.

hash_api_key()/verify_api_key() are the stdlib-only (hashlib/hmac) offline
hashing primitives backing ApiKeyAuth's hashed_keys= path (§D-S14-hash,
§D-S14-algo, §D-S14-compare).
"""

from __future__ import annotations

import pytest


class TestHashApiKey:
    def test_deterministic_without_pepper(self):
        from varco_core.auth.api_key import hash_api_key

        assert hash_api_key("my-key") == hash_api_key("my-key")

    def test_differs_with_and_without_pepper(self):
        from varco_core.auth.api_key import hash_api_key

        assert hash_api_key("my-key") != hash_api_key("my-key", pepper="pepper-1")

    def test_scheme_prefix_present_without_pepper(self):
        from varco_core.auth.api_key import hash_api_key

        assert hash_api_key("my-key").startswith("sha256$")

    def test_scheme_prefix_present_with_pepper(self):
        from varco_core.auth.api_key import hash_api_key

        assert hash_api_key("my-key", pepper="p").startswith("hmac-sha256$")

    def test_empty_string_raises_value_error(self):
        from varco_core.auth.api_key import hash_api_key

        with pytest.raises(ValueError):
            hash_api_key("")


class TestVerifyApiKey:
    def test_true_for_matching_key(self):
        from varco_core.auth.api_key import hash_api_key, verify_api_key

        digest = hash_api_key("my-key")
        assert verify_api_key("my-key", digest) is True

    def test_false_for_one_character_difference(self):
        from varco_core.auth.api_key import hash_api_key, verify_api_key

        digest = hash_api_key("my-key")
        assert verify_api_key("my-kez", digest) is False

    def test_false_for_wrong_pepper(self):
        from varco_core.auth.api_key import hash_api_key, verify_api_key

        digest = hash_api_key("my-key", pepper="right-pepper")
        assert verify_api_key("my-key", digest, pepper="wrong-pepper") is False

    def test_true_for_matching_key_and_pepper(self):
        from varco_core.auth.api_key import hash_api_key, verify_api_key

        digest = hash_api_key("my-key", pepper="p")
        assert verify_api_key("my-key", digest, pepper="p") is True

    def test_unknown_scheme_prefix_raises_value_error_naming_scheme(self):
        from varco_core.auth.api_key import verify_api_key

        with pytest.raises(ValueError, match="bcrypt"):
            verify_api_key("my-key", "bcrypt$deadbeef")
