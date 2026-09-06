"""
Unit tests for the two new revocation exceptions (Plan 034 / S13a, Step 22,
§D-S13-error). TokenRevokedError's str() must never leak scope/key/reason.
"""

from __future__ import annotations


class TestTokenRevokedError:
    def test_str_is_fixed_and_does_not_leak_attributes(self):
        from varco_core.authority.exceptions import TokenRevokedError
        from varco_core.revocation import RevocationScope

        exc = TokenRevokedError(scope=RevocationScope.TOKEN, key="jti-1", reason="compromised")
        message = str(exc)
        assert "jti-1" not in message
        assert "compromised" not in message
        assert message == "Token has been revoked."

    def test_attributes_carry_the_data(self):
        from varco_core.authority.exceptions import TokenRevokedError
        from varco_core.revocation import RevocationScope

        exc = TokenRevokedError(scope=RevocationScope.TOKEN, key="jti-1", reason="compromised")
        assert exc.scope == RevocationScope.TOKEN
        assert exc.key == "jti-1"
        assert exc.reason == "compromised"

    def test_is_authority_error_subclass(self):
        from varco_core.authority.exceptions import AuthorityError, TokenRevokedError

        assert issubclass(TokenRevokedError, AuthorityError)


class TestRevocationStoreUnavailableError:
    def test_is_authority_error_subclass(self):
        from varco_core.authority.exceptions import (
            AuthorityError,
            RevocationStoreUnavailableError,
        )

        assert issubclass(RevocationStoreUnavailableError, AuthorityError)
