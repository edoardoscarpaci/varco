"""
Plan 038 (S19) / Step 15 — red-mode tests for
``varco_core.exception.webhook`` (``WebhookSignatureError``,
``WebhookReplayError``) and the §D-S19-secret non-leak obligation.

Fails with ``ModuleNotFoundError`` until Step 14 lands.
"""

from __future__ import annotations

from varco_core.exception.http import error_code_for, error_message_for


def test_webhook_signature_error_maps_to_401() -> None:
    from varco_core.exception.webhook import WebhookSignatureError

    exc = WebhookSignatureError("stripe")
    code = error_code_for(exc)
    assert code.http_status == 401


def test_webhook_replay_error_maps_to_409() -> None:
    from varco_core.exception.webhook import WebhookReplayError

    exc = WebhookReplayError("stripe")
    code = error_code_for(exc)
    assert code.http_status == 409


def test_webhook_signature_error_params_is_empty() -> None:
    from varco_core.exception.webhook import WebhookSignatureError

    exc = WebhookSignatureError("stripe")
    assert exc.error_params() == {}


def test_webhook_replay_error_params_is_empty() -> None:
    from varco_core.exception.webhook import WebhookReplayError

    exc = WebhookReplayError("stripe")
    assert exc.error_params() == {}


def test_neither_error_leaks_a_secret_or_signature_in_str_or_params() -> None:
    from varco_core.exception.webhook import WebhookReplayError, WebhookSignatureError

    secret_marker = "whsec_super_secret_value_should_never_appear"
    signature_marker = "v1,dGhpc2lzYXNpZ25hdHVyZQ=="

    sig_exc = WebhookSignatureError("stripe")
    replay_exc = WebhookReplayError("stripe")

    for exc in (sig_exc, replay_exc):
        rendered = str(exc)
        params_repr = repr(exc.error_params())
        assert secret_marker not in rendered
        assert secret_marker not in params_repr
        assert signature_marker not in rendered
        assert signature_marker not in params_repr


def test_error_message_for_renders_without_secret_leak() -> None:
    from varco_core.exception.webhook import WebhookSignatureError

    exc = WebhookSignatureError("stripe")
    msg = error_message_for(exc)
    assert "whsec_" not in msg.model_dump_json()
