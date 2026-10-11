"""
Plan 051 (DI-5) / §D-verifier — RED tests: ``register_webhook_verifier()``
(the profiling-backend registration precedent), never multi-binding, for
``WebhookVerifier`` providers.

Expected to fail until Step 9 lands — ``register_webhook_verifier`` does
not exist today.
"""

from __future__ import annotations

import pytest
from varco_core.webhook.inbound.verifiers import WebhookVerifier, get_verifier


@pytest.fixture(autouse=True)
def _isolated_provider_registry(monkeypatch: pytest.MonkeyPatch) -> None:
    """Give every test a throwaway copy of the process-global provider registry.

    Plan 059 / Step 3: ``replace=True`` below swaps the built-in ``"stripe"``
    for ``_CustomVerifier``; without a reset that leaked into every later
    test resolving ``"stripe"`` (``test_webhook_settings_wiring.py``) under
    pytest-randomly. ``monkeypatch`` restores the original dict at teardown.

    Args:
        monkeypatch: pytest's monkeypatch fixture.
    """
    from varco_core.webhook.inbound import verifiers

    monkeypatch.setattr(verifiers, "_PROVIDERS", dict(verifiers._PROVIDERS))


class _CustomVerifier(WebhookVerifier):
    provider = "custom"

    def __init__(self, secrets: list[str], **kwargs: object) -> None:
        self.secrets = secrets

    def verify(self, *, payload: bytes, headers: dict[str, str]):  # pragma: no cover
        raise NotImplementedError


def test_register_webhook_verifier_makes_it_resolvable_by_name() -> None:
    from varco_core.webhook.inbound.verifiers import register_webhook_verifier

    register_webhook_verifier("custom", _CustomVerifier)

    verifier = get_verifier("custom", secrets=["s1"])
    assert isinstance(verifier, _CustomVerifier)


def test_register_webhook_verifier_taken_name_raises_without_replace() -> None:
    from varco_core.webhook.inbound.verifiers import register_webhook_verifier

    with pytest.raises(ValueError):
        register_webhook_verifier("stripe", _CustomVerifier)


def test_register_webhook_verifier_replace_true_swaps_binding() -> None:
    from varco_core.webhook.inbound.verifiers import register_webhook_verifier

    register_webhook_verifier("stripe", _CustomVerifier, replace=True)

    verifier = get_verifier("stripe", secrets=["s1"])
    assert isinstance(verifier, _CustomVerifier)


def test_unknown_name_error_lists_registered_names() -> None:
    with pytest.raises(ValueError, match="stripe"):
        get_verifier("totally-unknown-provider", secrets=[])


def test_registered_webhook_verifiers_accessor_exists() -> None:
    from varco_core.webhook.inbound.verifiers import registered_webhook_verifiers

    names = registered_webhook_verifiers()
    assert "stripe" in names
    assert "github" in names
