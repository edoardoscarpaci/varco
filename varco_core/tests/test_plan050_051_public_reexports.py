"""
Follow-up to Plans 050/051 — three public-import-path additions:

1. ``register_webhook_verifier``/``registered_webhook_verifiers`` now also
   resolve from top-level ``varco_core`` (Plan 051 Step 9's PEP 562 lazy
   map), in addition to ``varco_core.webhook.inbound`` (unchanged).
2. ``WebhookModule`` now also resolves from ``varco_core.webhook`` (Plan 050),
   in addition to ``varco_core.webhook.module`` (unchanged).
3. ``TrustStoreModule`` now also resolves from ``varco_core.tls`` (Plan 050),
   in addition to ``varco_core.tls.module`` (unchanged).

Each assertion is identity (``is``), not just equality — a re-export that
silently rebuilt the object (e.g. via a bad ``deprecated_alias`` or a copy)
would pass an ``==`` check but break ``isinstance``/``is`` callers
elsewhere in the repo.
"""

from __future__ import annotations

import varco_core


def test_register_webhook_verifier_resolves_from_varco_core_top_level() -> None:
    from varco_core.webhook.inbound import (
        register_webhook_verifier as long_path_register,
    )
    from varco_core.webhook.inbound import (
        registered_webhook_verifiers as long_path_registered,
    )

    assert varco_core.register_webhook_verifier is long_path_register
    assert varco_core.registered_webhook_verifiers is long_path_registered


def test_register_webhook_verifier_importable_via_from_import() -> None:
    # Exercises CPython's IMPORT_FROM path, not just __getattr__/getattr().
    from varco_core.webhook.inbound import (
        register_webhook_verifier as long_path_register,
    )
    from varco_core.webhook.inbound import (
        registered_webhook_verifiers as long_path_registered,
    )

    from varco_core import register_webhook_verifier, registered_webhook_verifiers

    assert register_webhook_verifier is long_path_register
    assert registered_webhook_verifiers is long_path_registered


def test_webhook_module_resolves_from_varco_core_webhook_short_path() -> None:
    from varco_core.webhook import WebhookModule as short_path
    from varco_core.webhook.module import WebhookModule as long_path

    assert short_path is long_path


def test_trust_store_module_resolves_from_varco_core_tls_short_path() -> None:
    from varco_core.tls import TrustStoreModule as short_path
    from varco_core.tls.module import TrustStoreModule as long_path

    assert short_path is long_path


def test_webhook_module_short_path_does_not_eagerly_import_providify() -> None:
    # §D-webhook / this follow-up's constraint: `import varco_core.webhook`
    # alone must not pull providify — only touching WebhookModule should.
    import subprocess
    import sys

    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import sys, varco_core.webhook\n"
            "assert 'providify' not in sys.modules, sys.modules.keys()\n"
            "varco_core.webhook.WebhookModule\n"
            "assert 'providify' in sys.modules\n",
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_tls_short_path_does_not_eagerly_import_providify_but_trust_store_does() -> None:
    # `import varco_core.tls` already binds TrustStore/ReloadingTrustStore/etc.
    # eagerly (unchanged); only the new TrustStoreModule name is lazy.
    import subprocess
    import sys

    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import sys, varco_core.tls\n"
            "assert 'providify' not in sys.modules, sys.modules.keys()\n"
            "varco_core.tls.TrustStoreModule\n"
            "assert 'providify' in sys.modules\n",
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_tls_unknown_attribute_still_raises_attribute_error() -> None:
    import pytest

    with pytest.raises(AttributeError, match="varco_core.tls"):
        varco_core.tls.NoSuchName  # type: ignore[attr-defined]  # noqa: B018


def test_webhook_unknown_attribute_still_raises_attribute_error() -> None:
    import pytest

    with pytest.raises(AttributeError, match="varco_core.webhook"):
        varco_core.webhook.NoSuchName  # type: ignore[attr-defined]  # noqa: B018
