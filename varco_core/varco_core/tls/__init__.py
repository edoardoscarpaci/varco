"""
varco_core.tls
===============

One unified TLS trust model (Plan 026 / T3, T5, T7): ``TrustStore`` (a strict superset of
``varco_core.connection.ssl.SSLConfig`` and 3.0's ``varco_fastapi.auth.trust_store.TrustStore``),
``ReloadingTrustStore`` (built on ``varco_core.watch``/``varco_core.reload``, Plan 025), and the
shared ``iter_cert_files`` cert-glob helper.

**Layer rule (mechanically enforced by ``varco_core/tests/test_tls_layering.py``)**: this
package imports NOTHING from ``varco_core.connection``, ``varco_fastapi``, or any backend
package (``varco_kafka``/``varco_redis``/``varco_sa``/``varco_beanie``/...). The bridge the
other direction — ``SSLConfig.to_trust_store()`` — lives in ``varco_core.connection.ssl``, not
here, precisely so this package can stay a leaf. ``varco_fastapi.auth.trust_store.TrustStore``
(the deprecated 3.0-semantics shim, §D-T3-oq1) is the one place in ``varco_fastapi`` that
imports *this* package — never the reverse.

**No ``@Configuration`` here, ever (§D-T3-oq3).** ``container.scan("varco_core", recursive=True)``
is a documented, in-use pattern (``README.md``) that auto-activates scanned ``@Configuration``
classes. A ``TlsConfiguration`` here would start a filesystem watcher in every app that scans
``varco_core`` — the opposite of the locked "Auto-injection: explicit, opt-in, never implicit"
decision. ``varco_core.tls.di.bind_trust_store`` is a plain ``bind_*`` function instead: it has
no lifecycle side effect, and the caller always starts (or ``async with``s) the store itself.

Usage::

    from varco_core.tls import TrustStore, ReloadingTrustStore, bind_trust_store

    spec = TrustStore(ca_folders=Path("/etc/ssl/private-ca"))
    store = ReloadingTrustStore(spec)
    async with store:
        ctx = store.context
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from varco_core.tls.clients import (
    MissingClientDependencyError,
    to_aiohttp_connector,
    to_httpx_verify,
    to_requests_adapter,
    to_urllib3_poolmanager,
)
from varco_core.tls.di import bind_trust_store
from varco_core.tls.discovery import CERT_FILE_PATTERNS, iter_cert_files
from varco_core.tls.install import RestoreHandle, install_process_trust
from varco_core.tls.pkcs12 import Pkcs12LoadError
from varco_core.tls.reload import ReloadingTrustStore, ReloadStrategy
from varco_core.tls.store import TrustStore

if TYPE_CHECKING:
    from varco_core.tls.module import TrustStoreModule

__all__ = [
    "CERT_FILE_PATTERNS",
    "MissingClientDependencyError",
    "Pkcs12LoadError",
    "ReloadStrategy",
    "ReloadingTrustStore",
    "RestoreHandle",
    "TrustStore",
    "TrustStoreModule",
    "bind_trust_store",
    "install_process_trust",
    "iter_cert_files",
    "to_aiohttp_connector",
    "to_httpx_verify",
    "to_requests_adapter",
    "to_urllib3_poolmanager",
]


def __getattr__(name: str) -> Any:
    """Resolve ``TrustStoreModule`` lazily (PEP 562).

    Every other name in ``__all__`` is bound eagerly above (this package has
    never been import-cheap the way ``varco_core.webhook`` is — ``TrustStore``
    et al. carry no providify dependency of their own). ``TrustStoreModule``
    (Plan 050 / §D-trust) is the one exception: ``tls/module.py`` imports
    ``providify`` and ``varco_core.di.module`` eagerly for its ``@Provider``
    closures (same reason as ``varco_core.webhook.module.WebhookModule``), so
    it is carved out into its own lazy path rather than pulling providify into
    every ``import varco_core.tls`` — including the DI-free `TrustStore`/
    `ReloadingTrustStore` usage this package's own docstring shows.

    Args:
        name: The attribute being looked up on ``varco_core.tls``.

    Returns:
        ``varco_core.tls.module.TrustStoreModule`` on first access; cached as
        a normal module global thereafter.

    Raises:
        AttributeError: If ``name`` is not ``"TrustStoreModule"``.
    """
    if name == "TrustStoreModule":
        from varco_core.tls.module import TrustStoreModule

        globals()["TrustStoreModule"] = TrustStoreModule
        return TrustStoreModule
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
