"""
varco_core.tls.di
==================

``bind_trust_store`` — the **only** DI-wiring surface ``varco_core.tls`` exposes
(Plan 026 / T3b, §D-T3-oq3).

**No ``@Configuration`` here, and this is deliberate, not an oversight.**
``container.scan("varco_core", recursive=True)`` is a documented, in-use pattern
(``README.md``; ``varco_fastapi/tests/test_di_binding_health_i18n_tz.py``;
``varco_core/tests/test_observability_di.py``) that **auto-activates** every scanned
``@Configuration`` class. A ``TlsConfiguration`` living in this package would therefore start
a background filesystem watcher in *every* app that scans ``varco_core`` — the exact opposite
of the locked "Auto-injection: explicit, opt-in, never implicit" decision (``BACKLOG.md:38``),
which brief 001 §3 grounds in the ``truststore`` library's own instruction that *libraries
must not* inject. ``varco_core/tests/test_tls_di.py`` is the mechanical guard that keeps this
true over time: it scans ``varco_core`` and asserts no TLS binding and no watcher appear.

Instead, ``ReloadingTrustStore.start()``/``.stop()`` structurally satisfy
``varco_fastapi.lifespan.AbstractLifecycle`` (a ``runtime_checkable`` Protocol, Plan 025's
Step 14 test proves this) — a FastAPI app registers a store built and started by the
*application*, via ``lifespan.register(store)``, with zero import from ``varco_core`` to
``varco_fastapi``. Non-FastAPI consumers use ``async with store:`` or call ``start()``/
``stop()`` directly. ``bind_trust_store`` only makes an already-constructed store resolvable
by other DI-managed components — it never constructs, starts, or owns one itself.

Thread safety:  N/A — ``bind_trust_store`` runs once at startup, synchronously.
Async safety:   ✅ No I/O; registers two singleton bindings and returns.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from varco_core.di import bind_instance
from varco_core.tls.reload import ReloadingTrustStore
from varco_core.tls.store import TrustStore

if TYPE_CHECKING:
    from providify import DIContainer


def bind_trust_store(container: DIContainer, store: ReloadingTrustStore) -> DIContainer:
    """
    Register an already-constructed ``ReloadingTrustStore`` (and its ``TrustStore`` spec) as
    DI singletons.

    Args:
        container: The ``DIContainer`` to register bindings into.
        store: A ``ReloadingTrustStore`` the caller owns the lifecycle of — this function
            does **not** call ``start()``/``stop()`` on it (§D-T3-oq3's "no lifecycle side
            effect" rule). An unstarted store, once resolved, still raises
            ``ResourceNotLoadedError`` on ``.context`` access — bind_trust_store cannot make
            an unstarted store appear started.

    Returns:
        The same ``container`` passed in — both bindings are registered as a
        side effect (Plan 046 §D-bind — both calls go through
        ``varco_core.di.bind_instance()``).

    Raises:
        AlreadyBoundError: A non-fallback ``ReloadingTrustStore`` or
            ``TrustStore`` binding already exists — use providify's
            ``di_overrides``/``ContainerOverrides`` in a test to replace it.

    Example::

        spec = TrustStore(ca_folders=Path("/etc/ssl/private-ca"))
        store = ReloadingTrustStore(spec)
        bind_trust_store(container, store)
        await store.start()  # the caller's responsibility, not this function's

        # elsewhere, DI-resolved:
        resolved = container.get(ReloadingTrustStore)
    """
    bind_instance(container, ReloadingTrustStore, store)
    bind_instance(container, TrustStore, store.spec)
    return container


__all__ = ["bind_trust_store"]
