"""
varco_core.retention.di
==========================
``bind_retention_registry`` — the **only** DI-wiring surface
``varco_core.retention`` exposes. Plan 039 (S20) / Step 17.

Against the DI wiring verb taxonomy (CLAUDE.md):

| Candidate | Verdict |
|---|---|
| ``bootstrap()`` | ❌ one per *package*, wraps ``container.scan(pkg)``. ``varco_core.retention`` must never be scanned into life (§D-S20-shape) — see ``retention/__init__.py``'s docstring. |
| ``enable_*`` | ❌ "flips on an opt-in binding that would shadow an app default". There is no default ``RetentionRegistry`` to shadow — nothing is registered at all. |
| ``mount_*`` | ❌ an ASGI privileged surface; there is none (§Non-goals — no retention admin HTTP surface). |
| ``install_*`` | Used elsewhere, for ``install_retention_metrics()`` only — shape (a), process-global side effect, no container. |
| **``bind_*``** | ✅ *"registers N typed bindings unknowable before app startup"* — the registry holds live repository handles known only at wiring time. Exact precedent: ``varco_core.tls.bind_trust_store(container, store)`` (``varco_core/varco_core/tls/di.py``), which registers an *already-constructed, already-owned* object with **no lifecycle side effect** — the same reason this cannot be a scanned ``@Configuration``. |

DESIGN: no lifecycle side effect
    ✅ Binding a ``RetentionRegistry`` never starts a sweep — that is
       ``RetentionScheduler.start()``'s job (or ``RetentionLifecycle`` in
       ``varco_fastapi``), an explicit, separate act. A DI binding that
       started a deletion loop would repeat the exact
       ``varco_core.tls``-scanned-``@Configuration`` failure mode this
       module's parent package's docstring warns against, with data loss
       instead of a stray file watcher.
    ❌ Two steps to get a running scheduler (bind, then start) instead of
       one. Accepted — deliberately, so binding alone is inert.

Thread safety:  N/A — runs once at startup, synchronously.
Async safety:   ✅ No I/O; registers one singleton binding and returns.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from providify import Provider

from varco_core.retention.policy import RetentionRegistry

if TYPE_CHECKING:
    from providify import DIContainer

__all__ = ["bind_retention_registry"]

# providify's `container.provide()` APPENDS a binding rather than replacing
# one for the same interface (verified against `container.py:1091`,
# `_get_best_candidate`'s `max(candidates, key=priority)` at
# `container.py:2354-2382` — ties resolve to the FIRST-registered binding,
# not the last). `DIContainer.override()` only replaces `ClassBinding`
# entries, not `ProviderBinding` (`container.py:6438-6465`), so it cannot
# express "replace an already-constructed-object binding". A strictly
# increasing `priority=` on each call is the documented mechanism
# `_get_best_candidate` actually honours, and is the only way
# "calling bind_retention_registry twice replaces the binding"
# (test_retention_di.py) can hold without reaching into
# `container._bindings` — see `bind_trust_store`'s identical
# already-constructed-object model, which does not need this because it is
# never called twice for one container in practice.
_bind_call_count = 0


def bind_retention_registry(container: DIContainer, registry: RetentionRegistry) -> None:
    """
    Register an already-constructed ``RetentionRegistry`` as a DI singleton.

    Args:
        container: The ``DIContainer`` to register the binding into.
        registry: A ``RetentionRegistry`` the caller owns — this function
            does not construct, populate, or start anything; it only makes
            ``registry`` resolvable by other DI-managed components (e.g.
            ``varco_fastapi.retention.RetentionLifecycle``).

    Returns:
        ``None`` — the binding is registered as a side effect on
        ``container``.

    Edge cases:
        - Calling this twice on the same container replaces the binding
          (the second ``registry`` wins) — ``providify``'s own
          ``Provider(singleton=True)`` semantics, not special-cased here.
        - Never touches ``DIContainer.current()`` — this binds into the
          ``container`` argument only.

    Example::

        registry = RetentionRegistry()
        registry.register(RetentionPolicy(...))
        bind_retention_registry(container, registry)
        # elsewhere, DI-resolved:
        resolved = container.get(RetentionRegistry)
    """

    global _bind_call_count
    _bind_call_count += 1
    priority = _bind_call_count

    def _registry_factory() -> RetentionRegistry:
        return registry

    container.provide(
        Provider(singleton=True, priority=priority)(_registry_factory), returns=RetentionRegistry
    )
