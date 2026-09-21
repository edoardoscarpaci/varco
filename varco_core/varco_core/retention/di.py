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

from varco_core.di import bind_instance
from varco_core.retention.policy import RetentionRegistry

if TYPE_CHECKING:
    from providify import DIContainer

__all__ = ["bind_retention_registry"]


def bind_retention_registry(container: DIContainer, registry: RetentionRegistry) -> DIContainer:
    """
    Register an already-constructed ``RetentionRegistry`` as a DI singleton.

    Args:
        container: The ``DIContainer`` to register the binding into.
        registry: A ``RetentionRegistry`` the caller owns — this function
            does not construct, populate, or start anything; it only makes
            ``registry`` resolvable by other DI-managed components (e.g.
            ``varco_fastapi.retention.RetentionLifecycle``).

    Returns:
        The same ``container`` passed in (Plan 046 §D-bind — this is now a
        one-liner over ``varco_core.di.bind_instance()``; the previous
        ``_bind_call_count`` priority-escalation hack is gone).

    Raises:
        AlreadyBoundError: A non-fallback ``RetentionRegistry`` binding
            already exists — use providify's ``di_overrides``/
            ``ContainerOverrides`` in a test to replace it.

    Edge cases:
        - Never reaches for the process-global container — this binds into the
          ``container`` argument only.

    Example::

        registry = RetentionRegistry()
        registry.register(RetentionPolicy(...))
        bind_retention_registry(container, registry)
        # elsewhere, DI-resolved:
        resolved = container.get(RetentionRegistry)
    """
    bind_instance(container, RetentionRegistry, registry)
    return container
