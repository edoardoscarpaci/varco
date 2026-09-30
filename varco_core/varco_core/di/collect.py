"""
varco_core.di.collect
========================
The one shared convention every "all implementations of X" collection seam
in varco follows (Plan 051 / DI-5): ``contribute()``, ``collect()``,
``acollect()``, ``collection_seams()``.

DESIGN — the convention, verbatim (§D-shape)
-----------------------------------------------
A collection seam is an ABC (or a frozen value type) stamped
``@Multibound`` (``providify.Multibound``). A contribution is any ordinary
non-``@Fallback`` binding of that type — a scanned ``@Singleton`` subclass,
a ``@Provider`` returning it, or ``contribute(container, Seam, obj)``. A
consumer reads it as a bare ``list[Seam]`` constructor hint or via
``collect()``/``acollect()``, which return an immutable ``tuple``. Zero
contributions is a legal, empty tuple — providify's own zero-tolerant
terminal (``_collect_sync``/``_collect_async``, ``container.py:1756-1763``)
is what makes this true; ``InjectInstances[T]`` is never used by varco
because it raises ``LookupError`` on zero contributions instead.

DESIGN — providify ``priority`` is never a collection order key (§D-order)
------------------------------------------------------------------------------
providify orders a ``list[T]``/``collect()`` result by **ascending**
priority, ties in registration order (``container.py:1704-1707,1793-1796``).
Because the same ``priority`` number also picks the *single* ``get(T)``
winner (**highest** wins, ``_get_best_candidate``'s ``max()``), one number
means opposite things for the two reads. **No varco contribution carries
``priority=``** — every consumer treats the tuple as a set with a stable,
documented-as-incidental order ("registration order — the order your
``bootstrap()``/``scan()`` calls ran"). The one seam whose order is
semantic (the tenant chain) sorts by its own ``TenantTrust`` domain key,
never by providify priority.

DESIGN — the tuple is made at the consumer, once (§D-immutable)
--------------------------------------------------------------------
providify hands back a fresh ``list`` per resolution
(``container.py:1793-1796``) — never a tuple. ``collect()``/``acollect()``
convert to ``tuple(...)`` so every consumer stores an immutable collection.
providify has no de-dup mechanism (brief 014 §1: neither does Spring, .NET,
or Guice) — ``contribute()`` refuses the exact **same object** twice
(identity); a seam whose contributions have their own notion of "the same
thing" (a policy ``name``, a chain source ``name``) enforces that
uniqueness itself, at its own consumption point, never here.

DESIGN — ``contribute()`` vs. ``bind_instance()``
------------------------------------------------------
| | ``bind_instance()`` (046) | ``contribute()`` (this module) |
|---|---|---|
| Says | "the one implementation of *interface*" | "one of many contributions to *seam*" |
| Second call, same object | Raises ``AlreadyBoundError`` | Raises ``AlreadyBoundError`` (identity) |
| Second call, a *different* object | Raises ``AlreadyBoundError`` | Registers a second, distinct contribution — this is the point |
| Binding shape | Non-fallback singleton | Non-fallback singleton |

Both share ``AlreadyBoundError``'s message shape and ``046``'s
``@Fallback`` discipline, but the two verbs cannot share one name — a
seam's whole purpose is admitting more than one non-fallback binding of the
same type, which is exactly what ``bind_instance()`` refuses.

Thread safety:  ⚠️ Every function here mutates/reads a ``DIContainer`` —
                    intended for single-threaded startup, same contract as
                    every other ``varco_core.di`` verb.
Async safety:   ✅ ``collect()`` is synchronous; ``acollect()`` awaits
                    providify's own async collection path
                    (``_collect_async``) for a seam contributed by an
                    ``async def @Provider`` factory.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, TypeVar

if TYPE_CHECKING:
    from providify import DIContainer
    from providify.binding import ProviderBinding

__all__ = ["contribute", "collect", "acollect", "collection_seams"]

T = TypeVar("T")


def contribute(container: DIContainer, seam: type[T], instance: T) -> ProviderBinding:
    """
    Register an already-constructed contribution to a ``@Multibound`` seam.

    Args:
        container: The ``DIContainer`` to register the binding into.
        seam: The ``@Multibound``-stamped type *instance* contributes to.
        instance: The already-constructed object to contribute.

    Returns:
        The ``ProviderBinding`` providify created — non-fallback, singleton,
        ``.interface is seam``.

    Raises:
        AlreadyBoundError: This exact object (identity, not equality) has
            already been contributed to *seam* on this container — almost
            certainly a double-bootstrap bug. Two distinct, even
            value-equal, objects are both legitimate contributions.

    Example::

        contribute(container, RetentionPolicy, my_policy)
        contribute(container, TenantSource, PartnerHeaderTenantSource())
    """
    from providify import Provider  # noqa: PLC0415 — import-budget discipline

    from varco_core.di import AlreadyBoundError  # noqa: PLC0415

    seam_name = getattr(seam, "__name__", repr(seam))
    for binding in container.get_all_bindings(seam):
        fn = getattr(binding, "fn", None)
        if getattr(fn, "__varco_contributed__", None) is instance:
            raise AlreadyBoundError(
                f"This exact {seam_name} instance is already contributed to "
                f"{seam_name} on this container — contribute() refuses a "
                "double contribution of the same object (almost always a "
                "double-bootstrap bug). A distinct, even value-equal, "
                "object is a legitimate second contribution."
            )

    def _factory() -> T:
        return instance

    _factory.__qualname__ = f"varco.di:contribute[{seam_name}]"
    _factory.__varco_contributed__ = instance  # type: ignore[attr-defined]

    return container.provide(Provider(singleton=True)(_factory), returns=seam)


def collect(container: DIContainer, seam: type[T]) -> tuple[T, ...]:
    """
    Read every non-fallback (and shadowed-fallback-excluded) contribution to
    a ``@Multibound`` seam, as an immutable tuple.

    Args:
        container: The ``DIContainer`` to read from.
        seam: The ``@Multibound``-stamped type to collect.

    Returns:
        A ``tuple`` of every contribution — providify's ascending-priority,
        stable-tie order (§D-order), which varco never reads as meaning.
        ``()`` when nothing has been contributed — never a ``LookupError``.

    Edge cases:
        A seam contributed only by an ``async def @Provider`` factory raises
        ``RuntimeError`` here (providify's ``_collect_sync`` refuses an
        async provider) — use ``acollect()`` for such a seam.
    """
    return tuple(container.get(list[seam]))  # type: ignore[valid-type]


async def acollect(container: DIContainer, seam: type[T]) -> tuple[T, ...]:
    """
    The ``async`` counterpart of ``collect()`` — awaits an ``async def
    @Provider`` contribution instead of raising.

    Args:
        container: The ``DIContainer`` to read from.
        seam: The ``@Multibound``-stamped type to collect.

    Returns:
        A ``tuple`` of every contribution, in providify's order. ``()``
        when nothing has been contributed.
    """
    return tuple(await container.aget(list[seam]))  # type: ignore[valid-type]


def collection_seams() -> tuple[type, ...]:
    """
    Every ``@Multibound`` collection seam varco ships.

    Imports each seam module **inside this function body** — never at
    module scope — so a bare ``import varco_core.di.collect`` (or
    ``import varco_core.di``) pulls in none of ``varco_core.retention``,
    ``varco_core.tenancy``, ``varco_core.posture``, ``varco_core.event``,
    or ``varco_core.health`` (the PEP 562 import-budget discipline every
    ``varco_core.di`` submodule follows).

    Returns:
        ``(EventConsumer, RetentionPolicy, TenantSource, PostureCollector,
        HealthCheck)`` — the five seams. Read by the ``di_wiring``
        conformance suite and by Plan 052's ``varco diagnose``.
    """
    from varco_core.event.consumer import EventConsumer  # noqa: PLC0415
    from varco_core.health import HealthCheck  # noqa: PLC0415
    from varco_core.posture import PostureCollector  # noqa: PLC0415
    from varco_core.retention.policy import RetentionPolicy  # noqa: PLC0415
    from varco_core.tenancy.source import TenantSource  # noqa: PLC0415

    return (EventConsumer, RetentionPolicy, TenantSource, PostureCollector, HealthCheck)
