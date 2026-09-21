"""Two-tier fallback priority model for varco's own default bindings.

Plan 045 (DI-15 close-out / DI-3) replaces three unnamed magic negative
priority floors (formerly the negation of Python's platform integer
ceiling plus one, that same negated ceiling alone, and a removed
``varco_core.auth.authorizer._FALLBACK_PRIORITY`` constant equal to the
negation of two to the 31st power) with providify 2.1.0's ``@Fallback``
marker (``providify/decorator/scope.py`` — bare marker, resolve-time
evaluation, a user binding always wins regardless of registration order or
priority). This module holds the *one* remaining priority literal varco's
own bindings still need.

DESIGN — two tiers, one named constant
---------------------------------------
Every varco default is ``@Fallback``. Within that population there are two
kinds:

1. **In-process defaults** (``InMemoryCache``, ``InMemoryEventBus``,
   ``NullFeatureFlags``, ``BaseAuthorizer``, ...) — carry **no** explicit
   priority (providify's own default, ``0``).
2. **Backend implementations of a core seam** (``KafkaEventBus``,
   ``RedisEventBusSelectorConfiguration.bus``, ``SQLAlchemyRepositoryProvider``,
   ``CasbinPolicyEngine``, ...) — must *outrank* the in-process default once
   scanned/installed, so they carry ``priority=BACKEND_PRIORITY``.

Both tiers remain ``@Fallback`` — an application binding beats either one,
at any priority, because ``@Fallback`` is an activation rule ("yield if a
non-fallback competitor exists"), not a priority comparison. Priority only
resolves ties *among* fallbacks (e.g. an in-process default vs. a scanned
backend for the same seam, both @Fallback, no app binding present).

    request get(AbstractEventBus)          candidates after activation/
                                            qualifier narrowing
      app binding (non-fallback, 0)  ───►  present → every @Fallback
                                            dropped → app wins
      KafkaEventBus (@Fallback, 100, kafka) ┐ all-fallback → max(priority)
      InMemoryEventBus (@Fallback, 0, in_memory) ┘ → Kafka wins
    request get(AbstractEventBus, qualifier="in_memory")
      → only the in_memory-qualified candidate → InMemoryEventBus, always

Why not a negative constant for the defaults (candidate 2 in §D-tiers)?
    ✅ Backends would look "ordinary" (bare ``@Fallback``, no number).
    ❌ It puts the constant on the *larger* population (~31 in-process
       sites vs. ~20 backend sites) and reads as "the old floor under a
       new name" — every future Null/InMemory default would have to
       remember to write it. Rejected.

Why not make backends plain non-fallback bindings at priority 0
(candidate 3)?
    ✅ No constant needed at all.
    ❌ A user's own unqualified ``AbstractEventBus`` binding at priority 0
       would *tie* with a scanned ``KafkaEventBus`` at priority 0 instead
       of losing to it outright — first-registered wins, a regression from
       today (where the user already beat the old negative-ceiling floor). Rejected.

Why ``100``, specifically, and headroom above it?
    Plan 047 (config-driven backend selection) needs an env-driven override
    tier that beats every varco backend but still loses to a real
    application code binding — i.e. a ``@Fallback`` binding at a priority
    *greater than* ``BACKEND_PRIORITY``. ``100`` leaves generous headroom
    for that without colliding with anything varco registers today.

The ``qualifier="in_memory"`` (and other backend-qualifier) rule
------------------------------------------------------------------
``get(T, qualifier="in_memory")`` narrows the candidate set to only
``in_memory``-qualified bindings *before* the fallback-drop runs
(providify ``container.py``'s ``_filter()``), so the in-process default is
returned even when an application binding and/or a backend both exist for
the *unqualified* request. This is the escape hatch every qualified
in-process default (cache, event bus, DLQ, deduplicator, distributed lock)
relies on — it is a specified providify behaviour, not a side effect.

Imperative-stamp ordering caveat
----------------------------------
For a class decorated *imperatively* (e.g. ``Fallback(Singleton(cls))``
rather than with ``@Fallback``/``@Singleton`` at class-definition time),
the marker must be stamped **before** any ``register()``/``bind()``/
``scan()`` call sees that class — ``ClassBinding.fallback`` (and
``ProviderBinding.fallback``) is resolved once, at binding construction.
varco itself no longer has any such imperative stamp site (Plan 045
removed the last three), but an application author replicating this
pattern must respect the same ordering.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Final

if TYPE_CHECKING:
    from providify import DIContainer
    from providify.binding import ProviderBinding

#: Priority carried by every varco binding that implements a core seam via
#: an external backend (Kafka, NATS, Redis, SQLAlchemy, Beanie, Casbin, ...).
#: Outranks the bare (priority 0) in-process default among fallbacks; an
#: application binding of the same interface still always wins, regardless
#: of priority, because both tiers are `@Fallback`. See the module
#: docstring's DESIGN block for why `100` and not a different scheme.
BACKEND_PRIORITY: Final[int] = 100


class AlreadyBoundError(RuntimeError):
    """
    Raised when a `bind_*`/`enable_*` call would register a second
    non-fallback binding of the same `(interface, qualifier)`.

    DESIGN — raise, never silently replace (Plan 046 §D-bind)
    -----------------------------------------------------------
    providify's `container.provide()` **appends**; a tie at equal priority
    resolves first-registered (`container.py:2814-2853`). Every "calling
    twice replaces the previous binding" docstring pre-046 was therefore
    false — the second call produced a second, dead-weight non-fallback
    binding, and an unqualified `get()` request against *two* non-fallback
    candidates is `AMBIGUOUS_BINDING`. Raising here surfaces that failure
    at the call that causes it, named, instead of letting it surface later
    as an opaque resolution-time error.

    ✅ One honest double-call story for all eight `bind_*` and six
       `enable_*` helpers.
    ❌ An app that called `enable_feature_flags()` twice "by accident" now
       fails at startup instead of at first resolution — accepted: it was
       already broken, just silently.

    To replace a binding in a test, use providify's `di_overrides`/
    `ContainerOverrides` fixture (CLAUDE.md's RL-3 rule) rather than a
    second call. To have two bindings of the same interface, qualify them.
    """


def is_user_bound(container: DIContainer, interface: Any, *, qualifier: str | None = None) -> bool:
    """
    Report whether a non-fallback ("user") binding of *interface* already
    exists on *container*.

    A scanned/installed varco default is always `@Fallback`
    (`ClassBinding.fallback` / `ProviderBinding.fallback`) — this function
    is what lets `bind_instance()`/`enable_*()` distinguish "only varco's
    own default is here" (safe to add a binding) from "the app (or a
    previous `enable_*`/`bind_*` call) already bound this seam" (raise
    `AlreadyBoundError` instead of creating a tie).

    Args:
        container: The `DIContainer` to inspect.
        interface: The interface/type to check.
        qualifier: When given, only that exact qualifier is checked
            (`container.py:2096-2101` — an *unqualified* request is
            inclusive of qualified candidates, so `qualifier=None` reports
            `True` if *any* qualified non-fallback binding exists too).

    Returns:
        `True` if `container.get_all_bindings(interface, qualifier=...)`
        contains at least one non-fallback binding, else `False`.

    Edge cases:
        An empty container, or a container holding only `@Fallback`
        bindings of *interface*, both return `False`.

    Example::

        if not is_user_bound(container, AbstractAuthorizer):
            enable_policy_authorizer(container)
    """
    return any(not b.fallback for b in container.get_all_bindings(interface, qualifier=qualifier))


def bind_instance(
    container: DIContainer,
    interface: Any,
    instance: Any,
    *,
    qualifier: str | None = None,
) -> ProviderBinding:
    """
    Register an already-constructed object as a non-fallback singleton
    binding of *interface*.

    The primitive every `bind_*` helper in this codebase now reduces to
    (Plan 046 §D-bind) — `bind_trust_store`, `bind_retention_registry`,
    and `bind_cloudevents_serializer` become one-liners over it; the
    lazy-factory `bind_*` helpers (`bind_websocket_adapter`, `bind_clients`,
    ...) keep their own factory bodies but adopt the same
    `AlreadyBoundError`-on-repeat contract.

    Args:
        container: The `DIContainer` to register the binding into.
        interface: The interface/type the instance is resolved as.
        instance: The already-constructed object to register. Captured by
            a closure, never re-constructed.
        qualifier: Optional qualifier distinguishing this binding from
            another of the same interface.

    Returns:
        The `ProviderBinding` providify created (`.fallback is False`,
        `.qualifier == qualifier`).

    Raises:
        AlreadyBoundError: A non-fallback binding of `(interface,
            qualifier)` already exists — see that class's docstring for
            why this is a raise, not a silent replace.

    Edge cases:
        Registering over an existing `@Fallback` default of the same
        interface is *not* an error — the new binding wins outright
        (`@Fallback` always loses to a non-fallback competitor) and
        `validate()` reports one `FALLBACK_SHADOWED` INFO for a
        provider-shaped default.

    Example::

        bind_instance(container, RedisEventBusSettings, RedisEventBusSettings(use_streams=True))
    """
    if is_user_bound(container, interface, qualifier=qualifier):
        qualifier_suffix = f" qualifier={qualifier!r}" if qualifier else ""
        raise AlreadyBoundError(
            f"A non-fallback binding of {_interface_name(interface)}{qualifier_suffix} "
            "already exists — qualify this binding, or the existing one, to register both, "
            "or use providify's di_overrides/ContainerOverrides in a test to replace it."
        )

    def _factory() -> Any:
        return instance

    _factory.__qualname__ = (
        f"varco.di:bind_instance[{_interface_name(interface)}"
        f"{'@' + qualifier if qualifier else ''}]"
    )

    from providify import Provider  # noqa: PLC0415 — see module docstring: import-budget reason

    return container.provide(
        Provider(singleton=True, qualifier=qualifier)(_factory), returns=interface
    )


def bootstrap(container: DIContainer) -> DIContainer:
    """
    Register every `varco_core` `@Fallback` default onto *container*.

    The tenth `bootstrap()` in the ten-package taxonomy (Plan 046
    §D-contract) — `container.scan("varco_core", recursive=True)` is the
    call every example already made by hand; this makes it the single
    documented entry point (e.g. `BusEventProducer`, Class A10, is a
    `varco_core` `@Fallback` default, so an app that calls this needs no
    separate `setup_event_producer()` step — that helper is deleted).

    Args:
        container: The `DIContainer` to register bindings into. Required —
            `bootstrap()` never creates one and never reads
            the process-global container (§D-locator).

    Returns:
        The same `container` object passed in.

    Edge cases:
        Idempotent — providify's scanner dedups a second scan of the same
        package (`scanner.py:204-206,249-251`), so calling this twice adds
        no new bindings and raises nothing.

    Async safety: No I/O; synchronous scan only.

    Example::

        container = DIContainer()
        varco_core.di.bootstrap(container)
        varco_redis.di.bootstrap(container)
    """
    container.scan("varco_core", recursive=True)
    return container


def _interface_name(interface: Any) -> str:
    """Best-effort human-readable name for an error message (Args: interface; Returns: str)."""
    return getattr(interface, "__name__", None) or repr(interface)


__all__ = [
    "BACKEND_PRIORITY",
    "AlreadyBoundError",
    "bind_instance",
    "bootstrap",
    "is_user_bound",
]
