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

from typing import Final

#: Priority carried by every varco binding that implements a core seam via
#: an external backend (Kafka, NATS, Redis, SQLAlchemy, Beanie, Casbin, ...).
#: Outranks the bare (priority 0) in-process default among fallbacks; an
#: application binding of the same interface still always wins, regardless
#: of priority, because both tiers are `@Fallback`. See the module
#: docstring's DESIGN block for why `100` and not a different scheme.
BACKEND_PRIORITY: Final[int] = 100

__all__ = ["BACKEND_PRIORITY"]
