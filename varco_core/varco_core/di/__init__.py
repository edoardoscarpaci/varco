"""
varco_core.di
===============
Two-tier fallback priority model (Plan 045 / DI-15, DI-3) plus config-driven
backend selection and the default-implementation catalogue (Plan 047 / DI-4,
DI-12, DI-17).

This package re-exports 045/046's original bare-module surface
(``BACKEND_PRIORITY``, ``AlreadyBoundError``, ``bind_instance``,
``bootstrap``, ``is_user_bound``) unchanged, plus Plan 047's selection/
feature/introspection surface. Every submodule follows the same rule: no
``providify`` import at module scope (046's import-budget discipline) —
this ``__init__`` is the one place providify names are imported eagerly
by the *symbols* re-exported here, but the underlying functions still
import providify inside their own bodies, so a bare
``import varco_core.di`` (with no call into it) never imports providify.

DESIGN — two tiers, one named constant per tier
-------------------------------------------------
See the historical module docstring (kept verbatim below the constant
definitions) for the full priority-tier rationale (045). Plan 047 adds a
third, higher tier: ``ENV_OVERRIDE_PRIORITY`` for the dotted-path
override (§D-precedence) — ``code > env-dotted (200) > env-named (100) >
default (0)``, all three of the latter remaining ``@Fallback`` so a real
application binding always wins outright, regardless of priority.
"""

from __future__ import annotations

import weakref
from typing import TYPE_CHECKING, Any, Final

from varco_core.di.errors import BackendSelectionError, CatalogueError
from varco_core.di.features import FEATURES, Feature, feature_default
from varco_core.di.introspect import BindingRecord, Catalogue, catalogue, inspect_bindings
from varco_core.di.seams import SEAMS, Seam, interface_of, seam_by_name
from varco_core.di.select import (
    BackendSelectionReport,
    SeamCheck,
    SeamSelection,
    bind_env_overrides,
    check_backend_selection,
    env_truthy,
    inspect_backend_selection,
    selected_by,
)

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

#: Priority carried by the dotted-path env override provider
#: (`bind_env_overrides()`, §D-dotted). Outranks `BACKEND_PRIORITY` among
#: fallbacks — a `VARCO_<SEAM>_BACKEND=pkg.mod:Name` override wins over a
#: shipped backend's named value, but still loses outright to a real
#: application code binding (both are `@Fallback`; see §D-precedence).
ENV_OVERRIDE_PRIORITY: Final[int] = 200


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

    Args:
        container: The `DIContainer` to inspect.
        interface: The interface/type to check.
        qualifier: When given, only that exact qualifier is checked.

    Returns:
        `True` if `container.get_all_bindings(interface, qualifier=...)`
        contains at least one non-fallback binding, else `False`.

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

    Args:
        container: The `DIContainer` to register the binding into.
        interface: The interface/type the instance is resolved as.
        instance: The already-constructed object to register.
        qualifier: Optional qualifier distinguishing this binding.

    Returns:
        The `ProviderBinding` providify created (`.fallback is False`).

    Raises:
        AlreadyBoundError: A non-fallback binding of `(interface,
            qualifier)` already exists.

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


#: Containers that have already had Plan 047's extra bootstrap-time
#: registrations applied (the dotted-path overrides are their own
#: idempotency mechanism; this guards the *explicit* `container.provide()`
#: calls below, since providify's `provide()` always appends — Plan 047
#: §D-dotted step 4's "the same shape 048's provenance registry uses").
_CORE_EXTRAS_REGISTERED: weakref.WeakSet[Any] = weakref.WeakSet()


def bootstrap(container: DIContainer) -> DIContainer:
    """
    Register every `varco_core` `@Fallback` default onto *container*,
    then apply Plan 047's selection layer (§D-dotted, §D-mechanism).

    Args:
        container: The `DIContainer` to register bindings into. Required.

    Returns:
        The same `container` object passed in.

    Edge cases:
        Idempotent — `container.scan()` dedups a second scan
        (`scanner.py:204-206,249-251`); `bind_env_overrides()` and the
        explicit revocation-twin registration below each track their own
        per-container idempotency, so a second `bootstrap()` call adds no
        new bindings and raises nothing.

    Async safety: No I/O; synchronous scan only.

    Example::

        container = DIContainer()
        varco_core.di.bootstrap(container)
        varco_redis.di.bootstrap(container)
    """
    container.scan("varco_core", recursive=True)
    bind_env_overrides(container)

    if container not in _CORE_EXTRAS_REGISTERED:
        # N9 — the three feature-twin env providers (revocation, feature
        # flags, tenant membership), deliberately registered by hand
        # rather than left to `scan()`. `varco_core.revocation.di`
        # documents that `scan("varco_core.revocation")` alone must never
        # silently start checking revocation (CLAUDE.md's revocation
        # two-step rule) — an explicit `provide()` call here keeps that
        # invariant literally true even though each twin is itself
        # `@Requires`-gated and therefore inert unless selected.
        #
        # Repair round, Decision 4: `_feature_flags_when_enabled`/
        # `_tenant_membership_when_enabled` used to be PUBLIC, scan-
        # discovered providers (`feature_flags_when_enabled`/
        # `tenant_membership_when_enabled`) — the opposite shape from the
        # revocation twin. All three now share the private,
        # hand-registered shape the revocation twin already used, for
        # one uniform pattern instead of two.
        from varco_core.flags.di import (
            _feature_flags_when_enabled,  # noqa: PLC0415
        )
        from varco_core.revocation.di import (
            _in_memory_revocation_store_when_selected,  # noqa: PLC0415
        )
        from varco_core.tenancy.di import (
            _tenant_membership_when_enabled,  # noqa: PLC0415
        )

        container.provide(_in_memory_revocation_store_when_selected)
        container.provide(_feature_flags_when_enabled)
        container.provide(_tenant_membership_when_enabled)
        _CORE_EXTRAS_REGISTERED.add(container)

    return container


def _interface_name(interface: Any) -> str:
    """Best-effort human-readable name for an error message (Args: interface; Returns: str)."""
    return getattr(interface, "__name__", None) or repr(interface)


__all__ = [
    "BACKEND_PRIORITY",
    "ENV_OVERRIDE_PRIORITY",
    "AlreadyBoundError",
    "BackendSelectionError",
    "BackendSelectionReport",
    "BindingRecord",
    "CatalogueError",
    "Catalogue",
    "Feature",
    "FEATURES",
    "Seam",
    "SEAMS",
    "SeamCheck",
    "SeamSelection",
    "bind_env_overrides",
    "bind_instance",
    "bootstrap",
    "catalogue",
    "check_backend_selection",
    "env_truthy",
    "feature_default",
    "inspect_backend_selection",
    "inspect_bindings",
    "interface_of",
    "is_user_bound",
    "seam_by_name",
    "selected_by",
]
