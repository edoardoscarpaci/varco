"""
varco_core.di.select
======================
The selection mechanism (Plan 047 §D-mechanism, §D-dotted, §D-check):
``selected_by()`` (the decorator every varco backend binding carries),
``env_truthy``/``EnvFlag`` (the feature on/off predicate primitive),
``check_backend_selection()`` (typo/"bootstrapped but unselected"
diagnostics), ``bind_env_overrides()`` (the dotted-path override tier),
and ``inspect_backend_selection()`` (the runtime "who is live and why"
read).

providify is imported inside function bodies throughout this module
(never at module scope) — the same import-budget discipline
``varco_core/varco_core/di/__init__.py``'s ``bind_instance`` already
follows (046).
"""

from __future__ import annotations

import importlib
import logging
import os
import re
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from varco_core.di.errors import BackendSelectionError
from varco_core.di.seams import SEAMS, interface_of, seam_by_name

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from providify import DIContainer

_logger = logging.getLogger(__name__)

#: `[a-z][a-z0-9-]*` — §D-vocab's value shape. Exact, lower-case only;
#: `Redis` != `redis` (providify `@Requires(value=)` is case-sensitive).
_VALUE_RE = re.compile(r"^[a-z][a-z0-9-]*$")

#: Every named value `selected_by()` has recorded *in this process*, keyed
#: by seam name — populated at import time of the backend module that
#: decorates a binding with it (the marker's own lifetime; see the plan's
#: §D-check). Never cleared — a value registered once (by importing the
#: backend module) stays known for the rest of the process.
_REGISTERED_VALUES: dict[str, set[str]] = {}

#: Per-container idempotency guard for `bind_env_overrides()` — a container
#: bootstrapped twice must not register a second dotted-path provider for
#: the same seam (§D-dotted step 4). `WeakKeyDictionary` so a container
#: that goes out of scope does not leak an entry here forever.
import weakref  # noqa: E402 — after the module-level constants it guards

_ENV_OVERRIDE_REGISTERED: weakref.WeakKeyDictionary[Any, set[str]] = weakref.WeakKeyDictionary()


def selected_by(seam_name: str, value: str) -> Callable[[Any], Any]:
    """
    Decorator gating a varco backend binding on ``VARCO_<SEAM>_BACKEND ==
    value`` (§D-mechanism candidate 1).

    Validates *value*'s shape and records it into the process-wide
    ``_REGISTERED_VALUES`` registry (what ``check_backend_selection()``
    considers "known" for this seam) **before** returning the decorator —
    calling ``selected_by(seam, value)`` alone (without applying the
    result to anything) already performs both checks and the recording,
    so a caller who merely wants to validate a value need not decorate
    anything.

    Args:
        seam_name: One of the six registered seam keys (``"cache"``,
            ``"dlq"``, ``"idempotency"``, ``"job_store"``,
            ``"event_bus"``, ``"revocation"``).
        value: The backend's named value. Must match ``[a-z][a-z0-9-]*``
            and must not equal the seam's default value name (``"memory"``/
            ``"null"``) — nothing may shadow the default.

    Returns:
        A decorator: applies ``providify.Requires(env=seam.env,
        value=value)`` to the target (a class or a ``@Provider`` method)
        and, for convenience/testability, mirrors the resulting marker
        tuple onto a plain ``target.__requires__`` attribute (providify's
        own marker lives under the private ``__di_requires__`` name —
        this mirror is the one place varco reads it, so no other module
        needs to know that name).

    Raises:
        LookupError: ``seam_name`` is not a registered seam
            (``seam_by_name()``).
        ValueError: ``value`` does not match ``[a-z][a-z0-9-]*``, or
            equals the seam's default value name.

    Example::

        @Fallback
        @Singleton(priority=BACKEND_PRIORITY, qualifier="redis")
        @selected_by("cache", "redis")
        class RedisCache(CacheBackend): ...
    """
    seam = seam_by_name(seam_name)
    if not _VALUE_RE.match(value):
        raise ValueError(
            f"selected_by({seam_name!r}, {value!r}): value must match "
            f"[a-z][a-z0-9-]* — values are exact and lower-case."
        )
    if value == seam.default_value:
        raise ValueError(
            f"selected_by({seam_name!r}, {value!r}): {value!r} is the seam's own "
            f"default value name — nothing may shadow the in-process default."
        )

    _REGISTERED_VALUES.setdefault(seam_name, set()).add(value)

    def decorator(target: Any) -> Any:
        from providify import Requires  # noqa: PLC0415 — import-budget rule

        Requires(env=seam.env, value=value)(target)
        # Mirror providify's private marker tuple onto a plain, documented
        # attribute — the one read site for it in this codebase.
        target.__requires__ = getattr(target, "__di_requires__", ())
        return target

    return decorator


class EnvFlag:
    """
    A zero-arg, named predicate reading one environment variable's
    truthiness — the callable ``@Requires(condition=...)`` needs
    (§D-features).

    Args:
        env: The environment variable name to read at call time.

    Thread safety: ✅ Pure read of ``os.environ``.
    Async safety:  ✅ No await points.

    Example::

        flag = EnvFlag("VARCO_FEATURE_FLAGS_ENABLED")
        flag()  # True/False, read fresh every call
    """

    def __init__(self, env: str) -> None:
        self.env = env
        # __qualname__/__name__ make CONDITION_INACTIVE's rendered message
        # readable (RequiresMarker.describe() -> "condition=<qualname>").
        self.__qualname__ = f"env_truthy({env!r})"
        self.__name__ = self.__qualname__

    def __call__(self) -> bool:
        raw = os.environ.get(self.env, "")
        return raw.strip().lower() in {"1", "true", "yes", "on"}

    def __repr__(self) -> str:
        return self.__qualname__


def env_truthy(name: str) -> EnvFlag:
    """
    Build an ``EnvFlag`` predicate for *name* (§D-features).

    Args:
        name: The environment variable to check.

    Returns:
        A zero-arg callable returning ``True`` for
        ``1``/``true``/``yes``/``on`` (case-insensitive), else ``False``
        (including unset).

    Example::

        @Requires(condition=env_truthy("VARCO_FEATURE_FLAGS_ENABLED"))
        @Provider(singleton=True)
        def feature_flags_when_enabled() -> AbstractFeatureFlags: ...
    """
    return EnvFlag(name)


@dataclass(frozen=True)
class SeamCheck:
    """One seam's env-var state, as read by ``check_backend_selection()``."""

    seam: str
    env: str
    raw_value: str | None
    kind: str  # "unset" | "default" | "named" | "dotted"
    known_values: tuple[str, ...]
    registered_values: frozenset[str]


@dataclass(frozen=True)
class BackendSelectionReport:
    """The full per-seam check, returned by ``check_backend_selection()``."""

    checks: tuple[SeamCheck, ...] = field(default_factory=tuple)


@dataclass(frozen=True)
class SeamSelection:
    """
    One seam's live selection, as read by ``inspect_backend_selection()``.

    Args:
        seam: The seam's registry key.
        value: The configured value — the raw ``VARCO_<SEAM>_BACKEND``
            string when set, else the seam's default value name.
        source: ``"code"`` (a non-fallback binding exists — always wins,
            regardless of the variable), ``"env-dotted"`` (the value
            contains ``:``), ``"env"`` (a plain named value is set), or
            ``"default"`` (unset).
        live_owner: Best-effort human-readable name of what actually
            resolves today (``None`` if nothing is bound at all — should
            not happen for any of the six seams, which all ship an
            in-process default).
    """

    seam: str
    value: str
    source: str
    live_owner: str | None


def _known_values_for(seam_name: str) -> tuple[str, ...]:
    """The seam's default plus every value registered in this process."""
    seam = seam_by_name(seam_name)
    registered = sorted(_REGISTERED_VALUES.get(seam_name, set()))
    return (seam.default_value, *registered)


def evaluate_backend_selection(
    *, environ: Mapping[str, str] | None = None
) -> BackendSelectionReport:
    """
    The pure half of :func:`check_backend_selection` — classify every seam's
    ``VARCO_<SEAM>_BACKEND`` value and raise on a typo, but log nothing.

    Exists so the wiring report (Plan 052 / §D-wired) can turn the
    "bootstrapped but unselected" case into a ``WiringFinding`` without
    emitting the same WARNING a second time.

    Args:
        environ: Defaults to ``os.environ``. Overridable for tests.

    Returns:
        A ``BackendSelectionReport`` — one ``SeamCheck`` per seam.

    Raises:
        BackendSelectionError: A named value is neither the seam's default
            nor registered in this process.
    """
    env = environ if environ is not None else os.environ
    checks: list[SeamCheck] = []
    for seam in SEAMS:
        raw = env.get(seam.env)
        registered = frozenset(_REGISTERED_VALUES.get(seam.seam, set()))
        if not raw:
            kind = "unset"
        elif ":" in raw:
            kind = "dotted"
        elif raw == seam.default_value:
            kind = "default"
        else:
            kind = "named"
        checks.append(
            SeamCheck(
                seam=seam.seam,
                env=seam.env,
                raw_value=raw,
                kind=kind,
                known_values=_known_values_for(seam.seam),
                registered_values=registered,
            )
        )

    for check in checks:
        if check.kind == "named" and check.raw_value not in check.registered_values:
            raise BackendSelectionError(
                f"{check.env}={check.raw_value!r} is not a known backend value — "
                f"known values: {', '.join(check.known_values)}. Values are exact "
                f"and lower-case; did you bootstrap the package that registers "
                f"{check.raw_value!r}?"
            )

    return BackendSelectionReport(checks=tuple(checks))


def check_backend_selection(
    container: DIContainer,
    *,
    environ: Mapping[str, str] | None = None,
) -> BackendSelectionReport:
    """
    Validate every ``VARCO_<SEAM>_BACKEND`` variable against what this
    process actually knows how to serve (§D-check).

    Args:
        container: Unused directly today (kept for the documented call
            sites and forward compatibility with a container-aware check)
            — the check is env-only, per §D-mechanism's accepted
            trade-off. Still required so a caller passes the same
            container it just bootstrapped, keeping the call-site
            docstring's "call after every bootstrap()" contract visible.
        environ: Defaults to ``os.environ``. Overridable for tests.

    Returns:
        A ``BackendSelectionReport`` — one ``SeamCheck`` per seam.

    Raises:
        BackendSelectionError: A named value (no ``:``) is neither the
            seam's default name nor among the values registered in this
            process (a typo, wrong case, or an unbootstrapped package).

    Edge cases:
        Logs one ``WARNING`` (never raises) per seam whose variable is
        unset AND at least one backend value was registered in this
        process — "bootstrapped but unselected". An explicit
        ``VARCO_<SEAM>_BACKEND=<default>`` silences it (kind ``"default"``,
        not ``"unset"``).

    Async safety: No awaits — a synchronous, pure-ish check (one log
        call as its only side effect).

    Example::

        container = DIContainer()
        varco_core.di.bootstrap(container)
        varco_redis.di.bootstrap(container)
        check_backend_selection(container)  # warns: redis bootstrapped, unselected
    """
    report = evaluate_backend_selection(environ=environ)
    checks = report.checks

    for check in checks:
        if check.kind == "unset" and check.registered_values:
            default_value = seam_by_name(check.seam).default_value
            _logger.warning(
                "%s is bootstrapped (registered values: %s) but %s is unset — "
                "the in-process default is live; set it, or set it to %r to "
                "silence this warning.",
                check.seam,
                ", ".join(sorted(check.registered_values)),
                check.env,
                default_value,
            )

    return BackendSelectionReport(checks=tuple(checks))


def bind_env_overrides(container: DIContainer) -> DIContainer:
    """
    Register the dotted-path override tier (§D-dotted) for every seam
    whose ``VARCO_<SEAM>_BACKEND`` value contains ``:``.

    Called by ``varco_core.di.bootstrap()`` — every app runs it first.

    Args:
        container: The ``DIContainer`` to register the override
            provider(s) into.

    Returns:
        The same container.

    Raises:
        BackendSelectionError: The module cannot be imported, the
            attribute does not exist, or (for a class target) the class
            does not subclass the seam's interface — all checked **at
            this call**, never deferred to first ``get()``. A *callable*
            target's return value is only ``isinstance``-checked lazily,
            at first resolution (no bootstrap-time signature inspection —
            §D-dotted step 2).

    Edge cases:
        Idempotent per container — a second call registers nothing new
        for a seam already processed on this container (tracked in a
        ``WeakKeyDictionary``, never a container attribute).

    Example::

        os.environ["VARCO_EVENT_BUS_BACKEND"] = "myapp.buses:MyBus"
        bind_env_overrides(container)
    """
    from providify import Fallback, Provider  # noqa: PLC0415 — import-budget rule

    from varco_core.di import ENV_OVERRIDE_PRIORITY  # noqa: PLC0415 — avoids a cycle at import time

    registered_seams = _ENV_OVERRIDE_REGISTERED.setdefault(container, set())

    for seam in SEAMS:
        raw = os.environ.get(seam.env)
        if not raw or ":" not in raw:
            continue
        if seam.seam in registered_seams:
            continue

        module_path, _, attr_name = raw.partition(":")
        try:
            module = importlib.import_module(module_path)
        except ImportError as exc:
            raise BackendSelectionError(
                f"{seam.env}={raw!r}: cannot import module {module_path!r}: {exc}"
            ) from exc

        target = getattr(module, attr_name, None)
        if target is None:
            raise BackendSelectionError(
                f"{seam.env}={raw!r}: module {module_path!r} has no attribute {attr_name!r}."
            )

        interface = interface_of(seam)
        if isinstance(target, type):
            if not issubclass(target, interface):
                raise BackendSelectionError(
                    f"{seam.env}={raw!r}: {target!r} does not subclass "
                    f"{interface.__name__} — use a zero-arg factory "
                    f"'pkg.mod:make_x' or bind it in code."
                )
        elif not callable(target):
            raise BackendSelectionError(
                f"{seam.env}={raw!r}: {attr_name!r} is neither a class nor a callable."
            )

        def _factory(
            _target: Any = target,
            _interface: type = interface,
            _raw: str = raw,
            _env: str = seam.env,
        ) -> Any:
            instance = _target()
            if not isinstance(instance, _interface):
                raise BackendSelectionError(
                    f"{_env}={_raw!r}: constructed instance {instance!r} is not a "
                    f"{_interface.__name__}."
                )
            return instance

        _factory.__qualname__ = f"varco.di:env_override[{seam.seam}]"
        Fallback(Provider(singleton=True, priority=ENV_OVERRIDE_PRIORITY)(_factory))
        container.provide(_factory, returns=interface)
        registered_seams.add(seam.seam)

    return container


def inspect_backend_selection(container: DIContainer) -> tuple[SeamSelection, ...]:
    """
    Report, per seam, what is actually configured and why (§D-introspect).

    Args:
        container: The bootstrapped container to inspect.

    Returns:
        One ``SeamSelection`` per registered seam, in ``SEAMS`` order.

    Edge cases:
        - A seam with no binding at all (should not happen — every seam
          ships an in-process default) reports ``live_owner=None`` rather
          than raising.
        - **Deliberate design (repair round, Decision 3 — §D-introspect in
          ``plans/047-config-driven-backend-selection-and-catalogue.md``):**
          ``source`` is derived from the *raw* ``VARCO_<SEAM>_BACKEND``
          string's own shape (unset/empty → ``"default"``; contains ``":"``
          → ``"env-dotted"``; anything else → ``"env"``), never from
          ``container.get_binding(interface).priority``. This matters for
          exactly one edge case: an env var names a backend whose owning
          package was never bootstrapped (e.g. ``VARCO_CACHE_BACKEND=redis``
          with no ``varco_redis.di.bootstrap(container)`` call). There, the
          requested backend's own ``@Fallback`` binding was never
          registered, so ``get_binding()`` resolves to the *default's*
          binding at priority ``0`` — a priority read would report
          ``source="default"``, silently hiding the operator's actual
          intent. The raw-string reading instead reports ``source="env"``
          (what was asked for) while ``live_owner`` still names the
          default's owner (what is actually serving the seam) — the two
          fields are allowed to disagree on this one case, by design, so
          together they show both the intent and the reality.
          ``check_backend_selection()`` is the function that raises
          ``BackendSelectionError`` on this same inconsistent state; this
          function only reports it.

    Example::

        for sel in inspect_backend_selection(container):
            print(sel.seam, sel.value, sel.source, sel.live_owner)
    """
    from varco_core.di import is_user_bound  # noqa: PLC0415 — import-budget rule

    selections: list[SeamSelection] = []
    for seam in SEAMS:
        interface = interface_of(seam)
        raw = os.environ.get(seam.env)

        if is_user_bound(container, interface):
            source = "code"
            value = raw if raw else seam.default_value
        elif not raw:
            source = "default"
            value = seam.default_value
        elif ":" in raw:
            source = "env-dotted"
            value = raw
        else:
            source = "env"
            value = raw

        try:
            binding = container.get_binding(interface)
        except LookupError:
            live_owner = None
        else:
            live_owner = _binding_owner_name(binding)

        selections.append(
            SeamSelection(seam=seam.seam, value=value, source=source, live_owner=live_owner)
        )

    return tuple(selections)


def _binding_owner_name(binding: Any) -> str:
    """Best-effort human-readable name of a binding's implementation."""
    implementation = getattr(binding, "implementation", None)
    if implementation is not None:
        return getattr(implementation, "__name__", repr(implementation))
    fn = getattr(binding, "fn", None)
    if fn is not None:
        return getattr(fn, "__qualname__", getattr(fn, "__name__", repr(fn)))
    return repr(binding)


__all__ = [
    "BackendSelectionReport",
    "SeamCheck",
    "SeamSelection",
    "bind_env_overrides",
    "check_backend_selection",
    "env_truthy",
    "inspect_backend_selection",
    "selected_by",
]
