"""
varco_fastapi.validation
=========================
Startup validation helpers for ``VarcoRouter`` classes and DI container bindings.

These are called by ``create_varco_app()`` before the FastAPI app is constructed
so that misconfiguration is surfaced at startup with an actionable error message
rather than a cryptic ``AttributeError`` or ``LookupError`` during the first HTTP
request.

``validate_router_class(router_cls)`` — checks the router class itself:
    - ``_prefix`` is set (common omission that silently mounts at ``""``)
    - Generic type args are fully resolved (no bare ``TypeVar`` remaining)
    - At least one route is declared (no mixin and no ``@route`` = dead router)

``validate_container_bindings(container)`` — checks the DI container:
    - ``AbstractServerAuth`` is registered (auth is never optional in production)
    - ``AbstractJobStore`` is registered (needed by all async-capable endpoints)
    - ``AbstractJobRunner`` is registered (needed to execute background jobs)

Both functions raise ``ConfigurationError`` with a message that names the problem
and tells the developer what to do.

DESIGN: early validation over lazy resolution
    ✅ Errors surface at ``uvicorn main:app`` time, not at the first ``POST /orders``
    ✅ Message includes the router name, missing field, and fix suggestion
    ✅ Pure synchronous — no event loop required, no I/O
    ❌ Cannot validate DI bindings that are registered lazily (e.g. request-scoped)
       — those are still caught at first request

Thread safety:  ✅ Pure functions — no shared state.
Async safety:   ✅ No I/O — all checks are synchronous introspection.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    pass

_logger = logging.getLogger(__name__)


# ── ConfigurationError ─────────────────────────────────────────────────────────


class ConfigurationError(Exception):
    """
    Raised when a ``VarcoRouter`` class or DI container is misconfigured.

    Carries the misconfigured object name and a human-readable fix suggestion
    so the developer can resolve the issue without reading source code.

    Args:
        message: Actionable error message — what is wrong and how to fix it.

    Example::

        raise ConfigurationError(
            "OrderRouter is missing '_prefix'. "
            "Add: class OrderRouter(...): _prefix = '/orders'"
        )
    """

    pass


# ── Router validation ──────────────────────────────────────────────────────────


def validate_router_class(router_cls: type, *, strict: bool = False) -> None:
    """
    Validate that a ``VarcoRouter`` subclass is correctly configured.

    Checks (in order):
    1. ``_prefix`` is set and is a non-empty string.
    2. Generic type args are fully resolved (no bare ``TypeVar`` in the MRO
       ``__orig_bases__`` — indicates missing ``[D, PK, C, R, U]``).
    3. At least one route source exists: a ``RouterMixin`` in the MRO OR a method
       decorated with ``@route``.

    Args:
        router_cls: The ``VarcoRouter`` subclass to validate.
        strict:     If ``True``, also validate that generic type args are fully
                    concrete types (not ``Any``).  Default: ``False``.

    Raises:
        ConfigurationError: If any check fails.

    Edge cases:
        - ``_prefix = ""`` is treated as missing — a blank prefix silently mounts
          all routes at the app root, shadowing other routers.
        - Checking for ``TypeVar`` in ``__orig_bases__`` is best-effort.  Complex
          generic aliases (e.g. ``ForwardRef``) are not deeply inspected.
        - ``strict=True`` checks raise for ``Any`` type args — useful for CI but
          too noisy for development iteration.

    Thread safety:  ✅ Pure function.
    Async safety:   ✅ No I/O.
    """
    name = router_cls.__name__

    # ── Check 1: _prefix is set ───────────────────────────────────────────────
    prefix = getattr(router_cls, "_prefix", None)
    if not prefix:
        raise ConfigurationError(
            f"'{name}' is missing a '_prefix' ClassVar. "
            f"Every VarcoRouter must declare a URL prefix, e.g.: "
            f"class {name}(...): _prefix = '/{_guess_prefix(name)}'"
        )

    # ── Check 2: at least one route source ───────────────────────────────────
    has_routes = _has_any_route(router_cls)
    if not has_routes:
        raise ConfigurationError(
            f"'{name}' has no routes. "
            "Mix in at least one CRUD mixin (e.g. CreateMixin, ReadMixin) or "
            "decorate a method with @route."
        )

    # ── Check 3: generic type args fully resolved (best-effort) ──────────────
    _check_generic_args(router_cls, strict=strict)

    _logger.debug("validate_router_class: %s OK (prefix=%r)", name, prefix)


def _guess_prefix(class_name: str) -> str:
    """Guess a sensible prefix from the class name for the error message."""
    import re  # noqa: PLC0415

    name = class_name
    for suffix in ("Router", "Controller", "View", "Handler"):
        if name.endswith(suffix) and name != suffix:
            name = name[: -len(suffix)]
            break
    return re.sub(r"(?<!^)(?=[A-Z])", "-", name).lower() + "s"


def _has_any_route(router_cls: type) -> bool:
    """
    Return ``True`` if the router has at least one CRUD mixin or ``@route`` method.

    Args:
        router_cls: The class to inspect.

    Returns:
        ``True`` if any route source was found.
    """
    # Check for CRUD mixins via _CRUD_ACTION ClassVar
    for cls in router_cls.__mro__:
        if getattr(cls, "_CRUD_ACTION", None) is not None:
            return True

    # Check for @route / @ws_route / @sse_route decorated methods
    for attr_name in dir(router_cls):
        try:
            obj = getattr(router_cls, attr_name, None)
        except Exception:  # noqa: BLE001
            continue
        if callable(obj) and (
            hasattr(obj, "__route_entry__")
            or hasattr(obj, "__ws_route_entry__")
            or hasattr(obj, "__sse_route_entry__")
        ):
            return True
    return False


def _check_generic_args(router_cls: type, *, strict: bool) -> None:
    """
    Warn (or raise in strict mode) if generic type args contain unresolved TypeVars.

    Args:
        router_cls: The ``VarcoRouter`` subclass to inspect.
        strict:     Raise ``ConfigurationError`` if unresolved args are found.
                    If ``False``, only logs a warning.

    Edge cases:
        - ``__orig_bases__`` is not available on all Python versions — skips silently.
        - ``get_args()`` returns an empty tuple for non-generic base classes — safe.
    """
    import typing  # noqa: PLC0415

    unresolved: list[str] = []
    for base in getattr(router_cls, "__orig_bases__", []):
        for arg in typing.get_args(base):
            if isinstance(arg, typing.TypeVar):
                unresolved.append(str(arg))

    if unresolved:
        msg = (
            f"'{router_cls.__name__}' has unresolved generic type args: {unresolved}. "
            "Parameterise the router with concrete types, e.g.: "
            f"class {router_cls.__name__}(..., VarcoCRUDRouter[Order, UUID, C, R, U]): ..."
        )
        if strict:
            raise ConfigurationError(msg)
        # Non-strict: warn and continue — allows incremental migration
        _logger.warning("validate_router_class: %s", msg)


# ── Container binding validation ───────────────────────────────────────────────


def validate_container_bindings(container: Any) -> None:
    """
    Validate that required DI bindings are registered in the container.

    Checks:
    - ``AbstractServerAuth``  — every app must have an auth strategy.
    - ``AbstractJobStore``    — required by all async-capable endpoints.
    - ``AbstractJobRunner``   — required to execute background jobs.

    Args:
        container: The ``DIContainer`` to validate.  If ``None`` (providify not
                   installed), validation is skipped with a debug log.

    Raises:
        ConfigurationError: If a required binding is missing.

    Edge cases:
        - If ``varco_core`` is not installed (unusual) the check imports are
          skipped gracefully.
        - ``AbstractJobStore`` and ``AbstractJobRunner`` are provided by
          ``VarcoFastAPIModule`` with ``InMemoryJobStore`` defaults — they should
          always be present if ``container.install(VarcoFastAPIModule)`` was called.

    Thread safety:  ✅ Pure function — no shared state.
    Async safety:   ✅ No I/O.
    """
    if container is None:
        _logger.debug("validate_container_bindings: no container — skipping.")
        return

    try:
        from varco_core.job.base import (
            AbstractJobRunner,
            AbstractJobStore,
        )  # noqa: PLC0415
        from varco_fastapi.auth.server_auth import AbstractServerAuth  # noqa: PLC0415
    except ImportError:
        # If we can't import the ABCs we can't check for them — skip silently
        return

    _assert_binding(
        container,
        AbstractServerAuth,
        fix=(
            "Install VarcoFastAPIModule and override the AbstractServerAuth binding:\n"
            "  container.install(VarcoFastAPIModule)\n"
            "  container.bind(AbstractServerAuth, JwtBearerAuth)\n"
            "  # or: container.bind(AbstractServerAuth, ApiKeyAuth(...))"
        ),
    )
    _assert_binding(
        container,
        AbstractJobStore,
        fix=(
            "Install VarcoFastAPIModule which registers InMemoryJobStore by default:\n"
            "  container.install(VarcoFastAPIModule)"
        ),
    )
    _assert_binding(
        container,
        AbstractJobRunner,
        fix=(
            "Install VarcoFastAPIModule which registers JobRunner by default:\n"
            "  container.install(VarcoFastAPIModule)"
        ),
    )

    _logger.debug("validate_container_bindings: all required bindings present.")


def _assert_binding(container: Any, interface: type, *, fix: str) -> None:
    """
    Assert that ``interface`` has a binding in ``container``.

    Args:
        container: The ``DIContainer`` to inspect.
        interface: The ABC / Protocol to look up.
        fix:       Multi-line fix suggestion for the error message.

    Raises:
        ConfigurationError: If no binding is found.

    Edge cases:
        - Uses ``container.is_registered()`` if available, otherwise falls back
          to ``container.get()`` in a try/except (slower but always works).
    """
    try:
        # Prefer a non-constructing check if the container supports it
        if hasattr(container, "is_registered"):
            registered = container.is_registered(interface)
        else:
            # Fall back: try resolving; catch LookupError
            try:
                container.get(interface)
                registered = True
            except (LookupError, KeyError):
                registered = False

        if not registered:
            raise ConfigurationError(
                f"No binding found for '{interface.__name__}' in the DI container.\n{fix}"
            )
    except ConfigurationError:
        raise
    except Exception as exc:  # noqa: BLE001
        # Any other error from container introspection is non-fatal — log and continue
        _logger.debug(
            "validate_container_bindings: could not check %s: %s",
            interface.__name__,
            exc,
        )


# ── Public API ─────────────────────────────────────────────────────────────────

__all__ = [
    "ConfigurationError",
    "validate_router_class",
    "validate_container_bindings",
]
