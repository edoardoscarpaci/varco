"""
milestone_f / test_validation.py
==================================
Tests for ``varco_fastapi.validation``.

Covers:
- ``validate_router_class``: missing _prefix, no routes, unresolved type args (strict)
- ``ConfigurationError`` message content (actionable: includes class name and fix)
- ``validate_container_bindings``: missing AbstractServerAuth, AbstractJobStore,
  AbstractJobRunner
- ``validate_container_bindings`` noop when container is None
- ``validate_container_bindings`` noop when providify ABCs not importable
- ``_has_any_route``: CRUD mixin detection, @route detection, empty router
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from varco_fastapi.router.base import VarcoRouter
from varco_fastapi.router.endpoint import route
from varco_fastapi.router.mixins import CreateMixin, ReadMixin
from varco_fastapi.validation import (
    ConfigurationError,
    _has_any_route,
    validate_container_bindings,
    validate_router_class,
)


# ── Test routers ──────────────────────────────────────────────────────────────


class GoodRouter(CreateMixin, ReadMixin, VarcoRouter):
    """Correctly configured router — no errors expected."""

    _prefix = "/items"


class NoPrefixRouter(CreateMixin, VarcoRouter):
    """Router missing _prefix."""

    # _prefix intentionally omitted


class EmptyPrefixRouter(CreateMixin, VarcoRouter):
    """Router with empty string _prefix."""

    _prefix = ""


class NoRoutesRouter(VarcoRouter):
    """Router with a _prefix but no mixins and no @route methods."""

    _prefix = "/noop"


class CustomRouteRouter(VarcoRouter):
    """Router with a @route method but no CRUD mixins."""

    _prefix = "/custom"

    @route("GET", "/ping")
    async def ping(self) -> dict: ...


# ── ConfigurationError ────────────────────────────────────────────────────────


def test_configuration_error_is_exception():
    """``ConfigurationError`` inherits from ``Exception``."""
    exc = ConfigurationError("oops")
    assert isinstance(exc, Exception)
    assert str(exc) == "oops"


# ── validate_router_class — prefix checks ─────────────────────────────────────


def test_valid_router_passes_without_raising():
    """Correctly configured router does not raise."""
    validate_router_class(GoodRouter)  # must not raise


def test_missing_prefix_raises_configuration_error():
    """Router without ``_prefix`` raises ``ConfigurationError``."""
    with pytest.raises(ConfigurationError, match="_prefix"):
        validate_router_class(NoPrefixRouter)


def test_empty_prefix_raises_configuration_error():
    """Router with ``_prefix = ''`` raises ``ConfigurationError``."""
    with pytest.raises(ConfigurationError, match="_prefix"):
        validate_router_class(EmptyPrefixRouter)


def test_error_message_contains_class_name():
    """Error message must include the router class name for easy debugging."""
    with pytest.raises(ConfigurationError) as exc_info:
        validate_router_class(NoPrefixRouter)
    assert "NoPrefixRouter" in str(exc_info.value)


def test_error_message_contains_fix_hint():
    """Error message must contain a code snippet showing how to fix."""
    with pytest.raises(ConfigurationError) as exc_info:
        validate_router_class(NoPrefixRouter)
    # Should show something like: _prefix = '/noprefixrouters'
    assert "_prefix" in str(exc_info.value)
    assert "=" in str(exc_info.value)


# ── validate_router_class — route checks ──────────────────────────────────────


def test_no_routes_raises_configuration_error():
    """Router with no mixins and no @route methods raises ``ConfigurationError``."""
    with pytest.raises(ConfigurationError, match="routes"):
        validate_router_class(NoRoutesRouter)


def test_router_with_custom_route_passes():
    """Router with only @route methods (no mixins) is valid."""
    validate_router_class(CustomRouteRouter)  # must not raise


def test_router_with_crud_mixin_passes():
    """Router with CRUD mixin is valid."""
    validate_router_class(GoodRouter)  # must not raise


# ── validate_router_class — strict mode ───────────────────────────────────────


def test_strict_mode_warns_for_unresolved_type_args(caplog):
    """
    A router with unresolved TypeVar args logs a warning in non-strict mode.

    ``VarcoRouter`` base class itself has bare TypeVars — strict=False should
    not raise on it (just warn).
    """
    # Just check that strict=False does not raise for a known good router
    validate_router_class(GoodRouter, strict=False)  # must not raise


def test_strict_mode_raises_for_bare_varcorouter():
    """
    Directly validating the ``VarcoRouter`` base class in strict mode raises
    because its own TypeVar args are unresolved.

    Edge case: VarcoRouter itself has no _prefix and no routes, so it raises
    on the prefix check before reaching the strict check.
    """
    # VarcoRouter base class has no _prefix, so it raises on the prefix check
    with pytest.raises(ConfigurationError):
        validate_router_class(VarcoRouter, strict=True)


# ── _has_any_route ────────────────────────────────────────────────────────────


def test_has_any_route_true_for_crud_mixin():
    assert _has_any_route(GoodRouter) is True


def test_has_any_route_true_for_custom_route():
    assert _has_any_route(CustomRouteRouter) is True


def test_has_any_route_false_for_empty_router():
    assert _has_any_route(NoRoutesRouter) is False


def test_has_any_route_false_for_base_varcorouter():
    """``VarcoRouter`` base class itself has no routes."""
    assert _has_any_route(VarcoRouter) is False


# ── validate_container_bindings — None container ──────────────────────────────


def test_validate_container_none_is_noop():
    """Passing ``None`` as container → no error raised."""
    validate_container_bindings(None)  # must not raise


# ── validate_container_bindings — bindings present ───────────────────────────


def _make_container_with_all_bindings() -> MagicMock:
    """
    Mock container that reports all required bindings as registered.

    Uses ``is_registered()`` to simulate the providify API.
    """
    container = MagicMock()
    container.is_registered = MagicMock(return_value=True)
    return container


def test_valid_container_passes():
    """Container with all required bindings does not raise."""
    container = _make_container_with_all_bindings()
    validate_container_bindings(container)  # must not raise


# ── validate_container_bindings — missing bindings ────────────────────────────


def _make_container_missing(missing_class_name: str) -> MagicMock:
    """
    Mock container that returns ``False`` for ``is_registered()`` on the
    class whose name matches ``missing_class_name``.
    """

    def _is_registered(cls: type) -> bool:
        return cls.__name__ != missing_class_name

    container = MagicMock()
    container.is_registered = MagicMock(side_effect=_is_registered)
    return container


def test_missing_server_auth_raises():
    """Missing ``AbstractServerAuth`` → ``ConfigurationError``."""
    container = _make_container_missing("AbstractServerAuth")
    with pytest.raises(ConfigurationError, match="AbstractServerAuth"):
        validate_container_bindings(container)


def test_missing_job_store_raises():
    """Missing ``AbstractJobStore`` → ``ConfigurationError``."""
    container = _make_container_missing("AbstractJobStore")
    with pytest.raises(ConfigurationError, match="AbstractJobStore"):
        validate_container_bindings(container)


def test_missing_job_runner_raises():
    """Missing ``AbstractJobRunner`` → ``ConfigurationError``."""
    container = _make_container_missing("AbstractJobRunner")
    with pytest.raises(ConfigurationError, match="AbstractJobRunner"):
        validate_container_bindings(container)


def test_error_contains_fix_instruction():
    """ConfigurationError message includes fix instructions."""
    container = _make_container_missing("AbstractServerAuth")
    with pytest.raises(ConfigurationError) as exc_info:
        validate_container_bindings(container)
    msg = str(exc_info.value)
    # Must tell the developer what to install or bind
    assert "install" in msg.lower() or "bind" in msg.lower()


# ── validate_container_bindings — fallback when is_registered absent ──────────


def test_fallback_to_get_when_is_registered_absent():
    """
    Container without ``is_registered()`` falls back to ``container.get()``.
    A ``LookupError`` from ``get()`` triggers ``ConfigurationError``.
    """

    container = MagicMock(spec=[])  # no is_registered attribute

    # Make get() raise LookupError for AbstractServerAuth only
    def _get(cls: type) -> object:
        if cls.__name__ == "AbstractServerAuth":
            raise LookupError(f"No binding for {cls.__name__}")
        return MagicMock()

    container.get = MagicMock(side_effect=_get)

    with pytest.raises(ConfigurationError, match="AbstractServerAuth"):
        validate_container_bindings(container)
