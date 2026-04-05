"""
varco_fastapi.client.configurator
===================================
Per-service-type URL / proxy / header configuration layer.

``ClientConfigurator[R]`` resolves the base URL, optional proxy, and default
headers for a specific remote service identified by router type ``R``.
Configuration flows from the most-specific to the least-specific source:

    explicit constructor kwarg > environment variable > default_url() override

Environment variable naming convention (auto-derived from ``router._prefix``)::

    VARCO_CLIENT_{SERVICE_NAME}_URL       — base URL
    VARCO_CLIENT_{SERVICE_NAME}_PROXY     — HTTP/SOCKS proxy URL
    VARCO_CLIENT_{SERVICE_NAME}_TIMEOUT   — timeout override in seconds
    VARCO_CLIENT_{SERVICE_NAME}_HEADERS   — JSON dict of extra request headers

``SERVICE_NAME`` derivation::

    Router._prefix = "/orders"         → SERVICE_NAME = "ORDERS"
    Router._prefix = "/user-profiles"  → SERVICE_NAME = "USER_PROFILES"
    Class name "OrderRouter"           → "ORDER" (fallback, strips "Router", camelCase→SNAKE)

DESIGN: class with overrideable methods over frozen dataclass
    ✅ ``default_url()`` is overrideable per service — frozen dataclass cannot do this
    ✅ ``get_url()`` layered resolution: explicit > env var > default_url()
    ✅ Composition: contains ``ClientProfile`` — orthogonal concerns stay separate
    ✅ DI-injectable — ``Inject[ClientConfigurator[OrderRouter]]`` works
    ❌ Not frozen — mitigated by treating configurators as singletons in DI

DESIGN: composition (contains ClientProfile) over inheritance
    ✅ ``ClientProfile`` = middleware / TLS / timeout.  Configurator = URL / proxy / env routing.
    ✅ Same profile can be shared across multiple configurators (multi-service)
    ✅ Configurator can be used without a profile (falls back to defaults)
    ❌ One extra concept — justified because URL management and middleware config
       change at different rates and for different reasons

Thread safety:  ✅ All resolution is read-only after construction.
Async safety:   ✅ No I/O — env var reads are synchronous.
"""

from __future__ import annotations

import json
import os
import re
import typing
from typing import TYPE_CHECKING, Any, ClassVar, Generic, TypeVar

if TYPE_CHECKING:
    from varco_fastapi.client.base import ClientProfile
    from varco_fastapi.router.base import VarcoRouter

R = TypeVar("R")


class ClientConfigurator(Generic[R]):
    """
    Per-service URL / proxy / header configurator.

    Subclass with ``ClientConfigurator[SomeRouter]`` and optionally override
    ``default_url()``, ``default_headers()``, and ``default_profile()`` to
    give a service its own defaults.

    Args:
        profile:       Explicit ``ClientProfile`` (overrides ``default_profile()``).
        base_url:      Explicit base URL (overrides env var and ``default_url()``).
        proxy_url:     Explicit proxy URL (overrides env var).
        extra_headers: Extra headers merged on top of ``default_headers()``.

    Edge cases:
        - No URL configured and ``default_url()`` raises → ``get_url()`` raises.
        - ``VARCO_CLIENT_{SERVICE}_HEADERS`` must be valid JSON dict → logged
          and ignored if malformed.
        - ``_service_name`` ClassVar overrides auto-derivation entirely.

    Thread safety:  ✅ Read-only after construction.
    Async safety:   ✅ No I/O.
    """

    # Override in subclass to bypass auto-derivation from router._prefix / class name
    _service_name: ClassVar[str | None] = None

    def __init__(
        self,
        *,
        profile: ClientProfile | None = None,
        base_url: str | None = None,
        proxy_url: str | None = None,
        extra_headers: dict[str, str] | None = None,
    ) -> None:
        # Store explicit overrides — resolution methods use these as highest priority
        self._explicit_profile = profile
        self._explicit_url = base_url
        self._explicit_proxy = proxy_url
        self._explicit_headers: dict[str, str] = extra_headers or {}

    # ── Overrideable defaults ─────────────────────────────────────────────────

    def default_url(self) -> str:
        """
        Fallback URL when neither constructor kwarg nor env var is set.

        Override in subclasses to provide a hardcoded default::

            class OrderConfigurator(ClientConfigurator[OrderRouter]):
                def default_url(self) -> str:
                    return "https://orders.internal"

        Raises:
            NotImplementedError: If not overridden and no env var is set.

        Edge cases:
            - Raises intentionally — forces explicit URL configuration.
        """
        svc = self.service_name()
        raise NotImplementedError(
            f"{type(self).__name__}.default_url() must be overridden "
            f"or VARCO_CLIENT_{svc}_URL must be set in the environment."
        )

    def default_profile(self) -> ClientProfile:
        """
        Default ``ClientProfile`` for this service.

        Override to provide custom middleware, TLS, or timeout::

            def default_profile(self) -> ClientProfile:
                return ClientProfile.production(authority=my_authority)

        Returns:
            ``ClientProfile.from_env()`` — reads standard env vars.
        """
        # Import here to avoid circular import at module level
        from varco_fastapi.client.base import ClientProfile  # noqa: PLC0415

        return ClientProfile.from_env()

    def default_headers(self) -> dict[str, str]:
        """
        Default headers added to every request to this service.

        Override to provide service-specific headers::

            def default_headers(self) -> dict[str, str]:
                return {"X-Service": "my-app", "X-Version": "1"}

        Returns:
            Empty dict — no default headers unless overridden.
        """
        return {}

    # ── Resolution methods ────────────────────────────────────────────────────

    def service_name(self) -> str:
        """
        Derive the ``SERVICE_NAME`` used in env var lookups.

        Resolution order:
        1. ``_service_name`` ClassVar (if set)
        2. Router ``_prefix`` (``"/orders"`` → ``"ORDERS"``)
        3. Class name fallback (``"OrderRouter"`` → ``"ORDER"``)

        Returns:
            Uppercase ``SERVICE_NAME`` string (e.g. ``"ORDERS"``).
        """
        if self._service_name is not None:
            return self._service_name
        # Try to resolve router type from __orig_bases__
        try:
            router_cls = self._resolve_router_type()
            return self._derive_service_name(router_cls)
        except TypeError:
            # No router type resolved — fall back to class name
            name = type(self).__name__
            if name.endswith("Configurator"):
                name = name[:-12]
            return re.sub(r"(?<=[a-z])(?=[A-Z])", "_", name).upper()

    def get_url(self) -> str:
        """
        Resolve base URL: explicit kwarg > env var > ``default_url()``.

        Returns:
            The base URL string for this service.

        Raises:
            NotImplementedError: If no URL source is configured.
        """
        if self._explicit_url:
            return self._explicit_url
        env_key = f"VARCO_CLIENT_{self.service_name()}_URL"
        env_val = os.environ.get(env_key)
        if env_val:
            return env_val
        return self.default_url()

    def get_proxy(self) -> str | None:
        """
        Resolve proxy URL: explicit kwarg > env var > ``None``.

        Returns:
            Proxy URL string or ``None`` if not configured.
        """
        if self._explicit_proxy:
            return self._explicit_proxy
        env_key = f"VARCO_CLIENT_{self.service_name()}_PROXY"
        return os.environ.get(env_key)

    def get_timeout(self) -> float | None:
        """
        Resolve timeout override from env var.

        The profile's timeout is used if this returns ``None``.

        Returns:
            Float seconds or ``None`` (fall back to profile timeout).
        """
        env_key = f"VARCO_CLIENT_{self.service_name()}_TIMEOUT"
        env_val = os.environ.get(env_key)
        return float(env_val) if env_val else None

    def get_headers(self) -> dict[str, str]:
        """
        Merge default headers + env headers + explicit headers.

        Precedence: explicit_headers > env > default_headers.

        Returns:
            Merged headers dict for this service.

        Edge cases:
            - ``VARCO_CLIENT_{SERVICE}_HEADERS`` env var must be a JSON object.
              If malformed, it is silently ignored (safe — headers are additive).
        """
        headers: dict[str, str] = {}
        headers.update(self.default_headers())

        env_key = f"VARCO_CLIENT_{self.service_name()}_HEADERS"
        env_val = os.environ.get(env_key)
        if env_val:
            try:
                parsed = json.loads(env_val)
                if isinstance(parsed, dict):
                    headers.update(parsed)
            except (json.JSONDecodeError, ValueError):
                # Silently ignore — headers are additive; malformed env should not crash
                pass

        headers.update(self._explicit_headers)
        return headers

    def profile(self) -> ClientProfile:
        """
        Resolve the final ``ClientProfile`` for this service.

        Applies service-specific headers and timeout override on top of the
        base profile.

        Returns:
            Final ``ClientProfile`` with service headers and timeout applied.
        """
        from varco_fastapi.client.middleware import HeadersMiddleware  # noqa: PLC0415

        base = self._explicit_profile or self.default_profile()

        # Append service-specific headers middleware if any headers are defined
        headers = self.get_headers()
        if headers:
            base = base.with_middleware(HeadersMiddleware(headers))

        # Override timeout from env if set
        timeout = self.get_timeout()
        if timeout is not None:
            base = base.with_timeout(timeout)

        return base

    def config(self) -> dict[str, Any]:
        """
        Full resolved configuration dict (for debugging / logging).

        Returns:
            Dict with all resolved configuration values.
        """
        return {
            "service_name": self.service_name(),
            "url": self.get_url(),
            "proxy": self.get_proxy(),
            "headers": self.get_headers(),
            "timeout": self.get_timeout(),
        }

    def __repr__(self) -> str:
        """Return a concise string representation for debugging."""
        svc = self.service_name()
        try:
            url = self.get_url()
        except NotImplementedError:
            url = "<not configured>"
        return f"{type(self).__name__}(service={svc!r}, url={url!r})"

    # ── Private helpers ───────────────────────────────────────────────────────

    def _resolve_router_type(self) -> type[VarcoRouter]:
        """
        Walk ``__orig_bases__`` to extract the router type ``R``.

        Same ``__orig_bases__`` pattern as ``AsyncService._entity_type()``.

        Returns:
            The ``VarcoRouter`` subclass this configurator targets.

        Raises:
            TypeError: If not parameterized with a ``VarcoRouter`` subclass.

        Edge cases:
            - Non-generic subclasses (no ``[R]`` parameter) → raises TypeError.
            - Multiple generic bases → uses first match.
        """
        from varco_fastapi.router.base import VarcoRouter  # noqa: PLC0415

        for base in getattr(type(self), "__orig_bases__", ()):
            origin = typing.get_origin(base)
            if (
                origin is not None
                and isinstance(origin, type)
                and issubclass(origin, ClientConfigurator)
            ):
                args = typing.get_args(base)
                if (
                    args
                    and isinstance(args[0], type)
                    and issubclass(args[0], VarcoRouter)
                ):
                    return args[0]
        raise TypeError(
            f"{type(self).__name__} must be parameterized with a VarcoRouter subclass "
            f"(e.g. ClientConfigurator[OrderRouter]). "
            f"Found __orig_bases__: {getattr(type(self), '__orig_bases__', ())}."
        )

    @staticmethod
    def _derive_service_name(router_cls: type) -> str:
        """
        Derive ``SERVICE_NAME`` from ``router._prefix`` or class name.

        Examples::

            "/orders"        → "ORDERS"
            "/user-profiles" → "USER_PROFILES"
            "OrderRouter"    → "ORDER" (fallback when prefix is empty)

        Args:
            router_cls: The router class to derive a name from.

        Returns:
            Uppercase SERVICE_NAME string.
        """
        prefix: str = getattr(router_cls, "_prefix", "") or ""
        if prefix:
            # Strip leading slash, uppercase, replace hyphens with underscores
            return prefix.strip("/").upper().replace("-", "_")

        # Fall back to class name: strip "Router" suffix, convert camelCase → SNAKE_CASE
        name = router_cls.__name__
        if name.endswith("Router"):
            name = name[:-6]
        # Insert underscore before each uppercase letter preceded by a lowercase letter
        return re.sub(r"(?<=[a-z])(?=[A-Z])", "_", name).upper()


__all__ = ["ClientConfigurator"]
