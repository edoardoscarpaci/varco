"""
varco_fastapi.modules.base
=============================
``HttpContributor`` — the Protocol an HTTP-shaped ``VarcoModule`` satisfies
(§D-http) — plus ``MiddlewareModule``, the adapter that replaces
``create_varco_app``'s pre-049 ``extra_middleware=`` keyword.

``HttpContributor`` is deliberately **not** on the core ``VarcoModule`` ABC
(``varco_core.di.module``) — ``varco_core`` stays transport-free, so a
worker/CLI host can run a plain module list with no FastAPI import at all
(a design goal 049 allows but does not build). A module that wants to
contribute HTTP middleware/mounts simply also implements these two
methods; ``isinstance(module, HttpContributor)`` (a ``runtime_checkable``
Protocol) is how ``create_varco_app`` tells which modules to ask.

Thread safety:  ✅ ``MiddlewareModule`` holds only its constructor
                    arguments; no mutable shared state.
Async safety:   ✅ Both Protocol methods are synchronous — HTTP
                    contribution is a build-time, not a runtime, concern.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

from varco_core.di.module import VarcoModule

from varco_fastapi.middleware.slots import EdgeSlot, MiddlewareSpec

if TYPE_CHECKING:
    from providify import DIContainer

    from varco_fastapi.middleware import MiddlewareEntry


@runtime_checkable
class HttpContributor(Protocol):
    """
    A ``VarcoModule`` that also contributes to the HTTP edge — middleware
    and/or an app mount.

    Structural (``runtime_checkable``) — a module satisfies this Protocol
    simply by defining both methods; no explicit inheritance needed.
    """

    def middleware(self, container: DIContainer) -> Sequence[MiddlewareSpec]:
        """Return every ``MiddlewareSpec`` this module wants installed."""
        ...

    def configure_app(self, app: Any, container: DIContainer) -> None:
        """Mount routers/perform any other one-time ``FastAPI`` app mutation."""
        ...


class MiddlewareModule(VarcoModule):
    """
    Wraps a list of ``MiddlewareEntry`` (the pre-049 ``extra_middleware=``
    shape) as a ``VarcoModule`` — §D-adapters.

    Args:
        entries: A sequence of ``MiddlewareEntry`` — a bare middleware
            class, or ``(class_or_callable, kwargs)``. The same shape
            ``varco_fastapi.middleware.install_middleware_stack`` already
            accepts.
        slot: Where these entries land. Defaults to ``EdgeSlot.EXTRA`` —
            the exact position the pre-049 ``extra_middleware=`` keyword
            occupied (outside ``ErrorMiddleware``; §D-order-bugs). Pass
            ``EdgeSlot.IDEMPOTENCY`` to finally land *inside*
            ``ErrorMiddleware``, the position CLAUDE.md's Idempotency
            section has always wanted and ``extra_middleware=`` could
            never give.

    Edge cases:
        Every entry in *entries* is installed at the **same** *slot* — an
        app that wants two different positions constructs two
        ``MiddlewareModule`` instances... except ``ModuleRegistry`` keys on
        class, so two ``MiddlewareModule()`` instances collide. Use
        ``name=``-style differentiation is not available here (the class
        itself is the identity key) — subclass ``MiddlewareModule`` for a
        second position, or combine entries destined for one slot into one
        instance.
    """

    name = "middleware_adapter"

    def __init__(
        self,
        entries: Sequence[MiddlewareEntry],
        *,
        slot: EdgeSlot = EdgeSlot.EXTRA,
        enabled: bool | None = None,
    ) -> None:
        super().__init__(enabled=enabled)
        self._entries = list(entries)
        self._slot = slot

    def _register(self, container: DIContainer) -> None:
        pass

    def middleware(self, container: DIContainer) -> list[MiddlewareSpec]:
        """Build one ``MiddlewareSpec`` per entry, all at ``self._slot``."""
        specs: list[MiddlewareSpec] = []
        for entry in self._entries:
            if isinstance(entry, tuple):
                target, kwargs = entry[0], entry[1] if len(entry) > 1 else {}
            else:
                target, kwargs = entry, {}
            specs.append(MiddlewareSpec(slot=self._slot, target=target, kwargs=kwargs))
        return specs

    def configure_app(self, app: Any, container: DIContainer) -> None:
        pass


__all__ = ["HttpContributor", "MiddlewareModule"]
