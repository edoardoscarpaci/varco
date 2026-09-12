"""
varco_core.tenancy.provenance
==============================
The ``TenantProvenance`` ambient var, and the two Plan-036 seams
(``assert_tenant_matches`` / ``CrossTenantAccessError``) built on top of it
(§D-S6-provenance, §D-036-seams).

DESIGN: a second ``AmbientVar`` instead of extending ``RequestContext``/``AuthContext``
    ✅ CLAUDE.md is categorical: ``RequestContext`` never holds the tenant;
       ``current_tenant()`` (``varco_core.service.tenant``) is the single
       source of truth for *who* the tenant is. ``TenantProvenance`` answers
       a different question — *how was it decided* — and is meaningful only
       because ``current_tenant()`` already holds the answer. This is
       composition by *ordering*, never containment.
    ✅ ``AmbientVar`` (``varco_core.context.ambient``) exists for exactly
       this — using it directly is the documented path, not an exception to
       it.
    ✅ It doubles as the coordination marker §D-S6-wiring needs:
       ``current_tenant_provenance() is not None`` is the unambiguous signal
       that a chain already ran, so ``RequestContextMiddleware`` must not
       decide the tenant again. No ``request.state`` key, no Starlette
       internals assumption.
    ❌ A third ambient concept for a reader to hold (tenant, request
       context, provenance). Mitigated by one Decision-Tree line and a
       three-row table in the feature doc.
    Rejected — a ``provenance`` field on ``RequestContext``: ❌
    ``RequestContext`` is the locale/timezone carrier, and
    ``LocalizationMiddleware`` runs *inside* the tenant middleware — the
    value would not exist yet when ``RequestContext`` is built.
    Rejected — a field on ``AuthContext``: ❌ ``AuthContext`` is the
    *token's* snapshot; provenance includes a header and a hostname, which
    the token never saw.

Thread safety:  ✅ Backed by ``AmbientVar`` — task-local.
Async safety:   ✅ No I/O.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any, Final

from varco_core.context.ambient import AmbientVar
from varco_core.exception import ServiceException
from varco_core.tenancy.source import TenantProvenance

# DESIGN: varco_core.service.tenant.current_tenant is imported lazily, inside
# assert_tenant_matches(), rather than at module scope.
# ✅ varco_core.meta imports varco_core.tenancy.settings (TenantScope), which
#    (via this package's own __init__.py) would otherwise import this module
#    eagerly, and varco_core.service.tenant's own import chain reaches back
#    into varco_core.model -> varco_core.meta — a genuine import cycle.
# ✅ current_tenant() is only needed inside the function body; nothing here
#    needs it at import time.
# ❌ One extra deferred import to notice when reading the function. Accepted
#    — the alternative is restructuring varco_core.meta's own imports, which
#    is out of scope for this plan.

__all__ = [
    "current_tenant_provenance",
    "provenance_context",
    "CrossTenantAccessError",
    "assert_tenant_matches",
]

_provenance: Final[AmbientVar[TenantProvenance]] = AmbientVar("varco_tenant_provenance")


def current_tenant_provenance() -> TenantProvenance | None:
    """
    Return the active ``TenantProvenance``, or ``None`` when no chain ran.

    Returns:
        The verdict published by the active ``TenantSourceChain``, or
        ``None`` outside any ``provenance_context()``.

    Async safety: ✅ Pure read of a task-local ``ContextVar``.
    """
    return _provenance.get()


@contextmanager
def provenance_context(prov: TenantProvenance) -> Iterator[None]:
    """
    Activate ``prov`` for the duration of the block.

    Args:
        prov: The verdict to publish.

    Edge cases:
        - Nesting restores the exact enclosing value on exit, not ``None``.
        - A value set in a parent ``asyncio.Task`` is visible to a child
          task spawned inside the ``with`` block (copy-on-spawn), and never
          visible to a sibling task started before the block was entered.

    Thread safety: ✅ Task-local, via ``AmbientVar``.
    """
    with _provenance.scope(prov):
        yield


class CrossTenantAccessError(ServiceException):
    """
    Raised when a caller addressed a tenant other than the resolved one.

    Consumed by Plan 036 / S4's cross-tenant admin guard. This plan
    deliberately builds neither the guard nor its HTTP mapping — only the
    exception and the assertion helper below.

    Args:
        requested: The tenant id the caller explicitly named.
        resolved: The tenant ``current_tenant()`` actually holds (or
            ``None``). **Never** included in ``error_params()`` — it is a new
            exfiltration surface, same rule as ``ServiceAuthorizationError``
            excluding ``reason``.
    """

    message_key = "varco.error.cross_tenant_denied"

    def __init__(self, requested: str, resolved: str | None) -> None:
        self._requested = requested
        self._resolved = resolved
        super().__init__(f"Cross-tenant access denied for tenant {requested!r}.")

    def error_params(self) -> dict[str, Any]:
        """Return interpolation data — ``requested`` only, never ``resolved``."""
        return {"requested": self._requested}


def assert_tenant_matches(
    requested: str | None,
    *,
    allow_cross_tenant: bool = False,
) -> str:
    """
    Return the tenant an operation must run against, or raise.

    Consumed by Plan 036 / S9's preflight and, per-route, by S4's admin
    guard: ``tenant = assert_tenant_matches(body.get("tenant_id"),
    allow_cross_tenant=ctx.has_role(cross_tenant_role))``.

    Args:
        requested: The tenant id the caller explicitly named, or ``None`` to
            mean "whatever the ambient tenant is".
        allow_cross_tenant: When ``True``, ``requested`` is returned as-is
            even if it disagrees with ``current_tenant()`` — the caller
            asserts it already checked a role.

    Returns:
        - ``requested is None`` → ``current_tenant()``, or raises if unset.
        - ``requested == current_tenant()`` → ``requested``.
        - ``allow_cross_tenant`` → ``requested``.
        - otherwise → raises.

    Raises:
        CrossTenantAccessError: ``requested`` disagrees with the resolved
            tenant and ``allow_cross_tenant`` is ``False`` (including when
            no tenant is resolved at all and ``requested`` is ``None``).
    """
    from varco_core.service.tenant import current_tenant

    resolved = current_tenant()

    if requested is None:
        if resolved is None:
            raise CrossTenantAccessError(requested="", resolved=None)
        return resolved

    if resolved is not None and requested == resolved:
        return requested

    if allow_cross_tenant:
        return requested

    raise CrossTenantAccessError(requested=requested, resolved=resolved)
