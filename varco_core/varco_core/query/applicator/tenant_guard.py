"""
varco_core.query.applicator.tenant_guard
===========================================
``assert_tenant_predicate()`` — an opt-in, dev-time-only AST walk asserting
that a tenant-scoped query was built with a tenant filter (Plan 037 / S15,
§D-S15-shape).

⛔ **Wording rule, and it is a review gate (§D-S15-shape).** This is
**a development-time assertion that a tenant-scoped query was built with a
tenant filter; it is not a security control — Postgres RLS (§S12) is.**
Nothing in this module, its callers' docstrings, error messages, the
README, or the feature doc may describe this guard as *enforcing*,
*guaranteeing*, or *securing* tenant isolation.

The backlog names this row "applicator-level" (S15). Brief 007 §7 and
brief 006's Evidence Gap 2 both **advise against** exactly that shape: a
whitebox assertion over *compiled SQL* is "not a reliable static or runtime
check", fooled by implicit casts, join conditions on another table,
subqueries, and raw SQL — "no mainstream framework ships a built-in
tenant-filter assertion". Both citations are right about compiled SQL.
varco has something their survey did not consider: its own typed, frozen
AST (``varco_core.query``, ``TransformerNode``), built *before* any backend
sees it.

+--------------------------------+------------------------------+---------------------------+
| Property                       | Compiled-SQL assertion (rej.) | AST assertion (chosen)   |
+--------------------------------+------------------------------+---------------------------+
| Implicit casts/type coercion   | fooled (brief 007 §7)         | n/a — no SQL yet          |
| Joins, CTEs, subqueries        | fooled (brief 007 §7)         | n/a — the AST has none   |
| SQLAlchemy-version fragility   | fragile/version-dependent      | none — varco owns the AST|
| Backend-agnostic               | no (Mongo has no compile step)| yes — one implementation |
| Raw SQL / no ``QueryParams``   | fooled                        | fooled — the one real    |
|                                 |                               | false negative           |
+--------------------------------+------------------------------+---------------------------+

DESIGN: AST-level, opt-in, dev-time — and never described as a security
control (§D-S15-shape)
    ✅ Sidesteps every false-negative mode brief 007 §7 enumerates (casts,
       joins, CTEs, subqueries) because none of those constructs exist in
       varco's AST.
    ✅ The only shape that makes "backend-agnostic" true — there is no
       Beanie analogue of SQLAlchemy's compilation phase; a compiled-
       artifact check would have needed two unrelated implementations.
    ✅ AST nodes are ``@dataclass(frozen=True)`` — this module adds **no
       field to any node** and mutates nothing; it is a pure, read-only
       walk.
    ✅ Requiring the predicate on the top-level ``AND`` spine avoids the
       obvious false *positive*: ``tenant_id = X OR status = 'public'``
       contains a tenant predicate that constrains nothing.
    ❌ Fooled by any path that does not build a ``QueryParams`` — raw
       ``session.execute(text(...))``, a hand-written ``Select``, ``get(pk)``,
       an aggregation pipeline. **This is why it is not a security control,
       and why S12 (Postgres RLS) lands first and is the actual backstop.**
    ❌ Moves the check away from the literal wording of the backlog row
       ("applicator-level") — see ``varco_core.query.applicator.applicator``
       and each backend repository's own docstring for why the guard is
       invoked from the repository's query sites instead (§D-S15-hook: the
       applicator is not on varco's own read path).
  Rejected — compiled-SQL analysis over ``Select``/``Compiled``: brief 007
  §7 + brief 006 Evidence Gap 2; no prior art; SQLAlchemy-version-fragile;
  not portable to Mongo.

Thread safety:  ✅ Pure, stateless function — no shared state.
Async safety:   ✅ Synchronous, no I/O.
"""

from __future__ import annotations

from typing import Any

from varco_core.exception.codes import ErrorCode
from varco_core.exception.http import register_error_code
from varco_core.exception.service import ServiceException
from varco_core.query.type import AndNode, ComparisonNode, Operation, TransformerNode


class TenantFilterError(ServiceException):
    """
    Raised by ``assert_tenant_predicate()`` when a tenant-scoped query's AST
    carries no equality predicate on the tenant field along its top-level
    ``AND`` spine.

    **Not a security control** — see the module docstring's wording rule.
    A caller catching ``ServiceException`` broadly will catch this too;
    that is expected (same family as every other service-layer exception).

    Thread safety:  ✅ Immutable after construction.
    Async safety:   ✅ Safe to raise in async contexts.
    """

    message_key = "varco.error.tenant_filter_missing"

    def __init__(self, entity: str, tenant_field: str, *args: Any, **kwargs: Any) -> None:
        """
        Args:
            entity:       Name of the entity the query targets (for the
                          error message only — never used to change
                          behaviour).
            tenant_field: The tenant column/field name that was expected.
            args:         Forwarded to ``Exception.__init__``.
            kwargs:       Forwarded to ``Exception.__init__``.
        """
        self.entity = entity
        self.tenant_field = tenant_field
        super().__init__(
            f"Query on {entity!r} has no equality filter on tenant field "
            f"{tenant_field!r} along its top-level AND spine. This is a "
            "development-time assertion that a tenant-scoped query was "
            "built with a tenant filter; it is not a security control — "
            "Postgres RLS (§S12) is. Add an explicit "
            f"`.eq({tenant_field!r}, ...)` filter, or disable "
            "VARCO_TENANCY_ASSERT_TENANT_FILTER if this query is "
            "intentionally unscoped.",
            *args,
            **kwargs,
        )

    def error_params(self) -> dict[str, Any]:
        return {"entity": self.entity, "tenant_field": self.tenant_field}


register_error_code(
    TenantFilterError,
    ErrorCode(
        code="VARCO_TENANT_GUARD_001",
        http_status=500,
        default_message=(
            "A tenant-scoped query was built with no tenant filter (development-time "
            "assertion — see VARCO_TENANCY_ASSERT_TENANT_FILTER)."
        ),
        message_key="varco.error.tenant_filter_missing",
    ),
)


def _has_top_level_tenant_equality(node: TransformerNode | None, tenant_field: str) -> bool:
    """
    Walk ``node``, descending only through ``AndNode``s, looking for an
    equality ``ComparisonNode`` on ``tenant_field``.

    Never descends into ``OrNode``/``NotNode``/anything else — a tenant
    predicate reachable only through an ``OR`` does not constrain the
    result (§D-S15-shape's false-positive guard: ``tenant_id = X OR
    status = 'public'`` must NOT pass).

    Args:
        node:         The AST node (or ``None`` — no query params at all).
        tenant_field: The field name to look for.

    Returns:
        ``True`` iff an ``Operation.EQUAL`` comparison on ``tenant_field``
        is reachable from ``node`` through zero or more ``AndNode`` hops.
    """
    if node is None:
        return False
    if isinstance(node, ComparisonNode):
        return node.field == tenant_field and node.op is Operation.EQUAL
    if isinstance(node, AndNode):
        return _has_top_level_tenant_equality(
            node.left, tenant_field
        ) or _has_top_level_tenant_equality(node.right, tenant_field)
    # OrNode, NotNode, or any other node type: opaque to this walk — a
    # predicate reached only through one of these never counts.
    return False


def assert_tenant_predicate(
    node: TransformerNode | None,
    *,
    tenant_field: str,
    entity: str,
) -> None:
    """
    Assert ``node`` carries an equality filter on ``tenant_field`` along its
    top-level ``AND`` spine.

    **Not a security control** — a development-time aid only. See the
    module docstring's wording rule and its one documented false-negative
    class (a query path that never builds a ``QueryParams`` AST at all).

    Args:
        node:         The query's root AST node, or ``None`` (no filter at
                      all — always raises).
        tenant_field: The tenant column/field name expected on the AST
                      (e.g. ``"tenant_id"``, or a custom
                      ``TenantAwareService._tenant_field`` override such as
                      ``"org_id"``).
        entity:       Name of the entity the query targets — included in
                      the raised error's message only.

    Raises:
        TenantFilterError: No ``Operation.EQUAL`` comparison on
            ``tenant_field`` is reachable from ``node`` through zero or
            more ``AndNode`` hops. This includes: ``node is None``; a bare
            comparison on a different field; a tenant equality reachable
            only through an ``OrNode`` (does not constrain the result); and
            ``Operation.NOT_EQUAL``/``Operation.IN`` on ``tenant_field``
            (neither constrains the result to exactly one tenant).

    Edge cases:
        - A bare ``ComparisonNode`` (no ``AndNode`` wrapper at all) on
          ``tenant_field`` with ``Operation.EQUAL`` passes — the "top-level
          AND spine" requirement degenerates to "the node itself" when
          there is only one predicate.
        - A nested ``AndNode`` anywhere under the top-level ``AndNode``
          still counts — the walk descends through every ``AndNode`` layer,
          not just the immediate root.
    """
    if not _has_top_level_tenant_equality(node, tenant_field):
        raise TenantFilterError(entity, tenant_field)


__all__ = ["TenantFilterError", "assert_tenant_predicate"]
