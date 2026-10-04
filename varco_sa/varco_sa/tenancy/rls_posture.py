"""
varco_sa.tenancy.rls_posture
==============================
The live RLS posture check behind ``TenancySettings.enforce_rls`` (Plan 056 /
FLIP-12): ``rls_findings()`` maps an ``RlsPosture`` onto ``data.*``
``PostureFinding``s, and ``RlsPostureCollector`` reads the catalog once at
``SAModule``'s ``sa.rls_posture`` lifecycle component and serves the result
to ``SecurityPosture``.

Ids this module can emit (each has a row in
``technical_docs/features/security-posture.md``'s severity table):

=================================== ============= =============================
check                               severity      fires when
=================================== ============= =============================
``data.rls_not_checked``            NOT_ASSESSED  no posture (not started, DB
                                                  unreachable, hand form unset)
``data.rls_not_applicable``         INFO          non-Postgres dialect, or
                                                  ``SCHEMA``/``DATABASE``
``data.rls_disabled``               HIGH          a candidate has RLS off
``data.rls_enabled_without_policy`` WARN          RLS on, no policy (deny-all)
``data.rls_not_forced``             WARN          RLS on, ``FORCE`` absent
``data.role_bypasses_rls``          HIGH          superuser / ``BYPASSRLS`` /
                                                  owns an unforced table
``data.rls_guc_setter_absent``      WARN          RLS live, nothing sets GUC
``data.rls_not_enforced``           WARN          ``enforce_rls=False`` with
                                                  candidate tables present
``data.rls_table_absent``           INFO          a candidate table does not
                                                  exist yet (fresh database)
=================================== ============= =============================

DESIGN: severity is assigned here, in ``varco_sa`` — not in
    ``varco_fastapi.posture`` (§D-flip12-mapping)
    ✅ One mapping, two entry points: the live collector below and
       ``varco_fastapi.posture.DataPostureCollector(rls_posture=...)`` (the
       hand-wired form), which now delegates to ``rls_findings()``.
    ✅ The package that owns the catalog query owns its interpretation —
       Plan 051's handoff ("a live RLS collector is a ``PostureCollector`` in
       ``varco_sa``, contributed by ``SAModule``").
    ❌ ``varco_fastapi/posture.py``'s "severity lives here, never in the
       sibling" rule gains a second exception (056's ``data.*`` ids) beside
       035's nine — recorded in that module's comment.

DESIGN: three narrowings over the 3.2 ``_collect_data`` mapping
    ✅ Reads ``TablePosture.rls_enabled``/``rls_forced`` — the 3.2 code read
       ``.enabled``/``.forced``, which do not exist (premise 2).
    ✅ Gates on ``RlsPosture.dialect`` — the degenerate all-``False``
       non-Postgres posture is INFO, never a false ``data.rls_disabled`` HIGH
       on SQLite (premise 3).
    ✅ ``data.role_bypasses_rls`` fires for an owned table only when that
       table is **not** forced — an owned, forced table is the textbook
       shape (brief 007 §1), not a bypass (premise 4).
    ❌ Two new WARN ids (``rls_enabled_without_policy``,
       ``rls_guc_setter_absent``) block a production start under Plan 054's
       derived ``refuse``. Both name a real outage; both are suppressible.

Thread safety:  ✅ ``rls_findings()`` is pure. ``RlsPostureCollector`` swaps one
                   immutable snapshot in ``refresh()``; ``collect()`` reads it.
Async safety:   ✅ ``refresh()`` is the only coroutine — awaited once, at
                   startup, by ``SAModule``'s ``sa.rls_posture`` component
                   (``RLS_POSTURE_PHASE`` = ``Phase.INFRA + 50``: after
                   migrations, before ``SecurityPostureModule`` at
                   ``Phase.EDGE``).
"""

from __future__ import annotations

import dataclasses
import logging
from collections.abc import Callable
from typing import TYPE_CHECKING

from sqlalchemy.exc import DBAPIError
from varco_core.posture import PostureCollector, PostureFinding, PostureSeverity
from varco_core.tenancy.settings import TenancySettings, TenantIsolation

from varco_sa.tenancy.rls_check import RlsPosture, inspect_rls_posture

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncEngine

__all__ = ["RlsPostureCollector", "rls_findings"]

logger = logging.getLogger(__name__)

# WHY two paths named: under create_varco_app the refresh is driven by
# SAModule's ``sa.rls_posture`` component (RLS_POSTURE_PHASE), not by
# SAModule.start() itself — naming only start() sent operators to the wrong
# place. The hand-wired path still calls start(), so both are listed.
_NOT_STARTED = (
    "the RLS posture refresh has not run (SAModule's sa.rls_posture component / SAModule.start())"
)

_ADOPT_RLS = (
    "plan_tenant_rls() -> tenant_rls_upgrade(op, plans=...) in an application-authored, "
    "reviewed revision (never varco-owned) — policy, then ENABLE, then FORCE "
    "(technical_docs/features/postgres-rls.md). Or defer: VARCO_TENANCY_ENFORCE_RLS=false "
    "plus VARCO_SECURITY_SUPPRESS=data.rls_not_enforced."
)

#: The failures ``refresh()`` downgrades to NOT_ASSESSED. Named, never bare
#: ``Exception``: an unreachable database is readiness's problem, a bug in
#: this module is not. ``DBAPIError`` covers SQLAlchemy's wrapping of every
#: driver error (``OperationalError``/``InterfaceError``/``ProgrammingError``);
#: ``OSError`` covers a raw socket failure and ``TimeoutError`` (an ``OSError``
#: subclass, and ``asyncio.TimeoutError``'s alias since 3.11).
_FETCH_ERRORS: tuple[type[BaseException], ...] = (OSError, DBAPIError, TimeoutError)


def _not_applicable(detail: str) -> PostureFinding:
    """
    Build the one INFO finding for a deployment where RLS is not asserted.

    Args:
        detail: Why (the isolation strategy, or the dialect).

    Returns:
        A ``data.rls_not_applicable`` finding.
    """
    return PostureFinding(
        check="data.rls_not_applicable",
        severity=PostureSeverity.INFO,
        detail=detail,
        remediation="No action required.",
    )


def _names(tables: list[str]) -> str:
    return ", ".join(sorted(tables))


def rls_findings(
    posture: RlsPosture | None,
    *,
    guc_setter_present: bool | None = None,
    not_checked_reason: str | None = None,
) -> list[PostureFinding]:
    """
    Map an ``RlsPosture`` onto ``data.*`` posture findings.

    Args:
        posture:            The posture read by ``inspect_rls_posture()``, or
                            ``None`` when none could be read.
        guc_setter_present: Whether something sets the RLS GUC per
                            transaction (``rls_set_tenant``, or a live
                            ``install_rls_tenant_hook``). ``None`` — unknown
                            (the hand-wired form) — never fires
                            ``data.rls_guc_setter_absent``.
        not_checked_reason: Detail for ``data.rls_not_checked`` when
                            ``posture is None``.

    Returns:
        Findings in a stable order; ``[]`` for a clean posture, or for a
        posture over zero tables (nothing to protect).

    Example::

        async with engine.connect() as conn:
            posture = await inspect_rls_posture(conn, tables=["orders"])
        for finding in rls_findings(posture, guc_setter_present=True):
            print(finding.check, finding.severity)
    """
    if posture is None:
        return [
            PostureFinding(
                check="data.rls_not_checked",
                severity=PostureSeverity.NOT_ASSESSED,
                detail=not_checked_reason or "No RlsPosture was supplied.",
                remediation="Start the app with SAModule; or await "
                "inspect_rls_posture(conn, tables=...) and pass rls_posture=.",
            )
        ]
    if posture.dialect != "postgresql":
        return [_not_applicable(f"dialect {posture.dialect!r} — RLS is a Postgres feature.")]
    if not posture.tables:
        return []

    # DESIGN (drift decision on Plan 056): a candidate with no pg_class row is
    # "not yet created", reported INFO — never data.rls_disabled.
    #   ✅ A fresh database on first deploy (migrations not run yet, or run
    #      after this read) has nothing to protect: no table, no rows. HIGH
    #      there refused every first deploy of an RLS-adopting app.
    #   ✅ INFO, not NOT_ASSESSED: the catalog WAS read and the table's
    #      absence is the assessment; NOT_ASSESSED means "could not check".
    #   ❌ A misnamed/mis-schema'd table reads as absent, not unprotected.
    #      Mitigated: the INFO names every table, and the next start (after
    #      the table exists) assesses it for real.
    missing = sorted(posture.missing_tables)
    tables = {t: p for t, p in posture.tables.items() if t not in posture.missing_tables}
    disabled = [t for t, p in tables.items() if not p.rls_enabled]
    no_policy = [t for t, p in tables.items() if p.rls_enabled and not p.has_policy]
    not_forced = [t for t, p in tables.items() if p.rls_enabled and not p.rls_forced]
    owned_unforced = [t for t in not_forced if t in posture.owned_tables]
    live = any(p.rls_enabled and p.has_policy for p in tables.values())

    findings: list[PostureFinding] = []
    if missing:
        findings.append(
            PostureFinding(
                check="data.rls_table_absent",
                severity=PostureSeverity.INFO,
                detail=f"TENANT-scoped candidate table(s) not created yet: {_names(missing)} "
                "— not assessed for RLS until they exist (assessed at the next start).",
                remediation="No action required if this is a first deploy; otherwise check the "
                "table name / schema (`rls_candidate_tables()`) and that migrations ran.",
            )
        )
    if disabled:
        findings.append(
            PostureFinding(
                check="data.rls_disabled",
                severity=PostureSeverity.HIGH,
                detail=f"TENANT-scoped table(s) with Row-Level Security disabled: "
                f"{_names(disabled)}.",
                remediation=_ADOPT_RLS,
            )
        )
    if no_policy:
        findings.append(
            PostureFinding(
                check="data.rls_enabled_without_policy",
                severity=PostureSeverity.WARN,
                detail=f"RLS is enabled with no policy on {_names(no_policy)} — default-deny: "
                "every non-bypassing query returns zero rows (brief 007 §5).",
                remediation="CREATE POLICY ... (the generator always emits it before ENABLE) "
                "in a reviewed revision.",
            )
        )
    if not_forced:
        findings.append(
            PostureFinding(
                check="data.rls_not_forced",
                severity=PostureSeverity.WARN,
                detail=f"RLS is ENABLED but not FORCEd on {_names(not_forced)} — the table "
                "owner still bypasses it.",
                remediation="ALTER TABLE <t> FORCE ROW LEVEL SECURITY in a reviewed revision.",
            )
        )
    reasons: list[str] = []
    if posture.is_superuser:
        reasons.append("is a superuser")
    if posture.rolbypassrls:
        reasons.append("has BYPASSRLS")
    if owned_unforced:
        reasons.append(f"owns unforced table(s) {_names(owned_unforced)}")
    if reasons:
        findings.append(
            PostureFinding(
                check="data.role_bypasses_rls",
                severity=PostureSeverity.HIGH,
                detail=f"Role {posture.current_role!r} {' / '.join(reasons)} — RLS is "
                "silently ineffective for this connection.",
                remediation="Run the app as a non-superuser, non-BYPASSRLS role; FORCE every "
                "owned table. Local docker Postgres: VARCO_SECURITY_ENV=development.",
            )
        )
    if guc_setter_present is False and live:
        findings.append(
            PostureFinding(
                check="data.rls_guc_setter_absent",
                severity=PostureSeverity.WARN,
                detail="RLS policies are live but nothing sets the tenant GUC per "
                "transaction — every query sees NULL and returns zero rows, no error.",
                remediation="VARCO_TENANCY_RLS_SET_TENANT=true (or install_rls_tenant_hook()). "
                "If the app sets the GUC itself: "
                "VARCO_SECURITY_SUPPRESS=data.rls_guc_setter_absent.",
            )
        )
    return findings


@dataclasses.dataclass(frozen=True)
class _Snapshot:
    """
    What ``refresh()`` concluded — exactly one of the three outcomes is set,
    or none (a gate that yields no finding: zero candidates).
    """

    posture: RlsPosture | None = None
    not_applicable: str | None = None
    not_checked: str | None = None
    not_enforced_count: int = 0
    guc_setter_present: bool | None = None


class RlsPostureCollector(PostureCollector):
    """
    The live ``data.*`` posture collector (Plan 056 / §D-flip12-collector).

    Contributed by ``SAModule.register()``, refreshed by ``SAModule``'s
    ``sa.rls_posture`` component (after every ``Phase.INFRA`` component,
    migrations included).
    ``collect()`` is synchronous (the ``PostureCollector`` contract) and so
    only reads what ``refresh()`` stored.

    Args:
        settings:       The app's ``TenancySettings`` (isolation, the
                        ``enforce_rls`` flag, ``rls_set_tenant``).
        engine_factory: Zero-arg callable returning the app's runtime
                        ``AsyncEngine`` (or ``None`` when none is bound) —
                        resolved at ``refresh()`` time, never at
                        construction, so ``SAConfig`` may be bound after the
                        module registers. ``None`` for the hand form.
        tenant_column:  Column identifying a tenant table. Default
                        ``"tenant_id"``.
        posture:        A pre-fetched ``RlsPosture`` — the hand-wiring and
                        test seam (same shape as
                        ``DataPostureCollector(rls_posture=)``). ``collect()``
                        serves it until a ``refresh()`` replaces it.

    Edge cases:
        - Never refreshed (``varco diagnose``, a hand-wired lifecycle that
          skipped ``SAModule``) → ``data.rls_not_checked`` NOT_ASSESSED.
        - A table enabled after boot is seen at the next start (RLS DDL is a
          deploy).

    Thread safety:  ⚠️ Holds one snapshot, replaced whole by ``refresh()``
                       (a single attribute assignment) — ``collect()`` never
                       sees a half-written state.
    Async safety:   ``refresh()`` must be awaited once, at startup, before
                    ``collect()``; ``collect()`` is a pure read.

    Example::

        collector = RlsPostureCollector(settings=TenancySettings(),
                                        engine_factory=lambda: engine)
        await collector.refresh()
        SecurityPostureLifecycle(collectors=[collector, ...])
    """

    name = "data"

    def __init__(
        self,
        *,
        settings: TenancySettings,
        engine_factory: Callable[[], AsyncEngine | None] | None,
        tenant_column: str = "tenant_id",
        posture: RlsPosture | None = None,
    ) -> None:
        self._settings = settings
        self._engine_factory = engine_factory
        self._tenant_column = tenant_column
        self._snapshot: _Snapshot | None = None if posture is None else _Snapshot(posture=posture)

    async def refresh(self) -> None:
        """
        Apply the §D-flip12-candidates gates, in order, and store the outcome.

        Gates: isolation is not ``SHARED`` → not applicable; no engine →
        not checked (only if candidates exist); dialect is not Postgres → not
        applicable; zero candidate tables → nothing; ``enforce_rls=False`` →
        ``data.rls_not_enforced``; catalog read fails → not checked (one
        WARNING); otherwise the read posture.

        Raises:
            Nothing for an unreachable database (``_FETCH_ERRORS``). Any other
            exception is a bug and propagates.
        """
        from varco_sa.factory import SAModelRegistry  # noqa: PLC0415
        from varco_sa.rls_autogen import rls_candidate_tables  # noqa: PLC0415
        from varco_sa.tenancy.rls_session import hook_installed  # noqa: PLC0415

        settings = self._settings
        if settings.isolation is not TenantIsolation.SHARED:
            self._snapshot = _Snapshot(
                not_applicable=f"isolation={settings.isolation.value} — RLS is additive "
                "and not asserted."
            )
            return

        if self._engine_factory is None:
            # Hand form: keep a pre-fetched posture; nothing to read with.
            if self._snapshot is None:
                self._snapshot = _Snapshot(not_checked="no engine_factory was supplied")
            return

        engine = self._engine_factory()
        candidates = rls_candidate_tables(
            SAModelRegistry.registered(), tenant_column=self._tenant_column
        )
        if engine is None:
            self._snapshot = _Snapshot(
                not_checked="no SAConfig engine is bound" if candidates else None
            )
            return

        dialect = engine.dialect.name
        if dialect != "postgresql":
            self._snapshot = _Snapshot(
                not_applicable=f"dialect {dialect!r} — RLS is a Postgres feature."
            )
            return

        if not candidates:
            self._snapshot = _Snapshot()
            return

        if not settings.enforce_rls:
            self._snapshot = _Snapshot(not_enforced_count=len(candidates))
            return

        try:
            async with engine.connect() as conn:
                posture = await inspect_rls_posture(conn, tables=candidates)
        except _FETCH_ERRORS as exc:
            reason = (
                f"the RLS catalog read failed ({type(exc).__name__}) — the database was "
                "unreachable at startup; readiness/health owns that failure"
            )
            logger.warning("RlsPostureCollector.refresh(): %s.", reason)
            self._snapshot = _Snapshot(not_checked=reason)
            return

        self._snapshot = _Snapshot(
            posture=posture,
            guc_setter_present=settings.rls_set_tenant or hook_installed(),
        )

    def collect(self) -> list[PostureFinding]:
        """
        Return the findings for the stored snapshot.

        Returns:
            ``data.*`` findings — see the module table.
        """
        snap = self._snapshot
        if snap is None:
            return rls_findings(None, not_checked_reason=_NOT_STARTED)
        if snap.not_applicable is not None:
            return [_not_applicable(snap.not_applicable)]
        if snap.not_checked is not None:
            return rls_findings(None, not_checked_reason=snap.not_checked)
        if snap.not_enforced_count:
            return [
                PostureFinding(
                    check="data.rls_not_enforced",
                    severity=PostureSeverity.WARN,
                    detail=f"enforce_rls=false — {snap.not_enforced_count} TENANT table(s) with "
                    "a tenant column are not checked for RLS.",
                    remediation="Unset VARCO_TENANCY_ENFORCE_RLS (default true); if deferring "
                    "deliberately, VARCO_SECURITY_SUPPRESS=data.rls_not_enforced.",
                )
            ]
        if snap.posture is None:
            return []
        return rls_findings(snap.posture, guc_setter_present=snap.guc_setter_present)
