"""
varco_fastapi.posture
========================
``SecurityPosture`` — the Plan 036 (S9) startup preflight, aggregating the
four sibling introspection seams (033's `inspect_tenant_provenance()`, 034's
`inspect_auth_posture()`/`inspect_revocation_posture()`, 035's
`inspect_http_edge()`, 037's `inspect_rls_posture()`) plus one local
collector for facts only this plan can see, into one report an operator
reads at startup (§D-S9-shape).

**Never a silent skip.** A collector whose sibling module is not installed,
or that raises for any other reason, reports exactly one ``NOT_ASSESSED``
finding rather than vanishing from the report — §D-S9-degrade.

**Never fails startup by default.** `SecurityPostureSettings.enforce`
defaults to ``"warn"`` — the locked 3.2 blast-radius rule (`BACKLOG.md:54`).
An opt-in ``"refuse"`` mode exists (§D-S9-enforce) and never treats
``NOT_ASSESSED`` as evidence of a problem.

Thread safety:  ✅ Frozen dataclasses throughout; `SecurityPostureLifecycle`
                   mutates only its own `._report` attribute, once, at
                   `start()` — no shared mutable state across requests.
Async safety:   ✅ `start()`/`stop()` are the only async entry points; every
                   collector is a synchronous, pure read over
                   already-constructed objects (no I/O).
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Sequence
from dataclasses import dataclass, replace
from typing import Any

from pydantic import AliasChoices, Field
from pydantic_settings import SettingsConfigDict
from varco_core.config import VarcoSettings

# Plan 051 / DI-5, §D-posture: PostureFinding/PostureSeverity/PostureCollector
# moved to varco_core.posture (a plugin outside varco_fastapi can now
# subclass PostureCollector with no varco_fastapi import). Re-exported here,
# identity-preserving, so every pre-051 `from varco_fastapi.posture import
# PostureFinding` import site keeps working.
from varco_core.posture import PostureCollector, PostureFinding, PostureSeverity

_logger = logging.getLogger(__name__)

__all__ = [
    "PostureSeverity",
    "PostureFinding",
    "PostureCollector",
    "SecurityPosture",
    "SecurityPostureSettings",
    "SecurityPostureLifecycle",
    "HttpEdgeCollector",
    "LocalPostureCollector",
    "AuthPostureCollector",
    "TenantPostureCollector",
    "DataPostureCollector",
]


@dataclass(frozen=True)
class SecurityPosture:
    """
    The full, point-in-time report (§D-S9-shape).

    Args:
        environment: ``"production"`` | ``"development"`` — presentation
            only (§D-S9-oq1); never governs whether a check runs.
        findings: Every finding produced by every collector, post
            environment-demotion and suppression-demotion.
        not_assessed: The `check` ids of every ``NOT_ASSESSED`` finding —
            the summary line's "not assessed" count, kept separate so it is
            never confused with "clean".
    """

    environment: str
    findings: tuple[PostureFinding, ...]
    not_assessed: tuple[str, ...]

    def counts(self) -> dict[PostureSeverity, int]:
        """Return the number of findings at each severity."""
        counts: dict[PostureSeverity, int] = dict.fromkeys(PostureSeverity, 0)
        for finding in self.findings:
            counts[finding.severity] += 1
        return counts

    def worst(self) -> PostureSeverity | None:
        """
        Return the single worst severity present, or ``None`` when
        `findings` is empty.

        Ordering: ``HIGH`` > ``WARN`` > ``NOT_ASSESSED`` > ``INFO`` —
        ``NOT_ASSESSED`` outranks ``INFO`` because "could not check" is a
        stronger signal than "checked and fine", even though it is never
        treated as evidence of a problem by `SecurityPostureLifecycle`'s
        `enforce="refuse"` mode.
        """
        if not self.findings:
            return None
        rank = {
            PostureSeverity.HIGH: 3,
            PostureSeverity.WARN: 2,
            PostureSeverity.NOT_ASSESSED: 1,
            PostureSeverity.INFO: 0,
        }
        return max((f.severity for f in self.findings), key=lambda s: rank[s])

    def summary(self) -> str:
        """
        Render the one-line summary always separating ``not assessed`` from
        the rest, and always naming the suppressed count (§D-S9-degrade,
        §D-S9-suppress) — even when it is zero, so the line's shape never
        changes across a report with vs. without suppressions.
        """
        counts = self.counts()
        suppressed = sum(1 for f in self.findings if f.suppressed)
        return (
            f"{counts[PostureSeverity.INFO]} info, "
            f"{counts[PostureSeverity.WARN]} warn, "
            f"{counts[PostureSeverity.HIGH]} high, "
            f"{counts[PostureSeverity.NOT_ASSESSED]} not assessed "
            f"({suppressed} suppressed)"
        )


class SecurityPostureSettings(VarcoSettings):
    """
    ``VARCO_SECURITY_*`` env vars governing the preflight's presentation and
    consequence (§D-S9-oq1, §D-S9-enforce, §D-S9-suppress).

    Attributes:
        environment: ``VARCO_SECURITY_ENV``, default ``"production"``.
            Presentation only — see the module docstring and §D-S9-oq1.
            Defaulting to strict is deliberate: an explicit flag that
            defaults to lenient is one nobody sets (the backlog's own
            framing).
        enforce: ``VARCO_SECURITY_ENFORCE``, default ``"warn"`` — the
            locked 3.2 blast-radius rule. ``"refuse"`` raises at `start()`
            when any non-suppressed finding is ``HIGH`` (or, in
            ``production``, ``WARN``) — never on ``NOT_ASSESSED`` alone.
        suppress: ``VARCO_SECURITY_SUPPRESS``, default ``""`` — a
            comma-separated list of `check` ids. A suppressed finding is
            still produced, demoted to ``INFO``, and flagged
            `PostureFinding.suppressed=True`.
        enabled: ``VARCO_SECURITY_POSTURE_ENABLED``, default ``True``
            (Plan 049 / FLIP-1, ``BACKLOG.md:81``). Read by
            ``SecurityPostureModule._enabled_from_settings()`` —
            ``create_varco_app()`` installs this module by default; this
            is the one-env-var escape hatch to turn the whole preflight
            off without touching code.
    """

    model_config = SettingsConfigDict(
        env_prefix="VARCO_SECURITY_",
        frozen=True,
        populate_by_name=True,
    )

    # Field name would otherwise map to VARCO_SECURITY_ENVIRONMENT by the
    # default env_prefix + FIELD_NAME.upper() rule — the plan's chosen env
    # var name is the shorter VARCO_SECURITY_ENV, so it needs an explicit
    # validation_alias naming the full var (bypasses the prefix rule). Same
    # `populate_by_name=True` + explicit alias shape as
    # `varco_core.jwt.config.JwtVerificationSettings.enforce_issuer`.
    environment: str = Field(
        default="production",
        validation_alias=AliasChoices("VARCO_SECURITY_ENV"),
    )
    enforce: str = "warn"
    suppress: str = ""
    # Plan 049 / FLIP-1: same alias shape as `environment` above — the
    # bare env_prefix + FIELD_NAME.upper() rule would map this to
    # VARCO_SECURITY_ENABLED, but BACKLOG.md:81/036 §D-S9-flip #1 name
    # VARCO_SECURITY_POSTURE_ENABLED explicitly.
    enabled: bool = Field(
        default=True,
        validation_alias=AliasChoices("VARCO_SECURITY_POSTURE_ENABLED"),
    )

    def suppressed_ids(self) -> frozenset[str]:
        """Parse `suppress` into a set of `check` ids, ignoring blanks."""
        return frozenset(s.strip() for s in self.suppress.split(",") if s.strip())


def _wrap_collector(
    collector: Callable[[], list[PostureFinding]], *, name: str
) -> Callable[[], list[PostureFinding]]:
    """
    Wrap a collector so an absent sibling module or any other exception
    degrades to exactly one ``NOT_ASSESSED`` finding rather than taking the
    whole preflight down (§D-S9-degrade).

    Args:
        collector: A zero-argument callable returning a list of findings.
        name: The collector's name, used only in the ``NOT_ASSESSED``
            finding's `check` id and `detail` — never in a way that reveals
            more than the collector's own identity.

    Returns:
        A zero-argument callable with the same return shape, guaranteed
        never to raise.

    DESIGN: exception type name only, never `str(exc)`
        ✅ The same rule as `error_params()` (CLAUDE.md) and 035's S3 fix —
           an exception message can carry a filesystem path, a DSN, or a
           stack detail an operator reading the *report* should never see.
           The full exception is still available server-side via normal
           logging, at `debug`, from the `except` block below.
        ❌ A generic message is less actionable than the real one would be.
           Accepted — the report is a security artifact, not a debug log.
    """

    def _wrapped() -> list[PostureFinding]:
        try:
            return collector()
        except (ImportError, ModuleNotFoundError) as exc:
            _logger.debug("posture collector %r: sibling module absent", name, exc_info=exc)
            return [
                PostureFinding(
                    check=f"posture.not_assessed.{name}",
                    severity=PostureSeverity.NOT_ASSESSED,
                    detail=(
                        f"could not import the sibling module this collector depends on "
                        f"({type(exc).__name__}) — the corresponding package may not be "
                        f"installed."
                    ),
                    remediation="install the sibling package this collector depends on",
                )
            ]
        except Exception as exc:  # noqa: BLE001 — deliberate: a collector must never take startup down
            _logger.debug("posture collector %r raised", name, exc_info=exc)
            return [
                PostureFinding(
                    check=f"posture.not_assessed.{name}",
                    severity=PostureSeverity.NOT_ASSESSED,
                    detail=f"collector raised {type(exc).__name__} — see server logs at debug.",
                    remediation="see server logs (debug level) for the underlying exception",
                )
            ]

    return _wrapped


class SecurityPostureLifecycle:
    """
    An `AbstractLifecycle`-shaped component (§D-S9-shape) — `start()` runs
    every collector, applies environment/suppression demotion, logs the
    report, stores it as `.report`, and (in `enforce="refuse"`) raises if
    warranted. Registered exactly like `TenancyLifecycle`/
    `ReliabilityLifecycle`: wrapped by `SecurityPostureModule` (in
    `default_modules()` by default, Plan 049/FLIP-1) and passed to
    `create_varco_app(container, modules=[...])`, or constructed and
    `await`ed directly (e.g. in a test).

    Args:
        collectors: A mix of `varco_core.posture.PostureCollector` instances
            and/or zero-argument callables, each returning a list of
            `PostureFinding` (Plan 051 / DI-5, §D-posture — both shapes are
            accepted so pre-051 hand-wired callables keep working). Every
            collector is wrapped via `_wrap_collector` before it runs — a
            raising collector never takes the others down with it. Two
            collectors sharing one `name` produce a
            `posture.duplicate_collector` `WARN` finding rather than a crash
            — both still run.
        settings: `SecurityPostureSettings`. Defaults to
            `SecurityPostureSettings()` (reads `VARCO_SECURITY_*`).

    Edge cases:
        - An unknown id in `settings.suppress` suppresses nothing and is
          itself reported as `posture.unknown_suppression` at ``INFO``
          (§D-S9-suppress).
        - `enforce="refuse"` never raises on `NOT_ASSESSED` alone — "could
          not check" is not evidence of a problem (§D-S9-enforce).
    """

    def __init__(
        self,
        *,
        collectors: Sequence[PostureCollector | Callable[[], list[PostureFinding]]],
        settings: SecurityPostureSettings | None = None,
    ) -> None:
        self._collectors = list(collectors)
        self._settings = settings if settings is not None else SecurityPostureSettings()
        self._report: SecurityPosture | None = None

    @property
    def report(self) -> SecurityPosture:
        """
        The report produced by the most recent `start()`.

        Raises:
            RuntimeError: `start()` has not run yet.
        """
        if self._report is None:
            raise RuntimeError("SecurityPostureLifecycle.start() has not run yet.")
        return self._report

    async def start(self) -> None:
        """
        Run every collector, build the report, log it, and (in
        ``enforce="refuse"``) raise if warranted.

        Raises:
            RuntimeError: `settings.enforce == "refuse"` and at least one
                non-suppressed finding is ``HIGH`` (or, in `production`,
                ``WARN``). Never raised for ``NOT_ASSESSED`` alone.
        """
        settings = self._settings
        suppressed_ids = settings.suppressed_ids()
        matched_suppressions: set[str] = set()

        raw_findings: list[PostureFinding] = []
        seen_names: dict[str, int] = {}
        for collector in self._collectors:
            if isinstance(collector, PostureCollector):
                name = collector.name
                fn: Callable[[], list[PostureFinding]] = collector.collect
            else:
                name = getattr(collector, "__name__", repr(collector))
                fn = collector
            seen_names[name] = seen_names.get(name, 0) + 1
            wrapped = _wrap_collector(fn, name=name)
            raw_findings.extend(wrapped())

        # Two collectors sharing one `name` (§D-immutable — dedup at
        # registration is providify's job; this is varco's own "name
        # collision" report) — one WARN finding, both collectors still ran.
        for name, count in seen_names.items():
            if count > 1:
                raw_findings.append(
                    PostureFinding(
                        check="posture.duplicate_collector",
                        severity=PostureSeverity.WARN,
                        detail=f"{count} posture collectors share the name {name!r}.",
                        remediation="Give each PostureCollector a unique `name`.",
                    )
                )

        # Unknown suppression ids are a typo that silently suppresses
        # nothing — surfaced rather than swallowed (§D-S9-suppress).
        for f in raw_findings:
            if f.check in suppressed_ids:
                matched_suppressions.add(f.check)
        unknown = suppressed_ids - matched_suppressions
        for unknown_id in sorted(unknown):
            raw_findings.append(
                PostureFinding(
                    check="posture.unknown_suppression",
                    severity=PostureSeverity.INFO,
                    detail=(
                        f"VARCO_SECURITY_SUPPRESS names {unknown_id!r}, which no finding's "
                        f"check id matches — nothing was suppressed."
                    ),
                    remediation="correct or remove the unknown id from VARCO_SECURITY_SUPPRESS",
                )
            )

        final_findings: list[PostureFinding] = []
        for f in raw_findings:
            severity = f.severity
            suppressed = f.check in suppressed_ids
            if suppressed:
                severity = PostureSeverity.INFO
            elif settings.environment == "development" and severity == PostureSeverity.WARN:
                # §D-S9-oq1: development demotes presentation only — the
                # finding is still produced, just quieter.
                severity = PostureSeverity.INFO
            final_findings.append(replace(f, severity=severity, suppressed=suppressed))

        not_assessed = tuple(
            f.check for f in final_findings if f.severity == PostureSeverity.NOT_ASSESSED
        )
        report = SecurityPosture(
            environment=settings.environment,
            findings=tuple(final_findings),
            not_assessed=not_assessed,
        )
        self._report = report

        _logger.warning("SecurityPosture: %s", report.summary())
        for f in report.findings:
            if f.severity in (PostureSeverity.WARN, PostureSeverity.HIGH):
                _logger.warning(
                    "SecurityPosture[%s] %s: %s (fix: %s)",
                    f.severity.value,
                    f.check,
                    f.detail,
                    f.remediation,
                )

        if settings.enforce == "refuse":
            blocking = [
                f
                for f in report.findings
                if not f.suppressed
                and (
                    f.severity == PostureSeverity.HIGH
                    or (f.severity == PostureSeverity.WARN and settings.environment == "production")
                )
            ]
            if blocking:
                ids = ", ".join(f.check for f in blocking)
                raise RuntimeError(
                    f"SecurityPostureLifecycle(enforce='refuse'): refusing to start — "
                    f"unsuppressed blocking findings: {ids}"
                )

    async def stop(self) -> None:
        """No-op — the preflight has no running resource to release."""
        return None


# ── §D-S9-checks — the five collectors ──────────────────────────────────────
#
# Each of the four sibling-backed collectors is a thin adapter: import the
# sibling's exported inspector **inside the function body** (§D-S9-degrade —
# an app that never installed the sibling package must never pay an import
# cost, and a missing module must degrade to NOT_ASSESSED rather than break
# import of this module itself), call it, and translate its report into
# `PostureFinding`s. Severity assignment lives here, never in the sibling —
# the one exception is 035's nine ids, re-emitted with 035's own severities
# unchanged (the two plans already commit to those together).
#
# DESIGN: every collector takes optional, already-resolved objects as kwargs
#     rather than a `container` it resolves things from itself.
#     ✅ Keeps each collector a pure, synchronous, dependency-free function —
#        callable with zero arguments (as `SecurityPostureLifecycle` does
#        when nothing is wired), and callable with real objects a caller
#        (typically the app's own wiring code, which already has them) can
#        pass in directly.
#     ✅ `inspect_rls_posture()` is `async def` and needs a live
#        `AsyncConnection` — a synchronous collector cannot call it inline
#        without an event loop; requiring the caller to supply an
#        already-fetched `RlsPosture` (or `None`) keeps this collector
#        synchronous and honest about what it did and did not check.
#     ❌ A caller who wants the full picture must do a little wiring of
#        their own (pass `app=`, `dispatcher=`, `rls_posture=`, ...). Named
#        in the feature doc's usage section (Phase 5).


def _tenant_finding_for_token(token: str) -> PostureFinding:
    """Map one of 033's stable ``tenant.*`` tokens to a severity + remediation."""
    table: dict[str, tuple[PostureSeverity, str, str]] = {
        "tenant.no_chain": (
            PostureSeverity.WARN,
            "No TenantSourceChain configured — tenant resolution is header-only.",
            "Configure a TenantSourceChain (varco_core.tenancy.source) with an "
            "explicit LegacyTenantSource, or a higher-trust source.",
        ),
        "tenant.legacy_source_implicit": (
            PostureSeverity.WARN,
            "The header-only fallback is implicit (chain=None), not an explicit "
            "LegacyTenantSource in a configured chain.",
            "Name LegacyTenantSource explicitly in a TenantSourceChain.",
        ),
        "tenant.no_membership_provider": (
            PostureSeverity.WARN,
            "No AbstractTenantMembership is bound — a resolved tenant is never "
            "checked against the caller's actual membership.",
            "Bind an AbstractTenantMembership (varco_core.tenancy.membership).",
        ),
        "tenant.membership_missing_claim_allows": (
            PostureSeverity.HIGH,
            "ClaimTenantMembership.on_missing_claim allows access when the "
            "membership claim itself is absent from the token.",
            "Set on_missing_claim to deny (or an equivalent fail-closed policy).",
        ),
        "tenant.cross_check_lenient": (
            PostureSeverity.INFO,
            "CrossCheckMode is LENIENT — disagreeing sources do not block the "
            "request. This is the documented, not-a-flip default.",
            "No action required; SET a stricter CrossCheckMode if your deployment needs one.",
        ),
        "tenant.unchained_claim_tenant_setter": (
            PostureSeverity.WARN,
            "RequestContextMiddleware still sets the tenant from a claim alone, "
            "with no catalog/membership check.",
            "Adopt a TenantSourceChain and disable RequestContextMiddleware.enable_tenant_context.",
        ),
        "tenant.delegation_unbound": (
            PostureSeverity.INFO,
            "No delegation policy is bound — act-as delegation (RFC 8693) is not in use.",
            "Bind a DelegationPolicy (varco_core.auth.delegation) if your "
            "deployment needs service-to-service delegation.",
        ),
        "tenant.third_party_source_high_trust": (
            PostureSeverity.INFO,
            "A non-built-in TenantSource in the active chain declares HIGH or higher "
            "trust — a plugin can elevate itself into the identity chain by claiming "
            "trust (this may be legitimate, e.g. an mTLS-derived source).",
            "Confirm the third-party TenantSource's trust level is deliberate "
            "(Plan 051 / DI-5, §D-tenant).",
        ),
    }
    severity, detail, remediation = table.get(
        token,
        (
            PostureSeverity.INFO,
            f"Unrecognised tenant-provenance finding token {token!r}.",
            "See technical_docs/features/tenant-provenance.md.",
        ),
    )
    return PostureFinding(check=token, severity=severity, detail=detail, remediation=remediation)


def _collect_tenant(
    *,
    chain: Any | None = None,
    membership: Any | None = None,
    delegation: Any | None = None,
) -> list[PostureFinding]:
    """§D-S9-checks: re-emit 033's `inspect_tenant_provenance()` tokens."""
    from varco_core.tenancy import inspect_tenant_provenance

    posture = inspect_tenant_provenance(chain, membership=membership, delegation=delegation)
    return [_tenant_finding_for_token(token) for token in posture.findings]


def _collect_auth(
    *,
    auth: Any | None = None,
    registry: Any | None = None,
    store: Any | None = None,
) -> list[PostureFinding]:
    """§D-S9-checks: 034's `inspect_auth_posture()` + `inspect_revocation_posture()`."""
    from varco_core.revocation import inspect_revocation_posture

    from varco_fastapi.auth.posture import inspect_auth_posture

    findings: list[PostureFinding] = []

    if auth is not None:
        report = inspect_auth_posture(auth)
        if report.api_key_query_fallback_enabled:
            findings.append(
                PostureFinding(
                    check="auth.api_key_query_fallback",
                    severity=PostureSeverity.WARN,
                    detail=(
                        f"ApiKeyAuth accepts the API key via query parameter "
                        f"{report.api_key_query_param_name!r} — credentials in a URL "
                        f"land in access/proxy logs."
                    ),
                    remediation="Do not pass param= to ApiKeyAuth unless required.",
                )
            )
        if report.api_key_plaintext_source:
            findings.append(
                PostureFinding(
                    check="auth.api_key_plaintext",
                    severity=PostureSeverity.WARN,
                    detail="ApiKeyAuth was constructed with keys= (plaintext) rather "
                    "than hashed_keys=.",
                    remediation="Use ApiKeyAuth(hashed_keys=...) with hash_api_key().",
                )
            )
        if report.passthrough_auth_bound:
            findings.append(
                PostureFinding(
                    check="auth.passthrough_bound",
                    severity=PostureSeverity.WARN,
                    detail="A PassthroughAuth is present in the auth tree — it trusts "
                    "caller-supplied identity with no verification.",
                    remediation="Replace PassthroughAuth with a verifying strategy in production.",
                )
            )

    rev = inspect_revocation_posture(registry=registry, store=store)
    if rev.store_bound and not rev.registry_wired:
        findings.append(
            PostureFinding(
                check="auth.revocation_registry_unwired",
                severity=PostureSeverity.WARN,
                detail=(
                    f"A revocation store ({rev.store_kind}) is bound, but was never "
                    f"passed to TrustedIssuerRegistry(revocation_store=...) — revocation "
                    f"checking is not actually happening."
                ),
                remediation="Pass revocation_store= to TrustedIssuerRegistry.",
            )
        )
    elif not rev.store_bound:
        findings.append(
            PostureFinding(
                check="auth.revocation_unbound",
                severity=PostureSeverity.INFO,
                detail="No token revocation store is bound.",
                remediation="enable_token_revocation()/enable_redis_token_revocation() "
                "if pre-expiry credential revocation is needed.",
            )
        )
    if rev.failure_mode == "fail_open":
        findings.append(
            PostureFinding(
                check="auth.revocation_fail_open",
                severity=PostureSeverity.HIGH,
                detail="JwtVerificationSettings.revocation_failure_mode is fail_open — a "
                "revocation-store outage silently ALLOWS every token.",
                remediation="Set VARCO_JWT_REVOCATION_FAILURE_MODE=fail_closed.",
            )
        )

    return findings


def _collect_http(*, app: Any | None = None) -> list[PostureFinding]:
    """§D-S9-checks: re-emit 035's nine `http.*` ids **unchanged** (severities
    included) — 035 already commits to them (`plans/035:§D-seam`)."""
    from varco_fastapi.middleware.introspect import inspect_http_edge

    if app is None:
        return []
    posture = inspect_http_edge(app)
    return [
        PostureFinding(
            check=f.check,
            severity=PostureSeverity(f.severity),
            detail=f.detail,
            remediation=f.remediation,
        )
        for f in posture.findings
    ]


def _collect_data(*, rls_posture: Any | None = None) -> list[PostureFinding]:
    """
    §D-S9-checks: 037's `inspect_rls_posture()`.

    `inspect_rls_posture()` is ``async def`` and needs a live
    ``AsyncConnection`` — this (synchronous) collector cannot call it
    inline. A caller who wants this check live must ``await
    inspect_rls_posture(conn, tables=...)`` themselves and pass the
    resulting ``RlsPosture`` in as ``rls_posture=``.
    """
    # Import proves the sibling package is (or is not) installed — the
    # only thing this collector can check for itself without a live
    # connection. A Mongo-only app that never installed varco_sa degrades
    # here exactly like every other absent-sibling collector.
    from varco_sa.tenancy.rls_check import RlsPosture  # noqa: F401

    if rls_posture is None:
        return [
            PostureFinding(
                check="data.rls_not_checked",
                severity=PostureSeverity.NOT_ASSESSED,
                detail="varco_sa is installed, but no RlsPosture was supplied — "
                "inspect_rls_posture() needs a live database connection this "
                "collector cannot open on its own.",
                remediation="await inspect_rls_posture(conn, tables=...) and pass the "
                "result as rls_posture=.",
            )
        ]

    findings: list[PostureFinding] = []
    if not rls_posture.tables:
        return findings
    if any(not t.enabled for t in rls_posture.tables.values()):
        findings.append(
            PostureFinding(
                check="data.rls_disabled",
                severity=PostureSeverity.HIGH,
                detail="At least one TENANT-scoped table has Row-Level Security disabled.",
                remediation="render_rls_ddl()/varco_sa.rls_autogen, then an "
                "application-authored, reviewed migration (never varco-owned).",
            )
        )
    if any(t.enabled and not t.forced for t in rls_posture.tables.values()):
        findings.append(
            PostureFinding(
                check="data.rls_not_forced",
                severity=PostureSeverity.WARN,
                detail="At least one table has RLS ENABLED but not FORCEd — the "
                "table owner still bypasses it.",
                remediation="ALTER TABLE ... FORCE ROW LEVEL SECURITY.",
            )
        )
    if rls_posture.is_superuser or rls_posture.rolbypassrls or rls_posture.owned_tables:
        findings.append(
            PostureFinding(
                check="data.role_bypasses_rls",
                severity=PostureSeverity.HIGH,
                detail="The connecting role is a superuser, has BYPASSRLS, or owns "
                "one or more RLS-protected tables — RLS is silently ineffective for "
                "this role.",
                remediation="Use a non-superuser, non-BYPASSRLS, non-owning role for "
                "the application's runtime connection.",
            )
        )
    return findings


def _collect_local(
    *,
    container: Any | None = None,
    webhook_mounted_unauthenticated: bool = False,
    reliability_mounted_unauthenticated: bool = False,
    tenant_admin_mounted: bool = False,
    webhook_repository: Any | None = None,
    webhook_dispatcher: Any | None = None,
) -> list[PostureFinding]:
    """
    §D-S9-checks: the one collector this plan owns outright — the admin
    mounts, the webhook secret-encryption posture, and whether
    `BaseAuthorizer` (the permissive fallback) is what `AbstractAuthorizer`
    actually resolves to.

    Every fact here is explicit, caller-supplied input (a `container`, and
    a handful of already-known booleans/objects a wiring call site has on
    hand) — never re-derived by walking `app.user_middleware` or poking a
    sibling module's private state (the Non-goals section's rule).
    """
    findings: list[PostureFinding] = []

    if webhook_mounted_unauthenticated or reliability_mounted_unauthenticated:
        findings.append(
            PostureFinding(
                check="posture.admin_mount_unauthenticated",
                severity=PostureSeverity.WARN,
                detail="An admin surface (webhook and/or reliability) is mounted with "
                "server_auth=None.",
                remediation="Pass server_auth= to mount_webhook_admin()/mount_reliability_admin().",
            )
        )

    if tenant_admin_mounted:
        findings.append(
            PostureFinding(
                check="posture.tenant_admin_mounted",
                severity=PostureSeverity.INFO,
                detail="mount_tenant_admin() is mounted — the tenant control plane is "
                "guarded by a role alone (§D-S4-control); it is deliberately not "
                "bound to the resolved tenant.",
                remediation="No action required; see technical_docs/features/"
                "admin-surface-tenancy.md's §D-S4-control note.",
            )
        )

    if webhook_repository is not None and getattr(webhook_repository, "_encryptor", None) is None:
        findings.append(
            PostureFinding(
                check="posture.webhook_secrets_plaintext",
                severity=PostureSeverity.WARN,
                detail="The bound WebhookSubscriptionRepository has no encryptor — "
                "signing secrets are stored in plaintext.",
                remediation="Construct the repository with encryptor=<a FieldEncryptor>.",
            )
        )

    if webhook_dispatcher is not None:
        from varco_core.webhook.dispatcher import WebhookDispatcher

        if type(webhook_dispatcher) is not WebhookDispatcher:
            findings.append(
                PostureFinding(
                    check="posture.webhook_transport_unverified",
                    severity=PostureSeverity.INFO,
                    detail="A custom webhook transport is bound; varco cannot verify it "
                    "connects to target.pinned_ip rather than re-resolving the hostname.",
                    remediation="Confirm the custom transport preserves the SSRF pin "
                    "(varco_core.webhook.ssrf.validate_target()).",
                )
            )

    if container is not None:
        try:
            from varco_core.auth.authorizer import BaseAuthorizer
            from varco_core.auth.base import AbstractAuthorizer

            authorizer = container.get(AbstractAuthorizer)
        except Exception:  # noqa: BLE001 — resolution failure is NOT_ASSESSED, never a crash
            findings.append(
                PostureFinding(
                    check="posture.not_assessed.base_authorizer_bound",
                    severity=PostureSeverity.NOT_ASSESSED,
                    detail="Could not resolve AbstractAuthorizer from the container.",
                    remediation="Ensure AbstractAuthorizer has a binding.",
                )
            )
        else:
            if type(authorizer) is BaseAuthorizer:
                findings.append(
                    PostureFinding(
                        check="posture.base_authorizer_bound",
                        severity=PostureSeverity.WARN,
                        detail="AbstractAuthorizer resolves to BaseAuthorizer — every "
                        "operation is allowed unconditionally.",
                        remediation="Register any AbstractAuthorizer binding — "
                        "varco's BaseAuthorizer is @Fallback and yields to it.",
                    )
                )

    return findings


def _collect_di(container: Any) -> list[PostureFinding]:
    """
    Plan 047 §D-posture — one ``di.in_process_backend`` finding per seam
    still served by its in-process default (``value == "memory"``).

    ``revocation``'s default is ``"null"``, never reported here — that
    finding belongs to 034's ``inspect_revocation_posture()`` (never
    duplicated). A seam whose live binding is a **code** binding
    (``source == "code"``) is never reported, regardless of its
    configured value — the operator explicitly chose it.

    Args:
        container: A bootstrapped ``DIContainer``.

    Returns:
        Zero or more ``PostureFinding``s, ``check="di.in_process_backend"``.

    Edge cases:
        Severity is always ``WARN`` here, same as every other collector in
        this module (see `_collect_auth`/`_collect_tenant`) — the
        `development`-environment demotion to ``INFO`` is applied
        centrally by `SecurityPostureLifecycle.start()`, which reads
        `SecurityPostureSettings.environment` (`VARCO_SECURITY_ENV`). This
        collector must never read a second, parallel environment variable
        to pre-demote its own finding — doing so would let a deployment
        that never set `VARCO_SECURITY_ENV` (and is therefore in the
        `"production"` default) demote itself independently, defeating
        `enforce="refuse"`. Never raises — an import/resolution failure
        here would be a programming error, not an unassessed fact, since
        every one of the six seams always has a live binding.
    """
    from varco_core.di.select import inspect_backend_selection  # noqa: PLC0415

    findings: list[PostureFinding] = []
    for selection in inspect_backend_selection(container):
        if selection.seam == "revocation":
            continue
        if selection.source == "code":
            continue
        if selection.value != "memory":
            continue
        findings.append(
            PostureFinding(
                check="di.in_process_backend",
                severity=PostureSeverity.WARN,
                detail=f"{selection.seam} is served by {selection.live_owner} "
                f"({selection.source}) — in-process, lost on restart, "
                f"single-process only.",
                remediation=f"set VARCO_{selection.seam.upper()}_BACKEND=<one of "
                f"registered values> or bind_instance(...).",
            )
        )
    return findings


# ── §D-posture — the five collectors as PostureCollector contributions ──────
#
# Plan 051 / DI-5: each class is a thin PostureCollector wrapper over the
# corresponding `_collect_*` function above — the function bodies are left
# untouched (the byte-identical-findings promise this plan and 057 both
# make); the class only owns construction (storing the old function's
# keyword arguments as instance attributes) so it can be a `@Multibound`
# contribution instead of a `functools.partial` in a hand-built list.
# `SecurityPostureModule.configure_app()` is what actually calls
# `contribute()` for each of these (varco_fastapi/modules/lifecycle.py).


class HttpEdgeCollector(PostureCollector):
    """`PostureCollector` wrapper over `_collect_http` (§D-posture)."""

    name = "http_edge"

    def __init__(self, *, app: Any | None = None) -> None:
        self._app = app

    def collect(self) -> list[PostureFinding]:
        return _collect_http(app=self._app)


class LocalPostureCollector(PostureCollector):
    """`PostureCollector` wrapper over `_collect_local` (§D-posture)."""

    name = "local"

    def __init__(
        self,
        *,
        container: Any | None = None,
        webhook_mounted_unauthenticated: bool = False,
        reliability_mounted_unauthenticated: bool = False,
        tenant_admin_mounted: bool = False,
        webhook_repository: Any | None = None,
        webhook_dispatcher: Any | None = None,
    ) -> None:
        self._container = container
        self._webhook_mounted_unauthenticated = webhook_mounted_unauthenticated
        self._reliability_mounted_unauthenticated = reliability_mounted_unauthenticated
        self._tenant_admin_mounted = tenant_admin_mounted
        self._webhook_repository = webhook_repository
        self._webhook_dispatcher = webhook_dispatcher

    def collect(self) -> list[PostureFinding]:
        return _collect_local(
            container=self._container,
            webhook_mounted_unauthenticated=self._webhook_mounted_unauthenticated,
            reliability_mounted_unauthenticated=self._reliability_mounted_unauthenticated,
            tenant_admin_mounted=self._tenant_admin_mounted,
            webhook_repository=self._webhook_repository,
            webhook_dispatcher=self._webhook_dispatcher,
        )


class AuthPostureCollector(PostureCollector):
    """`PostureCollector` wrapper over `_collect_auth` (§D-posture)."""

    name = "auth"

    def __init__(
        self,
        *,
        auth: Any | None = None,
        registry: Any | None = None,
        store: Any | None = None,
    ) -> None:
        self._auth = auth
        self._registry = registry
        self._store = store

    def collect(self) -> list[PostureFinding]:
        return _collect_auth(auth=self._auth, registry=self._registry, store=self._store)


class TenantPostureCollector(PostureCollector):
    """`PostureCollector` wrapper over `_collect_tenant` (§D-posture)."""

    name = "tenant"

    def __init__(
        self,
        *,
        chain: Any | None = None,
        membership: Any | None = None,
        delegation: Any | None = None,
    ) -> None:
        self._chain = chain
        self._membership = membership
        self._delegation = delegation

    def collect(self) -> list[PostureFinding]:
        return _collect_tenant(
            chain=self._chain, membership=self._membership, delegation=self._delegation
        )


class DataPostureCollector(PostureCollector):
    """`PostureCollector` wrapper over `_collect_data` (§D-posture)."""

    name = "data"

    def __init__(self, *, rls_posture: Any | None = None) -> None:
        self._rls_posture = rls_posture

    def collect(self) -> list[PostureFinding]:
        return _collect_data(rls_posture=self._rls_posture)
