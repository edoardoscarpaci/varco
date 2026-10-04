"""
varco_fastapi.posture
========================
``SecurityPosture`` — the Plan 036 (S9) startup preflight, aggregating the
five sibling introspection seams (033's `inspect_tenant_provenance()`, 034's
`inspect_auth_posture()`/`inspect_revocation_posture()`, 035's
`inspect_http_edge()`, 037's `inspect_rls_posture()`, 040's
`inspect_redaction_posture()`) plus one local collector for facts only this plan can see, into one report an operator
reads at startup (§D-S9-shape).

**Never a silent skip.** A collector whose sibling module is not installed,
or that raises for any other reason, reports exactly one ``NOT_ASSESSED``
finding rather than vanishing from the report — §D-S9-degrade.

**Refuses a production start by default (4.0, Plan 054 / FLIP-2).**
`SecurityPostureSettings.enforce` is unset by default and *derives*
``"refuse"`` unless ``VARCO_SECURITY_ENV`` is exactly ``development`` (where
it derives ``"warn"``). Under ``refuse`` an unsuppressed ``HIGH`` finding
blocks in every environment and an unsuppressed ``WARN`` blocks in
``production`` (§D-S9-enforce, unchanged since 3.2); ``NOT_ASSESSED`` and
``INFO`` never block. ``VARCO_SECURITY_ENFORCE=warn`` restores the 3.x
warn-only preflight; the per-id consequence is tabulated in
``technical_docs/features/security-posture.md``'s severity table.

Thread safety:  ✅ Frozen dataclasses throughout; `SecurityPostureLifecycle`
                   mutates only its own `._report` attribute, once, at
                   `start()` — no shared mutable state across requests.
Async safety:   ✅ `start()`/`stop()` are the only async entry points; every
                   collector is a synchronous, pure read over
                   already-constructed objects (no I/O).
"""

from __future__ import annotations

import logging
import os
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
    "DIPostureCollector",
    "RedactionPostureCollector",
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
        enforce: The *effective* enforcement mode that judged this report
            (`SecurityPostureSettings.effective_enforce`) — Plan 054 /
            §D-flip2-report. Trailing and defaulted so positional
            construction keeps working.
    """

    environment: str
    findings: tuple[PostureFinding, ...]
    not_assessed: tuple[str, ...]
    enforce: str = "warn"

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
        changes across a report with vs. without suppressions. Ends with
        ``· enforce=<mode>`` (Plan 054 / §D-flip2-report) so the one line an
        operator greps says which rule judged the report.
        """
        counts = self.counts()
        suppressed = sum(1 for f in self.findings if f.suppressed)
        return (
            f"{counts[PostureSeverity.INFO]} info, "
            f"{counts[PostureSeverity.WARN]} warn, "
            f"{counts[PostureSeverity.HIGH]} high, "
            f"{counts[PostureSeverity.NOT_ASSESSED]} not assessed "
            f"({suppressed} suppressed) · enforce={self.enforce}"
        )


class SecurityPostureSettings(VarcoSettings):
    """
    ``VARCO_SECURITY_*`` env vars governing the preflight's presentation and
    consequence (§D-S9-oq1, §D-S9-enforce, §D-S9-suppress).

    Attributes:
        environment: ``VARCO_SECURITY_ENV``, default ``"production"``.
            Presentation only for the findings — see the module docstring
            and §D-S9-oq1. The same variable (read from ``os.environ``, not
            from this field) also gates the permissive ``BaseAuthorizer``
            DI binding (Plan 053 / FLIP-5), so a code-configured
            ``environment="development"`` demotes findings but does NOT
            bind ``BaseAuthorizer``.
            Defaulting to strict is deliberate: an explicit flag that
            defaults to lenient is one nobody sets (the backlog's own
            framing).
        enforce: ``VARCO_SECURITY_ENFORCE``, default ``None`` (unset) since
            4.0 (Plan 054 / FLIP-2). An explicit ``"warn"``/``"refuse"``
            (code or env) wins; ``None`` derives — see
            `effective_enforce`. ``"refuse"`` raises at `start()` when any
            non-suppressed finding is ``HIGH`` (or, in ``production``,
            ``WARN``) — never on ``NOT_ASSESSED`` alone. Never read this
            field directly to decide behaviour; read `effective_enforce`
            (guarded by ``tests/test_posture_enforce_readers.py``).
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
    # DESIGN: sentinel-derived default, keyed on `!= "development"`
    # (Plan 054 / FLIP-2, §D-flip2-default).
    #   ✅ "refuse in production" with `development` as the only lenient
    #      word — an unrecognised VARCO_SECURITY_ENV (`staging`, `prod`)
    #      derives the strict mode, never a more lenient one.
    #   ✅ 048's per-field precedence holds with no validator trick:
    #      code `enforce="warn"` > env VARCO_SECURITY_ENFORCE > derived; the
    #      field itself still says `None`, so 052's report can render the
    #      provenance honestly as "derived". `frozen=True` stays intact.
    #   ❌ `settings.enforce` is no longer the value the lifecycle uses —
    #      every reader must go through `effective_enforce`.
    #   Rejected: a literal `enforce: str = "refuse"` — `development` would
    #   then refuse on HIGH (`ErrorMiddleware(debug=True)`, an unbound
    #   authorizer), making the documented dev setup unstartable.
    enforce: str | None = None
    suppress: str = ""
    # Plan 049 / FLIP-1: same alias shape as `environment` above — the
    # bare env_prefix + FIELD_NAME.upper() rule would map this to
    # VARCO_SECURITY_ENABLED, but BACKLOG.md:81/036 §D-S9-flip #1 name
    # VARCO_SECURITY_POSTURE_ENABLED explicitly.
    enabled: bool = Field(
        default=True,
        validation_alias=AliasChoices("VARCO_SECURITY_POSTURE_ENABLED"),
    )

    @property
    def effective_enforce(self) -> str:
        """
        The enforcement mode the lifecycle actually applies.

        Returns:
            `enforce` verbatim when set explicitly (code or env — never
            normalised; an unrecognised value is reported as
            ``posture.unrecognised_enforce`` and behaves like ``"warn"``);
            otherwise ``"warn"`` when `environment` is exactly
            ``"development"`` and ``"refuse"`` for every other value.
        """
        if self.enforce is not None:
            return self.enforce
        return "warn" if self.environment == "development" else "refuse"

    @property
    def enforce_is_derived(self) -> bool:
        """
        Returns:
            ``True`` when `enforce` was not set (code or env) and
            `effective_enforce` was derived from `environment`.
        """
        return self.enforce is None

    def suppressed_ids(self) -> frozenset[str]:
        """Parse `suppress` into a set of `check` ids, ignoring blanks."""
        return frozenset(s.strip() for s in self.suppress.split(",") if s.strip())


def _enforce_origin(settings: SecurityPostureSettings) -> str:
    """
    Render how `settings.effective_enforce` was decided, for the refusal
    message (Plan 054 / §D-flip2-message).

    Args:
        settings: The settings the lifecycle judged with.

    Returns:
        ``"enforce='refuse', derived from VARCO_SECURITY_ENV='production'"``
        when derived; otherwise ``"enforce='refuse', from
        VARCO_SECURITY_ENFORCE"`` or ``"…, from code"``.

    DESIGN: an ``os.environ`` presence check, not ``model_fields_set``
        ✅ pydantic-settings puts env-populated fields into
           ``model_fields_set`` (verified, Plan 054 Step 1), so that set
           cannot tell env from code; the lifecycle has no container to ask
           048's ``provenance_of()``. A presence check (not a value read) is
           also not a loose settings read (``LOOSE_ENV_READ_ALLOWLIST``).
        ❌ A code value set while the variable is also exported is labelled
           ``env`` — the operator still sees the variable worth checking.
    """
    mode = settings.effective_enforce
    if settings.enforce_is_derived:
        return f"enforce={mode!r}, derived from VARCO_SECURITY_ENV={settings.environment!r}"
    if "VARCO_SECURITY_ENFORCE" in os.environ:
        return f"enforce={mode!r}, from VARCO_SECURITY_ENFORCE"
    return f"enforce={mode!r}, from code"


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
            RuntimeError: `settings.effective_enforce == "refuse"` (the
                derived default outside ``development`` since 4.0) and at
                least one non-suppressed finding is ``HIGH`` (or, in
                `production`, ``WARN``). Never raised for ``NOT_ASSESSED``
                alone. The message names every blocking id, how the mode
                was decided, and the three fixes.
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

        # Plan 054 / §D-flip2-env: an unrecognised value is reported, never
        # normalised (same "report the typo" shape as unknown suppression).
        effective_enforce = settings.effective_enforce
        if settings.environment not in ("production", "development"):
            raw_findings.append(
                PostureFinding(
                    check="posture.unrecognised_environment",
                    severity=PostureSeverity.INFO,
                    detail=(
                        f"VARCO_SECURITY_ENV is {settings.environment!r} — not 'production' "
                        f"or 'development'. Treated as strict for enforcement (refuse on "
                        f"HIGH) but WARN findings neither block nor demote."
                    ),
                    remediation="Set VARCO_SECURITY_ENV to exactly 'production' or 'development'.",
                )
            )
        if effective_enforce not in ("warn", "refuse"):
            raw_findings.append(
                PostureFinding(
                    check="posture.unrecognised_enforce",
                    severity=PostureSeverity.INFO,
                    detail=(
                        f"VARCO_SECURITY_ENFORCE is {effective_enforce!r} — not 'warn' or "
                        f"'refuse'. Treated as 'warn': nothing blocks startup."
                    ),
                    remediation="Set VARCO_SECURITY_ENFORCE to exactly 'warn' or 'refuse', "
                    "or unset it to get the derived default.",
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
            enforce=effective_enforce,
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

        if effective_enforce == "refuse":
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
                # §D-flip2-message: the 3.2 fragment "refusing to start —
                # unsuppressed blocking findings:" is kept verbatim (operator
                # alert rules and 049's module test match on it).
                raise RuntimeError(
                    f"SecurityPostureLifecycle({_enforce_origin(settings)}): refusing to start — "
                    f"unsuppressed blocking findings: {ids}. Fix each finding (see the "
                    f"SecurityPosture[...] lines above), suppress a knowingly-accepted id "
                    f"with VARCO_SECURITY_SUPPRESS=<id>, or set VARCO_SECURITY_ENFORCE=warn "
                    f"to restore the 3.x warn-only preflight."
                )

    async def stop(self) -> None:
        """No-op — the preflight has no running resource to release."""
        return None


# ── §D-S9-checks — the six collectors ───────────────────────────────────────
#
# Each of the five sibling-backed collectors is a thin adapter: import the
# sibling's exported inspector **inside the function body** (§D-S9-degrade —
# an app that never installed the sibling package must never pay an import
# cost, and a missing module must degrade to NOT_ASSESSED rather than break
# import of this module itself), call it, and translate its report into
# `PostureFinding`s. Severity assignment lives here, never in the sibling —
# the one exception is 035's nine ids, re-emitted with 035's own severities
# unchanged (the two plans already commit to those together); the second
# exception is 056's `data.*` ids, assigned in `varco_sa.tenancy.rls_posture`
# because the live collector lives there (051's handoff).
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
    """
    Map one of the stable ``tenant.*`` tokens to a severity + remediation.

    Every token ``varco_core.tenancy.posture`` (and 051's
    ``tenant.third_party_source_high_trust``, and this module's own
    ``tenant.no_catalog_gate``) can emit has a row (Plan 055 §D-collector) —
    the "Unrecognised" default is reachable only by a genuinely new token,
    guarded by ``test_every_tenant_token_has_a_severity_row``.

    Args:
        token: A ``tenant.*`` finding id.

    Returns:
        The ``PostureFinding`` for *token*.
    """
    table: dict[str, tuple[PostureSeverity, str, str]] = {
        "tenant.no_chain": (
            PostureSeverity.WARN,
            "Tenant resolution is wired but no TenantSourceChain is configured — the "
            "deprecated implicit X-Tenant-Id fallback is active (removed in 5.0.0).",
            "Set VARCO_TENANT_SOURCES=jwt[,…], or name header-only explicitly: "
            "TenantSourceChain(sources=(LegacyTenantSource(),)).",
        ),
        "tenant.legacy_source_implicit": (
            PostureSeverity.WARN,
            "The header-only fallback is implicit (chain=None with nothing configured), "
            "not an explicit LegacyTenantSource in a configured chain.",
            "Set VARCO_TENANT_SOURCES=jwt[,…], or name header-only explicitly: "
            "TenantSourceChain(sources=(LegacyTenantSource(),)).",
        ),
        "tenant.legacy_source_explicit": (
            PostureSeverity.WARN,
            "An explicit LegacyTenantSource (LOW trust, unbound header) is in the chain — "
            "safe only behind an ingress that strips and re-writes the header.",
            "Prefer a claim/subdomain source; if the ingress guarantee holds, "
            "VARCO_SECURITY_SUPPRESS=tenant.legacy_source_explicit.",
        ),
        "tenant.single_source": (
            PostureSeverity.INFO,
            "One source in the chain — no cross-check is possible.",
            "Add a second source if your deployment has one.",
        ),
        "tenant.no_membership_provider": (
            PostureSeverity.WARN,
            "No membership check runs — the resolved tenant is never verified against "
            "the caller (brief 006 §2).",
            "enable_tenant_membership(container); a deliberate single-tenant deployment: "
            "VARCO_SECURITY_SUPPRESS=tenant.no_membership_provider.",
        ),
        "tenant.membership_missing_claim_allows": (
            PostureSeverity.HIGH,
            "ClaimTenantMembership on_missing_claim=allow — explicitly opted out of the "
            "4.0 fail-closed default; a token without the membership claim is allowed.",
            "Unset VARCO_TENANT_MEMBERSHIP_ON_MISSING; during an IdP rollout, "
            "VARCO_SECURITY_SUPPRESS=tenant.membership_missing_claim_allows.",
        ),
        "tenant.cross_check_lenient": (
            PostureSeverity.INFO,
            "CrossCheckMode is LENIENT — disagreeing sources do not block the "
            "request. This is the documented, not-a-flip default.",
            "No action required; SET a stricter CrossCheckMode if your deployment needs one.",
        ),
        "tenant.subdomain_trusts_forwarded_host": (
            PostureSeverity.WARN,
            "Host header trusted from X-Forwarded-Host — only correct behind a proxy "
            "that overwrites it.",
            "Set trust_forwarded_host=False on SubdomainTenantSource unless your proxy "
            "overwrites X-Forwarded-Host; otherwise suppress by id.",
        ),
        "tenant.unchained_claim_tenant_setter": (
            PostureSeverity.WARN,
            "A component sets the tenant from a claim alone, with no catalog/membership "
            "check (the RequestContextMiddleware path was removed in 4.0; this id remains "
            "for hand-wired setters).",
            "Remove the hand-wired setter; resolve the tenant through TenancyModule "
            "with VARCO_TENANT_SOURCES=jwt.",
        ),
        "tenant.no_catalog_gate": (
            PostureSeverity.INFO,
            "TenantResolutionMiddleware has no catalog — a suspended/deleted tenant is not "
            "rejected at the edge (SHARED isolation without a control plane).",
            "Pass catalog= to TenancyModule if you provision/suspend tenants.",
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
    catalog_gated: bool = True,
) -> list[PostureFinding]:
    """
    §D-S9-checks: re-emit 033's ``inspect_tenant_provenance()`` tokens, plus
    Plan 055's ``tenant.no_catalog_gate``.

    Args:
        chain: The resolved ``TenantSourceChain``, or ``None`` (the
            deprecated implicit legacy fallback is active).
        membership: The bound ``AbstractTenantMembership``, or ``None``. A
            ``NullTenantMembership`` (``name == "null"``) is treated as
            ``None`` — it always allows, so it is not a check (Plan 055
            §D-collector narrowing #2). The pure inspector is not narrowed;
            it reports what it is given.
        delegation: The bound ``DelegationPolicy``, or ``None``.
        catalog_gated: ``False`` when the middleware has no catalog.

    Returns:
        One ``PostureFinding`` per token.
    """
    from varco_core.tenancy import inspect_tenant_provenance

    if membership is not None and getattr(membership, "name", None) == "null":
        membership = None
    # request_context_sets_tenant stays at its False default: varco's own
    # RequestContextMiddleware never sets the tenant since 4.0 (FLIP-9).
    posture = inspect_tenant_provenance(chain, membership=membership, delegation=delegation)
    tokens = list(posture.findings)
    if not catalog_gated:
        tokens.append("tenant.no_catalog_gate")
    return [_tenant_finding_for_token(token) for token in tokens]


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
                    remediation="Use ApiKeyAuth(hashed_keys=...) with hash_api_key() — "
                    "keys= is deprecated since 4.0.0 and removed in 5.0.0.",
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
    if rev.registry_wired and not rev.require_jti:
        # Plan 053 / FLIP-034-3: a real store is wired but the derived
        # require_jti=True default was explicitly opted out of. INFO, not
        # WARN — SUBJECT/TENANT/ISSUER scopes still apply, and a foreign IdP
        # with no jti (Auth0/Keycloak/Cognito) is a legitimate reason.
        findings.append(
            PostureFinding(
                check="auth.revocation_jti_not_required",
                severity=PostureSeverity.INFO,
                detail="A revocation store is wired but revocation_require_jti is False — "
                "TOKEN-scope revocation silently never matches a jti-less token "
                "(SUBJECT/TENANT/ISSUER scopes still apply).",
                remediation="Unset VARCO_JWT_REVOCATION_REQUIRE_JTI (the 4.0 default derives "
                "True from a wired store), or configure your issuer to emit jti.",
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
    §D-S9-checks: 037's `inspect_rls_posture()` — the hand-wired form.

    Since Plan 056 the live check is ``varco_sa.tenancy.rls_posture.
    RlsPostureCollector``, contributed by ``SAModule``; this function is the
    hand form for a caller that fetched an ``RlsPosture`` itself, and
    delegates to ``rls_findings()`` so the mapping exists exactly once.
    """
    # Import proves the sibling package is (or is not) installed — a
    # Mongo-only app that never installed varco_sa degrades here exactly like
    # every other absent-sibling collector.
    from varco_sa.tenancy.rls_posture import rls_findings  # noqa: PLC0415

    return rls_findings(
        rls_posture,
        not_checked_reason="varco_sa is installed, but no RlsPosture was supplied — "
        "inspect_rls_posture() needs a live database connection this collector "
        "cannot open on its own.",
    )


# Plan 057 / RO-5, §D-posture: remediation per stable `redaction.*` id.
# Severities come from 040's inspector unchanged; only the remediation text
# is assigned here. The keys are the four ids 040 committed to as stable —
# an operator's VARCO_SECURITY_SUPPRESS list must keep matching, so they are
# never renamed (the meaning changed in 4.0 to "opted out", the spelling not).
_REDACTION_REMEDIATION: dict[str, str] = {
    "redaction.audit.disabled": (
        "Remove `_audit_redactor = None` (the 4.0 default redacts), or override "
        "`_audit_diff()`; suppress with VARCO_SECURITY_SUPPRESS=redaction.audit.disabled "
        "if the opt-out is deliberate."
    ),
    "redaction.error_params.disabled": "Unset VARCO_ERROR_REDACT_PARAMS (default `true`).",
    "redaction.default.custom": (
        "No action required; confirm the custom redactor covers DEFAULT_REDACT_PATTERNS."
    ),
    "redaction.patterns.substring_mode": (
        "Drop `match_mode='substring'`, or add the exact keys you relied on as patterns "
        "under word mode."
    ),
}


def _collect_redaction(
    *,
    service_classes: tuple[type, ...] = (),
    envelope_settings: Any | None = None,
) -> list[PostureFinding]:
    """
    §D-posture (Plan 057 / RO-5): 040's `inspect_redaction_posture()`.

    Ids and severities are re-emitted unchanged (same shape as
    `_collect_http`); remediation is assigned here from
    `_REDACTION_REMEDIATION`. Since 4.0 (FLIP-040-1/-2) a default app
    produces no finding — every finding means "opted out".

    Args:
        service_classes: `AuditLogMixin` subclasses to inspect. Explicit
            caller input (036's rule — never discovered by walking
            `__subclasses__()`); bind it with `functools.partial(...)` in a
            hand-built collector list.
        envelope_settings: The `ErrorEnvelopeSettings` in force; `None`
            lets the inspector construct a fresh, env-driven one.

    Returns:
        One `PostureFinding` per `RedactionFinding`, in inspector order.

    Raises:
        ImportError: `varco_core.redaction.posture` is absent — converted
            to one ``NOT_ASSESSED`` finding by `_wrap_collector`.

    Edge cases:
        - Contributed by default through `RedactionPostureCollector`
          (`SecurityPostureModule(audited_services=...)` supplies
          `service_classes`). With no `service_classes`,
          `redaction.audit.disabled` can never fire.
    """
    from varco_core.redaction.posture import inspect_redaction_posture  # noqa: PLC0415

    posture = inspect_redaction_posture(
        service_classes=service_classes,
        envelope_settings=envelope_settings,
    )
    return [
        PostureFinding(
            check=f.check,
            severity=PostureSeverity(f.severity),
            detail=f.detail,
            remediation=_REDACTION_REMEDIATION.get(f.check, ""),
        )
        for f in posture.findings
    ]


def _resolve_webhook_repository(container: Any) -> tuple[Any | None, PostureFinding | None]:
    """
    Resolve the bound ``WebhookSubscriptionRepository`` for the plaintext check
    (Plan 056 / FLIP-13), never letting a resolution failure break the preflight.

    DESIGN: same guard shape as the authorizer block in ``_collect_local``
        ✅ Unbound / unresolvable → ``(None, None)``: nothing to assess, no noise.
        ✅ A raising provider (e.g. a durable backend selected with no bound
           ``FieldEncryptor``) → one NOT_ASSESSED finding, never a crash — the
           missing binding is ``validate()``'s ``MISSING_BINDING`` to report.
        ❌ Resolving a singleton at preflight constructs it if nothing has yet;
           accepted — the repositories do no I/O in ``__init__``.

    Args:
        container: The app's ``DIContainer``.

    Returns:
        ``(repository | None, not_assessed_finding | None)``.
    """
    from varco_core.webhook.base import WebhookSubscriptionRepository  # noqa: PLC0415

    try:
        if not container.is_resolvable(WebhookSubscriptionRepository):
            return None, None
        return container.get(WebhookSubscriptionRepository), None
    except LookupError:
        return None, None
    except Exception:  # noqa: BLE001 — construction failed: NOT_ASSESSED, never a crash
        return None, PostureFinding(
            check="posture.not_assessed.webhook_secrets_plaintext",
            severity=PostureSeverity.NOT_ASSESSED,
            detail="Could not construct WebhookSubscriptionRepository from the container.",
            remediation="Fix the WebhookSubscriptionRepository binding's construction error "
            "(a durable backend needs a bound FieldEncryptor).",
        )


def _webhook_secrets_plaintext(webhook_repository: Any | None) -> bool:
    """
    Whether a webhook repository stores its signing secrets in plaintext
    (Plan 056 / §D-flip13-posture).

    Args:
        webhook_repository: The bound ``WebhookSubscriptionRepository``, or
            ``None`` (nothing to assess).

    Returns:
        ``True`` for a bound ``PlaintextFieldEncryptor`` (the FLIP-13 dev
        hatch), or a durable repository with no/``None`` ``_encryptor`` (an
        out-of-tree repository that never took the flip). Always ``False``
        for ``InMemoryWebhookSubscriptionRepository`` — its secrets are never
        at rest, so "encryption at rest" has no referent (§D-flip13-inmemory).
    """
    if webhook_repository is None:
        return False
    from varco_core.encryption import PlaintextFieldEncryptor  # noqa: PLC0415
    from varco_core.webhook.base import InMemoryWebhookSubscriptionRepository  # noqa: PLC0415

    if isinstance(webhook_repository, InMemoryWebhookSubscriptionRepository):
        return False
    encryptor = getattr(webhook_repository, "_encryptor", None)
    return encryptor is None or isinstance(encryptor, PlaintextFieldEncryptor)


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

    ``posture.admin_mount_unauthenticated`` (WARN): since 4.0 (Plan 054 /
    FLIP-3/4) no varco mount or builder can produce this state — each
    refuses ``server_auth=None`` with ``ValueError``; the id remains for
    hand-wired surfaces that feed the two booleans themselves.

    Every fact here is explicit, caller-supplied input (a `container`, and
    a handful of already-known booleans/objects a wiring call site has on
    hand) — never re-derived by walking `app.user_middleware` or poking a
    sibling module's private state (the Non-goals section's rule).

    Authorizer ids (Plan 053 / FLIP-5): ``posture.authorizer_unbound``
    (HIGH — no active ``AbstractAuthorizer`` binding, the production state
    with no app authorizer), ``posture.base_authorizer_bound`` (WARN — the
    resolved instance is exactly ``BaseAuthorizer``: development, or an
    explicit instance/provider binding in production), and
    ``posture.not_assessed.base_authorizer_bound`` (construction raised
    something other than ``LookupError``).

    Webhook id (Plan 056 / FLIP-13): ``posture.webhook_secrets_plaintext``
    (WARN). With ``webhook_repository=None`` and a ``container``, the bound
    ``WebhookSubscriptionRepository`` is resolved here so a stock app is
    assessed; a provider that fails to construct is
    ``posture.not_assessed.webhook_secrets_plaintext``, never a crash.

    Edge cases:
        An app authorizer whose own ``__init__`` raises ``LookupError`` (an
        unbound dependency) is reported as ``posture.authorizer_unbound``.
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

    if webhook_repository is None and container is not None:
        # Plan 056 / FLIP-13: the default LocalPostureCollector only receives the
        # container, so the DI-bound repository (and a bound PlaintextFieldEncryptor
        # behind it) must be resolved here or a stock app would never be assessed.
        webhook_repository, not_assessed = _resolve_webhook_repository(container)
        if not_assessed is not None:
            findings.append(not_assessed)

    if _webhook_secrets_plaintext(webhook_repository):
        findings.append(
            PostureFinding(
                check="posture.webhook_secrets_plaintext",
                severity=PostureSeverity.WARN,
                detail="The bound WebhookSubscriptionRepository stores signing secrets in "
                "plaintext (PlaintextFieldEncryptor(acknowledge_plaintext=True) is bound, or "
                "the repository has no encryptor).",
                remediation="Bind a real FieldEncryptor (FernetFieldEncryptor(key)); if the "
                "plaintext run is deliberate (dev/test), "
                "VARCO_SECURITY_SUPPRESS=posture.webhook_secrets_plaintext.",
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
        from varco_core.auth.authorizer import BaseAuthorizer
        from varco_core.auth.base import AbstractAuthorizer

        # DESIGN (Plan 053 / §D-flip5-posture): a three-way split, not one
        # blanket except.
        #   ✅ LookupError == "no ACTIVE binding" — since FLIP-5 the gated
        #      BaseAuthorizer is absent outside VARCO_SECURITY_ENV=development,
        #      which is a HIGH fact, not something we failed to assess.
        #   ✅ container.get() (not get_binding()) — the WARN must see the
        #      *instance's* type to catch an explicit BaseAuthorizer()
        #      instance/provider binding in production.
        #   ❌ A LookupError raised *inside* an app authorizer's own __init__
        #      (its dependency is unbound) is reported as authorizer_unbound;
        #      the remediation (fix the binding) is still right, and
        #      validate() names the real missing parameter.
        try:
            authorizer = container.get(AbstractAuthorizer)
        except LookupError:
            findings.append(
                PostureFinding(
                    check="posture.authorizer_unbound",
                    severity=PostureSeverity.HIGH,
                    detail="No active AbstractAuthorizer binding — every service that "
                    "injects AbstractAuthorizer will fail with LookupError.",
                    remediation="No AbstractAuthorizer is bound. varco's permissive "
                    "BaseAuthorizer binds only under VARCO_SECURITY_ENV=development (4.0, "
                    "FLIP-5). Register your own AbstractAuthorizer (an app @Singleton, "
                    "enable_policy_authorizer(), …); for a development process set "
                    "VARCO_SECURITY_ENV=development. Every service that injects "
                    "AbstractAuthorizer will fail with LookupError until then.",
                )
            )
        except Exception:  # noqa: BLE001 — construction failed: NOT_ASSESSED, never a crash
            findings.append(
                PostureFinding(
                    check="posture.not_assessed.base_authorizer_bound",
                    severity=PostureSeverity.NOT_ASSESSED,
                    detail="Could not construct AbstractAuthorizer from the container.",
                    remediation="Fix the AbstractAuthorizer binding's construction error.",
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
                        remediation="AbstractAuthorizer resolves to BaseAuthorizer — every "
                        "operation is allowed unconditionally. In production this can only "
                        "be an explicit bind_instance()/@Provider of BaseAuthorizer(); remove "
                        "it or, if deliberate, suppress with "
                        "VARCO_SECURITY_SUPPRESS=posture.base_authorizer_bound.",
                    )
                )

    return findings


def _collect_di(container: Any) -> list[PostureFinding]:
    """
    Plan 047 §D-posture — one ``di.in_process_backend`` finding per seam
    still served by its in-process default (``value == "memory"``).

    Runs only through the **opt-in** ``DIPostureCollector`` (Plan 054
    decision) — never one of ``SecurityPostureModule``'s defaults.

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
    """
    `PostureCollector` wrapper over `_collect_local` (§D-posture).

    ``webhook_mounted_unauthenticated``/``reliability_mounted_unauthenticated``
    feed ``posture.admin_mount_unauthenticated``; since 4.0 no varco mount
    or builder can produce that state (``ValueError`` on
    ``server_auth=None``, Plan 054 / FLIP-3/4) — the id remains for
    hand-wired surfaces.
    """

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
    """
    ``PostureCollector`` wrapper over ``_collect_tenant`` (§D-posture).

    Contributed by ``TenancyModule`` — never by ``SecurityPostureModule``
    (Plan 055 §D-collector): ``tenant.*`` findings exist iff tenant
    resolution is actually wired, so a non-tenant app emits none.

    Args:
        chain: The chain the middleware was built with, or ``None``.
        membership: The membership provider, or ``None``.
        delegation: The ``DelegationPolicy``, or ``None``.
        catalog_gated: ``False`` adds ``tenant.no_catalog_gate`` (INFO).
        chain_implicit: ``True`` when the middleware will take the
            deprecated implicit legacy fallback. Recorded for diagnostics;
            the findings already follow from ``chain is None``.
    """

    name = "tenant"

    def __init__(
        self,
        *,
        chain: Any | None = None,
        membership: Any | None = None,
        delegation: Any | None = None,
        catalog_gated: bool = True,
        chain_implicit: bool = False,
    ) -> None:
        self._chain = chain
        self._membership = membership
        self._delegation = delegation
        self._catalog_gated = catalog_gated
        self.chain_implicit = chain_implicit

    def collect(self) -> list[PostureFinding]:
        return _collect_tenant(
            chain=self._chain,
            membership=self._membership,
            delegation=self._delegation,
            catalog_gated=self._catalog_gated,
        )


class DataPostureCollector(PostureCollector):
    """
    `PostureCollector` wrapper over `_collect_data` (§D-posture) — the
    hand-wired form. Not a default collector since Plan 056: ``SAModule``
    contributes the live ``varco_sa.tenancy.rls_posture.RlsPostureCollector``
    (same ``name``, so never wire both).
    """

    name = "data"

    def __init__(self, *, rls_posture: Any | None = None) -> None:
        self._rls_posture = rls_posture

    def collect(self) -> list[PostureFinding]:
        return _collect_data(rls_posture=self._rls_posture)


class DIPostureCollector(PostureCollector):
    """
    `PostureCollector` wrapper over `_collect_di` (Plan 047 §D-posture) —
    **opt-in**: never contributed by `SecurityPostureModule`'s defaults.

    Reports one ``di.in_process_backend`` WARN per selectable seam still
    served by its in-process default. Under Plan 054's derived
    ``enforce="refuse"`` a contributed instance therefore blocks a
    production start until every such seam names a real backend
    (``VARCO_<SEAM>_BACKEND=…``) or the id is suppressed.

    DESIGN: opt-in, not a default collector
        ✅ An in-process backend is a legitimate deployment shape (a single
           process, a worker that owns its own state, a demo) — a default
           collector would make every such app suppress an id to start in
           production, which is exactly the "refuse by surprise" 054's flip
           list must not contain.
        ✅ An app that does want the guarantee states it once, greppably
           (`rg -n DIPostureCollector`), and the id keeps its 054 meaning:
           WARN → blocks a production start.
        ❌ A forgotten ``VARCO_<SEAM>_BACKEND`` is not caught by posture in a
           stock app. Mitigated by `check_backend_selection()`'s WARNING log
           and `varco diagnose` (Plan 047 / 052), both of which still fire.
        Rejected — contribute it by default (Plan 054's first draft): every
        stock 4.0 app on in-memory seams refused a production start.
        Rejected — a ``SecurityPostureModule(check_backends=True)`` flag: a
        second spelling of what ``contribute()`` already says, on a module
        whose ``collectors=`` already means "replace the defaults".

    Args:
        container: The app's bootstrapped ``DIContainer`` — the one whose
            seam selection is inspected.

    Thread safety:  ✅ Holds only the container reference; ``collect()``
                       is a pure read of the container's bindings.
    Async safety:   ✅ Synchronous, no I/O.

    Example::

        from varco_core.di import contribute
        from varco_core.posture import PostureCollector
        from varco_fastapi.posture import DIPostureCollector

        container = DIContainer()
        contribute(container, PostureCollector, DIPostureCollector(container))
        app = create_varco_app(container)  # production + in-memory seams → refuses
    """

    name = "di"

    def __init__(self, container: Any) -> None:
        self._container = container

    def collect(self) -> list[PostureFinding]:
        """
        Return one ``di.in_process_backend`` finding per in-process seam.

        Returns:
            Zero or more WARN findings (``_collect_di``'s contract).
        """
        return _collect_di(self._container)


class RedactionPostureCollector(PostureCollector):
    """
    `PostureCollector` wrapper over `_collect_redaction` (Plan 057 / RO-5,
    Plan 051 §D-posture) — a **default** collector, contributed by
    `SecurityPostureModule`.

    Since 4.0 every ``redaction.*`` finding means "opted out" of a default
    (FLIP-040-1/-2), so a stock app contributes this collector and emits
    no finding; ``redaction.error_params.disabled`` (WARN) fires only when
    the app set ``VARCO_ERROR_REDACT_PARAMS=false``, and
    ``redaction.audit.disabled`` (WARN) only for a listed audited service
    that set ``_audit_redactor = None``.

    DESIGN: ``service_classes`` is explicit input (`SecurityPostureModule(
    audited_services=...)`), never discovered
        ✅ 036's rule — posture never walks ``AuditLogMixin.__subclasses__()``:
           a subclass imported by a test, a plugin, or a dead module would be
           reported as if the app ran it.
        ❌ An audited service the app forgets to list is not inspected — the
           finding is absent, not wrong. Accepted; the docs say so.

    Args:
        service_classes: ``AuditLogMixin`` subclasses to inspect.
        envelope_settings: The ``ErrorEnvelopeSettings`` in force; ``None``
            lets the inspector construct a fresh, env-driven one.

    Thread safety:  ✅ Holds only immutable configuration.
    Async safety:   ✅ Synchronous, no I/O.

    Example::

        SecurityPostureModule(audited_services=(OrderService,))
    """

    name = "redaction"

    def __init__(
        self,
        *,
        service_classes: Sequence[type] = (),
        envelope_settings: Any | None = None,
    ) -> None:
        self._service_classes = tuple(service_classes)
        self._envelope_settings = envelope_settings

    def collect(self) -> list[PostureFinding]:
        """
        Return the ``redaction.*`` findings for the configured inputs.

        Returns:
            `_collect_redaction`'s findings, unchanged.
        """
        return _collect_redaction(
            service_classes=self._service_classes,
            envelope_settings=self._envelope_settings,
        )
