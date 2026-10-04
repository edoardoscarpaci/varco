"""
Plan 036 (S9b) / Phase 3, Step 22 — red-mode tests for the five posture
collectors (§D-S9-checks, §D-S9-degrade).

Depends on ``varco_fastapi.posture`` (Phase 2) plus the four sibling
inspectors, all of which already ship: ``varco_core.tenancy.
inspect_tenant_provenance``, ``varco_core.revocation.
inspect_revocation_posture`` + ``varco_fastapi.auth.posture.
inspect_auth_posture``, ``varco_fastapi.middleware.introspect.
inspect_http_edge``, ``varco_sa.tenancy.rls_check.inspect_rls_posture``.

Every test imports ``varco_fastapi.posture``'s collector functions, which
do not exist yet — expected failure mode is ``ModuleNotFoundError`` /
``ImportError`` / ``AttributeError`` on the first missing name.
"""

from __future__ import annotations

import builtins

import pytest
from providify import DIContainer, Provider, Singleton
from varco_core.auth.authorizer import BaseAuthorizer
from varco_core.auth.base import AbstractAuthorizer
from varco_fastapi.posture import (
    LocalPostureCollector,
    PostureSeverity,
    SecurityPostureLifecycle,
    SecurityPostureSettings,
    _collect_auth,
)


def _block_import(monkeypatch: pytest.MonkeyPatch, *module_prefixes: str) -> None:
    """Monkeypatch builtins.__import__ so importing any of the given module
    prefixes raises ImportError — simulates the sibling package being
    absent, per §D-S9-degrade."""
    real_import = builtins.__import__

    def _fake_import(name, globals=None, locals=None, fromlist=(), level=0):  # noqa: A002
        if any(name == p or name.startswith(p + ".") for p in module_prefixes):
            raise ImportError(f"simulated absence of {name!r}")
        return real_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", _fake_import)


async def test_report_has_four_not_assessed_when_every_sibling_absent(monkeypatch) -> None:
    """§D-S9-degrade: with varco_core.tenancy / varco_core.revocation /
    varco_fastapi.auth.posture / varco_fastapi.middleware.introspect /
    varco_sa all unimportable, the report must contain NOT_ASSESSED
    entries for each collector — never a silently clean report — and
    start() must still return normally."""
    from varco_fastapi.posture import (
        PostureSeverity,
        SecurityPostureLifecycle,
        _collect_auth,
        _collect_data,
        _collect_http,
        _collect_tenant,
    )

    _block_import(
        monkeypatch,
        "varco_core.tenancy",
        "varco_core.revocation",
        "varco_fastapi.auth.posture",
        "varco_fastapi.middleware.introspect",
        "varco_sa",
    )

    lifecycle = SecurityPostureLifecycle(
        collectors=[_collect_tenant, _collect_auth, _collect_http, _collect_data]
    )

    # Must not raise even though every collector's import fails.
    await lifecycle.start()

    report = lifecycle.report
    not_assessed = [f for f in report.findings if f.severity == PostureSeverity.NOT_ASSESSED]
    assert len(not_assessed) >= 4
    assert len(report.not_assessed) >= 4


async def test_collector_raising_non_import_exception_yields_type_name_only() -> None:
    """A collector raising something other than ImportError must yield
    exactly one NOT_ASSESSED finding whose detail carries ONLY the
    exception's type name — never str(exc), per §D-S9-degrade and
    CLAUDE.md's error_params() rule."""
    from varco_fastapi.posture import (
        PostureSeverity,
        _wrap_collector,
    )

    secret_detail = "super secret internal path /etc/shadow leaked here"

    def _broken_collector():
        raise RuntimeError(secret_detail)

    wrapped = _wrap_collector(_broken_collector, name="broken")
    findings = wrapped()

    assert len(findings) == 1
    finding = findings[0]
    assert finding.severity == PostureSeverity.NOT_ASSESSED
    assert "RuntimeError" in finding.detail
    assert secret_detail not in finding.detail


async def test_full_report_against_worst_case_app_has_expected_local_ids(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The local collector must report the two admin-mount findings and
    the base-authorizer finding for a deliberately worst-case app: no
    posture lifecycle configured, BaseAuthorizer bound, nothing mounted
    with auth."""
    # FLIP-5 (Plan 053): a class-keyed bind of BaseAuthorizer is gated on
    # VARCO_SECURITY_ENV=development, exactly like the scanned fallback.
    monkeypatch.setenv("VARCO_SECURITY_ENV", "development")
    from providify import DIContainer
    from varco_core.auth.authorizer import BaseAuthorizer
    from varco_core.auth.base import AbstractAuthorizer
    from varco_fastapi.posture import _collect_local

    container = DIContainer()
    container.bind(AbstractAuthorizer, BaseAuthorizer)

    findings = _collect_local(container=container)
    ids = {f.check for f in findings}

    assert "posture.base_authorizer_bound" in ids


# ── Plan 053 / FLIP-5 / FLIP-034-3 / FLIP-034-4 posture findings ─────────────────────────


_ENV = "VARCO_SECURITY_ENV"


def _scanned() -> DIContainer:
    container = DIContainer()
    container.scan("varco_core.auth.authorizer")
    return container


def _authorizer_findings(container):
    return [
        f for f in LocalPostureCollector(container=container).collect() if "authorizer" in f.check
    ]


async def test_development_reports_base_authorizer_bound_at_warn(monkeypatch):
    # Control: pre-demotion severity through the collector directly.
    monkeypatch.setenv(_ENV, "development")
    findings = _authorizer_findings(_scanned())
    assert [(f.check, f.severity) for f in findings] == [
        ("posture.base_authorizer_bound", PostureSeverity.WARN)
    ]


async def test_production_without_authorizer_is_unbound_high(monkeypatch):
    monkeypatch.delenv(_ENV, raising=False)
    findings = _authorizer_findings(_scanned())
    assert len(findings) == 1
    finding = findings[0]
    assert finding.check == "posture.authorizer_unbound"
    assert finding.severity is PostureSeverity.HIGH
    assert "VARCO_SECURITY_ENV=development" in finding.remediation
    assert "AbstractAuthorizer" in finding.remediation


@pytest.mark.parametrize("environment", ["production", "development"])
async def test_unbound_refuses_under_refuse_in_any_environment(monkeypatch, environment):
    # HIGH is never demoted (posture.py demotes WARN only).
    monkeypatch.delenv(_ENV, raising=False)
    lifecycle = SecurityPostureLifecycle(
        collectors=[LocalPostureCollector(container=_scanned()).collect],
        settings=SecurityPostureSettings(enforce="refuse", environment=environment),
    )
    with pytest.raises(RuntimeError, match="posture.authorizer_unbound"):
        await lifecycle.start()


async def test_explicit_instance_binding_in_production_is_warn(monkeypatch):
    monkeypatch.delenv(_ENV, raising=False)
    container = DIContainer()

    def _make() -> AbstractAuthorizer:
        return BaseAuthorizer()

    container.provide(Provider(singleton=True)(_make), returns=AbstractAuthorizer)
    findings = _authorizer_findings(container)
    assert [(f.check, f.severity) for f in findings] == [
        ("posture.base_authorizer_bound", PostureSeverity.WARN)
    ]
    assert "VARCO_SECURITY_SUPPRESS" in findings[0].remediation


async def test_construction_failure_is_still_not_assessed(monkeypatch):
    # Control: a non-LookupError construction failure stays NOT_ASSESSED.
    monkeypatch.delenv(_ENV, raising=False)

    @Singleton
    class Broken(AbstractAuthorizer):
        def __init__(self) -> None:
            raise RuntimeError("boom")

        async def authorize(self, ctx, action, resource) -> None:  # pragma: no cover
            return None

    container = DIContainer()
    container.bind(AbstractAuthorizer, Broken)
    checks = [f.check for f in _authorizer_findings(container)]
    assert checks == ["posture.not_assessed.base_authorizer_bound"]


# ── FLIP-034-3 ────────────────────────────────────────────────────────────────


def _registry(store):
    from varco_core.authority.registry import TrustedIssuerRegistry

    return TrustedIssuerRegistry(revocation_store=store)


async def test_jti_not_required_is_info_when_real_store_wired_and_env_false(monkeypatch):
    from varco_core.revocation.memory import InMemoryTokenRevocationStore

    monkeypatch.setenv("VARCO_JWT_REVOCATION_REQUIRE_JTI", "false")
    findings = _collect_auth(registry=_registry(InMemoryTokenRevocationStore()))
    hits = [f for f in findings if f.check == "auth.revocation_jti_not_required"]
    assert len(hits) == 1
    assert hits[0].severity is PostureSeverity.INFO


async def test_jti_not_required_absent_when_derived(monkeypatch):
    # Control-ish: absent today too; must stay absent after the flip.
    from varco_core.revocation.memory import InMemoryTokenRevocationStore

    monkeypatch.delenv("VARCO_JWT_REVOCATION_REQUIRE_JTI", raising=False)
    findings = _collect_auth(registry=_registry(InMemoryTokenRevocationStore()))
    assert "auth.revocation_jti_not_required" not in {f.check for f in findings}


async def test_jti_not_required_absent_with_null_store(monkeypatch):
    # Control: Null store is not "registry_wired".
    from varco_core.revocation.null import NullTokenRevocationStore

    monkeypatch.setenv("VARCO_JWT_REVOCATION_REQUIRE_JTI", "false")
    findings = _collect_auth(registry=_registry(NullTokenRevocationStore()))
    assert "auth.revocation_jti_not_required" not in {f.check for f in findings}


# ── FLIP-034-4 ────────────────────────────────────────────────────────────────


async def test_acknowledged_passthrough_is_still_warn():
    from varco_fastapi.auth.server_auth import PassthroughAuth

    findings = _collect_auth(auth=PassthroughAuth(acknowledge_unverified=True))
    hits = [f for f in findings if f.check == "auth.passthrough_bound"]
    assert [f.severity for f in hits] == [PostureSeverity.WARN]


# ── Plan 055 §D-collector — tenant findings homed in TenancyModule ───────────


def _jwt_chain():
    from varco_core.tenancy.source import TenantSourceChain
    from varco_core.tenancy.sources import JwtClaimTenantSource

    return TenantSourceChain(sources=(JwtClaimTenantSource(),))


async def test_no_tenant_findings_without_tenancy_module(monkeypatch) -> None:
    # Premise (4): a stock app with no tenancy must emit no tenant.* finding,
    # or 054's derived refuse would block every non-tenant production app.
    from varco_fastapi.app import create_varco_app
    from varco_fastapi.modules import SecurityPostureModule
    from varco_fastapi.modules.defaults import default_modules

    monkeypatch.delenv("VARCO_TENANT_SOURCES", raising=False)
    posture = SecurityPostureModule()
    app = create_varco_app(
        DIContainer(),
        modules=[*default_modules(exclude=(SecurityPostureModule,)), posture],
        routers=[],
        validate=False,
    )

    async with app.router.lifespan_context(app):
        ids = {f.check for f in posture.report.findings}

    assert not {i for i in ids if i.startswith("tenant.")}, ids


async def test_tenancy_module_contributes_tenant_collector() -> None:
    from fastapi import FastAPI
    from varco_core.di.collect import collect
    from varco_core.posture import PostureCollector
    from varco_fastapi.modules import TenancyModule
    from varco_fastapi.posture import TenantPostureCollector

    container = DIContainer()
    module = TenancyModule(chain=_jwt_chain())
    module.middleware(container)
    module.configure_app(FastAPI(), container)

    collectors = collect(container, PostureCollector)  # type: ignore[type-abstract]

    assert any(isinstance(c, TenantPostureCollector) for c in collectors)


async def test_every_tenant_token_has_a_severity_row() -> None:
    # The "Unrecognised token" INFO default must be reachable only by a
    # genuinely new token — every shipped token gets a real row.
    import re
    from pathlib import Path

    import varco_core.tenancy.posture as tenancy_posture
    from varco_fastapi.posture import _tenant_finding_for_token

    source = Path(tenancy_posture.__file__).read_text(encoding="utf-8")
    tokens = set(re.findall(r'"(tenant\.[a-z_]+)"', source))
    tokens |= {"tenant.third_party_source_high_trust", "tenant.no_catalog_gate"}
    assert "tenant.single_source" in tokens  # sanity: the scan found real tokens

    unmapped = {t for t in tokens if _tenant_finding_for_token(t).detail.startswith("Unrecognised")}

    assert unmapped == set()


@pytest.mark.parametrize(
    ("token", "severity"),
    [
        ("tenant.legacy_source_explicit", PostureSeverity.WARN),
        ("tenant.single_source", PostureSeverity.INFO),
        ("tenant.subdomain_trusts_forwarded_host", PostureSeverity.WARN),
        ("tenant.no_catalog_gate", PostureSeverity.INFO),
        ("tenant.no_chain", PostureSeverity.WARN),
        ("tenant.legacy_source_implicit", PostureSeverity.WARN),
        ("tenant.membership_missing_claim_allows", PostureSeverity.HIGH),
    ],
)
async def test_tenant_severity_rows(token, severity) -> None:
    from varco_fastapi.posture import _tenant_finding_for_token

    finding = _tenant_finding_for_token(token)

    assert finding.severity is severity
    assert not finding.detail.startswith("Unrecognised")


async def test_null_membership_counts_as_no_provider() -> None:
    # Narrowing #2: NullTenantMembership always allows — it is not a check.
    from varco_core.tenancy.membership import NullTenantMembership
    from varco_fastapi.posture import TenantPostureCollector

    collector = TenantPostureCollector(
        chain=_jwt_chain(),
        membership=NullTenantMembership(),
        delegation=None,
        catalog_gated=True,
        chain_implicit=False,
    )

    ids = {f.check for f in collector.collect()}

    assert "tenant.no_membership_provider" in ids


async def test_collector_reports_no_catalog_gate_when_ungated() -> None:
    from varco_core.tenancy.membership import ClaimTenantMembership, MissingClaimPolicy
    from varco_fastapi.posture import TenantPostureCollector

    membership = ClaimTenantMembership(on_missing_claim=MissingClaimPolicy.DENY)
    gated = TenantPostureCollector(
        chain=_jwt_chain(), membership=membership, catalog_gated=True, chain_implicit=False
    )
    ungated = TenantPostureCollector(
        chain=_jwt_chain(), membership=membership, catalog_gated=False, chain_implicit=False
    )

    assert "tenant.no_catalog_gate" not in {f.check for f in gated.collect()}
    finding = next(f for f in ungated.collect() if f.check == "tenant.no_catalog_gate")
    assert finding.severity is PostureSeverity.INFO


async def test_collector_never_reports_unchained_setter_by_default() -> None:
    # FLIP-9: varco's own middleware cannot set the tenant any more.
    from varco_fastapi.posture import TenantPostureCollector

    collector = TenantPostureCollector(chain=_jwt_chain(), membership=None, delegation=None)

    assert "tenant.unchained_claim_tenant_setter" not in {f.check for f in collector.collect()}


# ════════════════════════════════════════════════════════════════════════════════
# Plan 056 / FLIP-13, Step 12 — posture.webhook_secrets_plaintext detects the
# PlaintextFieldEncryptor hatch (and an encryptor-less out-of-tree repo), but
# never the in-memory repository (§D-flip13-posture / §D-flip13-inmemory).
# ════════════════════════════════════════════════════════════════════════════════

_WEBHOOK_ID = "posture.webhook_secrets_plaintext"


class _RepoStub:
    def __init__(self, encryptor: object | None) -> None:
        self._encryptor = encryptor


class _FernetShaped:
    def encrypt(self, plaintext: bytes, *, context: str | None = None) -> bytes:
        return plaintext[::-1]

    def decrypt(self, ciphertext: bytes, *, context: str | None = None) -> bytes:
        return ciphertext[::-1]


def _plaintext_encryptor():
    from varco_core.encryption import PlaintextFieldEncryptor

    return PlaintextFieldEncryptor(acknowledge_plaintext=True)


def _webhook_findings(repo: object) -> list:
    return [
        f
        for f in LocalPostureCollector(webhook_repository=repo).collect()
        if f.check == _WEBHOOK_ID
    ]


async def test_plaintext_encryptor_hatch_is_webhook_secrets_plaintext_warn() -> None:
    (finding,) = _webhook_findings(_RepoStub(_plaintext_encryptor()))

    assert finding.severity is PostureSeverity.WARN
    assert "PlaintextFieldEncryptor" in finding.detail


async def test_out_of_tree_repo_without_encryptor_still_fires() -> None:
    assert len(_webhook_findings(_RepoStub(None))) == 1


async def test_real_encryptor_does_not_fire() -> None:
    assert _webhook_findings(_RepoStub(_FernetShaped())) == []


async def test_in_memory_webhook_repository_is_not_a_finding() -> None:
    # Secrets never at rest: "encryption at rest" has no referent here.
    from varco_core.webhook.base import InMemoryWebhookSubscriptionRepository

    assert _webhook_findings(InMemoryWebhookSubscriptionRepository()) == []


async def test_plaintext_hatch_refuses_production_start(monkeypatch) -> None:
    for name in ("VARCO_SECURITY_ENFORCE", "VARCO_SECURITY_ENV", "VARCO_SECURITY_SUPPRESS"):
        monkeypatch.delenv(name, raising=False)
    lifecycle = SecurityPostureLifecycle(
        collectors=[LocalPostureCollector(webhook_repository=_RepoStub(_plaintext_encryptor()))],
        settings=SecurityPostureSettings(),
    )

    with pytest.raises(RuntimeError, match=_WEBHOOK_ID):
        await lifecycle.start()


async def test_suppressed_plaintext_hatch_does_not_block(monkeypatch) -> None:
    for name in ("VARCO_SECURITY_ENFORCE", "VARCO_SECURITY_ENV"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("VARCO_SECURITY_SUPPRESS", _WEBHOOK_ID)
    lifecycle = SecurityPostureLifecycle(
        collectors=[LocalPostureCollector(webhook_repository=_RepoStub(_plaintext_encryptor()))],
        settings=SecurityPostureSettings(),
    )

    try:
        await lifecycle.start()
    except RuntimeError as exc:  # an unrelated default finding may block; this id must not
        assert _WEBHOOK_ID not in str(exc)
