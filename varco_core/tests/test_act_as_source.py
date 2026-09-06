"""
Failing-first tests for ActAsTenantSource (Plan 033, Phase 6, Step 39 —
DROPPABLE per §D-S16-cut) — §D-S16-shape.
"""

from __future__ import annotations

import logging
import threading


def _smod():
    from varco_core.tenancy import source

    return source


def _sources_mod():
    from varco_core.tenancy import sources

    return sources


def _delegation_mod():
    from varco_core.auth import delegation

    return delegation


def _auth_ctx(**metadata):
    from varco_core.auth.base import AuthContext

    return AuthContext(user_id="svc-billing", metadata=metadata)


class _AllowPolicy:
    name = "allow-all-test-policy"

    async def allows(self, actor, principal, tenant_id):
        return True


class _DenyPolicy:
    name = "deny-all-test-policy"

    async def allows(self, actor, principal, tenant_id):
        return False


class TestActAsTenantSourceNoActClaim:
    def test_no_act_claim_returns_none_even_with_header_present(self) -> None:
        smod, srcs = _smod(), _sources_mod()
        s = srcs.ActAsTenantSource(policy=_AllowPolicy())
        req = smod.TenantRequest(
            headers={"x-act-as-tenant": "acme"},
            auth=_auth_ctx(),  # no "actor" claim -> bare impersonation, unsupported
        )

        assert s.resolve(req) is None


class TestActAsTenantSourcePolicyAllows:
    def test_act_present_policy_allows_resolves_at_highest(self) -> None:
        smod, srcs = _smod(), _sources_mod()
        s = srcs.ActAsTenantSource(policy=_AllowPolicy())
        req = smod.TenantRequest(
            headers={"x-act-as-tenant": "acme"},
            auth=_auth_ctx(actor={"sub": "svc-billing"}),
        )

        claim = s.resolve(req)
        assert claim is not None
        assert claim.tenant_id == "acme"
        assert claim.trust is smod.TenantTrust.HIGHEST


class TestActAsTenantSourcePolicyDenies:
    def test_act_present_policy_denies_returns_none_with_record(self) -> None:
        smod, srcs = _smod(), _sources_mod()
        s = srcs.ActAsTenantSource(policy=_DenyPolicy())
        req = smod.TenantRequest(
            headers={"x-act-as-tenant": "acme"},
            auth=_auth_ctx(actor={"sub": "svc-billing"}),
        )

        claim = s.resolve(req)
        assert claim is None

        # Every decision (allow AND deny) must emit a DelegationRecord
        # (§D-S16-shape) — exposed via the source's last-resolved-record
        # attribute, since resolve() itself returns TenantClaim | None.
        record = s.last_record
        assert record is not None
        assert record.allowed is False


class TestActAsTenantSourceNoPolicyBound:
    def test_no_policy_returns_none_delegation_unbound(self) -> None:
        smod, srcs = _smod(), _sources_mod()
        s = srcs.ActAsTenantSource(policy=None)
        req = smod.TenantRequest(
            headers={"x-act-as-tenant": "acme"},
            auth=_auth_ctx(actor={"sub": "svc-billing"}),
        )

        assert s.resolve(req) is None


class _AllowAllPolicy:
    """A policy that always allows — the race below is about identity
    leakage between concurrent requests, not about allow/deny logic."""

    name = "concurrent-allow-all-test-policy"

    async def allows(self, actor, principal, tenant_id):
        return True


class TestActAsTenantSourceConcurrencyRace:
    """
    §D-S16-shape's mandatory audit exists specifically to prevent a
    cross-request delegation-identity leak (CVE-2025-55241's class of bug).
    A `last_record` implemented as plain instance state on a `TenantSource`
    — which, like every other shipped source, is constructed once and
    shared across every request the chain ever resolves — reopens exactly
    that leak: two in-flight delegated requests racing on the SAME
    `ActAsTenantSource` instance could have one request observe the
    OTHER's `DelegationRecord`.

    This drives two genuinely concurrent `resolve()` calls, on two real OS
    threads, against one shared instance, and forces both threads to finish
    writing before either one reads back via a `threading.Barrier` — the
    exact interleaving an instance-attribute implementation gets wrong
    (whichever thread wrote last "wins" for BOTH readers) and a
    request-scoped `ContextVar`-backed implementation gets right (each
    thread's default context is independent, so each sees only what it
    itself wrote).
    """

    def test_concurrent_resolves_on_shared_instance_do_not_leak_records(self) -> None:
        smod, srcs = _smod(), _sources_mod()

        shared_source = srcs.ActAsTenantSource(policy=_AllowAllPolicy())
        barrier = threading.Barrier(2, timeout=5)
        results: dict[str, object] = {}
        errors: list[BaseException] = []

        def _worker(actor_sub: str, tenant_id: str) -> None:
            try:
                req = smod.TenantRequest(
                    headers={"x-act-as-tenant": tenant_id},
                    auth=_auth_ctx(actor={"sub": actor_sub}),
                )
                shared_source.resolve(req)
                # Force both threads to have completed their OWN resolve()
                # (and therefore their own write) before EITHER thread reads
                # back — this is what actually exercises the race: an
                # instance-attribute implementation has exactly one shared
                # slot, so after this barrier it holds whichever thread's
                # write happened to land last, regardless of which thread
                # is doing the reading.
                barrier.wait()
                results[actor_sub] = shared_source.last_record
            except BaseException as exc:  # noqa: BLE001 — re-raised on the main thread below
                errors.append(exc)
                barrier.abort()

        t1 = threading.Thread(target=_worker, args=("svc-a", "acme"))
        t2 = threading.Thread(target=_worker, args=("svc-b", "beta"))
        t1.start()
        t2.start()
        t1.join(timeout=5)
        t2.join(timeout=5)

        assert not errors, errors

        record_a = results["svc-a"]
        record_b = results["svc-b"]

        assert record_a is not None
        assert record_b is not None
        # Each thread must observe ONLY the DelegationRecord it itself
        # produced — never the other (concurrent) request's actor/tenant.
        assert record_a.actor == "svc-a"
        assert record_a.tenant_id == "acme"
        assert record_b.actor == "svc-b"
        assert record_b.tenant_id == "beta"


class TestActAsTenantSourceMandatoryAudit:
    def test_every_branch_logs_one_info_record_with_principal_and_actor(self, caplog) -> None:
        smod, srcs = _smod(), _sources_mod()
        for policy in (_AllowPolicy(), _DenyPolicy()):
            caplog.clear()
            s = srcs.ActAsTenantSource(policy=policy)
            req = smod.TenantRequest(
                headers={"x-act-as-tenant": "acme"},
                auth=_auth_ctx(actor={"sub": "svc-billing"}),
            )
            with caplog.at_level(logging.INFO):
                s.resolve(req)

            info_records = [r for r in caplog.records if r.levelno == logging.INFO]
            assert len(info_records) == 1
            message = info_records[0].getMessage()
            assert "svc-billing" in message  # actor
            assert "svc-billing" in message  # principal (same subject here)
