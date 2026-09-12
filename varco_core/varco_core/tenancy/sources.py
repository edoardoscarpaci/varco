"""
varco_core.tenancy.sources
============================
The three shipped ``TenantSource`` implementations (Plan 033 / S6b,
§D-S6-oq3): ``JwtClaimTenantSource``, ``SubdomainTenantSource``,
``LegacyTenantSource``.

DESIGN: explicit base domains over a bundled public-suffix list (§D-S6-oq3)
    ✅ CLAUDE.md's zero-new-runtime-dependencies rule for ``varco_core`` is
       decisive on its own.
    ✅ A PSL is a data file that goes stale; explicit config cannot, because
       the operator's own DNS is the thing it describes.
    ✅ It removes the class of bug rather than implementing it carefully —
       with ``base_domains`` there is no "where does the registrable domain
       end" computation anywhere in the code path.
    ✅ The single-label rule (step 4 of the algorithm below) closes the
       sneaky multi-label-prefix variant: ``victim.attacker.example.com``
       against ``base_domains=("example.com",)`` yields nothing, not
       ``"victim"`` and not ``"victim.attacker"``.
    ❌ An operator running two TLD variants of the same brand must list
       both base domains. Documented; it is a two-element tuple.
    ❌ ``base_domains=("co.uk",)`` yields tenant ``"example"`` for
       ``example.co.uk`` — varco cannot detect this and does not try
       (a documented Pitfalls row, pinned by test).
    Rejected — ``publicsuffix2``/``tldextract``: ❌ a runtime dependency
    with a stale-data failure mode whose symptom is tenant confusion;
    ``tldextract`` additionally does network refreshes by default.
    Rejected — "strip the first label" with no base domain: ❌ silent
    tenant-confusion bugs (``api.example.com`` → ``"api"``).

Thread safety:  ✅ All three sources are stateless beyond immutable
    configuration set at construction.
Async safety:   ✅ No I/O — pure string/mapping work.
"""

from __future__ import annotations

import asyncio
import logging
import threading
from collections.abc import Coroutine, Sequence
from typing import Any, ClassVar, Final

from varco_core.auth.delegation import ActorContext, DelegationPolicy, DelegationRecord
from varco_core.context.ambient import AmbientVar
from varco_core.tenancy.source import TenantClaim, TenantRequest, TenantSource, TenantTrust

logger = logging.getLogger(__name__)

__all__ = [
    "JwtClaimTenantSource",
    "SubdomainTenantSource",
    "LegacyTenantSource",
    "ActAsTenantSource",
]

_DEFAULT_RESERVED_LABELS: frozenset[str] = frozenset(
    {"www", "api", "app", "admin", "static", "cdn"}
)

# DESIGN: ActAsTenantSource's last-resolved DelegationRecord is request-scoped
# ambient state, not instance state.
# ✅ A TenantSource instance (like every other shipped source) is shared
#    across concurrent requests — the chain and its sources are constructed
#    once, at app-startup time, and `resolve()` is called once per request
#    on the same objects. A `self.last_record` attribute would let two
#    in-flight delegated requests race: request A's DelegationRecord could
#    be overwritten by request B's before A's caller (TenantResolutionMiddleware)
#    reads it back — a cross-request delegation-identity leak, precisely the
#    CVE-2025-55241 class the mandatory audit exists to prevent.
# ✅ `AmbientVar` (`varco_core.context.ambient`, already used by
#    `varco_core.tenancy.provenance`) is task-local via `ContextVar` — each
#    request's own asyncio task sees only the value it wrote.
# ✅ The write happens on the CALLING thread, after `_run_coroutine_sync`
#    returns: `_run_coroutine_sync` only computes and returns the `allowed`
#    boolean — `record = DelegationRecord(...)` and the `AmbientVar.set_for_task()`
#    call both happen back in `resolve()`, on whatever thread/task called it
#    (the request's own task, since `TenantSourceChain.resolve()` is called
#    synchronously from `TenantResolutionMiddleware.dispatch()`). The
#    worker thread `_run_coroutine_sync` may spawn never touches this var.
# ❌ Two `ActAsTenantSource` instances resolving within the very same
#    request would share one ambient slot (the second overwrites the
#    first). Accepted — a chain with more than one `act_as` source is not a
#    supported configuration.
_last_delegation_record: Final[AmbientVar[DelegationRecord | None]] = AmbientVar(
    "varco_act_as_last_delegation_record"
)


class JwtClaimTenantSource(TenantSource):
    """
    Reads the tenant id from an already-**verified** ``AuthContext``.

    This is the ``TenantTrust.HIGHEST`` source — its trust is anchored in
    the token's signature, verified before this source ever runs
    (``TenantRequest.auth`` is documented as pre-verified).

    Args:
        metadata_key: The ``AuthContext.metadata`` key to read. Defaults to
            ``"tenant_id"`` — the key ``varco_core.jwt.parser`` already
            populates.

    Edge cases:
        - ``request.auth is None`` → ``None``.
        - The claim value is present but not a ``str`` → ``None``, **never**
          coerced with ``str()`` (a list value would otherwise produce a
          plausible-looking but wrong tenant id).
    """

    name: ClassVar[str] = "jwt"
    trust: ClassVar[TenantTrust] = TenantTrust.HIGHEST

    def __init__(self, *, metadata_key: str = "tenant_id") -> None:
        self.metadata_key = metadata_key

    def resolve(self, request: TenantRequest) -> TenantClaim | None:
        if request.auth is None:
            return None
        metadata = getattr(request.auth, "metadata", None)
        if not metadata:
            return None
        value = metadata.get(self.metadata_key)
        if not isinstance(value, str) or not value:
            return None
        return TenantClaim(tenant_id=value, source=self.name, trust=self.trust)


class SubdomainTenantSource(TenantSource):
    """
    Reads the tenant id from a subdomain of the connection's own ``Host``.

    ⛔ SubdomainTenantSource is not by itself a security control; deploy it
    behind TrustedHostMiddleware or an edge that rejects unknown Host
    values. Its security value is that it disagrees with a forged JWT
    claim, and that a forged host disagrees with a real one.

    Resolution algorithm, in order:
        1. Take ``request.host`` (the connection's own ``Host``). Read
           ``forwarded_host_header`` **only** when ``trust_forwarded_host=True``,
           and then emit ``TenantTrust.MEDIUM`` rather than ``HIGH``.
        2. Normalise: lower-case, strip the port, strip one trailing dot,
           then ``label.encode("idna")`` per label using the stdlib codec.
           Any ``UnicodeError`` → return ``None``, never raise.
        3. Exact suffix match against the configured base domains, longest
           first, so ``eu.example.com`` wins over ``example.com`` for
           ``acme.eu.example.com``. The host must equal
           ``f"{label}.{base}"`` for exactly one base.
        4. ``label`` must be a **single DNS label** — if it still contains a
           dot (``a.b.example.com``), return ``None``. Never join, never
           take the leftmost.
        5. ``label`` in ``reserved_labels``, or the bare base domain itself,
           → ``None`` (not an error).

    Args:
        base_domains: Required, non-empty. The registrable domains this
            source is allowed to route under.
        trust_forwarded_host: Read ``forwarded_host_header`` instead of
            ``request.host``. Never ``True`` by default — see the
            docstring's security statement above.
        forwarded_host_header: Header name to read when
            ``trust_forwarded_host=True``. Defaults to ``X-Forwarded-Host``.
        reserved_labels: Subdomain labels that are never a tenant.

    Raises:
        ValueError: ``base_domains`` is empty.

    Edge cases:
        - ``base_domains=("co.uk",)`` yields tenant ``"example"`` for
          ``example.co.uk`` — this is documented operator error varco
          cannot detect, and the behaviour is pinned by test.
        - An empty or >63-char DNS label makes ``str.encode("idna")`` raise
          ``UnicodeError``, caught here → ``None``.
    """

    name: ClassVar[str] = "subdomain"
    trust: ClassVar[TenantTrust] = TenantTrust.HIGH

    def __init__(
        self,
        *,
        base_domains: Sequence[str],
        trust_forwarded_host: bool = False,
        forwarded_host_header: str = "X-Forwarded-Host",
        reserved_labels: frozenset[str] = _DEFAULT_RESERVED_LABELS,
    ) -> None:
        if not base_domains:
            raise ValueError("SubdomainTenantSource requires at least one base domain.")
        # Longest-first, so a more specific base domain (eu.example.com)
        # always wins over a shorter one (example.com) for the same host.
        self.base_domains: tuple[str, ...] = tuple(
            sorted((d.lower().rstrip(".") for d in base_domains), key=len, reverse=True)
        )
        self.trust_forwarded_host = trust_forwarded_host
        self.forwarded_host_header = forwarded_host_header.lower()
        self.reserved_labels = reserved_labels

    def resolve(self, request: TenantRequest) -> TenantClaim | None:
        if self.trust_forwarded_host:
            raw_host = request.headers.get(self.forwarded_host_header)
            trust = TenantTrust.MEDIUM
        else:
            raw_host = request.host
            trust = TenantTrust.HIGH

        if not raw_host:
            return None

        normalised = self._normalise(raw_host)
        if normalised is None:
            return None

        for base in self.base_domains:
            suffix = f".{base}"
            if normalised == base:
                return None  # the bare base domain itself is never a tenant
            if normalised.endswith(suffix):
                label = normalised[: -len(suffix)]
                if "." in label:
                    return None  # multi-label prefix — never join, never leftmost
                if not label or label in self.reserved_labels:
                    return None
                return TenantClaim(tenant_id=label, source=self.name, trust=trust)

        return None

    @staticmethod
    def _normalise(raw_host: str) -> str | None:
        host = raw_host.strip().lower()
        # Strip a port, being careful of a bracketed IPv6 literal (out of
        # scope for tenant subdomains, but never crash on one).
        if not host.startswith("["):
            host = host.rsplit(":", 1)[0]
        host = host.rstrip(".")
        if not host:
            return None
        labels = host.split(".")
        try:
            encoded = [label.encode("idna").decode("ascii") for label in labels]
        except UnicodeError:
            return None
        return ".".join(encoded)


class LegacyTenantSource(TenantSource):
    """
    Reads the tenant id from a bare, unbound HTTP header.

    §Security properties (short form — see the full statement in
    ``technical_docs/features/tenant-provenance.md``): this source binds
    nothing to the authenticated caller. Any client that can reach the
    service can claim any active tenant. It is ``TenantTrust.LOW`` — the
    ❌ LOWEST row of brief 006 §1's trust table — and is safe only behind an
    ingress that strips a client-supplied header and re-appends a verified
    value over an authenticated channel. It exists so a deployment can
    upgrade without an outage and is removed in 4.0.

    Does **not** warn on construction (§D-S6-blast) — the warning belongs
    to ``TenantResolutionMiddleware(chain=None)``, which builds this source
    implicitly exactly once.

    Args:
        header: The header name to read. Defaults to ``X-Tenant-Id``.
    """

    name: ClassVar[str] = "legacy"
    trust: ClassVar[TenantTrust] = TenantTrust.LOW

    def __init__(self, header: str = "X-Tenant-Id") -> None:
        self.header = header.lower()

    def resolve(self, request: TenantRequest) -> TenantClaim | None:
        value = request.headers.get(self.header)
        if not value:
            return None
        return TenantClaim(tenant_id=value, source=self.name, trust=self.trust)


def _run_coroutine_sync(coro: Coroutine[Any, Any, Any]) -> Any:
    """
    Run an async coroutine to completion and return its result, from
    synchronous code — whether or not an ``asyncio`` event loop is already
    running on the calling thread.

    DESIGN: a thread-isolated event loop, rather than an async ``resolve()``
        ✅ ``TenantSource.resolve()`` must stay sync (§D-S6-abc) — this is
           the sync/async boundary crossing OQ1 anticipated.
        ✅ Works identically whether called from plain synchronous test code
           (no loop running — ``asyncio.run()`` directly) or from inside a
           real ASGI request (``TenantResolutionMiddleware.dispatch`` is
           itself a coroutine, so a loop IS already running there;
           ``asyncio.run()`` would raise). Spawning a fresh thread with its
           own new loop and joining on it works in both cases, at the cost
           of one thread hop.
        ❌ A thread hop per delegated request. Accepted — ``act_as`` traffic
           is by construction rare (§D-S16-shape).
        ❌ Requires no new dependency (``nest_asyncio`` was considered and
           rejected — CLAUDE.md's zero-new-runtime-dependency rule).

    Args:
        coro: The coroutine to run.

    Returns:
        The coroutine's result.
    """
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)

    result: dict[str, Any] = {}
    error: dict[str, BaseException] = {}

    def _runner() -> None:
        try:
            result["value"] = asyncio.run(coro)
        except BaseException as exc:  # noqa: BLE001 — re-raised on the caller's thread below
            error["exc"] = exc

    thread = threading.Thread(target=_runner, daemon=True)
    thread.start()
    thread.join()
    if "exc" in error:
        raise error["exc"]
    return result["value"]


class ActAsTenantSource(TenantSource):
    """
    Consumes an already-verified RFC 8693 ``act`` claim to let a delegated
    service act as a policy-approved tenant (Plan 033 / S16, §D-S16-shape).

    varco **consumes** an exchanged token here; it never issues one — see
    ``varco_core.auth.delegation`` for the full design and CVE-2025-55241
    motivation for the mandatory audit below.

    Emits a claim at ``TenantTrust.HIGHEST`` **only** when the token carries
    an ``act`` claim *and* the bound policy allows the requested tenant.
    Every decision — allow *and* deny — is logged at INFO with both
    principal and actor, and exposed via ``last_record`` for a caller (e.g.
    ``TenantResolutionMiddleware``) that wants to attach it to
    ``TenantProvenance.delegation``.

    Args:
        policy: The ``DelegationPolicy`` to consult, or ``None`` — deny by
            construction (§D-S16-shape: "no policy bound ⇒ no claim, ever").
        requested_from: The header naming the tenant to act as. Defaults to
            ``X-Act-As-Tenant``.

    Edge cases:
        - No ``act`` claim on the token → ``None``, no record at all — a
          bare impersonation token (``sub`` swapped, no ``act``) can never
          reach this source's policy check.
        - No policy bound → ``None``, logged, no ``DelegationRecord`` (there
          is no policy to attribute one to) — surfaced instead by
          ``inspect_tenant_provenance``'s ``tenant.delegation_unbound``
          finding.

    Thread safety: ``last_record`` is **request-scoped ambient state**, not
        instance state — backed by an ``AmbientVar`` (task-local via
        ``ContextVar``), same mechanism as
        ``varco_core.tenancy.provenance.current_tenant_provenance()``. Two
        concurrent requests calling ``resolve()`` on the very same shared
        instance each see only their own ``DelegationRecord`` — there is no
        cross-request race, and therefore no delegation-identity leak
        (CVE-2025-55241's class of bug). The record is written on the
        calling task/thread — the one thing `_run_coroutine_sync`'s worker
        thread (used only when an event loop is already running) never
        touches.
    """

    name: ClassVar[str] = "act_as"
    trust: ClassVar[TenantTrust] = TenantTrust.HIGHEST

    def __init__(
        self, policy: DelegationPolicy | None, *, requested_from: str = "X-Act-As-Tenant"
    ) -> None:
        self.policy = policy
        self.requested_from = requested_from.lower()

    @property
    def last_record(self) -> DelegationRecord | None:
        """
        The ``DelegationRecord`` produced by the most recent ``resolve()``
        call **on this request's task** — never a different, concurrently
        in-flight request's record. ``None`` before any ``resolve()`` call
        in this task, or when the most recent call had nothing to record
        (no ``act`` claim, no requested tenant header).
        """
        return _last_delegation_record.get()

    def resolve(self, request: TenantRequest) -> TenantClaim | None:
        _last_delegation_record.set_for_task(None)

        if request.auth is None:
            return None
        metadata = getattr(request.auth, "metadata", None) or {}
        actor = ActorContext.from_metadata(metadata)
        if actor is None:
            return None  # bare impersonation token — unsupported by design

        tenant_id = request.headers.get(self.requested_from)
        if not tenant_id:
            return None

        principal = getattr(request.auth, "user_id", None)

        if self.policy is None:
            # Still a full, attributable DelegationRecord — "no policy bound"
            # is a deny, not a silent pass-through, so a caller consuming
            # last_record (e.g. TenantResolutionMiddleware) rejects the
            # request instead of treating an act-as attempt with no policy
            # as if the source had nothing to say at all.
            record = DelegationRecord(
                actor=actor.subject,
                actor_chain=actor.chain,
                principal=principal,
                tenant_id=tenant_id,
                allowed=False,
                policy="unbound",
            )
            _last_delegation_record.set_for_task(record)
            logger.info(
                "Delegation denied: no DelegationPolicy bound (actor=%r, "
                "principal=%r, requested tenant_id=%r).",
                actor.subject,
                principal,
                tenant_id,
            )
            return None

        allowed = bool(_run_coroutine_sync(self.policy.allows(actor, principal, tenant_id)))

        record = DelegationRecord(
            actor=actor.subject,
            actor_chain=actor.chain,
            principal=principal,
            tenant_id=tenant_id,
            allowed=allowed,
            policy=self.policy.name,
        )
        _last_delegation_record.set_for_task(record)

        # Mandatory audit — CVE-2025-55241: an unlogged delegation decision
        # is the bug, not merely a missing feature. Logged for BOTH allow
        # and deny, exactly once per resolve() call.
        logger.info(
            "Delegation %s: actor=%r principal=%r tenant_id=%r policy=%r.",
            "allowed" if allowed else "denied",
            actor.subject,
            principal,
            tenant_id,
            self.policy.name,
        )

        if not allowed:
            return None
        return TenantClaim(tenant_id=tenant_id, source=self.name, trust=self.trust)
