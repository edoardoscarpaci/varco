"""
varco_fastapi.auth.server_auth
==============================
Server-side authentication hierarchy for FastAPI.

Each ``AbstractServerAuth`` subclass is a FastAPI-callable dependency that
extracts credentials from the incoming ``Request`` and returns an ``AuthContext``.
Inject it into route handlers via ``Depends()``, or into ``RequestContextMiddleware``
which sets the auth context for the entire request scope.

Hierarchy::

    AbstractServerAuth (ABC)
      ├── JwtBearerAuth          — verify Bearer JWT via TrustedIssuerRegistry
      ├── ApiKeyAuth             — verify X-API-Key header (query param opt-in via param=)
      ├── PassthroughAuth        — decode JWT claims WITHOUT verifying signature
      ├── AnonymousAuth          — always returns anonymous AuthContext
      ├── CompositeServerAuth    — try each strategy in order; first success wins
      └── WebSocketAuth          — extract token from WS upgrade (header, protocol, query)

FastAPI usage::

    auth = JwtBearerAuth(registry=my_registry)

    @app.get("/orders/")
    async def list_orders(ctx: AuthContext = Depends(auth)):
        ...

    # Or via RequestContextMiddleware (sets context for ALL routes):
    app.add_middleware(RequestContextMiddleware, server_auth=auth)

DESIGN: Callable dependency (``__call__``) over FastAPI ``Security()``
    ✅ No FastAPI-specific import needed in route handlers — just ``Depends(auth)``
    ✅ ``AbstractServerAuth`` is a plain ABC; testable without FastAPI
    ✅ Composable via ``CompositeServerAuth`` without framework coupling
    ❌ No built-in OpenAPI security scheme generation — add manually via
       ``app.openapi()`` override if needed

Thread safety:  ✅ Implementations hold no mutable state per-call.
Async safety:   ✅ All ``__call__`` methods are ``async def``.
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

from fastapi import HTTPException, Request, status
from varco_core.auth.base import AuthContext

# Sentinel distinguishing "audience kwarg omitted" from "audience=None passed
# explicitly" (Plan 005, Phase 2 / U-13). The two must NOT be treated the
# same: an omitted audience with no VARCO_JWT_AUDIENCE means the caller never
# thought about it (fail closed — raise); an explicit audience=None is the
# pre-Phase-2 idiom for "I am deliberately not enforcing audience" and must
# keep working exactly as before (see
# varco_fastapi/tests/milestone_a/test_server_auth.py::
# test_audience_none_does_not_enforce_either_way, which stays green
# unmodified).
_AUDIENCE_UNSET = object()

_logger = logging.getLogger(__name__)

# Anonymous AuthContext — reused by AnonymousAuth and as the fallback in
# JwtBearerAuth when required=False and no token is present.
_ANONYMOUS: AuthContext = AuthContext()

if TYPE_CHECKING:
    from starlette.datastructures import Headers, QueryParams
    from varco_core.authority import TrustedIssuerRegistry

# ── AbstractServerAuth ────────────────────────────────────────────────────────


class AbstractServerAuth(ABC):
    """
    FastAPI callable dependency that returns an ``AuthContext`` from a request.

    Subclass and implement ``__call__`` to add new authentication strategies.
    The returned ``AuthContext`` is stored in ``auth_context_var`` by
    ``RequestContextMiddleware``.

    Thread safety:  ✅ Implementations must not hold per-request mutable state.
    Async safety:   ✅ ``__call__`` is ``async def``.
    """

    @abstractmethod
    async def __call__(self, request: Request) -> AuthContext:
        """
        Extract credentials from ``request`` and return an ``AuthContext``.

        Args:
            request: The incoming FastAPI/Starlette ``Request`` object.

        Returns:
            An ``AuthContext`` populated from the verified credentials.

        Raises:
            HTTPException: 401 if credentials are invalid or missing (when required).
            HTTPException: 403 if the token is valid but the operation is denied.

        Edge cases:
            - Must not mutate ``request``.
            - For optional auth, return ``_ANONYMOUS`` (user_id=None) rather than
              raising — callers can check ``ctx.is_anonymous()``.
        """


# ── JwtBearerAuth ─────────────────────────────────────────────────────────────


class JwtBearerAuth(AbstractServerAuth):
    """
    Verify a Bearer JWT using ``TrustedIssuerRegistry``.

    Extracts the ``Authorization: Bearer <token>`` header, calls
    ``registry.verify(token)``, and maps the decoded ``JsonWebToken`` to an
    ``AuthContext``.

    The ``JsonWebToken`` produced by varco_core's ``JwtParser`` already includes
    a pre-parsed ``auth_ctx`` field when the token contains ``roles``, ``scopes``,
    or ``grants`` claims.  If ``auth_ctx`` is not present in the token, this auth
    builds one from the ``sub`` claim only.

    Args:
        registry:           ``TrustedIssuerRegistry`` for signature verification.
        required:           If ``True`` (default), missing/invalid tokens raise 401.
                            If ``False``, missing tokens return anonymous auth.
        anonymous_context:  ``AuthContext`` to return when ``required=False`` and
                            no token is present.  Defaults to ``AuthContext()``.
        audience:           Expected ``aud`` claim value(s), threaded into
                            ``registry.verify(audience=...)`` (Plan 002 C-2).
                            Omitted (default) reads ``VARCO_JWT_AUDIENCE``; if
                            that is also unset, construction **raises
                            ``ValueError``** (Plan 005 Phase 2 / U-13 — a
                            BREAKING security-default change: a service that
                            forgets to set an audience used to log one
                            warning and proceed; it now refuses to start).
                            Pass ``audience=None`` **explicitly** to keep the
                            pre-Phase-2 "not enforced" behaviour for this one
                            instance (audience is a deliberate, per-instance
                            opt-out — distinct from ``allow_any_audience``,
                            which is a process-wide policy statement); or use
                            ``allow_any_audience=True`` /
                            ``VARCO_JWT_ALLOW_ANY_AUDIENCE=true`` for the
                            documented, named escape hatch.
        allow_any_audience: Explicit, named opt-out from the ``ValueError``
                            above (Plan 005 Phase 2 / U-13). ``False``
                            (default) reads
                            ``VARCO_JWT_ALLOW_ANY_AUDIENCE``. When ``True``
                            and no audience is configured, construction
                            succeeds but logs a **single** warning — the same
                            shape as the old default, now opt-in only.
        leeway:             Clock-skew leeway in seconds, threaded into
                            ``registry.verify(leeway=...)`` (Plan 002 C-1).
                            ``None`` (default) reads ``VARCO_JWT_LEEWAY_SECONDS``
                            (default ``0.0`` — no leeway, today's behaviour).

    DESIGN: delegates to TrustedIssuerRegistry (not JwtAuthority)
        ✅ Supports multiple issuers — gateway, service-to-service, etc.
        ✅ Key rotation handled by registry; auth layer is rotation-agnostic
        ✅ Same verification path for JWT-based server and client auth
        ❌ Requires at least one issuer to be registered in the registry

    DESIGN: sentinel default for ``audience`` distinguishes "omitted" from
    "explicitly None"
        ✅ A caller who never thought about audience enforcement (omitted the
           kwarg, no env var) gets a startup failure — the whole point of
           Phase 2.
        ✅ A caller who explicitly wrote ``audience=None`` is making a
           deliberate statement and keeps working unmodified — no test in
           this repo asserting that behaviour needs to change.
        ❌ One more sentinel object in the codebase — the alternative
           (treating omitted and explicit-None identically) would silently
           break every existing ``audience=None`` call site on upgrade,
           which is a worse failure mode than "one more sentinel".

    Thread safety:  ✅ ``TrustedIssuerRegistry.verify`` is thread-safe.
    Async safety:   ✅ ``verify`` is ``async def``.

    Edge cases:
        - ``Authorization: Bearer`` with no token value raises 401.
        - Expired tokens raise 401 with "Token expired" detail.
        - Tokens with unknown ``kid`` or ``iss`` raise 401 with "Unknown issuer".
        - Omitted ``audience``, no ``VARCO_JWT_AUDIENCE``, no
          ``allow_any_audience`` → ``ValueError`` at construction.
    """

    def __init__(
        self,
        registry: TrustedIssuerRegistry,
        *,
        required: bool = True,
        anonymous_context: AuthContext | None = None,
        audience: str | list[str] | None = _AUDIENCE_UNSET,  # type: ignore[assignment]
        leeway: float | None = None,
        allow_any_audience: bool | None = None,
    ) -> None:
        from varco_core.jwt.config import JwtVerificationSettings

        settings = JwtVerificationSettings.from_env()

        self._registry = registry
        self._required = required
        self._anonymous = anonymous_context or _ANONYMOUS

        audience_omitted = audience is _AUDIENCE_UNSET
        if audience_omitted:
            self._audience = settings.audience
        else:
            self._audience = audience  # type: ignore[assignment]

        self._leeway = leeway if leeway is not None else settings.leeway_seconds

        effective_allow_any_audience = (
            allow_any_audience if allow_any_audience is not None else settings.allow_any_audience
        )

        if self._audience is None and audience_omitted:
            # Fail closed (Plan 005 Phase 2 / U-13) — only when the caller
            # never configured an audience at all. An explicit
            # audience=None is a deliberate opt-out and never reaches here.
            if not effective_allow_any_audience:
                raise ValueError(
                    "JwtBearerAuth: no audience configured — set audience=..., "
                    "VARCO_JWT_AUDIENCE, or explicitly opt out with "
                    "allow_any_audience=True / VARCO_JWT_ALLOW_ANY_AUDIENCE=true "
                    "if this service must accept tokens minted for any audience."
                )
            _logger.warning(
                "JwtBearerAuth: audience is not enforced (aud claim is never "
                "checked) — allow_any_audience=True. Set audience=... or "
                "VARCO_JWT_AUDIENCE to harden against tokens minted for a "
                "different service."
            )

    async def __call__(self, request: Request) -> AuthContext:
        """
        Args:
            request: Incoming HTTP request.

        Returns:
            Decoded ``AuthContext`` from the JWT, or ``anonymous_context`` when
            ``required=False`` and no token is present.

        Raises:
            HTTPException 401: Token missing (when required), expired,
                invalid, or (when ``audience`` is configured) minted for a
                different ``aud``.
        """
        authorization = request.headers.get("Authorization", "")
        if not authorization.startswith("Bearer "):
            if self._required:
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="Missing Bearer token",
                    headers={"WWW-Authenticate": "Bearer"},
                )
            return self._anonymous

        raw_token = authorization.removeprefix("Bearer ").strip()
        if not raw_token:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Empty Bearer token",
                headers={"WWW-Authenticate": "Bearer"},
            )

        # Build verify() kwargs conditionally — omit "audience"/"leeway"
        # entirely when they are at their "not configured" defaults so a
        # zero-config JwtBearerAuth calls registry.verify(raw_token) with no
        # extra kwargs, byte-identical to pre-Plan-002 behaviour (this is
        # asserted by test_jwt_bearer_auth_calls_registry_verify's exact-args
        # mock assertion). Functionally equivalent either way — verify()'s
        # own defaults (audience=None, leeway resolved from env) are the same
        # values — this is purely about not changing the observed call shape.
        verify_kwargs: dict[str, Any] = {}
        if self._audience is not None:
            verify_kwargs["audience"] = self._audience
        if self._leeway:
            verify_kwargs["leeway"] = self._leeway

        from varco_core.authority.exceptions import (
            RevocationStoreUnavailableError,
            TokenRevokedError,
        )

        try:
            jwt = await self._registry.verify(raw_token, **verify_kwargs)
        except RevocationStoreUnavailableError as exc:
            # Plan 034 / S13, §D-S13-error: a store outage is an outage, not
            # a bad credential — 503, never 401, and never the raw
            # exception message (which may carry backend internals).
            _logger.error("JwtBearerAuth: revocation store unavailable: %s", exc)
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Token verification is temporarily unavailable.",
            ) from exc
        except TokenRevokedError as exc:
            # §D-S13-error: str(exc) is already the fixed "Token has been
            # revoked." string — never interpolate exc.scope/.key/.reason,
            # which are for the log only.
            _logger.warning(
                "JwtBearerAuth: token revoked (scope=%s key=%s reason=%s)",
                exc.scope,
                exc.key,
                exc.reason,
            )
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail=str(exc),
                headers={"WWW-Authenticate": "Bearer"},
            ) from exc
        except Exception as exc:
            _logger.debug("JwtBearerAuth: token verification failed: %s", exc)
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail=f"Invalid or expired token: {exc}",
                headers={"WWW-Authenticate": "Bearer"},
            ) from exc

        # Use pre-parsed AuthContext from the token if available, otherwise
        # build a minimal one from the sub claim.
        if jwt.auth_ctx is not None:
            return jwt.auth_ctx

        # Fallback: build from sub claim only
        return AuthContext(user_id=jwt.sub)


# ── ApiKeyAuth ────────────────────────────────────────────────────────────────


class ApiKeyAuth(AbstractServerAuth):
    """
    Verify an API key from the ``X-API-Key`` header, or optionally a query
    parameter.

    Callers may configure either plaintext keys (``keys=``, hashed at
    construction time) or pre-hashed digests (``hashed_keys=``, the
    production path — see ``varco_core.auth.api_key.hash_api_key``).
    Exactly one of the two must be given.

    Args:
        keys:        Plaintext ``dict``/``Mapping`` of API key string ->
                     ``AuthContext``. Hashed immediately at construction
                     (§D-S14-hash) — no raw key is retained past
                     ``__init__``. Mutually exclusive with ``hashed_keys``.
        hashed_keys: Pre-hashed ``dict``/``Mapping`` of digest (as produced
                     by ``hash_api_key()``) -> ``AuthContext``. The
                     production path: the plaintext key never enters this
                     process at all. Mutually exclusive with ``keys``.
        pepper:      Optional pepper (bytes or str) applied to ``keys=``
                     hashing and to verifying a presented key against
                     either mapping. ``None`` (default) reads
                     ``VARCO_API_KEY_PEPPER``. Must be identical to
                     whatever pepper produced any digest in ``hashed_keys``,
                     or every key silently 401s (Pitfalls table).
        header:      Header name to check. Default: ``"X-API-Key"``.
        param:       Query parameter name fallback. **Default: ``None`` —
                     the fallback is off.** Naming a parameter (e.g.
                     ``param="api_key"``) re-enables it (§D-S2-param). A
                     credential in a URL query string is already in the
                     access log, the proxy log, and the ``Referer`` header
                     by the time anything can warn about it (brief 006 §5)
                     — prefer the header.
        required:    Raise 401 if no key is provided. Default: ``True``.

    Raises:
        ValueError: Both ``keys`` and ``hashed_keys`` given, or neither;
                    ``param=""`` (an empty string is a typo, not a way to
                    disable the fallback — use ``param=None``, the
                    default); a digest in ``hashed_keys`` carries an
                    unrecognized scheme prefix.

    DESIGN: static dict over DB lookup per request
        ✅ Zero latency — no I/O per request.
        ✅ Trivially testable — inject a plain dict in tests.
        ❌ Keys must be loaded at startup; not suitable for dynamic key
           issuance (use ``JwtBearerAuth`` for dynamic auth).

    DESIGN: ``param: str | None`` over a separate ``allow_query_param: bool``
    (§D-S2-param, full argument in the plan's design section)
        ✅ One knob cannot disagree with itself — ``allow_query_param=False,
           param="api_key"`` would be a readable-but-meaningless state.
        ✅ Every caller that already named ``param=`` explicitly keeps
           working unchanged; only the population that never opted in
           loses the fallback — exactly the row's intent.
        Rejected — keep the fallback and log a warning: ❌ the credential is
          already leaked into three logs by the time anything is logged.
        Rejected — an env var to re-enable it globally: ❌ a
          security-weakening default an operator could flip without a code
          review, invisible at any call site.

    Thread safety:  ✅ The hashed-key dict is read-only after construction.
    Async safety:   ✅ ``__call__`` is ``async def`` but does no I/O.

    Edge cases:
        - API key lookup is case-sensitive.
        - Header takes priority over query param when both are present and
          ``param=`` is set (unchanged from before this plan).
        - ``keys={}``/``hashed_keys={}`` remains legal — every presented
          key 401s.
    """

    def __init__(
        self,
        keys: Mapping[str, AuthContext] | None = None,
        *,
        hashed_keys: Mapping[str, AuthContext] | None = None,
        pepper: bytes | str | None = None,
        header: str = "X-API-Key",
        param: str | None = None,
        required: bool = True,
    ) -> None:
        if (keys is None) == (hashed_keys is None):
            raise ValueError(
                "ApiKeyAuth: exactly one of keys= or hashed_keys= must be given "
                f"(keys={'given' if keys is not None else 'omitted'}, "
                f"hashed_keys={'given' if hashed_keys is not None else 'omitted'})"
            )
        if param == "":
            raise ValueError(
                "ApiKeyAuth: param='' is not a way to disable the query "
                "fallback — omit param (the default, None) instead"
            )

        import os

        from varco_core.auth.api_key import hash_api_key

        self._pepper: bytes | str | None = (
            pepper if pepper is not None else os.environ.get("VARCO_API_KEY_PEPPER")
        )

        digest_map: dict[str, AuthContext]
        if keys is not None:
            # §D-S14-hash: hash every plaintext key immediately; nothing
            # keeps a reference to `keys` or any raw key past this line.
            digest_map = {hash_api_key(raw, pepper=self._pepper): ctx for raw, ctx in keys.items()}
        else:
            assert hashed_keys is not None  # narrowed by the XOR check above
            # Validate every digest's scheme prefix up front so a
            # misconfigured store fails loudly at construction, not on the
            # first request that happens to hit the bad entry.
            digest_map = dict(hashed_keys)
            for digest in digest_map:
                scheme, sep, _ = digest.partition("$")
                if not sep or scheme not in ("sha256", "hmac-sha256"):
                    raise ValueError(
                        f"ApiKeyAuth: hashed_keys contains an unrecognized digest "
                        f"scheme {scheme!r} (expected 'sha256' or 'hmac-sha256')"
                    )

        self._digest_keys = digest_map
        self._header = header
        self._param = param
        self._required = required
        # Which constructor path populated _digest_keys — surfaced read-only
        # via inspect_auth_posture()'s api_key_plaintext_source field
        # (Plan 034 / Phase 4). Never used for any auth decision; the
        # already-hashed _digest_keys dict is the only thing __call__ reads.
        self._constructed_from_plaintext = keys is not None

    async def __call__(self, request: Request) -> AuthContext:
        """
        Args:
            request: Incoming HTTP request.

        Returns:
            ``AuthContext`` for the matched API key.

        Raises:
            HTTPException 401: Key is missing (when required) or not recognized.
        """
        from varco_core.auth.api_key import hash_api_key

        # Header takes priority; the query param is only ever consulted
        # when a fallback name was explicitly configured (§D-S2-param).
        api_key = request.headers.get(self._header)
        if not api_key and self._param is not None:
            api_key = request.query_params.get(self._param)

        if not api_key:
            if self._required:
                detail = f"Missing API key (header: {self._header!r}"
                detail += f" or query param: {self._param!r})" if self._param else ")"
                raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail=detail)
            return _ANONYMOUS

        # §D-S14-compare: dict lookup selects the O(1) candidate, then
        # hmac.compare_digest (inside hash_api_key's deterministic digest
        # equality) is the actual accept decision — never a raw `==` on a
        # credential.
        digest = hash_api_key(api_key, pepper=self._pepper)
        ctx = self._digest_keys.get(digest)
        if ctx is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid API key",
            )
        return ctx


# Restore a *real* union-type object (rather than the PEP 563-stringified
# "str | None") on ApiKeyAuth.__init__'s `param` annotation. `from __future__
# import annotations` at the top of this module stringifies every annotation
# at class-definition time; that is fine for static tools (mypy resolves
# strings against imports) but means `inspect.signature(...).annotation` at
# runtime is the literal string, not a type — and the §D-034-gate regression
# guard (`test_param_default_is_none_by_signature`) needs a real
# `types.UnionType` to walk `.__args__` and assert `None` is a member,
# exactly the way `api_surface.py --check` cannot for a class member.
ApiKeyAuth.__init__.__annotations__["param"] = str | None


# ── PassthroughAuth ───────────────────────────────────────────────────────────


class PassthroughAuth(AbstractServerAuth):
    """
    Decode a Bearer JWT WITHOUT verifying the signature.

    Intended for internal services behind an API gateway that has already
    verified the token.  The gateway strips the token of its signature or
    forwards it as a trusted header; PassthroughAuth just reads the claims.

    WARNING: Never use this on public-facing endpoints.  It bypasses all
    cryptographic verification.

    Args:
        required: Raise 401 if no token is present.  Default: ``False``
                  (internal services often allow requests without tokens for
                  health checks and inter-service calls).

    DESIGN: passthrough over re-verification behind gateway
        ✅ Avoids redundant signature verification for internal routes
        ✅ Works when the gateway strips the signature (JWT format preserved)
        ❌ Completely insecure on public endpoints — document the use case clearly

    Thread safety:  ✅ Stateless.
    Async safety:   ✅ ``__call__`` is ``async def``; no I/O.
    """

    def __init__(self, *, required: bool = False) -> None:
        self._required = required

    async def __call__(self, request: Request) -> AuthContext:
        """
        Returns:
            ``AuthContext`` decoded from claims (no signature check), or
            anonymous if no token is present.

        Raises:
            HTTPException 401: When ``required=True`` and no token present,
                or the token is not well-formed enough to decode at all.

        Edge cases:
            - Delegates to ``JwtParser.parse_unverified()`` (Plan 002 step 35)
              instead of hand-rolling base64/JSON decoding — this is the ONE
              varco_fastapi call site that previously duplicated claim→
              ``AuthContext`` parsing outside ``JwtParser``, so it now
              benefits from the claim-transform pipeline (env-driven or
              explicit) for free, exactly like ``JwtBearerAuth``.
            - ``token.extra_claims`` (raw, non-reserved claims) is merged
              into the resulting ``AuthContext.metadata`` — preserving the
              pre-refactor behaviour where every non-standard claim landed
              in ``metadata``.
        """
        authorization = request.headers.get("Authorization", "")
        if not authorization.startswith("Bearer "):
            if self._required:
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="Missing Bearer token",
                )
            return _ANONYMOUS

        raw_token = authorization.removeprefix("Bearer ").strip()

        from dataclasses import replace

        from varco_core.jwt import JwtParser

        try:
            token = JwtParser.parse_unverified(raw_token)
        except Exception as exc:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail=f"Could not decode token claims: {exc}",
            ) from exc

        ctx = token.auth_ctx or AuthContext(user_id=token.sub)
        if token.extra_claims:
            ctx = replace(ctx, metadata={**ctx.metadata, **token.extra_claims})
        return ctx


# ── AnonymousAuth ─────────────────────────────────────────────────────────────


class AnonymousAuth(AbstractServerAuth):
    """
    Always return an anonymous ``AuthContext`` (user_id=None).

    Use for fully public endpoints that require no credentials.
    The returned context has no roles, scopes, or grants.

    Thread safety:  ✅ Stateless singleton.
    Async safety:   ✅ ``__call__`` is ``async def``; no I/O.
    """

    async def __call__(self, request: Request) -> AuthContext:
        """Always returns an anonymous ``AuthContext``."""
        return _ANONYMOUS


# ── CompositeServerAuth ───────────────────────────────────────────────────────


class CompositeServerAuth(AbstractServerAuth):
    """
    Try each ``AbstractServerAuth`` strategy in order; first success wins.

    Useful for APIs that accept both JWT Bearer tokens AND API keys, or that
    support optional auth (try JWT, fall back to anonymous).

    Args:
        strategies: List of ``AbstractServerAuth`` instances to try in order.

    DESIGN: composite over subclassing
        ✅ Open/closed — add new strategies without modifying existing ones
        ✅ Mirrors CompositeHealthCheck from varco_core for consistency
        ✅ Order-sensitive — first success short-circuits (most specific first)
        ❌ Error messages can be confusing when all strategies fail — the last
           strategy's 401 is raised

    Thread safety:  ✅ Strategies are read-only after construction.
    Async safety:   ✅ Tries strategies sequentially until one succeeds.

    Edge cases:
        - Empty ``strategies`` list always raises 401.
        - A strategy that returns ``_ANONYMOUS`` counts as success — subsequent
          strategies are NOT tried.  For optional-auth fallback, put ``AnonymousAuth``
          last in the list.
    """

    def __init__(self, strategies: list[AbstractServerAuth]) -> None:
        if not strategies:
            raise ValueError("CompositeServerAuth requires at least one strategy.")
        self._strategies = strategies

    async def __call__(self, request: Request) -> AuthContext:
        """
        Args:
            request: Incoming HTTP request.

        Returns:
            ``AuthContext`` from the first succeeding strategy.

        Raises:
            HTTPException 401: All strategies failed — the last strategy's
                exception is re-raised.
        """
        last_exc: HTTPException | None = None
        for strategy in self._strategies:
            try:
                return await strategy(request)
            except HTTPException as exc:
                last_exc = exc
                continue

        # All strategies failed — raise the last error
        raise last_exc or HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication failed: no strategy succeeded",
        )


# ── WebSocketAuth ─────────────────────────────────────────────────────────────


class WebSocketAuth(AbstractServerAuth):
    """
    Authentication for WebSocket connections.

    WebSocket connections cannot set custom headers after the initial HTTP
    upgrade request.  This class extracts credentials from one of three sources,
    checked in priority order:

    1. ``Authorization: Bearer <token>`` header on the upgrade request (standard).
    2. ``Sec-WebSocket-Protocol`` sub-protocol with ``protocol_prefix`` (browser
       workaround — JS WebSocket API cannot set custom headers).
       Example: ``Sec-WebSocket-Protocol: bearer.eyJhbGciOi...``
    3. Query parameter ``?token=<jwt>`` (last resort — visible in server logs;
       **off unless ``token_query_param=`` names it**, see below).

    Once extracted, the raw token is injected as a synthetic ``Authorization:
    Bearer`` header and delegated to the ``inner`` auth strategy for verification.

    Args:
        inner:               The auth strategy to delegate to after extraction.
                             Typically ``JwtBearerAuth`` or ``ApiKeyAuth``.
        token_query_param:   Query param name for the token fallback.
                             **Default: ``None`` — the fallback is off**
                             (§D-S2-ws, mirrors ``ApiKeyAuth``'s ``param=``).
                             Naming a parameter (e.g. ``"token"``) re-enables
                             it and logs a ``warning`` (not ``debug``) on
                             every use — the credential is in the URL, and a
                             browser client should prefer the
                             ``Sec-WebSocket-Protocol: bearer.<token>`` path
                             below instead.
        protocol_prefix:     Sub-protocol prefix for the token.
                             Default: ``"bearer."``.

    DESIGN: extraction wrapper over a new ABC
        ✅ Reuses existing JwtBearerAuth/ApiKeyAuth for verification — no duplication
        ✅ All three browser-compatible auth patterns in one place
        ✅ Works for both ws:// (dev) and wss:// (prod)
        ❌ The query-param fallback, once opted into, is still visible in
           server access logs — hence off by default and logged at
           ``warning`` (not ``debug``, corrected per BACKLOG correction 1)
           whenever it fires.

    Thread safety:  ✅ Delegates to inner strategy; no mutable state.
    Async safety:   ✅ Delegates to ``inner.__call__``.

    Edge cases:
        - Only one source is tried per connection (header > protocol > query).
        - The sub-protocol token is NOT included in the ``Sec-WebSocket-Protocol``
          response header — use ``websocket.accept(subprotocol=...)`` if the
          client expects sub-protocol echo.
        - ``token_query_param=None`` (the default) means the query source is
          never consulted at all — the browser sub-protocol path above is
          the supported alternative for clients that cannot set headers.
    """

    def __init__(
        self,
        inner: AbstractServerAuth,
        *,
        token_query_param: str | None = None,
        protocol_prefix: str = "bearer.",
    ) -> None:
        self._inner = inner
        self._token_query_param = token_query_param
        self._protocol_prefix = protocol_prefix

    async def __call__(self, request: Request) -> AuthContext:
        """
        Extract credentials and delegate to ``inner`` for verification.

        Args:
            request: The incoming upgrade request.  For WS connections this is
                     the HTTP upgrade ``Request``.

        Returns:
            ``AuthContext`` from the ``inner`` strategy.

        Raises:
            HTTPException 401: No credentials found, or inner verification fails.
        """
        # 1. Standard Authorization header
        if request.headers.get("Authorization", "").startswith("Bearer "):
            return await self._inner(request)

        # 2. Sec-WebSocket-Protocol sub-protocol token
        protocols_header = request.headers.get("Sec-WebSocket-Protocol", "")
        for proto in (p.strip() for p in protocols_header.split(",")):
            if proto.startswith(self._protocol_prefix):
                raw_token = proto.removeprefix(self._protocol_prefix).strip()
                if raw_token:
                    # Inject as synthetic Authorization header via a wrapped request
                    return await self._inner(
                        _RequestWithBearerOverride(request, raw_token)  # type: ignore[arg-type]
                    )

        # 3. Query parameter fallback — only ever consulted when a param
        # name was explicitly configured (§D-S2-ws mirrors ApiKeyAuth's
        # param=None-by-default rule).
        if self._token_query_param is not None:
            query_token = request.query_params.get(self._token_query_param)
            if query_token:
                _logger.warning(
                    "WebSocketAuth: using query param token — visible in "
                    "server access logs; prefer the Authorization header or "
                    "the Sec-WebSocket-Protocol sub-protocol path for "
                    "production."
                )
                return await self._inner(_RequestWithBearerOverride(request, query_token))  # type: ignore[arg-type]

        # All extraction methods failed
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="WebSocket authentication failed: no credentials found "
            "(checked Authorization header, Sec-WebSocket-Protocol, and query param).",
        )


class _RequestWithBearerOverride:
    """
    Thin request wrapper that injects a synthetic ``Authorization: Bearer``
    header, allowing inner auth strategies to work without modification.

    This is an internal implementation detail — not part of the public API.

    Thread safety:  ✅ Wraps an existing immutable Request; no shared state.
    """

    def __init__(self, request: Request, raw_token: str) -> None:
        self._request = request
        self._raw_token = raw_token

    @property
    def headers(self) -> _HeadersWithBearer:
        return _HeadersWithBearer(self._request.headers, self._raw_token)

    @property
    def query_params(self) -> QueryParams:
        return self._request.query_params

    def __getattr__(self, name: str) -> Any:
        return getattr(self._request, name)


class _HeadersWithBearer:
    """Header accessor that overrides the Authorization entry."""

    def __init__(self, original: Headers, raw_token: str) -> None:
        self._original = original
        self._bearer = f"Bearer {raw_token}"

    def get(self, key: str, default: str = "") -> str:
        if key.lower() == "authorization":
            return self._bearer
        return self._original.get(key, default)

    def __getitem__(self, key: str) -> str:
        if key.lower() == "authorization":
            return self._bearer
        return self._original[key]


# ── Public API ────────────────────────────────────────────────────────────────

__all__ = [
    "AbstractServerAuth",
    "JwtBearerAuth",
    "ApiKeyAuth",
    "PassthroughAuth",
    "AnonymousAuth",
    "CompositeServerAuth",
    "WebSocketAuth",
]
