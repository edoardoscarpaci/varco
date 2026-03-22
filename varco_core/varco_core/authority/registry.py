"""
varco_core.authority.registry
==================================

``TrustedIssuerRegistry`` — the single object that holds all configured
trusted issuers and verifies incoming tokens against their keys.

Architecture
------------
The registry maps a human-readable ``label`` (e.g. ``"GOOGLE"``,
``"SYSTEM_SVC"``) to an ``IssuerSource`` + the expected ``iss`` claim value.
At verification time:

    1. Decode JWT header (unverified) → extract ``kid``.
    2. Search all loaded keysets for a ``JsonWebKey`` with that ``kid``.
    3. If not found → refresh all sources (kid-not-found = rotation signal).
    4. Convert the found ``JsonWebKey`` → PyJWT ``PyJWK`` → crypto key object.
    5. ``jwt.decode()`` to verify signature + claims.
    6. Return typed ``JsonWebToken``.

``iss`` enforcement is intentionally NOT done here — it is the framework
user's responsibility.  The registry only checks that the signature is valid
for a registered key.  Use ``JwtUtil(token).is_issuer(...)`` after verify()
if you need to enforce the issuer.

DESIGN: asyncio.Lock created lazily
    ✅ asyncio.Lock() must be created inside a running event loop.
       Creating it at module level or in __init__() before the loop is
       running raises "no running event loop" errors.
    ✅ Lazy creation in _get_lock() ensures the lock is always created
       inside the correct loop.
    ❌ Not thread-safe — the registry is designed for use inside a single
       async event loop.  Do not share a registry across threads.

Thread safety:  ❌ Not safe for multi-threaded use — designed for async only.
Async safety:   ✅ Safe — asyncio.Lock serialises concurrent verify() calls.
                   Each verify() holds the lock only during the rare
                   kid-not-found refresh; normal (cached) verifications are
                   nearly lock-free.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, ClassVar

import jwt as _jwt
from jwt import PyJWK  # PyJWT >= 2.4 — converts JWK dict → crypto key object

if TYPE_CHECKING:
    # Imported only under TYPE_CHECKING — these classes don't import registry.py
    # so there is no circular dependency, but the guard keeps the runtime import
    # graph clean and makes the optional nature of the dependency explicit.
    from varco_core.authority.jwt_authority import JwtAuthority
    from varco_core.authority.multi_key_authority import MultiKeyAuthority

    # Instance is a Providify injection annotation — only needed for type hints
    # in from_container().  Not needed at runtime because InstanceProxy.aget_all()
    # carries the type internally; from_container() never references the class
    # directly at runtime, removing the latent NameError that existed when the
    # code called container.aget_all(MultiKeyAuthority) with that class only
    # imported under TYPE_CHECKING.
    from providify import Instance

from varco_core.jwk.model import JsonWebKey, JsonWebKeySet
from varco_core.jwt.model import JsonWebToken
from varco_core.jwt.parser import JwtParser

from varco_core.authority.exceptions import (
    IssuerNotFoundError,
    KeyLoadError,
    UnknownKidError,
)
from varco_core.authority.sources.protocol import IssuerSource


# ── TrustedIssuerEntry ────────────────────────────────────────────────────────


@dataclass
class TrustedIssuerEntry:
    """
    A single registered trusted issuer.

    Binds a human-readable ``label`` (the env var suffix) to an
    ``IssuerSource`` and the expected ``iss`` claim value.  The cached
    ``_keyset`` is set by the registry after ``load_all()`` or ``refresh()``.

    Thread safety:  ⚠️ _keyset is mutable — only the registry mutates it,
                       serialised by the registry's own asyncio.Lock.
    """

    # Human-readable label — the env var suffix (e.g. "GOOGLE", "SYSTEM_SVC")
    label: str

    # Expected iss claim value for tokens from this issuer.
    # Not enforced by the registry itself — available for callers.
    iss: str

    # The key source — knows how to load/refresh the keyset
    source: IssuerSource

    # Cached keyset — None until first load_all() or load(label)
    _keyset: JsonWebKeySet | None = field(default=None, init=False, repr=False)

    def find_key(self, kid: str) -> JsonWebKey | None:
        """
        Search the cached keyset for a key with the given ``kid``.

        Returns:
            The matching ``JsonWebKey``, or ``None`` if not loaded or not found.
        """
        if self._keyset is None:
            return None
        return self._keyset.find_by_kid(kid)


# ── TrustedIssuerRegistry ─────────────────────────────────────────────────────


class TrustedIssuerRegistry:
    """
    Registry of trusted issuers — verifies JWTs against configured key sources.

    Populate via ``register()`` or load from env via ``from_env()``.  Call
    ``load_all()`` at application startup to eagerly fetch all remote JWKS
    endpoints.

    Thread safety:  ❌ Not thread-safe — async-only (single event loop).
    Async safety:   ✅ Safe — asyncio.Lock prevents concurrent mutations.

    Example::

        registry = TrustedIssuerRegistry.from_env()
        await registry.load_all()

        # In a request handler:
        token = await registry.verify(raw_token)
        if token.iss != "my-service":
            raise PermissionError("unexpected issuer")
    """

    __slots__ = ("_entries", "_lock", "_last_refresh", "_min_refresh_interval")

    # ── DI injection handles ───────────────────────────────────────────────────
    #
    # Declare the Providify Instance[T] handles that from_container() expects.
    # These are ClassVar annotations — documentation of the DI contract, not
    # runtime state.  Providify does not inject ClassVars automatically; callers
    # obtain InstanceProxy objects via constructor injection in their own
    # @Configuration class and pass them explicitly to from_container().
    #
    # DESIGN: ClassVar annotations rather than constructor params
    #   ✅ TrustedIssuerRegistry remains constructable without a DI container
    #      (via __init__(), from_env(), or register()) — DI is opt-in.
    #   ✅ Documents the expected DI types at the class level — a reader can see
    #      at a glance what from_container() needs without reading its signature.
    #   ❌ Not injected automatically — callers must wire these in their config.
    #   Alternative considered: making TrustedIssuerRegistry a @Singleton with
    #   Instance[T] constructor params — rejected because it would prevent
    #   direct instantiation (from_env(), TrustedIssuerRegistry()) without a
    #   DI container, which is the primary non-DI usage path.
    _multi_key_handle: ClassVar[Instance[MultiKeyAuthority]]
    _jwt_handle: ClassVar[Instance[JwtAuthority]]

    def __init__(self) -> None:
        # label → TrustedIssuerEntry
        self._entries: dict[str, TrustedIssuerEntry] = {}

        # asyncio.Lock created lazily — must be inside a running event loop
        self._lock: asyncio.Lock | None = None

        # Rate-limit for kid-not-found global refresh
        self._last_refresh: float = 0.0
        self._min_refresh_interval: float = 10.0

    def _get_lock(self) -> asyncio.Lock:
        """
        Return the asyncio.Lock, creating it lazily on first access.

        Laziness is required because asyncio.Lock() can only be created
        inside a running event loop.  Creating it in __init__() would fail
        when the registry is constructed before the loop starts.

        Returns:
            The registry's asyncio.Lock.
        """
        if self._lock is None:
            # Created inside the running loop — safe.
            self._lock = asyncio.Lock()
        return self._lock

    # ── Registration ──────────────────────────────────────────────────────────

    def register(
        self,
        label: str,
        iss: str,
        source: IssuerSource,
    ) -> None:
        """
        Register a trusted issuer.

        Args:
            label:  Human-readable label (e.g. ``"GOOGLE"``, ``"SYSTEM_SVC"``).
                    Used as the key in ``jwks(label)``.
            iss:    Expected ``iss`` claim value for tokens from this issuer.
                    Available on ``TrustedIssuerEntry.iss`` — not enforced here.
            source: Key source that loads/refreshes the issuer's public keys.

        Edge cases:
            - Registering a label that already exists replaces the old entry.
            - ``load_all()`` must be called after all ``register()`` calls to
              load the keysets.
        """
        self._entries[label] = TrustedIssuerEntry(label=label, iss=iss, source=source)

    def register_authority(
        self,
        authority: JwtAuthority | MultiKeyAuthority,
        *,
        label: str | None = None,
    ) -> None:
        """
        Register a local signing authority as a trusted issuer.

        Wraps the authority in an ``AuthoritySource`` and calls ``register()``.
        This is the zero-config path for verifying tokens produced by the
        framework itself — no env vars, no PEM files, no JWKS URLs needed.

        After calling ``register_authority()``, include this registry in the
        normal startup sequence:

            registry.register_authority(system_authority)
            await registry.load_all()     # loads the authority's keyset too
            token = await registry.verify(raw_token)  # ✅ works for system tokens

        The registry stays in sync with key rotation automatically: the next
        kid-not-found refresh (or explicit ``load_all()``) will call
        ``AuthoritySource.refresh()``, which reads the live ``jwks()`` output
        from the ``MultiKeyAuthority`` — including any newly rotated keys.

        Args:
            authority: ``JwtAuthority`` or ``MultiKeyAuthority`` to register.
                       Must have ``.issuer`` (str) and ``.jwks()`` -> ``JsonWebKeySet``.
            label:     Human-readable label for this entry (e.g. ``"SYSTEM_SVC"``).
                       Defaults to the issuer string uppercased with dashes
                       replaced by underscores — e.g. ``"system-svc"`` → ``"SYSTEM_SVC"``.
                       Overriding is useful when two authorities share an issuer
                       but should be distinguishable in ``jwks(label)`` calls.

        Raises:
            Nothing — replaces any existing entry with the same label silently
            (consistent with ``register()`` behaviour).

        Edge cases:
            - ``label`` collision: if a label derived from the issuer already
              exists (e.g. from an env var), the new ``AuthoritySource`` replaces
              the old one.  Use an explicit ``label`` to avoid this.
            - Registering the same authority twice under two labels creates two
              independent entries that both return the same keyset.  Harmless but
              wasteful — avoid it.
            - ``load_all()`` must be called after all ``register_authority()``
              calls to populate the keyset cache.  For ``AuthoritySource`` this
              is a no-op I/O-wise, but the registry still needs to call
              ``source.load()`` to set ``entry._keyset``.
        """
        # Local import — avoids circular dependency at module level.
        # registry.py is imported by authority/__init__.py which also imports
        # jwt_authority.py and multi_key_authority.py.  If we imported them
        # at the top of this file we'd create a cycle at import time.
        from varco_core.authority.sources.authority import AuthoritySource

        iss: str = authority.issuer  # duck typing — both concrete types have .issuer

        # Derive label from issuer if not provided.
        # "system-svc" → "SYSTEM_SVC" matches the env var label convention so
        # that env-var-configured and code-configured entries use the same key.
        derived_label = label or iss.upper().replace("-", "_")

        source = AuthoritySource(authority, issuer=iss)
        self.register(label=derived_label, iss=iss, source=source)

    # ── Loading ───────────────────────────────────────────────────────────────

    async def load_all(self) -> None:
        """
        Eagerly load keysets from all registered sources.

        Call once at application startup.  PEM sources load synchronously
        (cheap); JWKS/OIDC sources make HTTP requests.

        All sources are loaded concurrently via ``asyncio.gather()``.
        Individual source failures are collected and re-raised as a single
        ``KeyLoadError`` after all loads complete — so a single offline
        endpoint doesn't prevent other sources from loading.

        Raises:
            KeyLoadError: One or more sources failed to load.  The message
                          contains details for all failures.

        Edge cases:
            - An empty registry (no registered issuers) is valid — no-op.
            - Calling ``load_all()`` again re-loads all sources (useful for
              forced refresh without a kid-not-found signal).
        """
        if not self._entries:
            return

        # Load all sources concurrently — return_exceptions=True collects
        # failures instead of aborting on the first one.
        results = await asyncio.gather(
            *(entry.source.load() for entry in self._entries.values()),
            return_exceptions=True,
        )

        errors: list[str] = []
        for entry, result in zip(self._entries.values(), results):
            if isinstance(result, BaseException):
                errors.append(f"  [{entry.label}] {result}")
            else:
                # result is a JsonWebKeySet — commit to the entry
                entry._keyset = result

        if errors:
            raise KeyLoadError(
                f"Failed to load {len(errors)} issuer source(s):\n"
                + "\n".join(errors)
                + "\nResolve the above issues and retry load_all()."
            )

    async def load(self, label: str) -> None:
        """
        Load (or reload) the keyset for a single registered issuer.

        Useful for lazy loading or forced reload of one issuer without
        triggering a full ``load_all()``.

        Args:
            label: The registered issuer label to load.

        Raises:
            IssuerNotFoundError: ``label`` is not registered.
            KeyLoadError:        The source failed to load.
        """
        entry = self._entries.get(label)
        if entry is None:
            raise IssuerNotFoundError(
                f"No issuer registered with label={label!r}. "
                f"Known labels: {list(self._entries.keys())}.",
                label=label,
            )
        entry._keyset = await entry.source.load()

    # ── Key lookup ────────────────────────────────────────────────────────────

    async def get_key(self, kid: str) -> JsonWebKey | None:
        """
        Find a ``JsonWebKey`` with the given ``kid`` across all loaded keysets.

        First searches the in-memory cached keysets.  On a miss, triggers a
        rate-limited refresh of all sources (kid-not-found = key rotation
        signal from a remote issuer) and searches again.

        Args:
            kid: Key ID to look up.

        Returns:
            The first matching ``JsonWebKey``, or ``None`` if not found after
            refresh.

        Edge cases:
            - If the global refresh rate limit blocks the re-fetch, returns
              ``None`` rather than waiting — callers should treat this as
              "key not found right now".
            - The same kid may theoretically appear in multiple issuers'
              keysets — the first match wins (first registered issuer).
        """
        # First pass — search all loaded caches
        for entry in self._entries.values():
            key = entry.find_key(kid)
            if key is not None:
                return key

        # Miss — check rate limit before hitting the network
        now = time.monotonic()
        if now - self._last_refresh < self._min_refresh_interval:
            # Rate-limited — don't hit remote sources again so soon
            return None

        # Trigger refresh of all sources under the lock — prevents concurrent
        # requests from all firing refresh() simultaneously on the same miss.
        async with self._get_lock():
            # Re-check inside lock — another task may have refreshed already
            for entry in self._entries.values():
                key = entry.find_key(kid)
                if key is not None:
                    return key

            self._last_refresh = time.monotonic()

            # Refresh all sources concurrently — failures are tolerated
            results = await asyncio.gather(
                *(entry.source.refresh() for entry in self._entries.values()),
                return_exceptions=True,
            )

            for entry, result in zip(self._entries.values(), results):
                if not isinstance(result, BaseException):
                    entry._keyset = result
                # Silently skip failed refreshes — stale cache is better than
                # a hard error during token verification.

        # Second pass — search updated caches
        for entry in self._entries.values():
            key = entry.find_key(kid)
            if key is not None:
                return key

        return None

    # ── Verification ──────────────────────────────────────────────────────────

    async def verify(
        self,
        token_str: str,
        *,
        audience: str | list[str] | None = None,
    ) -> JsonWebToken:
        """
        Verify a JWT string against all registered issuers' public keys.

        Routing is by ``kid`` header claim.  The first registered issuer that
        holds a key with the matching kid is used for verification.

        Does NOT enforce the ``iss`` claim — that is the caller's
        responsibility.  Use ``JwtUtil(token).is_issuer(expected_iss)`` after
        this call when issuer identity matters.

        Args:
            token_str: Raw JWT string.
            audience:  Expected ``aud`` value(s).  ``None`` skips audience check.

        Returns:
            ``JsonWebToken`` with all claims populated.

        Raises:
            UnknownKidError:            Token has no ``kid`` header, or no
                                        registered issuer has a key for it.
            jwt.ExpiredSignatureError:  Token has passed its ``exp`` time.
            jwt.InvalidSignatureError:  Signature verification failed.
            jwt.DecodeError:            Token is malformed.
            jwt.InvalidAudienceError:   ``aud`` mismatch when ``audience``
                                        is provided.

        Edge cases:
            - A token with no ``kid`` header raises ``UnknownKidError`` because
              routing by kid is the only strategy — brute-force trying all keys
              would be a timing oracle vulnerability.
            - ``iss`` is NOT validated — this is by design.  Call
              ``JwtUtil(token).is_issuer(expected)`` after verify() if needed.
            - If multiple registered issuers share the same kid (misconfiguration),
              the first-registered one's key is used.
        """
        # Decode header only — cheap base64, no signature verification
        header = _jwt.get_unverified_header(token_str)
        kid: str | None = header.get("kid")

        if kid is None:
            raise UnknownKidError(
                "Token header has no 'kid' claim — cannot route to the correct key. "
                "Ensure tokens are signed with JwtAuthority which injects 'kid' into "
                "the JWT header automatically.",
                kid=None,
            )

        jwk = await self.get_key(kid)

        if jwk is None:
            registered = list(self._entries.keys())
            raise UnknownKidError(
                f"No registered issuer has a key with kid={kid!r}. "
                f"Registered issuers: {registered}. "
                f"Check FASTREST_AUTHORIZATION__* env vars — the issuer may not be "
                f"configured, or the remote JWKS may not yet contain this kid.",
                kid=kid,
            )

        # DESIGN: PyJWK converts JsonWebKey.to_dict() → cryptography key object.
        # This is the bridge between our JWK model and PyJWT's verification path.
        # PyJWK is available in PyJWT >= 2.4 (project requires >= 2.8).
        #
        # Tradeoffs vs alternative (extracting key manually):
        #   ✅ Handles RSA / EC / oct transparently — no type dispatch needed.
        #   ✅ PyJWK picks the algorithm from the jwk "alg" field automatically.
        #   ❌ Extra object construction on every token — negligible for JWTs.
        try:
            pyjwk = PyJWK(jwk.to_dict())
        except Exception as e:
            raise KeyLoadError(
                f"Cannot construct verification key for kid={kid!r}: {e}. "
                f"The key may be malformed or use an unsupported algorithm."
            ) from e

        decode_kwargs: dict[str, Any] = {"algorithms": [pyjwk.algorithm_name]}
        if audience is not None:
            decode_kwargs["audience"] = audience

        # Delegate to PyJWT for the actual signature + claims verification.
        # Any jwt.exceptions.* propagates unchanged — callers may catch them.
        raw = _jwt.decode(token_str, pyjwk.key, **decode_kwargs)

        # Reuse JwtParser's claim reconstruction — AuthContext, timestamps, etc.
        return JwtParser._from_raw_claims(raw)

    # ── JWKS exposure ──────────────────────────────────────────────────────────

    def jwks(self, label: str) -> JsonWebKeySet:
        """
        Return the current keyset for a specific registered issuer.

        Args:
            label: The registered issuer label.

        Returns:
            The issuer's ``JsonWebKeySet``.  Empty if not yet loaded.

        Raises:
            IssuerNotFoundError: ``label`` is not registered.
        """
        entry = self._entries.get(label)
        if entry is None:
            raise IssuerNotFoundError(
                f"No issuer registered with label={label!r}. "
                f"Known labels: {list(self._entries.keys())}.",
                label=label,
            )
        return entry._keyset or JsonWebKeySet(keys=())

    def all_jwks(self) -> JsonWebKeySet:
        """
        Return a merged ``JsonWebKeySet`` containing all registered public keys.

        Useful for serving a single merged JWKS endpoint.  Returns only the
        keys that have been loaded (``_keyset`` is not ``None``).

        Returns:
            ``JsonWebKeySet`` with one entry per loaded key across all issuers.

        Edge cases:
            - Issuers whose keyset has not been loaded (``load_all()`` not yet
              called) contribute no keys to the merged set.
            - Duplicate kids across issuers are NOT deduplicated.
        """
        all_keys = tuple(
            key
            for entry in self._entries.values()
            if entry._keyset is not None
            for key in entry._keyset.keys
        )
        return JsonWebKeySet(keys=all_keys)

    def entry(self, label: str) -> TrustedIssuerEntry:
        """
        Return the ``TrustedIssuerEntry`` for a given label.

        Useful for inspecting ``entry.iss`` or passing it to custom logic.

        Args:
            label: The registered issuer label.

        Returns:
            The ``TrustedIssuerEntry`` for this label.

        Raises:
            IssuerNotFoundError: ``label`` is not registered.
        """
        e = self._entries.get(label)
        if e is None:
            raise IssuerNotFoundError(
                f"No issuer registered with label={label!r}. "
                f"Known labels: {list(self._entries.keys())}.",
                label=label,
            )
        return e

    # ── Factory ───────────────────────────────────────────────────────────────

    @classmethod
    def from_env(cls) -> TrustedIssuerRegistry:
        """
        Construct a ``TrustedIssuerRegistry`` from environment variables.

        Reads ``FASTREST_AUTHORIZATION__<LABEL>__URL`` and
        ``FASTREST_AUTHORIZATION__<LABEL>__ISS`` pairs from the environment.

        Returns:
            Populated ``TrustedIssuerRegistry``.  Call ``load_all()`` after
            construction to fetch remote keysets.

        Example env vars::

            FASTREST_AUTHORIZATION__SYSTEM_SVC__URL = pem::/etc/certs/system.pem
            FASTREST_AUTHORIZATION__SYSTEM_SVC__ISS = system-svc
            FASTREST_AUTHORIZATION__GOOGLE__URL = https://accounts.google.com
            FASTREST_AUTHORIZATION__GOOGLE__ISS = https://accounts.google.com
        """
        from varco_core.authority.config import AuthorizationConfig

        return AuthorizationConfig.from_env().to_registry()

    @classmethod
    async def from_container(
        cls,
        multi_key_authorities: Instance[MultiKeyAuthority],
        jwt_authorities: Instance[JwtAuthority],
        *,
        include_env: bool = True,
    ) -> TrustedIssuerRegistry:
        """
        Build a fully loaded registry from Providify ``Instance[T]`` handles.

        Accepts pre-wired ``InstanceProxy`` objects (obtained via constructor
        injection in a ``@Configuration`` class) rather than a raw
        ``DIContainer``.  This keeps the registry decoupled from the container
        API — it never calls ``container.aget_all(SomeType)`` directly.

        When ``include_env=True`` (the default), env-var configured issuers
        (``FASTREST_AUTHORIZATION__*``) are loaded first so that the final
        registry combines both configuration sources in one call:

            ┌──────────────────────┐   ┌─────────────────────────────────────┐
            │  env-var issuers     │ + │  DI-registered signing authorities  │
            │  (Google, Auth0, …)  │   │  (MultiKeyAuthority, JwtAuthority)  │
            └──────────────────────┘   └─────────────────────────────────────┘
                                       ↓
                         TrustedIssuerRegistry (loaded)

        DESIGN: ``Instance[T]`` proxies instead of a raw ``DIContainer``
            ✅ No import of ``DIContainer`` at runtime — providify remains an
               optional dependency for users who don't use DI at all.
            ✅ ``InstanceProxy.resolvable()`` avoids try/except LookupError —
               a side-effect-free probe that creates no instances.
            ✅ ``InstanceProxy.aget_all()`` does not reference the concrete type
               at the call site — the proxy carries the type internally, which
               also removes the latent NameError that existed when the old code
               called ``container.aget_all(MultiKeyAuthority)`` with that class
               only visible under TYPE_CHECKING.
            ❌ Callers must obtain ``InstanceProxy`` objects via their own
               ``@Configuration`` class; cannot be called with just a container.
            Alternative considered: accepting ``DIContainer`` directly — rejected
            because it couples this class to the providify container API and
            requires a runtime import of ``DIContainer``.

        Args:
            multi_key_authorities: ``InstanceProxy[MultiKeyAuthority]`` injected
                                   via ``Instance[MultiKeyAuthority]`` in the
                                   caller's ``@Configuration`` constructor.
                                   All registered ``MultiKeyAuthority`` bindings
                                   are resolved and registered as trusted issuers.
            jwt_authorities:       ``InstanceProxy[JwtAuthority]`` injected via
                                   ``Instance[JwtAuthority]`` in the caller's
                                   ``@Configuration`` constructor.  All registered
                                   ``JwtAuthority`` bindings are resolved and
                                   registered.  Prefer ``MultiKeyAuthority`` for
                                   new code — ``JwtAuthority`` is the fallback for
                                   simple single-key scenarios.
            include_env:           When ``True`` (default), also loads issuers
                                   from ``FASTREST_AUTHORIZATION__*`` environment
                                   variables before resolving DI authorities.
                                   Set to ``False`` to use only DI-registered
                                   authorities.

        Returns:
            Fully loaded ``TrustedIssuerRegistry``.  All ``AuthoritySource``
            keysets are populated (in-memory, instant); JWKS/OIDC sources from
            env vars have been fetched from the network.

        Raises:
            KeyLoadError: One or more env-var sources (JWKS URL, OIDC) failed
                          to load.  In-memory ``AuthoritySource`` entries never
                          fail.

        Edge cases:
            - No DI bindings for ``MultiKeyAuthority`` → ``resolvable()``
              returns ``False`` — proxy is skipped entirely, no error raised.
            - No DI bindings for ``JwtAuthority`` → same as above.
            - ``include_env=False`` and no DI bindings → empty registry, valid,
              ``verify()`` will always raise ``UnknownKidError``.
            - A label collision between env-var config and a DI-registered
              authority (same derived label) → the DI entry overwrites the env
              entry silently.  Use explicit ``label`` in ``register_authority()``
              calls to avoid this.

        Example::

            from providify import Configuration, Provider, Instance

            @Configuration
            class AuthConfig:
                def __init__(
                    self,
                    multi_key: Instance[MultiKeyAuthority],
                    jwt:        Instance[JwtAuthority],
                ) -> None:
                    self._multi_key = multi_key
                    self._jwt       = jwt

                @Provider(singleton=True)
                async def registry(self) -> TrustedIssuerRegistry:
                    return await TrustedIssuerRegistry.from_container(
                        self._multi_key,
                        self._jwt,
                    )
        """
        # Build the base registry — env vars give external issuers (Google, Auth0…)
        # The DI-registered authorities are added on top.
        registry = cls.from_env() if include_env else cls()

        # Resolve all MultiKeyAuthority bindings — preferred type because it
        # supports zero-downtime key rotation after initial registration.
        # resolvable() is a side-effect-free probe: skips aget_all() entirely
        # when no bindings exist, avoiding a LookupError try/except.
        if multi_key_authorities.resolvable():
            for authority in await multi_key_authorities.aget_all():
                registry.register_authority(authority)

        # Resolve bare JwtAuthority bindings — simpler, single-key case.
        # Less common (users are encouraged to wrap in MultiKeyAuthority for
        # rotation support) but perfectly valid to register directly.
        if jwt_authorities.resolvable():
            for authority in await jwt_authorities.aget_all():
                registry.register_authority(authority)

        # load_all() bootstraps every registered source:
        #   - AuthoritySource → instant (in-memory jwks() call, no I/O)
        #   - JwksUrlSource / OidcDiscoverySource → real HTTP requests
        # Must be called even for AuthoritySource entries because load_all()
        # sets entry._keyset; until that field is set, get_key() finds nothing.
        await registry.load_all()

        return registry

    def __repr__(self) -> str:
        labels = list(self._entries.keys())
        return (
            f"TrustedIssuerRegistry("
            f"issuers={labels!r}, "
            f"loaded={sum(1 for e in self._entries.values() if e._keyset is not None)}/"
            f"{len(self._entries)})"
        )
