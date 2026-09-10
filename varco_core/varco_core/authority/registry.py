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
import logging
import os
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, ClassVar

import jwt as _jwt
from jwt import PyJWK  # PyJWT >= 2.4 — converts JWK dict → crypto key object

if TYPE_CHECKING:
    # Imported only under TYPE_CHECKING — these classes don't import registry.py
    # so there is no circular dependency, but the guard keeps the runtime import
    # graph clean and makes the optional nature of the dependency explicit.
    # Instance is a Providify injection annotation — only needed for type hints
    # in from_container().  Not needed at runtime because InstanceProxy.aget_all()
    # carries the type internally; from_container() never references the class
    # directly at runtime, removing the latent NameError that existed when the
    # code called container.aget_all(MultiKeyAuthority) with that class only
    # imported under TYPE_CHECKING.
    from providify import Instance

    from varco_core.authority.jwt_authority import JwtAuthority
    from varco_core.authority.multi_key_authority import MultiKeyAuthority

from varco_core.authority.exceptions import (
    IssuerNotFoundError,
    KeyLoadError,
    RevocationStoreUnavailableError,
    TokenRevokedError,
    UnknownKidError,
)
from varco_core.authority.sources.protocol import IssuerSource
from varco_core.jwk.model import JsonWebKey, JsonWebKeySet
from varco_core.jwt.model import JsonWebToken, _from_utc_timestamp
from varco_core.jwt.parser import JwtParser
from varco_core.revocation.base import AbstractTokenRevocationStore
from varco_core.revocation.model import RevocationFailureMode, RevocationScope

_registry_logger = logging.getLogger(__name__)

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

    __slots__ = (
        "_entries",
        "_lock",
        "_last_refresh",
        "_min_refresh_interval",
        "_ttl_seconds",
        "_loaded_at",
        "_revocation_store",
        "_refresh_task",
        "_refresh_stop",
        "_refresh_interval",
        "_refresh_in_error",
    )

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

    def __init__(
        self,
        *,
        min_refresh_interval: float | None = None,
        ttl_seconds: float | None = None,
        revocation_store: AbstractTokenRevocationStore | None = None,
    ) -> None:
        """
        Args:
            min_refresh_interval: Rate limit (seconds) between kid-not-found
                triggered global refreshes.  ``None`` (default) reads
                ``VARCO_JWKS_MIN_REFRESH_SECONDS`` (default ``10.0`` —
                identical to pre-Plan-002 behaviour, which hardcoded ``10.0``).
            ttl_seconds: Proactive reload age threshold (Plan 002 C-4).  When
                the cached keyset's age exceeds this many seconds,
                ``get_key()`` refreshes all sources BEFORE searching, instead
                of waiting for a kid-miss.  ``None`` (default) reads
                ``VARCO_JWKS_TTL_SECONDS`` (default ``0.0`` — disabled,
                identical to pre-Plan-002 behaviour: only kid-miss triggers
                a refresh).
            revocation_store: Optional ``AbstractTokenRevocationStore``
                (Plan 034 / S13, §D-S13-hook). ``None`` (the default) means
                **no check, no await, no cost** — zero-config ``verify()``
                behaviour is byte-identical to before this parameter
                existed. Binding a store in DI (``enable_token_revocation``/
                ``enable_redis_token_revocation``) does NOT by itself wire
                it here — that binding must be passed to this constructor
                explicitly (§D-S13-di's two-step; ``varco_core`` never
                reaches for ``DIContainer.current()``).
        """
        # label → TrustedIssuerEntry
        self._entries: dict[str, TrustedIssuerEntry] = {}

        # asyncio.Lock created lazily — must be inside a running event loop
        self._lock: asyncio.Lock | None = None

        # Rate-limit for kid-not-found global refresh
        self._last_refresh: float = 0.0
        self._min_refresh_interval: float = (
            min_refresh_interval
            if min_refresh_interval is not None
            else float(os.environ.get("VARCO_JWKS_MIN_REFRESH_SECONDS", "10.0"))
        )

        # Plan 002 C-4 — proactive age-based reload knobs (0.0 = disabled).
        self._ttl_seconds: float = (
            ttl_seconds
            if ttl_seconds is not None
            else float(os.environ.get("VARCO_JWKS_TTL_SECONDS", "0.0"))
        )
        # Monotonic timestamp of the last successful full load/refresh.
        # 0.0 sentinel = "never loaded" — _should_proactively_reload() never
        # fires from the initial (unloaded) state.
        self._loaded_at: float = 0.0

        # Plan 034 / S13 — None means "no check, no await, no cost".
        self._revocation_store = revocation_store

        # Plan 041 / S22, §D-S22-loop — background JWKS refresher state.
        # DESIGN: lazy construction, same rule as self._lock above.
        #   ✅ No asyncio.Event/Task is created here — only inside start_refresh(),
        #      which is `async def` and therefore always has a running loop.
        #   ✅ A registry constructed and never `start_refresh()`ed (the default —
        #      ttl_seconds=0.0) carries zero background-task footprint.
        self._refresh_task: asyncio.Task[None] | None = None
        self._refresh_stop: asyncio.Event | None = None
        self._refresh_interval: float = 0.0
        # §D-S22-failure — log-once-per-transition latch, same shape as
        # StatPollWatcher._in_error (varco_core/watch/poll.py:73).
        self._refresh_in_error: bool = False

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
            KeyError: Never raised — replaces any existing entry with the same
                label silently (consistent with ``register()`` behaviour).

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

        # A full load_all() counts as "freshly loaded" for BOTH the TTL age
        # check (Plan 002 C-4) and the kid-miss rate limit — otherwise an
        # immediate get_key() miss right after startup would trigger a
        # redundant second refresh (the sources were just fetched).
        now = time.monotonic()
        self._loaded_at = now
        self._last_refresh = now

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

        First checks whether the cached keysets have aged past
        ``ttl_seconds`` (Plan 002 C-4) and, if so, proactively refreshes all
        sources BEFORE searching at all.  Otherwise (the pre-Plan-002
        behaviour, ``ttl_seconds=0`` default): searches the in-memory cached
        keysets; on a miss, triggers a rate-limited reactive refresh of all
        sources (kid-not-found = key rotation signal from a remote issuer)
        and searches again.

        Args:
            kid: Key ID to look up.

        Returns:
            The first matching ``JsonWebKey``, or ``None`` if not found after
            any refresh performed.

        Edge cases:
            - If the global refresh rate limit blocks the re-fetch, returns
              ``None`` rather than waiting — callers should treat this as
              "key not found right now".
            - The same kid may theoretically appear in multiple issuers'
              keysets — the first match wins (first registered issuer).
            - A proactive TTL-triggered reload does NOT also run the
              reactive kid-miss refresh afterward — the cache was just
              refreshed, so falling through would double-fetch every
              remote source on every call once the cache goes stale.
        """
        # Delegates to _resolve_key (Plan 005, Phase 2) — identical resolution
        # logic, this method just discards the matched TrustedIssuerEntry.
        # Public signature is unchanged.
        found = await self._resolve_key(kid)
        return found[0] if found is not None else None

    def _search_caches(self, kid: str) -> JsonWebKey | None:
        """Search every registered entry's cached keyset for ``kid``."""
        for entry in self._entries.values():
            key = entry.find_key(kid)
            if key is not None:
                return key
        return None

    def _search_caches_with_entry(self, kid: str) -> tuple[JsonWebKey, TrustedIssuerEntry] | None:
        """Same as ``_search_caches`` but also returns the matched entry —
        used by ``_resolve_key`` to recover which issuer's ``iss`` claim to
        enforce (Plan 005, Phase 2)."""
        for entry in self._entries.values():
            key = entry.find_key(kid)
            if key is not None:
                return key, entry
        return None

    async def _resolve_key(self, kid: str) -> tuple[JsonWebKey, TrustedIssuerEntry] | None:
        """
        Find a ``JsonWebKey`` **and** the ``TrustedIssuerEntry`` it resolved
        from — the entry carries the ``iss`` value ``verify()`` must enforce
        (Plan 005, Phase 2 / U-13).

        This is ``get_key()``'s resolution logic, generalised to also return
        which issuer matched — ``get_key()``'s public signature is unchanged
        and now delegates here, discarding the entry.

        Returns:
            ``(key, entry)`` on a match, ``None`` on a miss (identical miss
            semantics to ``get_key()``).
        """
        if self._should_proactively_reload():
            async with self._get_lock():
                if self._should_proactively_reload():
                    await self._refresh_all_sources()
            return self._search_caches_with_entry(kid)

        found = self._search_caches_with_entry(kid)
        if found is not None:
            return found

        now = time.monotonic()
        if now - self._last_refresh < self._min_refresh_interval:
            return None

        async with self._get_lock():
            found = self._search_caches_with_entry(kid)
            if found is not None:
                return found
            await self._refresh_all_sources()

        return self._search_caches_with_entry(kid)

    def _should_proactively_reload(self) -> bool:
        """
        Return ``True`` iff a TTL-based proactive reload is due.

        ``ttl_seconds <= 0`` (the default) always returns ``False`` — the
        pre-Plan-002 behaviour (reactive-only refresh on kid-miss).
        ``_loaded_at == 0.0`` (never loaded at all) also returns ``False`` —
        there is nothing to consider "stale" yet; the reactive kid-miss path
        handles the very first load.
        """
        if self._ttl_seconds <= 0 or self._loaded_at <= 0.0:
            return False
        return (time.monotonic() - self._loaded_at) >= self._ttl_seconds

    async def _refresh_all_sources(self) -> None:
        """
        Refresh every registered source concurrently, tolerating individual
        failures (stale cache is better than a hard error during token
        verification), and stamp ``_last_refresh``/``_loaded_at``.
        """
        results = await asyncio.gather(
            *(entry.source.refresh() for entry in self._entries.values()),
            return_exceptions=True,
        )

        for entry, result in zip(self._entries.values(), results):
            if not isinstance(result, BaseException):
                entry._keyset = result
            # Silently skip failed refreshes — stale cache is better than
            # a hard error during token verification.

        now = time.monotonic()
        self._last_refresh = now
        self._loaded_at = now

    # ── Background refresh (Plan 041 / S22) ──────────────────────────────────

    @property
    def refresh_running(self) -> bool:
        """
        Whether the background refresher task is currently running.

        Returns:
            ``True`` iff ``start_refresh()`` created a task that has not since
            completed/been stopped. ``False`` before the first ``start_refresh()``
            call, after ``stop_refresh()``, or when the effective period was
            ``<= 0`` (no task was ever created).
        """
        return self._refresh_task is not None and not self._refresh_task.done()

    @property
    def refresh_interval(self) -> float:
        """
        The background refresher's effective tick period, in seconds.

        Returns:
            ``0.0`` when the refresher has never run (the "off" sentinel —
            matches ``ttl_seconds``'s own "0.0 = disabled" convention). Once
            ``start_refresh()`` has run at least once, this is the resolved,
            possibly-clamped period from that call — it is **not** reset to
            ``0.0`` by ``stop_refresh()``, so a caller can inspect what period
            was last used even after stopping.
        """
        return self._refresh_interval

    async def start_refresh(self, *, interval: float | None = None) -> None:
        """
        Start a background task that periodically calls ``_refresh_all_sources()``.

        This makes ``ttl_seconds``/``min_refresh_interval`` deliver their
        documented intent even when no ``verify()`` call ever arrives — see
        ``varco_core.authority.registry`` module docstring and
        ``technical_docs/features/jwt-claim-transformer.md`` for the full
        design (§D-S22-seam/§D-S22-interval/§D-S22-loop/§D-S22-failure).

        Args:
            interval: Explicit tick period in seconds. ``None`` (default)
                uses ``self._ttl_seconds`` (§D-S22-interval — the knob
                already means "the age at which the cached keyset is
                considered stale"; ticking at that period delivers exactly
                that intent). A resolved period ``<= 0`` means "off" — no
                task is created, and this is a silent no-op (the same
                off-by-default posture as ``ttl_seconds=0.0`` itself). A
                resolved period below ``min_refresh_interval`` is clamped up
                to it (a period below it provably produces ticks that do
                nothing at all — ``JwksUrlSource.refresh()`` returns the
                cache without a network call inside its own
                ``min_refresh_interval``) and logs one WARNING naming both
                values.

        Returns:
            None.

        Raises:
            Never raises — a misconfigured period is clamped, not rejected;
            an already-running refresher makes this call an idempotent no-op.

        Edge cases:
            - Called twice: idempotent — the second call is a no-op and the
              original task keeps running with its original period.
            - Zero registered issuers: still creates a task (if the period is
              positive) — each tick's ``_refresh_all_sources()`` gathers
              nothing and stamps timestamps, which is harmless.
            - Never calls ``load_all()`` — a down issuer at construction time
              must not prevent the refresher from starting (§D-S22-failure);
              the initial load remains the caller's own explicit
              ``await registry.load_all()``.

        Async safety: ✅ ``async def`` — always has a running event loop, so
            the ``asyncio.Event``/``asyncio.Task`` created here are always
            constructed inside it (the same lazy-primitive rule as
            ``_get_lock()``).
        """
        if self.refresh_running:
            return  # idempotent — one task per registry

        effective_interval = interval if interval is not None else self._ttl_seconds
        if effective_interval <= 0:
            return  # "off" — no task, no log noise beyond DEBUG

        if effective_interval < self._min_refresh_interval:
            _registry_logger.warning(
                "TrustedIssuerRegistry.start_refresh: requested interval=%.3fs is below "
                "min_refresh_interval=%.3fs — clamping up to min_refresh_interval, because "
                "a tick faster than it would re-read the same cache without a network call.",
                effective_interval,
                self._min_refresh_interval,
            )
            effective_interval = self._min_refresh_interval

        self._refresh_interval = effective_interval
        self._refresh_stop = asyncio.Event()
        self._refresh_task = asyncio.create_task(self._refresh_loop())

    async def stop_refresh(self) -> None:
        """
        Stop the background refresher task, if running.

        Idempotent — safe to call before ``start_refresh()`` and safe to call
        twice. Always awaits the cancelled task, so no orphaned task survives
        this call (§D-S22-loop — "no `Task was destroyed but it is pending`").

        Returns:
            None.

        Edge cases:
            - A tick mid-fetch when this is called: the task is cancelled and
              awaited; the in-flight fetch is abandoned, and the resulting
              ``CancelledError`` is swallowed here only (never inside
              ``_refresh_loop`` itself, which re-raises it).

        Async safety: ✅ Sets the stop event, then cancels and awaits the
            task — the same set→cancel→await→swallow shape as
            ``AbstractPathWatcher.stop()`` (``varco_core/watch/base.py``).
        """
        if self._refresh_stop is not None:
            self._refresh_stop.set()
        task, self._refresh_task = self._refresh_task, None
        if task is None:
            return
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass  # expected — stop_refresh() owns cancellation

    async def _refresh_loop(self) -> None:
        """
        Background tick loop — calls ``_refresh_all_sources()`` on a fixed
        period until ``stop_refresh()`` is called.

        DESIGN: one task for all issuers, no retry inside a tick (§D-S22-loop,
        §D-S22-failure)
            ✅ ``_refresh_all_sources()`` already gathers with
               ``return_exceptions=True`` and commits only successes — one
               dead issuer cannot stop another from refreshing, and a task
               per issuer would buy isolation that already exists.
            ✅ The periodic loop *is* the retry cadence — an immediate retry
               inside a tick would be a provable no-op:
               ``JwksUrlSource.refresh()`` returns the cached keyset without
               a network call inside its own ``min_refresh_interval``, so a
               second attempt in the same window "retries" by re-reading the
               same cache.
            ❌ A pathologically slow issuer delays the next tick for all of
               them. Bounded in practice — ``JwksUrlSource`` carries its own
               ``timeout`` (default 10s).
            ❌ No exponential backoff for a permanently dead issuer — the
               rate is the operator's own resolved interval, and a JWKS
               fetch is a single small GET.

        The whole tick body is wrapped in ``try/except Exception`` so no
        failure — including one raised by ``_refresh_all_sources()`` itself,
        which should not happen given its own internal ``return_exceptions``,
        but this loop must never die regardless — can ever kill the loop.
        ``asyncio.CancelledError`` is deliberately NOT caught here; it must
        propagate so ``stop_refresh()``'s ``await task`` observes it.

        Async safety: ✅ ``asyncio.wait_for(stop_event.wait(), timeout=period)``
            means shutdown is immediate on ``stop_refresh()``, never up to one
            period late (the same shape as ``StatPollWatcher._run()``).
        """
        assert self._refresh_stop is not None  # set by start_refresh()
        stop_event = self._refresh_stop

        while not stop_event.is_set():
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=self._refresh_interval)
                break  # stop_refresh() was called during the sleep
            except TimeoutError:
                pass  # normal tick

            if stop_event.is_set():
                break

            try:
                await self._refresh_all_sources()
            except Exception:  # noqa: BLE001 — the loop must never die
                if not self._refresh_in_error:
                    _registry_logger.warning(
                        "TrustedIssuerRegistry: background JWKS refresh tick failed",
                        exc_info=True,
                    )
                    self._refresh_in_error = True
                continue

            if self._refresh_in_error:
                _registry_logger.info(
                    "TrustedIssuerRegistry: background JWKS refresh tick recovered"
                )
                self._refresh_in_error = False

    # ── Verification ──────────────────────────────────────────────────────────

    async def verify(
        self,
        token_str: str,
        *,
        audience: str | list[str] | None = None,
        leeway: float | None = None,
        enforce_issuer: bool | None = None,
        check_revocation: bool | None = None,
        revocation_require_jti: bool | None = None,
        revocation_failure_mode: RevocationFailureMode | str | None = None,
    ) -> JsonWebToken:
        """
        Verify a JWT string against all registered issuers' public keys.

        Routing is by ``kid`` header claim.  The first registered issuer that
        holds a key with the matching kid is used for verification.

        **Enforces the ``iss`` claim by default** (Plan 005, Phase 2 / U-13 —
        a BREAKING security-default change from earlier releases): after
        signature verification, the token's ``iss`` claim is compared against
        the ``TrustedIssuerEntry.iss`` of the issuer whose key matched the
        token's ``kid``. A mismatch means the token claims to be from a
        different issuer than the one whose key actually signed it — the
        exact "misrouted/forged iss" hole this closes.

        Args:
            token_str:      Raw JWT string.
            audience:       Expected ``aud`` value(s).  ``None`` skips audience check.
            leeway:         Clock-skew leeway in seconds for ``exp``/``nbf`` checks
                            (Plan 002 C-1).  ``None`` (default) reads
                            ``VARCO_JWT_LEEWAY_SECONDS`` (default ``0.0`` — no
                            leeway, today's behaviour).
            enforce_issuer: Whether to check ``iss`` against the resolved
                            issuer's registered value.  ``None`` (default)
                            reads ``JwtVerificationSettings.enforce_issuer``
                            (env ``VARCO_JWT_ENFORCE_ISS``, default ``True``).
                            Pass ``False`` (or set the env var to ``false``)
                            to restore the pre-Phase-2 behaviour.
            check_revocation: Whether to consult ``self._revocation_store``
                            (Plan 034 / S13, §D-S13-hook). ``None`` (default)
                            reads ``JwtVerificationSettings.revocation_enabled``
                            (default ``True``). Has no effect at all when no
                            store is bound — pass ``False`` as an explicit,
                            per-call bypass (e.g. for an internal health
                            check route).
            revocation_require_jti: Per-call override of
                            ``JwtVerificationSettings.revocation_require_jti``
                            (§D-S13-jti). ``None`` (default) reads the
                            setting (default ``False`` — Auth0/Keycloak/
                            Cognito do not emit ``jti`` by default, brief
                            009 §2).
            revocation_failure_mode: Per-call override of
                            ``JwtVerificationSettings.revocation_failure_mode``
                            (§D-S13-fail). ``None`` (default) reads the
                            setting (default ``FAIL_CLOSED``).

        Returns:
            ``JsonWebToken`` with all claims populated.

        Raises:
            UnknownKidError:            Token has no ``kid`` header, or no
                                        registered issuer has a key for it.
            jwt.ExpiredSignatureError:  Token has passed its ``exp`` time.
            jwt.InvalidSignatureError:  Signature verification failed.
            jwt.DecodeError:            Token is malformed.
            TokenRevokedError:          The resolved revocation store reports
                                        the token as revoked (``jti``
                                        denylist, or a ``SUBJECT``/``TENANT``/
                                        ``ISSUER`` watermark), or
                                        ``revocation_require_jti`` is in
                                        effect and the token has no ``jti``.
            RevocationStoreUnavailableError: The bound store raised during
                                        ``is_revoked()`` and
                                        ``RevocationFailureMode.FAIL_CLOSED``
                                        is in effect. Mapped to HTTP 503 by
                                        ``JwtBearerAuth`` — an outage, not a
                                        bad credential.
            jwt.InvalidAudienceError:   ``aud`` mismatch when ``audience``
                                        is provided.
            jwt.InvalidIssuerError:     ``iss`` claim does not match the
                                        resolved issuer's registered ``iss``
                                        (only when ``enforce_issuer=True``).

        Edge cases:
            - A token with no ``kid`` header raises ``UnknownKidError`` because
              routing by kid is the only strategy — brute-force trying all keys
              would be a timing oracle vulnerability.
            - If multiple registered issuers share the same kid (misconfiguration),
              the first-registered one's key is used — and the ``iss`` check now
              catches an `iss` mismatch this previously let through silently.
            - ``enforce_issuer=False`` is an explicit, auditable opt-out — prefer
              it over disabling the check globally when only one caller needs
              the legacy behaviour.
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

        resolved = await self._resolve_key(kid)

        if resolved is None:
            registered = list(self._entries.keys())
            raise UnknownKidError(
                f"No registered issuer has a key with kid={kid!r}. "
                f"Registered issuers: {registered}. "
                f"Check FASTREST_AUTHORIZATION__* env vars — the issuer may not be "
                f"configured, or the remote JWKS may not yet contain this kid.",
                kid=kid,
            )
        jwk, matched_entry = resolved

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

        if leeway is None:
            from varco_core.jwt.config import JwtVerificationSettings

            leeway = JwtVerificationSettings.from_env().leeway_seconds

        # verify_iat=False (PyJWT >= 2.10 added this check, default True):
        # varco's own revocation watermark rule (§D-S13-nvb) deliberately
        # allows a future `iat` (clock skew between issuer and verifier —
        # "not revoked" is the documented Edge case) and interprets it
        # itself; PyJWT's blanket ImmatureSignatureError would reject such
        # a token before it ever reaches that logic, which is stricter than
        # any behaviour varco has ever documented for `iat`.
        decode_options: dict[str, Any] = {"verify_iat": False}
        decode_kwargs: dict[str, Any] = {
            "algorithms": [pyjwk.algorithm_name],
            "leeway": leeway,
        }
        if audience is not None:
            decode_kwargs["audience"] = audience
        else:
            # PyJWT defaults verify_aud=True: a token that happens to carry
            # an "aud" claim would raise InvalidAudienceError even though no
            # expected audience was ever configured. D-17 ("audience=None
            # means NOT enforced") requires explicitly disabling aud
            # verification in this case — otherwise "not enforced" would
            # only be true for tokens that happen to omit "aud" entirely.
            decode_options["verify_aud"] = False
        decode_kwargs["options"] = decode_options

        # Delegate to PyJWT for the actual signature + claims verification.
        # Any jwt.exceptions.* propagates unchanged — callers may catch them.
        raw = _jwt.decode(token_str, pyjwk.key, **decode_kwargs)

        # Plan 005, Phase 2 / U-13 — fail-closed issuer enforcement.
        # Resolve the effective flag: explicit arg > JwtVerificationSettings
        # (env VARCO_JWT_ENFORCE_ISS, default True).
        effective_enforce_issuer = enforce_issuer
        if effective_enforce_issuer is None:
            from varco_core.jwt.config import JwtVerificationSettings

            effective_enforce_issuer = JwtVerificationSettings.from_env().enforce_issuer

        if effective_enforce_issuer:
            token_iss = raw.get("iss")
            if token_iss != matched_entry.iss:
                raise _jwt.InvalidIssuerError(
                    f"Token 'iss' claim {token_iss!r} does not match the "
                    f"registered issuer {matched_entry.iss!r} whose key "
                    f"(kid={kid!r}) signed this token. "
                    f"Pass enforce_issuer=False or set "
                    f"VARCO_JWT_ENFORCE_ISS=false to opt out."
                )

        # Plan 034 / S13, §D-S13-hook — revocation check, AFTER iss
        # enforcement (§D-S13-order): a forged/misrouted token must fail on
        # its signature/issuer, never reach the store. This also means an
        # unauthenticated request never costs a store round trip.
        if self._revocation_store is not None:
            effective_check_revocation = check_revocation
            if effective_check_revocation is None:
                from varco_core.jwt.config import JwtVerificationSettings

                effective_check_revocation = JwtVerificationSettings.from_env().revocation_enabled

            if effective_check_revocation:
                await self._check_revocation(
                    raw,
                    require_jti=revocation_require_jti,
                    failure_mode=revocation_failure_mode,
                )

        # Reuse JwtParser's claim reconstruction — AuthContext, timestamps, etc.
        return JwtParser._from_raw_claims(raw)

    async def _check_revocation(
        self,
        raw: dict[str, Any],
        *,
        require_jti: bool | None,
        failure_mode: RevocationFailureMode | str | None,
    ) -> None:
        """
        Consult ``self._revocation_store`` for the already-verified claims.

        Called only after signature + ``iss`` enforcement have both
        succeeded (§D-S13-order) and only when a store is actually bound
        (§D-S13-hook) — callers never pay this cost otherwise.

        Args:
            raw:          The raw, verified claim dict from ``_jwt.decode()``.
            require_jti:  Per-call override of
                          ``JwtVerificationSettings.revocation_require_jti``.
            failure_mode: Per-call override of
                          ``JwtVerificationSettings.revocation_failure_mode``.

        Raises:
            TokenRevokedError:               The store reports the token
                                              revoked, or ``require_jti`` is
                                              in effect and the token has no
                                              ``jti``.
            RevocationStoreUnavailableError: The store raised and
                                              ``FAIL_CLOSED`` is in effect.

        Edge cases:
            - ``tenant_id`` is read from the token's own ``tenant_id``
              claim, **never** ``current_tenant()`` (§D-S13-scope) — this
              runs before any ambient tenant is necessarily resolved, and a
              compromised token must not be able to dodge a tenant kill
              switch by being presented on a request that resolves a
              different ambient tenant.
        """
        from varco_core.jwt.config import JwtVerificationSettings

        settings = JwtVerificationSettings.from_env()
        effective_require_jti = (
            require_jti if require_jti is not None else settings.revocation_require_jti
        )
        effective_failure_mode = (
            RevocationFailureMode(failure_mode)
            if failure_mode is not None
            else settings.revocation_failure_mode
        )

        jti = raw.get("jti")
        if effective_require_jti and jti is None:
            raise TokenRevokedError(
                scope=RevocationScope.TOKEN,
                key="<no-jti>",
                reason="revocation_require_jti=True and token has no jti claim",
            )

        iss = raw.get("iss")
        sub = raw.get("sub")
        subject_key = f"{iss}|{sub}" if iss is not None and sub is not None else None
        tenant_id = raw.get("tenant_id")
        iat_ts = raw.get("iat")
        issued_at = _from_utc_timestamp(iat_ts) if iat_ts is not None else None

        assert self._revocation_store is not None  # narrowed by caller
        try:
            verdict = await self._revocation_store.is_revoked(
                jti=jti,
                subject=subject_key,
                issuer=iss,
                tenant_id=tenant_id,
                issued_at=issued_at,
            )
        except Exception as exc:
            if effective_failure_mode == RevocationFailureMode.FAIL_OPEN:
                _registry_logger.error(
                    "TrustedIssuerRegistry: revocation store %s raised during "
                    "is_revoked() — FAIL_OPEN in effect, verification proceeds: %s",
                    type(self._revocation_store).__name__,
                    exc,
                )
                return
            raise RevocationStoreUnavailableError(
                "Token verification is temporarily unavailable (revocation store error)."
            ) from exc

        if verdict.revoked:
            raise TokenRevokedError(
                scope=verdict.scope,  # type: ignore[arg-type]
                key=verdict.key or "",
                reason=verdict.reason,
            )

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

    # The CA env var names that, if ANY is non-empty, trigger building an ssl_context for
    # from_env()'s URL-based issuer sources (Plan 026 / T5) — exactly the set
    # varco_core.tls.TrustStore.from_env() itself reads (VARCO_* names + the additive
    # SSL_CERT_FILE/SSL_CERT_DIR pair, §D-T3-env).
    _CA_TRIGGER_ENV_VARS: ClassVar[tuple[str, ...]] = (
        "VARCO_TRUST_STORE_DIR",
        "VARCO_CA_CERT",
        "VARCO_CLIENT_CERT",
        "VARCO_CLIENT_KEY",
        "SSL_CERT_FILE",
        "SSL_CERT_DIR",
    )

    @classmethod
    def from_env(
        cls, *, revocation_store: AbstractTokenRevocationStore | None = None
    ) -> TrustedIssuerRegistry:
        """
        Construct a ``TrustedIssuerRegistry`` from environment variables.

        Reads ``FASTREST_AUTHORIZATION__<LABEL>__URL`` and
        ``FASTREST_AUTHORIZATION__<LABEL>__ISS`` pairs from the environment.

        Also builds an ``ssl.SSLContext`` for the two URL-based issuer source kinds
        (``jwks::``/``oidc::``, Plan 026 / T5) from ``varco_core.tls.TrustStore.from_env()`` —
        but **only when at least one of the CA env vars it reads is actually set**
        (``VARCO_TRUST_STORE_DIR``, ``VARCO_CA_CERT``, ``VARCO_CLIENT_CERT``,
        ``VARCO_CLIENT_KEY``, ``SSL_CERT_FILE``, ``SSL_CERT_DIR``). With none of them set,
        ``ssl_context`` stays ``None`` — byte-identical to pre-Plan-026 behaviour
        (``urlopen(..., context=None)``, the stdlib default).

        Returns:
            Populated ``TrustedIssuerRegistry``.  Call ``load_all()`` after
            construction to fetch remote keysets.

        Example env vars::

            FASTREST_AUTHORIZATION__SYSTEM_SVC__URL = pem::/etc/certs/system.pem
            FASTREST_AUTHORIZATION__SYSTEM_SVC__ISS = system-svc
            FASTREST_AUTHORIZATION__GOOGLE__URL = https://accounts.google.com
            FASTREST_AUTHORIZATION__GOOGLE__ISS = https://accounts.google.com

        Args:
            revocation_store: Optional ``AbstractTokenRevocationStore``
                (Plan 034 / S13). **No env var constructs a store** — a
                store is an object with a connection, not a string; pass
                an already-constructed one explicitly, e.g. one obtained
                from DI (``enable_token_revocation``/
                ``enable_redis_token_revocation``).
        """
        from varco_core.authority.config import AuthorizationConfig
        from varco_core.tls.store import TrustStore

        ssl_context = None
        if any(os.environ.get(name) for name in cls._CA_TRIGGER_ENV_VARS):
            ssl_context = TrustStore.from_env().build_ssl_context()

        registry = AuthorizationConfig.from_env().to_registry(ssl_context=ssl_context)
        registry._revocation_store = revocation_store
        return registry

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
            for jwt_authority in await jwt_authorities.aget_all():
                registry.register_authority(jwt_authority)

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
