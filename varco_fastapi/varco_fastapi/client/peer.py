"""
varco_fastapi.client.peer
============================
``PeerRegistry`` — "consuming another varco service is one env var + one
inject" (Plan 009, Phase 11 / C4).

RD-5 — peer env vars carry references, never secrets. ``VARCO_PEER_<NAME>_URL``
(required), ``_TIMEOUT``, ``_VERIFY``, ``_PROFILE``, ``_CONTRACT`` (path to a
``.contract.json``), and ``_TOKEN_REF``. ``_TOKEN_REF`` is a **reference**
resolved by an injected ``SecretResolver`` hook — the same ``dsn_ref`` shape
as Plan 007 RD-2. A value that looks like a literal credential (starts with
``ey``, contains a 3-segment ``.``-delimited JWT shape, or is > 200 chars)
raises ``ValueError`` at registry construction naming RD-5, with
``allow_literal_secret=True`` as the explicit test/bootstrap escape hatch.

DESIGN: registry owns the per-peer ``CircuitBreaker`` and ``ClientProfile``
    ✅ Shared instances by construction — the parked "per-endpoint circuit
       breaker" idea, landed as a `ClientProfile` recipe exactly as the
       backlog says. One breaker per peer NAME, never per call (CLAUDE.md's
       per-call-breaker pitfall).
    ❌ The registry itself must be a DI singleton — enforced by documentation
       (``bind_peers`` registers it as one).

Thread safety:  ⚠️ Registry construction (``from_env``) happens once at
                   startup; per-peer breaker/client caches are plain dicts,
                   same GIL-is-the-guard reasoning as ``_instrument_cache``.
Async safety:   ✅ All methods here are synchronous; the returned client's
                   own async methods are unaffected.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from providify import DIContainer
    from varco_core.resilience.circuit_breaker import CircuitBreaker

    from varco_fastapi.client.base import AsyncVarcoClient, ClientProfile

_ENV_PREFIX = "VARCO_PEER_"


class SecretResolver(Protocol):
    def resolve(self, ref: str) -> str | None: ...


class _EnvironSecretResolver:
    """Default ``SecretResolver`` — reads ``os.environ[ref]`` verbatim."""

    def resolve(self, ref: str) -> str | None:
        return os.environ.get(ref)


def _looks_like_literal_secret(value: str) -> bool:
    """
    Heuristic literal-credential detector (RD-5).

    A JWT-looking value (``ey``-prefixed base64url header, or a 3-segment
    ``.``-delimited shape) or anything over 200 chars is almost certainly a
    literal credential, not a reference name — refuse it by default.
    """
    if len(value) > 200:
        return True
    if value.startswith("ey"):
        return True
    return value.count(".") == 2


@dataclass(frozen=True)
class PeerConfig:
    """One peer service's resolved configuration."""

    name: str
    url: str
    timeout: float = 30.0
    verify: bool | str = True
    profile_name: str | None = None
    contract_path: str | None = None
    token_ref: str | None = None


class PeerRegistry:
    """
    Registry of peer service configs, with shared per-peer resources
    (``CircuitBreaker``, resolved ``ClientProfile``) and a ``.client()``
    factory.

    Args:
        peers:            Explicit peer configs, keyed by peer name.
        secret_resolver:  Resolves a ``token_ref`` to a raw secret at call
                          time. Defaults to reading ``os.environ[ref]``.
        profiles:         Named ``ClientProfile``s a peer's ``profile_name``
                          may reference. Falls back to the built-in default
                          profile (auth-forward + correlation + OTel + retry
                          + timeout) when unset/unmatched.
        allow_literal_secret: RD-5 escape hatch — skip literal-credential
                          detection on every ``token_ref``.
    """

    def __init__(
        self,
        peers: Mapping[str, PeerConfig] | None = None,
        *,
        secret_resolver: SecretResolver | None = None,
        profiles: Mapping[str, ClientProfile] | None = None,
        allow_literal_secret: bool = False,
    ) -> None:
        self._peers: dict[str, PeerConfig] = dict(peers or {})
        self._secret_resolver = secret_resolver or _EnvironSecretResolver()
        self._profiles = dict(profiles or {})
        self._allow_literal_secret = allow_literal_secret
        self._breakers: dict[str, CircuitBreaker] = {}
        self._clients: dict[tuple[str, type], AsyncVarcoClient[Any]] = {}

        if not allow_literal_secret:
            for cfg in self._peers.values():
                if cfg.token_ref and _looks_like_literal_secret(cfg.token_ref):
                    raise ValueError(
                        f"PeerConfig({cfg.name!r}).token_ref looks like a literal "
                        f"credential, not a reference (RD-5) — VARCO_PEER_"
                        f"{cfg.name.upper()}_TOKEN_REF must name an environment "
                        f"variable/secret reference, never the secret itself. "
                        f"Pass allow_literal_secret=True to override (tests/bootstrap only)."
                    )

    @classmethod
    def from_env(
        cls,
        *,
        environ: Mapping[str, str] | None = None,
        secret_resolver: SecretResolver | None = None,
        profiles: Mapping[str, ClientProfile] | None = None,
        allow_literal_secret: bool = False,
    ) -> PeerRegistry:
        """
        Parse every ``VARCO_PEER_<NAME>_*`` var into a ``PeerConfig``.

        Peer names are upper-snake in env, lower-snake in code
        (``VARCO_PEER_ORDERS_URL`` → peer ``"orders"``).

        Raises:
            ValueError: a peer has any suffix set but no ``_URL`` — a
                half-configured peer is treated as a deploy bug, not a
                partial default.
        """
        env = environ if environ is not None else os.environ

        names: set[str] = set()
        for key in env:
            if not key.startswith(_ENV_PREFIX):
                continue
            rest = key[len(_ENV_PREFIX) :]
            for suffix in (
                "_URL",
                "_TIMEOUT",
                "_VERIFY",
                "_PROFILE",
                "_CONTRACT",
                "_TOKEN_REF",
            ):
                if rest.endswith(suffix):
                    names.add(rest[: -len(suffix)])
                    break

        peers: dict[str, PeerConfig] = {}
        for upper_name in sorted(names):
            peer_name = upper_name.lower()
            url_key = f"{_ENV_PREFIX}{upper_name}_URL"
            url = env.get(url_key)
            if not url:
                raise ValueError(
                    f"{_ENV_PREFIX}{upper_name}_* vars are set but "
                    f"{url_key} is missing — a half-configured peer is a "
                    f"deploy bug. Set {url_key} or remove the other vars."
                )
            timeout_raw = env.get(f"{_ENV_PREFIX}{upper_name}_TIMEOUT")
            verify_raw = env.get(f"{_ENV_PREFIX}{upper_name}_VERIFY")
            verify: bool | str = True
            if verify_raw is not None:
                verify = (
                    verify_raw
                    if verify_raw.lower() not in ("true", "false")
                    else verify_raw.lower() == "true"
                )

            peers[peer_name] = PeerConfig(
                name=peer_name,
                url=url,
                timeout=float(timeout_raw) if timeout_raw else 30.0,
                verify=verify,
                profile_name=env.get(f"{_ENV_PREFIX}{upper_name}_PROFILE"),
                contract_path=env.get(f"{_ENV_PREFIX}{upper_name}_CONTRACT"),
                token_ref=env.get(f"{_ENV_PREFIX}{upper_name}_TOKEN_REF"),
            )

        return cls(
            peers,
            secret_resolver=secret_resolver,
            profiles=profiles,
            allow_literal_secret=allow_literal_secret,
        )

    def names(self) -> tuple[str, ...]:
        return tuple(sorted(self._peers.keys()))

    def config(self, name: str) -> PeerConfig:
        try:
            return self._peers[name]
        except KeyError:
            raise KeyError(f"Unknown peer {name!r}. Known peers: {self.names()}") from None

    def _breaker_for(self, name: str) -> CircuitBreaker:
        """Return (creating once) the shared ``CircuitBreaker`` for peer ``name``."""
        breaker = self._breakers.get(name)
        if breaker is None:
            from varco_core.resilience.circuit_breaker import (
                CircuitBreaker,
                CircuitBreakerConfig,
            )

            breaker = CircuitBreaker(CircuitBreakerConfig())
            self._breakers[name] = breaker
        return breaker

    def _default_profile_for(self, cfg: PeerConfig) -> ClientProfile:
        """
        The "resilience pre-wired" default profile:
        AuthForward → CorrelationId → OTel → Retry(3, 0.2) → Timeout(cfg.timeout).
        """
        from varco_core.resilience.retry import RetryPolicy

        from varco_fastapi.client.base import ClientProfile
        from varco_fastapi.client.middleware import (
            AuthForwardMiddleware,
            CorrelationIdMiddleware,
            OTelClientMiddleware,
            RetryMiddleware,
            TimeoutMiddleware,
        )

        return ClientProfile(
            middleware=(
                AuthForwardMiddleware(),
                CorrelationIdMiddleware(),
                OTelClientMiddleware(),
                RetryMiddleware(RetryPolicy(max_attempts=3, base_delay=0.2)),
                TimeoutMiddleware(cfg.timeout),
            ),
            timeout=cfg.timeout,
        )

    def client(self, name: str, router_cls: type | None = None) -> AsyncVarcoClient[Any]:
        """
        Build (and cache) a client for peer ``name``.

        Args:
            name:       Peer name (lower-snake).
            router_cls: The peer's ``VarcoRouter`` subclass — importable
                        (monorepo) topology. Omit when ``PeerConfig.contract_path``
                        is set instead (cross-repo topology).

        Raises:
            KeyError:   Unknown peer name — message lists known peers.
            ValueError: Neither ``router_cls`` nor ``contract_path`` given.
        """
        cfg = self.config(name)

        cache_key = (name, router_cls or type(None))
        cached = self._clients.get(cache_key)
        if cached is not None:
            return cached

        profile = self._profiles.get(cfg.profile_name or "") or self._default_profile_for(cfg)

        headers: dict[str, str] = {}
        if cfg.token_ref:
            token = self._secret_resolver.resolve(cfg.token_ref)
            if token:
                headers["Authorization"] = f"Bearer {token}"

        if router_cls is not None:
            from varco_fastapi.client.front_door import client_for

            client = client_for(
                router_cls,
                cfg.url,
                profile=profile,
                timeout=cfg.timeout,
                verify=cfg.verify,
                headers=headers or None,
            )
        elif cfg.contract_path:
            from varco_fastapi.contract.runtime import contract_client

            client = contract_client(
                cfg.contract_path,
                cfg.url,
                profile=profile,
                timeout=cfg.timeout,
                verify=cfg.verify,
            )
        else:
            raise ValueError(
                f"PeerRegistry.client({name!r}) needs either router_cls= or "
                f"PeerConfig.contract_path set — neither was given."
            )

        # Attach the shared breaker so callers can wrap request methods with
        # `breaker.protect(...)` (or a future CircuitBreakerMiddleware) — one
        # instance per peer name, never per call (CLAUDE.md pitfall).
        client._circuit_breaker = self._breaker_for(name)  # type: ignore[attr-defined]

        self._clients[cache_key] = client
        return client


def bind_peers(
    container: DIContainer, mapping: Mapping[str, type], *, registry: PeerRegistry | None = None
) -> DIContainer:
    """
    Bind ``AsyncVarcoClient[RouterCls]`` for each peer name → router class.

    Args:
        container: The ``DIContainer`` to register bindings into.
        mapping:   ``{peer_name: router_cls}``.
        registry:  An existing ``PeerRegistry``. Defaults to
                   ``PeerRegistry.from_env()`` — call sites needing a custom
                   ``secret_resolver``/``profiles`` should build one and pass
                   it explicitly.

    Returns:
        The same ``container`` passed in.

    Raises:
        AlreadyBoundError: Delegates to ``bind_clients`` (§D-bind) — a
            non-fallback binding of one of the synthesized client classes
            already exists.
    """
    reg = registry or PeerRegistry.from_env()

    from varco_fastapi.di import bind_clients

    client_classes = []
    for peer_name, router_cls in mapping.items():
        client = reg.client(peer_name, router_cls)
        client_classes.append(type(client))
    return bind_clients(container, *client_classes)


__all__ = ["PeerConfig", "PeerRegistry", "SecretResolver", "bind_peers"]
