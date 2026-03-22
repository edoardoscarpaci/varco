"""
varco_core.authority.config
================================

``AuthorizationConfig`` — parses ``FASTREST_AUTHORIZATION__*`` environment
variables and constructs a ``TrustedIssuerRegistry``.

Environment variable format
----------------------------
Each trusted issuer requires two env vars:

    FASTREST_AUTHORIZATION__<LABEL>__URL = <source descriptor>
    FASTREST_AUTHORIZATION__<LABEL>__ISS = <iss claim value>

``<LABEL>`` is an arbitrary uppercase identifier (the env var suffix).
``__URL`` and ``__ISS`` are fixed field suffixes.

Source descriptors (``__URL`` value)
--------------------------------------
    ``pem::/path/key.pem``              — single PEM file
    ``pem-folder::/path/keys/``         — directory of PEM files
    ``jwks::https://…/.well-known/…``   — remote JWKS endpoint
    ``oidc::https://accounts.google.com`` — OIDC issuer (auto-discover JWKS)
    ``/path/key.pem``                   — heuristic: local path → PemFileSource
    ``https://accounts.google.com``     — heuristic: HTTPS URL → OidcDiscoverySource

``__ISS`` fallback
-------------------
When ``__ISS`` is missing, the label is normalised to a guessed issuer:
``SYSTEM_SVC`` → ``"system-svc"``.  This is a convenience for internal
issuers where the iss claim matches the label.  For external providers
(Google, Auth0), always specify ``__ISS`` explicitly.

DESIGN: plain class + os.environ over pydantic-settings
    ✅ No extra dependency — pydantic-settings is not in pyproject.toml.
    ✅ Simple parsing logic — the env var schema is deliberately flat.
    ❌ No type coercion or validation beyond what the source factory provides.
    Alternative considered: pydantic-settings with nested model.  Rejected
    to avoid adding a hard dependency for what is essentially string splitting.

Thread safety:  ✅ from_env() reads env vars once; the resulting config is
                   immutable after construction.
Async safety:   ✅ No I/O — to_registry() is synchronous construction only.
                   Network fetches happen later in registry.load_all().
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from varco_core.authority.sources.factory import IssuerSourceFactory

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from varco_core.authority.registry import TrustedIssuerRegistry


# ── Constants ─────────────────────────────────────────────────────────────────

# Env var prefix — all FASTREST_AUTHORIZATION__* vars are scanned
_ENV_PREFIX: str = "FASTREST_AUTHORIZATION__"

# Field suffixes — what comes after <LABEL>__
_FIELD_URL: str = "URL"
_FIELD_ISS: str = "ISS"


# ── IssuerConfig (internal value object) ─────────────────────────────────────


@dataclass(frozen=True)
class IssuerConfig:
    """
    Parsed configuration for a single trusted issuer.

    Attributes:
        label:     The env var label (e.g. ``"GOOGLE"``, ``"SYSTEM_SVC"``).
        url_value: Raw source descriptor string (passed to ``IssuerSourceFactory``).
        iss:       Expected ``iss`` claim value.

    Thread safety:  ✅ Frozen dataclass — immutable after construction.
    """

    label: str
    url_value: str
    iss: str


# ── AuthorizationConfig ───────────────────────────────────────────────────────


@dataclass(frozen=True)
class AuthorizationConfig:
    """
    Parsed representation of all ``FASTREST_AUTHORIZATION__*`` env vars.

    Use ``from_env()`` to construct from the process environment, then
    ``to_registry()`` to produce a ready-to-use ``TrustedIssuerRegistry``.

    Thread safety:  ✅ Frozen — immutable after construction.
    Async safety:   ✅ to_registry() is pure construction — no I/O.

    Attributes:
        issuers: Tuple of parsed issuer configs in the order they were
                 discovered in the environment.

    Edge cases:
        - An empty environment (no ``FASTREST_AUTHORIZATION__*`` vars) →
          empty ``issuers`` tuple → empty registry (valid).
        - A label with ``__URL`` but no ``__ISS`` → ``iss`` is inferred by
          normalising the label (uppercase → lowercase, ``_`` → ``-``).
        - A label with ``__ISS`` but no ``__URL`` → silently ignored.
        - Extra field suffixes (e.g. ``__COMMENT``) are silently ignored.

    Example::

        config = AuthorizationConfig.from_env()
        registry = config.to_registry()
        await registry.load_all()
    """

    issuers: tuple[IssuerConfig, ...] = field(default_factory=tuple)

    # ── Construction ──────────────────────────────────────────────────────────

    @classmethod
    def from_env(cls) -> AuthorizationConfig:
        """
        Parse all ``FASTREST_AUTHORIZATION__*`` env vars from the environment.

        Scans ``os.environ`` for vars matching the prefix, groups them by
        label (the middle segment between the prefix and the field suffix),
        and builds ``IssuerConfig`` for each complete label.

        Returns:
            Parsed ``AuthorizationConfig``.

        Edge cases:
            - Env var values are stripped of leading/trailing whitespace.
            - Labels are case-preserved (GOOGLE ≠ google).
            - Vars whose name does not contain a second ``__`` separator are
              silently ignored (e.g. ``FASTREST_AUTHORIZATION__ORPHAN``).
        """
        # Group env vars by label → {field_suffix → value}
        # e.g. {"GOOGLE": {"URL": "https://accounts.google.com", "ISS": "..."}}
        groups: dict[str, dict[str, str]] = {}

        for key, value in os.environ.items():
            if not key.startswith(_ENV_PREFIX):
                continue

            # Strip the prefix → "SYSTEM_SVC__URL" or "GOOGLE__ISS" etc.
            rest = key[len(_ENV_PREFIX) :]

            # Must have at least one more __ to split on
            if "__" not in rest:
                continue

            # Split on the LAST __ to isolate the field suffix.
            # This handles labels that themselves contain __ (unlikely but safe).
            label, field_suffix = rest.rsplit("__", 1)

            groups.setdefault(label, {})[field_suffix] = value.strip()

        # Build IssuerConfig for each label that has at least a URL
        configs: list[IssuerConfig] = []

        for label, fields in groups.items():
            url_value = fields.get(_FIELD_URL)
            if url_value is None:
                # A label with ISS but no URL is meaningless — skip it
                continue

            iss = fields.get(_FIELD_ISS)
            if iss is None:
                # Fallback: normalise label to a guessed iss value.
                # SYSTEM_SVC → "system-svc"
                # GOOGLE     → "google"
                iss = label.lower().replace("_", "-")

            configs.append(IssuerConfig(label=label, url_value=url_value, iss=iss))

        return cls(issuers=tuple(configs))

    def to_registry(self) -> TrustedIssuerRegistry:
        """
        Construct a ``TrustedIssuerRegistry`` from this config.

        For each ``IssuerConfig``, calls ``IssuerSourceFactory.from_string()``
        to build the appropriate source, then registers it in the registry.

        Returns:
            Populated ``TrustedIssuerRegistry``.  Keysets are NOT loaded yet —
            call ``await registry.load_all()`` to fetch remote JWKS endpoints.

        Raises:
            ValueError: A source descriptor is unrecognised or empty.

        Edge cases:
            - The registry is returned immediately without any network I/O.
            - ``from_env()`` followed by ``to_registry()`` followed by
              ``await registry.load_all()`` is the standard startup sequence.
        """
        # Import here to avoid circular import at module load time.
        # registry.py imports from config.py (from_env classmethod);
        # config.py imports from registry.py only in to_registry().
        from varco_core.authority.registry import TrustedIssuerRegistry

        registry = TrustedIssuerRegistry()

        for issuer_config in self.issuers:
            source = IssuerSourceFactory.from_string(issuer_config.url_value)
            registry.register(
                label=issuer_config.label,
                iss=issuer_config.iss,
                source=source,
            )

        return registry

    def __repr__(self) -> str:
        labels = [c.label for c in self.issuers]
        return f"AuthorizationConfig(issuers={labels!r})"
