"""
varco_core.authority.config
================================

``AuthorizationConfig`` — parses ``VARCO_AUTHORIZATION__*`` environment
variables and constructs a ``TrustedIssuerRegistry``.

Environment variable format
----------------------------
Each trusted issuer requires two env vars:

    VARCO_AUTHORIZATION__<LABEL>__URL = <source descriptor>
    VARCO_AUTHORIZATION__<LABEL>__ISS = <iss claim value>

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

DESIGN: ``VarcoSettings`` with a ``model_validator(mode="before")`` env scan
    **Plan 048 / DI-6, §D-prefix rule 1 (rename): the prefix moves from the
    legacy, non-``VARCO_`` ``FASTREST_AUTHORIZATION__`` to
    ``VARCO_AUTHORIZATION__``** (one release startup warning if the old name
    is still set — see ``bind_settings()``'s legacy-name check).
    ``issuers`` is a label-keyed family (§D-prefix rule 4) — pydantic-settings'
    ordinary per-field env binding cannot enumerate an unbounded set of
    ``<LABEL>`` env vars, so this class keeps its own environment scan, now
    inside a ``model_validator(mode="before")`` so ``bind_settings()``'s
    normal construction/provenance/fail-fast machinery still applies —
    ``issuers`` just reports provenance ``"env"`` as a whole rather than
    per-label (052's diagnose renders this as "issuers: env (label-keyed)").
    ✅ Registered via ``bind_settings()`` like every other class in this
       plan — no second bootstrap path for authorization config.
    ✅ Simple parsing logic unchanged — the env var schema is deliberately
       flat; only the prefix and the base class changed.
    ❌ No per-label type coercion beyond what the source factory provides —
       unchanged from before this conversion.

Thread safety:  ✅ Frozen after construction (``model_config``).
Async safety:   ✅ No I/O — to_registry() is synchronous construction only.
                   Network fetches happen later in registry.load_all().
"""

from __future__ import annotations

import os
import ssl
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from pydantic import model_validator
from pydantic_settings import SettingsConfigDict

from varco_core.authority.sources.factory import IssuerSourceFactory
from varco_core.config import VarcoSettings

if TYPE_CHECKING:
    from varco_core.authority.registry import TrustedIssuerRegistry


# ── Constants ─────────────────────────────────────────────────────────────────

# Env var prefix — all VARCO_AUTHORIZATION__* vars are scanned. The old
# FASTREST_AUTHORIZATION__ name is renamed away by Plan 048 / DI-6 — see
# the module docstring's DESIGN block.
_ENV_PREFIX: str = "VARCO_AUTHORIZATION__"

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


def _scan_issuers_from_environ() -> tuple[IssuerConfig, ...]:
    """
    Parse all ``VARCO_AUTHORIZATION__*`` env vars from ``os.environ``.

    Groups them by label (the middle segment between the prefix and the
    field suffix) and builds an ``IssuerConfig`` for each complete label.

    Returns:
        Parsed issuer configs, in discovery order.

    Edge cases:
        - Env var values are stripped of leading/trailing whitespace.
        - Labels are case-preserved (GOOGLE ≠ google).
        - Vars whose name does not contain a second ``__`` separator are
          silently ignored (e.g. ``VARCO_AUTHORIZATION__ORPHAN``).
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

    return tuple(configs)


class AuthorizationConfig(VarcoSettings):
    """
    Parsed representation of all ``VARCO_AUTHORIZATION__*`` env vars.

    Use ``from_env()`` (or ``bind_settings()``) to construct from the
    process environment, then ``to_registry()`` to produce a
    ready-to-use ``TrustedIssuerRegistry``.

    Thread safety:  ✅ Frozen — immutable after construction.
    Async safety:   ✅ to_registry() is pure construction — no I/O.

    Attributes:
        issuers: Tuple of parsed issuer configs in the order they were
                 discovered in the environment. Built by a
                 ``model_validator(mode="before")`` env scan — see the
                 module DESIGN block for why this label-keyed family
                 cannot use ordinary per-field env binding.

    Edge cases:
        - An empty environment (no ``VARCO_AUTHORIZATION__*`` vars) →
          empty ``issuers`` tuple → empty registry (valid).
        - A label with ``__URL`` but no ``__ISS`` → ``iss`` is inferred by
          normalising the label (uppercase → lowercase, ``_`` → ``-``).
        - A label with ``__ISS`` but no ``__URL`` → silently ignored.
        - Extra field suffixes (e.g. ``__COMMENT``) are silently ignored.
        - An explicit ``issuers=(...)`` keyword construction (the "code"
          path, e.g. ``bind_settings(c, AuthorizationConfig,
          instance=AuthorizationConfig(issuers=(...)))``) bypasses the
          environment scan entirely — the ``model_validator`` only fills
          ``issuers`` in when the caller did not already supply it.

    Example::

        config = AuthorizationConfig.from_env()
        registry = config.to_registry()
        await registry.load_all()
    """

    model_config = SettingsConfigDict(env_prefix=_ENV_PREFIX, frozen=True)

    issuers: tuple[IssuerConfig, ...] = ()

    @model_validator(mode="before")
    @classmethod
    def _scan_issuers(cls, data: Any) -> Any:
        """
        Fill ``issuers`` from ``os.environ`` when the caller did not pass
        it explicitly — see the class docstring's "code path" edge case.
        """
        if isinstance(data, dict) and "issuers" not in data:
            data = {**data, "issuers": _scan_issuers_from_environ()}
        return data

    def to_registry(self, *, ssl_context: ssl.SSLContext | None = None) -> TrustedIssuerRegistry:
        """
        Construct a ``TrustedIssuerRegistry`` from this config.

        For each ``IssuerConfig``, calls ``IssuerSourceFactory.from_string()``
        to build the appropriate source, then registers it in the registry.

        Args:
            ssl_context: Forwarded to ``IssuerSourceFactory.from_string()`` for every issuer
                (Plan 026 / T5) — applies uniformly to the two URL-based source kinds
                (``jwks::``/``oidc::``) and is ignored for PEM-based ones.  ``None`` (the
                default) is byte-identical to pre-Plan-026 behaviour.

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
            source = IssuerSourceFactory.from_string(
                issuer_config.url_value, ssl_context=ssl_context
            )
            registry.register(
                label=issuer_config.label,
                iss=issuer_config.iss,
                source=source,
            )

        return registry

    def __repr__(self) -> str:
        labels = [c.label for c in self.issuers]
        return f"AuthorizationConfig(issuers={labels!r})"
