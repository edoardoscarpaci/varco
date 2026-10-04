"""
varco_core.jwt.transform.runtime
=====================================

Process-global, read-mostly registry resolution for
``varco_core.jwt.parser.JwtParser`` — the "no DI required" runtime path.

DESIGN: process-global registry instead of an injected instance
    ✅ ``JwtParser`` stays stateless classmethods — no breaking API change,
       no parser instances threaded through ``JwtBuilder``/``registry``/
       ``verify()`` call sites.
    ✅ Works with zero DI (library users, CLI tools, varco_core-only apps)
       via ``JwtTransformConfig.from_env()``.
    ✅ Zero-config cost is one dict lookup + ``IDENTITY`` (which returns the
       input dict as-is, no copy) — the hot path is unchanged.
    ❌ Mutable module state; two different mappings cannot coexist in one
       process.  Mitigated by the explicit ``transformer=`` parameter on
       ``parse()``/``_from_raw_claims()`` (always wins) and by
       ``reset_claim_transforms()`` in tests (autouse fixture,
       ``varco_core/tests/conftest.py``).
    ❌ Configuration is read at first-use, not at import — a late env
       mutation is picked up only on the *next* parse.  Call
       ``configure_claim_transforms()`` to force a rebuild immediately.
    Alternative considered: make ``JwtParser`` instantiable
    (``JwtParser(transformer=…)``).  ✅ pure, no globals.  ❌ breaks every
    existing ``JwtParser.parse(...)`` call site plus
    ``TrustedIssuerRegistry.verify()`` (which calls the classmethod
    ``JwtParser._from_raw_claims`` directly), and forces every non-DI
    caller to thread an instance through.  Rejected.
    No ``asyncio.Lock`` is used: the registry is set once at startup and
    only read afterwards; dict/attribute assignment is atomic under the
    GIL and the lazy init is idempotent (re-running it twice produces an
    equivalent registry).

Thread safety:  ⚠️ Module-global state — safe for the single-process,
                   mostly-read-after-startup usage pattern this targets.
                   Not designed for concurrent ``configure_*``/``reset_*``
                   calls from multiple threads.
Async safety:   ✅ No I/O, no awaits — safe to call from sync or async code.
"""

from __future__ import annotations

from varco_core.jwt.transform.protocol import ClaimTransformer
from varco_core.jwt.transform.registry import ClaimTransformerRegistry

# Module-global, lazily-built registry.  `None` means "not yet resolved from
# the environment" — the next resolve_claim_transformer() call builds it.
_registry: ClaimTransformerRegistry | None = None


def resolve_claim_transformer(iss: str | None) -> ClaimTransformer:
    """
    Resolve the ``ClaimTransformer`` to use for a token with the given
    ``iss`` claim.

    Lazily builds the process-global registry from
    ``JwtTransformConfig.from_env()`` on first call and caches it.

    Args:
        iss: The token's ``iss`` claim, or ``None`` if absent.

    Returns:
        The resolved ``ClaimTransformer`` — ``IDENTITY`` when no
        ``VARCO_JWT_TRANSFORM*`` env vars are set and no explicit
        configuration has been installed via ``configure_claim_transforms()``.

    Edge cases:
        - Configuration is read once (or on the first call after a
          ``reset_claim_transforms()``) — a later env mutation is NOT
          picked up automatically.  Call ``configure_claim_transforms()``
          to force a rebuild.
    """
    global _registry
    if _registry is None:
        # Local import — avoids a hard import cycle between config.py (which
        # imports mapping/registry) and runtime.py at module load time.
        from varco_core.jwt.transform.config import JwtTransformConfig

        _registry = JwtTransformConfig.from_env().to_registry()
    return _registry.for_issuer(iss)


def configure_claim_transforms(
    registry: ClaimTransformerRegistry | None = None,
) -> None:
    """
    Install a ``ClaimTransformerRegistry`` as the process-global registry,
    overriding whatever ``resolve_claim_transformer()`` would otherwise
    build from the environment.

    Args:
        registry: The registry to install.  When ``None``, eagerly rebuilds
                  from the current environment immediately (rather than
                  lazily on next resolve) — useful to force-refresh after a
                  runtime env mutation.

    Edge cases:
        - Takes effect immediately for every subsequent
          ``resolve_claim_transformer()`` call — the global is *replaced*,
          not merged with any prior state.
        - Call ``reset_claim_transforms()`` to return to normal lazy
          env-driven resolution (the standard pattern in tests).
    """
    global _registry
    if registry is None:
        from varco_core.jwt.transform.config import JwtTransformConfig

        _registry = JwtTransformConfig.from_env().to_registry()
    else:
        _registry = registry


def reset_claim_transforms() -> None:
    """
    Clear the process-global registry, restoring lazy env-driven resolution.

    Intended for use in an autouse test fixture (see
    ``varco_core/tests/conftest.py``) so no test leaks env-driven or
    explicitly-configured JWT transform state into another test.
    """
    global _registry
    _registry = None


def configure_jwt_from_env() -> None:
    """
    Eagerly (re)build BOTH the claim-transform registry and the token-profile
    registry from the current environment.

    Convenience wrapper combining ``configure_claim_transforms(None)`` (this
    module) and ``configure_token_profiles(None)``
    (``varco_core.jwt.profile``) into one call — used by
    ``create_varco_app()`` at startup so the process-global state matches
    what ``VarcoFastAPIConfiguration``'s DI providers hand out, without callers
    needing to know both registries exist.

    Edge cases:
        - Safe to call multiple times — each call rebuilds from the current
          environment (idempotent given a stable environment).
        - Does NOT read/require a DI container — pure env-var resolution,
          consistent with every other ``*_from_env()`` factory in this layer.
    """
    # Local import — varco_core.jwt.profile imports JsonWebToken from the
    # sibling model.py; importing it at this module's top level would create
    # an import-order dependency between the two process-global runtimes
    # that isn't otherwise needed (they are independent registries).
    from varco_core.jwt.profile import configure_token_profiles

    configure_claim_transforms(None)
    configure_token_profiles(None)


__all__ = [
    "resolve_claim_transformer",
    "configure_claim_transforms",
    "reset_claim_transforms",
    "configure_jwt_from_env",
]
