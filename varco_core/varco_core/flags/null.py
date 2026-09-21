"""
varco_core.flags.null
=======================
``NullFeatureFlags`` — the no-op ``AbstractFeatureFlags`` default (Plan 032
/ D7). Bound automatically by ``container.scan("varco_core.flags", ...)``
so an application that never opts into feature flags sees zero behaviour
change: every resolution simply returns the caller's own default.

``@Fallback`` — the in-process default; any application binding of
``AbstractFeatureFlags`` (including ``enable_feature_flags()``'s
``InMemoryFeatureFlags``) wins regardless of registration order
(`technical_docs/features/dependency-injection.md`).
"""

from __future__ import annotations

from typing import Any

from providify import Fallback, Singleton

from varco_core.flags.base import AbstractFeatureFlags, FlagEvaluationContext, FlagResolution

__all__ = ["NullFeatureFlags"]


@Fallback
@Singleton
class NullFeatureFlags(AbstractFeatureFlags):
    """
    No-op feature flags — always returns the caller's default, unchanged.

    DESIGN: a scanned ``@Singleton`` default, an opt-in ``@Provider`` override
        ✅ Importing/bootstrapping ``varco_core.flags`` never changes
           behaviour for an app that does not call ``enable_feature_flags``.
        ✅ Matches ``varco_casbin``'s ``enable_policy_authorizer`` precedent —
           the default is real DI (not a hardcoded module-level singleton),
           satisfying CLAUDE.md's "DI defaults" rule.
        ❌ Two files to read (``null.py`` + ``di.py``) to understand the full
           binding story. Accepted — the split is what makes the opt-in
           truthful.

    Thread safety:  ✅ Stateless.
    Async safety:   ✅ No I/O.
    """

    async def resolve_bool(
        self, key: str, default: bool, *, context: FlagEvaluationContext | None = None
    ) -> FlagResolution[bool]:
        return FlagResolution(value=default, reason="NULL")

    async def resolve_string(
        self, key: str, default: str, *, context: FlagEvaluationContext | None = None
    ) -> FlagResolution[str]:
        return FlagResolution(value=default, reason="NULL")

    async def resolve_numeric(
        self, key: str, default: float, *, context: FlagEvaluationContext | None = None
    ) -> FlagResolution[float]:
        return FlagResolution(value=default, reason="NULL")

    async def resolve_object(
        self, key: str, default: Any, *, context: FlagEvaluationContext | None = None
    ) -> FlagResolution[Any]:
        return FlagResolution(value=default, reason="NULL")
