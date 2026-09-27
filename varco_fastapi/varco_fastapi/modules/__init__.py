"""
varco_fastapi.modules
========================
Every ``VarcoModule`` subsystem ``varco_fastapi`` ships (Plan 049 / DI-1) —
the edge modules (``modules.edge``), the subsystem lifecycle modules
(``modules.lifecycle``), the HTTP contribution primitives
(``modules.base``: ``HttpContributor``, ``MiddlewareModule``), and
``default_modules()`` (``modules.defaults``).

See ``technical_docs/features/dependency-injection.md``'s §Modules for the
full picture, and ``varco_core.di.module``'s module docstring for the
``VarcoModule`` ABC itself.
"""

from __future__ import annotations

from varco_fastapi.modules.base import HttpContributor, MiddlewareModule
from varco_fastapi.modules.edge import (
    BodyLimitModule,
    CorsModule,
    ErrorEnvelopeModule,
    FastAPIModule,
    LocalizationModule,
    LoggingModule,
    MetricsModule,
    ProfilingModule,
    RateLimitModule,
    SecurityHeadersModule,
    TracingModule,
)
from varco_fastapi.modules.lifecycle import (
    JwksRefreshModule,
    MigrationsModule,
    ReliabilityModule,
    RetentionModule,
    SecurityPostureModule,
    TenancyModule,
)

__all__ = [
    "BodyLimitModule",
    "CorsModule",
    "ErrorEnvelopeModule",
    "FastAPIModule",
    "HttpContributor",
    "JwksRefreshModule",
    "LocalizationModule",
    "LoggingModule",
    "MetricsModule",
    "MiddlewareModule",
    "MigrationsModule",
    "ProfilingModule",
    "RateLimitModule",
    "ReliabilityModule",
    "RetentionModule",
    "SecurityHeadersModule",
    "SecurityPostureModule",
    "TenancyModule",
    "TracingModule",
]
