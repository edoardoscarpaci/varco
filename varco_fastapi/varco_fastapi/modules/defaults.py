"""
varco_fastapi.modules.defaults
=================================
``default_modules()`` — the module list ``create_varco_app(modules=None)``
installs (Plan 049 / DI-1, §D-signature).

DESIGN: a function returning fresh instances, never a module-level tuple
    ✅ A module holds state (``SecurityPostureModule.report``) — sharing
       one instance across two apps in one process would leak that state
       between them; every test file in this repo constructs at least one
       app.
    ❌ Every caller pays one list-build. Negligible next to constructing a
       ``FastAPI`` app.
"""

from __future__ import annotations

from varco_core.di.module import CoreModule, ObservabilityModule, VarcoModule

from varco_fastapi.modules.edge import (
    BodyLimitModule,
    CorsModule,
    ErrorEnvelopeModule,
    FastAPIModule,
    LocalizationModule,
    LoggingModule,
    SecurityHeadersModule,
    TracingModule,
)
from varco_fastapi.modules.lifecycle import SecurityPostureModule

#: The literal, documented order — outside code should never depend on the
#: *identity* of these classes beyond what ``default_modules()`` returns;
#: this tuple exists purely so ``exclude=`` can name a class type.
_DEFAULT_MODULE_CLASSES: tuple[type[VarcoModule], ...] = (
    CoreModule,
    ObservabilityModule,
    FastAPIModule,
    CorsModule,
    SecurityHeadersModule,
    ErrorEnvelopeModule,
    BodyLimitModule,
    LoggingModule,
    TracingModule,
    LocalizationModule,
    SecurityPostureModule,
)


def default_modules(*, exclude: tuple[type[VarcoModule], ...] = ()) -> list[VarcoModule]:
    """
    Build the default module list ``create_varco_app(modules=None)``
    installs — fresh instances, in the documented order.

    Args:
        exclude: Module classes to drop from the returned list entirely.
            E.g. ``default_modules(exclude=(SecurityPostureModule,))``
            turns FLIP-1's default-on preflight off without an env var.

    Returns:
        A new list of freshly constructed module instances, in order:
        ``CoreModule, ObservabilityModule, FastAPIModule, CorsModule,
        SecurityHeadersModule, ErrorEnvelopeModule, BodyLimitModule,
        LoggingModule, TracingModule, LocalizationModule,
        SecurityPostureModule``.

    Edge cases:
        Two calls never share an instance — a module holds state
        (``SecurityPostureModule.report``) that must not leak between two
        apps built in the same process.
    """
    return [cls() for cls in _DEFAULT_MODULE_CLASSES if cls not in exclude]


__all__ = ["default_modules"]
