"""
varco_fastapi.diagnose
=========================
``wiring_report(app)`` — re-render ``app.state.varco_wiring_report`` with
the **realised** (post-start) lifecycle order/failures filled in from
``app.state.varco_lifespan.last_runner`` when the app has actually started
(Plan 052 / §D-startup). A pure read, for tests and an app-mounted
diagnostic endpoint — never a preflight (that is
``varco_core.di.diagnose.run_wiring_preflight``, run once at startup).
"""

from __future__ import annotations

import dataclasses
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from fastapi import FastAPI
    from varco_core.di.report import WiringReport

__all__ = ["posture_header", "wiring_report"]


def posture_header(container: Any, modules: Any = ()) -> dict[str, Any]:
    """
    Read the report-header facts (``environment``/``enforce``/
    ``enforce_source``) from ``SecurityPostureSettings``.

    ``varco_core`` must not import that class, so the fastapi layer reads
    it and hands the values to ``build_wiring_report`` /
    ``run_wiring_preflight`` / ``diagnose`` as keyword arguments (Plan 052
    §D-model).

    Args:
        container: The container the modules were registered on; a bound
            ``SecurityPostureSettings`` wins over a fresh env read.
        modules: The active modules — a ``SecurityPostureModule`` built with
            ``settings=`` makes the source ``"code"``.

    Returns:
        ``{"environment", "enforce", "enforce_source"}``; ``enforce`` is
        the *effective* mode (``effective_enforce``); ``enforce_source`` is
        ``"derived"`` (unset — derived from ``VARCO_SECURITY_ENV``, Plan
        054 / FLIP-2), ``"code"`` (explicit settings object) or ``"env"``
        (``VARCO_SECURITY_ENFORCE`` set).
    """
    from varco_fastapi.modules.lifecycle import SecurityPostureModule  # noqa: PLC0415
    from varco_fastapi.posture import SecurityPostureSettings  # noqa: PLC0415

    # A LookupError means "not bound after all" (a stub/partial container
    # can report resolvable and still refuse) — fall back to the same env
    # read an unbound settings class would get, never a guess.
    settings: SecurityPostureSettings
    try:
        settings = container.get(SecurityPostureSettings)
    except LookupError:
        settings = SecurityPostureSettings()
    # Plan 054 / §D-compose: a derived mode is reported as "derived" first
    # — the field itself is still None, so no other source is honest.
    source: str
    if settings.enforce_is_derived:
        source = "derived"
    elif any(isinstance(m, SecurityPostureModule) and m._settings_arg is not None for m in modules):
        source = "code"
    else:
        source = _bound_enforce_source(container, SecurityPostureSettings)
    return {
        "environment": settings.environment,
        "enforce": settings.effective_enforce,
        "enforce_source": source,
    }


def _bound_enforce_source(container: Any, cls: type[Any]) -> str:
    """
    Provenance of an explicitly-set ``enforce`` field: 048's
    ``provenance_of()`` when the settings came through ``bind_settings()``,
    else ``"env"`` iff ``VARCO_SECURITY_ENFORCE`` is set, else ``"code"``.

    Args:
        container: The container the settings class may be bound on.
        cls: ``SecurityPostureSettings``.

    Returns:
        ``"code"`` or ``"env"``.
    """
    import os  # noqa: PLC0415

    from varco_core.settings import provenance_of  # noqa: PLC0415

    try:
        field_source = provenance_of(container, cls).fields.get("enforce")
    except (KeyError, TypeError):
        # Not bound via bind_settings() on this container (or not a
        # container at all) — fall back to the env-presence read.
        field_source = None
    if field_source in ("code", "env"):
        return field_source
    return "env" if "VARCO_SECURITY_ENFORCE" in os.environ else "code"


def wiring_report(app: FastAPI) -> WiringReport:
    """
    Re-render *app*'s wiring report, filling in the realised lifecycle
    order/failures when the lifespan has started components.

    Args:
        app: A ``FastAPI`` object built by ``create_varco_app()``.

    Returns:
        The report built at startup (``app.state.varco_wiring_report``),
        with ``lifecycle.realised``/``lifecycle.failures`` replaced by the
        actually-started order when ``app.state.varco_lifespan.last_runner``
        is available (``None``/empty before the lifespan has run its
        ``start()`` pass).

    Raises:
        RuntimeError: The lifespan has not run yet — no
            ``app.state.varco_wiring_report`` exists.
    """
    base: WiringReport | None = getattr(app.state, "varco_wiring_report", None)
    if base is None:
        raise RuntimeError(
            "wiring_report(app): lifespan has not run — app.state.varco_wiring_report "
            "is not set. Enter the app's lifespan (e.g. via TestClient) first."
        )

    lifespan: Any = getattr(app.state, "varco_lifespan", None)
    runner = getattr(lifespan, "last_runner", None) if lifespan is not None else None
    if runner is None:
        return base

    realised = tuple(getattr(runner, "plan", ()) or ())
    failures = tuple(
        (f.name, repr(f.exception)) for f in (getattr(runner, "last_failures", None) or ())
    )
    lifecycle = dataclasses.replace(base.lifecycle, realised=realised or None, failures=failures)
    return dataclasses.replace(base, lifecycle=lifecycle)
