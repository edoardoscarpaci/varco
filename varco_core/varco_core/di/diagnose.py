"""
varco_core.di.diagnose
=========================
The ``varco diagnose`` driver (Plan 052 / §D-driver): obtains a
``WiringReport`` from a module list without ever calling ``start()`` —
``register`` -> (optional) ``async_init`` -> module ``validate()`` ->
(optional) ``container.validate()`` -> :func:`build_wiring_report`.

The split is 049's own (``register``/``async_init``/``validate`` are
wiring; ``start``/``stop`` are runtime) — :func:`diagnose` stops exactly
where a ``LifecycleRunner`` would begin. The caller owns
``await container.ashutdown()``.

Thread safety:  ⚠️ Single-threaded startup only, same contract as every
                    other ``varco_core.di`` composition-root verb.
Async safety:   ✅ ``diagnose``/``run_wiring_preflight`` are ``async def``;
                    every I/O point is an awaited module call.
"""

from __future__ import annotations

import warnings
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Literal

from providify.validation import Severity

from varco_core.deprecation import DeprecationRecord

if TYPE_CHECKING:
    from collections.abc import Iterator, Sequence

    from providify import DIContainer

    from varco_core.di.lifecycle import LifecycleComponent
    from varco_core.di.module import VarcoModule
    from varco_core.di.report import WiringReport

__all__ = [
    "ModuleAsyncInitError",
    "WiringValidationError",
    "capture_direct_deprecations",
    "direct_deprecation_records",
    "diagnose",
    "run_wiring_preflight",
]


class ModuleAsyncInitError(RuntimeError):
    """
    A module's ``async_init()`` raised while :func:`diagnose` drove it.

    The message is ``"<ModuleName>.async_init failed: <exc>"`` (the plan's
    edge case) so the CLI can print it verbatim; the original exception is
    chained as ``__cause__``.
    """


class WiringValidationError(RuntimeError):
    """
    Raised by :func:`run_wiring_preflight` when the assembled
    ``WiringReport`` carries an ERROR (always) or a WARNING (only when
    ``strict=True``) — the runtime half of ``create_varco_app(validate=
    True)`` (§D-startup). ``varco_fastapi`` maps this to its own
    ``ConfigurationError``; the CLI maps it to exit 1.

    Args:
        report: The offending ``WiringReport`` — callers read
            ``report.validation.errors``/``report.findings`` for detail.
    """

    def __init__(self, report: WiringReport) -> None:
        self.report = report
        lines = [
            f"{issue.get('owner')}: {issue.get('message')}" for issue in report.validation.errors
        ]
        # Warning-tier findings are listed too: under strict=True they are
        # what refused startup, and under non-strict they are context.
        warning_findings = [f for f in report.findings if f.severity == Severity.WARNING]
        lines.extend(f"{f.check}: {f.detail}" for f in warning_findings)
        super().__init__(
            f"varco wiring validation failed: {len(report.validation.errors)} error(s), "
            f"{len(warning_findings)} warning finding(s).\n  " + "\n  ".join(lines)
        )


def direct_deprecation_records(
    warning_list: Sequence[warnings.WarningMessage],
) -> tuple[DeprecationRecord, ...]:
    """
    Convert every ``DeprecationWarning`` caught in *warning_list* into a
    ``DeprecationRecord`` (§D-deprecations' "capture window" for the eight
    direct ``warnings.warn`` sites outside ``varco_core.deprecation``).

    Args:
        warning_list: The list a ``warnings.catch_warnings(record=True)``
            context populated.

    Returns:
        One ``DeprecationRecord`` per captured ``DeprecationWarning``,
        ``since``/``removed_in``/``replacement`` all ``None`` (this window
        does not know a direct site's metadata — only the ledger, for
        sites that go through ``deprecated()``, carries that).
    """
    records: list[DeprecationRecord] = []
    for w in warning_list:
        if issubclass(w.category, DeprecationWarning):
            records.append(
                DeprecationRecord(
                    subject=str(w.message),
                    since=None,
                    removed_in=None,
                    replacement=None,
                    count=1,
                    first_location=f"{w.filename}:{w.lineno}",
                )
            )
    return tuple(records)


@contextmanager
def capture_direct_deprecations() -> Iterator[list[warnings.WarningMessage]]:
    """
    Open the §D-deprecations capture window: every ``DeprecationWarning``
    raised inside the ``with`` body is recorded, regardless of the ambient
    warnings filter. Convert the yielded list with
    :func:`direct_deprecation_records` after the block.

    Shared by :func:`diagnose` and the ``create_varco_app`` lifespan so
    both see the direct ``warnings.warn`` sites identically.

    Yields:
        The list ``warnings.catch_warnings(record=True)`` fills.

    Async safety:   ⚠️ ``catch_warnings`` swaps process-global state, so a
                    window held across an ``await`` also records warnings
                    other tasks raise meanwhile. Accepted: startup is
                    effectively single-task and a false record is benign.
    """
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always", DeprecationWarning)
        yield caught


async def diagnose(
    container: DIContainer,
    modules: Sequence[VarcoModule],
    *,
    async_init: bool = True,
    validate: bool = True,
    environment: str = "unknown",
    enforce: str = "unknown",
    enforce_source: Literal["code", "env", "default"] = "default",
) -> WiringReport:
    """
    Obtain a ``WiringReport`` from *modules*, never calling ``start()``
    (§D-driver).

    Args:
        container: The ``DIContainer`` to register *modules* onto (fresh
            or already partially registered — ``register()`` is
            idempotent per module class).
        modules: The modules to register, in the order passed (dependency
            order is resolved internally via ``resolve_module_order``).
        async_init: Whether to await each enabled module's
            ``async_init(container)`` — ``False`` is the Docker-free
            ``--static`` mode (§D-driver's candidate table).
        validate: Whether to run ``container.validate(raise_on_error=
            False)``. Module-level ``validate()`` slots always run,
            independent of this flag.
        environment, enforce, enforce_source: Report-header values the
            caller read from ``SecurityPostureSettings`` (``varco_core``
            cannot import it); default ``"unknown"``.

    Returns:
        A ``WiringReport``. Never raises on an ERROR/WARNING finding —
        see :func:`run_wiring_preflight` for the raising variant.

    Raises:
        ModuleAsyncInitError: A module's ``async_init`` raised.
        Exception: Whatever a module's ``register``/``validate`` raises,
            propagated unchanged (a bug in the module, not a wiring
            finding).
    """
    from varco_core.di.lifecycle import phased_components  # noqa: PLC0415
    from varco_core.di.module import ModuleRegistry, resolve_module_order  # noqa: PLC0415
    from varco_core.di.report import build_wiring_report  # noqa: PLC0415

    order = resolve_module_order(list(modules))

    with capture_direct_deprecations() as caught:
        for module in order:
            module.register(container)

        registry: ModuleRegistry = container.get(ModuleRegistry)
        active = [m for m in order if registry.enabled(m)]

        if async_init:
            for module in active:
                try:
                    await module.async_init(container)
                except Exception as exc:
                    raise ModuleAsyncInitError(
                        f"{type(module).__name__}.async_init failed: {exc}"
                    ) from exc

    direct_deprecations = direct_deprecation_records(caught)

    messages: dict[Any, list[str]] = {m: list(m.validate(container)) for m in active}

    validation_report = container.validate(raise_on_error=False) if validate else None
    components: Sequence[LifecycleComponent] = phased_components(active, container)

    report = build_wiring_report(
        container,
        validation=validation_report,
        modules=active,
        lifecycle=components,
        module_validate_messages=messages,
        static=not async_init,
        extra_deprecations=direct_deprecations,
        environment=environment,
        enforce=enforce,
        enforce_source=enforce_source,
    )

    return report


async def run_wiring_preflight(
    container: DIContainer,
    *,
    modules: Sequence[VarcoModule],
    lifecycle: Sequence[LifecycleComponent],
    validate: bool,
    strict: bool,
    extra_deprecations: Sequence[DeprecationRecord] = (),
    environment: str = "unknown",
    enforce: str = "unknown",
    enforce_source: Literal["code", "env", "default"] = "default",
) -> WiringReport:
    """
    Build a ``WiringReport`` for *modules* (already registered on
    *container*) and refuse on an ERROR (always) or a WARNING finding
    (only when ``strict=True``) — the runtime half of
    ``create_varco_app(validate=True, strict_validation=...)`` (§D-startup).

    Unlike :func:`diagnose`, this function does not call ``register()``/
    ``async_init()`` — the caller (the lifespan, or a CLI driver that
    already called :func:`diagnose`) is expected to have done that.

    Args:
        container: The ``DIContainer`` *modules* are already registered on.
        modules: The active (enabled) modules, in dependency order.
        lifecycle: The planned ``LifecycleComponent`` sequence (typically
            ``phased_components(modules, container)``).
        validate: Whether to run ``container.validate(raise_on_error=
            False)``. Module-level ``validate()`` slots always run.
        strict: Whether a WARNING-severity finding also refuses (an ERROR
            always refuses, regardless of this flag).
        extra_deprecations: Direct-site records the caller captured with
            :func:`capture_direct_deprecations` around its own setup.
        environment, enforce, enforce_source: Report-header values the
            caller read from ``SecurityPostureSettings``.

    Returns:
        The assembled ``WiringReport``, when it does not refuse.

    Raises:
        WiringValidationError: An ERROR-severity validation issue is
            present, or (when ``strict=True``) a WARNING-severity
            ``WiringFinding`` is present.
    """
    from varco_core.di.report import build_wiring_report  # noqa: PLC0415

    messages = {m: list(m.validate(container)) for m in modules}
    validation_report = container.validate(raise_on_error=False) if validate else None

    report = build_wiring_report(
        container,
        validation=validation_report,
        modules=list(modules),
        lifecycle=lifecycle,
        module_validate_messages=messages,
        extra_deprecations=extra_deprecations,
        environment=environment,
        enforce=enforce,
        enforce_source=enforce_source,
    )

    has_error = bool(report.validation.errors)
    has_warning_finding = any(f.severity == Severity.WARNING for f in report.findings)

    if has_error or (strict and has_warning_finding):
        raise WiringValidationError(report)

    return report
