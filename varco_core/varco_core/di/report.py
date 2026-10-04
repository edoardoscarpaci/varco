"""
varco_core.di.report
=======================
``WiringReport`` — one frozen, JSON-serialisable answer to "what is this
app running and why" (Plan 052 / DI-7, §D-model). ``build_wiring_report()``
assembles it from the public reads 045–051 already shipped: 047's
``inspect_bindings``/``catalogue``/``inspect_backend_selection``, 048's
``all_provenance``, 049's ``ModuleRegistry``, 050's ``phased_components``,
051's ``collection_seams``/``collect``, and providify's own
``ValidationReport`` (reused verbatim, never re-derived).

DESIGN: varco-owned frozen dataclasses, providify's ``Severity`` reused
(§D-model, see the plan's candidate table for the full argument)
    ✅ One severity vocabulary across providify's own validation issues and
       varco's own findings — a reader never has to learn two scales.
    ✅ 047's ``Catalogue`` is embedded verbatim under ``catalogue`` — the
       committed snapshot and this runtime report are never allowed to
       disagree about what a *default* is.
    ❌ A second top-level ``schema_version`` beside the catalogue's own
       (both currently ``1``) — documented as independent fields.

DESIGN: shadowing is computed from public reads, never from providify's
``FALLBACK_SHADOWED`` INFO (§D-provenance, §D-P28)
    ✅ Correct today for a *scanned* ``@Fallback`` class shadowed by a
       ``bind()`` — 045 §D-proof found providify emits **zero** such INFOs
       for that case. Computing ``shadowed``/``inactive`` from
       ``inspect_bindings()`` records never depends on that gap closing.
    ❌ A value-tuple match (owner/qualifier/priority/fallback) identifies
       "the live record" among same-interface candidates rather than
       object identity — ``BindingRecord`` (047) does not carry the raw
       binding object, only display fields. Accepted: two fallback
       bindings of one interface that coincidentally share all four values
       is not a shape any varco default produces.

Every submodule this file imports from is imported **inside function
bodies**, not at module scope (the ``varco_core.di`` import-budget
discipline) — ``varco_core.di.report`` itself is reached only via the PEP
562 lazy map, so the one module-scope exception is ``providify.validation``
(``Severity``), unavoidable since it is this module's own public vocabulary.

Thread safety:  ✅ Every class here is frozen; ``build_wiring_report()`` is
                   a pure read over a container snapshot at call time.
Async safety:   ✅ No awaits — a synchronous read. The driver (§D-driver,
                   ``varco_core.di.diagnose``) is what awaits ``async_init``.
"""

from __future__ import annotations

import os
from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING, Any, Literal

from providify.validation import Severity
from pydantic_settings import SettingsConfigDict

from varco_core.config import VarcoSettings
from varco_core.deprecation import DeprecationRecord, fired_deprecations

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from providify import DIContainer
    from providify.validation import ValidationReport

    from varco_core.di.module import VarcoModule

__all__ = [
    "BindingSummary",
    "DeprecationRecord",
    "DiagnoseSettings",
    "LifecyclePlan",
    "ModuleStatus",
    "Provenance",
    "SeamStatus",
    "Severity",
    "SettingsStatus",
    "ValidationSummary",
    "WiringFinding",
    "WiringReport",
    "build_wiring_report",
]

#: Where a live binding came from — §D-provenance's one rule per section.
Provenance = Literal["code", "profile", "env-dotted", "env", "default", "absent"]


@dataclass(frozen=True)
class BindingSummary:
    """One binding, display-ready — built from 047's ``BindingRecord`` or a raw providify binding."""

    owner: str
    implementation: str
    package: str
    qualifier: str | None
    priority: int
    fallback: bool
    shape: Literal["class", "provider"]
    conditions: tuple[str, ...]
    profiles: tuple[str, ...]
    active: bool


@dataclass(frozen=True)
class SeamStatus:
    """
    One row of the Seams / Features / Other defaults / Collections sections.

    Args:
        interface: The **raw interface object** (a type, or a generic
            alias such as ``Serializer[Event]``) — deliberately not a
            string, so a consumer (the ``di_wiring`` conformance suite's
            ``expected_defaults``, §D-tests) can assert membership by
            identity against the same class it imported. ``to_dict()``
            renders it as a display string for JSON output; the live
            dataclass field never does.
    """

    kind: Literal["seam", "feature", "other", "collection"]
    name: str
    interface: Any
    qualifier: str | None
    env: str | None
    env_value: str | None
    live: BindingSummary | None
    source: Provenance
    shadowed: tuple[BindingSummary, ...]
    inactive: tuple[BindingSummary, ...]
    contributors: tuple[BindingSummary, ...] = ()
    enabled: bool | None = None


@dataclass(frozen=True)
class ModuleStatus:
    """One ``VarcoModule``'s wiring state."""

    name: str
    cls: str
    package: str
    enabled: bool
    enabled_source: Literal["code", "env", "default"]
    phase: int
    phase_name: str
    depends_on: tuple[str, ...]
    components: tuple[tuple[str, int], ...]
    validate_messages: tuple[str, ...] = ()


@dataclass(frozen=True)
class SettingsStatus:
    """One settings class's per-field provenance (from 048's ``SettingsProvenance``)."""

    cls: str
    qualifier: str | None
    env_prefix: str
    source: Literal["fallback", "instance", "external"]
    fields: dict[str, Literal["code", "env", "default"]]


@dataclass(frozen=True)
class ValidationSummary:
    """providify's ``ValidationReport``, folded in — never re-derived."""

    ran: bool
    checked_bindings: int = 0
    errors: tuple[dict[str, Any], ...] = ()
    warnings: tuple[dict[str, Any], ...] = ()
    infos: tuple[dict[str, Any], ...] = ()


@dataclass(frozen=True)
class WiringFinding:
    """One varco-side finding (never a posture finding — those stay in ``SecurityPosture``)."""

    check: str
    severity: Severity
    detail: str
    remediation: str


@dataclass(frozen=True)
class LifecyclePlan:
    """The planned (pre-start) or realised (post-start) lifecycle order."""

    components: tuple[tuple[str, int], ...] = ()
    realised: tuple[str, ...] | None = None
    failures: tuple[tuple[str, str], ...] = ()


class DiagnoseSettings(VarcoSettings):
    """
    ``VARCO_DIAGNOSE_*`` — presentation knobs for the startup wiring report
    (Plan 052 / §D-startup). The *gate* (whether an ERROR/WARNING refuses
    startup) is `create_varco_app(validate=, strict_validation=)` —
    code-controlled; these settings only control whether/how the report is
    *logged*, never suppress a problem line.

    Args:
        startup_report: Log the rendered ``report.render()`` block at INFO.
            ``True`` by default. Setting this ``False`` never suppresses
            the per-issue/per-finding WARNING/ERROR lines (§D-startup).
        startup_report_json: Log ``report.to_dict()`` as one JSON line
            instead of the rendered text block — for structured-log
            shippers. ``False`` by default.
    """

    model_config = SettingsConfigDict(env_prefix="VARCO_DIAGNOSE_", frozen=True)

    startup_report: bool = True
    startup_report_json: bool = False


def _interface_repr(interface: Any) -> str:
    """Display/JSON string for a ``SeamStatus.interface`` value (a type, generic alias, or already a string)."""
    if isinstance(interface, str):
        return interface
    module = getattr(interface, "__module__", None)
    name = getattr(interface, "__name__", None)
    if module and name:
        return f"{module}.{name}"
    return str(interface)


def _safe_get(container: DIContainer, interface: Any) -> Any | None:
    """
    Resolve *interface* if it is bound, tolerating providify's
    ``validate_bindings()`` side effect on first ``get()``.

    DESIGN: ``container.get()`` triggers a full, container-wide
    ``validate_bindings()`` scope-leak sweep the first time it is called on
    a container that never fully validated clean — and that sweep
    **raises** for *any* scope leak anywhere in the graph, not just on the
    interface being resolved. A wiring report's entire purpose is to still
    render for a container that has errors, so any resolution this module
    performs for its own bookkeeping (``ModuleRegistry``, a revocation
    store/registry for §D-wired) must not let an unrelated leak abort the
    whole report. The result is absent, not a crash — the seams/findings
    it feeds simply render as unavailable for this call.
    """
    try:
        if not container.is_resolvable(interface):
            return None
        return container.get(interface)
    except Exception:  # noqa: BLE001 — see DESIGN above: never let this crash the report
        return None


def _owner_name(binding: Any) -> str:
    """Best-effort human-readable name of a raw providify binding's implementation."""
    implementation = getattr(binding, "implementation", None)
    if implementation is not None:
        return str(getattr(implementation, "__name__", repr(implementation)))
    fn = getattr(binding, "fn", None)
    if fn is not None:
        return str(getattr(fn, "__qualname__", getattr(fn, "__name__", repr(fn))))
    return repr(binding)


def _summary_from_binding(binding: Any) -> BindingSummary:
    """Build a ``BindingSummary`` from a raw providify binding object (``get_binding()``'s result)."""
    owner = _owner_name(binding)
    implementation = getattr(binding, "implementation", None)
    impl_name = getattr(implementation, "__name__", owner) if implementation is not None else owner
    package_source = implementation if implementation is not None else getattr(binding, "fn", None)
    package = (getattr(package_source, "__module__", "") or "").split(".", 1)[0]
    conditions = tuple(marker.describe() for marker in getattr(binding, "conditions", ()))
    active = all(marker.is_satisfied() for marker in getattr(binding, "conditions", ()))
    return BindingSummary(
        owner=owner,
        implementation=impl_name,
        package=package,
        qualifier=getattr(binding, "qualifier", None),
        priority=getattr(binding, "priority", 0),
        fallback=getattr(binding, "fallback", False),
        shape="class" if implementation is not None else "provider",
        conditions=conditions,
        profiles=tuple(getattr(binding, "profiles", ())),
        active=active,
    )


def _summary_from_record(record: Any) -> BindingSummary:
    """Build a ``BindingSummary`` from a 047 ``BindingRecord``."""
    implementation_name = getattr(record.implementation, "__name__", None) or record.owner
    module_src = record.implementation
    package = (
        (getattr(module_src, "__module__", "") or "").split(".", 1)[0]
        if module_src is not None
        else ""
    )
    return BindingSummary(
        owner=record.owner,
        implementation=implementation_name,
        package=package,
        qualifier=record.qualifier,
        priority=record.priority,
        fallback=record.fallback,
        shape="class" if record.implementation is not None else "provider",
        conditions=record.conditions,
        profiles=getattr(record, "profiles", ()),
        active=record.active,
    )


def _live_key(binding: Any) -> tuple[str, str | None, int, bool]:
    """A value-tuple identifying a raw binding among same-interface ``BindingRecord`` candidates."""
    return (
        _owner_name(binding),
        getattr(binding, "qualifier", None),
        getattr(binding, "priority", 0),
        getattr(binding, "fallback", False),
    )


def _record_key(record: Any) -> tuple[str, str | None, int, bool]:
    """The ``BindingRecord`` counterpart of ``_live_key`` — see that function's docstring."""
    return (record.owner, record.qualifier, record.priority, record.fallback)


def _seam_rows(container: DIContainer) -> tuple[SeamStatus, ...]:
    """The Seams section — one row per 047 seam (§D-provenance)."""
    from varco_core.di.introspect import inspect_bindings  # noqa: PLC0415
    from varco_core.di.seams import SEAMS, interface_of  # noqa: PLC0415
    from varco_core.di.select import inspect_backend_selection  # noqa: PLC0415

    all_records = inspect_bindings(container)
    selection_by_seam = {s.seam: s for s in inspect_backend_selection(container)}

    rows: list[SeamStatus] = []
    for seam in SEAMS:
        interface = interface_of(seam)
        selection = selection_by_seam[seam.seam]
        records = [r for r in all_records if r.interface is interface]

        try:
            live_binding = container.get_binding(interface)
        except LookupError:
            live_binding = None

        if live_binding is None:
            source: Provenance = "absent"
        else:
            source = selection.source  # type: ignore[assignment]
            profiles = tuple(getattr(live_binding, "profiles", ()))
            if (
                not getattr(live_binding, "fallback", True)
                and profiles
                and (set(profiles) & container.active_profiles)
            ):
                source = "profile"

        live_key = _live_key(live_binding) if live_binding is not None else None
        shadowed = tuple(
            _summary_from_record(r)
            for r in records
            if r.fallback and r.active and _record_key(r) != live_key
        )
        inactive = tuple(_summary_from_record(r) for r in records if not r.active)

        rows.append(
            SeamStatus(
                kind="seam",
                name=seam.seam,
                interface=interface,
                qualifier=getattr(live_binding, "qualifier", None)
                if live_binding is not None
                else None,
                env=seam.env,
                env_value=os.environ.get(seam.env),
                live=_summary_from_binding(live_binding) if live_binding is not None else None,
                source=source,
                shadowed=shadowed,
                inactive=inactive,
            )
        )
    return tuple(rows)


def _feature_rows(container: DIContainer) -> tuple[SeamStatus, ...]:
    """The Features section — one row per 047 ``Feature`` (§D-provenance)."""
    import importlib  # noqa: PLC0415

    from varco_core.di import BACKEND_PRIORITY, is_user_bound  # noqa: PLC0415
    from varco_core.di.features import FEATURES  # noqa: PLC0415
    from varco_core.di.introspect import _category_for  # noqa: PLC0415

    rows: list[SeamStatus] = []
    for feat in FEATURES:
        module_path, _, name = feat.interface_dotted.partition(":")
        module = importlib.import_module(module_path)
        interface = getattr(module, name)
        try:
            live_binding = container.get_binding(interface)
        except LookupError:
            live_binding = None

        if live_binding is None:
            source: Provenance = "default"
            enabled = False
        else:
            category = _category_for(_owner_name(live_binding))
            enabled = category != "null"
            if is_user_bound(container, interface):
                source = "code"
            elif (
                getattr(live_binding, "fallback", False)
                and getattr(live_binding, "priority", 0) == BACKEND_PRIORITY
                and getattr(live_binding, "conditions", ())
            ):
                source = "env"
            else:
                source = "default"

        rows.append(
            SeamStatus(
                kind="feature",
                name=feat.feature,
                interface=interface,
                qualifier=None,
                env=feat.env,
                env_value=os.environ.get(feat.env) if feat.env else None,
                live=_summary_from_binding(live_binding) if live_binding is not None else None,
                source=source,
                shadowed=(),
                inactive=(),
                enabled=enabled,
            )
        )
    return tuple(rows)


def _other_default_rows(container: DIContainer) -> tuple[SeamStatus, ...]:
    """
    The Other defaults section — one row per fallback binding 047's
    ``catalogue()`` would classify as "other" (a varco default that is
    neither a seam, a feature, nor a settings class).

    DESIGN: re-derives the classification directly from
    ``inspect_bindings()`` rather than round-tripping through ``catalogue()
    .other_defaults`` (whose entries carry a ``"module:Name"`` *string*,
    §D-introspect's JSON schema). A generic alias interface (e.g.
    ``Serializer[Event]``) has no importable ``__name__`` attribute, so
    reconstructing an object from that string loses identity — and the
    ``di_wiring`` conformance suite's ``expected_defaults`` (§D-tests)
    asserts membership by identity against the real interface object it
    imported. Duplicating the small classification predicate here keeps
    every row's ``interface`` field a real object, always.
    """
    import importlib  # noqa: PLC0415

    from varco_core.di import is_user_bound  # noqa: PLC0415
    from varco_core.di.features import FEATURES  # noqa: PLC0415
    from varco_core.di.introspect import _is_settings_class, inspect_bindings  # noqa: PLC0415
    from varco_core.di.seams import SEAMS, interface_of  # noqa: PLC0415

    seam_interfaces: set[Any] = set()
    for seam in SEAMS:
        try:
            seam_interfaces.add(interface_of(seam))
        except (ImportError, AttributeError):  # pragma: no cover — defensive, mirrors catalogue()
            continue

    feature_interfaces: set[Any] = set()
    for feat in FEATURES:
        module_path, _, name = feat.interface_dotted.partition(":")
        try:
            module = importlib.import_module(module_path)
            feature_interfaces.add(getattr(module, name))
        except (ImportError, AttributeError):  # pragma: no cover — defensive, mirrors catalogue()
            continue

    rows: list[SeamStatus] = []
    for record in inspect_bindings(container):
        if not record.fallback or record.exact_only:
            continue
        interface = record.interface
        if interface in seam_interfaces or interface in feature_interfaces:
            continue
        if _is_settings_class(record.implementation):
            continue
        module_name = getattr(interface, "__module__", "") or ""
        if not module_name.startswith("varco_"):
            # catalogue() raises CatalogueError here (an undeclared default is
            # a build-time failure for the committed snapshot); a runtime
            # report must never crash on the same condition, so it is simply
            # omitted — the snapshot gate is what enforces completeness.
            continue

        try:
            live_binding = container.get_binding(interface, qualifier=record.qualifier)
        except LookupError:
            live_binding = None

        code = is_user_bound(container, interface, qualifier=record.qualifier)
        source: Provenance = "code" if code else "default"

        rows.append(
            SeamStatus(
                kind="other",
                name=_interface_repr(interface),
                interface=interface,
                qualifier=record.qualifier,
                env=None,
                env_value=None,
                live=_summary_from_binding(live_binding) if live_binding is not None else None,
                source=source,
                shadowed=(),
                inactive=(),
            )
        )
    return tuple(rows)


def _collection_rows(container: DIContainer) -> tuple[SeamStatus, ...]:
    """
    The Collections section — one row per 051 collection seam.

    DESIGN: reads ``get_all_bindings()`` (a pure, non-instantiating read),
    never ``collect()`` — ``collect()`` goes through ``container.get()``,
    which triggers providify's ``validate_bindings()`` as a side effect on
    first call and **raises** ``ScopeViolationDetectedError`` unconditionally
    if *any* binding anywhere on the container has a scope leak, even one
    completely unrelated to this seam. A wiring report's entire purpose is
    to still render on a container with errors — it must never itself
    trigger a hard raise as a side effect of reading one section.
    """
    from varco_core.di.collect import collection_seams  # noqa: PLC0415

    rows: list[SeamStatus] = []
    for seam in collection_seams():
        bindings = [
            b for b in container.get_all_bindings(seam) if not getattr(b, "fallback", False)
        ]
        contributors = tuple(_summary_from_binding(b) for b in bindings)
        rows.append(
            SeamStatus(
                kind="collection",
                name=seam.__name__,
                interface=f"{seam.__module__}.{seam.__name__}",
                qualifier=None,
                env=None,
                env_value=None,
                live=None,
                source="code" if contributors else "absent",
                shadowed=(),
                inactive=(),
                contributors=contributors,
            )
        )
    return tuple(rows)


def _settings_rows(container: DIContainer) -> tuple[SettingsStatus, ...]:
    """The Settings section — one row per 048 ``all_provenance()`` record."""
    from varco_core.settings import all_provenance  # noqa: PLC0415

    return tuple(
        SettingsStatus(
            cls=f"{prov.cls.__module__}.{prov.cls.__qualname__}",
            qualifier=prov.qualifier,
            env_prefix=prov.env_prefix,
            source=prov.source,
            fields=dict(prov.fields),
        )
        for prov in all_provenance(container)
    )


def _module_rows(
    container: DIContainer,
    modules: Sequence[VarcoModule] | None,
    module_validate_messages: Mapping[VarcoModule, Sequence[str]] | None,
) -> tuple[ModuleStatus, ...]:
    """The Modules section — from the container's ``ModuleRegistry`` (or an explicit ``modules=``)."""
    from varco_core.di.lifecycle import Phase  # noqa: PLC0415
    from varco_core.di.module import ModuleRegistry  # noqa: PLC0415

    registry: ModuleRegistry | None = _safe_get(container, ModuleRegistry)

    mods: Sequence[VarcoModule]
    if modules is not None:
        mods = modules
    elif registry is not None:
        mods = registry.modules
    else:
        mods = ()

    messages_by_module = module_validate_messages or {}

    rows: list[ModuleStatus] = []
    for module in mods:
        if registry is not None:
            try:
                enabled = registry.enabled(module)
            except KeyError:
                enabled = module.is_enabled(container)
        else:
            enabled = module.is_enabled(container)

        enabled_kwarg = getattr(module, "_enabled_kwarg", None)
        enabled_source: Literal["code", "env", "default"] = (
            "code" if enabled_kwarg is not None else "default"
        )

        phase = getattr(module, "phase", Phase.CONSUMERS)
        try:
            phase_name = Phase(phase).name
        except ValueError:
            phase_name = str(phase)

        depends_on = tuple(c.__name__ for c in getattr(module, "depends_on", ()))

        components: tuple[tuple[str, int], ...] = ()
        if enabled:
            try:
                components = tuple((c.name, c.phase) for c in module.components(container))
            except Exception:  # noqa: BLE001 — a component-listing failure must not break the report
                components = ()

        rows.append(
            ModuleStatus(
                name=module.name,
                cls=type(module).__name__,
                package=(type(module).__module__ or "").split(".", 1)[0],
                enabled=enabled,
                enabled_source=enabled_source,
                phase=int(phase),
                phase_name=phase_name,
                depends_on=depends_on,
                components=components,
                validate_messages=tuple(messages_by_module.get(module, ())),
            )
        )
    return tuple(rows)


def _lifecycle_plan(
    container: DIContainer,
    modules: Sequence[VarcoModule] | None,
    lifecycle: Sequence[Any] | None,
) -> LifecyclePlan:
    """The Lifecycle section — planned order from ``phased_components()``, when derivable."""
    if lifecycle is not None:
        components = tuple((c.name, c.phase) for c in lifecycle)
        return LifecyclePlan(components=components, realised=None, failures=())

    if modules:
        from varco_core.di.lifecycle import phased_components  # noqa: PLC0415

        try:
            planned = phased_components(list(modules), container)
        except Exception:  # noqa: BLE001 — never let lifecycle-planning break the report
            return LifecyclePlan()
        return LifecyclePlan(
            components=tuple((c.name, c.phase) for c in planned), realised=None, failures=()
        )

    return LifecyclePlan()


def _unowned_settings_findings(
    module_rows: Sequence[ModuleStatus], *, explicit_modules: bool
) -> list[WiringFinding]:
    """
    ``settings.unowned`` — ``VARCO_MIGRATE_MODE``/``VARCO_TENANCY_ISOLATION``
    set with no module owning them (§D-wired; moved here from
    ``create_varco_app``'s deleted ``_warn_unowned_settings``).

    Modules are matched by their stable ``name`` slot (``"migrations"`` /
    ``"tenancy"``), never by class, because ``varco_core`` must not import
    ``varco_fastapi``'s module classes.

    Args:
        module_rows: The report's Modules section.
        explicit_modules: ``True`` when the caller passed ``modules=`` (or a
            registry exists) — a hand-wired container with no module
            concept at all is never accused of a missing module.

    Returns:
        Zero to two WARNING findings.
    """
    if not explicit_modules and not module_rows:
        return []
    owned = {m.name for m in module_rows if m.enabled}
    findings: list[WiringFinding] = []
    if "migrations" not in owned and "VARCO_MIGRATE_MODE" in os.environ:
        findings.append(
            WiringFinding(
                check="settings.unowned",
                severity=Severity.WARNING,
                detail=(
                    f"VARCO_MIGRATE_MODE={os.environ['VARCO_MIGRATE_MODE']!r} is set but no "
                    "MigrationsModule is registered — no migration will run."
                ),
                remediation="add MigrationsModule(<migrator>) to modules=",
            )
        )
    if "tenancy" not in owned and "VARCO_TENANCY_ISOLATION" in os.environ:
        findings.append(
            WiringFinding(
                check="settings.unowned",
                severity=Severity.WARNING,
                detail=(
                    "VARCO_TENANCY_ISOLATION is set but no TenancyModule is registered — "
                    "no tenancy lifecycle will run."
                ),
                remediation="add TenancyModule(pool=...) to modules=",
            )
        )
    return findings


def _bootstrapped_unselected_findings() -> list[WiringFinding]:
    """
    ``di.bootstrapped_unselected`` — a backend package is bootstrapped but
    its ``VARCO_<SEAM>_BACKEND`` is unset, so the in-process default is
    live (§D-wired). Uses the non-logging evaluator so the WARNING is not
    emitted twice; a typo's ``BackendSelectionError`` propagates.

    Note:
        The registry behind this is process-global (``_REGISTERED_VALUES``):
        any backend imported/bootstrapped anywhere in the interpreter counts,
        so the finding can fire for an app that never wired that backend.

    Returns:
        One WARNING finding per such seam.

    Raises:
        BackendSelectionError: A ``VARCO_<SEAM>_BACKEND`` value is a typo
            (propagated from ``evaluate_backend_selection``).
    """
    from varco_core.di.seams import seam_by_name  # noqa: PLC0415
    from varco_core.di.select import evaluate_backend_selection  # noqa: PLC0415

    findings: list[WiringFinding] = []
    for check in evaluate_backend_selection().checks:
        if check.kind == "unset" and check.registered_values:
            default_value = seam_by_name(check.seam).default_value
            findings.append(
                WiringFinding(
                    check="di.bootstrapped_unselected",
                    severity=Severity.WARNING,
                    detail=(
                        f"{check.seam} is bootstrapped (registered values: "
                        f"{', '.join(sorted(check.registered_values))}) but {check.env} is "
                        "unset — the in-process default is live."
                    ),
                    remediation=f"set {check.env}, or set it to {default_value!r} to silence this",
                )
            )
    return findings


def _wired_findings(
    container: DIContainer,
    module_validate_messages: Mapping[VarcoModule, Sequence[str]] | None,
    *,
    module_rows: Sequence[ModuleStatus] = (),
    explicit_modules: bool = False,
    deprecations: Sequence[DeprecationRecord] = (),
) -> tuple[WiringFinding, ...]:
    """The "bound but not wired" findings (§D-wired)."""
    findings: list[WiringFinding] = []

    findings.extend(_bootstrapped_unselected_findings())
    findings.extend(_unowned_settings_findings(module_rows, explicit_modules=explicit_modules))

    # di.revocation_store_not_wired
    try:
        from varco_core.authority.registry import TrustedIssuerRegistry  # noqa: PLC0415
        from varco_core.revocation.base import AbstractTokenRevocationStore  # noqa: PLC0415
        from varco_core.revocation.posture import inspect_revocation_posture  # noqa: PLC0415

        store = _safe_get(container, AbstractTokenRevocationStore)
        registry = _safe_get(container, TrustedIssuerRegistry)
        if registry is not None:
            posture = inspect_revocation_posture(registry=registry, store=store)
            if posture.store_bound and not posture.registry_wired:
                findings.append(
                    WiringFinding(
                        check="di.revocation_store_not_wired",
                        severity=Severity.WARNING,
                        detail=(
                            f"A {posture.store_kind} is bound, but no TrustedIssuerRegistry "
                            f"was wired to use it."
                        ),
                        remediation="TrustedIssuerRegistry(revocation_store=...)",
                    )
                )
    except ImportError:  # pragma: no cover — defensive, revocation is always shipped in varco_core
        pass

    # module.<name>.validate
    for module, messages in (module_validate_messages or {}).items():
        for message in messages:
            findings.append(
                WiringFinding(
                    check=f"module.{module.name}.validate",
                    severity=Severity.WARNING,
                    detail=message,
                    remediation=f"see {type(module).__name__}.validate()",
                )
            )

    # deprecation.fired — one INFO per record; never blocks (§D-wired).
    for record in deprecations:
        findings.append(
            WiringFinding(
                check="deprecation.fired",
                severity=Severity.INFO,
                detail=f"{record.subject} (x{record.count}) at {record.first_location}",
                remediation=record.replacement or "see the deprecation message",
            )
        )

    return tuple(findings)


def _deprecation_records() -> tuple[DeprecationRecord, ...]:
    """The Deprecations section — the process-global ledger, as-is."""
    return fired_deprecations()


def build_wiring_report(
    container: DIContainer,
    *,
    validation: ValidationReport | None,
    modules: Sequence[VarcoModule] | None = None,
    lifecycle: Sequence[Any] | None = None,
    extra_findings: Sequence[WiringFinding] = (),
    module_validate_messages: Mapping[VarcoModule, Sequence[str]] | None = None,
    static: bool = False,
    extra_deprecations: Sequence[DeprecationRecord] = (),
    environment: str = "unknown",
    enforce: str = "unknown",
    enforce_source: Literal["code", "env", "default", "derived"] = "default",
) -> WiringReport:
    """
    Assemble a ``WiringReport`` from public reads over *container*
    (Plan 052 / §D-model, §D-provenance).

    Args:
        container: The bootstrapped ``DIContainer`` to report on.
        validation: providify's own ``container.validate(raise_on_error=
            False)`` result, or ``None`` when the caller skipped validation
            (``validation.ran`` is ``False`` in that case).
        modules: The modules to report in the Modules/Lifecycle sections.
            ``None`` (default) reads the container's own ``ModuleRegistry``
            if one is bound, else the Modules section is empty.
        lifecycle: An explicit ``Sequence[LifecycleComponent]`` (the
            planned order the driver already computed via
            ``phased_components()``). ``None`` (default) computes it from
            *modules* when given, else the Lifecycle section is empty.
        extra_findings: Additional ``WiringFinding``s to fold in (used by
            the driver for 047's ``check_backend_selection()`` warnings and
            by tests).
        module_validate_messages: ``{module_instance: [messages]}`` — the
            049 ``validate()`` slot's output, sequenced here into
            ``module.<name>.validate`` findings.
        static: Whether this report was built without running
            ``async_init`` (the driver's ``--static`` mode) — rendered as a
            banner line; never changes any computed value.
        extra_deprecations: Records captured by a deprecation window the
            caller opened (direct ``warnings.warn`` sites); merged with the
            ledger, de-duplicated on ``subject``.
        environment, enforce, enforce_source: The report header's
            ``SecurityPostureSettings`` facts. Supplied by a
            ``varco_fastapi``-aware caller because ``varco_core`` must not
            import that class; default to the honest ``"unknown"``.
            ``enforce_source="derived"`` (Plan 054 / §D-compose) means
            ``enforce`` was unset and derived from ``environment`` — the
            renderer prints ``(derived from VARCO_SECURITY_ENV=<env>)``.

    Returns:
        A frozen ``WiringReport``.

    Example::

        validation = container.validate(raise_on_error=False)
        report = build_wiring_report(container, validation=validation)
        print(report.render())
    """
    seams = _seam_rows(container)
    features = _feature_rows(container)
    other_defaults = _other_default_rows(container)
    collections = _collection_rows(container)
    settings = _settings_rows(container)
    module_rows = _module_rows(container, modules, module_validate_messages)
    lifecycle_plan = _lifecycle_plan(container, modules, lifecycle)
    ledger = _deprecation_records()
    seen = {d.subject for d in ledger}
    deprecations = (*ledger, *(d for d in extra_deprecations if d.subject not in seen))

    from varco_core.di.introspect import catalogue  # noqa: PLC0415

    cat = catalogue(container)

    if validation is None:
        validation_summary = ValidationSummary(ran=False)
    else:
        validation_summary = ValidationSummary(
            ran=True,
            checked_bindings=validation.checked_bindings,
            errors=tuple(i.to_dict() for i in validation.errors),
            warnings=tuple(i.to_dict() for i in validation.warnings),
            infos=tuple(i.to_dict() for i in validation.infos),
        )

    findings = (
        *_wired_findings(
            container,
            module_validate_messages,
            module_rows=module_rows,
            explicit_modules=modules is not None,
            deprecations=deprecations,
        ),
        *extra_findings,
    )

    # DESIGN: `environment`/`enforce`/`enforce_source` describe
    # `varco_fastapi.posture.SecurityPostureSettings`, which `varco_core`
    # must never import (no sibling dependencies). The caller supplies them
    # (the startup preflight and `varco diagnose --app` do); a caller with
    # no fastapi in scope leaves the honest "unknown" defaults.
    # ✅ layering intact; ❌ a caller can pass a stale value — accepted.

    import importlib.metadata as metadata  # noqa: PLC0415

    try:
        varco_version = metadata.version("varco-core")
    except metadata.PackageNotFoundError:  # pragma: no cover — editable/dev tree
        varco_version = "unknown"

    return WiringReport(
        schema_version=1,
        varco_version=varco_version,
        providify_version=cat.providify_version,
        environment=environment,
        enforce=enforce,
        enforce_source=enforce_source,
        modules=module_rows,
        seams=seams,
        features=features,
        other_defaults=other_defaults,
        collections=collections,
        settings=settings,
        deprecations=deprecations,
        validation=validation_summary,
        findings=findings,
        lifecycle=lifecycle_plan,
        catalogue=cat.to_dict(),
        static=static,
    )


@dataclass(frozen=True)
class WiringReport:
    """
    One frozen, JSON-serialisable answer to "what is this app running and
    why" (§D-model). Build via :func:`build_wiring_report`; never
    constructed by hand outside tests.
    """

    schema_version: Literal[1]
    varco_version: str
    providify_version: str
    environment: str
    enforce: str
    enforce_source: Literal["code", "env", "default", "derived"]
    modules: tuple[ModuleStatus, ...]
    seams: tuple[SeamStatus, ...]
    features: tuple[SeamStatus, ...]
    other_defaults: tuple[SeamStatus, ...]
    collections: tuple[SeamStatus, ...]
    settings: tuple[SettingsStatus, ...]
    deprecations: tuple[DeprecationRecord, ...]
    validation: ValidationSummary
    findings: tuple[WiringFinding, ...]
    lifecycle: LifecyclePlan
    catalogue: dict[str, Any]
    static: bool = False

    def to_dict(self) -> dict[str, Any]:
        """JSON-safe representation — same "no additive tier" rule as 047's snapshot."""

        def _seam_dict(row: SeamStatus) -> dict[str, Any]:
            d = asdict(row)
            d["interface"] = _interface_repr(row.interface)
            return d

        return {
            "schema_version": self.schema_version,
            "varco_version": self.varco_version,
            "providify_version": self.providify_version,
            "environment": self.environment,
            "enforce": self.enforce,
            "enforce_source": self.enforce_source,
            "static": self.static,
            "modules": [asdict(m) for m in self.modules],
            "seams": [_seam_dict(s) for s in self.seams],
            "features": [_seam_dict(s) for s in self.features],
            "other_defaults": [_seam_dict(s) for s in self.other_defaults],
            "collections": [_seam_dict(s) for s in self.collections],
            "settings": [asdict(s) for s in self.settings],
            "deprecations": [asdict(d) for d in self.deprecations],
            "validation": asdict(self.validation),
            "findings": [{**asdict(f), "severity": str(f.severity)} for f in self.findings],
            "lifecycle": asdict(self.lifecycle),
            "catalogue": self.catalogue,
        }

    def render(self) -> str:
        """The text form ``varco diagnose`` and the startup log print."""
        lines: list[str] = []
        # Plan 054 / §D-compose: a derived mode names what it was derived
        # from, so an operator reading the report before deploy knows the
        # consequence (e.g. `refuse` because VARCO_SECURITY_ENV is unset).
        enforce_origin = (
            f"derived from VARCO_SECURITY_ENV={self.environment}"
            if self.enforce_source == "derived"
            else self.enforce_source
        )
        lines.append(
            f"varco {self.varco_version} · providify {self.providify_version} · "
            f"environment={self.environment} · enforce={self.enforce} ({enforce_origin})"
        )
        if self.static:
            lines.append(
                "static: async_init skipped — providers registered by an ainstall()'d "
                "configuration are absent, so a MISSING_BINDING naming one of them may be spurious"
            )

        lines.append("")
        lines.append("Modules")
        for m in self.modules:
            lines.append(
                f"  {m.name} · {m.phase_name} · enabled={m.enabled} ({m.enabled_source}) · "
                f"components={[c[0] for c in m.components]}"
            )
            for message in m.validate_messages:
                lines.append(f"    ! {message}")

        for title, rows in (
            ("Seams", self.seams),
            ("Features", self.features),
            ("Other defaults", self.other_defaults),
            ("Collections", self.collections),
        ):
            lines.append("")
            lines.append(title)
            for row in rows:
                live_desc = row.live.implementation if row.live is not None else "<none>"
                extra = f" enabled={row.enabled}" if row.enabled is not None else ""
                lines.append(f"  {row.name}  -> {live_desc}  source={row.source}{extra}")
                for shadow in row.shadowed:
                    lines.append(f"    shadowed: {shadow.implementation}")
                for inactive in row.inactive:
                    lines.append(
                        f"    inactive: {inactive.implementation} ({', '.join(inactive.conditions)})"
                    )
                for contributor in row.contributors:
                    lines.append(f"    contributor: {contributor.implementation}")

        lines.append("")
        lines.append("Settings")
        for s in self.settings:
            field_desc = " ".join(f"{k}={v}" for k, v in s.fields.items())
            lines.append(f"  {s.cls}  {s.source}  {field_desc}")

        lines.append("")
        lines.append("Deprecations")
        lines.append(f"  {len(self.deprecations)} fired")
        for d in self.deprecations:
            lines.append(f"    {d.subject} (x{d.count}) — {d.first_location}")

        lines.append("")
        lines.append("Lifecycle")
        for name, phase in self.lifecycle.components:
            lines.append(f"  {name} (phase={phase})")
        if self.lifecycle.realised is not None:
            lines.append(f"  realised: {list(self.lifecycle.realised)}")

        lines.append("")
        lines.append("Validation")
        lines.append(
            f"  {len(self.validation.errors)} errors, {len(self.validation.warnings)} warnings, "
            f"{len(self.validation.infos)} infos (ran={self.validation.ran})"
        )
        for issue in (*self.validation.errors, *self.validation.warnings):
            lines.append(
                f"    [{issue.get('severity')}] {issue.get('owner')}: {issue.get('message')}"
            )

        lines.append("")
        lines.append("Findings")
        for finding in self.findings:
            lines.append(
                f"  [{finding.severity}] {finding.check}: {finding.detail} (fix: {finding.remediation})"
            )

        return "\n".join(lines)

    def worst(self) -> Severity | None:
        """
        The worst severity across providify's validation **errors** and
        every varco-side ``WiringFinding``.

        DESIGN: deliberately excludes ``validation.warnings``/``.infos``
        (§D-startup's own vocabulary: a providify WARNING "describes a
        legal-but-notable pattern ... never raises on its own",
        `validation.py:44-45`). A stock, fully-default varco container
        already carries real ``missing_binding_defaulted`` WARNINGs (e.g.
        ``InMemoryEventBus.__init__``'s ``error_policy``/``dispatch_mode``
        parameters) purely from providify's own defaulted-parameter
        reporting — ``worst()`` answering "is this report clean" must not
        be poisoned by that noise every default app would otherwise carry.
        A caller that wants the raw provider tier reads
        ``self.validation.warnings``/``.infos`` directly. Note that
        ``run_wiring_preflight(strict=True)`` likewise promotes only
        varco-side WARNING *findings*, never ``validation.warnings``
        (Plan 052 deviation from §D-startup's table, which names "WARNING
        present" without distinguishing the two tiers).

        Returns:
            ``Severity.ERROR`` if providify reported an ERROR or any finding
            is ERROR; else the worst finding severity; ``None`` when there is
            nothing (note an INFO finding makes this ``Severity.INFO``, not
            ``None``).
        """
        severities: list[Severity] = [Severity(s.severity) for s in self.findings]
        if self.validation.errors:
            severities.append(Severity.ERROR)
        if not severities:
            return None
        order = {Severity.ERROR: 0, Severity.WARNING: 1, Severity.INFO: 2}
        return min(severities, key=lambda s: order[s])
