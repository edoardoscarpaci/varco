"""
varco_core.di.introspect
==========================
``inspect_bindings()`` (the one private ``container._bindings`` read,
relocated from 046's testkit shim — §D-introspect), and ``Catalogue``/
``catalogue()`` (the default-implementation catalogue — DI-17).
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from varco_core.di.errors import CatalogueError

if TYPE_CHECKING:
    from providify import DIContainer

_VALUE_FROM_CONDITION_RE = re.compile(r"value='([^']*)'")


@dataclass(frozen=True)
class BindingRecord:
    """
    One binding, as read off a container (§D-introspect).

    Args:
        interface: The type the binding resolves as.
        implementation: The concrete class (``ClassBinding``), or ``None``
            for a ``@Provider``-shaped (``ProviderBinding``) binding.
        owner: Best-effort human-readable name — the implementation
            class's ``__name__``, or the provider function's
            ``__qualname__``.
        qualifier: The binding's qualifier, if any.
        priority: The binding's priority.
        fallback: Whether ``@Fallback`` was applied.
        exact_only: Whether this is providify's synthetic self-binding
            entry (never a real varco default — excluded from the
            catalogue).
        conditions: Every ``@Requires`` marker's ``describe()`` string.
        active: Whether every condition currently holds (evaluated now,
            never cached).
        profiles: Every ``@Profile`` expression the implementation carries
            (``binding.py:202``, Plan 052 / premise 5 — added for the
            wiring report's ``source == "profile"`` provenance; empty for
            a plain unconditional binding).
    """

    interface: Any
    implementation: Any
    owner: str
    qualifier: str | None
    priority: int
    fallback: bool
    exact_only: bool
    conditions: tuple[str, ...]
    active: bool
    profiles: tuple[str, ...] = ()


def _binding_owner_name(binding: Any) -> str:
    """Best-effort human-readable name of a binding's implementation."""
    implementation = getattr(binding, "implementation", None)
    if implementation is not None:
        return getattr(implementation, "__name__", repr(implementation))
    fn = getattr(binding, "fn", None)
    if fn is not None:
        return getattr(fn, "__qualname__", getattr(fn, "__name__", repr(fn)))
    return repr(binding)


def inspect_bindings(container: DIContainer) -> tuple[BindingRecord, ...]:
    """
    Return a ``BindingRecord`` for every binding registered on
    *container* — the one private ``container._bindings`` read in
    ``varco_core`` (§D-introspect; the testkit's
    ``fallback_bindings()`` becomes a one-line alias over this).

    Args:
        container: The container to inspect.

    Returns:
        Every registered binding, in registration order.

    Edge cases:
        ``providify.container.BindingDescriptor`` (the public
        ``describe()`` surface) carries no ``fallback``/``conditions``
        field (P29) — this is why this one private read exists at all.
        Delete it once P29 ships a public fallback-aware read.
    """
    records: list[BindingRecord] = []
    for binding in container._bindings:  # noqa: SLF001 — the one sanctioned private read (P29)
        conditions = tuple(marker.describe() for marker in getattr(binding, "conditions", ()))
        active = all(marker.is_satisfied() for marker in getattr(binding, "conditions", ()))
        records.append(
            BindingRecord(
                interface=getattr(binding, "interface", None),
                implementation=getattr(binding, "implementation", None),
                owner=_binding_owner_name(binding),
                qualifier=getattr(binding, "qualifier", None),
                priority=getattr(binding, "priority", 0),
                fallback=getattr(binding, "fallback", False),
                exact_only=getattr(binding, "exact_only", False),
                conditions=conditions,
                active=active,
                profiles=tuple(getattr(binding, "profiles", ())),
            )
        )
    return tuple(records)


def _category_for(owner_name: str) -> str:
    """Infer a catalogue category from an implementation/owner name."""
    lowered = owner_name.lower()
    if "null" in lowered:
        return "null"
    if "inmemory" in lowered or "in_memory" in lowered:
        return "in-memory"
    return "production-safe"


def _value_from_conditions(conditions: tuple[str, ...]) -> str | None:
    """Extract the ``value='...'`` clause from a rendered `@Requires` marker."""
    for condition in conditions:
        match = _VALUE_FROM_CONDITION_RE.search(condition)
        if match:
            return match.group(1)
    return None


def _is_settings_class(cls: Any) -> bool:
    """Whether *cls* is a pydantic ``BaseSettings`` subclass."""
    if not isinstance(cls, type):
        return False
    try:
        from pydantic_settings import BaseSettings  # noqa: PLC0415
    except ImportError:
        return False
    return issubclass(cls, BaseSettings)


@dataclass(frozen=True)
class SeamDefaultEntry:
    value: str
    implementation: str
    package: str
    qualifier: str | None
    category: str


@dataclass(frozen=True)
class SeamBackendEntry:
    value: str
    implementation: str
    package: str
    owner: str
    qualifier: str | None
    priority: int
    shape: str


@dataclass(frozen=True)
class SeamCatalogueRow:
    seam: str
    env: str
    interface: str
    default: SeamDefaultEntry | None
    backends: tuple[SeamBackendEntry, ...] = field(default_factory=tuple)


@dataclass(frozen=True)
class FeatureDefaultEntry:
    implementation: str
    category: str


@dataclass(frozen=True)
class FeatureEnabledEntry:
    implementation: str
    owner: str


@dataclass(frozen=True)
class FeatureCatalogueRow:
    feature: str
    env: str | None
    shape: str
    interface: str
    category: str
    default_on: bool
    code: str
    default: FeatureDefaultEntry | None = None
    enabled: FeatureEnabledEntry | None = None


@dataclass(frozen=True)
class OtherDefaultEntry:
    interface: str
    implementation: str
    package: str
    qualifier: str | None
    priority: int
    shape: str
    category: str


@dataclass(frozen=True)
class Catalogue:
    """
    The full generated default-implementation catalogue (DI-17,
    §D-introspect's JSON schema, ``schema_version: 1``).
    """

    schema_version: int
    providify_version: str
    packages: tuple[str, ...]
    seams: tuple[SeamCatalogueRow, ...]
    features: tuple[FeatureCatalogueRow, ...]
    other_defaults: tuple[OtherDefaultEntry, ...]
    settings_defaults_count: int

    def to_dict(self) -> dict[str, Any]:
        """JSON-safe representation — the committed snapshot and 052's runtime report."""
        return {
            "schema_version": self.schema_version,
            "providify": self.providify_version,
            "packages": list(self.packages),
            "seams": [
                {
                    "seam": row.seam,
                    "env": row.env,
                    "interface": row.interface,
                    "default": None
                    if row.default is None
                    else {
                        "value": row.default.value,
                        "implementation": row.default.implementation,
                        "package": row.default.package,
                        "qualifier": row.default.qualifier,
                        "category": row.default.category,
                    },
                    "backends": [
                        {
                            "value": b.value,
                            "implementation": b.implementation,
                            "package": b.package,
                            "owner": b.owner,
                            "qualifier": b.qualifier,
                            "priority": b.priority,
                            "shape": b.shape,
                        }
                        for b in row.backends
                    ],
                }
                for row in self.seams
            ],
            "features": [
                {
                    "feature": row.feature,
                    "env": row.env,
                    "shape": row.shape,
                    "interface": row.interface,
                    "category": row.category,
                    "default_on": row.default_on,
                    "code": row.code,
                    "default": None
                    if row.default is None
                    else {
                        "implementation": row.default.implementation,
                        "category": row.default.category,
                    },
                    "enabled": None
                    if row.enabled is None
                    else {
                        "implementation": row.enabled.implementation,
                        "owner": row.enabled.owner,
                    },
                }
                for row in self.features
            ],
            "other_defaults": [
                {
                    "interface": d.interface,
                    "implementation": d.implementation,
                    "package": d.package,
                    "qualifier": d.qualifier,
                    "priority": d.priority,
                    "shape": d.shape,
                    "category": d.category,
                }
                for d in self.other_defaults
            ],
            "settings_defaults_count": self.settings_defaults_count,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Catalogue:
        """Inverse of ``to_dict()`` — used for the committed-snapshot round trip."""
        seams = tuple(
            SeamCatalogueRow(
                seam=row["seam"],
                env=row["env"],
                interface=row["interface"],
                default=None if row["default"] is None else SeamDefaultEntry(**row["default"]),
                backends=tuple(SeamBackendEntry(**b) for b in row["backends"]),
            )
            for row in data["seams"]
        )
        features = tuple(
            FeatureCatalogueRow(
                feature=row["feature"],
                env=row["env"],
                shape=row["shape"],
                interface=row["interface"],
                category=row["category"],
                default_on=row["default_on"],
                code=row["code"],
                default=None if row["default"] is None else FeatureDefaultEntry(**row["default"]),
                enabled=None if row["enabled"] is None else FeatureEnabledEntry(**row["enabled"]),
            )
            for row in data["features"]
        )
        other_defaults = tuple(OtherDefaultEntry(**d) for d in data["other_defaults"])
        return cls(
            schema_version=data["schema_version"],
            providify_version=data["providify"],
            packages=tuple(data["packages"]),
            seams=seams,
            features=features,
            other_defaults=other_defaults,
            settings_defaults_count=data["settings_defaults_count"],
        )

    @staticmethod
    def merge_backend_entries(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """
        Merge backend entries from multiple packages' catalogues, raising
        ``CatalogueError`` on a duplicate ``(seam, value)`` pair (§D-script
        — "a plugin re-using a shipped value must pick a distinct one").

        Args:
            entries: A flat list of ``{"seam": ..., "value": ..., ...}``
                dicts collected across every package's catalogue.

        Returns:
            The same list, unchanged, when no duplicate exists.

        Raises:
            CatalogueError: Two entries share the same ``(seam, value)``.
        """
        seen: dict[tuple[str, str], dict[str, Any]] = {}
        for entry in entries:
            key = (entry["seam"], entry["value"])
            if key in seen:
                raise CatalogueError(
                    f"Duplicate backend value {entry['value']!r} for seam "
                    f"{entry['seam']!r}: registered by both "
                    f"{seen[key].get('package')!r} and {entry.get('package')!r}."
                )
            seen[key] = entry
        return entries


def _providify_version() -> str:
    try:
        import importlib.metadata as metadata

        return metadata.version("providify")
    except Exception:  # noqa: BLE001 — best-effort, never a hard failure
        return "unknown"


def catalogue(container: DIContainer) -> Catalogue:
    """
    Classify every **fallback** binding on *container* into the
    catalogue's buckets (§D-introspect / DI-17): seam default, seam
    backend, feature default, feature twin, settings (counted, not
    listed), or "other default".

    Args:
        container: The bootstrapped container to classify.

    Returns:
        A ``Catalogue`` (``schema_version=1``).

    Raises:
        CatalogueError: A fallback binding fits no bucket — its
            interface does not belong to a known varco package (its
            ``__module__`` does not start with ``"varco_"``) and is not a
            recognised seam/feature/settings default. This is what turns
            an undeclared new default into a build failure instead of a
            silently stale catalogue.

    Example::

        result = catalogue(container)
        result.to_dict()["seams"]
    """
    from varco_core.di.features import FEATURES
    from varco_core.di.seams import SEAMS, interface_of

    records = inspect_bindings(container)
    fallback_records = [r for r in records if r.fallback and not r.exact_only]

    seam_by_interface: dict[Any, Any] = {}
    for seam in SEAMS:
        try:
            seam_by_interface[interface_of(seam)] = seam
        except (ImportError, AttributeError):  # pragma: no cover — defensive
            continue

    feature_by_interface: dict[Any, list[Any]] = {}
    for feat in FEATURES:
        module_path, _, name = feat.interface_dotted.partition(":")
        try:
            import importlib

            module = importlib.import_module(module_path)
            iface = getattr(module, name)
        except (ImportError, AttributeError):  # pragma: no cover — defensive
            continue
        feature_by_interface.setdefault(iface, []).append(feat)

    seam_state: dict[str, dict[str, Any]] = {
        s.seam: {"default": None, "backends": []} for s in SEAMS
    }
    feature_state: dict[str, dict[str, Any]] = {
        f.feature: {"default": None, "enabled": None} for f in FEATURES
    }
    other_defaults: list[OtherDefaultEntry] = []
    settings_count = 0
    packages: set[str] = set()

    for record in fallback_records:
        interface = record.interface
        module_name = getattr(interface, "__module__", "") or ""
        package = module_name.split(".", 1)[0]

        if _is_settings_class(record.implementation):
            settings_count += 1
            continue

        matched_seam = seam_by_interface.get(interface)
        if matched_seam is not None:
            packages.add(package)
            if record.priority == 0:
                seam_state[matched_seam.seam]["default"] = SeamDefaultEntry(
                    value=matched_seam.default_value,
                    implementation=record.owner,
                    package=package,
                    qualifier=record.qualifier,
                    category=_category_for(record.owner),
                )
            else:
                value = _value_from_conditions(record.conditions) or "?"
                seam_state[matched_seam.seam]["backends"].append(
                    SeamBackendEntry(
                        value=value,
                        implementation=record.owner,
                        package=package,
                        owner=record.owner,
                        qualifier=record.qualifier,
                        priority=record.priority,
                        shape="class" if record.implementation is not None else "provider",
                    )
                )
            continue

        feats = feature_by_interface.get(interface)
        if feats:
            matched = False
            for feat in feats:
                if record.priority == 0:
                    feature_state[feat.feature]["default"] = FeatureDefaultEntry(
                        implementation=record.owner, category=_category_for(record.owner)
                    )
                    matched = True
                else:
                    feature_state[feat.feature]["enabled"] = FeatureEnabledEntry(
                        implementation=record.owner, owner=record.owner
                    )
                    matched = True
            if matched:
                packages.add(package)
                continue

        if not module_name.startswith("varco_"):
            raise CatalogueError(
                f"catalogue(): fallback binding for {interface!r} (owner="
                f"{record.owner!r}) does not belong to any known seam/feature/"
                f"settings bucket, and its interface module {module_name!r} is "
                f"not a recognised varco package. Classify it explicitly — a new "
                f"default must never be silently omitted from the catalogue."
            )

        packages.add(package)
        other_defaults.append(
            OtherDefaultEntry(
                interface=f"{module_name}:{getattr(interface, '__name__', str(interface))}",
                implementation=record.owner,
                package=package,
                qualifier=record.qualifier,
                priority=record.priority,
                shape="class" if record.implementation is not None else "provider",
                category=_category_for(record.owner),
            )
        )

    seams_out = tuple(
        SeamCatalogueRow(
            seam=seam.seam,
            env=seam.env,
            interface=seam.interface_dotted,
            default=seam_state[seam.seam]["default"],
            backends=tuple(seam_state[seam.seam]["backends"]),
        )
        for seam in SEAMS
    )
    features_out = tuple(
        FeatureCatalogueRow(
            feature=feat.feature,
            env=feat.env,
            shape=feat.shape,
            interface=feat.interface_dotted,
            category=feat.category,
            default_on=feat.default_on,
            code=feat.code_verb,
            default=feature_state[feat.feature]["default"],
            enabled=feature_state[feat.feature]["enabled"],
        )
        for feat in FEATURES
    )

    return Catalogue(
        schema_version=1,
        providify_version=_providify_version(),
        packages=tuple(sorted(packages)),
        seams=seams_out,
        features=features_out,
        other_defaults=tuple(other_defaults),
        settings_defaults_count=settings_count,
    )


__all__ = [
    "BindingRecord",
    "Catalogue",
    "FeatureCatalogueRow",
    "FeatureDefaultEntry",
    "FeatureEnabledEntry",
    "OtherDefaultEntry",
    "SeamBackendEntry",
    "SeamCatalogueRow",
    "SeamDefaultEntry",
    "catalogue",
    "inspect_bindings",
]
