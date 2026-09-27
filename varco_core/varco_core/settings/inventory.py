"""
varco_core.settings.inventory
================================
Introspection over ``VarcoSettings``/``BaseSettings`` subclasses (Plan 048 /
DI-6, §D-script, §D-prefix). Backs three consumers: ``bind_settings()``'s
error messages (an env var name per invalid field), ``scripts/
settings_reference.py`` (the generated ``VARCO_*`` reference), and this
module's own uniqueness/allowlist checks.

DESIGN: a pure, class-shaped walk over ``model_fields`` — never an instance
    ✅ ``env_vars_for(cls)`` never constructs ``cls`` — the whole point is
       to describe a class's env-var surface *before* anything reads the
       environment, so it works even for a class with a required field
       (``CloudEventsSettings``) that can never construct from an empty
       environment.
    ✅ Reuses ``varco_core.redaction.is_sensitive_key``/``RedactionPolicy``
       for the "is this field secret" question instead of a second
       pattern list — the one-pattern-list rule CLAUDE.md's Redaction
       section states.
    ❌ Nested-model recursion is one level deep only (matches every
       in-tree nested-model field today, e.g. ``PostgresConnectionSettings
       .ssl``); a doubly-nested model would need a second pass. No in-tree
       class needs it yet.

Thread safety:  N/A — pure functions over class objects, no shared state.
Async safety:   N/A — no I/O; ``discover_settings_classes`` imports
    packages synchronously, exactly like ``scripts/api_surface.py``'s own
    ``discover_packages()`` (Plan 048 rides the same RL-18 discipline).
"""

from __future__ import annotations

import importlib
import pkgutil
from dataclasses import dataclass
from typing import Any, get_origin

from pydantic import BaseModel
from pydantic_settings import BaseSettings

from varco_core.redaction import RedactionPolicy, is_sensitive_key

#: Every ``VarcoSettings``/``BaseSettings`` subclass this plan deliberately
#: excludes from ``bind_settings()`` registration, with the one-line reason
#: required by §D-prefix rule 5. Keys are the *class objects* — imported
#: lazily inside ``discover_settings_classes`` callers, not here, to avoid
#: importing every backend package just to build this dict.
_EXCLUDED_SETTINGS: dict[type, str] = {}


def _register_excluded(cls: type, reason: str) -> None:
    """Idempotently record *cls* in ``_EXCLUDED_SETTINGS`` with *reason*."""
    _EXCLUDED_SETTINGS[cls] = reason


def _populate_excluded() -> None:
    """
    Populate ``_EXCLUDED_SETTINGS`` with the classes §D-core-list/§D-connection/
    §Non-goals name.

    ⚠️ This function runs at **this module's import time** — the unconditional
    call at module scope below, not on first use. It is not lazy, and nothing
    defers it. What *is* deferred is the set of imports in its body: they live
    here rather than at the module's top level so that importing ``inventory``
    pulls in only the two cheap ``varco_core``-internal modules named below,
    never a backend or ``varco_fastapi``. Keep it that way — the comment block
    at the end of this function records the 200-400 ms per-package regression
    that adding one ``varco_fastapi`` import here caused, and why it was
    removed. Anything expensive added to this body is paid by every package
    that imports any converted settings class.

    Edge cases:
        Idempotent — returns immediately once ``_EXCLUDED_SETTINGS`` is
        non-empty, so a second call (e.g. from a test) is free.
    """
    if _EXCLUDED_SETTINGS:
        return
    from varco_core.event.cloudevents import CloudEventsSettings

    _register_excluded(
        CloudEventsSettings,
        "requires `source`; stays on the explicit bind_cloudevents_serializer() path",
    )

    # §D-connection: the ConnectionSettings family describes a third-party
    # service in that service's own tooling's env-var names (POSTGRES_HOST,
    # REDIS_HOST, ...) — deliberately never VARCO_-prefixed, never
    # bind_settings()-registered. Excluded here so check_unique_env_names()
    # (which only enforces uniqueness among registered classes) never flags
    # them, and so test_settings_inventory's "every discovered class is
    # registered or excluded" rule stays satisfied.
    from varco_core.connection.base import ConnectionSettings

    _register_excluded(
        ConnectionSettings,
        "§D-connection: describes a third-party service's own env-var names, never VARCO_-prefixed",
    )
    # ⚠️ Deliberately NOT importing varco_fastapi.connection.HttpConnectionSettings
    # here, even guarded by try/except ImportError — this module is on the
    # transitive import path of `varco_core.settings` (imported by, e.g.,
    # varco_core.migration.settings, varco_core.tenancy.settings), and every
    # `varco_x` package that imports any converted settings class would pay a
    # full `import varco_fastapi` cost merely from `_populate_excluded()`
    # running at THIS module's own import time (measured: varco_redis/
    # varco_sa/varco_beanie/varco_memcached's import-budget all jumped
    # 200-400ms when this import was here). `check_unique_env_names`'s own
    # callers exclude it by class identity when they explicitly pass
    # ["varco_fastapi"] to discover_settings_classes() instead (see
    # test_settings_inventory.py's uniqueness test).


_populate_excluded()


@dataclass(frozen=True)
class EnvVarSpec:
    """
    Describes one field's env-var surface for a single ``VarcoSettings``
    subclass.

    Attributes:
        env: The fully-qualified environment variable name a caller sets
            to control this field (e.g. ``"VARCO_REDIS_URL"``), or
            ``None`` when the field cannot be populated from a single env
            var (a ``dict``/mapping field — "not env-readable, pass it via
            instance=").
        field_path: The field's dotted path within the class — a plain
            field name (``"url"``) or, for a nested ``BaseModel`` field,
            ``"outer.inner"`` (e.g. ``"ssl.ca_cert"``).
        type_repr: ``repr()`` of the field's annotation, for the generated
            reference table.
        default_repr: ``repr()`` of the field's default value, or ``"***"``
            when ``secret`` is ``True`` — a secret's default must never
            appear in the generated, committed reference page.
        description: The field's docstring/``Field(description=...)``, or
            ``""`` when none is set.
        secret: ``True`` when the field name matches
            ``varco_core.redaction``'s default sensitive-key patterns.
    """

    env: str | None
    field_path: str
    type_repr: str
    default_repr: str
    description: str
    secret: bool


_REDACTION_POLICY = RedactionPolicy()


def _is_secret_field(name: str) -> bool:
    """Whether *name* matches the default redaction key-name patterns."""
    return is_sensitive_key(name, _REDACTION_POLICY)


def _default_repr(field: Any, *, secret: bool) -> str:
    """
    Render a field's default value for the generated reference, masking a
    secret field's default so it never appears in a committed doc.
    """
    if secret:
        return "***"
    default = field.default
    # ``PydanticUndefined`` marks a required field with no default at all
    # (e.g. ``CloudEventsSettings.source``) — render it plainly rather than
    # pydantic's internal sentinel repr.
    if default is ... or type(default).__name__ == "PydanticUndefinedType":
        return "<required>"
    # A frozenset's repr() order depends on PYTHONHASHSEED (str hash
    # randomization) — non-deterministic across process runs, which would
    # make settings_reference.py --check flake in CI. Render it via a
    # sorted tuple instead so the reference snapshot is stable.
    if isinstance(default, frozenset):
        return f"frozenset({sorted(default)!r})"
    return repr(default)


def _alias_env_name(field: Any) -> str | None:
    """Return the first ``validation_alias``/``AliasChoices`` name, if any."""
    alias = field.validation_alias
    if alias is None:
        return None
    choices = getattr(alias, "choices", None)
    if choices:
        first = choices[0]
        return first if isinstance(first, str) else None
    return alias if isinstance(alias, str) else None


def env_vars_for(
    cls: type[BaseSettings], *, env_prefix: str | None = None
) -> tuple[EnvVarSpec, ...]:
    """
    Describe every env var *cls* reads, one ``EnvVarSpec`` per field (two
    per nested-``BaseModel`` field's own fields — see ``field_path``).

    Args:
        cls: A ``pydantic_settings.BaseSettings`` subclass (typically a
            ``VarcoSettings``).
        env_prefix: Override the class's own configured
            ``model_config["env_prefix"]`` — used by ``bind_settings()``'s
            DI-16 named-instance path, where a qualified binding reads a
            *different* prefix than the class's own default.

    Returns:
        A tuple of ``EnvVarSpec``, one per top-level field (or per
        nested-model leaf field, for a field whose annotation is itself a
        ``BaseModel``).

    Edge cases:
        - A ``dict``/mapping-typed field -> ``EnvVarSpec.env is None``
          ("not env-readable — pass it via ``instance=``").
        - A field with ``validation_alias``/``AliasChoices`` -> the first
          alias name is used verbatim as ``env`` (the prefix is not
          re-applied — the alias IS the full name, per
          ``SecurityPostureSettings.environment`` -> ``VARCO_SECURITY_ENV``).
        - A secret-named field -> ``secret=True``, ``default_repr="***"``.
    """
    prefix = env_prefix if env_prefix is not None else cls.model_config.get("env_prefix", "")
    delimiter = cls.model_config.get("env_nested_delimiter") or "__"
    specs: list[EnvVarSpec] = []

    for name, field in cls.model_fields.items():
        secret = _is_secret_field(name)
        annotation = field.annotation
        origin = get_origin(annotation)

        alias_env = _alias_env_name(field)
        if alias_env is not None:
            specs.append(
                EnvVarSpec(
                    env=alias_env,
                    field_path=name,
                    type_repr=repr(annotation),
                    default_repr=_default_repr(field, secret=secret),
                    description=field.description or "",
                    secret=secret,
                )
            )
            continue

        if origin in (dict,) or (isinstance(annotation, type) and issubclass(annotation, dict)):
            specs.append(
                EnvVarSpec(
                    env=None,
                    field_path=name,
                    type_repr=repr(annotation),
                    default_repr="not env-readable — pass via instance=",
                    description=field.description or "",
                    secret=secret,
                )
            )
            continue

        if (
            isinstance(annotation, type)
            and issubclass(annotation, BaseModel)
            and not issubclass(annotation, BaseSettings)
        ):
            outer_env = f"{prefix}{name.upper()}"
            for inner_name, inner_field in annotation.model_fields.items():
                inner_secret = _is_secret_field(inner_name)
                specs.append(
                    EnvVarSpec(
                        env=f"{outer_env}{delimiter}{inner_name.upper()}",
                        field_path=f"{name}.{inner_name}",
                        type_repr=repr(inner_field.annotation),
                        default_repr=_default_repr(inner_field, secret=inner_secret),
                        description=inner_field.description or "",
                        secret=inner_secret,
                    )
                )
            continue

        specs.append(
            EnvVarSpec(
                env=f"{prefix}{name.upper()}",
                field_path=name,
                type_repr=repr(annotation),
                default_repr=_default_repr(field, secret=secret),
                description=field.description or "",
                secret=secret,
            )
        )

    return tuple(specs)


def discover_settings_classes(packages: list[str]) -> list[type[BaseSettings]]:
    """
    Import every module of every package in *packages* and return every
    ``pydantic_settings.BaseSettings`` subclass reachable from them.

    Args:
        packages: Top-level importable package names (e.g. ``["varco_core",
            "varco_redis"]``).

    Returns:
        A de-duplicated list of ``BaseSettings`` subclasses, in first-seen
        order. ``BaseSettings`` itself is never included.

    Raises:
        ImportError: If any named package, or a module within it, fails to
            import — the caller (``scripts/settings_reference.py``) turns
            this into a named, non-zero exit rather than a silently shorter
            snapshot.
    """
    seen: dict[type, None] = {}
    for pkg_name in packages:
        package = importlib.import_module(pkg_name)
        if hasattr(package, "__path__"):
            for module_info in pkgutil.walk_packages(package.__path__, prefix=f"{pkg_name}."):
                importlib.import_module(module_info.name)
        for cls in _walk_subclasses(BaseSettings):
            if cls is not BaseSettings and cls.__module__.startswith(pkg_name):
                seen.setdefault(cls, None)
    return list(seen)


def _walk_subclasses(base: type) -> list[type]:
    """Return every transitive subclass of *base*, including itself."""
    result = [base]
    for sub in base.__subclasses__():
        result.extend(_walk_subclasses(sub))
    return result


def check_unique_env_names(classes: list[type[BaseSettings]]) -> list[str]:
    """
    Report every env var name claimed by more than one class in *classes*.

    Args:
        classes: The settings classes to check (typically the full result
            of ``discover_settings_classes``, minus label-keyed families
            per §D-prefix rule 4).

    Returns:
        A list of human-readable collision descriptions
        (``"VARCO_CLIENT_CERT: ClientProfile, TlsSettings"``), empty when
        every expanded env-var name across every class is unique. §D-prefix
        rule 2 — this checks *expanded names*, never prefix disjointness.
    """
    owners: dict[str, list[str]] = {}
    for cls in classes:
        for spec in env_vars_for(cls):
            if spec.env is None:
                continue
            owners.setdefault(spec.env, []).append(cls.__name__)

    collisions = []
    for env_name, class_names in sorted(owners.items()):
        if len(class_names) > 1:
            collisions.append(f"{env_name}: {', '.join(sorted(set(class_names)))}")
    return collisions


#: The frozen allowlist of loose ``VARCO_*`` env reads outside any settings
#: class (§D-loose). Each entry is a process-global/CLI-default/observability
#: read that is deliberately NOT folded into a settings class — see
#: technical_docs/features/dependency-injection.md's Settings section for
#: the reasoning behind each row. A new loose read that is not in this tuple
#: fails ``test_settings_no_loose_env_reads.py`` until it is either folded
#: into a settings class or added here with a reason (in the linked doc,
#: not inline — this tuple is grepped by the test, not read as prose).
LOOSE_ENV_READ_ALLOWLIST: tuple[str, ...] = (
    # §D-loose — allowlisted by design (see the module's own §D-loose table):
    "varco_core/varco_core/observability/params.py:669",
    "varco_core/varco_core/observability/params.py:676",
    "varco_core/varco_core/profiling/config.py:33",
    "varco_fastapi/varco_fastapi/app.py:406",
    "varco_fastapi/varco_fastapi/auth/server_auth.py:435",
    "varco_core/varco_core/cli/tenant.py:71",
    "varco_core/varco_core/cli/retention.py:66",
    "varco_core/varco_core/cli/retention.py:67",
    "varco_core/varco_core/cli/retention.py:75",
    "varco_core/varco_core/cli/retention.py:76",
    # §D-loose — the observability global-attribute registry. These four reads
    # are allowlisted by §D-loose's own table, but were INVISIBLE to this
    # guard until Plan 048's repair round 2 replaced its line-oriented regex
    # with an AST walk: `attributes.py` aliases the mapping first
    # (`env = environ if environ is not None else os.environ`) and then reads
    # `env.get("VARCO_...")`, so the literal token `environ` never appears on
    # the reading line. They are genuine, sanctioned loose reads — the global
    # attribute registry is a process-wide observability concern read once at
    # import/registration time, deliberately not a bound settings class
    # (§Out of scope / Parked: folding VARCO_OTEL_* into an ObservabilitySettings
    # would redesign OtelConfig, which is code-only by design).
    "varco_core/varco_core/observability/attributes.py:450",
    "varco_core/varco_core/observability/attributes.py:461",
    "varco_core/varco_core/observability/attributes.py:477",
    "varco_core/varco_core/observability/attributes.py:478",
)


__all__ = [
    "LOOSE_ENV_READ_ALLOWLIST",
    "EnvVarSpec",
    "check_unique_env_names",
    "discover_settings_classes",
    "env_vars_for",
]
