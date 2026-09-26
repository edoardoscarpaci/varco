"""
varco_core.settings.bind
===========================
``bind_settings()`` — the one recipe every varco settings class is
registered through (Plan 048 / DI-6, §D-provider/§D-eager/§D-precedence/
§D-named).

DESIGN: a ``@Provider`` closure over an already-constructed value, never a
class binding, never a lazily-reconstructing factory
    ✅ ``resolve_params()`` skips ``VAR_POSITIONAL``/``VAR_KEYWORD``
       (``providify/_annotations.py:574-578``), but that is not why a class
       binding is rejected here — the real trap is that an unresolved
       *annotated, defaulted* keyword parameter is silently left to its
       default (``providify/container.py:3753-3759``), and
       ``pydantic_settings.BaseSettings.__init__`` declares 29 of exactly
       that shape (``_env_prefix: str | None = None`` etc.,
       ``pydantic_settings/main.py:180-210``). A class binding's
       configuration would then depend on whatever else happens to be
       bound in the container. A zero-argument closure has no parameters
       for providify's resolver to see at all.
    ✅ The value is constructed **once, eagerly, at the ``bind_settings()``
       call** (fail-fast — brief 015 §Evidence Gap 4's "synchronous with
       registration", not "somewhere before first use"), then the closure
       simply returns that already-built value. Whether the closure is
       ever *called* by the container is a separate question — see the
       precedence note below.
    ❌ The env is read even when a competing non-fallback binding will
       shadow ours — mitigated: only the FALLBACK path constructs eagerly;
       when a non-fallback binding for ``(cls, qualifier)`` already exists
       BEFORE this call, ``bind_settings()`` skips constructing anything at
       all and simply resolves+returns the winner (see the "already
       fully-bound" branch below) — this is what makes
       ``test_d_code_override_registered_before_bind_settings_never_reads_env``
       true even though, in the general case, the fallback path is eager.

DESIGN: providify's own fallback semantics decide precedence, not this module
    A ``@Fallback`` binding is *shadowed*, not skipped — it is still
    registered so ``container.validate()`` can report ``FALLBACK_SHADOWED``
    (an ``INFO``, brief 015's "one precedence order, not per-setting"
    convergence), but the CONTAINER, not this function, decides at resolve
    time which candidate wins (``providify/decorator/scope.py:976-1053``).
    This is why the fallback branch below always registers, even when a
    non-fallback binding already exists elsewhere in the container: the
    registration itself is cheap (a closure over an already-built value,
    never re-invoked unless it actually wins), and skipping it would make
    ``FALLBACK_SHADOWED`` detection impossible.

Thread safety:  ⚠️ Not synchronized — ``bind_settings()`` mutates a
    module-level, per-container bookkeeping dict. Call it from a single
    bootstrap thread/coroutine, the same discipline every other
    ``bind_*``/``enable_*`` helper in this tree already requires.
Async safety:   ✅ Fully synchronous — no ``await`` anywhere in this module.
"""

from __future__ import annotations

import logging
import os
import weakref
from dataclasses import dataclass
from typing import Literal

from providify import DIContainer, Fallback, Provider
from pydantic import ValidationError
from pydantic_settings import BaseSettings

from varco_core.settings.errors import SettingsAlreadyBoundError, SettingsValidationError
from varco_core.settings.inventory import env_vars_for

_Kind = Literal["instance", "fallback"]

_logger = logging.getLogger(__name__)

#: §Risks' "renames break deployments silently" mitigation — a one-release
#: (removed in 5.0.0) startup WARNING naming old -> new whenever a 4.0.0
#: rename's *old* name is still present in ``os.environ``. Two shapes:
#: exact names (JWKS/TLS, whose new names are simple 1:1 renames) and
#: prefixes (``VARCO_PROFILER_``/``FASTREST_AUTHORIZATION__``, which cover
#: a whole family of field-suffixed names under one feature).
#: Keep in sync with this plan's §Migration table (plans/048-…, "Old"/"New"
#: columns) — a new rename lands here, never as a silent default fallback.
_LEGACY_EXACT_NAMES: tuple[tuple[str, str], ...] = (
    ("VARCO_CA_CERT", "VARCO_TLS_CA_CERT"),
    ("VARCO_CLIENT_CERT", "VARCO_TLS_CLIENT_CERT"),
    ("VARCO_CLIENT_KEY", "VARCO_TLS_CLIENT_KEY"),
    ("VARCO_TRUST_STORE_DIR", "VARCO_TLS_TRUST_STORE_DIR"),
    ("VARCO_JWKS_MIN_REFRESH_SECONDS", "VARCO_JWT_JWKS_MIN_REFRESH_SECONDS"),
    ("VARCO_JWKS_TTL_SECONDS", "VARCO_JWT_JWKS_TTL_SECONDS"),
)
_LEGACY_PREFIXES: tuple[tuple[str, str], ...] = (
    ("VARCO_PROFILER_", "VARCO_PROFILING_"),
    ("FASTREST_AUTHORIZATION__", "VARCO_AUTHORIZATION__"),
)

#: Per-container, per-name "already warned" bookkeeping — a
#: ``WeakKeyDictionary`` (same rationale as ``_REGISTRY`` below) so a
#: process that calls ``bind_settings()`` many times during bootstrap logs
#: each legacy name exactly once per container, not once per call.
_LEGACY_WARNED: weakref.WeakKeyDictionary[DIContainer, set[str]] = weakref.WeakKeyDictionary()


def _warn_legacy_env_names(container: DIContainer) -> None:
    """
    Log one WARNING per legacy env-var name still present in ``os.environ``,
    naming the old name and its 4.0.0 replacement (removed in 5.0.0).

    Called unconditionally at the top of every ``bind_settings()`` call —
    cheap (a handful of ``in os.environ`` / ``startswith`` checks) and
    deduplicated per *container* so a bootstrap sequence with dozens of
    ``bind_settings()`` calls logs each hit exactly once.

    Args:
        container: The container this ``bind_settings()`` call targets —
            only used as the dedup key, never inspected otherwise.

    Edge cases:
        - A prefixed legacy name (e.g. ``VARCO_PROFILER_ENABLED``) warns
          once per *distinct* old env var name, not once per prefix — two
          different suffixes under the same legacy prefix both warn.
    """
    warned = _LEGACY_WARNED.setdefault(container, set())
    for old, new in _LEGACY_EXACT_NAMES:
        if old in os.environ and old not in warned:
            warned.add(old)
            _logger.warning(
                "%s is deprecated and will stop being read in 5.0.0 — use %s instead", old, new
            )
    for old_prefix, new_prefix in _LEGACY_PREFIXES:
        for name in os.environ:
            if name.startswith(old_prefix) and name not in warned:
                warned.add(name)
                new_name = new_prefix + name[len(old_prefix) :]
                _logger.warning(
                    "%s is deprecated and will stop being read in 5.0.0 — use %s instead",
                    name,
                    new_name,
                )


@dataclass
class _BindRecord:
    """Per-``(cls, qualifier)`` bookkeeping this module owns on a container."""

    kind: _Kind
    value: BaseSettings | None
    env_prefix: str


@dataclass(frozen=True)
class SettingsProvenance:
    """
    Per-field provenance for one bound settings class, for 052's ``varco
    diagnose`` to render.

    Attributes:
        cls: The settings class.
        qualifier: The qualifier it was bound under, or ``None``.
        env_prefix: The env-var prefix actually used to construct it.
        fields: ``{field_name: "code" | "env" | "default"}`` — see
            ``provenance_of()``'s docstring for exactly how each value is
            derived.
        source: ``"instance"`` (this call's own code-provided value won),
            ``"fallback"`` (this call's own env-constructed value won), or
            ``"external"`` (some other, non-fallback binding this module
            never saw wins resolution — e.g. registered directly via
            ``container.provide()``, or registered by a caller *after*
            this ``bind_settings()`` call, per the "not evicted" caveat).
    """

    cls: type
    qualifier: str | None
    env_prefix: str
    fields: dict[str, Literal["code", "env", "default"]]
    source: Literal["instance", "fallback", "external"]


#: Per-container bookkeeping. A ``WeakKeyDictionary`` keyed by the
#: container itself — no container attribute is ever monkey-patched, and a
#: garbage-collected container's bookkeeping is collected with it. Never
#: iterate this from application code; use ``provenance_of()``.
_REGISTRY: weakref.WeakKeyDictionary[DIContainer, dict[tuple[type, str | None], _BindRecord]] = (
    weakref.WeakKeyDictionary()
)


def _registry_for(container: DIContainer) -> dict[tuple[type, str | None], _BindRecord]:
    """Return (creating if needed) *container*'s bookkeeping dict."""
    return _REGISTRY.setdefault(container, {})


def _env_present(cls: type[BaseSettings], field_name: str, env_prefix: str) -> bool:
    """Whether the env var backing top-level *field_name* is currently set."""
    import os

    for spec in env_vars_for(cls, env_prefix=env_prefix):
        if spec.field_path == field_name:
            return spec.env is not None and spec.env in os.environ
    return False


def _fields_provenance(
    value: BaseSettings, *, kind: _Kind, env_prefix: str
) -> dict[str, Literal["code", "env", "default"]]:
    """Compute the per-field provenance dict for a resolved *value*."""
    cls = type(value)
    result: dict[str, Literal["code", "env", "default"]] = {}
    fields_set = value.model_fields_set if kind == "instance" else frozenset()
    for name in cls.model_fields:
        if name in fields_set:
            result[name] = "code"
        elif _env_present(cls, name, env_prefix):
            result[name] = "env"
        else:
            result[name] = "default"
    return result


def _validation_error_to_settings_error(
    cls: type[BaseSettings], error: ValidationError, env_prefix: str
) -> SettingsValidationError:
    """
    Convert a pydantic ``ValidationError`` into a ``SettingsValidationError``
    naming the offending env var(s) and the class, never the raw value.
    """
    specs_by_path = {spec.field_path: spec for spec in env_vars_for(cls, env_prefix=env_prefix)}
    lines = []
    for err in error.errors():
        loc = ".".join(str(part) for part in err["loc"])
        spec = specs_by_path.get(loc)
        env_name = (
            spec.env if spec is not None and spec.env is not None else f"{env_prefix}{loc.upper()}"
        )
        lines.append(f"{env_name}: {err['msg']}")
    return SettingsValidationError(cls, lines)


def bind_settings(
    container: DIContainer,
    cls: type[BaseSettings],
    *,
    instance: BaseSettings | None = None,
    qualifier: str | None = None,
    env_prefix: str | None = None,
) -> BaseSettings:
    """
    Register *cls* on *container* and return the constructed/resolved value.

    Exactly one of two paths runs:

    - **Code path** (``instance=`` given): registers *instance* as a
      non-fallback singleton binding. A second call for the same
      ``(cls, qualifier)`` raises ``SettingsAlreadyBoundError`` — this is a
      bug, not a precedence question (use ``di_overrides`` in tests).
    - **Env path** (``instance=`` omitted): constructs ``cls()`` (or
      ``cls(_env_prefix=env_prefix)`` when *env_prefix* is given) eagerly,
      right now, and registers it as a ``@Fallback`` singleton — so any
      non-fallback binding registered elsewhere (before or after this
      call, per the "not evicted" caveat) wins resolution instead.

    Either way, ``container.get(cls, qualifier=qualifier)`` is called
    before returning, so the return value is always what resolution
    actually yields — the winner, not necessarily what was just
    registered.

    Args:
        container: The container to bind into.
        cls: A ``pydantic_settings.BaseSettings`` subclass (typically a
            ``VarcoSettings``).
        instance: An already-constructed value to bind non-fallback. Still
            env-aware **per field** — see the precedence diagram in
            ``technical_docs/features/dependency-injection.md``'s Settings
            section: "code wins per field, not per object".
        qualifier: A DI-16 named-instance qualifier. Required whenever
            *env_prefix* is given (two unqualified bindings would tie).
        env_prefix: Override *cls*'s own configured ``env_prefix`` for this
            one binding — the DI-16 mechanism (e.g. a second Redis cluster
            under ``VARCO_REDIS_REPLICA_``).

    Returns:
        The settings instance ``container.get(cls, qualifier=qualifier)``
        resolves to right now.

    Raises:
        ValueError: If *env_prefix* is given without *qualifier*.
        TypeError: If *instance* is not an instance of *cls*.
        SettingsAlreadyBoundError: If *instance* is given and a previous
            ``bind_settings()`` call already registered a non-fallback
            binding for this ``(cls, qualifier)``.
        SettingsValidationError: If the env path fails pydantic validation
            — names the offending env var(s) and the class, never the raw
            value.

    Edge cases:
        - Called twice with no *instance* and no competing binding: the
          second call is a pure no-op resolve — exactly one binding ends
          up registered (idempotent).
        - A non-fallback binding for ``(cls, qualifier)`` already exists
          when the env path runs: the environment is never read (the
          eager construction only happens after this check finds nothing).
    """
    _warn_legacy_env_names(container)
    if env_prefix is not None and qualifier is None:
        raise ValueError("env_prefix= requires qualifier= (two unqualified bindings would tie)")
    if instance is not None and not isinstance(instance, cls):
        raise TypeError(
            f"instance= must be an instance of {cls.__name__}, got {type(instance).__name__}"
        )

    registry = _registry_for(container)
    key = (cls, qualifier)
    effective_prefix = (
        env_prefix if env_prefix is not None else cls.model_config.get("env_prefix", "")
    )

    if instance is not None:
        if key in registry:
            raise SettingsAlreadyBoundError(cls, qualifier)
        fn = Provider(qualifier=qualifier, singleton=True, returns=cls)(lambda: instance)
        fn.__qualname__ = f"varco.settings:{cls.__name__}[{qualifier}]"
        container.provide(fn, returns=cls)
        registry[key] = _BindRecord(kind="instance", value=instance, env_prefix=effective_prefix)
        return container.get(cls, qualifier=qualifier)

    # Env/fallback path. Skip re-registering if we already own this key —
    # a second call is a pure idempotent resolve (see the Edge cases note).
    if key not in registry:

        def _factory() -> BaseSettings:
            # Deliberately LAZY: providify only calls a provider factory
            # when it actually wins resolution. A binding that is
            # SHADOWED by a non-fallback competitor (registered before OR
            # after this call — the "not evicted" caveat only applies to
            # an already-cached winner) never has this closure invoked at
            # all, so the environment is never read for it. This is what
            # makes test_d's "a value that would raise ValidationError if
            # ever parsed" contract true without any try/except dance at
            # registration time.
            try:
                return cls(_env_prefix=env_prefix) if env_prefix is not None else cls()
            except ValidationError as exc:
                raise _validation_error_to_settings_error(cls, exc, effective_prefix) from exc

        fn = Fallback(Provider(qualifier=qualifier, singleton=True, returns=cls)(_factory))
        fn.__qualname__ = f"varco.settings:{cls.__name__}[{qualifier}]"
        container.provide(fn, returns=cls)
        # `value` is filled in below, once resolution tells us who actually
        # won — never by calling `_factory()` ourselves.
        registry[key] = _BindRecord(kind="fallback", value=None, env_prefix=effective_prefix)

    resolved: BaseSettings = container.get(cls, qualifier=qualifier)
    registry[key].value = resolved
    return resolved


def provenance_of(
    container: DIContainer, cls: type[BaseSettings], *, qualifier: str | None = None
) -> SettingsProvenance:
    """
    Report per-field provenance for a class this container's
    ``bind_settings()`` calls have touched.

    Args:
        container: The container ``bind_settings()`` was called on.
        cls: The settings class.
        qualifier: The qualifier it was bound under, or ``None``.

    Returns:
        A ``SettingsProvenance`` reflecting the CURRENT resolution winner
        — if a non-fallback binding was registered elsewhere after this
        module's own fallback, ``source`` reports ``"external"`` even
        though this module's own record still says ``"fallback"``
        internally (the "not evicted" caveat: the cached fallback
        singleton may still be what OTHER already-built objects hold, but
        a fresh ``get()`` call now returns the external winner).

    Raises:
        KeyError: If ``bind_settings()`` was never called for this
            ``(cls, qualifier)`` on this container.
    """
    registry = _registry_for(container)
    key = (cls, qualifier)
    if key not in registry:
        raise KeyError(
            f"{cls.__name__} (qualifier={qualifier!r}) was never bound via bind_settings()"
        )
    record = registry[key]

    if record.kind == "instance":
        source: Literal["instance", "fallback", "external"] = "instance"
    else:
        all_bindings = container.get_all_bindings(cls, qualifier=qualifier)
        source = "external" if any(not b.fallback for b in all_bindings) else "fallback"

    if source == "external":
        fields: dict[str, Literal["code", "env", "default"]] = {
            name: "code" for name in cls.model_fields
        }
    else:
        # record.value is set by the time provenance_of() can be called —
        # bind_settings() always resolves and stores it before returning
        # (both the instance= and fallback branches), and the "not in
        # registry" case already raised KeyError above.
        assert record.value is not None
        fields = _fields_provenance(record.value, kind=record.kind, env_prefix=record.env_prefix)

    return SettingsProvenance(
        cls=cls, qualifier=qualifier, env_prefix=record.env_prefix, fields=fields, source=source
    )


__all__ = ["SettingsProvenance", "bind_settings", "provenance_of"]
