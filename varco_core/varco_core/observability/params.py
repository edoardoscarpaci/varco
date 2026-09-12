"""
varco_core.observability.params
================================
Automatic parameter capture for ``@span`` (Plan 004, deliverable A).

Pure-Python helpers that turn a decorated function's call arguments into
sanitised ``param.<name>`` span attributes: ``ParamCaptureConfig`` (structural
config), ``CapturePlan`` / ``build_capture_plan`` (signature introspection +
extraction), ``sanitize_value`` (the value-rendering table), and the
process-wide capture kill switch (``capture_enabled`` / ``set_capture_enabled``)
plus its env-var bootstrap (``param_capture_from_env``).

Import direction is strictly one-way — this module imports only stdlib and
``varco_core.redaction`` (itself stdlib-only), never any other
``varco_core.observability`` module.  It is imported BY ``span.py`` /
``mixin.py`` / ``repository_mixin.py`` / ``helpers.py``, never the other way
around.

DESIGN: decoration time vs. call time
    ``@span`` decorators run at *import* time, before ``OtelConfiguration``
    (or any user code) has had a chance to call ``set_param_capture_defaults()``
    or ``set_capture_enabled()``.  So:

    - Signature introspection → ``CapturePlan`` happens on the **first call**
      of the decorated function and is memoised on the wrapper closure.
      ``inspect.signature()`` costs ~10 µs — too expensive to repeat per call,
      but calling it at decoration time would freeze a config that is not
      loaded yet.
    - The ``enabled`` kill switch is read on **every call** (a module-level
      bool read, ~30 ns) so it always reflects the current runtime state.
    - Structural config (prefix, limits, redaction patterns) is snapshotted
      into the plan on first call.  Changing it later requires
      ``reset_param_capture_state()`` (a test helper) — documented, not a bug.

    ✅ Near-zero overhead — no ``Signature.bind()`` on the hot path.
    ✅ Works correctly even though decorators run before bootstrap.
    ❌ A ``set_param_capture_defaults()`` call issued *after* a decorated
       function has already been called once has no effect on that function's
       already-memoised plan (only on functions not yet called).

DESIGN: ``"scalars"`` value_mode default instead of ``repr()`` everywhere
    ✅ A DTO carrying an email/IBAN/address never leaks into the trace backend
       by accident — opaque objects render as ``"<TypeName>"``.
    ✅ Native scalar types keep backend numeric filters (``param.limit > 100``)
       working.
    ✅ Bounded cost — never ``repr()`` a multi-MB payload on the hot path.
    ❌ ``param.dto=<OrderCreateDTO>`` is not very informative; opt into
       ``value_mode="repr"`` (or set explicit attributes in the function body)
       for full payload visibility.

DESIGN: capture defaults to ON
    ✅ Exactly what was requested — traces are useful for debugging with zero
       opt-in effort, safe-by-construction (scalars-only + name redaction +
       truncation).
    ❌ A parameter named ``email`` holding a string IS captured — the redact
       pattern list cannot know every PII field name.  Mitigation:
       ``VARCO_OTEL_CAPTURE_PARAMS=false`` kill switch,
       ``VARCO_OTEL_CAPTURE_PARAMS_EXCLUDE``, and per-decorator
       ``SpanConfig(capture_params=False)``.
    Reviewer escape hatch: flipping ``_DEFAULT_ENABLED`` below to ``False`` is
    a one-line change if the team prefers opt-in.

Thread safety:  ✅ Module-level state (``_enabled`` / ``_defaults``) consists
                   of single-reference assignments — atomic under CPython's
                   GIL, same treatment as ``metrics._instrument_cache``.
                   Two threads racing to build the same function's
                   ``CapturePlan`` on first call both succeed; last write wins
                   and both plans are structurally equivalent.
Async safety:   ✅ All helpers here are synchronous and stateless per call
                   (aside from the memoised plan closure built by the caller).
"""

from __future__ import annotations

import inspect
import logging
import os
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Any, Literal
from uuid import UUID

from varco_core.redaction.patterns import DEFAULT_REDACT_PATTERNS
from varco_core.redaction.policy import RedactionPolicy
from varco_core.redaction.policy import is_sensitive_key as _redaction_is_sensitive_key

_logger = logging.getLogger(__name__)

# ── Redaction ────────────────────────────────────────────────────────────────
#
# Plan 040 / S21, §D-S21-compat: DEFAULT_REDACT_PATTERNS now lives in
# varco_core.redaction.patterns — this is a re-export of the SAME object
# (identity, not equality; asserted by
# varco_core/tests/test_redaction_extraction.py), not a duplicate copy or a
# deprecated alias. The two names denote the same tuple with the same
# semantics — no DeprecationWarning, no removal planned (§D-S21-compat).

_REDACTION_PLACEHOLDER_DEFAULT = "[REDACTED]"


# ── ParamCaptureConfig ───────────────────────────────────────────────────────


@dataclass(frozen=True)
class ParamCaptureConfig:
    """
    Immutable structural configuration for automatic parameter capture.

    Args:
        enabled:
            ``None`` (default) means "inherit the process default" — see
            ``capture_enabled()`` / ``VARCO_OTEL_CAPTURE_PARAMS`` /
            ``OtelConfig.capture_params``.  An explicit ``True``/``False``
            here always wins over the process default for the decorator this
            config is attached to.
        prefix:
            Span attribute key prefix.  ``"a"`` becomes ``"param.a"``.
        include:
            Allow-list of parameter names.  Empty tuple (default) means
            "capture everything eligible" — no allow-list filtering.
        exclude:
            Deny-list, applied *after* ``include``.
        value_mode:
            ``"scalars"`` (default) — native scalars, PII-safe type-name
            summaries for opaque objects.  ``"repr"`` — full ``repr()``
            output (truncated), which is far more informative but risks
            leaking PII/large payloads.
        max_value_length:
            Truncation length for any string-rendered value.
        max_params:
            Maximum number of ``param.*`` attributes captured per call.
            ``0`` disables capture entirely for the decorated callable (no
            attributes, no ``param._truncated`` marker — see edge cases).
        max_sequence_items:
            Sequences (list/tuple/set) longer than this are summarised as a
            string instead of rendered natively.
        capture_varargs:
            When ``True``, extra ``*args`` positions and ``**kwargs`` entries
            not matching a named parameter are also captured.
        capture_self:
            When ``True``, the first parameter is captured even if it is
            named ``self``/``cls``.  Detected by *name*, not descriptor type
            — the decorator only ever sees the plain function at
            class-definition time.
        redact_patterns:
            Case-insensitive substrings matched against parameter names.
            A match replaces the value with ``redaction_placeholder`` —
            applied even to names explicitly listed in ``include``
            (fail-closed).
        redaction_placeholder:
            The literal value written in place of a redacted parameter.

    Edge cases:
        - ``max_params=0`` → no ``param.*`` attributes at all, and crucially
          *no* ``param._truncated`` marker (there is nothing to truncate from
          — capture is simply off).
        - ``include`` and ``exclude`` both list the same name → ``exclude``
          wins (applied after ``include``).

    Thread safety:  ✅ Frozen dataclass — immutable, safe to share.
    """

    enabled: bool | None = None
    prefix: str = "param."
    include: tuple[str, ...] = ()
    exclude: tuple[str, ...] = ()
    value_mode: Literal["scalars", "repr"] = "scalars"
    max_value_length: int = 256
    max_params: int = 32
    max_sequence_items: int = 10
    capture_varargs: bool = False
    capture_self: bool = False
    redact_patterns: tuple[str, ...] = DEFAULT_REDACT_PATTERNS
    redaction_placeholder: str = _REDACTION_PLACEHOLDER_DEFAULT


# ── sanitize_value ───────────────────────────────────────────────────────────

_STRINGABLE_TYPES = (UUID, Decimal, datetime, Path)


def _truncate_str(value: str, max_value_length: int) -> str:
    """Slice-only truncation — never build a full copy before slicing."""
    if len(value) <= max_value_length:
        return value
    return value[:max_value_length] + "…"


def _is_homogeneous_scalar_sequence(items: list[Any]) -> bool:
    """True when every item is the same scalar type (str/int/float/bool)."""
    if not items:
        return True
    first_type = type(items[0])
    if first_type not in (str, int, float, bool):
        return False
    return all(type(item) is first_type for item in items)


def sanitize_value(
    value: Any,
    *,
    max_value_length: int = 256,
    max_sequence_items: int = 10,
    value_mode: Literal["scalars", "repr"] = "scalars",
) -> Any:
    """
    Render an arbitrary Python value as an OTel-attribute-safe value.

    Follows the Plan 004 rendering table:

    - ``str`` → truncated to ``max_value_length`` with a ``"…"`` suffix.
    - ``int``/``float``/``bool`` → returned as the native scalar (OTel
      supports these types directly — no ``str()`` needed, keeps numeric
      filters usable in the backend).
    - ``None`` → the string ``"None"`` (OTel forbids ``None`` attribute
      values).
    - ``UUID``/``Decimal``/``datetime``/``Enum``/``Path`` → ``str(value)``,
      truncated.
    - ``list``/``tuple``/``set`` of a single homogeneous scalar type, with
      ``len <= max_sequence_items`` → returned as a native OTel-compatible
      tuple.  Longer or heterogeneous sequences are summarised as a string
      (``"<list len=1000>"`` in ``"scalars"`` mode, ``repr()`` truncated in
      ``"repr"`` mode).
    - Anything else (``dict``, dataclass, Pydantic model, arbitrary object)
      → ``"<TypeName>"`` in ``"scalars"`` mode (after validating the object
      is representable via ``repr()`` — see below), or ``repr()`` truncated
      in ``"repr"`` mode.

    Args:
        value: The raw argument value to render.
        max_value_length: Truncation length for string-rendered values.
        max_sequence_items: Sequences longer than this are summarised.
        value_mode: ``"scalars"`` (default, PII-safe) or ``"repr"`` (verbose).

    Returns:
        An OTel-attribute-safe value (``str``, ``bool``, ``int``, ``float``,
        or a homogeneous tuple thereof).  Never raises — an object whose
        ``__repr__``/``__str__`` raises yields the literal string
        ``"<unrepresentable>"``.

    Edge cases:
        - A 10 MB string is truncated via a slice — never fully copied or
          ``repr()``-ed first.
        - An object with a broken ``__repr__``/``__str__`` never propagates
          its exception — this function is the safety boundary for
          "instrumentation must never break the application".

    Thread safety:  ✅ Pure function, no shared state.
    """
    try:
        return _sanitize_value_inner(value, max_value_length, max_sequence_items, value_mode)
    except Exception:
        return "<unrepresentable>"


def _sanitize_value_inner(
    value: Any,
    max_value_length: int,
    max_sequence_items: int,
    value_mode: Literal["scalars", "repr"],
) -> Any:
    if value is None:
        return "None"
    if isinstance(value, bool):
        # bool is a subclass of int — must be checked first.
        return value
    if isinstance(value, (int, float)):
        return value
    if isinstance(value, str):
        return _truncate_str(value, max_value_length)
    if isinstance(value, _STRINGABLE_TYPES) or isinstance(value, Enum):
        return _truncate_str(str(value), max_value_length)
    if isinstance(value, (list, tuple, set)):
        items = list(value)
        n = len(items)
        if n <= max_sequence_items and _is_homogeneous_scalar_sequence(items):
            return tuple(items)
        if value_mode == "repr":
            return _truncate_str(repr(value), max_value_length)
        return f"<{type(value).__name__} len={n}>"
    # dict / dataclass / Pydantic model / any other opaque object.
    if value_mode == "repr":
        return _truncate_str(repr(value), max_value_length)
    # scalars mode: validate representability first (an object whose
    # __repr__ raises must yield "<unrepresentable>", not a fabricated
    # type-name placeholder for a genuinely broken object), then discard the
    # preview in favour of the PII-safe type-name summary.
    repr(value)
    return f"<{type(value).__name__}>"


# ── Redaction / eligibility ──────────────────────────────────────────────────


def _is_redacted(name: str, config: ParamCaptureConfig) -> bool:
    # Plan 040 / S21, §D-S21-compat: the decision now delegates to
    # varco_core.redaction.is_sensitive_key — byte-identical
    # case-insensitive substring semantics (RedactionPolicy's default
    # match_mode), against whatever patterns THIS config carries (which may
    # differ from DEFAULT_REDACT_PATTERNS if the caller overrode
    # redact_patterns=). Rendering (sanitize_value) is untouched — only the
    # decision half of the incumbent split moves.
    return _redaction_is_sensitive_key(name, RedactionPolicy(patterns=config.redact_patterns))


def _is_eligible(name: str, config: ParamCaptureConfig) -> bool:
    if config.include and name not in config.include:
        return False
    if name in config.exclude:
        return False
    return True


def _render_captured(collected: dict[str, Any], config: ParamCaptureConfig) -> dict[str, Any]:
    """
    Apply ``max_params`` truncation, redaction, and value sanitisation to an
    already-collected ``{raw_name: raw_value}`` dict, in declaration order.

    Shared by ``CapturePlan.extract`` and ``render_captured_params`` so the
    truncation/redaction/sanitisation logic lives in exactly one place.

    Edge cases:
        - When truncating, the ``param._truncated`` marker itself counts
          toward ``max_params`` — the real params kept are capped at
          ``max_params - 1`` so the total attribute count (real + marker)
          never exceeds ``max_params``.
    """
    truncated = False
    if len(collected) > config.max_params:
        truncated = True
        keep_n = max(config.max_params - 1, 0)
        collected = dict(list(collected.items())[:keep_n])

    result: dict[str, Any] = {}
    for name, value in collected.items():
        key = f"{config.prefix}{name}"
        if _is_redacted(name, config):
            result[key] = config.redaction_placeholder
        else:
            result[key] = sanitize_value(
                value,
                max_value_length=config.max_value_length,
                max_sequence_items=config.max_sequence_items,
                value_mode=config.value_mode,
            )
    if truncated:
        result[f"{config.prefix}_truncated"] = True
    return result


def render_captured_params(
    raw: Mapping[str, Any], config: ParamCaptureConfig | None = None
) -> dict[str, Any]:
    """
    Render an already-known ``{name: value}`` mapping (e.g. supplied by a
    caller of ``create_span(..., params=...)``) through the same
    redact/sanitise/truncate pipeline as decorator-based capture.

    Unlike ``CapturePlan.extract``, this does not do signature introspection
    — it trusts the caller's mapping as the full set of eligible names
    (``include``/``exclude``/``capture_varargs`` do not apply; redaction and
    ``max_params`` truncation still do).

    Args:
        raw: Mapping of parameter name → raw value.
        config: Structural config to apply.  Defaults to
            ``param_capture_defaults()``.

    Returns:
        A dict of ``param.<name>`` → sanitised value, never raises.
    """
    cfg = config if config is not None else param_capture_defaults()
    if cfg.max_params == 0:
        return {}
    try:
        return _render_captured(dict(raw), cfg)
    except Exception:
        _logger.debug(
            "varco.observability.params: render_captured_params failed; returning empty capture",
            exc_info=True,
        )
        return {}


# ── CapturePlan / build_capture_plan ────────────────────────────────────────


@dataclass(frozen=True)
class CapturePlan:
    """
    Precomputed, memoisable extraction plan for one callable's signature.

    Built once (on first call of the decorated function) via
    ``build_capture_plan`` and cached on the wrapper closure.  ``extract()``
    never calls ``Signature.bind()`` — it zips ``args`` against a
    precomputed tuple of positional parameter names and does a filtered pass
    over ``kwargs``.

    Args:
        config: The ``ParamCaptureConfig`` this plan was built from.
        positional_names: Index-aligned with the callable's positional
            parameters (excluding ``*args``/``**kwargs``).  ``None`` at an
            index means "skip this slot" (e.g. ``self``/``cls`` when
            ``capture_self=False``).
        declared_names: The set of named (non-skipped) parameter names —
            used to distinguish "declared parameter passed by keyword" from
            "arbitrary **kwargs entry".
        has_var_positional: Whether the signature declares ``*args``.
        has_var_keyword: Whether the signature declares ``**kwargs``.
        valid: ``False`` when signature introspection failed (lambda,
            C-implemented callable, etc.) — ``extract()`` always returns
            ``{}`` for an invalid plan.

    Thread safety:  ✅ Frozen dataclass — immutable once built; safe to read
                       from multiple threads without synchronisation.
    """

    config: ParamCaptureConfig
    positional_names: tuple[str | None, ...]
    declared_names: frozenset[str]
    has_var_positional: bool
    has_var_keyword: bool
    valid: bool = True

    def extract(self, args: tuple[Any, ...], kwargs: dict[str, Any]) -> dict[str, Any]:
        """
        Extract and render ``param.<name>`` attributes for one call.

        Args:
            args: Positional arguments as received by the wrapper.
            kwargs: Keyword arguments as received by the wrapper.

        Returns:
            A dict of ``param.<name>`` → sanitised value.  Never raises —
            any unexpected failure yields ``{}`` (logged once at DEBUG).

        Edge cases:
            - Wrong-arity calls (more positional args than the plan knows
              about) never raise here — extra args are simply ignored;
              the callee will raise its own ``TypeError`` afterward.
            - An invalid plan (failed signature introspection) always
              returns ``{}``.
        """
        config = self.config
        if not self.valid or config.max_params == 0:
            return {}
        try:
            collected: dict[str, Any] = {}

            for name, value in zip(self.positional_names, args):
                if name is None:
                    continue
                if not _is_eligible(name, config):
                    continue
                collected[name] = value

            if (
                config.capture_varargs
                and self.has_var_positional
                and len(args) > len(self.positional_names)
            ):
                for i, value in enumerate(args[len(self.positional_names) :]):
                    collected[f"args.{i}"] = value

            for key, value in kwargs.items():
                if key in self.declared_names:
                    if not _is_eligible(key, config):
                        continue
                    collected[key] = value
                elif config.capture_varargs and self.has_var_keyword:
                    collected[key] = value

            return _render_captured(collected, config)
        except Exception:
            _logger.debug(
                "varco.observability.params: CapturePlan.extract failed; returning empty capture",
                exc_info=True,
            )
            return {}


_EMPTY_PLAN_SENTINEL = CapturePlan(
    config=ParamCaptureConfig(),
    positional_names=(),
    declared_names=frozenset(),
    has_var_positional=False,
    has_var_keyword=False,
    valid=False,
)


def build_capture_plan(func: Callable[..., Any], config: ParamCaptureConfig) -> CapturePlan:
    """
    Build a ``CapturePlan`` for ``func`` from ``inspect.signature(func)``.

    Args:
        func: The callable to introspect.  For methods, pass the plain
            (unbound) function as seen at class-definition time.
        config: Structural ``ParamCaptureConfig`` to bake into the plan.

    Returns:
        A ``CapturePlan``.  If signature introspection fails (lambdas,
        C-implemented builtins, some ``functools.partial`` wrappings raise
        ``ValueError``/``TypeError``, or other unexpected errors), an
        invalid, always-empty plan is returned instead of raising — the
        span is still created, just without captured parameters.

    Edge cases:
        - Bound method vs. plain function vs. ``@staticmethod``/
          ``@classmethod`` — ``self``/``cls`` is detected by the *name* of
          the first parameter (not descriptor type), because the decorator
          only ever sees the plain function at class-definition time.
          Override via ``config.capture_self=True``.

    Thread safety:  ✅ Stateless — safe to call concurrently; two threads
                       racing on the same function both build equivalent
                       plans (last write to the caller's memo cell wins).
    """
    try:
        sig = inspect.signature(func)
    except Exception:
        _logger.debug(
            "varco.observability.params: signature introspection failed for %r; "
            "parameter capture disabled for this callable",
            func,
            exc_info=True,
        )
        return CapturePlan(
            config=config,
            positional_names=(),
            declared_names=frozenset(),
            has_var_positional=False,
            has_var_keyword=False,
            valid=False,
        )

    positional_names: list[str | None] = []
    declared_names: set[str] = set()
    has_var_positional = False
    has_var_keyword = False

    for i, p in enumerate(sig.parameters.values()):
        if p.kind is inspect.Parameter.VAR_POSITIONAL:
            has_var_positional = True
            continue
        if p.kind is inspect.Parameter.VAR_KEYWORD:
            has_var_keyword = True
            continue

        name = p.name
        if i == 0 and name in ("self", "cls") and not config.capture_self:
            positional_names.append(None)
            continue

        positional_names.append(name)
        declared_names.add(name)

    return CapturePlan(
        config=config,
        positional_names=tuple(positional_names),
        declared_names=frozenset(declared_names),
        has_var_positional=has_var_positional,
        has_var_keyword=has_var_keyword,
        valid=True,
    )


# ── Process-wide capture kill switch ────────────────────────────────────────

# DESIGN: module-level bool/reference instead of threading.Lock
#   ✅ Read on every @span call — must be as cheap as possible (~30 ns).
#   ✅ Single-reference assignment is atomic under CPython's GIL, matching
#      the existing `metrics._instrument_cache` treatment.
#   ❌ Not safe under free-threaded Python (3.13t) without a lock — same
#      caveat already documented on `_instrument_cache`.
_DEFAULT_ENABLED = True  # flip to False here for an opt-in-by-default team.
_enabled: bool = _DEFAULT_ENABLED
_defaults: ParamCaptureConfig = ParamCaptureConfig()


def capture_enabled() -> bool:
    """Return the current process-wide parameter-capture kill switch state."""
    return _enabled


def set_capture_enabled(enabled: bool) -> None:
    """Set the process-wide parameter-capture kill switch."""
    global _enabled
    _enabled = enabled


def param_capture_defaults() -> ParamCaptureConfig:
    """Return the current process-wide default ``ParamCaptureConfig``."""
    return _defaults


def set_param_capture_defaults(config: ParamCaptureConfig) -> None:
    """
    Set the process-wide default ``ParamCaptureConfig``.

    Only affects decorated callables whose ``CapturePlan`` has not yet been
    built (i.e. not yet called once) — already-memoised plans keep their
    snapshotted structural config.  Use ``reset_param_capture_state()`` in
    tests to fully reset.
    """
    global _defaults
    _defaults = config


def reset_param_capture_state() -> None:
    """
    Test helper: restore ``capture_enabled()`` and ``param_capture_defaults()``
    to their process defaults.

    Global mutable state hygiene (Plan 004 "Risks" section) — call this in
    an autouse fixture teardown in any test module that touches the capture
    kill switch or defaults.
    """
    global _enabled, _defaults
    _enabled = _DEFAULT_ENABLED
    _defaults = ParamCaptureConfig()


# ── Env-var bootstrap ────────────────────────────────────────────────────────

_TRUE_TOKENS = {"true", "1", "yes", "on"}
_FALSE_TOKENS = {"false", "0", "no", "off"}


def _parse_bool_env(raw: str, default: bool, var_name: str) -> bool:
    token = raw.strip().lower()
    if token in _TRUE_TOKENS:
        return True
    if token in _FALSE_TOKENS:
        return False
    _logger.warning(
        "varco.observability.params: invalid boolean value %r for %s; falling back to default %s",
        raw,
        var_name,
        default,
    )
    return default


def param_capture_from_env() -> ParamCaptureConfig:
    """
    Build a ``ParamCaptureConfig`` from ``VARCO_OTEL_CAPTURE_PARAMS*`` env vars.

    Reads:
        - ``VARCO_OTEL_CAPTURE_PARAMS`` (default ``"true"``) — boolean.
        - ``VARCO_OTEL_CAPTURE_PARAMS_EXCLUDE`` — comma-separated parameter
          names added to ``exclude``.

    Returns:
        A ``ParamCaptureConfig`` with ``enabled`` always resolved to a
        concrete ``True``/``False`` (never ``None``).

    Edge cases:
        - An invalid ``VARCO_OTEL_CAPTURE_PARAMS`` value (not a recognised
          boolean token) logs a ``WARNING`` and falls back to the default
          (``True``) — never crashes the process on a malformed env var.

    Thread safety:  ✅ Pure function; reads ``os.environ`` at call time.
    """
    raw_enabled = os.environ.get("VARCO_OTEL_CAPTURE_PARAMS")
    enabled = (
        True
        if raw_enabled is None
        else _parse_bool_env(raw_enabled, True, "VARCO_OTEL_CAPTURE_PARAMS")
    )

    raw_exclude = os.environ.get("VARCO_OTEL_CAPTURE_PARAMS_EXCLUDE", "")
    exclude = tuple(x.strip() for x in raw_exclude.split(",") if x.strip())

    return ParamCaptureConfig(enabled=enabled, exclude=exclude)


__all__ = [
    "DEFAULT_REDACT_PATTERNS",
    "ParamCaptureConfig",
    "CapturePlan",
    "build_capture_plan",
    "sanitize_value",
    "render_captured_params",
    "capture_enabled",
    "set_capture_enabled",
    "param_capture_defaults",
    "set_param_capture_defaults",
    "param_capture_from_env",
    "reset_param_capture_state",
]
