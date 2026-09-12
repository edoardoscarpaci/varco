"""
varco_core.redaction.redactor
===============================
``Redactor`` (the seam's one-method Protocol), ``PolicyRedactor`` (the
default implementation), ``redact_mapping`` (the nested-walk traversal),
``redact_query_string`` and ``json_safe`` (value-shape helpers), and the
process-wide default-redactor cell (Plan 040 / S21, §D-S21-shape,
§D-S21-failsafe, §D-S21-nesting, §D-S21-di).

DESIGN: one-method Protocol; traversal is a free function (§D-S21-shape)
    ✅ Four consumers, three of which need only the leaf decision (span
       capture keeps its own rendering; ``error_params()``/a log entry are
       flat mappings). Only the audit diff needs a nested walk. Putting the
       walk on the Protocol would force every out-of-tree implementer to
       write traversal, depth limiting and cycle detection to satisfy a seam
       they wanted one predicate from.
    ✅ ``runtime_checkable`` — ``isinstance(x, Redactor)`` works for the
       posture inspector and defensive boundary checks.
    ❌ A caller can pass a ``Redactor`` and a disagreeing ``policy=`` —
       resolved below: ``redactor=`` wins, ``policy=`` is a shorthand for
       ``PolicyRedactor(policy)``; both together is a ``ValueError``.

DESIGN: fail-safe polarity — a redactor that raises redacts (§D-S21-failsafe)
    Data can be dropped safely; it cannot be un-emitted. The incumbent
    already chose this polarity twice (``sanitize_value`` ->
    ``"<unrepresentable>"``, ``render_captured_params`` -> ``{}`` on
    failure) — this module keeps the polarity and makes it stricter where
    the surface is more sensitive: **any** failure anywhere in the walk —
    one leaf's ``redact()`` raising is enough — degrades the **whole**
    result to ``{k: placeholder for k in data}``: every top-level key
    preserved, every value redacted, **never** a partially-successful
    pass-through and **never** the original input. A redactor that is
    broken for one key is not trusted for its neighbours either.

DESIGN: module-level process default, copied in shape from
``varco_core.observability.params.param_capture_defaults()`` (§D-S21-di)
    ✅ ``error_message_for()`` is a pure function with no container — this
       gives it a reachable, swappable default with no DI verb.
    ✅ Same GIL-atomic single-reference-assignment thread-safety note as the
       precedent it copies.
    ❌ Process-global mutable state — wrong if two apps in one process want
       different policies. Accepted: identical to the incumbent, and every
       per-surface consumer accepts an explicit redactor that wins over the
       default.

Thread safety:  ✅ ``PolicyRedactor``/``RedactionPolicy`` are frozen. The
                   default-redactor cell is a single-reference assignment,
                   atomic under CPython's GIL (same treatment as
                   ``params._defaults``).
Async safety:   ✅ Every function here is synchronous and does no I/O.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable
from urllib.parse import parse_qsl, urlencode

from varco_core.redaction.policy import RedactionPolicy, is_sensitive_key

__all__ = [
    "Redactor",
    "PolicyRedactor",
    "redact_mapping",
    "redact_query_string",
    "json_safe",
    "default_redactor",
    "set_default_redactor",
    "reset_redaction_state",
]

_logger = logging.getLogger(__name__)

REDACTION_PLACEHOLDER = "[REDACTED]"

# Traversal guard markers (§D-S21-nesting) — always a visible string, never
# a silent drop.
_CYCLE_MARKER = "<cycle>"
_MAX_DEPTH_MARKER = "<max-depth>"
_TRUNCATED_MARKER = "<truncated>"


@runtime_checkable
class Redactor(Protocol):
    """
    The whole seam: one method.

    Implement this to plug a custom redaction policy (Vault-backed,
    classifier-backed, whatever) into span capture, ``error_params()``, the
    audit trail, or request logging — no traversal, no depth/cycle
    handling required; ``redact_mapping()`` (below) supplies that.
    """

    def redact(self, key: str, value: Any) -> Any:
        """
        Return ``value`` unchanged, or a placeholder if ``key`` names
        something sensitive.

        Args:
            key: The field/parameter name being considered.
            value: The raw value at that key.

        Returns:
            ``value`` (pass-through) or a redaction placeholder. A redactor
            that **raises** is treated by every caller as "redact this leaf"
            (§D-S21-failsafe) — never propagate an exception from here if
            avoidable, but callers do not trust you if you do.
        """
        ...


@dataclass(frozen=True)
class PolicyRedactor:
    """
    The default ``Redactor`` — delegates the leaf decision to
    ``is_sensitive_key()`` against a ``RedactionPolicy``.

    Args:
        policy: The ``RedactionPolicy`` to evaluate against. Defaults to
            ``RedactionPolicy()`` — byte-identical to span capture's
            incumbent behaviour.

    Thread safety:  ✅ Frozen — immutable, hashable, safe to share.
    """

    policy: RedactionPolicy = RedactionPolicy()

    def redact(self, key: str, value: Any) -> Any:
        """
        Args:
            key: The field/parameter name.
            value: The raw value.

        Returns:
            ``REDACTION_PLACEHOLDER`` when ``is_sensitive_key(key, self.policy)``
            is ``True``; ``value`` unchanged otherwise. Never raises.
        """
        if is_sensitive_key(key, self.policy):
            return REDACTION_PLACEHOLDER
        return value


def json_safe(value: Any) -> Any:
    """
    Render ``value`` as JSON-shape-safe, **without truncation**.

    Deliberately not ``varco_core.observability.params.sanitize_value`` —
    that function's 256-char truncation ceiling would silently clip e.g.
    ``ServiceConflictError.detail``. This function preserves JSON-native
    scalars and containers exactly, and renders anything else (a live
    object, a ``vars(exc)`` dump target) as ``"<TypeName>"``.

    Args:
        value: Any Python value.

    Returns:
        ``value`` unchanged if it is ``None``/``bool``/``int``/``float``/
        ``str``/``list``/``dict`` (JSON-native shapes); otherwise
        ``f"<{type(value).__name__}>"``. Never raises.

    Edge cases:
        - A ``tuple``/``set`` is **not** JSON-native and renders as its
          type name — only ``list`` (JSON's array shape) passes through.
        - Nesting is not walked here — callers needing a nested walk with
          redaction use ``redact_mapping()``.

    Thread safety:  ✅ Pure function, no shared state.
    """
    if value is None or isinstance(value, (bool, int, float, str, list, dict)):
        return value
    return f"<{type(value).__name__}>"


def _resolve_redactor(redactor: Redactor | None, policy: RedactionPolicy | None) -> Redactor:
    if redactor is not None and policy is not None:
        raise ValueError(
            "redact_mapping() accepts either redactor= or policy=, not both "
            "— policy= is a shorthand for PolicyRedactor(policy)."
        )
    if redactor is not None:
        return redactor
    if policy is not None:
        return PolicyRedactor(policy)
    return default_redactor()


def _walk(
    data: Mapping[Any, Any],
    redactor: Redactor,
    *,
    max_depth: int,
    max_items: int,
    render_non_json: bool,
    depth: int,
    seen: set[int],
) -> dict[Any, Any]:
    # DESIGN: the depth check happens BEFORE recursing into a *child*
    # mapping, not at this call's own entry — so `max_depth` counts the
    # number of "hops" a caller walking the *output* takes to reach the
    # `"<max-depth>"` marker, matching the guard's documented meaning
    # ("a subtree deeper than max_depth is replaced"). Checking at entry
    # instead would let one extra hop through before the marker appears.
    # Cycle detection lives at the *caller* (before recursing into a child
    # mapping, using `child_id in seen`) — by the time `_walk` is entered
    # for a given mapping, that mapping's id has already been cleared as
    # unvisited. `seen` is threaded through, scoped to this call only.
    seen = seen | {id(data)}

    items = list(data.items())
    truncated = len(items) > max_items
    if truncated:
        items = items[:max_items]

    result: dict[Any, Any] = {}
    for key, value in items:
        str_key = str(key)
        if isinstance(value, Mapping):
            child_id = id(value)
            new_depth = depth + 1
            if child_id in seen:
                result[key] = _CYCLE_MARKER
            elif new_depth >= max_depth:
                result[key] = _MAX_DEPTH_MARKER
            else:
                result[key] = _walk(
                    value,
                    redactor,
                    max_depth=max_depth,
                    max_items=max_items,
                    render_non_json=render_non_json,
                    depth=new_depth,
                    seen=seen,
                )
            continue
        # NOT caught here, deliberately: a leaf redactor that raises is not
        # trusted with a partial result either — the exception propagates
        # to redact_mapping()'s outer handler, which redacts every
        # top-level value (§D-S21-failsafe: a broken redactor degrades to
        # maximum redaction, never a partially-successful pass-through that
        # could mask which leaves it actually inspected).
        rendered = redactor.redact(str_key, value)
        if render_non_json and rendered is value:
            rendered = json_safe(rendered)
        result[key] = rendered
    if truncated:
        result[_TRUNCATED_MARKER] = True
    return result


def redact_mapping(
    data: Mapping[str, Any],
    redactor: Redactor | None = None,
    *,
    policy: RedactionPolicy | None = None,
) -> dict[str, Any]:
    """
    Redact a (possibly nested) mapping.

    Only the audit diff needs this nested walk today — ``error_params()``
    and a log entry are flat mappings, and span capture keeps its own
    rendering. Guarded against unbounded depth, unbounded item counts, and
    cycles (§D-S21-nesting) because a hand-built ``_audit_diff`` override
    can produce any of the three, unlike a ``model_dump()``.

    Args:
        data: The mapping to redact. Top-level keys are always preserved in
            the output, even on total failure.
        redactor: The ``Redactor`` to consult per leaf. Defaults to
            ``default_redactor()`` when both this and ``policy`` are
            omitted.
        policy: Shorthand for ``PolicyRedactor(policy)``. Mutually exclusive
            with ``redactor``.

    Returns:
        A new ``dict`` with sensitive leaves replaced. Nested mappings
        deeper than ``policy.max_depth`` become ``"<max-depth>"``; a
        container with more than ``policy.max_items`` entries is truncated
        with a ``"<truncated>"`` marker entry; a revisited container (a
        cycle) becomes ``"<cycle>"``. **Never raises** — a failure anywhere
        in the walk, including one leaf's ``redact()`` raising, degrades
        the **whole** result to
        ``{k: REDACTION_PLACEHOLDER for k in data}`` (§D-S21-failsafe):
        maximum redaction, never a partial or pass-through result.

    Raises:
        ValueError: Both ``redactor`` and ``policy`` were given.

    Edge cases:
        - ``redact_mapping({})`` returns ``{}`` without consulting the
          redactor at all.
        - A non-``str`` key (e.g. ``1``) is stringified for the predicate
          but preserved as-is in the output.
    """
    resolved = _resolve_redactor(redactor, policy)
    if not data:
        return {}

    the_policy = resolved.policy if isinstance(resolved, PolicyRedactor) else RedactionPolicy()
    try:
        result = _walk(
            data,
            resolved,
            max_depth=the_policy.max_depth,
            max_items=the_policy.max_items,
            render_non_json=the_policy.render_non_json,
            depth=0,
            seen=set(),
        )
        if not isinstance(result, dict):  # pragma: no cover - defensive
            raise TypeError("redact_mapping walk did not return a dict")
        return result
    except Exception:
        _logger.error(
            "varco.redaction: redact_mapping() failed; redacting every top-level value (%d keys)",
            len(data),
            exc_info=True,
        )
        return {key: REDACTION_PLACEHOLDER for key in data}


def redact_query_string(query: str, redactor: Redactor | None = None) -> str:
    """
    Redact matching keys in a URL query string, preserving order and
    repeated-key counts.

    For a subclass of ``RequestLoggingMiddleware`` (or any caller) that logs
    a full URL rather than just ``request.url.path`` — 034's "a credential
    in a URL is already in the access log by the time anything could warn
    about it" concern, applied at the key-name level (§D-S21-logging).

    Args:
        query: A raw query string (no leading ``"?"`` required either way).
        redactor: The ``Redactor`` to consult per key. Defaults to
            ``default_redactor()``.

    Returns:
        The query string re-encoded with matching values replaced by
        ``REDACTION_PLACEHOLDER`` (URL-encoded in the output, like any other
        value). **Never raises** — a malformed query string is returned
        unchanged, logged at ``DEBUG`` (§D-S21-failsafe: a logging helper
        must never raise).

    Edge cases:
        - A repeated key (``?token=a&token=b``) has every occurrence
          redacted independently; order and count are preserved.
    """
    resolved = redactor if redactor is not None else default_redactor()
    try:
        pairs = parse_qsl(query, keep_blank_values=True, strict_parsing=False)
        redacted_pairs = [(key, resolved.redact(key, value)) for key, value in pairs]
        return urlencode(redacted_pairs)
    except Exception:
        _logger.debug(
            "varco.redaction: redact_query_string() failed to parse %r; returning unchanged",
            query,
            exc_info=True,
        )
        return query


# ── Process-wide default redactor (§D-S21-di) ───────────────────────────────

# DESIGN: module-level single-reference assignment, copied in shape from
# varco_core.observability.params.param_capture_defaults() (params.py:598).
# ✅ Atomic under CPython's GIL — same treatment as `params._defaults`.
# ❌ Not safe under free-threaded Python (3.13t) without a lock — same
#    caveat already documented on `_instrument_cache`/`_defaults`.
_default_redactor: Redactor = PolicyRedactor()


def default_redactor() -> Redactor:
    """Return the current process-wide default ``Redactor``."""
    return _default_redactor


def set_default_redactor(redactor: Redactor) -> None:
    """Set the process-wide default ``Redactor``."""
    global _default_redactor
    _default_redactor = redactor


def reset_redaction_state() -> None:
    """
    Test helper: restore ``default_redactor()`` to a fresh ``PolicyRedactor()``
    and clear the ``is_sensitive_key`` cache.

    Global mutable state hygiene, the same shape as
    ``varco_core.observability.params.reset_param_capture_state()`` — call
    this in an autouse fixture teardown in any test module that touches
    ``set_default_redactor()``.
    """
    global _default_redactor
    _default_redactor = PolicyRedactor()
    from varco_core.redaction.policy import _is_sensitive_key_cached

    _is_sensitive_key_cached.cache_clear()
