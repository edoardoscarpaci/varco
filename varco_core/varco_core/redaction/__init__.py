"""
varco_core.redaction
======================
Plan 040 / S21 — the unified redaction seam: one small public surface behind
span capture, ``error_params()``, the audit trail, and request logging.

Public surface (all re-exported at this package's top level)::

    from varco_core.redaction import (
        Redactor,                  # the one-method Protocol
        PolicyRedactor,            # the default implementation
        RedactionPolicy,           # frozen config (patterns, match_mode, ...)
        is_sensitive_key,          # the leaf predicate
        redact_mapping,            # depth/cycle/item-safe nested walk
        redact_query_string,       # URL query-string helper
        json_safe,                 # untruncated JSON-shape rendering
        default_redactor,          # process-wide default (getter)
        set_default_redactor,      # process-wide default (setter)
        reset_redaction_state,     # test helper
        DEFAULT_REDACT_PATTERNS,   # the incumbent 15 patterns (byte-identical)
        EXTENDED_REDACT_PATTERNS,  # opt-in — signature/bearer/jwt/...
        PII_REDACT_PATTERNS,       # opt-in, never a default
    )

``varco_core.redaction.posture`` (``inspect_redaction_posture`` +
``RedactionFinding``/``RedactionPosture``) is a separate, deliberately
un-re-exported module — see its own docstring.

⛔ No ``@Singleton``/``@Provider``/``@Configuration`` anywhere in this
package (§D-S21-di) — ``container.scan("varco_core", recursive=True)`` is a
documented, in-use pattern, and a scanned decorator here would silently
change behaviour in every app that scans ``varco_core``. Nothing in this
package injects; the default-redactor cell is a plain module-level getter/
setter, the same shape as
``varco_core.observability.params.param_capture_defaults()``.

Import direction:  stdlib only. This package imports nothing from
``varco_core.observability`` (asserted by
``varco_core/tests/test_redaction_extraction.py``) — the dependency runs
the other way (``params.py`` imports ``DEFAULT_REDACT_PATTERNS`` from here).

Thread safety / Async safety: see each submodule.
"""

from __future__ import annotations

from varco_core.redaction.patterns import (
    DEFAULT_REDACT_PATTERNS,
    EXTENDED_REDACT_PATTERNS,
    PII_REDACT_PATTERNS,
)
from varco_core.redaction.policy import RedactionPolicy, is_sensitive_key
from varco_core.redaction.redactor import (
    PolicyRedactor,
    Redactor,
    default_redactor,
    json_safe,
    redact_mapping,
    redact_query_string,
    reset_redaction_state,
    set_default_redactor,
)

__all__ = [
    "Redactor",
    "PolicyRedactor",
    "RedactionPolicy",
    "is_sensitive_key",
    "redact_mapping",
    "redact_query_string",
    "json_safe",
    "default_redactor",
    "set_default_redactor",
    "reset_redaction_state",
    "DEFAULT_REDACT_PATTERNS",
    "EXTENDED_REDACT_PATTERNS",
    "PII_REDACT_PATTERNS",
]
