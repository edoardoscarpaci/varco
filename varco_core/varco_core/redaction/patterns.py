"""
varco_core.redaction.patterns
===============================
The canonical home of ``DEFAULT_REDACT_PATTERNS`` (Plan 040 / S21, §D-S21-patterns).

Moved here **byte-identical** from ``varco_core.observability.params``
(``params.py:92-108``) — the same 15 case-insensitive substring patterns,
the same order, the same comment. ``params.py`` now imports this tuple
*object* rather than owning a second copy (§D-S21-compat) — span capture's
behaviour does not change one character.

Two additive constants ship beside it, **neither in any default**:
``EXTENDED_REDACT_PATTERNS`` (opt-in — signature/bearer/jwt-shaped header
names, relevant to Plan 038's webhook surface) and ``PII_REDACT_PATTERNS``
(opt-in, and deliberately never a default — an audit trail exists to record
PII; crypto-shredding is varco's answer to PII-at-rest, not redaction).

Patterns explicitly rejected (recorded so a later "completeness" edit has to
argue with this list): ``"key"`` (matches ``primary_key``/``cache_key``/
``idempotency_key``/``partition_key``), ``"name"``, ``"id"``, ``"user"``,
``"account"``.

Thread safety:  ✅ Module-level immutable tuples — safe to import and share.
Async safety:   ✅ No I/O, no state.
"""

from __future__ import annotations

# Case-insensitive **substring** match on the parameter/key name.  Fail-closed:
# redaction wins even over an explicit `include=(...)` allow-list entry.
#
# ⛔ Verbatim from varco_core/observability/params.py:92-108 — do not add,
# remove, or reorder any entry here. A pattern added to this tuple silently
# changes span capture for every existing deployment, with no error and no
# log line (§D-S21-patterns). Use EXTENDED_REDACT_PATTERNS/PII_REDACT_PATTERNS
# for anything new, or a caller-supplied RedactionPolicy(patterns=...).
DEFAULT_REDACT_PATTERNS: tuple[str, ...] = (
    "password",
    "passwd",
    "secret",
    "token",
    "authorization",
    "auth",
    "api_key",
    "apikey",
    "credential",
    "private_key",
    "cookie",
    "session_id",
    "otp",
    "pin",
    "ssn",
)

# DESIGN: opt-in only, never folded into DEFAULT_REDACT_PATTERNS
#   ✅ "signature" is this plan's entire answer to the Plan 038 (inbound
#      webhook verification) adjacency — it substring-matches
#      `webhook-signature`, `Stripe-Signature`, `X-Hub-Signature-256` as a
#      **key** name. 038's own undertaking is to never log the secret/header
#      in the first place; this does not weaken that into "log it, we'll
#      scrub it".
#   ❌ Not a default — an operator must opt in via
#      `RedactionPolicy(patterns=DEFAULT_REDACT_PATTERNS + EXTENDED_REDACT_PATTERNS)`.
#   ✅ "api-key" (Open Question 3): HTTP header names are hyphenated
#      (`X-Api-Key`, `x-api-key`), while DEFAULT's `"api_key"` is
#      underscore-shaped — `"api_key" in "x-api-key"` is `False`, so the
#      hyphenated header name is silently *not* caught by DEFAULT alone.
#      Added here rather than to DEFAULT for the same reason as every other
#      entry in this tuple: it changes matching behaviour and must be an
#      explicit opt-in, never a silent default change.
EXTENDED_REDACT_PATTERNS: tuple[str, ...] = (
    "signature",
    "passphrase",
    "bearer",
    "jwt",
    "salt",
    "api-key",
)

# DESIGN: PII patterns are opt-in and deliberately NEVER a default
#   ✅ An audit trail exists to record PII — an audit row that cannot show an
#      email address changed is not a safer audit trail, it is a broken one
#      (GDPR Art. 30 processing records).
#   ✅ varco already ships the right answer for PII-at-rest:
#      `varco_core.encryption` (field-level encryption / crypto-shredding).
#      Redaction destroys; crypto-shredding defers destruction to a key.
PII_REDACT_PATTERNS: tuple[str, ...] = (
    "email",
    "phone",
    "address",
    "iban",
    "card_number",
    "cvv",
    "birth",
    "national_id",
    "tax_id",
)

__all__ = [
    "DEFAULT_REDACT_PATTERNS",
    "EXTENDED_REDACT_PATTERNS",
    "PII_REDACT_PATTERNS",
]
