"""
varco_core.redaction.policy
=============================
``RedactionPolicy`` — the frozen configuration object, and ``is_sensitive_key``
— the leaf predicate every ``Redactor`` implementation ultimately answers
(Plan 040 / S21, §D-S21-shape, §D-S21-falsepos, §D-S21-perf).

DESIGN: word-boundary default (Plan 057 / FLIP-040-1, §D-word)
    ``match_mode="word"`` is the default since 4.0. A key **and** every
    pattern are tokenised by the same tokenizer (``_tokenize``: non-alnum
    runs, camelCase transitions, acronym→word transitions, and digit runs
    are all boundaries); a pattern matches when its token sequence occurs as
    a contiguous subsequence of the key's tokens, with the pattern's *last*
    token also accepting its simple plural (``token + "s"``).
    ``match_mode="substring"`` is the 3.2 matcher, byte-identical — the
    explicit escape hatch.

    ✅ No false positives on domain-chosen keys: ``"pin"`` no longer matches
       ``shipping_address`` and ``"auth"`` no longer matches ``author``
       (040 §D-S21-falsepos — the reason audit redaction could not be a
       default in 3.2).
    ✅ Multi-token defaults work in every spelling: ``"api_key"`` tokenises
       to ``("api", "key")`` and so matches ``api_key``/``apiKey``/
       ``API_KEY``/``x-api-key`` — hyphenated headers are caught by DEFAULT
       alone (3.2's word mode compared the *untokenised* pattern against the
       key's tokens, so ``api_key``/``private_key``/``session_id`` could
       never match; flipping onto that matcher would have un-redacted them).
    ✅ The plural rule closes the most common un-redaction
       (``credentials``/``secrets``/``tokens``/``cookies``) without growing
       ``DEFAULT_REDACT_PATTERNS`` (whose fifteen literals are pinned).
    ❌ Suffix-glued names with no separator (``csrftoken``, ``secretkey``,
       ``totp_code``, ``oauth``) are one token and are no longer matched —
       answered by the substring escape hatch or by listing the exact token
       as a pattern, never by loosening the tokenizer.
    ❌ The plural rule is one more rule to know.
    Rejected: prefix-token matching (``author.startswith("auth")``
    reintroduces the headline false positive); plural literals in the
    default tuple (endless, and two homes for one fact); a ``"regex"`` mode
    (the ``Redactor`` Protocol is the extension point).

DESIGN: precomputed lowercase + a bounded ``functools.lru_cache`` (§D-S21-perf)
    ✅ An audit diff walks every field of two DTO dumps on every write — the
       *same* field names, every request. Caching turns repeat lookups into
       a dict hit after the first sighting.
    ✅ The cache key includes the policy itself (patterns + match_mode), so
       ``set_default_redactor()`` or a per-service policy cannot read a
       stale decision — a different policy is a different key.
    ✅ ``maxsize=4096`` bounds the cache against an attacker-influenced key
       space — the same ``InMemoryRateLimiter`` unbounded-keyspace lesson
       CLAUDE.md/``plans/035-http-edge-hardening.md`` already names.
    ❌ Another piece of module state to reset in tests — covered by
       ``reset_redaction_state()`` (``redactor.py``), which clears it.

Thread safety:  ✅ ``RedactionPolicy`` is frozen and hashable — safe to share
                   across threads/tasks. The ``lru_cache`` is CPython's own
                   thread-safe implementation.
Async safety:   ✅ Pure, synchronous, no I/O.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from functools import lru_cache
from typing import Literal

from varco_core.redaction.patterns import DEFAULT_REDACT_PATTERNS

__all__ = ["AUDIT_REDACTION_POLICY", "RedactionPolicy", "is_sensitive_key"]

# Splits a key into word-boundary tokens: non-alnum separators (``_``/``-``/
# ``.``/space/...), camelCase transitions, an acronym followed by a
# capitalised word (``HTTPAuthorization``), and digit runs as their own token
# (``password2`` -> ``password``/``2`` — Plan 057; 3.2 glued digits to the
# preceding run, so ``password2`` never matched ``password``). Stdlib `re`.
_WORD_TOKEN_RE = re.compile(r"[A-Z]+(?=[A-Z][a-z])|[A-Z]?[a-z]+|[A-Z]+|[0-9]+")


@dataclass(frozen=True)
class RedactionPolicy:
    """
    Immutable configuration for key-name-based redaction and (for the one
    nested surface, the audit diff) traversal guards.

    Args:
        patterns: Case-insensitive patterns matched against a key name
            (interpretation depends on ``match_mode``). Defaults to
            ``DEFAULT_REDACT_PATTERNS`` — the same fifteen literals span
            capture has always used.
        match_mode: ``"word"`` (default since 4.0 — word-boundary token
            matching with a last-token plural rule, see the module DESIGN
            block) or ``"substring"`` (the 3.2 semantics, byte-identical,
            the escape hatch).
        max_depth: ``redact_mapping()``'s nested-walk depth cap. A subtree
            deeper than this is replaced by the string ``"<max-depth>"``.
        max_items: Per-container item cap for ``redact_mapping()``. Extra
            entries are dropped and a ``"<truncated>"`` marker entry is
            appended — never a silent drop.
        render_non_json: When ``True`` (default), ``redact_mapping()``
            renders each surviving non-JSON leaf via ``json_safe()``
            (``datetime`` -> ``"<datetime>"``). ``AUDIT_REDACTION_POLICY``
            sets it ``False`` so persisted leaves stay byte-identical.

    Raises:
        ValueError: ``match_mode == "word"`` and a pattern tokenises to
            nothing (e.g. ``"-"``) — such a pattern can never match, so it
            is a configuration error, reported loudly. Substring mode keeps
            accepting it (3.2 behaviour).

    Edge cases:
        - ``patterns`` is normalised to lowercase once, in
          ``__post_init__``, via the frozen-dataclass ``object.__setattr__``
          idiom — so ``is_sensitive_key`` never re-lowercases per call.
        - A pattern is tokenised like a key, from its original case:
          ``"apiKey"`` and ``"APIKey"`` both become ``("api", "key")``, the
          same as ``"api_key"``. In word mode a pattern containing an
          uppercase letter is therefore stored in its canonical token form
          (``"apiKey"`` -> ``"api_key"``); an all-lowercase pattern is stored
          exactly as given (only lowercased), and substring mode only
          lowercases.

    Thread safety:  ✅ Frozen — immutable, hashable, safe to share.
    """

    patterns: tuple[str, ...] = DEFAULT_REDACT_PATTERNS
    match_mode: Literal["substring", "word"] = "word"
    max_depth: int = 6
    max_items: int = 1000
    render_non_json: bool = field(default=True)

    def __post_init__(self) -> None:
        # DESIGN: lowercase once here, not per-call in is_sensitive_key.
        # object.__setattr__ is the standard frozen-dataclass idiom for a
        # normalised-on-construction field (⚠️ ASSUMPTION in the plan's
        # Risks table — untested against mypy strict at plan-writing time;
        # if it ever fails the strict gate, the one-line fallback is
        # dropping normalisation and lowercasing inside the cached
        # predicate instead, at the same measured cost after first call).
        if self.match_mode == "word":
            # DESIGN: tokenise from the ORIGINAL case, store the canonical
            # token form only when lowercasing would lose a boundary.
            #   ✅ "apiKey"/"APIKey" mean ("api","key") — the same as a key
            #      spelled that way; lowercasing first silently made them
            #      the single token "apikey" and un-redacted api_key/apiKey.
            #   ✅ Idempotent: "api_key" re-tokenises to itself, so
            #      dataclasses.replace() (which re-runs this on the stored
            #      value) never loses the split. A lowercase pattern —
            #      every DEFAULT_REDACT_PATTERNS literal — is stored as-is.
            #   ❌ `.patterns` shows "api_key" for an input of "apiKey".
            #   Rejected: a private original-case field — replace() would
            #   re-derive it from the lowercased stored value, and two
            #   policies with equal `.patterns` could then behave differently.
            normalised = tuple(
                "_".join(_tokenize(p)) if p != p.lower() else p for p in self.patterns
            )
        else:
            normalised = tuple(p.lower() for p in self.patterns)
        object.__setattr__(self, "patterns", normalised)
        if self.match_mode == "word":
            for pattern in self.patterns:
                if not _tokenize(pattern):
                    raise ValueError(
                        f"RedactionPolicy pattern {pattern!r} has no alphanumeric token and "
                        f"can never match under match_mode='word'; remove it, or use "
                        f"match_mode='substring'."
                    )


def _tokenize(name: str) -> tuple[str, ...]:
    """
    Split ``name`` into lowercase word-boundary tokens.

    Splits on any non-alphanumeric run (``_``, ``-``, ``.``, space, ...),
    on camelCase/PascalCase transitions, on an acronym followed by a
    capitalised word, and around digit runs (Plan 057 — digits are their own
    token), e.g. ``"userPin"`` -> ``("user", "pin")``,
    ``"HTTPAuthorization"`` -> ``("http", "authorization")``,
    ``"password2"`` -> ``("password", "2")``. Applied to keys *and* (via
    ``_pattern_tokens``) to word-mode patterns.

    Args:
        name: The raw key/parameter name.

    Returns:
        A tuple of lowercase tokens. Never raises — an unmatched character
        run simply produces no token for that run.
    """
    return tuple(match.group(0).lower() for match in _WORD_TOKEN_RE.finditer(name))


# DESIGN: the audit default is a named, audit-specific policy, not
# `PolicyRedactor()` (Plan 057 / FLIP-040-2, §D-audit-policy)
#   ✅ `render_non_json=False`: `AuditLogMixin` feeds `redact_mapping()` a
#      python-mode `model_dump()` — `datetime`/`UUID`/`Decimal` objects. With
#      the default `render_non_json=True` every `created_at` in every audit
#      row would become the literal "<datetime>" — a silent, irreversible,
#      hash-attested corruption. Here a non-sensitive leaf is the SAME object
#      in and out; only secret-named keys change.
#   ✅ No explicit `match_mode`: it inherits the seam's default (word), so the
#      constant can never silently diverge from it. The precondition test
#      (`test_audit_redaction.py::test_audit_default_precondition_word_mode`)
#      pins `match_mode == "word"` — the mechanical form of "FLIP-040-2 is
#      conditional on FLIP-040-1".
#   ✅ Extension idiom: `dataclasses.replace(AUDIT_REDACTION_POLICY,
#      patterns=DEFAULT_REDACT_PATTERNS + ("csrftoken",))` keeps
#      `render_non_json=False`.
#   ❌ One more exported constant (an `__all__` addition → snapshot regen).
#   Rejected: teaching `json_safe()` to stringify datetimes — it would change
#   `error_params()` output and still not keep audit leaves native.
AUDIT_REDACTION_POLICY: RedactionPolicy = RedactionPolicy(render_non_json=False)


@lru_cache(maxsize=64)
def _pattern_tokens(patterns: tuple[str, ...]) -> tuple[tuple[str, ...], ...]:
    """
    Tokenise every pattern of one ``patterns`` tuple, memoised per tuple.

    Args:
        patterns: A policy's (already lowercased) pattern tuple.

    Returns:
        One token tuple per pattern, in order.

    DESIGN: memoised per tuple, not per pattern
        ✅ A key-cache miss in ``_is_sensitive_key_cached`` does not
           re-tokenise all fifteen defaults — one dict hit per policy.
        ✅ ``maxsize=64`` — the number of distinct policies in one process
           is tiny (default, audit, span capture, a few custom ones).
        ❌ One more cache for ``reset_redaction_state()`` to clear.
    """
    return tuple(_tokenize(p) for p in patterns)


def _word_match(key_tokens: tuple[str, ...], pattern_tokens: tuple[str, ...]) -> bool:
    """
    Contiguous-subsequence match with a last-token plural rule (§D-word).

    Args:
        key_tokens: The tokenised key.
        pattern_tokens: One tokenised pattern (never empty —
            ``RedactionPolicy.__post_init__`` rejects empty ones in word mode).

    Returns:
        ``True`` when ``pattern_tokens`` occurs contiguously in
        ``key_tokens``, the last pattern token also accepting ``token + "s"``
        (``api_keys`` matches ``api_key``; ``apis_key`` does not).
    """
    n = len(pattern_tokens)
    if n == 0:
        return False
    head, last = pattern_tokens[:-1], pattern_tokens[-1]
    plural = last + "s"
    for i in range(len(key_tokens) - n + 1):
        if key_tokens[i : i + n - 1] == head and key_tokens[i + n - 1] in (last, plural):
            return True
    return False


@lru_cache(maxsize=4096)
def _is_sensitive_key_cached(key: str, patterns: tuple[str, ...], match_mode: str) -> bool:
    if match_mode == "word":
        # Tokenise the ORIGINAL-case key — camelCase boundaries (e.g.
        # "userPin" -> "user"/"Pin") are only detectable before lowering;
        # `_tokenize` lowercases each token itself once it has split them.
        key_tokens = _tokenize(key)
        return any(_word_match(key_tokens, ptoks) for ptoks in _pattern_tokens(patterns))
    # "substring" — the 3.2 semantics, verbatim (the escape hatch).
    return any(pattern in key.lower() for pattern in patterns)


def is_sensitive_key(key: str, policy: RedactionPolicy) -> bool:
    """
    Decide whether ``key`` names something sensitive under ``policy``.

    The leaf predicate every ``Redactor`` implementation ultimately answers
    — not on the ``Redactor`` Protocol itself (§D-S21-shape: traversal and
    the leaf decision are free functions, not Protocol methods).

    Args:
        key: The key/parameter name to test. Stringified via ``str(key)``
            first, so a non-``str`` key (e.g. ``1`` from a hand-built dict)
            never raises here.
        policy: The ``RedactionPolicy`` to evaluate against.

    Returns:
        ``True`` when ``key`` matches one of ``policy.patterns`` under
        ``policy.match_mode``. Never raises.

    Edge cases:
        - ``match_mode="word"`` (default): the pattern's tokens must occur
          contiguously in the key's tokens, the last one also matching its
          plural — ``"pin"`` matches ``"pin"``/``"user_pin"``/``"userPin"``/
          ``"pins"`` but not ``"shipping"``; ``"api_key"`` matches
          ``"apiKey"``/``"x-api-key"``. Case boundaries are read from the
          *original* key, so this must not be pre-lowered before tokenising.
        - ``match_mode="substring"``: the 3.2 semantics, byte-identical, the
          escape hatch — case-insensitive substring match.
        - A key with no alphanumerics (``"---"``, ``""``) never matches in
          word mode.

    Thread safety:  ✅ Pure function; the backing cache is CPython's own
                       thread-safe ``lru_cache``.
    """
    return _is_sensitive_key_cached(str(key), policy.patterns, policy.match_mode)
