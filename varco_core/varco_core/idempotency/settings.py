"""
varco_core.idempotency.settings
================================
``IdempotencySettings`` — off by default (Plan 029 / D1a, Step 5).

Bound by ``varco_core.settings.bind_settings()`` (Plan 048 / DI-6) from
``bind_core_settings()``, never by a module-level ``@Provider`` or
``@Singleton``. ``bind_settings()`` registers a ``@Provider`` *closure*
because a pydantic ``BaseSettings.__init__`` carries 29 annotated, defaulted
keyword parameters (``_env_prefix``, ``_env_file``, …) that providify's
per-parameter resolver would otherwise treat as injection points and fill
from whatever else happens to be bound — ``**values`` being skipped is not
the actual trap. Same treatment as every other settings class in the tree.
"""

from __future__ import annotations

from pydantic_settings import SettingsConfigDict

from varco_core.config import VarcoSettings

__all__ = ["IdempotencySettings"]


class IdempotencySettings(VarcoSettings):
    """
    Configuration for ``IdempotencyMiddleware`` (§D-D1-optin, §D-D1-ttl).

    Attributes:
        enabled:                 Master switch. ``False`` (default) — no
                                  middleware behaviour; the plain pass-through
                                  path CLAUDE.md's "off by default" convention
                                  requires for every opt-in feature.
        ttl_seconds:              How long a completed record (and, before
                                  completion, its reservation) remains valid.
                                  Default ``86400`` (24 hours), following
                                  Stripe — the draft (brief 005 §1) leaves
                                  retention as a SHOULD-publish with no
                                  concrete number.
        require_key:              If ``True``, a ``POST``/``PATCH`` without
                                  an ``Idempotency-Key`` header is rejected
                                  with 400 rather than executed normally.
                                  Default ``False``.
        max_key_length:            Maximum accepted length of the
                                  ``Idempotency-Key`` header value. A longer
                                  (or empty) key is rejected with 400.
        max_stored_body_bytes:     Ceiling on a non-streaming response body
                                  size that will be captured for replay.
                                  Over this ceiling, the reservation is
                                  released and the response passes through
                                  unrecorded (§D-D1-replay) — deliberately
                                  the same behaviour as a streaming response.
        replay_header_allowlist:   Additional header names (beyond the
                                  hard-coded ``Content-Type``/``Location``/
                                  ``Content-Language``) that are replayed
                                  verbatim on a cache hit.

    Edge cases:
        - The draft this feature implements
          (``draft-ietf-httpapi-idempotency-key-header-07``) expired
          2026-04-18 and was never published as an RFC — every place it is
          silent, these defaults follow Stripe's de-facto practice instead
          (§D-D1-ttl). Do not describe this feature as RFC-conformant.

    Thread safety:  ✅ ``VarcoSettings`` — immutable after construction
                    (frozen via subclass config, same as every other
                    settings class in this codebase).
    Async safety:   ✅ Pure value object — no I/O.
    """

    model_config = SettingsConfigDict(env_prefix="VARCO_IDEMPOTENCY_")

    enabled: bool = False
    ttl_seconds: float = 86400.0
    require_key: bool = False
    max_key_length: int = 255
    max_stored_body_bytes: int = 1_048_576
    replay_header_allowlist: tuple[str, ...] = ()
