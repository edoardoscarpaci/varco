"""
varco_fastapi.client.settings
================================
``ClientProfileSettings`` — the ``VARCO_CLIENT_*`` env-driven half of
``ClientProfile`` (Plan 048 / DI-6, §D-convert, Decision 1).
``ClientProfile.from_env()`` delegates to
``ClientProfile.from_settings(ClientProfileSettings())``.

DESIGN: a new class, not a retrofit of ``ClientProfile`` itself
    ✅ ``ClientProfile`` is a value object with non-env fields
       (``middleware: tuple[AbstractClientMiddleware, ...]``,
       ``trust_store: TrustStore | None``) — same shape as ``TrustStore``
       beside ``TlsSettings``, which is the pattern this class mirrors.
       ``ClientProfileSettings`` isolates only the four flat, scalar,
       env-readable knobs.
    ✅ Homogeneous with every other converted settings class in this repo
       (``TlsSettings``/``TrustStore``, ``CasbinSettings``, …) — the owner's
       explicit "make it as homogeneous as possible" instruction.
    ❌ ``retry_max``/``retry_delay``/``otel_enabled`` are not stored on
       ``ClientProfile`` itself — they are consumed once, in
       ``ClientProfile.from_settings()``, to build the ``middleware`` tuple.
       This mirrors how ``TlsSettings.trust_store_dir`` is consumed once by
       ``TrustStore.from_settings()`` to build ``ca_folders``.

Thread safety:  ✅ ``frozen=True`` — safe to share across coroutines/threads.
Async safety:   ✅ No I/O.
"""

from __future__ import annotations

from pydantic_settings import SettingsConfigDict
from varco_core.config import VarcoSettings


class ClientProfileSettings(VarcoSettings):
    """
    Env-driven half of ``ClientProfile`` (Plan 048 / DI-6, §D-convert).

    Attributes:
        timeout: Default request timeout in seconds.
            Env: ``VARCO_CLIENT_TIMEOUT`` (default ``30.0``).
        retry_max: Max retry attempts; ``0`` disables ``RetryMiddleware``
            entirely. Env: ``VARCO_CLIENT_RETRY_MAX`` (default ``0``).
        retry_delay: Base retry delay in seconds, forwarded to
            ``RetryPolicy(base_delay=...)``. Env: ``VARCO_CLIENT_RETRY_DELAY``
            (default ``0.5``).
        otel_enabled: Whether ``OTelClientMiddleware`` is added.
            Env: ``VARCO_CLIENT_OTEL_ENABLED`` (default ``True``).

    Edge cases:
        - An empty environment yields the same defaults
          ``ClientProfile.from_env()`` has always produced — every default
          below is byte-identical to the four loose ``os.environ.get()``
          reads it replaces.
    """

    model_config = SettingsConfigDict(env_prefix="VARCO_CLIENT_", frozen=True)

    timeout: float = 30.0
    retry_max: int = 0
    retry_delay: float = 0.5
    otel_enabled: bool = True


__all__ = ["ClientProfileSettings"]
