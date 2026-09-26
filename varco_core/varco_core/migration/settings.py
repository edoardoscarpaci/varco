"""
varco_core.migration.settings
==============================
``MigrationSettings`` — the ``VARCO_MIGRATE_*`` env-driven configuration for
every migration posture (``off``/``check``/``upgrade``).

DESIGN: ``VarcoSettings`` (pydantic), not a hand-rolled ``from_env()``
    **Plan 048 / DI-6: converted from a frozen dataclass with a hand-rolled
    ``from_env(env: Mapping | None)`` to a pydantic ``VarcoSettings``.**
    Same class name, same field names, every default literal
    byte-identical. ``Literal["off", "check", "upgrade"]`` /
    ``Literal["fail", "warn"]`` now validate natively — the old
    ``_LEGAL_MODES``/``_LEGAL_ON_FAILURE`` manual-membership loops are gone,
    replaced by pydantic's own enum-style error message (which still names
    the legal values, satisfying
    ``test_from_env_unknown_mode_raises_valueerror_naming_legal_values``).
    The ``env: Mapping | None`` constructor parameter is gone — it was only
    ever used by tests (grep: ``test_migration_contracts.py``); those now
    use ``monkeypatch.setenv`` or plain keyword construction, matching every
    other converted class in this plan (§D-convert).
    ✅ Registered via ``bind_settings()`` like every other settings class —
       one recipe, fail-fast at bootstrap, provenance tracked for 052.
    ✅ Type validation (``lock_timeout: float``) is now pydantic's, not a
       hand-rolled ``float(...)`` call that raises a bare ``ValueError``
       with no env-var name in the message.
    ❌ None — this is a pure shape conversion; no behavioural change.

Thread safety:  ✅ ``frozen=True`` — safe to share across coroutines/threads.
Async safety:   ✅ No I/O.
"""

from __future__ import annotations

from typing import Literal

from pydantic import AliasChoices, Field
from pydantic_settings import SettingsConfigDict

from varco_core.config import VarcoSettings


class MigrationSettings(VarcoSettings):
    """
    Env-driven migration posture configuration.

    Attributes:
        mode:        ``"off"`` (default, nothing runs) / ``"check"`` (fail
                     startup if the schema is behind, never writes DDL) /
                     ``"upgrade"`` (acquire lock, upgrade heads, release).
                     Env: ``VARCO_MIGRATE_MODE``.
        on_failure:  ``"fail"`` (default, raise) / ``"warn"`` (log ERROR and
                     continue — a deliberately dangerous escape hatch).
                     Env: ``VARCO_MIGRATE_ON_FAILURE``.
        lock_key:    Distributed lock key. Env: ``VARCO_MIGRATE_LOCK_KEY``.
        lock_timeout: Seconds to wait for the lock before re-checking
                     ``pending()``. Env: ``VARCO_MIGRATE_LOCK_TIMEOUT``.
        timeout:     Overall seconds budget for the migration run itself.
                     Env: ``VARCO_MIGRATE_TIMEOUT``.
        target:      Revision target for ``upgrade()``. Env:
                     ``VARCO_MIGRATE_TARGET_REV``.
        dry_run:     Render without applying. Env: ``VARCO_MIGRATE_DRY_RUN``.

    Edge cases:
        - An out-of-set ``VARCO_MIGRATE_MODE``/``VARCO_MIGRATE_ON_FAILURE``
          raises ``pydantic.ValidationError`` (wrapped as
          ``SettingsValidationError`` when bound via ``bind_settings()``),
          naming the legal literal values in the message.

    Thread safety:  ✅ Frozen.
    Async safety:   ✅ No I/O.
    """

    model_config = SettingsConfigDict(
        env_prefix="VARCO_MIGRATE_", frozen=True, populate_by_name=True
    )

    mode: Literal["off", "check", "upgrade"] = "off"
    on_failure: Literal["fail", "warn"] = "fail"
    lock_key: str = "varco:migrate"
    lock_timeout: float = 30.0
    timeout: float = 300.0
    # ⚠️ The env var is VARCO_MIGRATE_TARGET_REV, not VARCO_MIGRATE_TARGET —
    # a validation_alias is required to keep the old name working.
    target: str = Field(default="heads", validation_alias=AliasChoices("VARCO_MIGRATE_TARGET_REV"))
    dry_run: bool = False


__all__ = ["MigrationSettings"]
