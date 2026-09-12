"""
varco_core.retention
=======================
Retention & purge automation (Plan 039 / S20) — a ``RetentionPolicy``
registry materialized onto the shipped cron→``Job`` path
(``varco_core.schedule`` + ``AbstractJobRunner``).

⛔ **This package re-exports nothing at import time** (see below) and
carries **no** module-level ``@Singleton``/``@Provider``/``@Configuration``
anywhere — ``container.scan("varco_core", recursive=True)`` is a
documented, in-use pattern (README, several ``varco_fastapi`` tests) that
auto-activates both shapes. A scanned ``@Configuration`` here would start a
**deletion** loop in every app that scans ``varco_core`` — the same rule
CLAUDE.md states for ``varco_core.tls`` and ``varco_core.event.cloudevents``,
except here the blast radius is data loss, not wire bytes.
``varco_core/tests/test_retention_no_di_side_effect.py`` is the mechanical
guard that keeps this true over time.

Import from the submodules directly — never from ``varco_core`` top level
(same PEP 562 import-budget reasoning as ``varco_core.schedule``):

    from varco_core.retention.policy import RetentionPolicy, RetentionRegistry
    from varco_core.retention.base import RetentionTarget
    from varco_core.retention.targets import DlqRetentionTarget
    from varco_core.retention.scheduler import RetentionScheduler
    from varco_core.retention.posture import inspect_retention_posture
    from varco_core.retention.di import bind_retention_registry

Full design: ``technical_docs/features/retention-and-purge.md``.
"""

from __future__ import annotations

__all__: list[str] = []
