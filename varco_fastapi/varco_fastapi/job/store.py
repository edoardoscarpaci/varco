"""
varco_fastapi.job.store
========================
**Moved in 4.0.0** — ``InMemoryJobStore`` now lives in
``varco_core.job.memory`` (Plan 047 §D-vocab: the ``job_store`` seam's
in-process default belongs beside every other in-process seam default,
not in a downstream package). This module is kept as a re-export so
``from varco_fastapi.job.store import InMemoryJobStore`` keeps working
(Migration table) — the canonical home is ``varco_core.job.memory``.
"""

from __future__ import annotations

from varco_core.job.memory import InMemoryJobStore

__all__ = ["InMemoryJobStore"]
