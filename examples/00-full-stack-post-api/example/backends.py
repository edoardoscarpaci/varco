"""
The backend each selectable seam runs on in this example (Plan 047 / DI-4).

Bootstrapping a backend package never makes it live — ``VARCO_<SEAM>_BACKEND``
does. The example runs the event bus and cache on Redis and deliberately keeps
the other four seams in-process; naming those too (``memory``, or ``null`` for
revocation) states the choice explicitly, so ``varco diagnose --check`` reports
no ``di.bootstrapped_unselected`` finding (Plan 052).

``docker-compose.yml`` and the README's run command set exactly these values;
the e2e conftest and ``test_diagnose_static.py`` import them from here.
"""

from __future__ import annotations

from typing import Final

# Seams backed by the docker-compose Redis service.
REDIS_BACKENDS: Final[dict[str, str]] = {
    "VARCO_EVENT_BUS_BACKEND": "redis",
    "VARCO_CACHE_BACKEND": "redis",
}

# Seams this demo deliberately keeps in-process (single instance, no durability needed).
IN_PROCESS_BACKENDS: Final[dict[str, str]] = {
    "VARCO_DLQ_BACKEND": "memory",
    "VARCO_IDEMPOTENCY_BACKEND": "memory",
    "VARCO_JOB_STORE_BACKEND": "memory",
    "VARCO_REVOCATION_BACKEND": "null",
}

# Every VARCO_<SEAM>_BACKEND the example sets.
BACKEND_ENV: Final[dict[str, str]] = {**REDIS_BACKENDS, **IN_PROCESS_BACKENDS}
