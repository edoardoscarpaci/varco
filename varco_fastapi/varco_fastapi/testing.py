"""
varco_fastapi.testing
========================
``in_memory_app_modules()`` — the FastAPI default module set on in-process
defaults, for a downstream test that wants a whole app under
``TestClient`` (Plan 052 / DI-11, §D-testprofile).

Differences from ``default_modules()``:

* ``CoreModule(job_runner=False)`` — no ``SCHEDULERS``-phase component.
* ``SecurityPostureModule`` is warn-only in the ``development`` environment,
  so a test app never refuses to start over a posture finding.
* ``InMemoryProfile`` last — it asserts the app stayed on in-process defaults.

Example::

    app = create_varco_app(DIContainer(), modules=in_memory_app_modules())
    with TestClient(app) as client:
        assert client.get("/health").status_code == 200

Thread safety:  ✅ Pure list construction; fresh instances per call.
Async safety:   ✅ No I/O.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from varco_core.di.module import CoreModule
from varco_core.testing import InMemoryProfile

from varco_fastapi.modules.defaults import default_modules
from varco_fastapi.modules.lifecycle import SecurityPostureModule
from varco_fastapi.posture import SecurityPostureSettings

if TYPE_CHECKING:
    from varco_core.di.module import VarcoModule

__all__ = ["in_memory_app_modules"]


def in_memory_app_modules(*extra: VarcoModule) -> list[VarcoModule]:
    """
    Build the FastAPI default module set on in-process defaults.

    Args:
        extra: Additional modules (an app's own, or a backend module under
            test) placed before ``InMemoryProfile``.

    Returns:
        ``default_modules()`` with ``CoreModule`` replaced by
        ``CoreModule(job_runner=False)`` and ``SecurityPostureModule``
        replaced by a ``development``/``warn`` instance, followed by
        *extra* and ``InMemoryProfile()``.
    """
    base = default_modules(exclude=(SecurityPostureModule,))
    # DESIGN: substitute in place rather than rebuild the list, so the
    # documented default ordering survives any future default_modules() edit.
    # ✅ order stays owned by default_modules(); ❌ a CoreModule-less default
    # set would silently get no scheduler opt-out — acceptable, nothing to opt out of.
    modules: list[VarcoModule] = [
        CoreModule(job_runner=False) if isinstance(m, CoreModule) else m for m in base
    ]
    modules.append(
        SecurityPostureModule(
            settings=SecurityPostureSettings(environment="development", enforce="warn")
        )
    )
    modules.extend(extra)
    modules.append(InMemoryProfile())
    return modules
