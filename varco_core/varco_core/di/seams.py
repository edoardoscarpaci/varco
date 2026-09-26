"""
varco_core.di.seams
=====================
The one seam registry (Plan 047 §D-vocab) — a frozen ``Seam`` dataclass per
config-driven-selectable interface (``cache``, ``dlq``, ``idempotency``,
``job_store``, ``event_bus``, ``revocation``), plus ``seam_by_name()`` and
``interface_of()``.

DESIGN: interfaces as lazy dotted strings, never a direct import
-------------------------------------------------------------------
``Seam.interface`` is stored as a ``"module:ClassName"`` string, resolved
only inside ``interface_of()`` (which imports the owning module at call
time). ``varco_core.event.dlq``/``varco_core.job.base``/etc. are cheap to
import individually, but this module is imported from ``varco_core.di``'s
``__init__`` — and 046's import-budget rule (CLAUDE.md) forbids adding a
new eager top-level import chain to a package whose cost is measured. A
dotted string costs nothing until a caller actually asks "what class is
this?" (``check_backend_selection()``'s error paths, the catalogue script).

    ✅ ``import varco_core.di.seams`` stays as cheap as a handful of frozen
       dataclass literals — verified by
       ``test_importing_seams_module_does_not_import_providify`` (a
       stand-in for the fuller import-budget measurement).
    ❌ A typo in the dotted string is only caught the first time
       ``interface_of()`` runs (not at import time). Accepted — every
       caller of ``interface_of()`` is itself only reached at DI-bootstrap
       or catalogue-build time, never at plain ``import varco_core``.
"""

from __future__ import annotations

import importlib
from dataclasses import dataclass, field


@dataclass(frozen=True)
class Seam:
    """
    One config-driven-selectable seam (Plan 047 §D-vocab).

    Args:
        seam: Short registry key (``"cache"``, ``"event_bus"``, ...).
        env: The ``VARCO_<SEAM>_BACKEND`` environment variable name.
        default_value: The in-process default's named value (``"memory"``
            for every seam except ``revocation``, which defaults to
            ``"null"``) — §D-vocab's "never gates anything, always known"
            value; ``selected_by()`` refuses to let any backend register
            this same name.
        interface_dotted: ``"module.path:ClassName"`` — resolved lazily by
            ``interface_of()``, never imported at module scope (see this
            module's docstring).
        known_values: Every named value ever registered against this seam
            *in this process* by ``selected_by()`` — populated at import
            time of the backend module that calls it, not at ``Seam``
            construction (kept in ``varco_core.di.select._REGISTERED_VALUES``,
            not here — this field exists only for the frozen "documented at
            design time" table, not the live registry).

    Thread safety: Frozen — immutable, safe to share across threads.
    """

    seam: str
    env: str
    default_value: str
    interface_dotted: str
    known_values: tuple[str, ...] = field(default_factory=tuple)


#: Per §D-vocab's table. `known_values` here is the *documented* vocabulary
#: (used by error messages and the catalogue's "known backends" column) —
#: the *live* registry of what was actually imported in this process is
#: `varco_core.di.select._REGISTERED_VALUES`, populated by `selected_by()`.
SEAMS: tuple[Seam, ...] = (
    Seam(
        seam="cache",
        env="VARCO_CACHE_BACKEND",
        default_value="memory",
        interface_dotted="varco_core.cache.base:CacheBackend",
        known_values=("redis", "layered", "memcached"),
    ),
    Seam(
        seam="dlq",
        env="VARCO_DLQ_BACKEND",
        default_value="memory",
        interface_dotted="varco_core.event.dlq:AbstractDeadLetterQueue",
        known_values=("kafka", "nats", "redis", "redis-streams", "sa", "beanie"),
    ),
    Seam(
        seam="idempotency",
        env="VARCO_IDEMPOTENCY_BACKEND",
        default_value="memory",
        interface_dotted="varco_core.idempotency.base:AbstractIdempotencyStore",
        known_values=("redis", "sa", "beanie"),
    ),
    Seam(
        seam="job_store",
        env="VARCO_JOB_STORE_BACKEND",
        default_value="memory",
        interface_dotted="varco_core.job.base:AbstractJobStore",
        known_values=("redis", "sa", "beanie"),
    ),
    Seam(
        seam="event_bus",
        env="VARCO_EVENT_BUS_BACKEND",
        default_value="memory",
        interface_dotted="varco_core.event.base:AbstractEventBus",
        known_values=("kafka", "nats", "redis"),
    ),
    Seam(
        seam="revocation",
        env="VARCO_REVOCATION_BACKEND",
        default_value="null",
        interface_dotted="varco_core.revocation.base:AbstractTokenRevocationStore",
        known_values=("memory", "redis"),
    ),
)


def seam_by_name(name: str) -> Seam:
    """
    Look up a ``Seam`` by its registry key.

    Args:
        name: One of ``"cache"``, ``"dlq"``, ``"idempotency"``,
            ``"job_store"``, ``"event_bus"``, ``"revocation"``.

    Returns:
        The matching ``Seam``.

    Raises:
        LookupError: ``name`` is not a registered seam.

    Example::

        seam_by_name("cache").env  # "VARCO_CACHE_BACKEND"
    """
    for seam in SEAMS:
        if seam.seam == name:
            return seam
    raise LookupError(f"Unknown seam {name!r} — known seams: {', '.join(s.seam for s in SEAMS)}.")


def interface_of(seam: Seam) -> type:
    """
    Resolve a ``Seam``'s dotted interface string into the real ABC class.

    Args:
        seam: The ``Seam`` whose ``interface_dotted`` field to resolve.

    Returns:
        The imported class object (e.g. ``AbstractEventBus``).

    Raises:
        ImportError: The dotted module cannot be imported.
        AttributeError: The module has no attribute of that name.

    Edge cases:
        Imports the owning module on every call — no caching. Each call
        site (bootstrap/catalogue-build time) is cold-start-infrequent, so
        the repeated ``importlib.import_module`` cost (a cheap dict lookup
        on subsequent imports, since the module is already in
        ``sys.modules``) is not worth a manual cache.

    Example::

        interface_of(seam_by_name("event_bus"))  # AbstractEventBus
    """
    module_path, _, class_name = seam.interface_dotted.partition(":")
    module = importlib.import_module(module_path)
    resolved: type = getattr(module, class_name)
    return resolved


__all__ = ["Seam", "SEAMS", "seam_by_name", "interface_of"]
