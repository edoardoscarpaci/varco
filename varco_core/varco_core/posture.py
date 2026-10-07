"""
varco_core.posture
=====================
``PostureSeverity``/``PostureFinding`` (moved verbatim from
``varco_fastapi/varco_fastapi/posture.py``) plus the new ``@Multibound``
collection seam ``PostureCollector`` (Plan 051 / DI-5, §D-posture).

DESIGN: the seam lives in ``varco_core``, the built-in collectors stay
in ``varco_fastapi`` (§D-posture)
    ✅ Layering: a plugin (or ``varco_sa``'s future live RLS check) can
       subclass ``PostureCollector`` without importing ``varco_fastapi`` —
       the same seam rule as ``varco_core.migration`` vs
       ``varco_fastapi.migrate``. Before this module existed, a
       non-``varco_fastapi`` posture collector was structurally impossible
       (R2 in the plan's registry table).
    ✅ ``varco_fastapi.posture`` re-exports ``PostureFinding``/
       ``PostureSeverity`` so every existing import site
       (``from varco_fastapi.posture import PostureFinding``) keeps working
       — identity-preserving (``varco_fastapi.posture.PostureFinding is
       varco_core.posture.PostureFinding``), never a copy.
    ❌ ``PostureFinding``/``PostureSeverity`` now have two import paths for
       one 4.0 cycle. Accepted — 058 may deprecate the ``varco_fastapi`` one
       later; not this plan's job.

DESIGN: ``PostureCollector`` is ``@Multibound`` — a plain seam, not a
callable protocol (§D-posture, rejected candidates)
    ✅ A class can carry constructor state (``HttpEdgeCollector(app)``) and
       is ``@Multibound``-able; a bare ``Callable[[], list[PostureFinding]]``
       cannot be marked with a providify class decorator.
    ✅ ``collect(container, PostureCollector)`` (``varco_core.di.collect``)
       is the one sanctioned read — zero contributions is a legal, empty
       ``()`` (never a ``LookupError``).
    ❌ Small classes where functions used to live in ``varco_fastapi``
       (seven: ``HttpEdgeCollector``/``LocalPostureCollector``/
       ``AuthPostureCollector``/``TenantPostureCollector``/
       ``DataPostureCollector``/``RedactionPostureCollector`` (Plan 057 /
       RO-5) and the opt-in ``DIPostureCollector`` (Plan 047/054)).
       Accepted —
       §D-posture's own trade-off record.

Thread safety:  ✅ ``PostureFinding``/``PostureSeverity`` are frozen/enum.
                   A ``PostureCollector`` subclass should be stateless or
                   hold only immutable configuration, mirroring
                   ``TenantSource``'s own contract — a container may share
                   one collector instance across concurrent preflight runs.
Async safety:   ✅ ``collect()`` is synchronous by contract (see the class
                   docstring) — the lifecycle wraps it, never awaits it.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import StrEnum
from typing import ClassVar

from providify import Multibound

__all__ = ["PostureSeverity", "PostureFinding", "PostureCollector"]


class PostureSeverity(StrEnum):
    """
    The four-value severity ladder (§D-S9-degrade, Plan 036). ``NOT_ASSESSED``
    is a severity, not an absence — a collector that could not run must
    never render as a clean report.
    """

    INFO = "info"
    WARN = "warn"
    HIGH = "high"
    NOT_ASSESSED = "not_assessed"


@dataclass(frozen=True)
class PostureFinding:
    """
    One reported fact.

    Args:
        check: Stable id — the contract operators suppress on
            (``VARCO_SECURITY_SUPPRESS``). Never renamed once shipped.
        severity: Current severity, post environment-demotion and
            suppression-demotion.
        detail: What was observed. **Never** a raw exception string — see
            ``varco_fastapi.posture._wrap_collector``.
        remediation: The exact env var / kwarg / role that fixes it.
        suppressed: ``True`` when ``check`` appears in
            ``SecurityPostureSettings.suppress`` — the finding is still
            produced (never removed), only demoted and marked.
    """

    check: str
    severity: PostureSeverity
    detail: str
    remediation: str
    suppressed: bool = False


@Multibound
class PostureCollector(ABC):
    """
    A single, pluggable contributor to the security-posture preflight
    (Plan 051 / DI-5, §D-posture).

    A collection seam: any ordinary non-``@Fallback`` binding of this type
    (a scanned ``@Singleton`` subclass, a ``@Provider`` returning it, or
    ``varco_core.di.collect.contribute()``) is picked up by
    ``collect(container, PostureCollector)`` /
    ``acollect(container, PostureCollector)`` — see
    ``varco_core.di.collect``'s module docstring for the shared convention
    every collection seam in varco follows.

    Args (class-level contract data):
        name: A short, stable identifier used in the ``NOT_ASSESSED``
            finding's ``check`` id if this collector raises, and in the
            ``posture.duplicate_collector`` finding if two collectors share
            a name. Duplicate names are reported, never fatal.

    Edge cases (the implementer contract):
        - ``collect()`` is synchronous and **must not raise** — the
          lifecycle wraps every collector so a raising one degrades to one
          ``NOT_ASSESSED`` finding rather than taking the whole preflight
          down (mirrors ``TenantSource.resolve()``'s "never raise"
          contract).
        - ``name`` must be unique per app; a duplicate is reported as a
          ``posture.duplicate_collector`` finding, never a crash.

    Thread safety: implementations should be stateless or hold only
        immutable configuration — a container may share one collector
        instance across concurrent preflight runs.
    """

    name: ClassVar[str]

    @abstractmethod
    def collect(self) -> list[PostureFinding]:
        """
        Return this collector's findings.

        Returns:
            Zero or more ``PostureFinding``s.

        Never raises — see the class *Edge cases* above.
        """
        raise NotImplementedError
