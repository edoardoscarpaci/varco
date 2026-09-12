"""
varco_core.retention.base
============================
``RetentionTarget`` — the ABC every retention adapter implements, plus
``CallableRetentionTarget``, the out-of-tree escape hatch. Plan 039 (S20) /
Step 5.

DESIGN: an ABC with two capability ``ClassVar`` flags, not a bare callable
registry (§D-S20-seam)
    ✅ Adapters wrap shipped verbs — no shipped ABC
       (``AbstractDeadLetterQueue``, ``AbstractIdempotencyStore``,
       ``AbstractTokenRevocationStore``, ``AuditRepository``,
       ``AbstractJobStore``) gains a method. The standing rule that keeps
       ``BulkCache`` off ``AsyncCache`` (Plan 011 / D-11) and ``remaining()``
       off ``RateLimiter`` (Plan 035 §D-S10-headers) applies here too — five
       ~30-line wrappers cost nothing an out-of-tree implementer notices.
    ✅ ``supports_older_than``/``supports_dry_run`` are **load-bearing, not
       decoration**. Two shipped verbs (``AbstractIdempotencyStore.
       delete_expired()``, ``AbstractTokenRevocationStore.delete_expired()``)
       have no cutoff and no preview. Without ``supports_dry_run=False``, a
       ``dry_run=True`` policy pointed at one of them would call
       ``delete_expired()`` and **actually delete** — a dry run that deletes
       is the single worst bug this plan could ship.
       ``RetentionRegistry.register()`` uses ``supports_older_than`` the
       same way, at wiring time, for the ``older_than`` capability guard.
    ✅ One ABC gives the CLI's ``list`` verb, ``inspect_retention_posture()``,
       and ``install_retention_metrics()`` a uniform ``kind``/counts surface.
    ❌ A sixth in-tree target is a new class. Accepted — ``targets.py``'s
       five adapters are each ~30 lines, and ``CallableRetentionTarget``
       covers the one-off case with zero new surface.
  Rejected — a registry of plain ``async`` callables (no ABC): ❌ no place
  to declare ``supports_dry_run``, the safety-critical fact above; ❌
  nothing to introspect for the posture inspector/CLI ``list`` verb; ❌
  every call site re-invents the chunked-sweep loop.
  Rejected — ``purge()`` on the shipped ABCs themselves: ⛔ forbidden by the
  standing "adapters, never ABC surgery" rule.

Thread safety:  N/A — no shared mutable state at this layer; each adapter
                   documents its own.
Async safety:   ✅ ``purge()`` is ``async def``; implementations await every
                   I/O call.
"""

from __future__ import annotations

import abc
from collections.abc import Awaitable, Callable
from datetime import datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from varco_core.retention.policy import RetentionOutcome

__all__ = ["CallableRetentionTarget", "RetentionTarget"]


class RetentionTarget(abc.ABC):
    """
    A cleanable "thing" — a thin adapter over one shipped bulk-delete verb.

    Class attributes:
        supports_older_than: Whether ``purge(older_than=...)`` is a
            meaningful cutoff for this target. ``False`` for the two
            ``delete_expired()``-backed targets (idempotency, revocation) —
            expiry there is intrinsic, not age-based.
        supports_dry_run: Whether ``purge(dry_run=True)`` can actually
            preview (count matches without deleting). ``False`` for the
            same two targets — there is no non-destructive way to ask
            "how many would `delete_expired()` remove?" without calling it.
    """

    # DESIGN: plain class attributes, not ClassVar — the five in-tree
    # adapters set these at class level (fixed per adapter type);
    # CallableRetentionTarget needs per-INSTANCE values (the caller
    # declares them per construction, since one CallableRetentionTarget
    # class wraps arbitrarily many different callables). A ClassVar
    # annotation would make mypy reject that per-instance override
    # ("Cannot assign to class variable via instance") — a plain `bool`
    # attribute supports both usages identically at runtime.
    supports_older_than: bool = True
    supports_dry_run: bool = True

    @property
    @abc.abstractmethod
    def kind(self) -> str:
        """A short, stable identifier — ``"dlq"``, ``"audit"``, ``"job"``,
        ``"idempotency"``, ``"revocation"``, or a caller-chosen string for
        ``CallableRetentionTarget``."""
        raise NotImplementedError

    @abc.abstractmethod
    async def purge(
        self, *, older_than: datetime | None, limit: int, dry_run: bool
    ) -> RetentionOutcome:
        """
        Purge (or preview purging) up to ``limit`` matching rows.

        Args:
            older_than: The age cutoff, or ``None`` when
                ``supports_older_than=False`` (the implementation must
                reject a non-``None`` value in that case, and reject
                ``None`` when it *does* support one — mirrors
                ``RetentionRegistry.register()``'s wiring-time guard, at
                call time, for a target used directly without going through
                a ``RetentionPolicy``).
            limit: Maximum rows to examine/delete in this one call —
                callers (``RetentionScheduler``/the CLI) chunk a large sweep
                with repeated calls, exactly like
                ``AbstractJobStore.delete_where``'s documented recipe.
            dry_run: When ``True`` on a ``supports_dry_run=True`` target,
                count matches without deleting. When ``True`` on a
                ``supports_dry_run=False`` target, the implementation MUST
                **skip** — call no delete method at all — and report
                ``skipped_reason``. This is the single most important
                contract in this plan.

        Returns:
            A ``RetentionOutcome`` describing what happened (or would have).

        Raises:
            ValueError: An ``older_than``/capability mismatch, or a
                predicate-free call the underlying verb refuses. Never
                swallowed — propagates to the caller unchanged.
        """
        raise NotImplementedError


class CallableRetentionTarget(RetentionTarget):
    """
    The out-of-tree escape hatch: wraps any
    ``async (older_than, limit, dry_run) -> int`` callable as a
    ``RetentionTarget``.

    Args:
        kind: The target's reported ``kind``.
        fn: An async callable matching ``purge()``'s keyword-only signature,
            returning the number of rows deleted (when ``dry_run=False``) or
            the number that would be deleted (when ``dry_run=True`` and
            ``supports_dry_run=True`` — the caller's ``fn`` is responsible
            for making that call non-destructive itself).
        supports_older_than: Declared by the caller — this class cannot
            introspect an arbitrary callable's capability.
        supports_dry_run: Declared by the caller. When ``False``,
            ``purge(dry_run=True)`` never calls ``fn`` at all — the same
            skip contract every in-tree adapter honours.

    Edge cases:
        - ``supports_dry_run=True`` but ``fn`` is not actually
          non-destructive under ``dry_run=True`` is a caller bug this class
          cannot detect — document it loudly at the call site.

    Thread safety:  ✅ Immutable after construction.
    Async safety:   ✅ Delegates directly to the wrapped coroutine function.
    """

    def __init__(
        self,
        *,
        kind: str,
        fn: Callable[..., Awaitable[int]],
        supports_older_than: bool = True,
        supports_dry_run: bool = True,
    ) -> None:
        self._kind = kind
        self._fn = fn
        self.supports_older_than = supports_older_than
        self.supports_dry_run = supports_dry_run

    @property
    def kind(self) -> str:
        return self._kind

    async def purge(
        self, *, older_than: datetime | None, limit: int, dry_run: bool
    ) -> RetentionOutcome:
        from varco_core.retention.policy import RetentionOutcome  # noqa: PLC0415

        if dry_run and not self.supports_dry_run:
            return RetentionOutcome(
                kind=self._kind,
                deleted=0,
                would_delete=0,
                dry_run=True,
                skipped_reason=(
                    f"CallableRetentionTarget(kind={self._kind!r}) declares "
                    "supports_dry_run=False — skipping under dry_run=True "
                    "rather than risk fn() being destructive."
                ),
            )
        count = await self._fn(older_than=older_than, limit=limit, dry_run=dry_run)
        if dry_run:
            return RetentionOutcome(kind=self._kind, deleted=0, would_delete=count, dry_run=True)
        return RetentionOutcome(kind=self._kind, deleted=count, would_delete=0, dry_run=False)
