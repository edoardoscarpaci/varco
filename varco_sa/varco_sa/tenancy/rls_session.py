"""
varco_sa.tenancy.rls_session
===============================
``install_rls_tenant_hook()`` — automatic ``set_tenant_local()`` via a
SQLAlchemy ``after_begin`` listener (Plan 037 / S12c, §D-S12-hook).

The row asks to "wire ``set_tenant_local()`` into the UoW automatically".
The obvious hook — ``SQLAlchemyUnitOfWork._begin()`` — is the wrong one:
``SQLAlchemyUnitOfWork.commit()`` ends the transaction, and the *next*
statement on that session autobegins a **new** transaction in which the
``set_config(..., true)`` GUC is gone (it is transaction-scoped, reverted on
COMMIT or ROLLBACK). A ``_begin()``-only wiring is therefore silently
correct until the first commit-then-read and silently returns zero rows
after it — and it never touches sessions created outside a UoW at all
(``SQLAlchemyRepositoryProvider.get_repository()``, or any app code holding
the session factory directly).

DESIGN: an ``after_begin`` event listener over an imperative UoW call
    ✅ Fires for every transaction on the target session factory —
       ``SQLAlchemyUnitOfWork``, ``get_repository()``, and any app code
       holding the session factory directly. ``_begin()`` covers one of
       those three.
    ✅ Reuses ``varco_sa.rls.set_tenant_local``'s exact SQL text and
       bound-parameter shape — nothing here builds SQL by concatenation.
    ✅ Returning an uninstaller keeps this testable and reversible; no
       process-global registry.
    ❌ Action at a distance — a ``SELECT set_config(...)`` appears in the
       query log at every ``BEGIN``. Documented; it is one round trip inside
       a transaction that was opening anyway.
  Rejected — ``SQLAlchemyUnitOfWork._begin()``: breaks after the first
  ``commit()``; misses ``get_repository()`` sessions entirely.
  Rejected — ``before_execute`` per statement (brief 007 §7's Mechanism 1):
  one extra round trip per *statement* rather than per transaction, for no
  benefit — the GUC is transaction-scoped, so per-statement setting buys
  nothing.

⚠️ **Empirical finding, Step 15/16 — read before touching this module.**
Brief 007 §6's own snippet shows an ``async def after_begin`` listener with
an ``await`` inside it. SQLAlchemy does **not** await a sync event hook —
``after_begin`` is always a plain synchronous callback, even when the
session in play is an ``AsyncSession``. Two consequences, verified against
this repo's pinned SQLAlchemy (2.0.48) with both SQLite (``aiosqlite``) and
Postgres:

1. **``after_begin`` only fires once a real DB connection has been
   acquired** — which, for an ``AsyncSession``, happens lazily on the
   *first statement* of a transaction, not at ``session.begin()`` itself.
   A transaction that opens and closes with no query in between never
   acquires a connection and never fires ``after_begin`` at all. **This is
   correct, and deliberately not "fixed" by forcing an eager connection** —
   an earlier version of this module did exactly that (via
   ``after_transaction_create`` + ``session.connection()``), and it broke
   the realistic, documented usage pattern of setting ``tenant_context(...)``
   *inside* an already-open ``session.begin()`` block, immediately before
   the first query::

       async with session.begin():
           with tenant_context(tenant_a):   # set AFTER begin(), BEFORE the query
               await session.execute(...)   # must see tenant_a's rows

   Forcing eager connection acquisition made ``after_begin`` fire at
   ``session.begin()`` itself — *before* ``tenant_context(tenant_a)`` was
   even entered — so the GUC was set to empty and every query saw zero
   rows. Verified by ``test_rls_session_hook.py``'s real-Postgres
   integration test (Step 16), which is the load-bearing contract here: a
   transaction that runs no query has no rows to protect anyway, so lazily
   firing on the first real statement is the *correct* behaviour, not a gap.
2. **The actual ``set_config()`` call executes against the ``Connection``
   the ``after_begin`` event provides — not the ``Session``.**
   ``after_begin``'s own docstring says so directly: *"To invoke SQL
   operations within this hook, use the Connection provided to the event;
   do not run SQL operations using the Session directly."* This works
   correctly under ``AsyncSession`` because the whole event fires
   synchronously *inside* the greenlet SQLAlchemy's asyncio extension
   already spawned to run the surrounding ``await session.execute(...)``
   call that triggered the lazy connection acquisition — a plain,
   non-awaited ``connection.execute(...)`` inside the listener performs
   real (greenlet-bridged) I/O and returns before the listener returns. No
   ``asyncio.run``/``await_only`` bridging is needed for this path.

   The one place this module *does* bridge sync-to-async is the injectable
   ``_set_tenant_local`` testing seam (an ``async def`` for symmetry with
   ``varco_sa.rls.set_tenant_local``'s real signature) — bridged via
   ``sqlalchemy.util.await_only``, which is exactly the primitive
   SQLAlchemy's own async dialect adapters use for this same sync-context
   greenlet bridge. Production code takes the ``connection.execute()`` path
   above and never needs this bridge.

Fail-closed is opt-in, deliberately. With ``require_tenant=False`` (the
default) and no ambient tenant, the hook sets the GUC to the empty string,
which ``render_rls_ddl()``'s ``NULLIF(..., '')`` maps to ``NULL`` → RLS
hides every row, no crash. With ``require_tenant=True`` it raises
``RuntimeError`` naming ``tenant_context()``. Not the default because a
background job, an ``OutboxRelay`` poll, a migration, and a health check all
legitimately run with no tenant.

Non-Postgres dialects: the hook still installs (it is dialect-agnostic) but
``set_config()`` is Postgres-only. Skipped with one WARNING **per engine**
(cached on the engine so a busy app does not log once per transaction) —
Open Question 1's resolution: per-transaction dialect detection (the dialect
is only known once a connection exists) with the result cached so the cost
is one dialect check per engine, not per ``BEGIN``.

Thread safety:  ⚠️ ``install_rls_tenant_hook()``/the returned uninstaller
                   mutate SQLAlchemy's global event registry — call both
                   from a single-threaded startup/teardown path, the same
                   convention as every other ``install_*``/``enable_*``
                   verb in this codebase.
Async safety:   ✅ The listener itself never awaits — see the empirical
                   finding above.
"""

from __future__ import annotations

from collections.abc import Callable, Coroutine
from typing import Any
from weakref import WeakKeyDictionary

from varco_core.service.tenant import current_tenant

#: Class attribute name marking a sync Session subclass as one this module
#: already installed a listener pair on — read/written only via
#: ``cls.__dict__`` (never inherited ``getattr``) so a subclass of a
#: hook-bearing class is never mistaken for already carrying its own hook.
_MARKER = "_varco_rls_hook_uninstall"

#: Postgres-dialect-check result, cached per engine (Open Question 1) — a
#: WeakKeyDictionary so an engine that is disposed and garbage collected
#: does not pin a WARNING-dedup entry forever.
_dialect_warned: WeakKeyDictionary[Any, bool] = WeakKeyDictionary()


async def _default_set_tenant_local(
    session: Any, tenant_id: str, *, setting: str = "rls.tenant_id"
) -> None:
    """
    Testing/override seam only — never called on the real path.

    Kept ``async def`` for signature symmetry with
    ``varco_sa.rls.set_tenant_local`` (and so a caller-supplied
    ``_set_tenant_local`` can be a real ``async def`` too, bridged via
    ``await_only``). Production code never reaches this function — see the
    module docstring's empirical finding §2: the real path executes against
    the event's ``Connection`` directly, synchronously, with no bridge.
    """
    import sqlalchemy as sa  # noqa: PLC0415

    session.execute(
        sa.text("SELECT set_config(:setting, :value, true)"),
        {"setting": setting, "value": tenant_id},
    )


def _resolve_sync_session_class(target: Any) -> type:
    """
    Resolve ``target`` (a ``Session``/``sessionmaker``/``async_sessionmaker``,
    or an already-built ``Session``/``AsyncSession`` instance) to the sync
    ``Session`` subclass to attach listeners to.

    For a ``sessionmaker``/``async_sessionmaker`` **instance**, a fresh,
    per-target ``Session`` subclass is created (and installed back onto the
    factory's own kwargs) the first time it is seen, so the hook is scoped
    to that one factory rather than every ``Session`` in the process —
    unless the caller passes the ``Session``/``AsyncSession`` **class**
    directly, in which case that exact scope (process-wide, or whatever
    that class already means) is honoured as asked.

    Raises:
        TypeError: ``target`` is not one of the supported shapes.
    """
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
    from sqlalchemy.orm import Session as SyncSession
    from sqlalchemy.orm import sessionmaker as sync_sessionmaker

    if isinstance(target, type):
        if issubclass(target, AsyncSession):
            return target.sync_session_class
        if issubclass(target, SyncSession):
            return target
        raise TypeError(
            f"install_rls_tenant_hook(): {target!r} is a class but neither a "
            "Session nor an AsyncSession subclass."
        )

    if isinstance(target, (async_sessionmaker, sync_sessionmaker)):
        kw_key = "sync_session_class" if isinstance(target, async_sessionmaker) else "class_"
        current = target.kw.get(kw_key) or SyncSession
        if getattr(current, "_varco_rls_scoped", False):
            return current  # already our own per-target subclass
        scoped = type(
            f"_VarcoRlsSession_{current.__name__}", (current,), {"_varco_rls_scoped": True}
        )
        target.kw[kw_key] = scoped
        return scoped

    if isinstance(target, AsyncSession):
        return type(target.sync_session)
    if isinstance(target, SyncSession):
        return type(target)

    raise TypeError(
        f"install_rls_tenant_hook(): unsupported target type {type(target)!r} — "
        "expected a Session, sessionmaker, async_sessionmaker, or an instance "
        "of Session/AsyncSession."
    )


def install_rls_tenant_hook(
    target: Any,
    *,
    setting: str = "rls.tenant_id",
    require_tenant: bool = False,
    _set_tenant_local: Callable[..., Coroutine[Any, Any, None]] | None = None,
) -> Callable[[], None]:
    """
    Install the automatic per-transaction RLS GUC-setter (§D-S12-hook).

    Registers an ``after_begin`` listener on the sync ``Session`` class
    underlying ``target``, so every transaction — via
    ``SQLAlchemyUnitOfWork``, ``get_repository()``, or app code holding the
    factory directly — sets ``rls.tenant_id`` (or ``setting``) from
    ``current_tenant()`` the moment its first statement acquires a
    connection (see the module docstring's empirical finding — this is
    lazy, deliberately, matching SQLAlchemy's own connection-acquisition
    timing).

    Args:
        target:          A ``Session``/``sessionmaker``/``async_sessionmaker``,
                         or an already-built ``Session``/``AsyncSession``
                         instance. See ``_resolve_sync_session_class()``'s
                         docstring for exactly how each shape is scoped.
        setting:         The Postgres GUC name to set. Must match whatever
                         ``render_rls_ddl()``'s ``setting=`` was for the
                         protected table(s). Default ``"rls.tenant_id"``.
        require_tenant:  When ``True``, raise ``RuntimeError`` instead of
                         clearing the GUC when no ``tenant_context()`` is
                         ambient. Default ``False`` — a background job, an
                         ``OutboxRelay`` poll, a migration, and a health
                         check all legitimately run with no tenant.
        _set_tenant_local: Testing/override seam — never set in production
                         code. See ``_default_set_tenant_local``'s docstring.

    Returns:
        An idempotent uninstall callable — calling it more than once, or
        after the listener was already removed, is a no-op.

    Raises:
        TypeError: ``target`` is not a supported shape.

    Edge cases:
        - Installing twice on the same ``target`` registers exactly one
          listener pair and returns the same uninstaller both times.
        - A non-Postgres dialect: the hook still installs (dialect-agnostic
          by construction) but skips the actual ``set_config()`` call with
          one WARNING per engine (cached — see the module docstring's Open
          Question 1 resolution), matching
          ``varco_sa.tenancy.rls_check.assert_rls_enabled()``'s and
          ``varco_sa.migration.ops``'s existing non-Postgres skip contract.
    """
    session_cls = _resolve_sync_session_class(target)

    existing = session_cls.__dict__.get(_MARKER)
    if existing is not None:
        return existing  # type: ignore[no-any-return]

    set_tenant_local = _set_tenant_local

    def _on_begin(session: Any, transaction: Any, connection: Any) -> None:
        tenant_id = current_tenant()
        if tenant_id is None:
            if require_tenant:
                raise RuntimeError(
                    "install_rls_tenant_hook(require_tenant=True): no ambient "
                    "tenant is set for this transaction. Call "
                    "tenant_context(...) (varco_core.service.tenant) before "
                    "opening it, or pass require_tenant=False to clear the "
                    "GUC instead of raising."
                )
            tenant_id = ""

        if set_tenant_local is not None:
            # Testing seam — bridge the (possibly real-async) override
            # synchronously via SQLAlchemy's own greenlet-bridge primitive.
            from sqlalchemy.util import await_only  # noqa: PLC0415

            await_only(set_tenant_local(session, tenant_id, setting=setting))
            return

        dialect_name = getattr(connection.dialect, "name", None)
        if dialect_name != "postgresql":
            engine = connection.engine
            if not _dialect_warned.get(engine, False):
                import logging  # noqa: PLC0415

                logging.getLogger(__name__).warning(
                    "install_rls_tenant_hook(): dialect %r is not postgresql — "
                    "RLS/set_config() is a Postgres-only feature. Skipping the "
                    "GUC-setter for this engine entirely (one WARNING per "
                    "engine, not per transaction).",
                    dialect_name,
                )
                _dialect_warned[engine] = True
            return

        import sqlalchemy as sa  # noqa: PLC0415

        # Empirical finding §2: run against the event's own Connection, not
        # session.execute() — after_begin's own docstring requires this.
        connection.execute(
            sa.text("SELECT set_config(:setting, :value, true)"),
            {"setting": setting, "value": tenant_id},
        )

    import sqlalchemy as sa

    sa.event.listen(session_cls, "after_begin", _on_begin)

    def _uninstall() -> None:
        if session_cls.__dict__.get(_MARKER) is None:
            return  # idempotent — already uninstalled
        sa.event.remove(session_cls, "after_begin", _on_begin)
        delattr(session_cls, _MARKER)

    setattr(session_cls, _MARKER, _uninstall)
    return _uninstall


__all__ = ["install_rls_tenant_hook"]
