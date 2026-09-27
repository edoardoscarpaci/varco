# Distributed locking: session vs transaction, and the pooling trap

Plan 005, Phase 5 (gap U-16). Closes: "`SAAdvisoryLock` uses PostgreSQL's
**session-level** advisory lock functions and documents the assumption that
each process holds its own advisory lock via its own connection — direct
connections, not a pooler. Behind a transaction-mode connection pooler, this
silently leaks locks."

## The four-step failure (why this matters)

`SAAdvisoryLock` (`varco_sa/varco_sa/advisory_lock.py`) pins one connection
per held lock and calls `pg_try_advisory_lock` / `pg_advisory_unlock` on it.
That is correct **only if the same physical connection serves both calls**.
Under a **transaction-mode** connection pooler (PgBouncer
`pool_mode=transaction`, pgcat, Supavisor in transaction mode — anything that
returns a physical connection to its pool between statements/transactions
rather than holding it for the whole logical session), that assumption
breaks in four concrete steps:

1. `try_acquire()` runs `pg_try_advisory_lock` on borrowed physical
   connection **A**.
2. The pooler returns **A** to its pool as soon as the implicit transaction
   around that single statement ends — a transaction-mode pooler does this
   between *every* statement, not just at logical session end.
3. `release()` later runs `pg_advisory_unlock` on whichever physical
   connection the pooler happens to route it to — call it **B**, a
   *different* physical connection. PostgreSQL's advisory-unlock functions
   only release a lock held by the **calling session**; B never held it, so
   the call returns `false` and nothing happens.
4. The lock **leaks on connection A** until A is closed (pool recycling,
   restart) — and because A is back in the pool, the **next, unrelated
   borrower of A silently inherits the held lock**, with no idea why some of
   its own advisory-lock operations on the same key are being rejected as
   contended.

This is exactly the failure the filer's U-16 gap named. The filer's own
assessment of documenting it prominently: "converts a silent leak into a
known constraint" — Phase 5 does that (Step 64) **and** ships the safe
alternative (Step 63).

## Session-vs-transaction table

| | `SAAdvisoryLock` | `SAXactAdvisoryLock` |
|---|---|---|
| PostgreSQL function | `pg_try_advisory_lock` / `pg_advisory_unlock` | `pg_try_advisory_xact_lock` |
| Scope | Session (the physical connection) | Transaction (COMMIT/ROLLBACK) |
| Release | Explicit `release()` call | Automatic at COMMIT/ROLLBACK — **no `release()` call for the primary `xact()` API** |
| Primary usage shape | `try_acquire`/`release` (ABC) | `xact(key, session)` — async context manager on the caller's own transaction |
| ABC-compat shape (`try_acquire`/`release`) | ✅ native | ✅ provided, but pins its own connection+transaction for the lock's whole lifetime — same cost as `SAAdvisoryLock`, just transaction- not session-scoped |
| Connection cost | One pinned connection per held lock | `xact()`: **zero extra** — reuses a transaction the caller already needed. ABC shape: one pinned connection, same as `SAAdvisoryLock` |
| `ttl` parameter | Accepted, **not enforced** at the DB level (lock lasts until connection closes) | Accepted on the ABC shape, but **meaningless**, not merely unenforced — the transaction's own commit/rollback is what bounds the lock; there is no timer construct for `ttl` to drive even in principle |
| Re-entrancy (same session/xact, same key) | NOT re-entrant — a second acquire on the same session/key requires a matched second unlock | Stackable per session/key within one top-level transaction — Postgres semantics; all stacked acquisitions release together at that transaction's end |
| Safe behind transaction-mode pooling | ❌ **No** — see the four-step failure above | ✅ **Yes** — the lock's entire lifetime is bounded by the one transaction it was taken in; there is no separate release call a pooler could misroute |

## Pooling compatibility matrix

| Topology | `SAAdvisoryLock` | `SAXactAdvisoryLock.xact()` |
|---|---|---|
| Direct connection (no pooler) | ✅ Safe | ✅ Safe |
| Session-mode pooler (PgBouncer `pool_mode=session`) | ✅ Safe — one logical connection maps to one physical connection for its lifetime | ✅ Safe |
| **Transaction-mode pooler** (PgBouncer `pool_mode=transaction`, pgcat, Supavisor transaction mode) | ❌ **Unsafe — leaks locks** (see the four-step failure) | ✅ Safe by construction |
| Statement-mode pooler | ❌ Unsafe — same failure, even faster | ✅ Safe — each statement's implicit transaction still ends with the lock released, but a real `xact()` call always wraps a caller-managed transaction so this reduces to the transaction-mode case |

**Rule of thumb:** if you are not certain which pooling mode your deployment
uses, default to `SAXactAdvisoryLock.xact()` — it is safe in every row of
this table, including the ones `SAAdvisoryLock` is unsafe for.

## Usage

```python
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from varco_sa.advisory_lock import SAAdvisoryLock, SAXactAdvisoryLock

engine = create_async_engine("postgresql+asyncpg://...")

# Direct connections / session-mode pooler — SAAdvisoryLock is fine.
lock = SAAdvisoryLock(engine)
handle = await lock.try_acquire("inventory:item_42", ttl=30)
if handle is not None:
    async with handle:
        await reserve_item(42)

# Recommended default — safe under every pooling topology.
xact_lock = SAXactAdvisoryLock()
session_factory = async_sessionmaker(engine, expire_on_commit=False)

async with session_factory() as session:
    async with session.begin():
        async with xact_lock.xact("inventory:item_42", session) as acquired:
            if acquired:
                await reserve_item_via(session, 42)
            else:
                ...  # contended — skip or retry
        # COMMIT here releases the lock automatically — no release() call.
```

`AbstractDistributedLock` DI binding (`varco_sa.di.SAConfiguration`, renamed from
`SAModule` by Plan 049 §D-names):
`AbstractDistributedLock` → `SAAdvisoryLock` remains the **default** binding
so upgrading to a Plan-005-Phase-5 release does not silently change any
app's runtime behaviour. `SAXactAdvisoryLock` is always separately
injectable via `Inject[SAXactAdvisoryLock]`. To make `SAXactAdvisoryLock`
win the `AbstractDistributedLock` binding instead: `sa_advisory_lock` is
declared `@Fallback` (Plan 045), so any non-fallback binding you register —
in any order, before or after `scan()`/`install()` — wins outright:

```python
@Provider(singleton=True)
def my_lock(config: Inject[SAConfig]) -> AbstractDistributedLock:
    return SAXactAdvisoryLock(config.engine)


container.provide(my_lock)          # any order — wins over the default
container.scan("varco_sa", recursive=True)
```

## Why `RedisLock` is not the answer here

`varco_redis.RedisLock` (`SET key NX PX ttl` + a Lua-scripted token-checked
release) is a perfectly good `AbstractDistributedLock` implementation, and
it does not have this pooling failure mode at all — Redis has no concept of
"session-scoped" locks tied to a TCP connection the way PostgreSQL advisory
locks are. **It is out of scope for U-16 specifically because the deployment
this gap was filed against is air-gapped / has no Redis available** — the
whole point of PostgreSQL advisory locks is that they need no additional
infrastructure beyond the database the application already depends on. If
your deployment *does* have Redis available and does not have this
constraint, `RedisLock` remains a fine choice and sidesteps the
session-vs-transaction distinction entirely — but it is a different
trade-off (an additional infrastructure dependency), not a strict
improvement, and this plan does not ask you to add one where none exists
today.

## Pitfalls

| Pitfall | Fix |
|---|---|
| `release()` returns `false` and the lock leaks on the original connection | `SAAdvisoryLock` behind a transaction-mode pooler — switch to `SAXactAdvisoryLock.xact(key, session)` |
| "Lock never seems to release even though I called `release()`" on a previously-fine deployment | A connection pooler was recently switched to transaction mode, or a new pooler was inserted in front of the database | Same fix — `SAXactAdvisoryLock.xact()` |
| `ttl=` passed to `SAXactAdvisoryLock.try_acquire` seems to do nothing | It's not merely unenforced — it's meaningless for a transaction-scoped lock; the transaction's own commit/rollback bounds it. Use `xact()` and keep the wrapping transaction short instead |
| Reaching for `RedisLock` to "fix" the pooling issue | Only relevant if Redis is already available in your deployment — for an air-gapped/no-Redis constraint, `SAXactAdvisoryLock` is the fix that needs no new infrastructure |

| Pitfall | Symptom | Root Cause | Fix |
|---|---|---|---|
| **`release()` returns false and the lock leaks** | A `SAAdvisoryLock` held by process A appears to release successfully but the next process to borrow that pooled connection inherits an already-locked key | Session-level advisory lock behind a transaction-mode connection pooler (PgBouncer `pool_mode=transaction`) — `release()` is routed to a different physical connection than `try_acquire()` used | Use `SAXactAdvisoryLock.xact(key, session)` instead — the lock is released by the caller's own COMMIT/ROLLBACK, so pooling never has a chance to misroute the release |

## Migration

None — Phase 5 adds a new class in the same module; no schema change.
