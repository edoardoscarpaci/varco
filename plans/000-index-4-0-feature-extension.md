# Plan index — 4.0.0 feature extension

**Cycle**: `# 4.0.0 feature extension` (BACKLOG.md, discover 2026-10-07; planned 2026-10-11).

**Version**: 4.0.0 (locked). These plans ride the same major as the DI standardization build
(plans/045–058). Breaking changes are allowed outright. Each breaking plan carries its own
`3-to-4.md` migration row. **The `v4.0.0` tag (Plan 058 Phase F) waits until every plan below has
shipped.**

**Locked scope decisions** (BACKLOG.md §Locked decisions, not to be relitigated):
- ORCH-1 covers the first stage only.
- ACID-3 is merged into ACID-1.
- GDPR-3 (consent) and Arrow/Parquet export are parked.
- RO-14 is pulled into 4.0.0.
- TEST-1 is built first.

**Research briefs backing the set**:
- [018 — transactionally correct endpoints & GDPR frameworks](../design/research/018-transactionally-correct-endpoints-and-gdpr-frameworks.md)
- [019 — streaming, orchestration & test performance](../design/research/019-streaming-orchestration-and-test-performance-2026.md)
- [020 — pytest parallelism & fake clocks](../design/research/020-pytest-parallelism-and-fake-clocks.md) (TEST-1)
- [021 — transactions, conditional requests & streaming](../design/research/021-transactions-conditional-requests-streaming.md) (ACID-1/2, STREAM-1/2). It carries a "Corrected 2026-10-10" banner: uvicorn 0.42.0 does not close ASGI `receive` once the response starts, and Plan 068 relies on that.
- [022 — GDPR classification, export & erasure](../design/research/022-gdpr-classification-export-erasure.md) (GDPR-1/2)

Every planner cited these briefs or `file:line` source. Anything it could not verify is marked
`⚠️ ASSUMPTION` in the plan itself.

## Slices

| # | Plan | BACKLOG rows | Depends on |
|---|---|---|---|
| 059 | [faster-unit-tests](059-faster-unit-tests.md): randomly-order audit, two-phase xdist capped at nproc, settle-time removal, measured before/after | TEST-1 | — (**build first**) |
| 060 | [auto-registered-consumer-dlq](060-auto-registered-consumer-dlq.md): `register_to(default_dlq=)`, applied to retrying handlers only, with three opt-outs | RO-14 | — |
| 061 | [optimistic-concurrency-and-conditional-requests](061-optimistic-concurrency-and-conditional-requests.md): `RowVersionMixin`, `expected_version=`, ETag/If-Match, 412/409/428, `OPTIMISTIC_LOCK_ERRORS` | ACID-2 | — |
| 062 | [ambient-transactions-core-and-sqlalchemy](062-ambient-transactions-core-and-sqlalchemy.md): `varco_core.transaction` (`transaction()`/`@transactional`, propagation, isolation, retry, `on_commit`), SA resource, conformance suite | ACID-1 (slice 1) | 061; 067 (for tests 13(j)/16) |
| 063 | [ambient-transactions-beanie-mongo](063-ambient-transactions-beanie-mongo.md): Beanie `TransactionalResource`, manual sessions, fixes the `transactional=True` no-op, `mongo.transactions_unsupported` | ACID-1 (slice 2) | 062 |
| 064 | [personal-data-classification](064-personal-data-classification.md): `PersonalData` field metadata, `PersonalDataCatalog`, redaction policy, posture collector, `varco gdpr inventory` | GDPR-1 | — |
| 065 | [data-subject-export](065-data-subject-export.md): `@Multibound` `DataSubjectContributor`, export job via `JobQueue.enqueue`, artifact store, admin mount | GDPR-2 (export) | 064, 069a |
| 066 | [data-subject-erasure](066-data-subject-erasure.md): `erase()`, per-step REQUIRES_NEW, crypto-shred last, `holds=` hook, audit tombstone, `DataSubjectErasedEvent`, `varco gdpr erase` | GDPR-2 (erasure) | 065, 069a, 062 |
| 067 | [streaming-reads-and-keyset-pagination](067-streaming-reads-and-keyset-pagination.md): NDJSON codec + `NdjsonResponse`, `ExportMixin`, fixes SA `stream()` (`varco_sa/repository.py:392`), opt-in cursor pagination, fixes the Link-header bug | STREAM-1 | — |
| 068 | [streaming-ndjson-bulk-ingest](068-streaming-ndjson-bulk-ingest.md): `IngestMixin` `POST /ingest`, one UoW per batch, `$error`/`$summary` lines, body-limit exempt | STREAM-2 | 067 (refs 062) |
| 069a | [job-dispatcher-and-enqueue-api](069a-job-dispatcher-and-enqueue-api.md): `JobDispatcher`, three claim/save bugs fixed, `Job.tenant_id`, `current_job()`, `JobSpec`, `JobQueue.enqueue()` | ORCH-1 (part 1), RO-9 | — |
| 069b | [schedule-driver-chaining-and-fan-in](069b-schedule-driver-chaining-and-fan-in.md): `ScheduleDriver`, `JobStatus.BLOCKED`, `enqueue_chain`/`enqueue_group`, cancel, backstop sweep | ORCH-1 (part 2), RO-6 | 069a |

## Build order

```
059 ─► (everything else uses the faster suites)

060                                         (independent, any time)
061 ─► 062 ─► 063
067 ─┴──────► 068                           (062 needs 067 only for two tests)
069a ─► 069b
064 ─┬► 065 ─► 066 ◄── 062
069a ┘
                                     all of the above ─► 058 Phase F (bump + tag v4.0.0)
```

Suggested linear order (one implementer at a time):

**059 → 060 → 061 → 067 → 069a → 062 → 064 → 065 → 063 → 068 → 069b → 066 → 058F**

- **059 first.** Its baseline measurement must be taken before any other plan adds tests. It also
  includes a pytest-randomly order audit, which surfaces hidden order dependence before nine more
  plans add to the suites.
- **060** is small and stands alone. It is breaking, which is why it rides 4.0.0.
- **061 before 062.** 062's retry predicate references `OPTIMISTIC_LOCK_ERRORS` and never redefines
  it, and 062 routes 061's `expected_version` paths through `_open_uow()` unchanged.
- **067 before 062.** 062 keeps `stream()` always detached from the ambient transaction (§D-stream),
  and its tests 13(j)/16 prove that 067's export streams in full under a boundary.
- **069a before 065/066.** Both run through `JobQueue.enqueue()`. 069b is not needed by GDPR.
- **066 last among the code plans.** It needs 062 (a REQUIRES_NEW transaction per step) and 065
  (the contributor seam, the artifact store and the mount barriers, reused verbatim).
- **058 Phase F last.** It runs `scripts/bump.py --set 4.0.0` and tags the release, and the
  migration guide has to collect every `3-to-4.md` row first.

## Cross-plan contracts (fixed during planning; do not drift)

| Producer → consumer | Contract |
|---|---|
| 061 → 062 | `OPTIMISTIC_LOCK_ERRORS` is never retried. A 40001 retry that ends in a version mismatch surfaces as 412. |
| 067 → 062 | `stream()` is always detached. It opens its own read session and never joins an ambient boundary. |
| 062 → 063 | Same `TransactionalResource`/`ResourceTransaction`/`JoinedUnitOfWork` seam. Mongo maps READ_COMMITTED→majority and REPEATABLE_READ→snapshot, and refuses SERIALIZABLE/NESTED. |
| 062 → 068 | Ingest uses one UoW per batch with `transaction=False` at route level (pinned by 068 Step 7). |
| 062 → 066 | Every erasure step runs in its own REQUIRES_NEW transaction, and the crypto-shred is the final step. |
| 069a → 065, 066 | Export and erasure are enqueued through `JobQueue.enqueue(JobSpec(...))`. Neither uses a hand-built `Job`. |
| 064 → 065, 066 | `PersonalDataCatalog` is the single declaration. Export and erasure read it and never keep a parallel field list. |

## Alembic framework revisions

The framework head is **0008** (`0008_schedule_task_name`). **069a claims `0009_job_tenant_id`**,
and **069b claims `0010` (`job_dependencies`)**. No other plan adds a framework revision. If one
gains a revision during build, it takes the next free number in **merge order** and re-points
`down_revision`. The numbers in a plan file are not reservations.

## Snapshot gates

Nearly every plan moves at least one of: `api-check`, `di-catalogue-check`, `settings-check`,
`entry-points-check`, `asyncapi-check`. Regenerate and commit them in the same change (CLAUDE.md
§Snapshot gates). Do not batch regeneration at the end of the cycle, because each plan's `make lint`
must be green on its own.

## Open questions awaiting the user

Every plan chose a default, so all of them can be built as written. These are the defaults most
worth a second look before `/build`:

- **059**: is pytest-randomly permanent? Is "≤ 0.5× runtime" a hard done-gate? Should the plan split if more than 10 order fixes are needed? Is the repo public (which gives 4-vCPU runners)?
- **060**: should the default DLQ cover retrying handlers only? Should a durable bus paired with the in-memory DLQ log WARNING or INFO? The CancelledError bug is filed as RO-15. Is the `WebhookDispatcher._on_trigger` loss accepted as-is?
- **061**: should `If-Match` be honoured on POST? Should the idempotency store keep the race-409? Is `RowVersionMixin` the right name? Should `require_if_match` stay `False` (the chosen default)?
- **062**: is the default 503 + `Retry-After` right? Should retry stay off by default? `OutboxEventProducer` is parked. Should GLOBAL scope never enlist? Should `stream()` always be detached?
- **063**: should `transactional=True` raise at construction? Should the standalone-server finding be INFO? Should directly injected repos never join? Is Motor in `varco_casbin` filed as RO-16?
- **064**: does `DataSubjectRef` count as personal data? Should `kind` default to `"user"`? Should modules auto-contribute? Should the Fideslang crosswalk ship only if its licence verifies?
- **065**: is the NDJSON export format right? Should there be built-in contributors for framework tables? Should the in-memory artifact store be an INFO finding? Should CSV be added?
- **066**: is the audit residual acceptable? Should `holds=`/`key_manager=` be required? Four-eyes confirmation? Should a tombstone failure fail the attempt? Should the event carry the raw subject id? Should dry runs be audited? `--replay` is parked.
- **069a**: should an unknown task release with back-off for up to 24 h? Should `JobQueue` be a scanned `@Fallback`? Should the `callback_url` hook be opt-in?
- **069b**: `BLOCKED` or `WAITING`? Should `Schedule.concurrency` default to `ALLOW`? Should retention coexistence be documentation-only?

## Definition of done (cycle)

- Every plan's own `## Done` checklist is ticked.
- `make lint` (all snapshot gates), `make type-check` and `make test` are green, and
  `make integration-test-clean` is green on the packages each plan touched.
- `technical_docs/migrations/3-to-4.md` has one row per breaking change. CHANGELOG `[4.0.0]` covers
  all twelve plans.
- BACKLOG.md rows are flipped to shipped, and new parks are recorded with their triggers.
- Then Plan 058 Phase F: `scripts/bump.py --set 4.0.0`, tag `v4.0.0`, release.
