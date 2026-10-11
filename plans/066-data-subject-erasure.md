# Plan 066 — Data-subject erasure (Art. 17): `DataSubjectContributor.erase()`, model-declared delete/anonymise strategies, subject-scope crypto-shredding, a resumable `DataSubjectEraser` job, the audit tombstone, the admin routes and `varco gdpr erase` (GDPR-2, part 2 of 2)

Covers the **erasure half** of BACKLOG 4.0.0-feature-extension row **`GDPR-2`** (`BACKLOG.md:60` —
*"`erase(subject)` → crypto-shred scope + delete/anonymise + audit tombstone (Art. 17); runs as a
job, admin mount"*). Ships inside **4.0.0** (locked decision, `BACKLOG.md:31`). The export half is
**Plan 065**; this plan implements exactly the seven items of its §Handoff (`plans/065:769-794`) and
nothing that would redesign 064/065. `GDPR-3` (consent, purposes, lawful basis) stays **parked**
(`BACKLOG.md:34,110`) — whether an Art. 17(1) ground exists is the controller's call, made before an
erasure is enqueued. Isolated in its own plan because it is **irreversible**: it deletes and
overwrites application data and destroys keys. **One plan, not 066a/066b** — 12 steps, the same
shape as 065; the integration tests ride the steps that build what they test.

**Prerequisites (build order):** Plan **064** (`PersonalData`, `DataSubjectRef`, `classify()`,
`EntityClassification`, `PersonalDataCatalog`, inventory, the `varco gdpr` group), Plan **065**
(`varco_core.data_subject`, `DataSubjectContributor`, `EntityDataSubjectContributor`,
`DataSubjectExporter`, `ExportArtifactStore.delete_for_subject()`, `DataSubjectExportModule`,
`mount_data_subject_admin`, `varco gdpr export`), Plan **069a** (`JobSpec`, `JobQueue.enqueue()`,
`Job.tenant_id`, `current_job()`, `JobDispatcher.run_once()`), and Plan **062 Phase A** (the
`varco_core.transaction` public API — `run_in_transaction`, `TransactionOptions`,
`Propagation.REQUIRES_NEW`, `TRANSACTION_RETRY`, `current_transaction`). Plan **061** is needed only
for one assertion (anonymisation bumps `row_version`); that test is written against 061's built name.

**Sources of record read for this plan (2026-10-10):** `BACKLOG.md:21-37,58-60,74,110,145`;
`plans/064-personal-data-classification.md` §Public API (`:116-231`), §D-marker/§D-catalog/§D-encrypt
(`:336-395`), §Handoff (`:447-459`), Risks "Encrypted is not erasable per subject" (`:678-681`), Open
question 2 (`:687-688`); `plans/065-data-subject-export.md` §Public API (`:186-331`), §D-seam
(`:429-451`), §D-tenant (`:542-567`), §D-job (`:582-612`), §D-artifact (`:614-642`), §D-audit
(`:644-664`), §D-mount (`:666-688`), §D-redaction (`:690-700`), §Handoff (`:769-794`), Open question 2
(`:1169-1171`); `plans/069a-job-dispatcher-and-enqueue-api.md` §D-tenant (`:187-205`), §D-context
(`:207-216`), §D-enqueue (`:218-257`), §D-loop at-least-once (`:282-284`); `plans/062-ambient-transactions-core-and-sqlalchemy.md`
§Goal/§Non-goals (`:98-153` — no Beanie seam until 063, job handlers not inside a boundary), §D-api
(`:156-232`) **only**; `plans/061-optimistic-concurrency-and-conditional-requests.md:18,144`
(`update_many_by_query` bumps `row_version`; erasure through the repository invalidates ETags);
`design/research/022-gdpr-classification-export-erasure.md` §4 (`:61-75`), §5 (`:77-89`), Version
notes (`:121-123`), Evidence gaps 1–2 (`:128-129`), Librarian's note (`:136-144`);
`design/research/018-transactionally-correct-endpoints-and-gdpr-frameworks.md` §B (`:59-95`) and its
options table (`:107-108`); `docs/regulatory-posture.md:1-8,92-98`;
`technical_docs/features/crypto-shredding.md:36-111`; `varco_core/varco_core/encryption.py:183`
(`KeyDestroyedError`), `:902-972` (`ScopedEncryptorRegistry`); `varco_core/varco_core/encryption_store.py:789-837`
(`build_scoped_registry` creates a key on miss), `:877-911` (`destroy_scope` — idempotent receipt);
`varco_core/varco_core/mapper.py:100-224` (encryption context = `tenant_id_field` only; `encryptor=None`
is a silent no-op at `:130-132`); `varco_sa/varco_sa/provider.py:158-220` (the DI provider builds every
entity with **no encryptor**, `:168,174`); `varco_beanie/varco_beanie/provider.py:80`;
`varco_core/varco_core/repository.py:225-334` (`delete_many`, `update_many_by_query` bypasses the
mapper, `stream_by_query`); `varco_core/varco_core/service/audit.py:101-187` (`AuditEntry`,
`entry_hash()` field list), `:341-384` (`delete_where` — no entity/subject predicate,
`allow_chain_break`), `:387-440` (`verify_chain`), `:496-564` (`_audit_redactor`), `:699-756`
(`AuditConsumer`); `varco_core/varco_core/redaction/patterns.py:35-104` (`DEFAULT_REDACT_PATTERNS`
pinned; PII "deliberately NEVER a default"; "crypto-shredding defers destruction to a key");
`varco_core/varco_core/event/audit_event.py:60-128`; `varco_core/varco_core/event/producer.py:76-130`;
`varco_core/varco_core/event/dlq.py:449-457`, `varco_core/varco_core/job/base.py:880-887` (retention
predicates — none by subject); `varco_core/varco_core/idempotency/base.py:34-200`,
`idempotency/record.py:20-70`; `varco_core/varco_core/webhook/inbound/replay.py:1-31` (the
"reuse `AbstractIdempotencyStore`, never a new ABC" precedent); `varco_core/varco_core/di/seams.py:89-93`
(idempotency seam, `memory` default); `varco_sa/tests/test_sa_audit_chain.py` (DB-free chained SA
audit over aiosqlite); `varco_core/tests/test_audit_redaction.py:362-…` (hand-built chained entries);
`CHANGELOG.md:19,541,621`; `plans/058` (house format).

> ⚠️ **Seven premises corrected from source before anything was decided.**
> (1) **There is no per-field crypto-shredding of mapped entity fields to orchestrate.** The DI-wired
> providers build every entity with `SAModelFactory(base=base).build(cls)` — **no encryptor**
> (`varco_sa/provider.py:168,174`; Beanie's factory has no encryption support at all,
> `varco_beanie/provider.py:80`). The mapper's encryption `context` is only ever `tenant_id_field`
> (`mapper.py:119-127,187-204`), and `FieldEncryptor.encrypt/decrypt` are sync while per-scope keys
> load async (`encryption_store.py:789-837`). So an `EncryptedHint` field is, at best, tenant-keyed —
> 064 already warned "encrypted ≠ erasable per subject" (`064:678-681`). Crypto-shredding in this plan
> is **per subject scope**, for data the app itself encrypted under `build_scoped_registry(scope)`,
> and runs as one final step (§D-shred). (This also means 065 §D-reads' "encrypted fields arrive
> decrypted" holds only on the hand-built factory path — Open question 9 for the coordinator.)
> (2) **Past audit rows cannot be erased per subject.** `AuditRepository.delete_where()` has no
> entity-id or subject predicate (`service/audit.py:341-349`), and on a chained table it refuses
> without `allow_chain_break=True` (`:362-368`). A per-subject delete needs a new abstract verb
> (CLAUDE.md: never) and breaks the chain; rewriting a row breaks `entry_hash()` (`:155-187`) and
> violates "never redact on the read path" (§D-audit).
> (3) **Plan 057's default audit redaction does not cover personal data.** `AUDIT_REDACTION_POLICY` is
> built on the pinned `DEFAULT_REDACT_PATTERNS` (secrets only, `patterns.py:40-56`); PII patterns are
> "deliberately NEVER a default — an audit trail exists to record PII" (`:87-93`). Past `create`/
> `update` diffs therefore hold personal data, and `entity_id`/`actor_id` are subject identifiers.
> (4) **Brief 022 §4's Art. 17(3) list is not the Regulation's.** It lists "vital interests of another
> person" and omits (c) public health and (d) archiving/research (`022:67-72`). `ErasureGround`
> follows the Regulation text (⚠️ ASSUMPTION, verified at Step 11).
> (5) **"Put beyond use" is not in brief 022 (or 018).** Brief 022 supports the backup argument through
> the EDPB 2025 CEF report (`022:65`) and Evidence gap 2 (`:129`). The phrase is ICO wording
> (⚠️ ASSUMPTION — quoted only if verified at Step 11, otherwise dropped).
> (6) **The two briefs disagree on crypto-shredding's regulatory status.** Brief 018 says EDPB
> Guidelines 02/2025 "explicitly endorse" it (`018:66-67`, sourced from a consultation *response*);
> brief 022 says it is "NOT a blanket formal endorsement — case-by-case acceptance based on
> proportionality" (`022:65`, `:138`), and its 02/2025 link points at the 01/2025 page (`:64`). This
> plan takes brief 022's reading. No doc may say "endorsed".
> (7) **Anonymisation needs no row hydration; deletion does.** `update_many_by_query` is one UPDATE
> that bypasses the mapper (`repository.py:253-292`), so it works even on rows whose ciphertext can no
> longer be decrypted, and after 061 it bumps `row_version` (`061:144`). There is no delete-by-query
> verb, so deletion is `find_by_query(limit)` + `delete_many` batches, which hydrate (decrypt).

## Goal

After this plan, an app that lists `DataSubjectExportModule` (065) can add
`DataSubjectErasureModule(holds=…, key_manager=…)`. Each subject-linked entity already declares on the
model what erasure does to it: `Meta.personal_data_erasure = "delete" | "anonymise"`, and per field
`PersonalData(…, erasure=FieldErasure.NULL | PLACEHOLDER | KEEP)`. An operator previews an erasure
with a mandatory dry run, then executes it by quoting the dry-run job as a confirmation token. The
erasure runs as a `JobQueue` job under one tenant. It asks the app's Art. 17(3) hold hook, then
erases every `DataSubjectContributor`'s data in order, each in its own `REQUIRES_NEW` transaction.
Per-step progress is recorded so a failed job resumes. It then purges the subject's pending export
artifacts and, last, crypto-shreds the subject's key scope. It appends one `"erase"` tombstone to the
audit trail without rewriting any chained row, so `verify_chain()` still returns `True`. It publishes
`DataSubjectErasedEvent` through `AbstractEventProducer` so downstream services can erase their
copies. Every manifest names the locations varco did **not** erase. Exporting a crypto-shredded
dataset reports it as erased instead of failing. No documentation claims GDPR compliance; docs say
varco "supports" or "helps implement" erasure.

## Non-goals

- **Deciding legality.** varco never decides whether Art. 17(1) applies, whether an Art. 17(3)
  exception holds, or whether a request is manifestly unfounded (Art. 12(5)). The hold hook is the
  app's; `ErasureGround` only names the paragraph the app relies on.
- **Erasing or rewriting past audit rows**, and any new `AuditRepository` verb (§D-audit). Per-subject
  encryption of audit diffs ("forgettable payloads") is **parked** with a trigger (§Parked).
- **Field-level crypto-shredding of mapped entity fields** (a `FieldErasure.SHRED`) — premise 1;
  parked (needs async per-subject key resolution in the mapper).
- **Hash / pseudonym "anonymisation".** Pseudonymised data remains personal data (brief 022
  `:122`); no strategy replaces a value with its hash.
- **Erasing framework surfaces in place** — DLQ entries, outbox rows, idempotency records, webhook
  deliveries, job payloads, caches, broker topics, logs, traces, backups. Each is a named
  **residual location** in every manifest with documented handling (§D-surfaces). `RO-8` (DLQ /
  `WebhookDelivery` payload redaction, `BACKLOG.md:74`) stays its own row.
- **Post-restore re-erasure tooling** (`varco gdpr erase --replay`) — §D-backups records the design;
  parked.
- **Art. 19 notification of recipients outside the app.** `DataSubjectErasedEvent` is internal
  plumbing; telling third parties is the controller's job.
- **Self-service erasure routes**, DSAR case management, deadlines — same as 065.
- **A new `@Multibound` seam** (holds are one hook) or a new progress-store ABC (§D-resume).
- **Re-opening 065's decisions** (one seam, repository reads, one tenant per request, artifact store,
  mount barriers) — reused verbatim.
- **Editing `BACKLOG.md`.**

## Design

### Module layout

```
varco_core/varco_core/personal_data/markers.py   + FieldErasure, ErasureAction, ERASED_PLACEHOLDER;
                                                   PersonalData.erasure (defaulted, appended)
varco_core/varco_core/personal_data/catalog.py   + PersonalField.erasure, EntityClassification.erasure /
                                                   .erasure_order (read from Meta by classify())
varco_core/varco_core/personal_data/inventory.py + "erasure" keys (entity, field) — schema v1 unchanged number
varco_core/varco_core/data_subject/
  base.py      + ErasureStepStatus, ErasureOutcome; DataSubjectContributor.erase() (concrete,
                 NOT_SUPPORTED default) and .erasure_order (default 0)
  holds.py     — ErasureGround, ErasureHold, ErasureHoldPolicy                         (new)
  events.py    — DataSubjectErasedEvent, ERASURE_EVENT_CHANNEL                          (new)
  ledger.py    — ErasureLedger (adapter over AbstractIdempotencyStore)                  (new)
  eraser.py    — ERASE_TASK_NAME, SUBJECT_SCOPE_STEP, EXPORT_ARTIFACTS_STEP, RESIDUAL_LOCATIONS,
                 ErasureStatus, ErasureRequest, ErasureManifest, subject_scope(),
                 SubjectScopeResolver, DataSubjectEraser                                (new)
  posture.py   — DataSubjectErasurePostureCollector                                     (new)
  settings.py  + DataSubjectErasureSettings (VarcoSettings, env VARCO_DSR_ERASE_)
  entity.py    + EntityDataSubjectContributor.erase()/.erasure_order/.validate_erasure()/.classification
  exporter.py  ~ KeyDestroyedError → dataset reported erased; ExportManifest.erased_datasets;
                 read-only .contributors/.catalog/.acknowledged_unreached
  module.py    + DataSubjectErasureModule (VarcoModule)
varco_core/varco_core/event/audit_event.py   action Literal gains "erase"
varco_core/varco_core/cli/gdpr.py            + `erase` verb
varco_fastapi/varco_fastapi/data_subject/router.py, mount.py   + erasure routes, eraser= kwarg
```

Import direction is 065's: `data_subject → personal_data, job, tenancy, transaction, encryption
(KeyDestroyedError, lazily), idempotency, event, posture`. `personal_data` never imports
`data_subject` (the markers live in `personal_data/markers.py` so a model module imports nothing
from `data_subject`). `EncryptionKeyManager` is imported under `TYPE_CHECKING` only (it is a
constructor argument, never an injected parameter).

### Flow

```
 operator ─POST {prefix}/erasures {kind,id,dry_run:true}──▶ router (admin_role)
            with tenant_context(t): JobQueue.enqueue(JobSpec(task=eraser.task.payload(kind,id,True,by)))
          ◀─202 Location {prefix}/erasures/{dry_job}
 operator ─GET  {prefix}/erasures/{dry_job}──▶ manifest: per-step counts, holds, retained fields,
                                                residual locations, subject_key   (no raw id)
 operator ─POST {prefix}/erasures {kind,id,dry_run:false,confirm:dry_job}──▶ (admin_role + erase_role)
            verify: erase task ∧ dry run ∧ COMPLETED ∧ same tenant ∧ subject_key match ∧ age ≤ ttl
            JobQueue.enqueue(JobSpec(job_id=uuid5(NS, f"execute:{dry_job}"), task=…payload(kind,id,False,by)))
                                                     ▼ (re-POST → same job: idempotent enqueue)
 worker: JobDispatcher.claim → tenant_context(job.tenant_id) → current_job()
   DataSubjectEraser._run_job → erase(ErasureRequest(subject, dry_run, request_id=job_id, by))
     holds = await holds(subject, tenant)        error → attempt fails (fail closed, nothing erased)
     whole-request hold ─▶ status HELD ─▶ tombstone ─▶ return
     for c in contributors serving kind, sorted (c.erasure_order, c.name):
        held(c)            → HELD
        ledger.get(rid, c) → replay recorded outcome            (execute only)
        run_in_transaction(TransactionOptions(REQUIRES_NEW, read_only=dry_run,
                           retry=TRANSACTION_RETRY), c.erase, subject, dry_run=dry_run)
        ledger.record(rid, outcome)                              (execute, DONE only)
     "varco.export_artifacts": artifact_store.delete_for_subject(subject_key, tenant_id=t)
     "varco.subject_scope"  (LAST): key_manager.destroy_scope(s, actor=by) for s in scopes(subject, t)
                                    auto-HELD if any step is held
     AuditEvent(action="erase", entity_type="DataSubjectErasure", entity_id=rid, diff=…) → "varco.audit"
     DataSubjectErasedEvent(…)                                                    → "varco.data_subject"
     return manifest.to_dict()  → Job.result     (any exception: best-effort "failed" tombstone, re-raise)
```

### Public API

```python
# personal_data/markers.py  (064 module; additive)
class FieldErasure(StrEnum):           # what ANONYMISE does to one classified field
    NULL = "null"                      # default — field must accept None (checked at startup)
    PLACEHOLDER = "placeholder"        # str fields only, never an EncryptedHint field → ERASED_PLACEHOLDER
    KEEP = "keep"                      # left in place; listed in every report as retained
ERASED_PLACEHOLDER: Final = "[erased]"

class ErasureAction(StrEnum):          # what erasure does to a subject-linked row
    DELETE = "delete"
    ANONYMISE = "anonymise"

@dataclass(frozen=True)
class PersonalData:                    # 064 fields unchanged, one appended:
    ...
    erasure: FieldErasure = FieldErasure.NULL     # a plain str is coerced

# personal_data/catalog.py  (additive, appended defaulted fields)
PersonalField.erasure: FieldErasure = FieldErasure.NULL
EntityClassification.erasure: ErasureAction | None = None        # Meta.personal_data_erasure
EntityClassification.erasure_order: int | None = None            # Meta.personal_data_erasure_order

# data_subject/base.py  (additive to 065's seam)
class ErasureStepStatus(StrEnum):
    DONE = "done"                      # everything this step holds for the subject is erased (or,
                                       # under dry_run, would be) — counts may be 0
    NOT_SUPPORTED = "not_supported"    # this step cannot erase — a gap, never success
    HELD = "held"                      # an Art. 17(3) hold applied; nothing touched

@dataclass(frozen=True)
class ErasureOutcome:
    step: str                          # contributor name, EXPORT_ARTIFACTS_STEP or SUBJECT_SCOPE_STEP
    status: ErasureStepStatus
    dry_run: bool
    deleted: int = 0                   # rows/documents/artifacts removed (or that would be)
    anonymised: int = 0                # rows overwritten in place (or that would be)
    destroyed: int = 0                 # key ids tombstoned (subject-scope step)
    retained_fields: tuple[str, ...] = ()   # KEEP + unclassified business fields left on anonymised rows
    reason: str | None = None          # required for NOT_SUPPORTED/HELD; ≤ 300 chars; never PII
    @classmethod
    def not_supported(cls, step: str, reason: str, *, dry_run: bool) -> ErasureOutcome: ...
    def to_dict(self) -> dict[str, Any]: ...

class DataSubjectContributor(ABC):     # 065's @Multibound seam, two concrete members added
    @property
    def erasure_order(self) -> int: ...            # default 0; lower runs first
    async def erase(self, subject: DataSubject, *, dry_run: bool) -> ErasureOutcome:
        """Default: ErasureOutcome.not_supported(self.name, …). Never raises, never DONE.
        Overrides MUST be idempotent (a re-run after success is DONE with zero counts), MUST NOT
        mutate under dry_run, and MUST return outcome.step == self.name."""

# data_subject/holds.py
class ErasureGround(StrEnum):          # Art. 17(3) — named, never judged
    FREEDOM_OF_EXPRESSION = "17(3)(a)"
    LEGAL_OBLIGATION_OR_PUBLIC_TASK = "17(3)(b)"
    PUBLIC_HEALTH = "17(3)(c)"
    ARCHIVING_RESEARCH_STATISTICS = "17(3)(d)"
    LEGAL_CLAIMS = "17(3)(e)"
    OTHER = "other"                    # e.g. "Art. 17(1) not met for this dataset" — reference required

@dataclass(frozen=True)
class ErasureHold:
    ground: ErasureGround
    reference: str                     # 1..200 chars: statute / case / policy id — NEVER personal
                                       # data; recorded verbatim in the audit tombstone
    step: str | None = None            # None = the whole request; else a contributor name or
                                       # SUBJECT_SCOPE_STEP / EXPORT_ARTIFACTS_STEP

ErasureHoldPolicy = Callable[[DataSubject, str | None], Awaitable[Sequence[ErasureHold]]]
                                       # receives current_tenant(); runs inside the job's tenant

# data_subject/events.py
ERASURE_EVENT_CHANNEL: Final = "varco.data_subject"
class DataSubjectErasedEvent(Event):
    __event_type__: ClassVar[str] = "varco.data_subject.erased"
    request_id: UUID
    subject_kind: str
    subject_id: str = Field(repr=False)   # raw — downstream needs it (§D-event); masked in repr
    subject_key: str
    tenant_id: str | None
    status: Literal["complete", "incomplete"]

# data_subject/settings.py
class DataSubjectErasureSettings(VarcoSettings):   # env_prefix="VARCO_DSR_ERASE_"
    max_attempts: int = 3                          # ≥ 1 → JobSpec.max_attempts
    confirmation_ttl: timedelta = timedelta(hours=24)   # > 0 — max age of the confirming dry run
    ledger_ttl: timedelta = timedelta(days=30)     # > 0 — progress-record lifetime
    delete_batch_size: int = 500                   # 1..10_000

# data_subject/ledger.py
class ErasureLedger:
    def __init__(self, store: AbstractIdempotencyStore, *, ttl: timedelta) -> None: ...
    async def get(self, request_id: UUID, step: str) -> ErasureOutcome | None: ...
    async def record(self, request_id: UUID, outcome: ErasureOutcome) -> None: ...   # write-once

# data_subject/eraser.py
ERASE_TASK_NAME: Final = "varco.data_subject.erase"
EXPORT_ARTIFACTS_STEP: Final = "varco.export_artifacts"
SUBJECT_SCOPE_STEP: Final = "varco.subject_scope"
RESIDUAL_LOCATIONS: Final[tuple[str, ...]] = ("audit_trail", "job_payloads", "dead_letters",
    "outbox", "idempotency_records", "webhook_deliveries", "caches", "event_streams",
    "logs_and_traces", "backups")

class ErasureStatus(StrEnum):
    COMPLETE = "complete"      # every step DONE and no acknowledged-unreached entity
    INCOMPLETE = "incomplete"  # ≥ 1 step HELD / NOT_SUPPORTED, or an acknowledged-unreached entity
    HELD = "held"              # a whole-request hold; nothing touched

def subject_scope(subject: DataSubject, *, tenant_id: str | None) -> str: ...
    # "varco.dsr:" + ("" if tenant_id is None else f"tenant={tenant_id}:") + subject_key(subject)
    # — never the raw id (crypto-shredding.md:45-47)
SubjectScopeResolver = Callable[[DataSubject, str | None], Iterable[str]]

@dataclass(frozen=True)
class ErasureRequest:
    subject: DataSubject
    dry_run: bool                          # NO default (the RetentionPolicy.dry_run rule)
    request_id: UUID = field(default_factory=uuid4)
    requested_by: str | None = None

@dataclass(frozen=True)
class ErasureManifest:                     # NO personal data, no raw subject id
    request_id: UUID; subject_kind: str; subject_key: str; tenant_id: str | None
    dry_run: bool; status: ErasureStatus; started_at: datetime; finished_at: datetime
    steps: tuple[ErasureOutcome, ...]      # run order: contributors, artifacts, subject scope
    holds: tuple[ErasureHold, ...]
    acknowledged_unreached: tuple[str, ...]
    residual_locations: tuple[str, ...]    # always RESIDUAL_LOCATIONS
    crypto_shred_configured: bool
    audit_recorded: bool; event_published: bool
    def to_dict(self) -> dict[str, Any]: ...   # schema_version 1, kind "varco.data-subject-erasure-manifest"

class DataSubjectEraser:
    def __init__(self, exporter: DataSubjectExporter, *,
                 holds: ErasureHoldPolicy | None,                 # required; None is a statement
                 key_manager: EncryptionKeyManager | None,        # required; None is a statement
                 subject_scopes: SubjectScopeResolver | None = None,  # None → (subject_scope(...),)
                 ledger: AbstractIdempotencyStore | None = None,  # None → every attempt re-runs all steps
                 settings: DataSubjectErasureSettings | None = None,
                 producer: AbstractEventProducer | None = None,
                 clock: Callable[[], datetime] | None = None) -> None: ...   # validation runs here
    @property
    def task(self) -> VarcoTask: ...               # name ERASE_TASK_NAME, fn = self._run_job
    @property
    def settings(self) -> DataSubjectErasureSettings: ...
    def subject_kinds(self) -> tuple[str, ...]: ...
    def gaps(self) -> tuple[str, ...]: ...         # default-erase() contributors + acknowledged-unreached names
    async def erase(self, request: ErasureRequest) -> ErasureManifest: ...

# data_subject/module.py
class DataSubjectErasureModule(VarcoModule):
    name = "data_subject_erasure"; phase = Phase.SCHEDULERS; depends_on = (DataSubjectExportModule,)
    def __init__(self, *, holds: ErasureHoldPolicy | None, key_manager: EncryptionKeyManager | None,
                 subject_scopes: SubjectScopeResolver | None = None,
                 settings: DataSubjectErasureSettings | None = None,
                 enabled: bool | None = None) -> None: ...   # `enabled` exactly as DataSubjectExportModule
    @property
    def eraser(self) -> DataSubjectEraser: ...   # after async_init

# varco_fastapi/data_subject/mount.py  (065 signature, three keywords appended)
def mount_data_subject_admin(app, *, exporter, job_queue, job_store, server_auth,
        acknowledge_bundled_admin=False, admin_role="data-subject-admin",
        cross_tenant_role="cross-tenant-admin", prefix="/data-subject-requests",
        eraser: DataSubjectEraser | None = None,
        acknowledge_irreversible_erasure: bool = False,
        erase_role: str = "data-subject-eraser") -> None: ...
```

065 additions (additive, all needed by this plan): `DataSubjectExporter.contributors`, `.catalog`,
`.acknowledged_unreached` read-only properties; `EntityDataSubjectContributor.classification`
read-only property; `ExportManifest.erased_datasets: tuple[str, ...] = ()` appended (+ `to_dict` key);
an export-document dataset carries `"erased": true` after `records` **only** when its contributor hit
`KeyDestroyedError` (schema stays v1 — both plans ship unreleased in 4.0.0).

Top-level `varco_core.__all__` gains (lazily: `_LAZY` + `TYPE_CHECKING` + `__all__`):
`DataSubjectEraser`, `ErasureAction`, `FieldErasure` (the two markers sit beside `PersonalData` on
models). Everything else is reachable from `varco_core.data_subject` / `varco_core.personal_data`.

Usage (the README snippet):

```python
@register
@dataclass
class Customer(DomainModel):
    pk: Annotated[UUID, PrimaryKey(PKStrategy.UUID_AUTO), DataSubjectRef("customer")] = pk_field()
    email: Annotated[str | None, PersonalData(PersonalDataCategory.CONTACT)]
    class Meta:
        table = "customers"
        personal_data_erasure = "delete"           # pk-linked → runs last (order 100)

@register
@dataclass
class Order(DomainModel):
    customer_id: Annotated[UUID | None, DataSubjectRef("customer")]   # nulled on anonymise
    shipping_address: Annotated[str, PersonalData(PersonalDataCategory.CONTACT,
                                                  erasure=FieldErasure.PLACEHOLDER)]
    shipping_country: Annotated[str, PersonalData(PersonalDataCategory.LOCATION,
                                                  erasure=FieldErasure.KEEP)]
    total: Decimal
    class Meta:
        table = "orders"
        personal_data_erasure = "anonymise"        # keep the invoice row, unlink the person

async def holds(subject: DataSubject, tenant_id: str | None) -> list[ErasureHold]:
    if await billing.has_open_dispute(subject.id):
        return [ErasureHold(ErasureGround.LEGAL_CLAIMS, "dispute policy DP-7", step=None)]
    return []

app = create_varco_app(container, modules=[
    *default_modules(),
    DataSubjectExportModule(catalog=catalog, controller=controller),
    DataSubjectErasureModule(holds=holds, key_manager=key_manager),   # or holds=None, key_manager=None
    JobDispatcherModule(),
])
mount_data_subject_admin(app, exporter=..., job_queue=..., job_store=..., server_auth=my_auth,
                         acknowledge_bundled_admin=True, eraser=container.get(DataSubjectEraser),
                         acknowledge_irreversible_erasure=True)
```

```
varco gdpr erase --target myapp.dsr:eraser --kind customer --id - --tenant acme --dry-run
varco gdpr erase --target myapp.dsr:eraser --kind customer --id - --tenant acme --execute --confirm <subject_key>
```

### DESIGN blocks

**§D-strategy — declared on the model (entity action in `Meta`, field strategy in `PersonalData`),
executed by the contributor; live rows are deleted or anonymised, never crypto-shredded (064 Q2
resolved)**
- ✅ One declaration on the model, beside 064's `PersonalData` and `Meta.personal_data_retention`.
  The inventory carries it (`"erasure"` keys), so the Art. 30 data map shows each entity's erasure
  method. This is the prior-art shape: leukeleu-django-gdpr's data map records "deletion methods"
  per model (brief 022 §6 `:95`).
- ✅ The row decision is per entity (`ErasureAction`), because deleting a row is a row fact. The
  field decision is per field (`FieldErasure`), because only fields are overwritten.
  `EntityDataSubjectContributor.erase` executes the declaration. A hand-written contributor owns its
  strategy in its own `erase()`.
- ✅ **No default for the entity action.** With `DataSubjectErasureModule` listed, a catalog
  contributor whose entity lacks `Meta.personal_data_erasure` fails startup (`ValueError` naming the
  entity and both remedies: declare it, or `exclude=` it from 065's module and hand-write a
  contributor). It is destructive and irreversible, so the source must state it (the `dry_run`
  no-default rule). `FieldErasure` does default to `NULL`, the least-retaining option, validated
  against the type.
- ✅ **DELETE** = `find_by_query(subject filter, limit=delete_batch_size)` → `delete_many` → repeat
  until a short batch. It is a hard delete, because the repository bypasses `SoftDeleteService`, and
  soft-deleted rows are deleted too (065 includes them in exports for the same reason).
- ✅ **ANONYMISE** = for each link field `f` of the erased kind: `update_many_by_query(eq(f, id)
  [∧ eq(tenant_field, t)], {f: None, **replacements})`. `replacements` are the classified fields
  with `NULL`→`None` and `PLACEHOLDER`→`ERASED_PLACEHOLDER`; `KEEP` fields are left alone. One
  statement per link field, with no hydration, so it works on undecryptable rows (premise 7). It
  bumps `row_version` (061), so outstanding ETags get 412. **Nulling the link fields is
  mandatory.** A row still carrying the subject's id is pseudonymised, not anonymised (brief 022
  `:75` WP29 05/2014 irreversibility standard; `:122` EDPB 01/2025). Re-running is idempotent
  because the rows no longer match the subject.
- ✅ Startup validation, all naming `Entity.field`: ANONYMISE on an entity linked through `pk`
  (`pk` cannot be nulled) → error ("use delete"). ANONYMISE with a non-Optional link field → error.
  `NULL` on a non-Optional classified field of an ANONYMISE entity → error. `PLACEHOLDER` on a
  non-`str` field or an `EncryptedHint` field → error: plaintext would be written into a ciphertext
  column. `KEEP` on a link field → error.
- ✅ Every outcome lists `retained_fields`: the `KEEP` fields plus every unclassified business field
  left on anonymised rows (excluding `pk`, the tenant field, `_`-prefixed attributes and other kinds'
  link fields). The dry run shows exactly what survives.
- ❌ "Anonymise" names the mechanics, not a legal result. Whether the remaining row is anonymous in
  the WP29 05/2014 sense (KEEP fields, free text, small cohorts) is the controller's assessment. The
  doc says so in those words.
- ❌ A multi-link entity (`Message.sender_id` + `recipient_id`, both `"user"`) is anonymised or
  deleted for **every** row the subject appears in, including the other party's received copy. That
  is the controller's declaration (Art. 15(4)-style judgement). Pitfall row; a hand-written
  contributor is the escape hatch.
- Rejected — **strategy per contributor / module argument** (`erasure={Order: "anonymise"}`): ❌ a
  second parallel list keyed by class, invisible to the inventory, the exact problem 064 removed.
  ✅ No 064 change.
- Rejected — **`FieldErasure.SHRED`** (per-subject mapper keys): premise 1 — the standard providers
  build no encryptor, the mapper's context is the tenant field, and the encryptor is sync. Parked.
- Rejected — **`FieldErasure.HASH`**: pseudonymisation, not erasure (brief 022 `:122`).

**§D-order — `(erasure_order, name)`, subject entity last, crypto-shred after everything**
- ✅ Catalog default `erasure_order`: `100` when `pk` is a subject link, else `0`. Override with
  `Meta.personal_data_erasure_order: int`. So `Order` (references the customer) is anonymised before
  `Customer` is deleted, without the app touching FK constraints.
- ✅ The artifact purge (`EXPORT_ARTIFACTS_STEP`) runs after every contributor, so an export that
  finished during the run is still purged. The subject-scope shred (`SUBJECT_SCOPE_STEP`) runs
  **last**: destroying the key first would make every later step that hydrates rows (DELETE) raise
  `KeyDestroyedError`, and a crash mid-run would leave data unreadable but un-deleted, with the
  evidence gone. Shred-last means that if the key is destroyed, every earlier step has completed.
- ❌ FK chains among non-subject entities (`OrderLine` → `Order`, both linked, both DELETE) need
  `Meta.personal_data_erasure_order` or an `ON DELETE CASCADE`. Otherwise the step fails loudly
  (FK violation → attempt fails), never silently. A dry run cannot detect it. Pitfall row.
- Rejected — **derive order from `ForeignKey` metadata (topological sort)**: ✅ automatic.
  ❌ Cycles, cross-backend references and FKs to non-catalog tables make it partial magic. An explicit
  int is greppable. Revisit on demand.

**§D-resume — idempotent steps + a write-once progress ledger over `AbstractIdempotencyStore`;
`dry_run` has no default anywhere**
- ✅ **Correctness comes from idempotency, not the ledger.** Every step's contract is "re-running
  after success is a no-op". DELETE finds nothing, ANONYMISE matches nothing (links nulled),
  `destroy_scope` returns `()` (`encryption_store.py:877-896`), and `delete_for_subject` returns 0. A
  resumed attempt and a full re-run with a new request id are therefore both safe.
- ✅ **The ledger keeps the evidence.** Without it, attempt 2 re-runs step 1 and reports `deleted=0`,
  losing what attempt 1 actually did. `ErasureLedger.record()` writes a DONE outcome under
  `f"varco.dsr.erase:{request_id}:{step}"` (no PII: a UUID and a contributor name) as `reserve` →
  `complete(IdempotencyRecord(status=200, body=outcome JSON, headers={}, fingerprint=key))`.
  `get()` replays it on the next attempt, and the manifest shows attempt 1's counts.
- ✅ It reuses a shipped ABC with four backends (memory/Redis/SA/Beanie, `di/seams.py:89-93`) —
  the exact precedent of `WebhookReplayGuard` (`webhook/inbound/replay.py:8-16`: "no new ABC, no new
  backend"). It uses `reserve`/`complete` as a write-once "done" marker, never as a lock. Mutual
  exclusion is 069a's single-owner lease. `REPLAY`/`IN_FLIGHT` on `record()` → `get()` the stored
  outcome (a crashed `record()` between `reserve` and `complete` → `release` + one retry, else a
  WARNING and continue; correctness does not depend on it).
- ✅ Only DONE outcomes are recorded. HELD/NOT_SUPPORTED are re-evaluated on every attempt, so a
  lifted hold takes effect on the next run.
- ✅ A step that raises → the step's transaction rolls back (§D-tx) → a best-effort tombstone with
  `status: "failed"` and `failed_step` is emitted (errors in this path are logged and swallowed) →
  `DataSubjectRequestError(f"erasure step {name!r} failed: {type(exc).__name__}") from exc` →
  069a's retry with `max_attempts` (default 3). `Job.error` is sanitised (065 §D-redaction).
- ✅ **`dry_run` is mandatory and explicit at every entry point:** `ErasureRequest.dry_run` and
  `DataSubjectContributor.erase(*, dry_run)` have no default, the route body field is required, and
  the CLI needs one of `--dry-run`/`--execute`. A dry run mutates nothing, writes no ledger, purges
  nothing, destroys nothing and emits no audit or subject-erased event. It runs each step in a
  `read_only=True` boundary and returns the same manifest shape with `dry_run: true`. The artifact
  step reports `deleted=0` with reason "not counted under dry_run" (the 065 ABC has no count verb,
  and none is added).
- ❌ A crash between a step's commit and its `record()` loses that step's counts (it re-runs as 0).
  Accepted; the window is one write.
- ❌ Ledger records expire after `ledger_ttl` (default 30 days) and the in-memory default is per
  process. Either way the next attempt re-runs everything, which is safe, with zero counts. Pitfall row.
- Rejected — **a new `ErasureProgressStore` ABC + SA/Beanie tables + a migration**: ✅ a purpose-built
  schema with no TTL. ❌ Three backends, a framework table and conformance rows for a write-once
  marker the shipped store already provides.
- Rejected — **no progress record at all**: ✅ simplest. ❌ The evidence loss above.
- Rejected — **one 069b job per step (a chain)**: ❌ n copies of the raw subject id in job payloads,
  a hard dependency on 069b, and no verb exists to resume a failed chain.

**§D-holds — Art. 17(3) is one app-supplied hook; varco names grounds, never judges them**
- ✅ `holds: ErasureHoldPolicy | None` is a **required** keyword on the module and the eraser.
  `holds=None` is the app's explicit statement "no exception applies anywhere", in source and
  greppable. A hook is called once per attempt (and per dry run) with `(subject, current_tenant())`.
- ✅ Granularity: a whole-request hold → status `HELD`, nothing touched, tombstone recorded (evidence
  of a refusal), no subject-erased event. A per-step hold → that step is `HELD`. **The subject-scope
  step is automatically `HELD` whenever any step is held**, because a held dataset may be encrypted
  under the subject scope, and destroying the scope would destroy held data.
- ✅ Fail closed: the hook raising, or returning a hold naming an unknown step, fails the attempt
  before any step runs.
- ✅ `reference` is recorded verbatim in the tombstone. Its docstring and the doc say it must be a
  statute, case or policy identifier, never personal data. It is length-capped at 200.
- ❌ Field-level holds ("keep the invoice amount") are not holds; they are `FieldErasure.KEEP`
  declarations. Holds are runtime, per-request legal judgements; strategies are static.
- Rejected — **a `@Multibound` hold seam**: ❌ a seventh seam for what is one policy decision per
  request. An app composing several sources writes one function. ✅ plugin-friendly.
- Rejected — **a default `holds=None`**: ❌ an app would erase invoices it is legally obliged to keep
  without ever having been asked.

**§D-audit — append one `"erase"` tombstone; never touch a chained row; past diffs are a named,
documented residual**
- ✅ `AuditEvent.action` widens to `Literal["create", "update", "delete", "export", "erase"]` (the SA
  column is `String(16)`). The eraser produces `AuditEvent(entity_type="DataSubjectErasure",
  entity_id=str(request_id), action="erase", actor_id=requested_by, tenant_id=current_tenant(),
  diff={subject_kind, subject_key, status, dry_run: false, steps: {name: {status, deleted,
  anonymised, destroyed}}, holds: [{ground, reference, step}], acknowledged_unreached, failed_step,
  channel: "job"|"cli", attempt})` on `"varco.audit"` through `AbstractEventProducer`. `AuditConsumer`
  appends it like any entry: on a chained repository it gets the next `seq`/`prev_hash`, so
  `verify_chain()` stays `True` (Step 5 and Step 6 prove it on a fake, on aiosqlite, on Postgres and
  on Mongo).
- ✅ **Chosen option for personal data already inside past audit diffs: documented retention**
  (brief 022 §5 option 3, with option 2 as the app's forward opt-in). Erasure never deletes,
  redacts or re-encrypts a past audit row:
  - **Deleting per subject** needs a predicate `delete_where()` does not have (a new abstract verb,
    forbidden) and breaks the chain by construction (premise 2).
  - **Redacting/re-encrypting in place** changes `entry_hash()` inputs (`diff`, `entity_id`,
    `actor_id`, `:172-185`) → `HashMismatch` on every later row. "Never redact on the audit read
    path" (CLAUDE.md) forbids the same thing from the other side.
  - The controller keeps the trail under its own Art. 17(3)(b)/(e) assessment (brief 022 `:73`:
    retention "CAN be kept if needed for legal claims") and prunes it with the shipped
    `AuditRetentionTarget` (`delete_where(older_than=…)`, `allow_chain_break` on chained tables).
  - **Forward opt-in:** an app that does not want classified values recorded in future diffs sets
    `_audit_redactor = PolicyRedactor(personal_data_redaction_policy(catalog,
    base=AUDIT_REDACTION_POLICY))` (064). That trades away the audit record of *what* changed, which
    `patterns.py:87-93` says an audit trail exists to keep. The doc presents both choices; varco's
    default is unchanged.
- ✅ The tombstone carries `subject_key`, not the raw id: evidence for accountability, a join key to
  065's export records, and the post-restore re-erasure key (§D-backups).
- ❌ **Residual gap, stated in the doc and in every manifest (`residual_locations` includes
  `"audit_trail"`):** after a COMPLETE erasure, the subject's personal data that past `create`/
  `update` diffs recorded, the subject entity's pk in `entity_id`, and the subject's id in `actor_id`
  where they acted all remain readable in the audit store until audit retention removes them. varco
  does not erase them. `subject_key` is an unsalted SHA-256 of `kind:id` and is dictionary-reversible
  for low-entropy ids such as emails, so the tombstone itself holds pseudonymous personal data
  (brief 022 `:122`). Both are controller decisions; the doc names them.
- Rejected — **per-subject encryption of audit diff values now ("forgettable payloads", brief 022 §5
  option 1, Verraes)**: ✅ It is the shape `patterns.py:91-93` points at, and the chain would hash
  ciphertext and survive shredding. ❌ It is its own M–L feature: the subject of every audited entity,
  multi-subject rows, an async key fetch on the write path, read-path decryption, and
  `entity_id`/`actor_id` still in plaintext. Brief 022 Evidence gap 1 (`:128`): no regulator guidance
  on hash chains + shredding. **Parked** with a trigger.
- Rejected — **a tombstone per step**: ❌ N rows per request. One row per attempt, with the per-step
  map, is enough evidence (failed attempts included).

**§D-surfaces — what `erase()` covers now, and the named residuals**

| Surface | Covered now? | How / why not |
|---|---|---|
| Subject-linked entity rows (catalog) | ✅ | DELETE / ANONYMISE (§D-strategy) |
| Indirect / external data | ✅ if a hand-written contributor implements `erase()` | otherwise NOT_SUPPORTED → INCOMPLETE |
| 065 export artifacts | ✅ | `ExportArtifactStore.delete_for_subject(subject_key, tenant_id=)` (`EXPORT_ARTIFACTS_STEP`) |
| App data encrypted under the subject scope | ✅ | `destroy_scope` (`SUBJECT_SCOPE_STEP`, §D-shred) |
| Audit trail | ❌ residual | §D-audit — retention + tombstone |
| Job payloads (`Job.task_payload` of export/erasure jobs hold the raw id) | ❌ residual | no subject predicate on `delete_where` (`job/base.py:880-887`); doc recipe: a short `JobRetentionTarget` policy |
| DLQ entries | ❌ residual | `delete_where` has no subject predicate (`event/dlq.py:449-457`); `RO-8` is the redaction row; DLQ retention |
| Outbox rows | ❌ residual | transient (relayed); an unpublished row is never deleted (CLAUDE.md retention rule) |
| Idempotency records | ❌ residual | TTL-bounded responses; `delete_expired` |
| Webhook deliveries | ❌ residual | `RO-8`; delivery retention |
| Caches | ❌ residual | entries expire by TTL; recipe: invalidate in a `DataSubjectErasedEvent` listener |
| Broker topics / event streams | ❌ residual | broker retention; downstream consumers erase on `DataSubjectErasedEvent` |
| Logs / traces | ❌ residual | 065 §D-redaction keeps raw ids out of varco's own messages |
| Backups | ❌ residual | §D-backups |

- ✅ Every manifest carries `residual_locations = RESIDUAL_LOCATIONS`, so no report can be read as
  "everything is gone".
- ✅ A catalog contributor never goes through `AsyncService`, so no per-entity `update` audit diff is
  emitted for an anonymisation. That diff would copy the before-values, the very personal data being
  erased, into the audit trail. The tombstone is the record.
- ❌ Projections or read models fed by entity events do not see erasure deletes. They must subscribe
  to `DataSubjectErasedEvent` (Pitfall row).
- Rejected — **built-in contributors for framework tables** (065 Open question 2): every one needs a
  subject predicate its ABC lacks (a new abstract method). Left as recipes and residuals.

**§D-shred — the subject scope is crypto-shredded last; an export of a shredded dataset reports
"erased"**
- ✅ `SUBJECT_SCOPE_STEP` calls `key_manager.destroy_scope(scope, actor=requested_by)` for each scope
  from `subject_scopes(subject, tenant)`. The default is `(subject_scope(subject, tenant_id=t),)`.
  `destroyed` is the total number of kids tombstoned; a re-run gives 0. `subject_scope()` is the
  convention varco ships, so app code that encrypts per-subject payloads
  (`await manager.build_scoped_registry(subject_scope(subject, tenant_id=current_tenant()))`) and the
  eraser agree. It embeds `subject_key`, never the raw id (`crypto-shredding.md:45-47`). An app with
  its own convention passes `subject_scopes=`.
- ✅ `key_manager` is a **required** keyword. `None` is the explicit statement "this app encrypts
  nothing per subject": the step is omitted (it does not make the status INCOMPLETE),
  `crypto_shred_configured=false` is in every manifest, and an INFO finding is raised (§D-posture).
- ✅ Crypto-shredding is the complement for copies varco cannot rewrite in place (app payloads,
  blobs, the backups of both), never the mechanism for live rows. That keeps varco's live-row
  erasure off the contested part of the regulatory picture: regulators accept crypto-shredding case
  by case, on a proportionality argument (brief 022 §4 `:65`, Librarian's note `:138`; contrast brief
  018 `:66-67`, premise 6).
- ✅ **065 hand-off item 5:** `DataSubjectExporter.build()` checks a contributor failure for a
  `KeyDestroyedError` in the exception or its `__cause__`/`__context__` chain (depth ≤ 10; imported
  inside the function). If found, the dataset's records are discarded, the dataset carries
  `"erased": true`, the manifest lists it in `erased_datasets`, the export audit diff includes
  `erased_datasets`, and the export **completes**. Any other exception keeps 065's behaviour (job
  FAILED). Rendering "erased" rather than "corrupt" is the crypto-shredding.md Pitfall
  (`crypto-shredding.md:141`) applied.
- ❌ The dataset is all-or-nothing: the mapper raises mid-stream (`repository.py:295`) and the
  iterator cannot resume. On the hand-built factory path with per-subject mapper keys, a multi-subject
  entity (a received message from an erased sender) loses its readable rows from the export too.
  Pitfall row; the remedy is a hand-written contributor that decrypts per record.
- ❌ After `destroy_scope`, any writer that calls `build_scoped_registry(scope)` again silently gets a
  **fresh** key (`encryption_store.py:825-830`). Data written for the subject after erasure is
  readable under the new key. Pitfall row ("stop writing for the subject before erasing it").

**§D-backups — varco cannot reach backups; it makes them re-erasable and keeps shredded ciphertext
unreadable**
- ✅ Subject-scoped ciphertext inside any backup becomes unreadable once the scope is destroyed,
  **provided key-store backups do not outlive the erasure window**. That is the operator obligation
  `crypto-shredding.md:98-104` already states verbatim; the erasure doc links it and does not
  restate it.
- ✅ For plaintext rows in database backups, the doc's procedure is: do not restore into production
  without re-applying erasures completed since the backup. The tombstones' `subject_key` makes that
  possible without keeping raw ids. For each restored subject-entity row, compute
  `subject_key(kind, id)` and re-erase the ids whose key appears in an `"erase"` tombstone newer than
  the backup. A `varco gdpr erase --replay` tool is **parked**.
- ✅ Brief 022 §4 (`:65`): backup deletion was the most common erasure failure in the EDPB 2025 CEF
  report, and backups are acceptable for longer "if inaccessible OR encrypted with deleted key".
  The doc cites it as the brief reports it and does not quote any number of days as a rule (Evidence
  gap 2, `:129`).
- ❌ The ICO's "put beyond use" phrasing appears only if verified (premise 5).

**§D-event — `DataSubjectErasedEvent` through `AbstractEventProducer`; the job row is the outbox**
- ✅ Published on `ERASURE_EVENT_CHANNEL` after the tombstone, for every non-dry-run attempt whose
  status is COMPLETE or INCOMPLETE (not HELD). `status` tells downstream whether the upstream was
  partial. Each downstream service applies its own holds. The eraser holds a producer, never the bus
  (CLAUDE.md event rule).
- ✅ It carries the **raw `subject_id`**, because a downstream erasure trigger without the identifier
  is useless. `Field(repr=False)` keeps it out of `repr()`, and the doc tells operators to give the
  `varco.data_subject` channel/topic a short broker retention and to use surrogate ids (UUIDs, not
  emails) as `DataSubject.id`.
- ✅ **Publish or tombstone failure fails the attempt** → job retry → steps replay from the ledger →
  re-publish (at-least-once; consumers dedupe on `request_id`). This deliberately differs from 065
  §D-audit's swallow-and-flag. An export that loses its audit record still delivered the subject's
  right; an erasure whose notification or evidence is lost is incomplete. The durable job row plays
  the role of the transactional outbox (CLAUDE.md: never drop an event after commit).
- ✅ `producer=None` (no bus wired) → `audit_recorded=false`, `event_published=false`, INFO finding
  (§D-posture). An explicit configuration, not a failure.
- ❌ The raw id now also lives in the broker for the channel's retention. Pitfall row.

**§D-confirm — three barriers at the mount, plus a dry run as the two-step token**
- ✅ Mount level (the `mount_*` acknowledgement precedent, `BACKLOG.md:182`): `eraser=` given without
  `acknowledge_irreversible_erasure=True` → `ValueError`. That acknowledgement is separate from 065's
  `acknowledge_bundled_admin`, because the risk is different. All of 065's barriers still apply
  (`server_auth` required, `admin_guard`, double-mount `WeakSet`, never a `create_varco_app` keyword
  or env var).
- ✅ Role level: executing requires `admin_role` **and** `erase_role` (default `"data-subject-eraser"`),
  so the people who can export are not automatically the people who can erase. A dry run needs only
  `admin_role`.
- ✅ Request level, two steps: an execute body must carry `confirm=<dry-run job id>`. The router
  loads that job and requires all of the following: it is an `ERASE_TASK_NAME` job with
  `dry_run=True`; it is COMPLETED; its `Job.tenant_id` equals the resolved tenant; its manifest's
  `subject_key` equals `subject_key(kind, id)` from the body; and it finished within
  `settings.confirmation_ttl`. The operator re-types the subject id and must have produced the
  preview. The execute job id is `uuid5(ERASURE_CONFIRM_NAMESPACE, f"execute:{confirm}")`, so a
  re-POST returns the same job (069a §D-enqueue: an existing id is returned unchanged). One preview
  executes at most once.
- ✅ The token is the job store itself: no new token store and no new secret.
- ❌ No four-eyes rule (confirmer ≠ dry-run requester). Open question 3.
- Rejected — **an acknowledgement header only** (`X-Confirm-Erasure: true`): ✅ simpler. ❌ Nothing
  forces the preview, and a scripted client sets the header unconditionally.

**§D-tx — each step in its own `REQUIRES_NEW` boundary via `varco_core.transaction` (062 public API
only)**
- ✅ `await run_in_transaction(TransactionOptions(propagation=Propagation.REQUIRES_NEW,
  read_only=dry_run, retry=TRANSACTION_RETRY), contributor.erase, subject, dry_run=dry_run)`. The
  boundary owns its transaction, so the step's writes are **committed before** its ledger record is
  written. That holds even when the caller (an app script, a request boundary from 062's
  `TransactionModule`) already has an ambient boundary, which `REQUIRES_NEW` suspends. An outer
  rollback can never undo a step that the ledger reports as done.
- ✅ Whole-step retry on transient serialization failures comes free from `TRANSACTION_RETRY`; it is
  safe because steps are idempotent. 062 honours `retry` only on an owning boundary, which this is.
- ✅ The catalog contributor calls `uow_provider.make_uow()`. With 062 built it joins the step's root
  lazily (SA). A provider without the seam (Beanie until 063) yields a standalone UoW that commits on
  its own exit, which is still exactly one unit per step. A `GlobalUoWProvider` UoW is DETACHED and
  commits on its own (062 §D-service).
- ❌ A hand-written contributor that writes to **two** transactional resources inside one step hits
  062's one-resource rule (`TransactionConfigurationError`). Remedy in the doc: split it into two
  contributors, or use `open_detached_uow()` for the second resource.
- ❌ The artifact purge and the scope shred are not DB-UoW steps. No boundary is opened; both are
  idempotent.

**§D-tenant — 065's rules, with "skip, don't fail" for erasure**
- ✅ One tenant per erasure, from `current_tenant()` via `Job.tenant_id` (069a); the CLI enters
  `tenant_context(--tenant)`. The tenant appears in the scope string and the artifact purge.
- ✅ With no active tenant, a TENANT-partitioned entity that has the tenant field → that step is
  `NOT_SUPPORTED` ("tenant-partitioned; run the erasure inside each tenant"), never an unfiltered
  mutation. Under a tenant, a **GLOBAL** entity → `NOT_SUPPORTED` ("shared across tenants; run a
  platform-level erasure"), because one tenant's request must not mutate shared rows. Both make the
  status INCOMPLETE and name the remedy. (065's export raises in the first case; for an irreversible
  write, skip-and-report is the fail-closed form.)
- ❌ A multi-tenant app erasing a platform user runs one erasure per tenant plus one platform-level
  erasure. Documented.

**§D-posture — one collector, contributed by the module, INFO only**
- ✅ `DataSubjectErasurePostureCollector` (name `"data_subject_erasure"`) is contributed by
  `DataSubjectErasureModule._register` through a `@Provider(singleton=True, returns=PostureCollector)`
  factory that declares `eraser: DataSubjectEraser`. That is the 055 §D-collector shape: the findings
  exist only when the module is listed, and nothing is edited in `varco_fastapi/posture.py`. Findings:

  | `check` | Fires when | Severity |
  |---|---|---|
  | `data_subject.erasure_gaps` | `eraser.gaps()` non-empty (default-`erase()` contributors, acknowledged-unreached entities) — every erasure will be INCOMPLETE | INFO |
  | `data_subject.erasure_no_crypto_shred` | `key_manager=None` | INFO |
  | `data_subject.erasure_no_producer` | no `AbstractEventProducer` resolvable — no tombstone, no event | INFO |
  | `data_subject.erasure_not_assessed` | `collect()` hit an exception (the "must not raise" contract) | NOT_ASSESSED |

- ✅ **Never WARN.** A WARN blocks a production start under the derived `enforce="refuse"`
  (`security-posture.md:263`, CLAUDE.md FLIP-2). Each condition is a legitimate configuration (an app
  with no per-subject encryption, an export-only external source), and the 064 §D-encrypt precedent
  applies. A Step 7 test proves a production start with all three findings succeeds.

**§D-module — a sibling `VarcoModule` that builds the eraser from the exporter**
- ✅ `depends_on=(DataSubjectExportModule,)` is an edge to one specific module, not a whole-phase
  follower. Erasure stays opt-in: an app can run exports without enabling an irreversible operation.
- ✅ The eraser is built **from the `DataSubjectExporter`** (`contributors`, `catalog`,
  `acknowledged_unreached`, `artifact_store`). Export and erasure therefore walk the same sources
  through one coverage check, which is 065 §D-seam's argument enforced by construction.
- ✅ `_register`: `bind_settings(container, DataSubjectErasureSettings, instance=settings)`. A
  non-fallback `@Provider(singleton=True)` → `DataSubjectEraser` declaring `exporter:
  DataSubjectExporter`, `settings: DataSubjectErasureSettings`, `ledger: AbstractIdempotencyStore`
  and an optional `AbstractEventProducer` (065's optional-dependency mechanism). It closes over
  `holds`/`key_manager`/`subject_scopes`. Plus the posture contribution. `async_init` runs
  `await container.aget(DataSubjectEraser)`, so validation fails the lifespan rather than a DSAR.
  `start` registers `ERASE_TASK_NAME`, with the same absent/same/different check as 065 §D-job.
  Parameter annotations are importable at module scope (common-pitfalls: quoted `@Provider`
  annotations).
- ✅ At `async_init` the module logs one INFO line listing `eraser.gaps()` (names only).
- ❌ Two modules to list. Intended.

**§D-cli — `varco gdpr erase`, inline, two-step by `--confirm <subject_key>`**
- ✅ `varco gdpr erase --target module:callable --kind K --id ID|- [--tenant T]
  (--dry-run | --execute --confirm SUBJECT_KEY) [--requested-by NAME] [--request-id UUID]
  [-o FILE|-] [--path DIR]`. `--target` (env fallback `VARCO_GDPR_ERASE_TARGET`) resolves to a
  `DataSubjectEraser` or a sync/async zero-arg factory, the 065 §D-cli shape. Exactly one mode is
  required (an argparse required mutually-exclusive group). `--execute` needs `--confirm` equal to
  `subject_key(kind, id)`, which the dry run prints. It proves the operator previewed this subject,
  and it is scriptable. `--request-id` resumes through the ledger.
- ✅ It runs `erase()` under `tenant_context(--tenant)` with `channel="cli"`, writes the manifest (no
  personal data) to `-o` or stdout, and `--id -` reads stdin.
- ✅ Exit codes: 0 COMPLETE or a successful dry run; **3** INCOMPLETE or HELD (scripts must be able
  to tell); 1 `DataSubjectRequestError`/runtime failure (no id on stderr); 2 usage (no mode,
  `--execute` without `--confirm`, a confirm mismatch, an unresolvable target, an unknown kind).

### Alternatives considered

- **Two seams (export + a separate erase seam)**: rejected (065 §D-seam). ✅ export-only sources could
  not be mistaken for erasable. ❌ two registries for one fact.
- **`erase` as an `@abstractmethod`**: rejected. ❌ It breaks every 065-era contributor. The concrete
  NOT_SUPPORTED default gives the same visibility (INCOMPLETE + posture).
- **Erase through `AsyncService.delete/update`**: rejected. ❌ Per-caller authorization, soft delete
  instead of hard delete, and the per-entity audit diff would copy the erased values into the
  audit trail (§D-surfaces). ✅ domain events for projections (replaced by `DataSubjectErasedEvent`).
- **Strategy per contributor / module map**: rejected (§D-strategy).
- **Field-level crypto-shred (`FieldErasure.SHRED`)**: rejected now, parked (premise 1).
- **Hash-as-anonymisation**: rejected (brief 022 `:122`).
- **New progress-store ABC / no ledger / 069b chain**: rejected (§D-resume).
- **Per-subject audit deletion, in-place audit redaction**: rejected. **Forgettable audit payloads**:
  parked (§D-audit).
- **`@Multibound` holds / default `holds=None`**: rejected (§D-holds).
- **Swallow tombstone/event failures like 065's export**: rejected (§D-event).
- **Header-only confirmation / a token store**: rejected (§D-confirm).
- **`enable_data_subject_erasure()` verb**: rejected. Same reasons as 065 §D-module (startup
  validation and task registration need a module).
- **Fail on a platform-level erasure that meets tenant entities (065's export rule)**: rejected for
  erasure (§D-tenant).

### Parked (with triggers)

| Item | Trigger |
|---|---|
| Forgettable audit payloads (per-subject encryption of audit diff values; chain over ciphertext) | A controller who can justify neither audit retention under 17(3) nor redaction, **and** a regulator statement on hash chains + shredding (brief 022 Evidence gap 1) |
| `FieldErasure.SHRED` (per-subject mapper keys) | A real request **and** an async per-subject key resolver in the mapper; also needs the DI providers to pass an encryptor at all (Open question 9) |
| `varco gdpr erase --replay` (post-restore re-erasure from tombstones) | The first operator restore that needs it |
| Built-in framework-table contributors (DLQ, jobs, webhook deliveries) | A subject predicate on those ABCs, designed in its own plan (`RO-8` is adjacent) |
| Four-eyes confirmation | A user request (Open question 3) |

## Steps

1. [ ] **Tests (failing) — markers, classification, seam additions, value types, settings, event,
   audit action.**
   - New `varco_core/tests/test_personal_data_erasure_markers.py`:
     - `FieldErasure`/`ErasureAction` values; `PersonalData(...).erasure` defaults to `NULL`; `"keep"`
       is coerced; `"shred"` → `ValueError` listing the legal values; markers are still frozen and
       hashable.
     - `classify()` reads `Meta.personal_data_erasure` (`"delete"` → `ErasureAction.DELETE`; absent →
       `None`; `"purge"` → `ValueError` naming the entity) and `Meta.personal_data_erasure_order`
       (int kept; `"1"`/`True` → `TypeError`).
     - `PersonalField.erasure` mirrors the marker.
   - Update `varco_core/tests/test_personal_data_inventory.py`: entity objects gain `"erasure"`
     (`null` when undeclared) after `declared_retention`, field objects gain `"erasure"` after
     `encrypted`, and the output is still deterministic.
   - New `varco_core/tests/test_data_subject_erasure_types.py`:
     - A 065-style contributor without `erase` still instantiates. Its `erase(subject,
       dry_run=True)` returns NOT_SUPPORTED with `step == name`, `dry_run=True`, all counts 0, and a
       reason. It never raises, even when `subject` is a valid `DataSubject` of a kind the
       contributor does not serve. `erasure_order` defaults to 0. `erase()` without `dry_run` →
       `TypeError`.
     - `ErasureOutcome`: a negative count → `ValueError`; NOT_SUPPORTED/HELD without a reason →
       `ValueError`; a reason over 300 chars → `ValueError`; `to_dict` round-trips.
     - `ErasureHold`: an empty/whitespace or over-200-char `reference` → `ValueError`.
       `ErasureGround` values are exactly the five `17(3)(x)` strings plus `"other"`.
     - `DataSubjectErasureSettings`: defaults (3, 24 h, 30 days, 500); bounds → validation errors;
       env prefix `VARCO_DSR_ERASE_` (monkeypatch).
     - `subject_scope()`: the exact format with and without a tenant; it never contains the raw id
       (an email id); it is deterministic.
     - `DataSubjectErasedEvent`: `__event_type__`, and `repr()` does not contain `subject_id`.
     - `AuditEvent(action="erase")` validates and `action="purge"` fails.
2. [ ] **Implement**:
   - `varco_core/varco_core/personal_data/markers.py`: `FieldErasure`, `ErasureAction`,
     `ERASED_PLACEHOLDER`, and `PersonalData.erasure`. The module docstring gains the §D-strategy
     DESIGN block.
   - `personal_data/catalog.py`: `PersonalField.erasure`, `EntityClassification.erasure` /
     `.erasure_order`, and the Meta reads in `classify()`, appended defaulted fields only.
   - `personal_data/inventory.py`: the two keys; `INVENTORY_SCHEMA_VERSION` stays 1, with a comment
     that 064 and 066 ship together in 4.0.0.
   - `varco_core/varco_core/data_subject/base.py`: `ErasureStepStatus`, `ErasureOutcome`, the
     concrete `erase()`/`erasure_order`. The ABC docstring's "RESERVED" note becomes the contract
     (idempotent, no mutation under dry_run, `step == name`).
   - New `holds.py` and `events.py`; `DataSubjectErasureSettings` in `settings.py`.
   - `varco_core/varco_core/event/audit_event.py:115` → `Literal["create","update","delete","export","erase"]`,
     with the docstrings at `audit_event.py:69` and `service/audit.py:121,137` updated.
   - Re-export the new names from `varco_core/varco_core/data_subject/__init__.py` and
     `personal_data/__init__.py`. Full `Args/Returns/Raises/Edge cases/Async safety` docstrings,
     `from __future__ import annotations`, frozen dataclasses.

   Step 1 green.
3. [ ] **Tests (failing) — the catalog contributor's `erase()`.**
   - Extend `varco_core/tests/_data_subject_fakes.py` (065): the fake repository implements `count`,
     `delete_many`, and `update_many_by_query` (records `(node, update)`, applies to its list, returns
     the matched count), and `find_by_query` honours `limit`.
   - New `varco_core/tests/test_data_subject_entity_erase.py`:
     - **DELETE**: every linked row is removed across several batches (`delete_batch_size=2`, 5
       rows); rows of other subjects are untouched; soft-deleted rows are deleted too; a second call
       is DONE with `deleted=0`.
     - **ANONYMISE**: one `update_many_by_query` per link field of the kind. The update nulls the
       link field and applies `NULL`/`PLACEHOLDER`; `KEEP` fields are absent from the update dict.
       `retained_fields` lists the KEEP plus unclassified business fields, sorted. A second call is
       DONE with `anonymised=0`. A multi-link entity anonymises rows matched by either link. Another
       kind's link field is not touched.
     - **dry_run**: counts equal what a real run then does; no `delete_many`/`update_many_by_query`
       call is made.
     - **tenant**: with a tenant, every query/update carries the tenant filter. With no tenant on a
       TENANT entity that has the field → NOT_SUPPORTED with the "inside each tenant" reason and no
       repository call. A GLOBAL entity under a tenant → NOT_SUPPORTED.
     - **order**: `erasure_order` is 100 when linked via `pk`, 0 otherwise, and `Meta`'s value wins.
     - **`validate_erasure()`**, each case → `ValueError` naming `Entity.field` (or the entity):
       undeclared action; ANONYMISE + pk link; ANONYMISE + non-Optional link field; `NULL` on a
       non-Optional field; `PLACEHOLDER` on an `int` field; `PLACEHOLDER` on an `EncryptedHint` field;
       `KEEP` on a link field. A DELETE entity ignores field strategies.
   - New `varco_sa/tests/test_data_subject_erase_sa_sqlite.py` (aiosqlite, the 065 sqlite test's
     setup): a real `SQLAlchemyRepositoryProvider`; `Customer` (DELETE, pk-linked) plus `Order`
     (ANONYMISE, nullable `customer_id`, an FK to customers). Run the contributors in `erasure_order`.
     Assert the order is anonymised before the customer is deleted (no FK error with SQLite FK
     enforcement on: `PRAGMA foreign_keys=ON`), the order row survives with NULLs/placeholder, the
     customer row is gone, and 065's export for the subject afterwards yields empty datasets.
4. [ ] **Implement** `EntityDataSubjectContributor.erase()`, `.erasure_order`, `.validate_erasure()`
   and `.classification` in `varco_core/varco_core/data_subject/entity.py`, reusing 065's query
   construction (subject OR-node, id coercion, tenant fail-closed). DESIGN blocks: §D-strategy,
   §D-order, §D-tenant. Optional-ness comes from `typing.get_type_hints(include_extras=True)` with
   `Annotated` stripped; `None in get_args(...)`. Step 3 green.
5. [ ] **Tests (failing) — ledger, eraser, crypto-shred round trip, export of a shredded dataset,
   audit chain, task.**
   - `varco_core/tests/test_data_subject_ledger.py` (an `InMemoryIdempotencyStore`):
     - `record` then `get` round-trips an outcome; a second `record` is a no-op and does not
       overwrite.
     - The key contains the request id and step but no subject data.
     - An expired TTL → `get` returns `None`.
     - A stuck reservation (reserve without complete) → `record` releases and succeeds.
   - `varco_core/tests/test_data_subject_eraser.py` (fake contributors that record calls and
     `current_transaction()`; a recording producer; `InMemoryExportArtifactStore`; an
     `EncryptionKeyManager` over `InMemoryEncryptionKeyStore` — read `encryption_store.py:470-500`
     for its name first):
     - **construction**: `holds`/`key_manager` omitted → `TypeError`. A catalog contributor that
       fails `validate_erasure()` → `ValueError`. `gaps()` lists default-erase contributors and
       acknowledged-unreached names.
     - **order**: steps run by `(erasure_order, name)`, then `EXPORT_ARTIFACTS_STEP`, then
       `SUBJECT_SCOPE_STEP` last.
     - **transactions**: inside each `erase()`, `current_transaction()` is a ROOT frame, a new one per
       step. With an outer `transaction()` open around `eraser.erase(...)`, the step frame is still
       ROOT (REQUIRES_NEW), and an outer rollback after the call does not undo a fake contributor's
       committed write (use 062's `InMemoryTransactionalResource`).
     - **holds**: a whole-request hold → HELD, no contributor called, a tombstone with
       `status: "held"`, no event. A per-step hold → that step HELD, the others DONE, the scope step
       auto-HELD, status INCOMPLETE. A hook raising → `DataSubjectRequestError` and no step called.
       An unknown step name → `DataSubjectRequestError`. `holds=None` → no call.
     - **status**: all DONE → COMPLETE; one NOT_SUPPORTED → INCOMPLETE; an acknowledged-unreached
       entity → INCOMPLETE.
     - **resume**: attempt 1, where contributor B raises after A is DONE → a `"failed"` tombstone
       naming B, then `DataSubjectRequestError` with a sanitised message and `__cause__`. Attempt 2
       with the same `request_id` → A is not called again and its attempt-1 counts appear in the
       manifest; B runs; COMPLETE. With `ledger=None`, attempt 2 calls A again.
     - **idempotent re-run**: a new request id after COMPLETE → COMPLETE, every count 0,
       `destroyed=0`.
     - **dry_run**: no mutation in the fakes, no ledger write, no `delete_for_subject`, no
       `destroy_scope`, no producer call; the manifest has `dry_run: true`; frames are `read_only`.
     - **artifacts**: `delete_for_subject` is called with `subject_key` and the tenant; another
       subject's artifact survives.
     - **scope**: `destroy_scope(subject_scope(...), actor=requested_by)`; `destroyed` equals the
       number of kids; a custom `subject_scopes=` resolver is honoured; `key_manager=None` → no
       scope step, `crypto_shred_configured=False`, status unaffected.
     - **audit/event**: one `AuditEvent` on `"varco.audit"` with `action="erase"` and the §D-audit
       diff keys, then one `DataSubjectErasedEvent` on `ERASURE_EVENT_CHANNEL` carrying the raw id.
       The diff, manifest and `to_dict()` contain no raw id and no record value. `producer=None` →
       flags false, COMPLETE. A raising producer → the attempt fails, and the retry replays the
       steps and re-publishes.
     - **caplog**: across a full erasure, no raw subject id and no record value in any message.
   - **crypto-shred round trip** (same file): a hand-written contributor stores one payload encrypted
     with `await manager.build_scoped_registry(subject_scope(s, tenant_id=None))`; its `export`
     decrypts and its `erase` returns DONE without touching the payload. An export before the
     erasure contains the plaintext. Run the erasure. The export afterwards **completes**: that
     dataset has `"erased": true` and `records: []`, `manifest.erased_datasets` names it, the
     catalog datasets are empty, and the key-store entry for the scope has `destroyed_at` set.
     Exporting a *different* subject still decrypts (R-045 isolation).
   - Update `varco_core/tests/test_data_subject_exporter.py` (065): a `KeyDestroyedError` wrapped two
     levels deep in `__cause__` → erased; any other exception still FAILS the export.
   - **audit chain**, new `varco_core/tests/test_data_subject_erasure_audit_chain.py`:
     - Use an in-test chained fake repository (assign `seq`/`prev_hash` the way
       `test_audit_redaction.py:362-…` does), an `AuditConsumer` on `InMemoryEventBus`, and an
       `AuditLogMixin` fake service writing create/update rows for the subject.
     - Snapshot each entry's `entry_hash()`, run the erasure, list again.
     - `AuditRepository.verify_chain(entries) is True`; the last entry has `action == "erase"`; every
       pre-existing entry's hash is unchanged.
     - New `varco_sa/tests/test_data_subject_erasure_audit_sqlite.py`: the same over
       `SAAuditRepository(hash_chain=True)` on aiosqlite (the `test_sa_audit_chain.py` setup).
   - `varco_core/tests/test_data_subject_erase_task.py` (enter the job scope like 069a's
     `test_job_context.py`):
     - `eraser.task.name == ERASE_TASK_NAME`; the payload round-trips through
       `TaskPayload.to_dict`/`from_dict`.
     - Under a job scope with `tenant_context("t1")`, `request_id == current_job().job_id`, the
       tenant reaches the contributors and the artifact purge, and the handler returns a
       JSON-serialisable `manifest.to_dict()`.
     - Outside a job scope → `DataSubjectRequestError`.
6. [ ] **Implement**:
   - `varco_core/varco_core/data_subject/ledger.py` (§D-resume DESIGN block, citing
     `webhook/inbound/replay.py` as precedent).
   - `eraser.py` (DESIGN blocks §D-order, §D-resume, §D-holds, §D-audit, §D-surfaces, §D-shred,
     §D-event, §D-tx; constants; `subject_scope()`; `ErasureManifest.to_dict()` deterministic key
     order).
   - In `exporter.py` (065): the `KeyDestroyedError` branch, `erased_datasets`, and the three
     read-only properties.

   Step 5 green. **Then the integration tests** (`pytestmark = pytest.mark.integration`, per-run
   `uuid4().hex[:8]` table/collection names, the shared session containers):
   - New `varco_sa/tests/test_data_subject_erasure_integration.py` (`postgres_url`):
     - Customer/Order/Comment under SHARED with a `tenant_id` column. Erase in tenant `t1` → only
       `t1` rows are affected.
     - FK order holds (real Postgres FK).
     - On a versioned entity, anonymisation bumps `row_version` (061).
     - 1 000 linked rows are deleted in batches.
     - `SAIdempotencyStore` as the ledger: a resumed run replays.
     - `SAAuditRepository(hash_chain=True)` → `verify_chain` is `True` after the erasure.
     - `SAEncryptionKeyStore` → `destroy_scope` tombstones the scope's rows.
   - New `varco_beanie/tests/test_data_subject_erasure_integration.py` (`mongo_url`): the same minus
     FKs, `row_version` if 061 versions Beanie documents, with `BeanieIdempotencyStore`,
     `BeanieAuditRepository(hash_chain=True)` and `BeanieEncryptionKeyStore`.

   Run with `-m integration`.
7. [ ] **Tests (failing) — module, posture, DI scan guard, end to end on the dispatcher.**
   - New `varco_core/tests/test_data_subject_erasure_module.py`:
     - The module requires `holds=` and `key_manager=` (`TypeError` when omitted). `depends_on`
       contains `DataSubjectExportModule`.
     - Settings come through `bind_settings` (both the env and the `instance=` paths).
     - `async_init` builds the eraser. An undeclared catalog entity makes `async_init` raise with
       both remedies in the message.
     - `start()` registers `ERASE_TASK_NAME`; a second module with another eraser → `ValueError`; no
       `TaskRegistry` → INFO and no error.
     - The `PostureCollector` contribution is non-fallback with `priority == 0`.
     - INFO gap line at `async_init`.
     - **End to end**: `InMemoryJobStore` + `JobQueue` + `JobDispatcher.run_once()` under
       `tenant_context("t1")`. A dry-run job → COMPLETED, its manifest in `Job.result`, nothing
       mutated. An execute job → COMPLETED, rows gone, `Job.tenant_id == "t1"`.
   - New `varco_core/tests/test_data_subject_erasure_posture.py`:
     - Each finding id fires on its condition with severity INFO.
     - An exception inside `collect()` → one NOT_ASSESSED `data_subject.erasure_not_assessed`.
   - New `varco_fastapi/tests/test_posture_data_subject_erasure.py`: with
     `VARCO_SECURITY_ENV=production` (monkeypatch, per test) and the derived `enforce="refuse"`, a
     `SecurityPostureLifecycle` holding a collector that reports all three INFO findings **starts**.
     Follow the existing `varco_fastapi/tests/test_posture*.py` construction and read one first.
   - Update `varco_core/tests/test_data_subject_no_di_side_effect.py` (065): no providify marker on
     any new object; `container.scan("varco_core", recursive=True)` binds no `DataSubjectEraser` and
     contributes no `DataSubjectErasurePostureCollector`.
   - Update `varco_core/tests/test_lazy_init.py` for the three new top-level names.
8. [ ] **Implement** `varco_core/varco_core/data_subject/posture.py` (§D-posture) and the
   `DataSubjectErasureModule` in `module.py` (§D-module DESIGN block; `DataSubjectEraser`/
   `DataSubjectErasureSettings`/`AbstractIdempotencyStore`/`DataSubjectExporter` imported at module
   scope for provider-annotation resolution). Add `DataSubjectEraser`, `ErasureAction` and
   `FieldErasure` to `varco_core/varco_core/__init__.py` (`_LAZY`, the `TYPE_CHECKING` block,
   `__all__`). Step 7 green.
9. [ ] **Tests (failing) — admin erasure routes + CLI.**
   - Extend `varco_fastapi/tests/test_data_subject_admin.py` (065) with the following.
     - **Mount guards**: `eraser=` without `acknowledge_irreversible_erasure=True` → `ValueError`.
       No `eraser` → no `/erasures` routes (404).
     - **Auth**: anonymous → 401/403 before 422.
     - **Dry runs**: a dry run with `admin_role` → 202, `Location`, and a PENDING job with
       `ERASE_TASK_NAME` and `dry_run=True` in its payload, `Job.tenant_id` the caller's, and
       `max_attempts` from settings.
     - **Body validation**: a missing `dry_run` → 422; extra keys → 422; an unknown kind → 422.
     - **Execute**: without `erase_role` → 403. Without `confirm` → 422.
     - **Confirm checks**: `confirm` that is an unknown job, a non-erase job, an execute job or a
       cross-tenant job → 404. A not-COMPLETED dry run → 409. A different subject id → 409. Older
       than `confirmation_ttl` (injectable clock) → 410.
     - **Valid execute**: 202 with the deterministic job id; a re-POST returns the same id and no
       second job is stored.
     - **GET `/erasures/{id}`**: an unknown or cross-tenant id → 404; COMPLETED → the manifest;
       FAILED → the sanitised error.
     - **End to end** through `JobDispatcher.run_once()`: dry run → GET → execute → GET.
     - Every response has `Cache-Control: no-store`.
   - New `varco_core/tests/test_cli_gdpr_erase.py` (call `main([...])`; target fixtures in a
     `tmp_path` package added via `--path`):
     - Instance, sync-factory and async-factory targets.
     - A dry run prints the manifest with `subject_key` and exits 0.
     - Exit 2: `--execute` without `--confirm`; `--execute --confirm <wrong>`; no mode.
     - `--execute --confirm <right>` → exit 0 and `channel="cli"` in the tombstone.
     - INCOMPLETE → 3; HELD → 3; a failing step → 1 with no id on stderr.
     - `--id -` reads stdin; `--tenant` enters `tenant_context`; `--request-id` resumes through the
       ledger; `-o` writes a `0o600` file and refuses to overwrite without `--force`.
     - The `VARCO_GDPR_ERASE_TARGET` fallback works.
10. [ ] **Implement** the erasure routes in `varco_fastapi/varco_fastapi/data_subject/router.py`. The
    body model is `extra="forbid"` with `subject_id` ≤ 512 and `dry_run: bool` with no default. Add
    a `DataSubjectErasureStatus` response model. Reuse 065's `admin_guard`/`resolve_ctx`/
    `allow_cross_tenant` and the §D-tenant mapping. Add `ERASURE_CONFIRM_NAMESPACE` as a fixed
    `UUID` constant and a §D-confirm DESIGN block. Add the three keywords in `mount.py`. Add the
    `erase` verb to `varco_core/varco_core/cli/gdpr.py` (§D-cli; heavy imports inside `_run`).
    Step 9 green.
11. [ ] **Docs (same change — never a follow-up).**
    - **`technical_docs/features/data-subject-requests.md`**: replace 065's `## Erasure (Art. 17)`
      placeholder with:
      - the "position, not legal advice / no claim of GDPR compliance" sentence, in
        `docs/regulatory-posture.md`'s voice;
      - what erasure does and does not do;
      - the strategy declarations and a worked `Customer`/`Order` example;
      - "anonymise is mechanics, not a legal result" (WP29 05/2014 as reported by brief 022 `:75`);
      - ordering and FKs; dry run → confirm → execute (routes table, CLI);
      - holds and the Art. 17(3) grounds table, quoting the Regulation's (a)–(e) **after verifying
        them against EUR-Lex 32016R0679 Art. 17(3)**, and noting brief 022's divergent list;
      - the audit decision and its residual gap, quoted from §D-audit, with both app choices
        (retention, the redaction opt-in);
      - the residual-locations table (§D-surfaces) with each one's handling;
      - crypto-shredding: `subject_scope()`, encrypting a payload under it, why it runs last, and that
        acceptance is reported as case-by-case/proportionality (brief 022 §4) — **never
        "endorsed"**;
      - backups (§D-backups; the post-restore procedure; link `crypto-shredding.md`'s operator
        obligation; "put beyond use" only if verified against ICO guidance);
      - `DataSubjectErasedEvent` and its consumers (cache invalidation and projection recipes);
      - transactions (§D-tx);
      - tenancy (§D-tenant);
      - the posture findings;
      - a **Pitfalls** table:
        - an undeclared entity blocks startup;
        - multi-link rows erase the other party's copy;
        - FK chains need `erasure_order`/cascade;
        - unclassified free text survives anonymisation;
        - `KEEP` fields and re-identification;
        - the subject id stays in job payloads (`JobRetentionTarget`), the broker (channel retention)
          and the audit trail;
        - `subject_key` is reversible for low-entropy ids (use surrogate ids);
        - writing for the subject after erasure gets a fresh key;
        - the ledger TTL / in-memory ledger per process;
        - an export running concurrently with an erasure;
        - projections fed by entity events;
        - a two-resource hand-written contributor (062);
        - a platform-level erasure in a multi-tenant app;
        - per-subject mapper keys make a whole dataset "erased";
        - `--execute` in scripts needs `--confirm`.
    - **`technical_docs/features/crypto-shredding.md`**: a "Data-subject erasure" section:
      `subject_scope()` is the shipped convention, the eraser destroys it last, and a cross-link.
      Plus one **Pitfalls** row: "`EncryptedHint` fields are tenant-keyed at best (mapper context =
      `tenant_id_field`), so they are not crypto-shreddable per subject".
    - **`technical_docs/features/database-auditing.md`**: the `"erase"` action, the tombstone diff
      shape, "erasure never rewrites a chained row", the residual, and a cross-link.
    - **One row or line in each of**: `technical_docs/features/security-posture.md` (severity table,
      the four ids); `admin-surface-tenancy.md` (erasure routes: tenant-bound, `erase_role`,
      by-id 404); `retention-and-purge.md` (a DSR job-payload retention recipe);
      `transactions.md` (erasure steps use `REQUIRES_NEW`).
    - **`docs/regulatory-posture.md`**: one paragraph under 064's GDPR section. Erasure "helps you
      implement" Art. 17 requests. It is not legal advice and claims no compliance. Name the
      residuals.
    - **`personal-data-classification.md`** (064): the `erasure=` field and the two `Meta` keys.
    - **`README.md`**: a "Data-subject requests (erasure)" section with the §Design usage snippet,
      the two CLI lines and a link; plus a TOC entry.
    - **`ARCHITECTURE.md`**: new `data_subject/` files, the erasure types in 065's type-hierarchy
      block, and `FieldErasure`/`ErasureAction` beside `PersonalData`.
    - **`CLAUDE.md`**: extend 065's `### Data-subject requests` section with ≤ 8 rule lines, ending at
      the doc link:
      - (a) `dry_run` never has a default, and executing via the mount needs a completed dry run as
        `confirm` + `erase_role` + `acknowledge_irreversible_erasure=True`;
      - (b) erasure strategy is declared on the model (`Meta.personal_data_erasure`,
        `PersonalData(erasure=)`) — never a module map, never hash-as-anonymisation;
      - (c) erasure never rewrites/deletes an audit row — one `"erase"` tombstone; never add a
        per-subject audit verb;
      - (d) varco never decides Art. 17(3) — `holds=` is required, `None` is a statement;
      - (e) crypto-shred = the subject scope (`subject_scope()`), destroyed last, never a raw id in a
        scope;
      - (f) each step runs in `REQUIRES_NEW`; steps are idempotent; progress goes through
        `ErasureLedger` over `AbstractIdempotencyStore` — never a new ledger ABC;
      - (g) `DataSubjectErasedEvent` goes through `AbstractEventProducer`, never the bus;
      - (h) `data_subject.erasure_*` findings are INFO.

      Plus a Decision-Tree branch: *"Erasing a data subject's data (Art. 17)? →
      `DataSubjectErasureModule` + `mount_data_subject_admin(eraser=…)` + `varco gdpr erase`; never
      a hand-written delete loop"*.
    - **`CHANGELOG.md` `[4.0.0]`**: a `### Added` group `(Plan 066, GDPR-2)` and a `### Changed`
      bullet (the `AuditEvent.action` value `"erase"`; a crypto-shredded dataset in an export is
      reported as erased).
    - **`technical_docs/migrations/3-to-4.md`**: extend 065's audit-action row with `"erase"`.
12. [ ] **Snapshot gates + full verification.**
    - `uv run python scripts/api_surface.py` → commit
      `design/api-freeze-and-standards/measurements/api-surface.{json,md}`. It adds three top-level
      names and changes the `mount_data_subject_admin` signature if `varco_fastapi` exports it at
      top level.
    - `make entry-points` → commit (`DataSubjectErasureModule`, the `mount_data_subject_admin`
      signature).
    - `make settings-reference` → commit (`DataSubjectErasureSettings`).
    - `make di-catalogue-check` is expected **unchanged**, because no bootstrap or default module
      lists the new module. Any movement → stop and find out why.
    - `make asyncapi-check` is unchanged, because the example app registers no consumer of the new
      event.
    - `uv run python scripts/import_budget.py --check --warn-only` → expect no delta (lazy names).
    - Then run the §Verification block.

## Edge cases

- Subject with no data anywhere → every step DONE with zero counts → COMPLETE; tombstone + event
  still emitted (evidence that the request was handled).
- Whole-request hold → HELD; no step, no purge, no shred; tombstone `status: "held"`; no event; CLI
  exit 3.
- Per-step hold on `Order` → `Order` HELD, `Customer` erased, subject scope auto-HELD → INCOMPLETE.
- Hold hook raises / names an unknown step → attempt fails before any mutation.
- Hand-written contributor without `erase()` → NOT_SUPPORTED → INCOMPLETE; posture INFO; never
  reported DONE.
- Contributor returns `outcome.step != name` or a dry-run outcome on a real run → `DataSubjectRequestError`
  (contract violation), attempt fails.
- Catalog entity without `Meta.personal_data_erasure` → module startup `ValueError` (both remedies
  named).
- ANONYMISE entity linked through `pk` / with a non-nullable link → startup `ValueError` ("use delete" /
  "make the link nullable").
- `PLACEHOLDER` on an encrypted or non-`str` field → startup `ValueError`.
- `Order` FK → `Customer`: `Order` (order 0) anonymised before `Customer` (order 100) deleted.
- `OrderLine` → `Order`, both DELETE, no cascade, no order override → FK violation → step fails,
  attempt fails loudly, failed tombstone names the step.
- Step fails on attempt 1, succeeds on 2 → ledger replays earlier steps with attempt-1 counts.
- Ledger record expired / in-memory ledger on another pod → steps re-run idempotently with zero
  counts; status unchanged.
- Same subject erased twice (new request ids) → second run COMPLETE, all zero, `destroyed=0`.
- Execute re-POSTed with the same `confirm` → same job id, no second job.
- `confirm` older than 24 h → 410; dry run of a different subject → 409; not a dry run → 404.
- Platform-level erasure (no tenant) meets TENANT entities → those steps NOT_SUPPORTED → INCOMPLETE.
- Tenant erasure meets a GLOBAL entity → NOT_SUPPORTED → INCOMPLETE.
- `key_manager=None` → no scope step; `crypto_shred_configured=false`; COMPLETE possible; INFO.
- Scope never had a key → `destroy_scope` returns `()` → DONE `destroyed=0`.
- Export running concurrently and finishing after the purge → its artifact survives (race;
  documented; re-run erasure purges it).
- Export after erasure: catalog datasets empty; a scope-encrypted dataset `"erased": true`, export
  COMPLETED.
- `KeyDestroyedError` during a catalog DELETE (rows encrypted under a scope destroyed earlier) →
  step fails; remedy (ANONYMISE never hydrates) in the Pitfalls.
- Producer `None` → COMPLETE, `audit_recorded=false`, `event_published=false`, INFO.
- Producer raises → attempt fails, retry replays, event re-published (at-least-once).
- Called inside an ambient `transaction()` → each step commits independently (REQUIRES_NEW); an
  outer rollback does not undo erasure.
- Hand-written contributor touching two resources in one step → `TransactionConfigurationError` →
  step fails (documented remedy).
- `dry_run=True` → zero side effects (no mutation, ledger, purge, shred, audit, event); counts
  reported; artifacts "not counted".
- Audit chain with prior entries for the subject → unchanged hashes; one appended `"erase"` entry;
  `verify_chain` is `True`.

## Verification

```bash
uv run pytest varco_core/tests/test_personal_data_erasure_markers.py varco_core/tests/test_personal_data_inventory.py \
  varco_core/tests/test_data_subject_erasure_types.py varco_core/tests/test_data_subject_entity_erase.py \
  varco_core/tests/test_data_subject_ledger.py varco_core/tests/test_data_subject_eraser.py \
  varco_core/tests/test_data_subject_exporter.py varco_core/tests/test_data_subject_erasure_audit_chain.py \
  varco_core/tests/test_data_subject_erase_task.py varco_core/tests/test_data_subject_erasure_module.py \
  varco_core/tests/test_data_subject_erasure_posture.py varco_core/tests/test_data_subject_no_di_side_effect.py \
  varco_core/tests/test_cli_gdpr_erase.py varco_core/tests/test_lazy_init.py -q
uv run pytest varco_sa/tests/test_data_subject_erase_sa_sqlite.py varco_sa/tests/test_data_subject_erasure_audit_sqlite.py \
  varco_fastapi/tests/test_data_subject_admin.py varco_fastapi/tests/test_posture_data_subject_erasure.py -q
uv run pytest varco_sa/tests/test_data_subject_erasure_integration.py -m integration -q
uv run pytest varco_beanie/tests/test_data_subject_erasure_integration.py -m integration -q
make test                    # all eleven suites
make lint                    # ruff + format + every snapshot gate (no PKG)
make type-check              # mypy strict
uv run python scripts/api_surface.py --check
uv run varco gdpr erase --help
# wording guard — must print nothing:
rg -n -i "gdpr[- ]compliant|compliant with (the )?gdpr|ensures? (gdpr )?compliance|guarantees? (gdpr )?compliance|gdpr compliance|right[- ]to[- ]erasure compliant" \
  technical_docs/features/data-subject-requests.md technical_docs/features/crypto-shredding.md \
  technical_docs/features/database-auditing.md docs/regulatory-posture.md README.md CLAUDE.md CHANGELOG.md \
  varco_core/varco_core/data_subject varco_fastapi/varco_fastapi/data_subject varco_core/varco_core/cli/gdpr.py
# no "endorsed" claim about crypto-shredding — must print nothing:
rg -n -i "endors" technical_docs/features/data-subject-requests.md technical_docs/features/crypto-shredding.md \
  docs/regulatory-posture.md README.md CHANGELOG.md
# no scanned decorator in the package — must print nothing:
rg -n "^@(Singleton|Provider|Configuration)" varco_core/varco_core/data_subject varco_fastapi/varco_fastapi/data_subject
# no raw subject id in a scope convention — must print nothing:
rg -n "subject\.id" varco_core/varco_core/data_subject/eraser.py | rg -i scope
```

## Risks

- ⚠️ **ASSUMPTION — the Art. 17(3)(a)–(e) enumeration** comes from the Regulation text, not a brief.
  Brief 022 §4 (`:67-72`) gives a different list. Before Step 11, verify `ErasureGround`'s docstrings
  against EUR-Lex 32016R0679 Art. 17(3). Reword docstrings, never values (values enter the audit
  record).
- ⚠️ **ASSUMPTION — regulators accept crypto-shredding case by case, on proportionality** (brief 022
  §4 `:65`, EDPB 2025 CEF report as the brief reports it). Brief 018 `:66-67` claims an explicit EDPB
  02/2025 endorsement from a consultation response, and brief 022's 02/2025 link points at the 01/2025
  page. Neither was independently verified. Mitigation: varco never relies on shredding for live rows
  (§D-shred), and docs never say "endorsed" (Verification `rg`).
- ⚠️ **ASSUMPTION — the WP29 Opinion 05/2014 anonymisation standard** (irreversibility, brief 022
  `:75`) and **EDPB 01/2025's "pseudonymised data remains personal data"** (`:122`) as the brief
  reports them. They drive "link fields must be nulled", "no hash strategy" and "`subject_key` is
  personal data".
- ⚠️ **ASSUMPTION — tombstoning is acceptable when the tombstone holds no personal data and the
  original is not recoverable** (brief 022 §5 item 4, `:87`). The brief gives no regulator source for
  this. Our tombstone holds `subject_key`, which is pseudonymous, so the doc calls it that and leaves
  the retention decision to the controller.
- ⚠️ **ASSUMPTION — Art. 17(3)(b)/(e) can justify keeping past audit diffs** (brief 022 `:73`). This
  is the controller's assessment; the doc says so and names the residual.
- ⚠️ **ASSUMPTION — backup handling** ("30 days … up to the rotation cycle if inaccessible or
  encrypted with a deleted key", brief 022 `:65`; Evidence gap 2 `:129`). No number of days is
  documented as a rule. **"Put beyond use"** is ICO wording found in neither brief; it is quoted only
  if verified at Step 11.
- ⚠️ **ASSUMPTION — Art. 19** (notifying recipients of erasure) is cited from the Regulation text, not
  a brief, and only to say the event does not discharge it.
- **Irreversibility.** A wrong declaration deletes data. Invariants: no default action; `dry_run`
  without a default at every entry point; preview-as-token + `erase_role` + the acknowledgement;
  startup validation of every declaration (Steps 3, 9).
- **Partial erasure.** A crash leaves a subject partially erased. Invariants: steps are idempotent;
  the shred runs last; a failed-attempt tombstone names the step; the ledger replays; retries via
  `max_attempts`.
- **Upstream plans not yet built** (062 Phase A, 064, 065, 069a; 061 for one assertion). Their names
  are used verbatim (`run_in_transaction`, `TransactionOptions`, `Propagation.REQUIRES_NEW`,
  `TRANSACTION_RETRY`, `current_transaction`, `InMemoryTransactionalResource`, `JobQueue.enqueue`,
  `current_job`, `JobDispatcher.run_once`, `EntityDataSubjectContributor`, `delete_for_subject`,
  `_data_subject_fakes.py`). Invariant: if a build renames one, follow the built name. A semantic
  change (e.g. `REQUIRES_NEW` no longer committing at exit, `JobQueue.enqueue` overwriting an
  existing id) stops this build for a re-plan.
- **065 is amended, not redesigned.** These are additive: `ExportManifest.erased_datasets`,
  `"erased": true`, the exporter properties and `EntityDataSubjectContributor.classification`.
  065's own tests must stay green (Step 5 runs `test_data_subject_exporter.py`).
- **064 inventory keys.** Apps with a committed `varco gdpr inventory --check` snapshot see drift once.
  Both plans ship unreleased in 4.0.0, so the schema stays at version 1. Called out in the CHANGELOG.
- **Raw subject id copies.** The id exists in two job payloads (dry run + execute), the
  `DataSubjectErasedEvent` in the broker, and whatever the app logs. Invariants: no raw id in the
  manifest, audit diff, ledger keys, scope strings or varco's log messages (Step 5 `caplog`, the
  `subject_scope` test). Residual: documented with retention recipes.
- **`AuditEvent.action` widening.** App code matching `action` exhaustively sees `"erase"`.
  CHANGELOG + `3-to-4.md`.
- **Ledger semantics on a shared idempotency store.** Erasure keys share the store with HTTP
  idempotency keys. Invariant: the `varco.dsr.erase:` prefix cannot collide with
  `IdempotencyMiddleware`'s scoped hash keys, and `delete_expired()` sweeps both (intended).
- **Wording.** Any doc line implying compliance or endorsement is a defect; the Verification `rg`
  guards it.

## Open questions

1. **Audit residual.** Is "retain past diffs under the controller's 17(3) assessment + one tombstone +
   the 064 redaction opt-in", with forgettable payloads parked, acceptable for 4.0.0? Or must per-subject
   audit-diff encryption ship now (it would need its own plan)?
2. **Required `holds=` and `key_manager=`** on the module (an explicit `None` is a statement). Is
   that too strict, or should one of them default to `None`?
3. **Four-eyes confirmation.** Should the execute request be refused when the confirmer is the same
   identity that requested the dry run (opt-in `require_distinct_confirmer=True`)?
4. **Tombstone/event failure fails the attempt** (diverging from 065's export, which swallows it).
   Agree?
5. **`DataSubjectErasedEvent` carries the raw subject id.** Agree, or should it carry only
   `subject_key` (downstream services would then need a hash index)?
6. **GLOBAL entities under a tenant's erasure → NOT_SUPPORTED** (a platform-level run is required).
   Agree?
7. **Should dry runs be audited** (e.g. `action="erase"` with `dry_run: true`, or a separate action
   value)? They are not audited in this plan.
8. **`--replay` after a backup restore.** Worth a row now, or wait for the first restore?
9. **Coordinator finding (out of this plan's scope):** the DI-wired `SQLAlchemyRepositoryProvider`
   builds every entity with no `FieldEncryptor` (`varco_sa/provider.py:168,174`), and Beanie's factory
   has no encryption support, so `EncryptedHint` is inert on the standard path (the mapper's "silent
   no-op", `mapper.py:130-132`). That contradicts 065 §D-reads' "encrypted fields arrive decrypted"
   and 064's posture finding's remedy ("add `EncryptedHint()`"). It probably needs its own BACKLOG row.
