# Plan 065 — Data-subject access & portability export: the `DataSubjectContributor` seam, catalog-driven contributors, `DataSubjectExporter` as a job, admin mount, `varco gdpr export` (GDPR-2, part 1 of 2)

Covers the **export half** of BACKLOG 4.0.0-feature-extension row **`GDPR-2`** (`BACKLOG.md:60` —
*"`@Multibound` `DataSubjectContributor` per entity; `export(subject)` → machine-readable JSON (Art.
15/20); … runs as a job, admin mount"*). Ships inside **4.0.0** (locked decision, `BACKLOG.md:31`).
The erasure half (`erase(subject)`, Art. 17) is **Plan 066**, planned after this one from §Handoff
below. `GDPR-3` (consent ledger + processing purposes) is **parked** (`BACKLOG.md:34,110`) — nothing
here models purposes or lawful basis; they are controller-supplied text.

**Prerequisites (build order):** Plan **064** (`varco_core.personal_data`: `PersonalData`,
`PersonalDataCategory`, `PersonalDataOrigin`, `DataSubjectRef`, `classify()`,
`EntityClassification`, `PersonalDataCatalog`, the `varco gdpr` CLI group) and Plan **069a**
(`JobSpec`, `JobQueue.enqueue()`, `Job.tenant_id`, `current_job()`, `JobDispatcher`/
`JobDispatcherModule`) must be merged first. Plan **067**'s `varco_core.ndjson` is referenced only
for value-encoding parity and a parked NDJSON follow-up. Plan **062** (ambient transactions) is
**not** a dependency.

**Sources of record read for this plan (2026-10-07 → 2026-10-10):** `BACKLOG.md:27-37,60,110`;
`design/research/022-gdpr-classification-export-erasure.md` §3 (`:44-59` — Art. 15(1)(a)-(h), Art.
15(3) copy, Art. 20 provided/observed vs derived, JSON per WP242, Art. 12(3) one month, identity
verification not prescribed), §2 (`:22-42`), Version notes (`:122` — pseudonymised data remains
personal data), Evidence gap 5 (`:132`); `design/research/018-…` §B (`:71-74` — no framework ships
DSAR export); `docs/regulatory-posture.md:3-8` (the "position, not legal advice" voice);
`plans/064-personal-data-classification.md` §Public API (`:116-235`), §Handoff (`:447-459`), Open
questions 1 and 5 (`:685-695`); `plans/069a-job-dispatcher-and-enqueue-api.md` §D-tenant
(`:187-205`), §D-context (`:207-216`), §D-enqueue (`:218-257`), §D-unknown (`:305-318`),
§D-coexist (`:320-332`), §D-di (`:345-377`); `plans/067-streaming-reads-and-keyset-pagination.md`
§D-codec (`:147-212`) and its 065 note (`:442-443`); `varco_core/varco_core/di/collect.py:81-190`
(`contribute`/`collect`/`acollect`/`collection_seams` — five seams today);
`varco_core/varco_core/retention/policy.py:89` (`@Multibound` precedent);
`varco_core/varco_core/job/task.py:88-205,244-345,432-537` (`TaskPayload`, `VarcoTask`,
`TaskRegistry.register` silently overwrites `:468-482`); `varco_core/varco_core/job/base.py:147-245`
(`Job.result: bytes | None`, opaque), `:1143-1193,1355-1402`; `varco_fastapi/varco_fastapi/job/runner.py:411-491`
(`enqueue_task` executes in-process, forwards `**kwargs` to the task), `:655-664` (result
`json.dumps`); `varco_fastapi/varco_fastapi/job/response.py:36-131`;
`varco_fastapi/varco_fastapi/router/base.py:1571-1610` (the only 202 producer — no generic job-status
route exists); `varco_fastapi/varco_fastapi/admin/_tenant_scope.py:37-171`
(`require_server_auth`, `admin_guard`, `allow_cross_tenant`, `resolve_tenant_scope`);
`varco_fastapi/varco_fastapi/webhook/mount.py:35-108`, `webhook/router.py:13-24` (by-id 404 on a
cross-tenant hit); `varco_core/varco_core/service/tenant.py:151-199,419-449`;
`varco_core/varco_core/tenancy/provenance.py:131-171` (`assert_tenant_matches`);
`varco_core/varco_core/tenancy/global_scope.py:49-84`; `varco_core/varco_core/service/base.py:212-254`
(`IUoWProvider`), `:998-1067` (`stream()` authorizes and applies `_scoped_params`);
`varco_core/varco_core/repository.py:295-334` (`stream_by_query`);
`varco_core/varco_core/providers.py:39-148`; `varco_sa/varco_sa/provider.py:176-220` and
`varco_beanie/varco_beanie/provider.py:99-142` (identical private `_repo_attr`);
`varco_core/varco_core/event/audit_event.py:107-128` (`action: Literal["create","update","delete"]`);
`varco_core/varco_core/service/audit.py:95,137,607-618`; `varco_sa/varco_sa/audit.py:129`
(`action` is `String(16)`); `varco_core/varco_core/encryption.py:92-140,183`;
`varco_core/varco_core/retention/targets.py:175-222`; `varco_core/varco_core/settings/bind.py:237-311`;
`varco_core/varco_core/di/module.py:232-290`; `varco_fastapi/varco_fastapi/modules/lifecycle.py:442-507`
(`RetentionModule` — `async_init` collect + `start` shape); `varco_core/varco_core/cli/main.py:27-62`,
`cli/retention.py:39-115` (`--target module:callable`); `varco_core/varco_core/di/features.py:52-89`;
`technical_docs/entry-points.md:53-70`; `plans/064` (house format).

> ⚠️ **Five premises corrected from source before anything was decided.**
> (1) **No generic job-status route exists.** `JobAcceptedResponse` is produced only by
> `router/base.py:1610` with `poll_url` left at its `""` default (`response.py:61-64`) and nothing
> serves `GET /jobs/{id}` — the admin mount owns its own status/download routes (§D-mount).
> (2) **`AbstractJobRunner.enqueue_task()` is not an enqueue API.** It executes the coroutine
> in-process (`runner.py:482-491`) and forwards `**kwargs` into the task, so control knobs collide
> with task arguments. Plan 069a's `JobQueue.enqueue(JobSpec)` is the enqueue API (its §D-enqueue
> names Plan 066 as a consumer; this plan is the first).
> (3) **`AuditEvent.action` is `Literal["create","update","delete"]`** (`audit_event.py:115`). An
> "export performed" record needs a one-value widening; the persisted column is `String(16)`
> (`varco_sa/audit.py:129`), so no migration (§D-audit).
> (4) **The service layer cannot be the read path.** `AsyncService.stream()` authorizes the caller
> and applies `_scoped_params` (`base.py:1047-1055`) — soft-delete filters would hide data that is
> still stored and processed. No generic in-memory repository exists in `varco_core`. Contributors
> read repositories through `IUoWProvider` (§D-reads).
> (5) **`Job.result` is the wrong home for the document.** It is opaque `bytes` in the jobs table
> (`base.py:217`), unencrypted and swept by job retention. The document goes to an
> `ExportArtifactStore`. `Job.result` holds only a manifest that contains no personal data
> (§D-artifact).

## Goal

After this plan, an app that has classified its models (Plan 064) adds one module —
`DataSubjectExportModule(catalog=…, controller=ControllerInformation(…))`. Each subject-linked
entity then gets a catalog-driven `DataSubjectContributor` automatically. Hand-written contributors
cover indirect links, unlinked entities and external stores through the same `@Multibound` seam.
An operator can produce a subject's data three ways: `POST /data-subject-requests/exports` (202 +
poll + download, `server_auth` and an admin role required, tenant-bound), `varco gdpr export`, or
`DataSubjectExporter.build()`. All three yield one versioned, machine-readable JSON document (schema
v1). It holds the subject's records per dataset, per-field category/origin annotations, the derived
Art. 15(1)(b) category list, and the controller-supplied Art. 15(1)(a)/(c)-(h) and 15(2)
information. A `portability` scope restricts records to classified provided/observed fields (Art. 20).

The export runs as a `JobQueue` job under its own tenant. The document is stored encrypted at rest
with a TTL that `varco_core.retention` enforces, and every export is recorded in the audit trail as
evidence that it happened, without the exported data. No documentation claims GDPR compliance.

## Non-goals

- **Erasure (Art. 17)** — Plan 066. This plan reserves the seam shape for it (§D-seam, §Handoff)
  and ships `ExportArtifactStore.delete_for_subject()` for it. It adds no `erase()`, no
  crypto-shred orchestration and no tombstones.
- **Self-service / subject-facing routes.** The admin mount is operator-facing. Verifying the
  requester's identity is the controller's job (Art. 12(6), brief 022 §3 `:59`, Evidence gap 5).
  An app that wants self-service builds its own route on `DataSubjectExporter` *after* its own
  verification; the feature doc says so and shows the warnings.
- **DSAR case management** — deadlines (Art. 12(3) one month), extensions, fees, refusal of
  manifestly unfounded requests, request intake, delivery to the subject. Controller workflow.
- **Direct controller-to-controller transmission** (Art. 20(2)).
- **Purposes / lawful basis / consent modelling** — `GDPR-3` is parked. Whether Art. 20 applies at
  all (consent/contract basis, brief 022 §3 `:53`) is the controller's call; varco only offers the
  `portability` scope.
- **CSV/XML output, and NDJSON for large exports.** JSON only, capped by `max_bytes`. NDJSON over
  Plan 067's `varco_core.ndjson` codec is a parked follow-up (§D-size).
- **Framework tables as auto-covered sources** — audit trail rows by `actor_id`, job rows, DLQ,
  outbox, webhook deliveries, idempotency records. The feature doc lists them as personal-data
  locations the controller must assess. Shipping contributors for them is Open question 2.
- **A declarative indirect-link form** (`DataSubjectRef(via=…)`) — §D-indirect.
- **Cross-tenant subject discovery** ("which tenants hold data on X") and multi-tenant aggregate
  documents — §D-tenant.
- **A read-only transaction boundary across contributors** — Plan 062 may add one later. Each
  contributor opens its own UoW, so the document is not a point-in-time snapshot (Risks).
- **Value-level PII detection, redacting the document** — the document is for the subject (§D-redaction).
- **An env-var or `create_varco_app` keyword that enables the export or mounts the surface**
  (CLAUDE.md RD-9 rule) and an `enable_*` verb (§D-module).
- **Editing `BACKLOG.md`.**

## Design

### Module layout

```
varco_core/varco_core/data_subject/          (new; shared with Plan 066)
  __init__.py   — re-exports; ⛔ no @Singleton/@Provider/@Configuration (scan rule)
  base.py       — DataSubject, subject_key(), ExportScope, ExportField, DataSubjectContributor
                  (@Multibound ABC), DataSubjectRequestError
  controller.py — ControllerInformation, ControllerInformationSource (type alias)
  settings.py   — DataSubjectExportSettings (VarcoSettings, env VARCO_DSR_EXPORT_)
  document.py   — EXPORT_SCHEMA_VERSION, EXPORT_MEDIA_TYPE, to_json_value()
  entity.py     — EntityDataSubjectContributor (the catalog-driven contributor)
  artifacts.py  — ExportArtifactStore (ABC), InMemoryExportArtifactStore,
                  FileSystemExportArtifactStore
  exporter.py   — ExportRequest, ExportManifest, ExportResult, DataSubjectExporter
                  (EXPORT_TASK_NAME, .task)
  module.py     — DataSubjectExportModule (VarcoModule)
varco_core/varco_core/providers.py            + repository_attribute() (the one UoW-attribute rule)
varco_core/varco_core/retention/targets.py    + ExportArtifactRetentionTarget
varco_core/varco_core/event/audit_event.py    action Literal gains "export"
varco_core/varco_core/di/collect.py           collection_seams() → six seams
varco_core/varco_core/cli/gdpr.py             + `export` verb (Plan 064 owns the group)
varco_fastapi/varco_fastapi/data_subject/     (new)
  __init__.py, router.py (build_data_subject_router), mount.py (mount_data_subject_admin)
```

Import direction: `data_subject → personal_data, job, tenancy, service.tenant, event, encryption,
retention(base only)`. Nothing in `personal_data`, `job` or `retention.policy` imports
`data_subject` (`retention/targets.py` imports `ExportArtifactStore` under `TYPE_CHECKING` only).
`varco_fastapi.data_subject` imports `varco_core.data_subject`, never the reverse. Heavy imports
(pydantic adapters, `hashlib`, `os`) live inside function bodies where they would otherwise hit the
import budget.

### Flow

```
 operator ──POST {prefix}/exports──▶ router (admin_guard: server_auth + admin_role)
                                      │  tenant = §D-tenant mapping (cross_tenant_role)
                                      │  kind ∈ exporter.subject_kinds()  else 422
                                      ▼
            with tenant_context(tenant):            ← JobQueue stamps Job.tenant_id
                JobQueue.enqueue(JobSpec(task=exporter.task.payload(kind, id, scope, by),
                                         job_id=uuid4(), max_attempts=settings.max_attempts))
                                      │ 202 + Location: {prefix}/exports/{job_id}
                                      ▼
   any pod listing JobDispatcherModule + DataSubjectExportModule
   JobDispatcher.claim → tenant_context(job.tenant_id) → current_job() = job
        └─ DataSubjectExporter._run_job(kind, id, scope, by)
              request_id = current_job().job_id
              for c in contributors serving kind (sequential, sorted by name):
                  async for row in c.export(subject):  → to_json_value → section.records
              document (schema v1) ─ max_bytes guard ─▶ ExportArtifactStore.put(request_id, …,
                                                          expires_at=now+artifact_ttl)
              AuditEvent(action="export", diff={…no data…}) ──▶ "varco.audit" (if producer)
              return manifest.to_dict()  ──▶ Job.result (no personal data)

 operator ──GET {prefix}/exports/{job_id}──────────▶ status + manifest      (404 cross-tenant)
 operator ──GET {prefix}/exports/{job_id}/document─▶ artifact bytes (no-store, attachment)
 operator ──DELETE {prefix}/exports/{job_id}/document─▶ 204 (delete after delivery)
 RetentionPolicy(ExportArtifactRetentionTarget(store)) ─▶ store.delete_expired()
```

### Public API (the contract Plan 066 plans against)

```python
# base.py
@dataclass(frozen=True)
class DataSubject:
    kind: str                     # slug ^[a-z][a-z0-9_]*$ — same rule as DataSubjectRef.kind (064)
    id: str                       # non-empty, ≤ 512 chars, never stripped; JSON-safe for TaskPayload
    def __repr__(self) -> str: ...  # DataSubject(kind='customer', id=<redacted>) — never the id

def subject_key(subject: DataSubject) -> str: ...   # sha256(f"{kind}:{id}").hexdigest() — §D-audit

class ExportScope(StrEnum):
    ACCESS = "access"             # Art. 15 — whole rows (§D-rows)
    PORTABILITY = "portability"   # Art. 20 — classified PROVIDED/OBSERVED fields only

@dataclass(frozen=True)
class ExportField:
    name: str
    category: PersonalDataCategory | None = None    # None = not classified
    origin: PersonalDataOrigin | None = None
    @property
    def special_category(self) -> bool: ...          # category is not None and category.special
    @property
    def portable(self) -> bool: ...                  # origin in {PROVIDED, OBSERVED}

class DataSubjectRequestError(Exception): ...        # one feature error; messages never carry the
                                                     # subject id or record values (§D-redaction)
@Multibound
class DataSubjectContributor(ABC):
    @property
    @abstractmethod
    def name(self) -> str: ...                       # stable, unique per container (checked at build)
    @property
    def label(self) -> str: ...                      # subject-facing dataset name; default = name
    @property
    @abstractmethod
    def subject_kinds(self) -> frozenset[str]: ...
    @property
    def covers(self) -> tuple[type[DomainModel], ...]: ...   # catalog entities accounted for; ()
    @property
    def fields(self) -> tuple[ExportField, ...]: ...         # declared descriptors; ()
    @abstractmethod
    def export(self, subject: DataSubject) -> AsyncIterator[Mapping[str, Any]]: ...
    # ⛔ RESERVED for Plan 066: the attribute names `erase` and `erasure_*`.

# controller.py
@dataclass(frozen=True)
class ControllerInformation:           # supplied by the controller, never invented (§D-controller)
    controller: str                    # identity + contact details (and DPO where applicable)
    purposes: tuple[str, ...]          # Art. 15(1)(a) — ≥ 1 non-empty item
    recipients: tuple[str, ...]        # Art. 15(1)(c) — explicit () only if there are none
    storage_period: str                # Art. 15(1)(d) — period or criteria
    rights: str                        # Art. 15(1)(e) — rectification/erasure/restriction/objection
    complaint: str                     # Art. 15(1)(f) — supervisory authority
    automated_decision_making: str     # Art. 15(1)(h) — state "none" explicitly if none
    data_source: str | None = None     # Art. 15(1)(g) — where not collected from the subject
    third_country_safeguards: str | None = None   # Art. 15(2)
ControllerInformationSource = ControllerInformation | Callable[[str | None], Awaitable[ControllerInformation]]
                                       # the callable receives current_tenant() — per-tenant controllers

# settings.py
class DataSubjectExportSettings(VarcoSettings):   # env_prefix="VARCO_DSR_EXPORT_"
    max_bytes: int = 64 * 1024 * 1024              # > 0; exceeding fails the export, never truncates
    artifact_ttl: timedelta = timedelta(days=7)    # > 0
    max_attempts: int = 3                          # ≥ 1 — forwarded to JobSpec.max_attempts

# entity.py
class EntityDataSubjectContributor(DataSubjectContributor):
    def __init__(self, classification: EntityClassification, uow_provider: Any, *,
                 exclude_fields: Iterable[str] = (), tenant_field: str | None = "tenant_id") -> None: ...
    # name = classification.name; label = entity.__name__; subject_kinds = link kinds;
    # covers = (classification.entity,); fields = one ExportField per exported field

# artifacts.py
class ExportArtifactStore(ABC):
    async def put(self, artifact_id: UUID, data: bytes, *, expires_at: datetime,
                  tenant_id: str | None, subject_key: str) -> None: ...   # idempotent overwrite
    async def get(self, artifact_id: UUID) -> bytes | None: ...           # None if absent/expired
    async def delete(self, artifact_id: UUID) -> bool: ...
    async def delete_expired(self, *, now: datetime | None = None) -> int: ...
    async def delete_for_subject(self, subject_key: str, *, tenant_id: str | None) -> int: ...  # for 066
class InMemoryExportArtifactStore(ExportArtifactStore): ...     # dev/tests, single process
class FileSystemExportArtifactStore(ExportArtifactStore):
    def __init__(self, directory: str | os.PathLike[str], *, encryptor: FieldEncryptor) -> None: ...

# exporter.py
EXPORT_TASK_NAME: Final = "varco.data_subject.export"

@dataclass(frozen=True)
class ExportRequest:
    subject: DataSubject
    scope: ExportScope = ExportScope.ACCESS
    request_id: UUID = field(default_factory=uuid4)
    requested_by: str | None = None          # operator identity, for the audit record

@dataclass(frozen=True)
class ExportManifest:                        # operator-facing; NO personal data, no raw subject id
    request_id: UUID; scope: ExportScope; subject_kind: str; subject_key: str
    tenant_id: str | None; generated_at: datetime; bytes: int; sha256: str
    records: Mapping[str, int]               # by contributor name
    acknowledged_unreached: tuple[str, ...]  # EntityClassification.name values
    artifact_expires_at: datetime | None; audit_recorded: bool
    def to_dict(self) -> dict[str, Any]: ... # schema_version + kind "varco.data-subject-export-manifest"

@dataclass(frozen=True)
class ExportResult:
    document: bytes; manifest: ExportManifest

class DataSubjectExporter:
    def __init__(self, contributors: Iterable[DataSubjectContributor], *,
                 controller: ControllerInformationSource,
                 settings: DataSubjectExportSettings | None = None,
                 artifact_store: ExportArtifactStore | None = None,
                 catalog: PersonalDataCatalog | None = None,
                 acknowledge_unreached: Iterable[type[DomainModel]] = (),
                 producer: AbstractEventProducer | None = None,
                 clock: Callable[[], datetime] | None = None) -> None: ...  # guards run here
    def subject_kinds(self) -> tuple[str, ...]: ...
    @property
    def artifact_store(self) -> ExportArtifactStore | None: ...
    @property
    def task(self) -> VarcoTask: ...          # name EXPORT_TASK_NAME, fn = self._run_job
    async def build(self, request: ExportRequest) -> ExportResult: ...   # pure assembly, no I/O side effects
    async def export(self, request: ExportRequest) -> ExportManifest: ...  # build + put + audit
    async def record_audit(self, manifest: ExportManifest, *, requested_by: str | None,
                           channel: str) -> bool: ...                  # used by export() and the CLI

# module.py
class DataSubjectExportModule(VarcoModule):
    name = "data_subject_export"; phase = Phase.SCHEDULERS; depends_on = (CoreModule,)
    def __init__(self, *, catalog: PersonalDataCatalog, controller: ControllerInformationSource,
                 settings: DataSubjectExportSettings | None = None,
                 exclude: Iterable[type[DomainModel]] = (),
                 exclude_fields: Mapping[type[DomainModel], Iterable[str]] | None = None,
                 acknowledge_unreached: Iterable[type[DomainModel]] = (),
                 tenant_field: str | None = "tenant_id", enabled: bool | None = None) -> None: ...
    @property
    def exporter(self) -> DataSubjectExporter: ...   # after async_init

# varco_fastapi/data_subject/mount.py
def mount_data_subject_admin(app: FastAPI, *, exporter: DataSubjectExporter, job_queue: JobQueue,
        job_store: AbstractJobStore, server_auth: Any, acknowledge_bundled_admin: bool = False,
        admin_role: str = "data-subject-admin", cross_tenant_role: str = "cross-tenant-admin",
        prefix: str = "/data-subject-requests") -> None: ...
```

Top-level `varco_core.__all__` gains (lazily: `_LAZY` + `TYPE_CHECKING` + `__all__`):
`DataSubject`, `DataSubjectContributor`, `DataSubjectExporter`, `ControllerInformation`.
`DataSubjectExportModule` is exported from `varco_core.data_subject` only (the
`JobDispatcherModule`/`WebhookModule` precedent, 069a §D-di).

Usage (the README snippet):

```python
catalog = PersonalDataCatalog.from_models([Customer, Order, Comment])
controller = ControllerInformation(
    controller="Acme Ltd, privacy@acme.example (DPO: dpo@acme.example)",
    purposes=("Order fulfilment", "Customer support"),
    recipients=("Payment processor", "Courier"),
    storage_period="Account lifetime + 24 months; invoices 10 years (tax law)",
    rights="You may request rectification, erasure, restriction or object: privacy@acme.example",
    complaint="You may lodge a complaint with your supervisory authority.",
    automated_decision_making="None.",
)
app = create_varco_app(container, modules=[
    *default_modules(),
    DataSubjectExportModule(catalog=catalog, controller=controller),
    JobDispatcherModule(),                       # or on a separate worker process
])
bind_instance(container, ExportArtifactStore,   # before create_varco_app; shadows the @Fallback
              FileSystemExportArtifactStore("/var/lib/acme/dsr", encryptor=my_encryptor))
mount_data_subject_admin(app, exporter=container.get(DataSubjectExporter),
                         job_queue=container.get(JobQueue), job_store=container.get(AbstractJobStore),
                         server_auth=my_server_auth, acknowledge_bundled_admin=True)
```

```
varco gdpr export --target myapp.dsr:exporter --kind customer --id - --tenant acme -o out.json
```

### Export document — schema v1 (`kind: "varco.data-subject-export"`, `application/json`)

```json
{
  "schema_version": 1,
  "kind": "varco.data-subject-export",
  "scope": "access",
  "request_id": "6f1c…",
  "generated_at": "2026-10-10T09:30:00Z",
  "subject": {"kind": "customer", "id": "4b2e…"},
  "controller": {
    "controller": "Acme Ltd, …",
    "purposes": ["Order fulfilment", "Customer support"],
    "personal_data_categories": ["contact", "health"],
    "recipients": ["Payment processor", "Courier"],
    "storage_period": "…",
    "rights": "…",
    "complaint": "…",
    "data_source": null,
    "automated_decision_making": "None.",
    "third_country_safeguards": null
  },
  "datasets": [
    {
      "dataset": "Customer",
      "fields": [
        {"name": "email", "category": "contact", "special_category": false,
         "origin": "provided", "portable": true},
        {"name": "created_at", "category": null, "special_category": false,
         "origin": null, "portable": false}
      ],
      "records": [{"pk": "4b2e…", "email": "…", "created_at": "2026-01-01T10:00:00Z"}]
    }
  ]
}
```

Rules:
- Key order is exactly as above. `controller.personal_data_categories` is the **derived** Art.
  15(1)(b) item: the distinct categories of fields in datasets with ≥ 1 record, in
  `PersonalDataCategory` enum order. Everything else under `controller` is copied verbatim from
  `ControllerInformation`. `null` means the controller supplied `None`.
- `datasets` are sorted by `(label, name)`. Records keep the contributor's yield order. The catalog
  contributor sorts by primary key, so output is deterministic. A dataset with zero records is still
  listed (`records: []`). Under `portability`, `fields` and records hold only portable fields
  (§D-rows).
- A record key with no declared `ExportField` gets an appended unclassified descriptor under
  `access`, sorted by name after the declared ones, and is dropped under `portability`.
- Serialisation is `json.dumps(…, ensure_ascii=False, indent=2, allow_nan=False)` + UTF-8. The
  subject reads this file, so it is indented. `generated_at` comes from the injectable `clock`, in
  RFC 3339 UTC with `Z`.
- **Value encoding (`to_json_value`)**: `None`/`bool`/`int`/`str` as-is. A finite `float` stays
  as-is; NaN/±inf → `str`. `UUID`, `Decimal` → `str`. Aware `datetime` → ISO 8601 in UTC with `Z`;
  naive `datetime`/`date`/`time` → `isoformat()`. `Enum` → `.value`. `bytes` → standard base64
  `str`. `list`/`tuple` → list; `set`/`frozenset` → list sorted by `str`. `Mapping` → dict with
  `str` keys. Dataclass / pydantic `BaseModel` → dict, recursively. Anything else → `TypeError`
  naming `dataset.field` and the type, never the value. The `datetime`/`date`/`UUID`/`Decimal`/`Enum`
  rules are identical to Plan 067's `encode_record` default (`067:210-212`), so a future NDJSON
  format produces identical field values.

### DESIGN blocks

**§D-seam — one `@Multibound` seam with `export` now; Plan 066 adds `erase` as a concrete,
fail-loud default**
- ✅ "Where does subject X's data live" is one fact. Export and erasure must walk the same set of
  sources, or a source that is exported but never erased (or the reverse) is silent drift. One
  seam, one coverage check (`covers`), one `name` uniqueness rule.
- ✅ Non-breaking for 066. 066 adds `async def erase(self, subject, *, dry_run: bool) ->
  ErasureOutcome` as a **concrete** method whose base implementation returns a `NOT_SUPPORTED`
  outcome naming the contributor. It never raises and never reports success. Every 065-era
  hand-written contributor keeps working and appears in the erasure report as a gap, never as
  silently erased. The names `erase`/`erasure_*` are reserved in the ABC docstring now.
- ✅ `@Multibound` + `contribute()`/`collect()` is the house convention (`di/collect.py:8-19`).
  `collection_seams()` gains it as the sixth seam, so `varco diagnose` and the `di_wiring`
  conformance (no `@Fallback`, no `priority=`) cover it automatically.
- ✅ `export()` yields plain mappings, so a contributor over an external store (S3 uploads, a
  payment provider, an analytics warehouse) needs no `DomainModel`.
- ❌ An export-only contributor can exist (e.g. a read-only external system), and its erasure gap
  only shows up in 066's report. Accepted: visible, not silent.
- Rejected — **two seams (`DataSubjectExporter` contributors + a separate `DataSubjectEraser`
  seam)**: ✅ export-only sources could not be mistaken for erasable. ❌ Two registries for one fact,
  two coverage checks, and two auto-derivations from the catalog that can disagree.
- Rejected — **066 adds `erase` as an `@abstractmethod`**: ❌ breaks every 065-era contributor
  between the two merges; ✅ would force a decision. The fail-loud default gets the same visibility
  without the break.

**§D-auto — catalog-driven contributors are per-entity DI contributions, with `exclude=` and a
wiring-time coverage check**
- ✅ Most apps write zero contributor code. `DataSubjectExportModule._register` contributes one
  `EntityDataSubjectContributor` per `catalog.entities` entry with `is_subject_linked`, unless the
  entity is in `exclude`.
- ✅ Per entity, not one catalog-wide contributor. `varco diagnose` lists each one, and an app
  replaces exactly one entity's auto contributor by `exclude=(Comment,)` plus a hand-written
  contributor whose `covers=(Comment,)`.
- ✅ Each contribution is a `@Provider(singleton=True)` factory registered with
  `returns=DataSubjectContributor`. It declares `uow_provider: IUoWProvider` as a parameter (and
  optionally `GlobalUoWProvider` for `TenantScope.GLOBAL` entities, falling back to
  `IUoWProvider`), so the UoW provider is resolved at collection time, never captured at register
  time. This is the CLAUDE.md "declare it on the provider method" rule. The factory's parameter
  annotations must resolve from `module.py`'s globals: import `IUoWProvider`/`GlobalUoWProvider` at
  module scope, never under `TYPE_CHECKING` (common-pitfalls: quoted `@Provider` annotations).
- ✅ **Coverage is checked in `DataSubjectExporter.__init__`**, which `async_init` forces, so a gap
  fails startup, not a DSAR. `required` = every catalog entity with `holds_personal_data`.
  `covered` = the union of all contributors' `covers`. If any of `required − covered −
  acknowledge_unreached` remain, `ValueError` names them (sorted `EntityClassification.name`) and
  both remedies: a contributor with `covers=`, or `acknowledge_unreached=(Entity,)`. Acknowledged
  entities appear in every manifest's `acknowledged_unreached`, never in the subject's document.
- ✅ Duplicate contributor `name` → `ValueError` at construction (the `RetentionRegistry` duplicate
  rule).
- ❌ An app with an unlinked personal-data entity (064's `personal_data.unlinked_entity`) cannot
  start the module until it decides. Intended — the alternative is a DSAR answer that silently
  omits data. The acknowledgement is per entity, in source, greppable (the retention `dry_run`
  no-default philosophy).
- ❌ Entities that hold personal data but carry no `PersonalData` marker are invisible to the check.
  Pitfall row; classification is 064's job.

**§D-rows — Plan 064 Q1 resolved: whole rows for `access`, classified provided/observed fields for
`portability`**
- ✅ Art. 15(1) grants "a copy of the personal data undergoing processing" (brief 022 §3 `:48`).
  A row linked to the subject relates to them as a whole: `Order.total`, `status` and timestamps are
  theirs to see even though no developer marked them `PersonalData`. Classified-only would
  under-answer. ⚠️ ASSUMPTION on the breadth of "relating to" (Risks).
- ✅ Classification still does work under `access`. Every exported field carries its
  category/origin descriptor, and the categories feed the derived Art. 15(1)(b) list.
- ✅ Art. 20 covers provided + observed data and excludes inferred/derived data (brief 022 §3
  `:53-54`). Only a classified field has an origin, so `portability` keeps only fields whose
  `ExportField.portable` is true. An unclassified field cannot be judged, so it is left out of
  portability (it stays in `access`).
- ✅ Always excluded from both scopes: `_`-prefixed attributes (`_raw_orm`, the 064 `classify()`
  rule) and the `tenant_field` column (internal routing, not data about the subject).
  `exclude_fields={Entity: {"fraud_score", …}}` removes more. Each name is validated against the
  entity's dataclass fields at `_register`, and an unknown name raises `ValueError`.
- ❌ Whole rows can carry **other people's** data (`Message.recipient_id`, a free-text note naming
  a third party) — the Art. 15(4) "rights and freedoms of others" limit. Mitigation:
  `exclude_fields` or a hand-written contributor, plus a Pitfalls row. varco cannot judge it.
- ❌ Portability drops pk/link columns unless classified, so cross-dataset joins are lost in the
  portable file. Accepted: internal ids mean nothing to a receiving controller.
- Rejected — **classified fields only for access**: under-exports by construction.
- Rejected — **one document with per-field `portable` flags and no scope**: ❌ a portability
  transmission would ship derived data to another controller.

**§D-indirect — Plan 064 Q5 resolved: indirect links are hand-written contributors, no declarative
`via=`**
- ✅ Indirect reach is a judgement about *whose* data it is. Comments on the subject's posts are
  primarily the commenters' data (Art. 15(4)). A declarative `DataSubjectRef(via="post.author_id")`
  would export them by default.
- ✅ Multi-hop paths across entities have fan-out, cycle and cross-backend semantics (an SA `Post`
  and a Mongo `Comment`) that a marker cannot express honestly. A contributor is ~20 lines (doc
  recipe).
- ❌ Every indirect link is code. Accepted; revisit with a concrete request (Open question 3).

**§D-reads — repositories through `IUoWProvider`, bypassing the service layer**
- ✅ The operator's authorization is the admin mount's role + tenant guard (§D-mount) or shell
  access (CLI). The per-entity `AbstractAuthorizer` evaluates an end-user caller and has no
  "operator answering a DSAR" action.
- ✅ Soft-deleted rows are **included**. They are still stored, therefore still "undergoing
  processing". The service's `_scoped_params` would hide them (`base.py:1053-1055`).
- ✅ The repository attribute is located by a new public `varco_core.providers.repository_attribute(cls)`
  (`User` → `"users"`). It is the rule `IUoWProvider.make_uow`'s contract documents
  (`providers.py:146-147`) and today two private copies implement it (`varco_sa/provider.py:218-220`,
  `varco_beanie/provider.py:140-142`), which now delegate to it. A missing attribute →
  `LookupError` naming the entity and "register it with your RepositoryProvider".
- ✅ Encrypted fields (`EncryptedHint`) arrive decrypted through the mapper. The subject gets
  plaintext, which is what they are owed.
- ✅ Query: `stream_by_query(QueryParams(node=OR(eq(f, coerced_id) for f in
  classification.subject_fields(kind)) [AND eq(tenant_field, tid)], sort=<pk asc>))` inside
  `contextlib.aclosing`. One UoW per contributor per export. The subject id string is coerced per
  field with `pydantic.TypeAdapter(<field type stripped of Annotated>)`. A coercion failure raises
  `DataSubjectRequestError` naming `Entity.field` and the target type, never the id. It means the
  subject-id fields of one kind disagree in type, which is a declaration bug.
- ❌ No cross-contributor snapshot. Plan 062 may wrap `build()` in one read-only boundary later
  (Risks).
- Rejected — **`AsyncService.stream()`**: per-caller authorization, hidden soft-deleted rows, and
  it needs a service per entity.

**§D-tenant — one tenant per export, `current_tenant()` the only source, fail-closed on
tenant-partitioned entities**
- ✅ A document never aggregates tenants. In a multi-tenant product each tenant is often the
  controller of its users' data (⚠️ ASSUMPTION, Risks), so one subject present in tenants A and B
  gets one export per tenant — two jobs.
- ✅ The tenant travels the 069a way. The route enters `tenant_context(tid)` around
  `JobQueue.enqueue()`, which stamps `Job.tenant_id` (069a §D-tenant: "to enqueue for another tenant,
  the caller enters `tenant_context(tid)`"). The executor enters `tenant_context(job.tenant_id)`
  around the handler. Contributors read `current_tenant()`. The CLI enters `tenant_context(--tenant)`
  itself. No `tenant_id` field exists on `ExportRequest` or in the task arguments.
- ✅ Route mapping, the export's own (not `resolve_tenant_scope`, which 403s a single-tenant app
  that has no resolved tenant): body `tenant_id` omitted → `current_tenant()` (may be `None`);
  given → `assert_tenant_matches(requested, allow_cross_tenant=allow_cross_tenant(ctx,
  cross_tenant_role))`, with `CrossTenantAccessError` → 403.
- ✅ **Fail closed in the catalog contributor.** For a `TenantScope.TENANT` entity whose dataclass
  has the `tenant_field`, an active tenant adds `eq(tenant_field, tid)`. No active tenant raises
  `DataSubjectRequestError("… is tenant-partitioned …; run the export inside a tenant")`, never an
  unfiltered read (the `tenancy_cache_key()` fail-closed rule). An entity without the field adds no
  filter: under SCHEMA/DATABASE isolation the UoW provider routes by `current_tenant()` and itself
  refuses without one (`service/tenant.py:328`); under SHARED + RLS the GUC scopes it. `GLOBAL`
  entities are read unfiltered through `GlobalUoWProvider` when bound and appear in every tenant's
  export (documented; `exclude=` removes them).
- ✅ `tenant_field` defaults to `"tenant_id"`, matching `TenantAwareService._tenant_field`
  (`service/tenant.py:419`). It is a module argument for apps that renamed it.
- ❌ SHARED isolation, no RLS, and a tenant-partitioned entity *without* a tenant column cannot be
  detected, so it is read unfiltered. Pitfall row.

**§D-controller — Art. 15 information is supplied, never invented; only (b) is derived**
- ✅ Purposes, recipients, storage period, rights, complaint, source, automated decision-making and
  transfer safeguards are business and legal statements (brief 022 §2 `:35-38`: "CANNOT be derived
  from code"). The required fields have **no defaults**, so a controller states each one in
  source. `recipients=()` and `automated_decision_making="None."` are explicit answers, not
  omissions. Construction validates non-empty strings and a non-empty `purposes`, and coerces
  lists to tuples.
- ✅ A callable source covers per-tenant controllers (a SaaS where each tenant is its own
  controller). It is called once per build with `current_tenant()`.
- ✅ (b) is derived from the fields actually exported, the one item code can answer faithfully.
- ❌ 064's `Meta.personal_data_retention` is **not** copied per dataset. It is a developer statement
  (064 §D-retention); (d) comes from the controller only (Open question 4).

**§D-job — `JobQueue.enqueue(JobSpec)`, task registered by the module, request id =
`current_job().job_id`**
- ✅ 069a's API, unchanged. The route builds `JobSpec(task=exporter.task.payload(kind, subject_id,
  scope.value, requested_by), job_id=uuid4(), max_attempts=settings.max_attempts)`. Control knobs
  on `JobSpec`, task arguments in `TaskPayload` — no kwarg collision.
- ✅ The handler `DataSubjectExporter._run_job(subject_kind, subject_id, scope, requested_by) ->
  dict` reads `current_job()` (069a §D-context). Its `job_id` is the `request_id` (artifact id,
  audit id), so retries of one job converge on one artifact through `put`'s idempotent overwrite.
  `current_job() is None` raises `DataSubjectRequestError` (it must run under an executor).
  `current_tenant()` is whatever the executor entered from `Job.tenant_id`.
- ✅ The handler returns `manifest.to_dict()`. 069a's `encode_job_result` turns it into
  `Job.result`, so the status route reads the manifest from the job. Personal data never enters
  `Job.result`.
- ✅ **Task registration is the module's `start()`** at `Phase.SCHEDULERS`, before 069a's
  `JOB_DISPATCH_PHASE = SCHEDULERS + 50`, so the task exists before the first claim.
  `TaskRegistry.register` silently overwrites (`task.py:468-482`), so the module first checks
  `registry.get(EXPORT_TASK_NAME)`. Absent → register. The same `VarcoTask` object → no-op. A
  different object → `ValueError` (two exporters in one process). No resolvable `TaskRegistry` →
  INFO "this process can enqueue exports but not run them".
- ✅ Retries are opt-in through `max_attempts` (default 3, `VARCO_DSR_EXPORT_MAX_ATTEMPTS`). The
  handler is idempotent apart from a possible duplicate audit event, which is identifiable by
  `request_id` (§D-audit).
- ❌ Exports run only where a `JobDispatcherModule` **and** a `DataSubjectExportModule` are listed.
  A worker without the latter hits 069a §D-unknown: the job is released with back-off, an ERROR is
  logged, and it is terminal after `unknown_task_timeout`. Pitfall row.
- ❌ The raw subject id is persisted in `Job.task_payload` (needed to run and to recover). Job
  retention governs it. Pitfall row.
- Rejected — **`AbstractJobRunner.enqueue_task()`**: executes in-process (`runner.py:482-491`) and
  routes `**kwargs` into the task (premise 2).
- Rejected — **a module-level `@varco_task` function**: it would have to fetch the exporter from a
  global container. ⛔ `test_no_service_locator.py`.

**§D-artifact — an `ExportArtifactStore` seam, encrypted file store, TTL + retention, never
`Job.result`**
- ✅ The document is a concentrated copy of one person's data, special categories included. It
  must not sit in the jobs table (premise 5). The ABC is small: put/get/delete/delete_expired/
  delete_for_subject.
- ✅ Two implementations, stdlib only. `InMemoryExportArtifactStore` (lazily-created
  `asyncio.Lock`, dev/tests) and `FileSystemExportArtifactStore` (directory created `0o700`,
  `<id>.bin` + `<id>.meta.json` written to a temp file then `os.replace`d, files `0o600`, I/O via
  `asyncio.to_thread`). Object storage (S3, GCS) is the app implementing the ABC; the doc has a
  recipe. No new dependency.
- ✅ **`encryptor` is required on the file store**, the same rule as the durable webhook
  repositories (Plan 056 / FLIP-13). The dev hatch is `PlaintextFieldEncryptor(acknowledge_plaintext=True)`.
  Encryption uses `context=tenant_id` (`encryption.py:118`), so a tenant-aware registry keys per
  tenant. ⛔ No `encryptor=None` branch.
- ✅ Expiry is enforced twice: `get()` returns `None` past `expires_at` (and deletes best-effort),
  and `ExportArtifactRetentionTarget(store=…)` wraps `delete_expired()` for a `RetentionPolicy`
  (`supports_older_than=False`, `supports_dry_run=False`, the `IdempotencyRetentionTarget` shape,
  `retention/targets.py:175-222`). The CLAUDE.md "adapters over shipped verbs" rule holds, because
  `delete_expired()` is part of this new ABC from day one.
- ✅ `delete_for_subject(subject_key, tenant_id=)` ships now and is unused by 065 except in tests,
  so 066 can purge a subject's pending exports without adding an abstract method later.
- ✅ The DI default is a `@Fallback` `InMemoryExportArtifactStore` registered by the module (CLAUDE.md
  "every varco default is `@Fallback`"). `bind_instance(container, ExportArtifactStore, …)` wins.
- ❌ The in-memory default loses artifacts across pods (and with 069a, the job may run on another
  pod than the one serving the download → 404). Pitfall row + doc; production uses a shared volume
  or an object-store implementation (Open question 6: a posture finding).
- Rejected — **SA/Beanie artifact stores**: multi-MiB blobs in DB rows, and more backend code with
  no gain over a volume or an object store.
- Rejected — **store in `Job.result`**: premise 5.

**§D-audit — every export is recorded as evidence, without the data**
- ✅ `export()` (and the CLI through `record_audit`) produces `AuditEvent(entity_type=
  "DataSubjectExport", entity_id=str(request_id), action="export", actor_id=requested_by,
  tenant_id=current_tenant(), diff={"scope", "subject_kind", "subject_key", "records": {name: n},
  "bytes", "sha256", "acknowledged_unreached", "channel": "job"|"cli"})` on `"varco.audit"`
  (`service/audit.py:95`) through the injected `AbstractEventProducer`. Services hold a producer,
  never the bus (CLAUDE.md event rule). `AuditConsumer` persists it when the app wired auditing.
- ✅ `AuditEvent.action` widens to `Literal["create", "update", "delete", "export"]`. 066 adds
  `"erase"`. It is additive for producers, `AuditEntry.action` is already `str`, and the SA column
  is `String(16)`. CHANGELOG + `3-to-4.md` note for consumers that match on `action` exhaustively.
- ✅ `subject_key` (SHA-256 of `kind:id`) is recorded, not the raw id. Matching a known subject
  later is a recompute, and the evidence row does not hold the identifier. ❌ An unsalted hash of a
  low-entropy id (an email) is reversible by dictionary, so it is still pseudonymous personal data
  (brief 022 Version notes `:122`). 066 must treat it as such.
- ✅ Producer `None` (no bus wired) → manifest `audit_recorded=false` + INFO. Producer raises →
  ERROR log (no data), `audit_recorded=false`, and the export still succeeds. A subject's right is
  not blocked on an audit-bus hiccup, and the operator sees the flag.
- ❌ At-least-once: a retried job whose failure came after the produce emits a second event with
  the same `request_id`. Identifiable; accepted.
- Rejected — **the job row as the only evidence**: job retention sweeps it.
- Rejected — **`AuditingAuthorizer`**: the export never calls `authorize()`.

**§D-mount — operator-facing admin surface, the `mount_webhook_admin` barrier set, 202 + poll**
- ✅ `mount_data_subject_admin` has the same barriers as `mount_webhook_admin`
  (`webhook/mount.py:35-108`): `acknowledge_bundled_admin=True` required (`ValueError`);
  `server_auth` a required keyword (`TypeError` if omitted, `ValueError` if `None`, via
  `_tenant_scope.require_server_auth`); a `WeakSet` double-mount guard; never a `create_varco_app`
  keyword, never an env var (RD-9); `exporter.artifact_store is None` → `ValueError`. A router-level
  `admin_guard(server_auth, admin_role)` means unauthenticated/unauthorised → 401/403 before
  validation (never 422).
- ✅ Routes under `prefix` (default `/data-subject-requests`):

  | Method + path | Result |
  |---|---|
  | `POST /exports` body `{subject_kind, subject_id, scope="access", tenant_id?}` (`extra="forbid"`, `subject_id` ≤ 512) | 202 `JobAcceptedResponse(job_id, poll_url=…)` + `Location`; 422 unknown kind; 403 tenant mismatch without `cross_tenant_role` |
  | `GET /exports/{job_id}` | 200 `DataSubjectExportStatus` (job status, timestamps, sanitised `error`, `manifest` when COMPLETED); 404 when unknown, not an export job (`task_payload.task_name != EXPORT_TASK_NAME`), or `job.tenant_id` ≠ the caller's tenant without `cross_tenant_role` (the by-id 404 rule, `webhook/router.py:22-24`) |
  | `GET /exports/{job_id}/document` | 200 `application/json`, `Content-Disposition: attachment; filename="data-subject-export-<job_id>.json"`, `Cache-Control: no-store`; 409 not COMPLETED; 410 past `artifact_expires_at`; 404 artifact missing (deleted, or a different store instance) |
  | `DELETE /exports/{job_id}/document` | 204, idempotent (an already-deleted artifact is still 204); 404 rules as above |

  Every response sets `Cache-Control: no-store`.
- ✅ `requested_by` = the resolved ctx's `user_id` when it is an `AuthContext`, else `None`. No raw
  token is ever stored: `JobSpec` has no token field.
- ❌ No list route. The job store has no task-name index, and a list of DSAR subjects is itself
  sensitive. The operator keeps the job id.
- Rejected — **a self-service route** (Non-goals).

**§D-redaction — the document is never redacted; nothing *about* an export carries its data**
- ✅ The document is for the subject. No `Redactor` touches it, including a
  `set_default_redactor()` the app installed. Test: a classified `password_hint` field is exported
  verbatim.
- ✅ Logs, `DataSubjectRequestError` messages, `Job.error`, the manifest and the audit diff carry
  request id, kind, entity/field names, counts, sizes and hashes — never a record value and never
  the raw subject id. `DataSubject.__repr__` masks the id, so an accidental `%r` cannot leak it.
- ✅ The handler wraps any contributor exception as `DataSubjectRequestError(f"contributor {name!r}
  failed: {type(exc).__name__}") from exc`, so `Job.error` (`str(exc)`) is sanitised. Driver
  exceptions can embed bound parameters (SQLAlchemy `[parameters: …]`), and the chained traceback in
  logs still may: Pitfall row (`create_async_engine(…, hide_parameters=True)`).

**§D-size — fail, never truncate; NDJSON is the follow-up**
- ✅ The document is built in memory with a running byte count. Past `max_bytes` (default 64 MiB),
  `DataSubjectRequestError` names the limit and `VARCO_DSR_EXPORT_MAX_BYTES`, and the export aborts
  at once, never after building everything. A truncated answer to a DSAR is worse than a failed job.
- ✅ One subject's data is usually small. 64 MiB covers it, with a peak memory of about 2–3× the
  limit (documented).
- ❌ Data-heavy subjects (stored uploads in DB columns) need a raised limit. Parked: an
  `ExportFormat.NDJSON` reusing Plan 067's `varco_core.ndjson.encode_record`/`encode_error` and
  `NDJSON_MEDIA_TYPE` (`067:442-443`). It needs a streaming `ExportArtifactStore` writer and a
  chunked encryption framing (Fernet is whole-message), which is a design of its own (Open
  question 1).

**§D-module — a `VarcoModule`, not an `enable_*` verb**
- ✅ The feature owns startup work: it forces exporter construction (the coverage check fails the
  lifespan, not a DSAR) and registers its task before the dispatcher's first claim. A binding-only
  verb cannot do either. CLAUDE.md: "a switchable subsystem is a `VarcoModule`".
- ✅ `_register`: `bind_settings(container, DataSubjectExportSettings, instance=settings)` (env path
  when `None`). The per-entity contributor factories (§D-auto). A `@Fallback`
  `InMemoryExportArtifactStore` provider. A non-fallback `@Provider(singleton=True)` →
  `DataSubjectExporter` that declares `contributors: list[DataSubjectContributor]`,
  `settings: DataSubjectExportSettings`, `artifact_store: ExportArtifactStore` and an optional
  `AbstractEventProducer`, and closes over `catalog`/`controller`/`acknowledge_unreached`. An
  existing user binding of `DataSubjectExporter` → `AlreadyBoundError` (§Wiring contract).
  `async_init`: `self._exporter = await container.aget(DataSubjectExporter)`. `start`: task
  registration (§D-job). `stop`: no-op. No lifecycle-bearing provider, so the `di_wiring` disposer
  rule has nothing to own.
- ✅ Constructor validation is immediate: an empty catalog with no subject-linked entity is fine
  (hand-written contributors only); a `tenant_field` that is an empty string → `ValueError`;
  `exclude_fields` keys that are not catalog entities → `ValueError`.
- ❌ Not a `Feature` (`di/features.py:52-89`): features are binding-shaped `enable_*` swaps with an
  env twin; this needs code-supplied catalog/controller. Stated in the doc so nobody adds
  `enable_data_subject_export()`.
- Rejected — **`enable_data_subject_export(container, …)`** (this plan's first draft):
  ❌ registration order vs `JobDispatcherModule` and the fail-fast check would both be the app's
  problem.

**§D-cli — `varco gdpr export`, operator-run, inline (no job)**
- ✅ `varco gdpr export --target module:callable --kind K --id ID|- [--tenant T]
  [--scope access|portability] [--requested-by NAME] -o FILE|- [--force] [--path DIR]`.
  `--target` (env fallback `VARCO_GDPR_EXPORT_TARGET`) resolves to a `DataSubjectExporter`, a
  zero-arg callable returning one, or a zero-arg `async` callable. The CLI has no container, the
  same reason as `retention --target` (`cli/retention.py:13-15`).
- ✅ It runs `build()` under `tenant_context(--tenant)` and writes the document. A file is created
  `0o600` and existing files are refused without `--force`; `-o -` means stdout, explicitly. It
  then calls `record_audit(…, channel="cli")` and prints the manifest JSON (no personal data) to
  stdout, or to stderr when the document went to stdout. `--id -` reads the id from stdin, so it
  stays out of shell history.
- ✅ Exit codes: 0 ok; 1 `DataSubjectRequestError`/runtime failure (message on stderr, no data);
  2 usage (no `-o`, unresolvable target, unknown kind).
- ❌ No artifact store and no job: the operator owns the file. The doc warns.

### Alternatives considered

- **Store the document in `Job.result`**: rejected — §D-artifact / premise 5 (PII in the jobs
  table, unencrypted, swept by job retention). ✅ zero new storage.
- **One catalog-wide contributor**: rejected — ❌ no per-entity replacement, ❌ `varco diagnose`
  shows one opaque entry. ✅ fewer bindings.
- **Separate erase seam for 066**: rejected — §D-seam.
- **Declarative indirect links**: rejected — §D-indirect.
- **Classified-fields-only access export**: rejected — §D-rows.
- **Reads through `AsyncService.stream()`**: rejected — §D-reads.
- **`enqueue_task()` / a module-level task**: rejected — §D-job.
- **`enable_*` verb**: rejected — §D-module.
- **Truncate on overflow**: rejected — §D-size.
- **Audit with the raw subject id**: rejected — §D-audit (hash recorded; the raw id stays in the
  job row only, under job retention).

### Handoff to Plan 066 (what erasure may rely on, and what it must add)

May rely on: `DataSubject`, `subject_key()`, `DataSubjectRequestError`, the `@Multibound`
`DataSubjectContributor` with `name`/`subject_kinds`/`covers`, the coverage check and
`acknowledge_unreached`, `EntityDataSubjectContributor` (its query construction, tenant fail-closed
rule, `exclude`/`exclude_fields`), `ExportArtifactStore.delete_for_subject()`,
`DataSubjectExportModule`'s registration shape, `build_data_subject_router`/`mount_data_subject_admin`,
and the `gdpr` CLI group.

Must add (all additive):
1. `DataSubjectContributor.erase(subject, *, dry_run) -> ErasureOutcome`, a **concrete** method
   whose default returns `NOT_SUPPORTED` (§D-seam). `EntityDataSubjectContributor.erase` uses
   064 Q2's defaulted `PersonalData(erasure=…)` strategy.
2. A `DataSubjectEraser` orchestrator + `EXPORT`-style task name (`varco.data_subject.erase`)
   enqueued via `JobQueue`; an erasure module or a `DataSubjectExportModule` sibling.
3. `AuditEvent.action` gains `"erase"`; a decision on how prior `"export"` audit records (which
   hold `subject_key`) are treated (Art. 17(3)(e) legal claims; brief 022 §4 `:67-73`).
4. Purge pending export artifacts of the erased subject via `delete_for_subject()`, and decide the
   `Job.task_payload` copies of the raw subject id.
5. Export of an already crypto-shredded subject: `KeyDestroyedError` (`encryption.py:183`) raised by
   the mapper during `stream_by_query` currently fails the export. 066 decides (e.g. the row is
   reported as erased).
6. Routes `POST {prefix}/erasures` etc. and an `eraser=` keyword on `mount_data_subject_admin`;
   a `varco gdpr erase` verb.
7. The "Erasure (Art. 17)" section of `technical_docs/features/data-subject-requests.md` (this plan
   leaves the heading as a placeholder).

## Steps

1. [ ] **Tests (failing) — value types, seam, controller information, settings.**
   New `varco_core/tests/test_data_subject_base.py`. `DataSubject`: kind slug rule (`"Customer"`,
   `""`, `"a b"` → `ValueError`); id empty/over 512 → `ValueError`; id not stripped; frozen and
   hashable; `repr()` contains `<redacted>` and not the id. `subject_key` is stable and equals the
   SHA-256 of `"kind:id"`. `ExportScope` values. `ExportField.portable` is true only for
   PROVIDED/OBSERVED; `special_category` follows the category. `DataSubjectContributor` cannot be
   instantiated without `name`/`subject_kinds`/`export`; `label` defaults to `name`, `covers` and
   `fields` default to `()`. The seam is `@Multibound`:
   `contribute(container, DataSubjectContributor, a)` + `b` → `collect()` returns both; zero
   contributions → `()`. `ControllerInformation`: the required fields have no defaults
   (`TypeError` when omitted); empty/whitespace strings and empty `purposes` → `ValueError`; a
   `list` is coerced to a `tuple`; `recipients=()` is accepted. `DataSubjectExportSettings`:
   defaults (64 MiB, 7 days, 3); `max_bytes<=0`, `artifact_ttl<=0`, `max_attempts<1` →
   validation error; env prefix `VARCO_DSR_EXPORT_` (monkeypatch). Update
   `varco_core/tests/test_di_collect.py`: `collection_seams()` returns **six** seams including
   `DataSubjectContributor`, and the lazy-import guard's module tuple gains
   `"varco_core.data_subject.base"`.
2. [ ] **Implement** `varco_core/varco_core/data_subject/__init__.py` (re-exports; docstring with
   the ⛔ no-scanned-decorator rule, in the spirit of `redaction/__init__.py:30-36`), `base.py`
   (§D-seam DESIGN block, including the reserved `erase`/`erasure_*` names), `controller.py`
   (§D-controller; each field's docstring quotes its Art. 15 paragraph), `settings.py` (a
   `VarcoSettings` subclass, `model_config = SettingsConfigDict(env_prefix="VARCO_DSR_EXPORT_")`).
   In `varco_core/varco_core/di/collect.py` add `DataSubjectContributor` to `collection_seams()`
   (import inside the function body; docstring: six seams). Full `Args/Returns/Raises/Edge
   cases/Async safety` docstrings, `from __future__ import annotations`, frozen dataclasses.
   Step 1 green.
3. [ ] **Tests (failing) — value encoding + the catalog contributor.**
   `varco_core/tests/test_data_subject_document.py`: every `to_json_value` rule in §Design
   (aware datetime → `…Z`, naive kept, `Decimal("1.10")` → `"1.10"`, bytes → base64, `Enum`, a set
   sorted, a nested dataclass, a nested `BaseModel`, NaN → `"nan"`); an unsupported type →
   `TypeError` naming `dataset.field` and not the value. New helper
   `varco_core/tests/_data_subject_fakes.py`: a fake repository that records the `QueryParams` it
   receives and evaluates `eq`/`and`/`or` nodes over a list (read the node classes in
   `varco_core/varco_core/query/` first), a fake UoW exposing repos under
   `repository_attribute()` names, and a fake `IUoWProvider`.
   `varco_core/tests/test_data_subject_entity_contributor.py` (models classified with 064 markers):
   - a link on `pk` and on a business field; two links of one kind are OR'ed; a kind the entity is
     not linked to → no rows;
   - UUID/int/str coercion; coercion failure → `DataSubjectRequestError` naming `Entity.field`,
     message free of the id;
   - with a tenant active and the entity carrying `tenant_id` → the filter is present; with no
     tenant → `DataSubjectRequestError` and no repository call; an entity without the field → no
     filter; `tenant_field=None` disables the rule; a custom `tenant_field`;
   - the `tenant_field` and `_`-prefixed attributes are absent from records; `exclude_fields`
     honoured;
   - `fields` carries the category/origin for classified fields and `None` for the others;
   - records sorted by pk; soft-deleted rows (a `deleted_at` set) are included;
   - a missing UoW attribute → `LookupError` naming the entity;
   - the iterator is closed on early exit (`aclosing`).
   New `varco_sa/tests/test_data_subject_sa_sqlite.py` (aiosqlite, the pattern of
   `varco_sa/tests/test_sa_module_rls.py`): a real `SQLAlchemyRepositoryProvider`, two entities
   with links, the contributor returns exactly the subject's rows ordered by pk, and an
   `EncryptedHint` field comes back as plaintext (wire a `FernetFieldEncryptor` the way the SA
   encryption tests do).
4. [ ] **Implement** `varco_core/varco_core/providers.py` — `repository_attribute(cls) -> str`
   (docstring: the rule `make_uow` documents). `varco_sa/varco_sa/provider.py` and
   `varco_beanie/varco_beanie/provider.py`: `_repo_attr` delegates to it, with behaviour
   unchanged and existing tests green. `varco_core/varco_core/data_subject/document.py`
   (`EXPORT_SCHEMA_VERSION = 1`, `EXPORT_MEDIA_TYPE = "application/json"`, `to_json_value`) and
   `entity.py` (§D-rows, §D-reads, §D-tenant DESIGN blocks). Step 3 green. Then **integration
   tests** (`pytestmark = pytest.mark.integration`, per-run `uuid4().hex[:8]` table/collection
   names, the shared session containers): `varco_sa/tests/test_data_subject_integration.py`
   (`postgres_url`: links, a tenant filter under SHARED with a `tenant_id` column, an encrypted
   field, UUID subject ids, 1 000 rows streamed in pk order) and
   `varco_beanie/tests/test_data_subject_integration.py` (`mongo_url`: the same minus encryption,
   UUID BSON representation). Run with `-m integration`.
5. [ ] **Tests (failing) — exporter, artifact stores, retention target, job handler.**
   `varco_core/tests/test_data_subject_exporter.py`:
   - document: schema v1 keys and order exactly; `access` vs `portability` (derived/unclassified
     fields dropped, undeclared keys handled per §Design); the derived `personal_data_categories`
     (only datasets with records, enum order); controller info from an instance and from an async
     callable that receives `current_tenant()`; deterministic bytes with a fixed `clock`; zero
     records → a valid document with empty datasets; datasets sorted by `(label, name)`;
   - contributors whose `subject_kinds` exclude the kind are skipped; an unknown kind →
     `DataSubjectRequestError`;
   - coverage: an unreached personal-data entity → `ValueError` at construction listing it; the
     same entity acknowledged → construction succeeds and the manifest lists it; covered by a
     hand-written contributor's `covers` → fine; duplicate names → `ValueError`;
   - `max_bytes` exceeded → `DataSubjectRequestError` raised before every contributor is drained
     (a sentinel contributor after the overflow is never iterated);
   - a contributor raising → `DataSubjectRequestError` with the sanitised message and `__cause__`
     set;
   - the document is not redacted even with `set_default_redactor()` installed (restore it in
     teardown);
   - audit: with a recording producer, one `AuditEvent` on `"varco.audit"`, `action == "export"`,
     a diff with `subject_key` and counts and no record values and no raw id; producer `None` →
     `audit_recorded is False`; producer raising → export succeeds, `audit_recorded is False`;
   - `caplog` across a full export: no record value and no raw subject id in any message.

   `varco_core/tests/test_data_subject_artifacts.py`, both stores (parametrised): put/get round
   trip; overwrite is idempotent; `get` past expiry → `None`; `delete` → `True`/`False`;
   `delete_expired(now=)` counts; `delete_for_subject` filters by key and tenant. File store only:
   files `0o600`, directory `0o700`; a ciphertext on disk ≠ the plaintext; the encryptor receives
   `context=tenant_id`; `encryptor` omitted → `TypeError`; an orphan temp file from a simulated
   crash is ignored by `get`.

   `varco_core/tests/test_retention_export_artifact_target.py`: `supports_older_than is False`;
   `older_than` given → `ValueError`; `dry_run` → skipped outcome, nothing deleted; a real run
   reports `deleted`.

   `varco_core/tests/test_data_subject_task.py` (enter the job scope the way 069a's
   `varco_core/tests/test_job_context.py` does): `exporter.task.name == EXPORT_TASK_NAME`; the
   payload round-trips through `TaskPayload.to_dict`/`from_dict`/`invoke`; under a job scope with
   `tenant_context("t1")` the handler stores the artifact under `current_job().job_id` with
   `tenant_id="t1"` and `expires_at = clock() + artifact_ttl`, and returns `manifest.to_dict()`
   (JSON-serialisable, no raw id); outside a job scope → `DataSubjectRequestError`.
6. [ ] **Implement** `varco_core/varco_core/data_subject/exporter.py` (§D-job, §D-audit,
   §D-redaction, §D-size DESIGN blocks), `artifacts.py` (§D-artifact; the lazily created lock;
   `asyncio.to_thread` file I/O), `ExportArtifactRetentionTarget` in
   `varco_core/varco_core/retention/targets.py` (+ export from `varco_core/varco_core/retention/__init__.py`),
   and widen `varco_core/varco_core/event/audit_event.py:115` to
   `Literal["create", "update", "delete", "export"]` (update the docstrings at
   `audit_event.py:69` and `service/audit.py:121,137`). Step 5 green.
7. [ ] **Tests (failing) — module, DI, scan guard, end to end on the dispatcher.**
   `varco_core/tests/test_data_subject_module.py`:
   - `_register` contributes one `EntityDataSubjectContributor` per subject-linked entity
     (`collect()` count and names), none for `exclude=` entities, and every contribution binding is
     non-fallback with `priority == 0`;
   - the `ExportArtifactStore` default is the `@Fallback` in-memory store, and a user
     `bind_instance(container, ExportArtifactStore, …)` wins;
   - settings come through `bind_settings` (env path; `instance=` path);
   - `async_init` builds the exporter, and an unreached entity makes `async_init` raise;
   - a pre-existing user `DataSubjectExporter` binding → `AlreadyBoundError`;
   - `start()` registers the task in a bound `TaskRegistry`; a second module in the same process
     with a different exporter → `ValueError`; no `TaskRegistry` resolvable → INFO and no error;
   - `exclude_fields` with an unknown field or a non-catalog entity → `ValueError`; `tenant_field=""`
     → `ValueError`;
   - a GLOBAL entity uses a bound `GlobalUoWProvider`, else `IUoWProvider`;
   - **end to end**: `InMemoryJobStore` + `JobQueue` + 069a's `JobDispatcher.run_once()` under
     `tenant_context("t1")` → COMPLETED, `Job.tenant_id == "t1"`, the manifest in `Job.result`,
     the artifact readable from the store.
   `varco_core/tests/test_data_subject_no_di_side_effect.py` (shape of
   `test_retention_no_di_side_effect.py`): no providify binding marker on any
   `varco_core.data_subject` object other than the `@Multibound` stamp on the seam;
   `container.scan("varco_core", recursive=True)` yields zero `DataSubjectContributor` bindings and
   no `DataSubjectExporter`/`ExportArtifactStore` binding. `varco_core/tests/test_lazy_init.py`
   parity for the four new top-level names.
8. [ ] **Implement** `varco_core/varco_core/data_subject/module.py` (§D-module, §D-auto DESIGN
   blocks; `IUoWProvider`/`GlobalUoWProvider`/`DataSubjectExportSettings` imported at module
   scope for provider-annotation resolution) and add `DataSubject`, `DataSubjectContributor`,
   `DataSubjectExporter`, `ControllerInformation` to `varco_core/varco_core/__init__.py` (`_LAZY`,
   the `TYPE_CHECKING` block, `__all__`). Step 7 green.
9. [ ] **Tests (failing) — admin mount + CLI.**
   `varco_fastapi/tests/test_data_subject_admin.py` (FastAPI `TestClient`/`httpx.AsyncClient`, the
   `server_auth` fake shape of `varco_fastapi/tests/test_webhook_router.py` or
   `test_audit_router.py` — read one first):
   - mount guards: `acknowledge_bundled_admin` false → `ValueError`; `server_auth=None` →
     `ValueError`, omitted → `TypeError`; a second mount → `ValueError`; an exporter with no artifact
     store → `ValueError`;
   - anonymous → 401/403 before 422; wrong role → 403;
   - POST → 202 with `Location` and a `poll_url` that equals the GET path; the job is PENDING in the
     store with the export task name, `Job.tenant_id` equal to the caller's tenant, no
     `request_token`, and `max_attempts` from settings;
   - an unknown kind → 422; `extra` keys → 422; a 513-char id → 422;
   - tenant: another `tenant_id` without the role → 403; with the role → 202 and
     `Job.tenant_id == requested`; a single-tenant app (no tenant, none requested) → 202 with
     `tenant_id=None`;
   - GET status: an unknown id, a non-export job, and a cross-tenant job without the role → 404; a
     COMPLETED job returns the manifest; a FAILED job returns the sanitised error;
   - document: pending → 409; completed → 200 with bytes equal to the store's, `no-store`,
     `attachment`; after the artifact TTL → 410; after DELETE → 404; DELETE twice → 204;
   - end to end through `JobDispatcher.run_once()` between POST and GET.
   `varco_core/tests/test_cli_gdpr_export.py` (call `main([...])`; target fixtures in a `tmp_path`
   package added via `--path`):
   - the instance, sync-factory and async-factory target forms;
   - writes `-o FILE` with mode `0o600` and prints the manifest; an existing file without `--force`
     → 1; `-o -` → the document on stdout and the manifest on stderr; `--id -` reads stdin;
   - `--tenant` enters `tenant_context` (assert through a contributor that records
     `current_tenant()`);
   - `record_audit` called with `channel="cli"`;
   - no `-o` → 2; an unresolvable target → 2; an unknown kind → 2; `DataSubjectRequestError` → 1
     with no subject id on stderr; `VARCO_GDPR_EXPORT_TARGET` fallback.
10. [ ] **Implement** `varco_fastapi/varco_fastapi/data_subject/__init__.py`, `router.py`
    (`build_data_subject_router(exporter, *, job_queue, job_store, server_auth, admin_role,
    cross_tenant_role, prefix)`; reuses `varco_fastapi.admin._tenant_scope`'s `admin_guard`,
    `resolve_ctx`, `require_server_auth`, `allow_cross_tenant`; pydantic body + a
    `DataSubjectExportStatus` model; §D-tenant mapping; DESIGN block) and `mount.py` (§D-mount
    DESIGN block). Add the `export` verb to `varco_core/varco_core/cli/gdpr.py` (§D-cli; heavy
    imports inside `_run`; `cli/main.py`'s docstring list updated if it enumerates verbs).
    Step 9 green.
11. [ ] **Docs (same change — never a follow-up).**
    - New `technical_docs/features/data-subject-requests.md` (shared with 066):
      - the opening "position, not legal advice / no claim of GDPR compliance" banner in
        `docs/regulatory-posture.md`'s voice;
      - what it does and does not do;
      - an Art. 15(1)(a)-(h)/15(2)/15(3)/Art. 20 table (derived by varco vs supplied by the
        controller vs out of scope), citing brief 022 §3;
      - the seam; catalog contributors and the coverage check; hand-written contributor recipes
        (indirect link `Comment → Post → author`, external store, unlinked entity, audit trail by
        `actor_id`);
      - scopes and §D-rows; the document schema v1 and the value encoding; `ControllerInformation`;
        tenancy (§D-tenant, including "one export per tenant");
      - running it: the module, `JobDispatcherModule`, the admin routes table, the CLI;
      - artifact storage (an object-store recipe, encryption, TTL + a `RetentionPolicy` recipe with
        `ExportArtifactRetentionTarget`); audit evidence; the redaction and logging rules;
      - an "Identity verification is yours (Art. 12(6))" section;
      - a **Pitfalls** table: in-memory store across pods / a dispatcher on another pod; a worker
        without `DataSubjectExportModule` (069a §D-unknown); unclassified entities are invisible;
        whole rows carry others' data (Art. 15(4)); a custom `_tenant_field`; SHARED without RLS
        and without a tenant column; driver exceptions leaking parameters (`hide_parameters=True`);
        `max_bytes`; request/response body logging on the admin prefix; the CLI id in shell
        history; raw ids in job rows (job retention); async-provider contributions need `aget`;
        not a snapshot; soft-deleted rows are included by design; an export of a crypto-shredded
        subject fails until 066;
      - a `## Erasure (Art. 17)` heading with a one-line "Plan 066" placeholder.
    - `mkdocs.yml` nav beside 064's "Personal-data classification".
    - `README.md`: a "Data-subject requests (export)" section (the §Design usage + the CLI line)
      and a TOC entry.
    - `ARCHITECTURE.md`: a `data_subject/` line in the `varco_core` map, a
      `varco_fastapi/data_subject/` line, and a short type-hierarchy block near 064's
      "Personal-data classification".
    - `technical_docs/features/dependency-injection.md` §Multi-binding: six shipped seams (add the
      row; fix every "five").
    - `technical_docs/features/admin-surface-tenancy.md`: a row for `mount_data_subject_admin`
      (tenant-bound, `cross_tenant_role`, by-id 404).
    - `technical_docs/features/retention-and-purge.md`: an `ExportArtifactRetentionTarget` row.
    - `technical_docs/features/database-auditing.md`: the `"export"` action value.
    - `docs/regulatory-posture.md`: one paragraph under 064's GDPR section linking the new doc
      ("helps you produce", never "compliant").
    - `CLAUDE.md`: a ≤ 10-line `### Data-subject requests (varco_core.data_subject, Plan 065 /
      GDPR-2)` section of rules, ending with the doc link:
      1. "all sources of a subject's data" is the `@Multibound` `DataSubjectContributor` — never a
         hand list;
      2. the export reads repositories, not services, and is authorised by the admin mount, never
         a self-service route;
      3. one tenant per export, from `current_tenant()` via `Job.tenant_id`, and the catalog
         contributor fails closed;
      4. the document is never redacted, and logs/errors/`Job.result`/audit never carry record
         values or the raw subject id;
      5. the document lives in an `ExportArtifactStore` with a TTL, never in `Job.result`, and the
         file store's `encryptor` is required;
      6. `DataSubjectExportModule` is a module, never an `enable_*`;
      7. 066 adds `erase` as a concrete, fail-loud default — never abstract.

      Also add `mount_data_subject_admin` to the Plan 054 FLIP-3/4 rule's list; change the
      Decision Tree's "five shipped seams" to six and add a branch: *"Answering a data-subject
      access/portability request? → `varco_core.data_subject` (`DataSubjectContributor` +
      `DataSubjectExportModule`) + `mount_data_subject_admin` + `varco gdpr export`; erasure →
      Plan 066"*.
    - `CHANGELOG.md` `[4.0.0]`: a `### Added` bullet group `(Plan 065, GDPR-2)` and a `### Changed`
      bullet for the `AuditEvent.action` widening.
    - `technical_docs/migrations/3-to-4.md`: a short "new audit action value `export`" row (only
      code matching `action` exhaustively is affected).
12. [ ] **Snapshot gates + full verification.**
    - `uv run python scripts/api_surface.py` (four new top-level names) → commit
      `design/api-freeze-and-standards/measurements/api-surface.{json,md}`.
    - `make entry-points` (`DataSubjectExportModule`, `mount_data_subject_admin`) → commit
      `technical_docs/entry-points.md`, its JSON, and the `ARCHITECTURE.md` marked block.
    - `make settings-reference` (`DataSubjectExportSettings`) → commit
      `technical_docs/settings-reference.md` and its JSON.
    - `make di-catalogue-check`: expected **unchanged**, because no bootstrap or default module
      lists the new module. If it moves, the only acceptable cause is the sixth collection seam
      appearing in a seams listing; then regenerate and commit. Any other movement → stop and find
      out why.
    - `make asyncapi-check` unchanged; `uv run python scripts/import_budget.py --check --warn-only`
      (names are lazy; expect no delta).
    - Then the §Verification block.

## Edge cases

- Subject with no rows anywhere → valid document, every dataset `records: []`, manifest's
  per-contributor `records` map has every count 0, job COMPLETED.
- Same subject in tenants A and B → two exports, one per tenant; never one document.
- Cross-tenant operator, no resolved tenant, none requested → job runs with `tenant_id=None` →
  the first tenant-partitioned entity raises → job FAILED "run the export inside a tenant".
- Single-tenant app (no tenant columns, no tenant resolution) → `tenant_id=None`, export succeeds.
- Entity linked to two kinds → serves both; a request for kind A uses only A's link fields.
- `Message(sender_id, recipient_id)` both kind `user` → rows where either matches; the other
  party's id is in the row (Pitfall; `exclude_fields`).
- Subject-id fields of one kind with different types (UUID vs int) and an id valid for only one →
  `DataSubjectRequestError` naming the field, never the id.
- Hand-written contributor yields a key with no `ExportField` → unclassified descriptor under
  `access`, dropped under `portability`.
- Hand-written contributor yields a non-JSON type → `TypeError` inside the export → job FAILED
  naming `dataset.field`.
- Two contributors with one `name` → `ValueError` at `async_init` (startup).
- Unreached personal-data entity → startup `ValueError` unless acknowledged; acknowledged names
  appear in every manifest, never in the document.
- Document over `max_bytes` → job FAILED immediately, nothing stored, no audit event.
- Artifact store `put` fails → job FAILED (retried up to `max_attempts`), no audit event.
- Audit producer missing/failing → export succeeds, `audit_recorded=false`.
- Job retried after a partial run → same `request_id`, artifact overwritten, at most one extra
  audit event with the same `request_id`.
- Worker pod without `DataSubjectExportModule` → 069a §D-unknown release/back-off, ERROR log,
  terminal after `unknown_task_timeout`.
- Download after TTL → 410; after DELETE or from a pod with a different in-memory store → 404.
- `GET /exports/{id}` for a non-export job id → 404 (no oracle over the job table).
- `KeyDestroyedError` while reading a shredded field → job FAILED (066 owns the fix).
- Contributor bound via an `async def @Provider` → `collect()` would raise; the module uses
  `aget`/`acollect`, and the mount recipe's sync `container.get(DataSubjectExporter)` must become
  `await container.aget(...)` in that case (documented).
- CLI `-o` pointing at an existing file without `--force` → exit 1, file untouched.

## Verification

```bash
uv run pytest varco_core/tests/test_data_subject_base.py varco_core/tests/test_data_subject_document.py \
  varco_core/tests/test_data_subject_entity_contributor.py varco_core/tests/test_data_subject_exporter.py \
  varco_core/tests/test_data_subject_artifacts.py varco_core/tests/test_retention_export_artifact_target.py \
  varco_core/tests/test_data_subject_task.py varco_core/tests/test_data_subject_module.py \
  varco_core/tests/test_data_subject_no_di_side_effect.py varco_core/tests/test_cli_gdpr_export.py \
  varco_core/tests/test_di_collect.py varco_core/tests/test_lazy_init.py varco_core/tests/test_wiring_report.py -q
uv run pytest varco_fastapi/tests/test_data_subject_admin.py varco_sa/tests/test_data_subject_sa_sqlite.py -q
uv run pytest varco_sa/tests/test_data_subject_integration.py -m integration -q
uv run pytest varco_beanie/tests/test_data_subject_integration.py -m integration -q
make test                    # all eleven suites
make lint                    # ruff + format + every snapshot gate (no PKG)
make type-check              # mypy strict
uv run python scripts/api_surface.py --check
uv run varco gdpr export --help
# wording guard — must print nothing:
rg -n -i "gdpr[- ]compliant|compliant with (the )?gdpr|ensures? (gdpr )?compliance|guarantees? (gdpr )?compliance|gdpr compliance" \
  technical_docs/features/data-subject-requests.md docs/regulatory-posture.md README.md CLAUDE.md \
  CHANGELOG.md varco_core/varco_core/data_subject varco_fastapi/varco_fastapi/data_subject varco_core/varco_core/cli/gdpr.py
# no scanned decorator in the package — must print nothing:
rg -n "^@(Singleton|Provider|Configuration)" varco_core/varco_core/data_subject varco_fastapi/varco_fastapi/data_subject
```

## Risks

- ⚠️ **ASSUMPTION — the Art. 15(1)(a)-(h) and 15(2) item list** is taken from brief 022 §3
  (`:46-48`), not from the Regulation text. Check each `ControllerInformation` field docstring and
  the doc's mapping table against EUR-Lex 32016R0679 Art. 15 at Step 11; reword field docs, never
  field names (field names enter the API freeze).
- ⚠️ **ASSUMPTION — Art. 20 scope** (provided + observed in, derived out, consent/contract basis
  only) comes from brief 022 §3 (`:53-54`), citing WP242 rev.01 via a generic Art. 29
  documentation link. `PersonalDataOrigin` labelling is the controller's judgement (064 Risks). The
  doc says whether portability applies is the controller's call.
- ⚠️ **ASSUMPTION — JSON satisfies "structured, commonly used and machine-readable"** (brief 022
  §3 `:55`, ICO/EDPB convergence as reported by the brief).
- ⚠️ **ASSUMPTION — whole rows linked to the subject are "personal data relating to" them** (Art.
  4(1)). Not sourced from a brief. It drives §D-rows; the Art. 15(4) third-party limit is the
  documented counterweight.
- ⚠️ **ASSUMPTION — in multi-tenant SaaS the tenant is often the controller and the platform a
  processor** (Art. 28). Not sourced from a brief. It drives "one tenant per export"; that rule is
  safe regardless (it never aggregates).
- ⚠️ **ASSUMPTION — identity verification is not prescribed** (brief 022 §3 `:59`, Evidence gap 5).
  It drives "operator-facing only".
- ⚠️ **ASSUMPTION — the one-month deadline (Art. 12(3))** is quoted from brief 022 §3 (`:50`) in the
  doc's out-of-scope note only; varco tracks nothing.
- **Upstream plans not yet built.** 064 and 069a are plans; their names (`EntityClassification.
  subject_fields`, `is_subject_linked`, `holds_personal_data`, `JobSpec`, `JobQueue.enqueue`,
  `Job.tenant_id`, `current_job`, `JobDispatcher.run_once`, `JOB_DISPATCH_PHASE`) are used verbatim.
  Invariant: if either plan's build renames one, this plan's code follows the built name. A
  semantic change, e.g. `JobQueue` no longer stamping the tenant, stops this build for a re-plan.
- **Forcing a decision at startup.** The coverage check refuses to start the module with an
  unreached entity. Invariant: the refusal happens only when the module is listed, and the error
  names both remedies.
- **PII leakage through side channels.** Invariant: Step 5's `caplog` test plus the sanitised
  `Job.error` and the masked `DataSubject.__repr__`. Residual: chained driver tracebacks in logs
  (Pitfall + `hide_parameters=True`).
- **Artifact at rest.** Invariant: the file store cannot be built without an encryptor; TTL is
  enforced on read and by retention. Residual: an app-written object-store implementation must
  encrypt itself (recipe says so).
- **No snapshot consistency.** A write during an export can appear in one dataset and not another.
  Accepted for 4.0; Plan 062's read-only boundary is the later fix.
- **Memory.** Peak ≈ 2–3× `max_bytes` (in-memory build). Invariant: the overflow check aborts on
  the record that crosses the limit.
- **`AuditEvent` widening.** Exhaustive `match` statements on `action` in app code see a new
  value. CHANGELOG + `3-to-4.md` row.
- **Sixth collection seam.** `test_collection_seams_returns_five_seam_types` and the DI guide/
  CLAUDE.md "five" wording must all move together (Steps 1, 11).
- **Refactor of `_repo_attr`.** Invariant: SA/Beanie UoW attribute names byte-identical (their
  existing UoW tests stay green).
- **Wording.** Any doc line implying compliance is a defect; the Verification `rg` guards it.

## Open questions

1. **NDJSON export format for data-heavy subjects** — reuse Plan 067's `encode_record`/
   `NDJSON_MEDIA_TYPE`, but it needs a streaming artifact writer and a chunked encryption frame.
   File a follow-up row now, or wait for a real over-limit export?
2. **Built-in contributors for framework tables** — the audit trail by `actor_id`, webhook
   subscriptions by tenant, job rows. They are personal-data locations varco itself owns. Should 065
   ship any (opt-in contributions), or leave them as doc recipes?
3. **Declarative indirect links** — rejected for now (§D-indirect). Revisit only on a concrete
   request?
4. **Per-dataset labels and stated retention in the document** — should 064's
   `Meta.personal_data_retention` (a developer statement) or a `Meta`-level dataset label reach the
   subject-facing document?
5. **Delivery evidence** — record a second audit event when an operator downloads or deletes the
   artifact (`action="export"` with `channel="download"`)?
6. **Posture finding for `InMemoryExportArtifactStore` in production** — INFO only (a WARN would
   block start)?
7. **CSV output** (WP242 lists it as acceptable for flat data) — demand-driven?
