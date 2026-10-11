# Plan 064 — Personal-data classification: `PersonalData`/`DataSubjectRef` markers, `PersonalDataCatalog`, opt-in redaction + encryption linkage, `varco gdpr inventory` (GDPR-1)

Covers BACKLOG 4.0.0-feature-extension row **`GDPR-1`** (`BACKLOG.md:58` — *"Personal-data
classification — field-level `personal_data(category=…)` on `DomainModel` driving
redaction/encryption/export/erasure + `varco gdpr inventory` (Art. 30 data map)"*). Ships inside
**4.0.0** (locked decision, `BACKLOG.md:31`). Prerequisite of `GDPR-2`, which is split into
**Plan 065** (Art. 15/20 export; owns the `@Multibound` `DataSubjectContributor` seam) and **Plan
066** (Art. 17 erasure). Those two are planned *from this plan's public API* — §Handoff lists
exactly what they may rely on. `GDPR-3` (consent ledger + processing purposes) is **parked**
(`BACKLOG.md:34,110`) and nothing here anticipates it.

**Sources of record read for this plan (2026-10-07):** `BACKLOG.md:1-80,110,145`;
`design/research/022-gdpr-classification-export-erasure.md` (§1 taxonomy, §2 Art. 30
derivability, §3 Art. 15/20 + provided/observed/derived, §6 prior art);
`design/research/018-transactionally-correct-endpoints-and-gdpr-frameworks.md` §B (`:59-95`) and
its options table (`:107`); `docs/regulatory-posture.md` (the "position, not legal advice" shape);
`varco_core/varco_core/model.py:41-98` (`DomainModel`, `pk`, `_raw_orm`);
`varco_core/varco_core/meta.py:97-137` (`EncryptedHint`), `:140-191` (`FieldHint`), `:617-671`
(`ParsedMeta`, incl. `encrypted_fields`, `tenant_scope`), `:697-826` (`MetaReader.read`),
`:970-999` (`_extract_metadata`, `domain_fields` — excludes `pk`); `varco_core/varco_core/registry.py`
(`DomainModelRegistry`, `@register`); `varco_core/varco_core/providers.py:154-183` (`_scan_module` —
the `autodiscover()` discovery rule); `varco_core/varco_core/redaction/{__init__,policy,patterns,posture}.py`
(`RedactionPolicy` frozen + `__post_init__` normalisation, word mode, `AUDIT_REDACTION_POLICY`,
`PII_REDACT_PATTERNS` "deliberately NEVER a default" DESIGN at `patterns.py:87-104`, the
no-scanned-decorator rule at `__init__.py:30-36`); `varco_core/varco_core/service/audit.py:496-564`
(`_audit_redactor`); `varco_core/varco_core/posture.py` (`PostureCollector`, `PostureFinding`,
`PostureSeverity`); `varco_core/varco_core/retention/posture.py:151-211`
(`RetentionPostureCollector` — the precedent: plain subclass, constructor arg, no decorator);
`technical_docs/features/security-posture.md:256-294` (WARN blocks production start, INFO never
does); `varco_core/varco_core/di/report.py:170-177` (`WiringFinding` — "never a posture finding");
`varco_core/varco_core/cli/main.py:27-62`, `cli/retention.py:39-115`, `cli/asyncapi.py:1-113`
(`--path`, `-o/--check` snapshot shape); `varco_core/pyproject.toml:79-80` (`varco.commands`);
`varco_core/varco_core/__init__.py:55-103,551,872-907` (PEP 562 `_LAZY` + `TYPE_CHECKING` block,
`test_lazy_init.py` parity); `varco_core/varco_core/dto/factory.py:82-88,154-176` (DTO generation
strips `Annotated`); `technical_docs/features/crypto-shredding.md:37-49` (scope strings never
parsed, never embed personal data); `varco_core/varco_core/auth/base.py:188-205`
(`AuthContext.user_id` — no "data subject" concept exists anywhere today);
`varco_core/tests/test_retention_no_di_side_effect.py` (the scan-guard test shape);
`CHANGELOG.md:10-22,541` (`[4.0.0]` + `### Added`); `mkdocs.yml:121`; `plans/058` (house format).

> ⚠️ **Three premises corrected from source before anything was decided.**
> (1) **The backlog's `personal_data(category=…)` is shorthand, not a shape.** Every field-level
> marker varco ships is an `Annotated` metadata *class* read by `MetaReader._extract_metadata`
> (`meta.py:970-987`) — `FieldHint`, `EncryptedHint`, `PrimaryKey`, `ForeignKey`. This plan ships
> `PersonalData(category=…)` in that shape (§D-marker); there is no lowercase factory.
> (2) **"Driving redaction" cannot mean a default.** `PII_REDACT_PATTERNS` is documented as
> *"opt-in and deliberately NEVER a default — an audit trail exists to record PII"*
> (`redaction/patterns.py:87-93`), and `DEFAULT_REDACT_PATTERNS` is pinned byte-identical
> (`patterns.py:35-39`). Classification *feeds* a policy the app opts into (§D-redact).
> (3) **"Driving encryption" cannot mean forcing it.** A `WARN` posture finding is a production
> start-blocker under the 4.0 derived `enforce="refuse"` (`security-posture.md:263`), so a WARN for
> "special-category field not encrypted" *would* force encryption. The finding is `INFO`
> (§D-encrypt), pinned by a test. `WiringFinding` is ruled out by its own docstring
> (`di/report.py:172`: "never a posture finding").

## Goal

After this plan, an app can declare — once, on the `DomainModel` field — that a field holds
personal data of a given category (`Annotated[str, PersonalData(PersonalDataCategory.CONTACT)]`)
and which field links an entity to a data subject (`Annotated[UUID, DataSubjectRef("customer")]`).
From that single declaration varco derives: a frozen, introspectable `PersonalDataCatalog`
(entities → personal fields → categories / special-category / origin / encrypted; subject kind →
linked entities → subject-id fields) that Plans 065/066 build on; an opt-in `RedactionPolicy`
extended with the classified field names; an `INFO`-only posture collector reporting
special-category fields stored without `EncryptedHint` and personal-data entities no subject can
reach; and `varco gdpr inventory` — a deterministic JSON or Markdown **partial Art. 30 input**
(entity / field / category / special-category / origin / encrypted / declared retention), with a
`--check` snapshot mode, that explicitly lists the controller-supplied Art. 30 items it cannot
derive. No documentation claims GDPR compliance.

## Non-goals

- **Export (Art. 15/20) and erasure (Art. 17)** — Plans 065/066. No `DataSubjectContributor`, no
  `export()`/`erase()`, no admin mount, no job, no crypto-shred orchestration here.
- **Consent, lawful basis, processing purposes, recipients, transfers** — `GDPR-3` is parked; Art.
  30(1)(b)/(d)/(e) are controller-supplied and *named as such* in the inventory, never modelled.
  ⛔ No `purpose=`/`lawful_basis=`/`consent=` field on any marker.
- **Forcing or auto-enabling encryption.** `PersonalData` never changes storage, column type, or
  the mapper; `EncryptedHint` stays the one storage marker. No WARN/HIGH finding.
- **Changing any redaction default** — `DEFAULT_REDACT_PATTERNS`, `AUDIT_REDACTION_POLICY`,
  `default_redactor()` are untouched. Value scanning stays out (CLAUDE.md redaction rule).
- **Retention enforcement for domain entities.** `RetentionPolicy` targets framework tables only
  (`retention/policy.py:89-161`); a declared entity retention is a documented statement carried
  into the inventory, never executed.
- **Indirect subject links** (e.g. `Comment → Post → author_id`). Only a field *on the entity*
  carrying `DataSubjectRef` links it. Indirect reach is a Plan 065 custom-contributor concern.
- **Vendoring Fideslang** or taking any dependency on `fideslang` (§D-taxonomy).
- **Auto-contribution of the posture collector by `SAModule`/`BeanieModule`.** Explicit
  `contribute()` only (Open Question 6).
- **Value-level PII detection** (Presidio etc., brief 018 §B `:81-85`) — key-name rule stands.
- **`RO-8`** (DLQ / `WebhookDelivery` payload redaction) — a natural follow-on, its own row.
- **`ParsedMeta` changes** — the factories never need classification (§D-catalog).
- **Editing `BACKLOG.md`.**

## Design

### Module layout (all new, `varco_core` only)

```
varco_core/varco_core/personal_data/
  __init__.py      — re-exports the public names below; ⛔ no @Singleton/@Provider/@Configuration
  markers.py       — PersonalDataCategory, PersonalDataOrigin, PersonalData, DataSubjectRef,
                     DEFAULT_SUBJECT_KIND, SPECIAL_CATEGORIES
  catalog.py       — PersonalField, SubjectLink, EntityClassification, classify(),
                     PersonalDataCatalog (incl. findings())
  redaction.py     — personal_data_redaction_policy()
  posture.py       — PersonalDataPostureCollector (plain PostureCollector subclass, no decorator)
  inventory.py     — build_inventory(), render_inventory_markdown(), INVENTORY_SCHEMA_VERSION
varco_core/varco_core/cli/gdpr.py — `varco gdpr inventory` (built-in, registered in cli/main.py)
```

Import direction: `personal_data → meta, model, redaction, posture, tenancy.settings`. Nothing in
`meta.py`, `redaction/`, or `posture.py` imports `personal_data` (keeps `redaction` stdlib-only,
`redaction/__init__.py:38-41`).

### Public API (the contract 065/066 plan against)

```python
# markers.py
class PersonalDataCategory(StrEnum):
    # ordinary personal data (Art. 4(1)) — varco's own small set
    IDENTITY = "identity"                  # name, username, photo
    CONTACT = "contact"                    # email, phone, postal address
    GOVERNMENT_ID = "government_id"        # national id, passport, tax id
    FINANCIAL = "financial"                # IBAN, card, income
    LOCATION = "location"                  # geolocation, location trace
    ONLINE_IDENTIFIER = "online_identifier"  # IP, device id, cookie id
    DEMOGRAPHIC = "demographic"            # date of birth, age, gender, nationality
    BEHAVIORAL = "behavioral"              # activity, preferences, usage
    CONTENT = "content"                    # user-generated content, messages, uploads
    OTHER = "other"
    # special categories — Art. 9(1) (fixed by the Regulation text)
    RACIAL_ETHNIC_ORIGIN = "racial_ethnic_origin"
    POLITICAL_OPINION = "political_opinion"
    RELIGIOUS_PHILOSOPHICAL_BELIEF = "religious_philosophical_belief"
    TRADE_UNION_MEMBERSHIP = "trade_union_membership"
    GENETIC = "genetic"
    BIOMETRIC = "biometric"                # for the purpose of uniquely identifying a person
    HEALTH = "health"
    SEX_LIFE_ORIENTATION = "sex_life_orientation"
    # Art. 10
    CRIMINAL_OFFENCE = "criminal_offence"

    @property
    def special(self) -> bool: ...                       # True for the 9 Art. 9/10 members
    @property
    def gdpr_article(self) -> Literal["9", "10"] | None: ...

SPECIAL_CATEGORIES: Final[frozenset[PersonalDataCategory]]   # the 9 members above

class PersonalDataOrigin(StrEnum):       # brief 022 §3 — Art. 20 scope (provided + observed)
    PROVIDED = "provided"                # actively supplied by the subject
    OBSERVED = "observed"                # generated by the subject's use of the service
    DERIVED = "derived"                  # inferred/derived by the controller (outside Art. 20)

@dataclass(frozen=True)
class PersonalData:                      # Annotated marker, like EncryptedHint/FieldHint
    category: PersonalDataCategory       # a plain str is coerced via PersonalDataCategory(value)
    label: str | None = None             # free-form refinement, e.g. a Fideslang key
    origin: PersonalDataOrigin = PersonalDataOrigin.PROVIDED
    @property
    def special(self) -> bool: ...

DEFAULT_SUBJECT_KIND: Final = "user"

@dataclass(frozen=True)
class DataSubjectRef:                    # Annotated marker on the field holding a subject id
    kind: str = DEFAULT_SUBJECT_KIND     # slug: ^[a-z][a-z0-9_]*$ — a "category of data subjects"

# catalog.py
@dataclass(frozen=True)
class PersonalField:
    name: str; category: PersonalDataCategory; label: str | None
    origin: PersonalDataOrigin; encrypted: bool
    @property
    def special(self) -> bool: ...

@dataclass(frozen=True)
class SubjectLink:
    field: str; kind: str

@dataclass(frozen=True)
class EntityClassification:
    entity: type[DomainModel]
    name: str                            # f"{entity.__module__}.{entity.__qualname__}" — stable id
    table: str                           # ParsedMeta.table
    tenant_scope: TenantScope            # ParsedMeta.tenant_scope
    fields: tuple[PersonalField, ...]    # dataclass declaration order (pk first if marked)
    subject_links: tuple[SubjectLink, ...]
    retention: str | None                # Meta.personal_data_retention — declared, never enforced
    @property
    def holds_personal_data(self) -> bool: ...
    @property
    def is_subject_linked(self) -> bool: ...
    @property
    def special_fields(self) -> tuple[PersonalField, ...]: ...
    def subject_fields(self, kind: str) -> tuple[str, ...]: ...   # () when not linked to kind

def classify(entity: type[DomainModel]) -> EntityClassification: ...

@dataclass(frozen=True)
class PersonalDataCatalog:
    entities: tuple[EntityClassification, ...]   # only entities with personal fields or links, sorted by .name
    @classmethod
    def from_models(cls, models: Iterable[type[DomainModel]]) -> PersonalDataCatalog: ...
    def get(self, entity: type[DomainModel]) -> EntityClassification | None: ...
    def subject_kinds(self) -> tuple[str, ...]: ...                       # sorted, distinct
    def linked_to(self, kind: str) -> tuple[EntityClassification, ...]: ...
    def unlinked(self) -> tuple[EntityClassification, ...]: ...           # personal data, no link
    def categories(self) -> tuple[PersonalDataCategory, ...]: ...         # distinct, enum order
    def field_names(self, *, categories: Iterable[PersonalDataCategory] | None = None,
                    special_only: bool = False) -> tuple[str, ...]: ...   # distinct, sorted
    def findings(self) -> tuple[PostureFinding, ...]: ...                 # INFO only

# redaction.py
def personal_data_redaction_policy(catalog: PersonalDataCatalog, *,
        base: RedactionPolicy | None = None,
        categories: Iterable[PersonalDataCategory] | None = None,
        special_only: bool = False) -> RedactionPolicy: ...

# posture.py
class PersonalDataPostureCollector(PostureCollector):
    name = "personal_data"
    def __init__(self, catalog: PersonalDataCatalog) -> None: ...
    def collect(self) -> list[PostureFinding]: ...

# inventory.py
INVENTORY_SCHEMA_VERSION: Final = 1
def build_inventory(catalog: PersonalDataCatalog) -> dict[str, Any]: ...
def render_inventory_markdown(inventory: Mapping[str, Any]) -> str: ...
```

Top-level `varco_core.__all__` gains (lazily, via `_LAZY` + the `TYPE_CHECKING` block):
`PersonalData`, `PersonalDataCategory`, `PersonalDataOrigin`, `DataSubjectRef`,
`PersonalDataCatalog`. Everything else is reachable from `varco_core.personal_data`.

Usage (the README snippet):

```python
@register
@dataclass
class Customer(DomainModel):
    pk: Annotated[UUID, PrimaryKey(PKStrategy.UUID_AUTO), DataSubjectRef("customer")] = pk_field()
    email: Annotated[str, PersonalData(PersonalDataCategory.CONTACT)]
    diagnosis: Annotated[str | None, PersonalData(PersonalDataCategory.HEALTH), EncryptedHint()]
    last_ip: Annotated[str | None, PersonalData(PersonalDataCategory.ONLINE_IDENTIFIER,
                                                origin=PersonalDataOrigin.OBSERVED)]

    class Meta:
        table = "customers"
        personal_data_retention = "Account lifetime + 24 months"

@register
@dataclass
class Order(DomainModel):
    customer_id: Annotated[UUID, DataSubjectRef("customer")]
    shipping_address: Annotated[str, PersonalData(PersonalDataCategory.CONTACT)]

catalog = PersonalDataCatalog.from_models([Customer, Order])
contribute(container, PostureCollector, PersonalDataPostureCollector(catalog))       # opt-in
set_default_redactor(PolicyRedactor(personal_data_redaction_policy(catalog)))         # opt-in
```

```
varco gdpr inventory --models myapp.models --format markdown -o docs/data-map.md
varco gdpr inventory --models myapp.models --format markdown -o docs/data-map.md --check
```

### `classify()` rules

1. `entity` must be a `DomainModel` subclass that is a dataclass → else `TypeError`.
2. `typing.get_type_hints(entity, include_extras=True)`; a `NameError` (marker imported only under
   `TYPE_CHECKING` with `from __future__ import annotations`) is re-raised as `TypeError` naming
   the entity and the fix.
3. `parsed = MetaReader.read(entity)` supplies `table`, `tenant_scope`, and **`encrypted_fields`**
   — the `encrypted` column is *exactly* what the mapper encrypts (one source of truth).
4. Iterate `dataclasses.fields(entity)` skipping names starting with `_` (so `pk` **is**
   included — `MetaReader.domain_fields` excludes it, but a subject entity marks its `pk`).
5. Per field, collect *all* `PersonalData` / `DataSubjectRef` instances from `Annotated` metadata
   (local `_markers(ann, type) -> list`; `_extract_metadata` returns only the first). More than
   one `PersonalData` on a field → `ValueError` (a silently dropped special category is a real
   harm); more than one `DataSubjectRef` on a field → `ValueError`. One of each on a field is
   allowed.
6. `Meta.personal_data_retention`: absent → `None`; non-empty `str` → kept (stripped); anything
   else → `TypeError`/`ValueError` naming the entity.

### Inventory document (schema v1, deterministic — no timestamps, no varco version)

```json
{
  "schema_version": 1,
  "kind": "varco.personal-data-inventory",
  "notice": "Code-derived, partial input to a GDPR Art. 30 record of processing activities. Not legal advice and not a complete record; the items under controller_supplied cannot be derived from code.",
  "controller_supplied": [
    "Art. 30(1)(a) controller, joint controller, representative and DPO name and contact details",
    "Art. 30(1)(b) purposes of the processing",
    "Art. 30(1)(d) categories of recipients",
    "Art. 30(1)(e) transfers to third countries and their safeguards",
    "Art. 30(1)(f) final erasure time limits (declared_retention below is a developer statement, not enforced)",
    "Art. 30(1)(g) organisational measures and infrastructure-level technical measures"
  ],
  "data_subject_categories": ["customer"],
  "personal_data_categories": ["contact", "online_identifier", "health"],
  "entities": [
    {
      "entity": "myapp.models.Customer",
      "table": "customers",
      "tenant_scope": "tenant",
      "data_subject_links": [{"field": "pk", "kind": "customer"}],
      "declared_retention": "Account lifetime + 24 months",
      "fields": [
        {"field": "email", "category": "contact", "label": null, "special_category": false,
         "gdpr_article": null, "origin": "provided", "encrypted": false}
      ]
    }
  ],
  "findings": [{"check": "...", "severity": "info", "detail": "...", "remediation": "..."}]
}
```

Art. 30(1)(c) (categories of subjects and of personal data) is the derived part; (g) is partially
derived (per-field `encrypted`). Mapping sourced from brief 022 §2.

### Findings (stable ids — never renamed once shipped; both `INFO`, never `WARN`)

| `check` | Fires when | Remediation text (summary) |
|---|---|---|
| `personal_data.special_category_unencrypted` | ≥ 1 field with `PersonalData.special` and not in `encrypted_fields` | add `EncryptedHint()` (needs a `FieldEncryptor` + a column migration), or suppress the id if the risk is accepted — varco never forces encryption |
| `personal_data.unlinked_entity` | ≥ 1 entity holds personal data but has no `DataSubjectRef` | add `DataSubjectRef(kind)` to the subject-id field; otherwise subject-scoped export/erasure cannot reach the entity |

One aggregated finding per id (suppression is by id); `detail` lists `Entity.field` / `Entity`
names sorted, capped at 20 with "… and N more". Same objects in posture, CLI JSON, and Markdown.

### DESIGN blocks

**§D-marker — `Annotated` marker class, not `dataclasses.field(metadata=)` or a `Meta` dict**
- ✅ Identical shape to `FieldHint`/`EncryptedHint`/`PrimaryKey`/`ForeignKey`; composes on one
  annotation with `EncryptedHint()` (`Annotated[str, PersonalData(HEALTH), EncryptedHint()]`).
- ✅ Works on `pk` via `pk_field()` (a `field(metadata=)` form would fight `pk_field()`'s own
  `dataclasses.field`), on inherited mixin fields, and is stripped by DTO generation
  (`dto/factory.py:82-88`) so DTOs are unchanged.
- ❌ The backlog wording `personal_data(category=…)` does not appear verbatim — the CHANGELOG and
  feature doc name the shape once.
- Rejected: `Meta.personal_data = {"email": "contact"}` — a parallel list keyed by strings, the
  exact "four parallel lists" problem GDPR-1 exists to remove; typos are silent.

**§D-taxonomy — a small varco `StrEnum` + free-form `label`, not Fideslang keys**
- ✅ The nine special members are fixed by Art. 9(1)/Art. 10 text — the only part with behaviour
  (`special` drives the finding) is the part that cannot churn.
- ✅ Fideslang is the only live open taxonomy but "not formally a de-facto standard … no published
  backward-compatibility guarantee" (brief 022 §1, §Version notes); binding an `api-check`-frozen
  enum to it would import its churn into varco's SemVer surface.
- ✅ `label=` carries any external key (`label="user.contact.email"`) with zero varco semantics —
  interop without dependency; a typo'd *category* still fails loudly at import (`ValueError`
  hinting `label=`).
- ✅ No vendored taxonomy text → no CC BY 4.0 attribution obligation in code; the feature doc's
  crosswalk table carries an attribution line anyway (cheap, correct if the brief's licence claim
  holds — ⚠️ ASSUMPTION, Risks).
- ❌ Ten ordinary categories are coarse (`CONTACT` lumps email and postal address) — `label`
  refines; inventory consumers needing Fideslang granularity read `label`.
- Rejected: adopting Fideslang keys as enum values (churn, licence-attribution in code, three
  taxonomies of which "data uses" = purposes = parked GDPR-3); free-form `str` category (typos
  silently change behaviour, `special` undecidable).

**§D-subject — `DataSubjectRef(kind)` on the subject-id field**
- ✅ Answers "who is the data subject" per entity in the declaration itself; `kind` is Art.
  30(1)(c)'s "category of data subjects" and the key 065/066 query by (`linked_to(kind)`).
- ✅ On `pk` it marks the subject entity itself (`Customer.pk`); multiple links per entity are
  allowed (`Message.sender_id` + `recipient_id`, both `"user"`), surfaced as a tuple.
- ❌ Not inferred from `ForeignKey(Customer)` — explicit beats magic; a FK to a subject entity is
  not always "about" the subject (e.g. `created_by` on shared reference data). Revisit only with a
  real request.
- Rejected: `Meta.data_subject = "customer_id"` — one string per entity, cannot express two links,
  breaks the one-declaration-on-the-field rule.

**§D-catalog — explicit, frozen `PersonalDataCatalog`, not a global registry or a `ParsedMeta` field**
- ✅ No new process-global state (`DomainModelRegistry` already is one, with a `clear()` test
  hazard); test isolation is free; bindable by instance for 065 (`bind_instance`).
- ✅ Factories untouched — `ParsedMeta` is a positional frozen dataclass consumed by SA/Beanie
  factories that never need classification; a second reflection pass is cheap and startup-only.
- ✅ Fail-fast: malformed declarations raise at `from_models()` (wiring time), never at request.
- ❌ The app passes its model list (no "everything registered" default) — the CLI covers
  discovery with `autodiscover()`'s own rule (`providers.py:154-183`).

**§D-encrypt — an `INFO` finding, never `WARN`, never auto-encryption**
- ✅ `WARN` blocks a production start under the derived `enforce="refuse"` (`security-posture.md:263`)
  — that *is* forcing encryption, which the brief forbids; `INFO` is visible in every report and
  suppressible by id.
- ✅ Encryption remains one storage decision with one marker (`EncryptedHint`), because it changes
  the column type to `LargeBinary` (`meta.py:106-108`) — a schema migration plus a `FieldEncryptor`
  binding no classification marker should imply.
- ❌ An app can ship unencrypted health data with only an `INFO` line. Accepted: the controller
  decides; varco reports.
- Rejected: `PersonalData(..., encrypt=True)` sugar honoured by `MetaReader` (two ways to say
  "encrypt", storage change hidden in a descriptive marker); `WiringFinding` (`di/report.py:172`).

**§D-redact — an opt-in policy builder, never a default**
- ✅ Respects `patterns.py:87-104` (PII never a default; audit records PII; crypto-shredding is
  varco's answer to PII at rest) and the pinned `DEFAULT_REDACT_PATTERNS`.
- ✅ Key-name based only: classified *field names* become patterns; word-mode matching
  (`policy.py:8-40`) makes `email` catch `email`/`userEmail`/`emails`.
- ✅ `base=` composes with any policy (`AUDIT_REDACTION_POLICY` keeps `render_non_json=False`).
- ❌ Generic field names over-redact in word mode (`name` matches `channel_name`); filters
  (`categories=`, `special_only=`) and a Pitfalls row are the answer — never loosening matching.
- ❌ Under a `match_mode="substring"` base, field names substring-match (`name` ⊂ `rename_count`)
  — documented.
- Rejected: folding classified names into `default_redactor()` automatically (silent behaviour
  change for every surface); redacting audit diffs by default (breaks the audit record — `patterns.py:88-90`).

**§D-cli — built-in `varco gdpr` group, deterministic, `--check` snapshot**
- ✅ Same `-o FILE` / `--check` drift shape as `export-asyncapi` (`cli/asyncapi.py:20-26`) — an
  app's CI can keep its data map from silently going stale (the "kept up to date" half of Art. 30).
- ✅ Built in via `cli/main.py` (like `retention`/`diagnose`), heavy imports inside `_run`; 065/066
  add `export`/`erase` verbs to the same `gdpr` subparser.
- ✅ JSON + Markdown only, stdlib — no `pyyaml` (same rule as AsyncAPI, CLAUDE.md).
- ❌ Findings never change the exit code — not a gate on encryption (§D-encrypt).
- ❌ `gdpr` names a jurisdiction while the module is neutral (`personal_data`); the CLI name is the
  backlog's locked wording.

**§D-origin — `PersonalDataOrigin` ships now**
- ✅ Art. 20 portability covers provided + observed data and excludes derived (brief 022 §3);
  065 needs exactly this per-field fact to split an Art. 15 export from an Art. 20 one.
- ✅ Defaulted (`PROVIDED`) — zero cost for apps that ignore it.
- ❌ One more concept in the marker before its consumer exists.

**§D-retention — `Meta.personal_data_retention: str`, declared free text**
- ✅ Art. 30(1)(f) is "envisaged time limits … where possible" — often criteria, not a duration
  (brief 022 §2); free text is faithful. Carried into the inventory as `declared_retention`.
- ❌ Not linked to any `RetentionPolicy` (those target framework tables only) and not enforced —
  the inventory and doc say so verbatim.

### Alternatives considered

- **Adopt Fideslang category keys as the enum**: rejected — ❌ no compatibility guarantee frozen
  into varco's API, ❌ licence attribution in code, ❌ drags "data uses" (purposes, parked) along;
  ✅ would have given richer granularity — recovered via `label=`.
- **Extend `ParsedMeta` with `personal_fields`/`subject_fields`**: rejected — ❌ touches the
  factories' contract for data they never use; ✅ one reflection pass — not worth it.
- **Global `PersonalDataRegistry` populated at `@register` time**: rejected — ❌ a second
  process-global, ❌ misses models registered via `provider.register()` without `@register`.
- **`WARN` posture finding / `--strict` exit on unencrypted special categories**: rejected —
  forcing encryption by another name.
- **Default-on personal-data redaction**: rejected — §D-redact.
- **Separate `PersonalDataFinding` type**: rejected — `PostureFinding` already has
  check/severity/detail/remediation; one type everywhere.

### Handoff to Plans 065/066 (what they may rely on)

- `PersonalDataCatalog.from_models(models)` + `.linked_to(kind)` → the entities a subject of
  `kind` reaches; `EntityClassification.subject_fields(kind)` → the field names to filter on
  (`pk` allowed); `.fields` → `PersonalField(category, origin, encrypted, special, label)`.
- `PersonalDataOrigin` → Art. 20 filter (`PROVIDED`/`OBSERVED`).
- `.unlinked()` → entities export/erasure cannot reach (065 decides whether to require a custom
  contributor for them).
- `catalog.subject_kinds()`, `DEFAULT_SUBJECT_KIND`.
- The `gdpr` CLI subparser in `varco_core/cli/gdpr.py` → add verbs there.
- Additive changes 065/066 may make: a defaulted field on `PersonalData` (e.g. `erasure=`) — class
  signatures are invisible to `api-check` (CLAUDE.md "Known limitation"), so this needs no
  snapshot entry; a new finding id (new row in the feature doc's table).

## Steps

1. [ ] **Tests (failing) — markers + `classify()`**: `varco_core/tests/test_personal_data_markers.py`
   and `varco_core/tests/test_personal_data_classify.py`. Cover: `SPECIAL_CATEGORIES` equals
   exactly the nine Art. 9/10 members and `special`/`gdpr_article` agree (`"10"` only for
   `CRIMINAL_OFFENCE`); `PersonalData("contact")` coerces; `PersonalData("user.contact.email")`
   → `ValueError` mentioning `label=`; `label=""`/whitespace → `ValueError`; markers are frozen
   and hashable; `origin` defaults to `PROVIDED`; `DataSubjectRef()` kind is `"user"`;
   `DataSubjectRef("User")`/`("")`/`("a b")` → `ValueError`. `classify()`: business-field and
   `pk` markers found; inherited mixin field classified on the subclass; `Annotated[str | None,
   PersonalData(...)]` works; `encrypted` mirrors `MetaReader.read(...).encrypted_fields`; two
   `PersonalData` on one field → `ValueError` naming `Entity.field`; two `DataSubjectRef` on one
   field → `ValueError`; one of each on a field allowed; multiple links per entity preserved in
   declaration order; `Meta.personal_data_retention` read/stripped, `30` → `TypeError`, `""` →
   `ValueError`; non-`DomainModel` → `TypeError`; marker under `TYPE_CHECKING` +
   `from __future__ import annotations` → `TypeError` with the fix in the message; unmarked
   entity → empty `fields`/`subject_links`; `name` is `module.qualname`.
2. [ ] **Implement** `varco_core/varco_core/personal_data/__init__.py`, `markers.py`, and the
   `PersonalField`/`SubjectLink`/`EntityClassification`/`classify()` half of `catalog.py` per
   §Design (module docstrings with the §D-marker/§D-taxonomy/§D-subject/§D-origin/§D-retention
   DESIGN blocks; full `Args/Returns/Raises/Edge cases/Thread safety` docstrings;
   `from __future__ import annotations`; frozen dataclasses). `__init__.py` docstring carries the
   ⛔ no-scanned-decorator rule verbatim in spirit from `redaction/__init__.py:30-36`. Step 1 green.
3. [ ] **Tests (failing) — catalog + findings**: `varco_core/tests/test_personal_data_catalog.py`.
   Cover: `from_models` dedupes the same class passed twice, drops entities with neither personal
   fields nor links, sorts by `name`; `get()` returns `None` for unknown/irrelevant; `subject_kinds()`
   sorted distinct; `linked_to("customer")` vs `linked_to("nope") == ()`; `unlinked()`;
   `categories()` in enum order; `field_names()` distinct+sorted, `categories=` and
   `special_only=` filters; `findings()`: none on a clean catalog, exactly one
   `personal_data.special_category_unencrypted` (INFO) listing every unencrypted special field
   sorted, exactly one `personal_data.unlinked_entity` (INFO), the 20-item cap with "… and N
   more", every severity is `PostureSeverity.INFO`; an empty model list → empty catalog, no
   findings; a link-only entity (no personal fields) → included, no finding.
4. [ ] **Implement** `PersonalDataCatalog` in `varco_core/varco_core/personal_data/catalog.py`
   (§D-catalog, §D-encrypt DESIGN blocks; findings built as `varco_core.posture.PostureFinding`;
   remediation strings per the findings table). Step 3 green.
5. [ ] **Tests (failing) — redaction builder + posture collector**:
   `varco_core/tests/test_personal_data_redaction.py`: default `base` is `RedactionPolicy()`
   with its patterns as a prefix, classified names appended deduped; `PolicyRedactor(policy)`
   redacts `email`/`userEmail`/`emails` and leaves `channel` intact; `categories=`/`special_only=`
   filter; `base=AUDIT_REDACTION_POLICY` keeps `render_non_json=False`; empty catalog → policy
   `==` base; `DEFAULT_REDACT_PATTERNS`, `AUDIT_REDACTION_POLICY`, and `default_redactor()` are
   unchanged after calling it (no side effect).
   `varco_core/tests/test_personal_data_posture.py`: collector `name == "personal_data"`,
   `collect()` returns the catalog's findings, all INFO; `contribute(container, PostureCollector,
   ...)` then `collect(container, PostureCollector)` includes it.
   `varco_fastapi/tests/test_posture_personal_data.py`: with `VARCO_SECURITY_ENV=production`
   (monkeypatch, per test) and the derived `enforce="refuse"`, a `SecurityPostureLifecycle`
   whose collectors include a `PersonalDataPostureCollector` reporting an unencrypted `HEALTH`
   field **starts** (no `RuntimeError`) and its report contains the INFO finding — the mechanical
   form of "never forces encryption". (Follow the construction pattern of the existing
   `varco_fastapi/tests/test_posture*.py` files — read one before writing.)
6. [ ] **Implement** `varco_core/varco_core/personal_data/redaction.py` (§D-redact DESIGN block)
   and `posture.py` (plain `PostureCollector` subclass, **no** `@Singleton`; `collect()` wraps
   in `try/except Exception` returning one `NOT_ASSESSED` finding `personal_data.not_assessed`,
   mirroring the "must not raise" contract at `posture.py:120-128`). Step 5 green.
7. [ ] **Tests (failing) — inventory + CLI**: `varco_core/tests/test_personal_data_inventory.py`:
   `build_inventory()` matches the schema v1 shape exactly (keys, order, `controller_supplied`
   six items, `notice` text contains "Not legal advice" and does not contain "compliant"/
   "compliance"); deterministic (two calls → identical `json.dumps`); no timestamp/version keys;
   Markdown has the notice, controller-supplied list, subject categories, one table per entity,
   a findings section. `varco_core/tests/test_cli_gdpr.py` (call `main([...])` directly, fixture
   models in a `tmp_path` package added via `--path`): `--models pkg` (autodiscover rule — only
   `@register`-stamped classes under `pkg`; clear `DomainModelRegistry` in teardown);
   `--models pkg.mod:MODELS` (iterable), `:factory` (zero-arg callable), `:CATALOG` (a
   `PersonalDataCatalog`); `--format json|markdown`; `-o FILE` writes; `--check` identical → 0,
   drifted → 1 with a diff on stderr, missing file → 1; `--check` without `-o` → 2; no
   `--models` → 2; unresolvable target → 2; a malformed declaration → 1 naming the entity; an
   empty discovery → 0 with a stderr warning; findings never change the exit code.
8. [ ] **Implement** `varco_core/varco_core/personal_data/inventory.py` and
   `varco_core/varco_core/cli/gdpr.py` (`register(subparsers)` adding `gdpr` →
   `inventory`; options `--models` (repeatable, required), `--path` (repeatable, prepended to
   `sys.path`), `--format {json,markdown}` default `json`, `-o/--output`, `--check`; package
   discovery via `varco_core.providers._scan_module` imported inside `_run` with a comment naming
   `autodiscover()`'s rule; exit codes 0/1/2 as in Step 7; module docstring with the §D-cli DESIGN
   block and usage block). Register in `varco_core/varco_core/cli/main.py` beside
   `_retention_module` (import + `register` call; update the module docstring's built-in list).
   Step 7 green.
9. [ ] **Exports + guard tests**: add `PersonalData`, `PersonalDataCategory`,
   `PersonalDataOrigin`, `DataSubjectRef`, `PersonalDataCatalog` to
   `varco_core/varco_core/__init__.py` — `_LAZY` map, the `TYPE_CHECKING` import block, and
   `__all__` (all three, or `test_lazy_init.py` fails). New
   `varco_core/tests/test_personal_data_no_di_side_effect.py` (shape of
   `test_retention_no_di_side_effect.py`): no providify marker on any `varco_core.personal_data`
   object; `container.scan("varco_core", recursive=True)` contributes zero `PostureCollector`
   instances of type `PersonalDataPostureCollector`. Compat guards: in
   `test_personal_data_classify.py`, `generate_dtos()` on a classified model yields the same DTO
   field types as the unmarked twin; new `varco_sa/tests/test_personal_data_factory_compat.py`
   — SA model generation for a classified entity (with and without `EncryptedHint`) produces the
   same columns as the unmarked twin (DB-free; follow the existing SA factory test pattern). If
   `varco_beanie`'s document generation is DB-free (check its existing factory tests), add the
   same assertion in `varco_beanie/tests/test_personal_data_factory_compat.py`; otherwise record
   why not in the test module of `varco_sa`.
10. [ ] **Feature doc + regulatory note + nav**: new
    `technical_docs/features/personal-data-classification.md` — opening "not legal advice / no
    compliance claim" banner in `docs/regulatory-posture.md`'s voice; what it does / does not do;
    the taxonomy table (each category, examples, Art. 9(1)/10 reference); `label=` and an
    **attributed, approximate** Fideslang crosswalk (only if Step-10 verification against a
    pinned Fideslang release succeeds — see Risks; otherwise omit the table and keep one sentence
    pointing at `label=`); `DataSubjectRef` and subject kinds; origin vs Art. 20; the findings
    table; the inventory schema + Art. 30 mapping (derived vs controller-supplied, brief 022 §2);
    the CLI and an app-CI `--check` recipe; the redaction recipe per surface (verify each
    surface's knob from `technical_docs/features/redaction.md` §§"error_params()", "The audit
    hook", "Request logging" — the logging middleware does **not** fall back to
    `default_redactor()`, `varco_fastapi/middleware/logging.py:65`); why audit redaction of PII is
    discouraged (crypto-shredding instead, `crypto-shredding.md`, and its "never embed personal
    data in a scope string" rule); a "For Plans 065/066" handoff section; a **Pitfalls** table
    (marker under `TYPE_CHECKING`; generic field names over-redact; substring-mode base; encrypted
    ≠ per-subject shreddable; declared retention is not enforced; indirect links unreachable;
    `--check` drift after adding a field). Add `docs/regulatory-posture.md` §"GDPR — varco is a
    library, not a controller or processor" (≤ 15 lines, links the feature doc, same
    "position, not legal advice" framing, explicit "no claim of GDPR compliance"). Add the page to
    `mkdocs.yml` nav beside `Crypto-Shredding` (`mkdocs.yml:121`).
11. [ ] **Repo docs**: `README.md` — new "Personal-data classification" section (the §Design
    usage snippet + the two CLI lines + a link to the feature doc) and a TOC entry;
    `ARCHITECTURE.md` — `personal_data/` line in the `varco_core` package map (near
    `retention/`, `:59`) and a short "Personal-data classification" type-hierarchy block near
    "Redaction" (`:793`); `CLAUDE.md` — a ≤ 8-line `### Personal-data classification
    (varco_core.personal_data, Plan 064 / GDPR-1)` section of rules only, ending with the
    feature-doc link: (a) classification is descriptive — never changes storage; `EncryptedHint`
    is the one encryption marker; (b) `personal_data.*` findings are `INFO`, never `WARN` (WARN
    blocks production start = forcing encryption); (c) personal-data redaction is opt-in via
    `personal_data_redaction_policy()`, never folded into a default or `AUDIT_REDACTION_POLICY`;
    (d) ⛔ no scanned `@Singleton`/`@Provider`/`@Configuration` in `varco_core.personal_data`;
    (e) ⛔ no consent/purpose/lawful-basis fields (GDPR-3 parked); (f) docs never claim GDPR
    compliance — "helps you produce", "supports". Plus one Decision-Tree branch: *"Marking a field
    as personal data / producing an Art. 30 data map? → `varco_core.personal_data`
    (`PersonalData`/`DataSubjectRef` + `PersonalDataCatalog`) + `varco gdpr inventory`; export/
    erasure → Plans 065/066"*. `CHANGELOG.md` `[4.0.0]` → `### Added` (`:541`): one
    `varco_core` bullet group tagged `(Plan 064, GDPR-1)` naming the markers, the catalog, the
    redaction builder, the posture collector + its two ids, and `varco gdpr inventory`. **No**
    `technical_docs/migrations/3-to-4.md` section — nothing breaks (no existing signature,
    default, or behaviour changes; state this in the CHANGELOG bullet).
12. [ ] **Snapshot gates + full verification**: `uv run python scripts/api_surface.py` (five new
    top-level names) and commit `design/api-freeze-and-standards/measurements/api-surface.{json,md}`;
    confirm `make entry-points-check`, `make di-catalogue-check`, `make settings-check`,
    `make asyncapi-check` are **unchanged/green** (this plan adds no `bind_*`/`enable_*`/
    `mount_*`/`install_*`/`VarcoModule`/`@Configuration`, no binding, no settings class — if any
    of them moves, stop and find out why); run `uv run python scripts/import_budget.py --check
    --warn-only` (names are lazy; expect no delta); then the §Verification block.

## Edge cases

- `PersonalData` twice on one field → `ValueError("Customer.email: more than one PersonalData marker …")` at `classify()`/`from_models()`.
- `PersonalData("user.contact.email")` → `ValueError` listing legal categories and suggesting `label=`.
- `DataSubjectRef("Customer")` / `""` / `"a b"` → `ValueError` (slug `^[a-z][a-z0-9_]*$`).
- Marker imported only under `TYPE_CHECKING` with postponed annotations → `TypeError` naming the entity and "import the marker at runtime".
- Inherited personal field from a non-`@register` mixin base → classified on the concrete subclass.
- `PersonalData` on `pk` → allowed; `encrypted` is always `False` for `pk` (`EncryptedHint` on `pk` is not a mapper feature).
- `DataSubjectRef` on a composite-PK business field → allowed.
- Entity with `DataSubjectRef` but no `PersonalData` → in the catalog (subject-linked), no finding.
- Entity with neither marker → excluded from the catalog and the inventory.
- Same class passed twice → one entry; two classes with the same `__qualname__` in different modules → two entries, ordered by full dotted name.
- Special-category field with `EncryptedHint` → no finding.
- `Meta.personal_data_retention = 30` → `TypeError`; `"  "` → `ValueError`; absent → `null` in the inventory.
- Empty model list / discovery finds nothing → empty catalog, empty `entities`, no findings; CLI exits 0 and warns on stderr.
- `--check` with no `-o` → exit 2; `-o` file missing → exit 1 ("snapshot missing — regenerate"); drift → exit 1 + unified diff.
- Redaction builder on an empty catalog → a policy equal to `base`; under a substring-mode `base` → substring semantics (documented).
- Posture: production + derived `refuse` + unencrypted special category → app starts, finding is INFO.
- `collect()` hitting an unexpected exception → one `NOT_ASSESSED` finding `personal_data.not_assessed`, never a raise.

## Verification

```bash
uv run pytest varco_core/tests/test_personal_data_markers.py varco_core/tests/test_personal_data_classify.py \
  varco_core/tests/test_personal_data_catalog.py varco_core/tests/test_personal_data_redaction.py \
  varco_core/tests/test_personal_data_posture.py varco_core/tests/test_personal_data_inventory.py \
  varco_core/tests/test_cli_gdpr.py varco_core/tests/test_personal_data_no_di_side_effect.py \
  varco_core/tests/test_lazy_init.py -q
uv run pytest varco_fastapi/tests/test_posture_personal_data.py varco_sa/tests/test_personal_data_factory_compat.py -q
make test                    # all eleven suites, accumulated summary
make lint                    # ruff check + ruff format --check + every snapshot gate (no PKG)
make type-check              # mypy strict over the ten dirs
uv run python scripts/api_surface.py --check
uv run varco gdpr inventory --help
# wording guard — must print nothing:
rg -n -i "gdpr[- ]compliant|compliant with (the )?gdpr|ensures? (gdpr )?compliance|guarantees? (gdpr )?compliance|gdpr compliance" \
  technical_docs/features/personal-data-classification.md docs/regulatory-posture.md README.md CLAUDE.md \
  CHANGELOG.md varco_core/varco_core/personal_data varco_core/varco_core/cli/gdpr.py
# no scanned decorator — must print nothing:
rg -n "@(Singleton|Provider|Configuration)" varco_core/varco_core/personal_data
```

## Risks

- ⚠️ **ASSUMPTION — Fideslang licence is CC BY 4.0** (brief 022 §1; not independently verified).
  Mitigation: nothing is vendored; the doc crosswalk carries an attribution line. If the licence
  cannot be confirmed from the Fideslang repository at Step 10, ship without the crosswalk table.
- ⚠️ **ASSUMPTION — Fideslang top-level data-category keys** (`user.contact`, `user.financial`,
  `user.health_and_medical`, … per brief 022 §1) are only partially quoted in the brief. Every
  crosswalk row must be checked against one pinned Fideslang release named in the doc; unverified
  rows are dropped, never guessed.
- ⚠️ **ASSUMPTION — the Art. 9(1) / Art. 10 enumeration** is taken from the Regulation text, not
  from a brief (briefs 018/022 do not list it). Check the nine members' wording against EUR-Lex
  32016R0679 at Step 10; the enum *values* are varco identifiers, the doc quotes the article.
- ⚠️ **ASSUMPTION — provided/observed/derived (WP242 rev.01, brief 022 §3)** is the Art. 20 scope
  split. `PersonalDataOrigin` is varco's labelling; whether a field is "observed" or "derived" is
  the controller's judgement — the doc says so.
- ⚠️ **ASSUMPTION — pydantic/Beanie ignore unknown `Annotated` metadata.** DTO generation strips
  `Annotated` (`dto/factory.py:82-88`); the SA factory reads markers by type. Guarded by Step 9's
  compat tests; Beanie is guarded only if its generation is DB-free.
- **Wording.** Any doc line implying compliance is a defect (CLAUDE.md "never claim compliance").
  Invariant: the Verification `rg` prints nothing; the inventory `notice` says "partial" and
  "not legal advice".
- **Forcing encryption by accident.** Invariant: both `personal_data.*` findings are `INFO`
  (Step 3 + the Step 5 `varco_fastapi` lifecycle test). Any future severity change is a reviewed
  decision with a security-posture.md table row.
- **Silent default change.** Invariant: `DEFAULT_REDACT_PATTERNS`, `AUDIT_REDACTION_POLICY`,
  `default_redactor()` byte-identical (Step 5 asserts no side effect); no scanned decorator in the
  package (Step 9 + `rg`).
- **Over-redaction.** Generic field names (`name`, `address`) produce broad word-mode patterns
  when an app opts in. Accepted (fail-closed in logs); filters + Pitfalls row.
- **API freeze.** Five new top-level names enter the `api-check` snapshot; removing or renaming
  one later is a break. 065/066 additions to `PersonalData` must be defaulted fields (additive).
- **Private helper reuse.** The CLI imports `varco_core.providers._scan_module` (same
  distribution). If that helper's semantics change, discovery changes with `autodiscover()` —
  intended coupling, commented at the import.
- **"Encrypted" is not "erasable per subject".** `EncryptedHint` fields use whatever
  `FieldEncryptor` the app binds — often a tenant or global key, not a per-subject scope. The
  inventory's `encrypted` column must never be described as "crypto-shreddable"; that guarantee is
  Plan 066's to design.

## Open questions

1. **Whole-row vs classified-fields-only export** for subject-linked entities — Plan 065 decides;
   this plan exposes both (`entity` for whole rows, `fields` for the classified subset).
2. **Per-field erasure strategy** (`PersonalData(erasure=…)`: delete / anonymise / shred) — Plan
   066 may add it as a defaulted field; not pre-empted here.
3. **Should `DataSubjectRef` fields count as personal data in the inventory** (a subject id is a
   pseudonymous identifier)? Chosen: listed under `data_subject_links`, not auto-classified — an
   app may also mark the field `PersonalData(ONLINE_IDENTIFIER)`. Revisit if a DPO reviewer asks.
4. **Default subject kind `"user"` vs required `kind`.** Chosen: defaulted for the single-subject
   app; a multi-subject app names kinds explicitly.
5. **Indirect links** (`Comment → Post → author_id`) — out of scope; does 065's contributor seam
   need a declarative "via" form, or is a hand-written contributor enough?
6. **Auto-contribution** of `PersonalDataPostureCollector` by `SAModule`/`BeanieModule` from their
   registered models — deferred; explicit `contribute()` for now. Worth a row if adoption shows
   the explicit step is forgotten.
7. **Fideslang crosswalk** ships only if the licence and keys verify (Risks).
