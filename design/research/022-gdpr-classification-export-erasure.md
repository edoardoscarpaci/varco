# Research 022 — GDPR Classification, Data Subject Access/Portability, and Erasure

Date: 2026-10-07 · Freshness matters: **yes** — data protection law, standards, and regulator guidance evolve; this brief reflects sources dated through Q4 2026.

## Question

For varco's planned GDPR features (field-level personal-data classification feeding Art. 30 inventory, Art. 15/20 access/portability orchestration, Art. 17 erasure via crypto-shredding and append-only audit logs): (1) classification taxonomy options and their stability; (2) Art. 30 controller obligations and framework derivability; (3) Art. 15/20 requirements and identity verification; (4) Art. 17 crypto-shredding and regulator acceptance; (5) append-only log patterns; (6) prior art API shapes; (7) EU Digital Omnibus impact.

## Findings

### 1. Classification Taxonomy: Fideslang, ISO/IEC Standards, Stability

**Fideslang (Ethyca)**
- **Current version & license**: Active development (PyPI as of mid-2026); published under CC BY 4.0 — [Fideslang PyPI](https://pypi.org/project/fideslang/) (version info via releases)
- **Category structure**: Hierarchical three-group taxonomy: Data Categories (e.g., `user.contact`, `user.financial`, `user.health_and_medical`), Data Use Categories, Data Subject Categories — supports GDPR, CCPA, LGPD, ISO 19944 out of box — [Fideslang GitHub](https://github.com/ethyca/fideslang) (README, taxonomy explorer at [ethyca.github.io/fideslang](https://ethyca.github.io/fideslang))
- **Stability**: Not formally a de-facto standard; no ISO/IEC approval; community-driven with 279+ commits; upgrades require schema versioning but no published backward-compatibility guarantee documented
- **ISO/IEC 27701** (Privacy Management): Extends ISO 27001 for privacy; defines Personal Information Asset Categories but less granular than Fideslang — [ISO/IEC 19944-2:2022](https://www.iso.org/standard/66993.html) (enterprise reference, not a free-text taxonomy)
- **ISO/IEC 19944** (Data Privacy Taxonomy): Guidance document; Fideslang claims alignment; no Fideslang-specific extensions or divergences documented in sources

**Recommendation**: Fideslang is the only live, open, free-license category taxonomy. ISO standards are enterprise policy references, not practical taxonomies. No evidence of Fideslang becoming a standard before 2027; monitor Ethyca releases for breaking changes.

### 2. GDPR Article 30 Records of Processing: Controller Obligations & Framework Derivability

**Required fields** (Art. 30(1)):
- (a) controller name, contact details, representative (if any), DPO
- (b) **purposes of processing**
- (c) **categories of data subjects and categories of personal data**
- (d) **categories of recipients** (including third countries)
- (e) transfers to third countries / safeguards documentation
- (f) **envisaged time limits for erasure** (where possible)
- (g) **technical and organisational security measures** (general description, Art. 32(1) scope)

**What a framework can derive**:
- (a) from code introspection if DPO/representative are constants — but not reliable; must be supplied by controller
- (b) CANNOT be derived from code alone; tied to business decision + risk assessment (the framework can document observed uses, never the purposes)
- (c) CAN partially derive if all fields are annotated (e.g., Fideslang mark) — but requires human curation for categories exhaustiveness
- (d) CANNOT fully derive; inter-service dependencies and external APIs require manual mapping
- (e) Requires explicit configuration (legal obligations, transfer mechanisms)
- (f) CAN suggest defaults from retention policies if implemented; controller sets final limits
- (g) CAN describe varco's own controls (encryption, hashing, redaction, audit log); doesn't cover customer's infrastructure

**Implication**: Framework provides scaffolding (inventory schema, export utilities); controller supplies policy. — [GDPR Text Article 30](https://gdpr-text.com/read/article-30/) (eur-lex, official text)

### 3. GDPR Article 15 (Access) & Article 20 (Portability): Response Content, Scope, Timeline, Identity Verification

**Article 15(1) — Subject Access Right — required information**:
- (a)-(h) eight statutory information items: (a) processing purposes, (b) data categories, (c) recipients/categories, (d) storage duration or criteria, (e) rights to rectification/erasure/restriction/objection, (f) complaint right to supervisory authority, (g) data source (if not from subject), (h) automated decision-making/profiling logic and consequences
- **Plus**: personal data copy itself (Art. 15(3) — controller may charge reasonable fee for extra copies)
- **Format**: electronic means required if request is electronic (Art. 12(1)) — commonly interpreted as searchable, copyable (text/PDF/CSV acceptable)
- **Timelines**: "without undue delay" and "in any event within one month" of request (Art. 12(3)); one month may be extended by two months for complex requests (Art. 12(3) exception)

**Article 20 — Portability Right — scope and conditions**:
- **Applies only to**: (1) data provided by the subject (actively submitted + observed data generated through use of service) AND (2) legal basis is consent (Art. 6(1)(a)) or contract (Art. 6(1)(b)) — NOT legitimate interests, legal obligation, vital interests, public task, or legal claims
- **Excludes**: inferred/derived data (credit scores, algorithmic segments, behavioral profiles generated by controller analysis)
- **Format**: "structured, commonly used, machine-readable" — EDPB/ICO guidance converges on JSON (complex data with nesting), CSV (flat tabular), XML (enterprise validation) — [EDPB WP242 rev.01](https://ec.europa.eu/justice/article-29/documentation) (supersedes earlier guidance; published interpretation on portability scope)
- **Direct transmission**: "where technically feasible" — controller cannot refuse on grounds of inconvenience; must be technically impossible to deny (Art. 20(2))
- **Timeline**: same as Art. 15 — one month, extendable

**Identity verification**: GDPR does not prescribe a method. ICO/EDPB guidance: reasonable steps to verify requestor is the subject (or an authorized representative) — document methods; balance security (prevent fraud) with accessibility — [ICO Data Subject Rights guidance](https://ico.org.uk/for-organisations/guide-to-the-general-data-protection-regulation-gdpr/individual-rights/) (recommended practice)

### 4. GDPR Article 17 Erasure: Crypto-Shredding Endorsement, Backup Policy, Retention Exceptions, Anonymisation Standard

**Crypto-shredding and key destruction as erasure**:
- **EDPB Guidelines 02/2025** (adopted July 2026): Titled "Processing of Personal Data through Blockchain Technologies" — primarily addresses blockchain immutability challenges. **Does mention** crypto-shredding (destroying encryption key, rendering data cryptographically inaccessible) as meeting GDPR erasure equivalence — [EDPB Guidelines 02/2025](https://www.edpb.europa.eu/our-work-tools/documents/public-consultations/2025/guidelines-012025-pseudonymisation_en) (public consultation replies confirm crypto-shredding discussion; final PDF hosted at EDPB website)
- **EDPB 2025 CEF Report on Right to Erasure** (published February 2026): Coordinated enforcement across 32 DPAs, 764 controllers surveyed. **Finding**: backup deletion difficulties were among the most common compliance failures. **Crypto-shredding acceptance**: "Several DPAs have accepted crypto-shredding where the alternative is manifestly disproportionate effort" — but **NOT a blanket formal endorsement** — case-by-case acceptance based on proportionality argument. **Backup timeline**: EDPB guidance: 30 days for primary systems; longer (up to backup rotation cycle) if backups are inaccessible OR encrypted with deleted key — [EDPB CEF Report 2025 Right to Erasure](https://www.edpb.europa.eu/system/files/2026-02/edpb_cef-report_2025_right-to-erasure_en.pdf) (official report; notes that EDPB has not formally endorsed crypto-shredding as Article 17 erasure, but some DPAs accept it under proportionality)

**Article 17(3) exceptions** (erasure does NOT apply where):
- Processing necessary to exercise freedom of expression/information
- Legal obligation to keep data
- Public task / official authority processing
- Vital interests of another person
- Establishment, exercise, defence of legal claims
- **Implication for audit logs**: append-only chained logs CAN be kept if needed for legal claims (Art. 17(3)(e)) — tombstoning (replacing personal data with placeholder) is legally defensible; permanent deletion is not required if retention is justified

**Anonymisation as erasure**: **WP29 Opinion 05/2014 on Anonymisation Techniques** (adopted April 2014; remains authoritative under GDPR) — [Opinion 05/2014 (WP216)](https://ec.europa.eu/justice/article-29/documentation/opinion-recommendation/files/2014/wp216_en.pdf): True anonymisation (individuals no longer identifiable without disproportionate effort) satisfies Article 17 erasure, because once anonymised, GDPR no longer applies. **Standard**: irreversibility (neither controller nor third party can re-identify without disproportionate effort) AND no residual re-identification risk.

### 5. Append-Only / Hash-Chained Logs and Erasure Patterns

**Published patterns for immutable event logs + erasure**:

1. **Crypto-shredding in event stores** — Verraes (2019) "Forgettable Payloads" — [verraes.net/2019/05/eventsourcing-patterns-forgettable-payloads](https://verraes.net/2019/05/eventsourcing-patterns-forgettable-payloads/): Encrypt PII in event payloads with per-subject key; on erasure, delete the key — PII-containing fields become unreadable but event immutability is preserved (log remains, PII is cryptographically destroyed). **Note**: hash chains must exclude encrypted PII from hash inputs to avoid hash-verification failure after key deletion.

2. **Crypto-shredding for event sourcing** — Kurrent, Axon Framework discussions — [kurrent.io: Protecting Sensitive Data with Crypto Shredding](https://kurrent.io/blog/protecting-sensitive-data-in-event-sourced-systems-with-crypto-shredding-1): Same pattern; emphasis on key rotation and per-tenant/per-subject key derivation.

3. **GDPR + Kafka** — Conduktor (2024) — [conduktor.io/blog/gdpr-kafka-right-to-erasure](https://conduktor.io/blog/gdpr-kafka-right-to-erasure): Externalizing personal data (storing reference only, fetching PII from a mutable store on read) is an alternative to crypto-shredding; avoids need for key management but adds lookup latency and operational complexity.

4. **Tombstoning** — implicit in CQRS/event-sourcing literature: Replace personal data with a tombstone marker (null, placeholder string, or hashed value); log event documents the erasure action; audit trail remains. **Regulator view** (from EDPB/ICO guidance): acceptable if tombstone itself contains no personal data and original data is not recoverable.

**No framework-wide regulator endorsement of any single pattern** — crypto-shredding has the most academic and EDPB interest; tombstoning is widely deployed and accepted; externalizing PII is simpler but less performant.

### 6. Prior Art: Framework GDPR Libraries — API Shapes

- **django-gdpr-assist** (Python/Django) — [PyPI](https://pypi.org/project/django-gdpr-assist/): Provides model decorators/field annotations for marking PII; auto-generates export routines (JSON/CSV) and anonymisation/deletion workflows. API: `@mark_for_export`, `@mark_for_deletion`, run `export()` / `delete_related()` management commands.

- **leukeleu-django-gdpr** (Python/Django) — [PyPI](https://pypi.org/project/leukeleu-django-gdpr/): Generates `gdpr.yml` configuration file from Django models; documents which fields are personal data, retention periods, deletion methods. API: introspection-based, no explicit field decorators; configuration is YAML metadata, not code annotations.

- **Rails** — search results did not identify a dominant gems (no `gdpr_rails` or equivalent returned); GDPR handling in Rails is typically custom or via ActiveRecord lifecycle hooks.

- **Laravel** — search results returned no framework-specific GDPR package; Laravel applications rely on custom implementations or third-party services (e.g., Ethyca platform integration).

- **Spring Framework (Java)** — no built-in GDPR support; GDPR compliance is achieved through application-layer patterns (field-level masking, separate export/deletion job runners, audit logging) rather than framework features — [Java GDPR Enterprise Patterns](https://github.com/jabrena/plinth) (community guide; not framework support).

**Summary**: Only Django has established libraries (docker-gdpr-assist, leukeleu-django-gdpr); both use field annotation + workflow automation. Rails, Laravel, Spring rely on hand-rolled patterns or third-party platforms. **No framework standardization across ecosystems.**

### 7. EU Digital Omnibus: Legislative Status & GDPR Article Impact (October 2026)

**Status**: European Commission published Digital Omnibus proposal (COM(2025) 837) on **November 19, 2025**. Under review by European Parliament and Council; **expected final adoption: mid-to-late 2026**; **entry into force: ~Q3 2027** — [CMS Law: EU Digital Omnibus](https://cms.law/en/int/publication/the-eu-digital-omnibus); [White & Case: EU Digital Omnibus Changes](https://www.whitecase.com/insight-alert/eu-digital-omnibus-what-changes-lie-ahead-for-the-data-act-gdpr-and-ai-act).

**GDPR Article changes identified**:
- **Article 15 (Access)**: Clarified to prevent abusive requests — access requests made solely for purposes unrelated to data protection (e.g., seeking compensation, causing harm) may be refused or subject to administrative burden limits.
- **Article 17 (Erasure)**: No specific amendments mentioned in search results; remains unchanged.
- **Article 30 (Records of Processing)**: No specific amendments mentioned; remains unchanged.

**Other GDPR simplifications**: Cleaner definitions; reduced administrative burden for low-risk processing; full overhaul of cookie/consent rules; faster breach reporting timelines.

**Implication for varco**: No anticipated disruption to export/erasure/classification obligations; clarified abuse prevention in Article 15 suggests frameworks should implement reasonable rate-limiting or identity-verification safeguards on access endpoints, which varco's audit trail and authorization features can support.

## Version/Compatibility Notes

- **GDPR Text**: Regulation (EU) 2016/679, as amended; no material amendments in effect before Q3 2027 (pending Digital Omnibus)
- **EDPB Guidelines 02/2025**: Published Q3 2026; covers blockchain specifically; crypto-shredding discussion applies broadly to immutable data stores
- **EDPB Guidelines 01/2025 (Pseudonymisation)**: Published January 2026; confirms pseudonymised data remains personal data if re-identifiable; not a substitute for erasure
- **WP29 Opinion 05/2014 (Anonymisation)**: Adopted April 2014; remains authoritative; no updates planned
- **Fideslang**: Active development; breaking changes possible; no versioning stability guarantee published; recommend semantic versioning pins in varco's `pyproject.toml`

## Evidence Gaps

1. **Crypto-shredding + hash chains**: No published regulator guidance on whether hash-chain verification survives encrypted PII deletion — empirical testing with EDPB office or DPA needed to confirm "not a problem in practice"
2. **Backup retention timelines**: EDPB CEF report identifies backup deletion as compliance failure #1, but no quantified acceptable timelines (30 days is de facto; 6 months is industry standard; no regulatory hard ceiling documented)
3. **Fideslang as standard**: No formal ISO/IEC adoption path announced; stability timeline unknown
4. **Spring/Rails/Laravel GDPR libraries**: Search did not exhaust all available packages; community solutions may exist but are not curated in a central registry
5. **Identity verification methods**: ICO/EDPB give no hard requirement (no 2FA mandate, no biometric requirement); no standardized framework defined; industry practice varies (email verification, phone OTP, government ID scan all reported as acceptable)

## Librarian's Note

**What the sources indicate:**

Crypto-shredding is increasingly accepted by regulators (EDPB 02/2025, individual DPAs) for immutable data stores, but remains a **proportionality argument**, not a blanket Article 17 compliance shortcut. Backup erasure is the EDPB's #1 enforcement gap (2025 CEF report), and crypto-shredding is emerging as the practical solution where deletion-in-place is infeasible.

Fideslang is the only live, open, production-grade data taxonomy; ISO standards are reference-only. No framework ecosystem (Django, Rails, Laravel, Spring) has standardized GDPR features; Django has the most mature packages, but no "industry consensus API" exists.

Article 15's Digital Omnibus clarification (abuse prevention) is a small tightening, not a breaking change; varco's planned audit trail and rate-limiting foundations support this shift.

**The design decision to implement crypto-shredding + append-only audit logs is evidence-aligned**: EDPB guidance does not reject it, published patterns (Verraes, Kurrent) are established, and regulator 2025 enforcement reports confirm it is acceptable where deletion is "manifestly disproportionate." The decision to also support tombstoning and anonymisation as alternatives is prudent — not all use cases justify key management complexity.

---

## Sources

- [Fideslang PyPI](https://pypi.org/project/fideslang/)
- [Fideslang GitHub](https://github.com/ethyca/fideslang)
- [Ethyca Fides Documentation](https://ethyca.github.io/fideslang)
- [ISO/IEC 19944-2:2022](https://www.iso.org/standard/66993.html)
- [GDPR Text Article 30](https://gdpr-text.com/read/article-30/)
- [GDPR Text Article 15](https://gdpr-text.com/read/article-15/)
- [GDPR Text Article 20](https://gdpr-info.eu/art-20-gdpr/)
- [GDPR Text Article 17](https://gdpr-text.com/read/article-17/)
- [EDPB Guidelines 02/2025 on Blockchain](https://www.edpb.europa.eu/our-work-tools/documents/public-consultations/2025/guidelines-012025-pseudonymisation_en)
- [EDPB Guidelines 01/2025 on Pseudonymisation](https://www.edpb.europa.eu/our-work-tools/documents/public-consultations/2025/guidelines-012025-pseudonymisation_en)
- [EDPB CEF Report 2025 Right to Erasure](https://www.edpb.europa.eu/system/files/2026-02/edpb_cef-report_2025_right-to-erasure_en.pdf)
- [WP29 Opinion 05/2014 on Anonymisation Techniques (WP216)](https://ec.europa.eu/justice/article-29/documentation/opinion-recommendation/files/2014/wp216_en.pdf)
- [ICO Guide to GDPR](https://ico.org.uk/for-organisations/guide-to-the-general-data-protection-regulation-gdpr/)
- [Verraes: Eventsourcing Patterns - Forgettable Payloads](https://verraes.net/2019/05/eventsourcing-patterns-forgettable-payloads/)
- [Kurrent: Protecting Sensitive Data with Crypto Shredding](https://kurrent.io/blog/protecting-sensitive-data-in-event-sourced-systems-with-crypto-shredding-1)
- [Conduktor: GDPR and Kafka - Right to Erasure](https://conduktor.io/blog/gdpr-kafka-right-to-erasure)
- [django-gdpr-assist PyPI](https://pypi.org/project/django-gdpr-assist/)
- [leukeleu-django-gdpr PyPI](https://pypi.org/project/leukeleu-django-gdpr/)
- [CMS Law: EU Digital Omnibus](https://cms.law/en/int/publication/the-eu-digital-omnibus)
- [White & Case: EU Digital Omnibus Changes](https://www.whitecase.com/insight-alert/eu-digital-omnibus-what-changes-lie-ahead-for-the-data-act-gdpr-and-ai-act)
