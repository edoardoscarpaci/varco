# Index — 3.2 extension rows (S17, S19–S23)

The `# 3.2 — security release` cycle in [BACKLOG.md](../BACKLOG.md) was **extended on 2026-09-07**
by a second `/discover` pass, after `S1`–`S16` all landed. That extension is **six rows split
across five plans**. This file is the map: what each slice owns, the edges between them, the order
to build them in, and what "done" means for the set.

It exists so the slices do not overlap and so a reader can see the whole extension without opening
five plan files. **It carries no design content of its own** — each plan owns its decisions.

⚠️ These rows ship in **3.2.0**, not 3.3.0. 3.3.0 is reserved for the DI fix.

---

## Why this is split at all

Split gate — three conditions held, any one would have sufficed:

- **2+ independently shippable deliverables.** Inbound webhook verification ships with nothing
  else; conformance-guard recovery is pure documentation integrity and touches no production code.
- **Far past ~12 steps and 3+ subsystems.** One plan would have spanned `varco_core.webhook`, a new
  `varco_core.retention`, a new `varco_core.redaction`, `varco_core.authority`,
  `varco_fastapi`'s middleware stack and lifespan, and `testkit/varco_conformance`. The five plans
  total **27 phases**.
- **Uneven risk.** 039 deletes data on a timer and 040 changes what is written into an existing
  audit trail; 042 changes no production code at all. Isolating those is the point.

Carved along **deliverable boundaries, not layers** — each plan is independently implementable,
testable and mergeable.

## ⚠️ The finding that shaped all five plans

**Every one of the six rows had a premise that was partly wrong**, and each planner verified its
row against source before designing. This is the single most important thing to know before
building, because the rows' own sentences are not a reliable spec:

| Row | What the row claimed | What source says |
|---|---|---|
| `S19` | varco "verifies nothing inbound" | `WebhookSigner.verify()` is already abstract (`signing.py:73-76`) and `StandardWebhooksSigner.verify()` already ships correct constant-time, rotation-aware, timestamp-first verification (`:145-178`). The gap is header parsing, replay, raw bytes, wiring |
| `S20` | five cleanup verbs exist, nothing schedules them | `varco retention prune` already ships for DLQ+audit (`cli/retention.py:39-60`); `JobPoller(retention_sweep=True)` already sweeps jobs (`poller.py:75-88`); **outbox pruning does not exist at all** (`service/outbox.py:62-64`). And the "shipped cron→Job path" is missing its driver *and* its payload |
| `S21` | four surfaces undefended | Exactly **one** has a live leak (audit before/after JSON, `audit.py:538,572-575`). `error_params()` is safe in-tree; the logging middleware emits no user data at all |
| `S23` | the findings were lost with the table | The findings **survived** as `KI-N` references in source docstrings — only the *index* died. And "zero `BUG:` xfails" is off by one (`varco_redis/tests/test_redis_cache_disposes.py:91-103`) |
| `S17` | (accurate) | Confirmed: metrics execute outside tracing (`app.py:582-604`) |
| `S22` | (accurate) | Confirmed: no refresher task exists |

## The slices

| Plan | Rows | Deliverable | Risk | Brief |
|---|---|---|---|---|
| [038](038-inbound-webhook-verification.md) | S19 | Inbound webhook signature verification — `WebhookVerifier` ABC, SW/Svix + Stripe/GitHub/Slack adapters, replay guard over the shipped idempotency store, one FastAPI route dependency | 🟢 pure addition, off by default | 013 |
| [039](039-retention-and-purge-automation.md) | S20 | Retention & purge automation — `RetentionPolicy`/`RetentionRegistry`/`RetentionTarget` + five adapters, materialized onto the cron→Job path | 🔴 **deletes data**; also carries a 🔴 job-runner correctness fix | — |
| [040](040-unified-redaction-seam.md) | S21 | Unified redaction seam — `Redactor` Protocol, `RedactionPolicy`, mechanical `error_params()` safety, the audit hook the docstring already promised | 🟡 changes what an existing audit trail stores | — |
| [041](041-metrics-ordering-and-jwks-refresh.md) | S17, S22 | `MetricsMiddleware` moves inside `TracingMiddleware`; `start_refresh()`/`stop_refresh()` + `JwksRefreshLifecycle` | 🟡 observability blast radius on every latency series | 012 |
| [042](042-conformance-guard-recovery.md) | S23 | Conformance-guard recovery — the findings register in `COVERAGE.md`, two Docker-free regression tests, CLAUDE.md repointed | 🟢 **no production code change** | — |

Briefs are in `design/research/`: **012** OTel exemplars and metrics span context (backs S17),
**013** inbound webhook signature verification (backs S19). **011** security platform table stakes
backs the extension's scoping decision. S20, S21 and S23 are repo-internal and are evidenced by
`file:line` citations throughout their plans, not by a brief.

### Grouping decisions worth recording

- **S17 and S22 share a plan because both are `varco_fastapi` application assembly** — one moves an
  `add_middleware()` call, the other adds a lifespan component. They share no code and are two
  independently mergeable phases. Splitting them would have made two ~4-step plan files.
- **S23 got its own plan despite being the smallest row.** Its deliverable is documentation
  integrity, it ships no production code, and it is the only plan touching
  `testkit/varco_conformance/`. Folding it into 041 would have mixed a zero-risk doc change into a
  plan with an observability blast radius.
- **No plan owns another plan's `COVERAGE.md` row.** Each plan that adds an ABC writes its own —
  038 (`WebhookVerifier`), 039 (`RetentionTarget`), 040 (`Redactor`). 042 restores the *register*
  and explicitly does not pre-write rows for siblings.

## Dependency edges

**There are no hard build-order dependencies.** All five slices are independently implementable and
mergeable. What exists instead is three shared-file edges — rebase friction, not sequencing:

| Edge | Files | Resolution |
|---|---|---|
| 038 · 039 · 040 · 042 | `testkit/varco_conformance/COVERAGE.md` | 042 appends a **new section**; 038/039/040 each append one row to the existing matrix. No conflict in the table itself; whichever lands last rebases |
| 039 · 041 | `varco_fastapi/varco_fastapi/app.py` keyword list | 039 adds `retention=`, 041 adds `jwks_refresh=`. A two-line rebase, not a semantic collision |
| 039 · 041 | `varco_fastapi/varco_fastapi/lifespan.py` | ⛔ **Off-limits for restructuring by both.** Each adds a component satisfying the existing `AbstractLifecycle` Protocol (`lifespan.py:73-90`) and registers it through the existing path. Recorded in both plans |
| 039 · 040 | `varco_core/varco_core/service/audit.py` | **040 owns what goes in** (write path, `:512-606`), **039 owns what goes out by age** (calls the shipped `delete_where`, adds nothing). 040's DoD requires zero diff inside `audit.py:267-444` |

## Build order

Ordered by risk ascending, so the reviewable-in-isolation work lands before the data-affecting work:

1. **042** — zero production code. Restores the convention the other plans' `COVERAGE.md` rows
   append to, so landing it first makes the shared file's shape settled.
2. **039 Phase 1 only** — 🔴 `JobRunner.recover()` violates `Job.run_at` (`runner.py:523-533`). A
   standalone correctness fix that is independent of the rest of 039; there is no reason to hold it
   behind the retention design.
3. **041 Phase 1 (S17)** — the ordering move. Small, one decision, its own review, and an
   observability blast radius that deserves not to be buried in a larger diff.
4. **040 Phase 1** — the zero-behaviour-change extraction. Mergeable on its own and must precede
   040's later per-surface phases.
5. **038** — fully independent; may land at any point from here.
6. **041 Phase 2 (S22)**, then **040 Phases 2–6**, then **039 Phases 2–8** — the behaviour-changing
   remainder, largest and most data-affecting last.

Steps 1–4 are each independently mergeable and independently revertible.

## What "done" means for the set

- All six rows moved to `## Shipped this cycle` in BACKLOG.md, verified **present in source**, not
  merely marked done — the standard the `S1`–`S16` pass set on 2026-09-07.
- `make lint`, `make test`, `make type-check` green; `scripts/api_surface.py --check` regenerated
  and committed wherever an `__all__` moved (038, 039, 040, 041 — **not** 042).
- CLAUDE.md's two now-false claims are **deleted, not amended**: the ⚠️ saying no JWKS background
  refresher exists (041), and the Test Conventions pointer at a BACKLOG table that does not exist
  (042).
- Nothing in the set is on by default that was not on by default before, with one deliberate
  exception: 040's mechanical `error_params()` safety, which is byte-identical for every in-tree
  exception.
- Each new public seam has a `technical_docs/features/*.md` with a **Pitfalls** table, a README
  section, an ARCHITECTURE.md entry, and a CLAUDE.md decision-tree row.

## Carried forward, not silently dropped

Each plan filed BACKLOG rows for things it found and deliberately did not fix. These are **new work
discovered during planning**, and they must not be lost the way S23's register was:

- **`MetricsMiddleware` counts mapped exceptions as `500`** while the client receives 404/409/422 —
  every error-rate dashboard is wrong for mapped exceptions (041, pre-existing, out of scope).
- **Outbox pruning does not exist**; excluded from 039 as event loss rather than invented.
- **`DEFAULT_REDACT_PATTERNS` substring matching** means `"pin"` redacts `shipping_address` and
  `"auth"` redacts `author` — filed by 040, and the stated reason audit redaction stays opt-in in 3.2.
- **`_get_audit_diff_create()` is documented at `audit.py:527-529` and exists nowhere** (040).
- **Whether the Prometheus exporter actually emits exemplars end-to-end** is unverified; brief 012
  has no evidence either way (041).
- **`inspect_jwks_posture()` and `inspect_retention_posture()` need wiring into `SecurityPosture`** —
  both plans export the pure inspector; Plan 036 owns the harness. One harness pass, not two.
