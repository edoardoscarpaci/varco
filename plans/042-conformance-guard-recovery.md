# Plan 042 — Conformance-guard recovery: restore the lost findings register and repoint CLAUDE.md (S23)

Covers BACKLOG 3.2 extension row **S23** (🟡 should, S — *conformance-guard recovery: restore the
lost `strict=True` xfail findings and fix CLAUDE.md's dangling pointer*).

**No research brief backs this row** — it is entirely repo-internal. Every claim below is cited
`file:line` against source read while writing this plan.

> ⚠️ **The row's premise is partly wrong, and the correction reshapes the plan.** All four named
> bugs are **already fixed in source**, the findings were **not** lost (they survived as `KI-N`
> references in source and test docstrings), and the "zero `BUG:` xfails" claim is **off by one**.
> What was actually lost is the **index**. See §D-premise. This is the same pattern both sibling
> planners hit (038: `verify()` already implemented; 039: two of three cleanup verbs already
> shipped).

## Scope and siblings

One of five plans covering the 3.2 extension rows (S17, S19–S23). **This is the only plan whose
deliverable is documentation integrity, and the only one touching `testkit/varco_conformance/`.**
It ships no production code change.

| Plan | Rows | Boundary with this plan |
|---|---|---|
| 038 | S19 — inbound webhook verification | No overlap. May append a `COVERAGE.md` row for its own ABC; **it owns that row, not this plan** |
| 039 | S20 — retention & purge automation | ⚠️ **Already owns a `COVERAGE.md` note row** for `RetentionTarget` (`plans/039-retention-and-purge-automation.md:812`, §D-S20-conformance). **This plan does not write it.** Both plans edit `COVERAGE.md`; see Risks |
| 040 | S21 — unified redaction seam | No overlap expected. If it adds an ABC, it owns its own `COVERAGE.md` row |
| 041 | S17, S22 — metrics ordering + JWKS refresh | No overlap ✅ |

**The precedent this plan preserves**: each plan that adds an ABC writes its own `COVERAGE.md`
row. This plan restores the *existing* guard convention and the register that indexes it — it
does **not** pre-write suites or rows for siblings' new ABCs.

⚠️ §D-register adds a **new section** to `COVERAGE.md`; it does not change the shape of the
existing matrix rows or the "Stated absences" bullets that 038/039/040 append to. Siblings' rows
therefore need **no follow-up edit**. If a sibling's row lands first, this plan's section appends
cleanly below it; whichever lands last rebases (one appended section, no conflict in the matrix
table itself).

## Goal

The repo stops documenting a guard it does not have. `CLAUDE.md`'s Test Conventions points at a
findings register that **exists**, that register records all five recovered conformance findings
with `file:line` evidence and current status, the two findings whose only regression guard is
Docker-backed gain a Docker-free one that runs in `make test`, and the convention itself is
sharpened to distinguish the three genuinely different things a red conformance run can mean.

## Non-goals

- **Fixing the four named bugs.** All four are already fixed (§D-premise). This plan changes no
  production source in `varco_redis`, `varco_memcached`, `varco_kafka`, or `varco_nats`.
- **Adding any `xfail` for currently-passing behaviour.** A `strict=True` xfail on a passing test
  *fails*, which would break `make test` on the first run. See §D-fixed.
- **Writing conformance suites or `COVERAGE.md` rows for siblings' new ABCs** (039's
  `RetentionTarget`, and whatever 038/040 add). Each plan owns its own row.
- **A new conformance module.** The suite count stays at eight.
- **Re-auditing the coverage matrix.** Plan 024 / C7 did that; this plan appends a section and
  corrects two stale counts, nothing more.
- **Any production behaviour change**, and therefore **no CHANGELOG entry** — see §D-changelog.
- **Reviving the `KI-N` numbering for non-conformance findings.** KI-8…KI-12 were general BACKLOG
  "Known issues" rows already resolved via Plans 020/024; they are out of scope and stay resolved.

## Design

### §D-premise — what is actually true, verified against source

Every one of the row's four named bugs is fixed, and each carries its recovered finding ID in a
source comment. This is the thread that recovers the lost register.

| ID | Finding | Status in source | Evidence |
|---|---|---|---|
| **KI-2** | `KafkaDLQ.delete_where()` never reached the ABC's no-predicate `ValueError` | **FIXED** — no-predicate check runs *first*, before `NotImplementedError` | `varco_kafka/varco_kafka/dlq.py:575-580`, rationale at `:558-566` |
| **KI-3** | `RedisCache.set()` truncated a sub-second `ttl` with `int()` | **FIXED** — `PSETEX` with `ms = round(effective_ttl * 1000)`, plus a `ValueError` when it rounds to ≤0 | `varco_redis/varco_redis/cache.py:291`, rationale `:284-290`, guard at `:292-300` |
| **KI-5** | `MemcachedCache.set()` truncated a sub-second `ttl` to `exptime=0` (= *never expire*) | **FIXED** — `math.ceil()`, rounds **up** to the smallest expressible non-zero `exptime` | `varco_memcached/varco_memcached/cache.py:361`, rationale `:340-349` |
| **KI-6** | `BeanieDeadLetterQueue.count_by_channel()` — beanie 2.0.1/motor 3.7.1 `await`s a non-coroutine cursor | **WORKED AROUND** — bypasses beanie's aggregation cursor with an `inspect.isawaitable()` guard | `technical_docs/features/dead-letter-queues.md:335-349` |
| **KI-7** | `NatsDLQ.delete_where()` — same defect as KI-2 | **FIXED** — no-predicate check first | `varco_nats/varco_nats/dlq.py:558-563`, rationale `:540-547` |

**Three corrections to the row's premise:**

1. **The findings were not lost — the *index* was.** Every ID above still resolves from a source
   or test docstring (`varco_nats/varco_nats/dlq.py:547` names KI-2; `varco_redis/varco_redis/cache.py:288`
   names KI-3; `varco_memcached/varco_memcached/cache.py:349` names KI-5;
   `varco_nats/tests/test_nats_conformance.py:55` names KI-7). Co-located documentation survived
   the trim that killed the table. That is evidence, not luck, and §D-register is built on it.
2. **"Zero `strict=True` xfails carrying a `BUG:` reason" is off by one.** There is exactly one:
   `varco_redis/tests/test_redis_cache_disposes.py:91-103`. It is **not** a conformance finding —
   it is a providify upstream gap, filed under the UPSTREAM-GAPS convention with its reason
   pointing at `design/upstream-gaps/providify-disposes-first-match.md`. It is the repo's
   **working precedent** for the shape §D-register adopts: *marker at the site, durable file for
   the detail, ledger is only an index.*
3. **The convention was applied — just with regression tests instead of xfails.** Because each bug
   was fixed in the same pass that found it, no xfail was ever warranted (§D-fixed). Two of the
   four also gained Docker-free regression tests. The convention's *failure* was never the marker;
   it was that "plus a one-line BACKLOG.md entry" targeted a file whose header explicitly warns it
   gets compacted (`BACKLOG.md:5-13`).

### §D-register — where the findings register lives so it cannot be trimmed away again

This is the actual decision in this row.

**Chosen: a new `## Conformance findings register` section in `testkit/varco_conformance/COVERAGE.md`,
plus a mandatory finding ID inside every `BUG:` xfail `reason=`, plus an `rg` liveness command in
CLAUDE.md.**

- ✅ `COVERAGE.md` is **already** the authoritative, never-trimmed conformance record (Plan 024 /
  C7, `COVERAGE.md:1-17`), and it has survived every trim that took the BACKLOG table. It is the
  one file whose stated job is answering *"for every implementation of one of the eight shared
  ABCs — is it covered, and if not, why not?"* — *"is it covered, and is it known-broken?"* is the
  same question, asked of the same audience, in the same reading session.
- ✅ **One pointer to repoint, not two.** CLAUDE.md already links `COVERAGE.md`
  (`CLAUDE.md:1209`); the dangling paragraph immediately below it (`:1217-1223`) gets to point at
  the file already cited one paragraph up.
- ✅ It lives **next to the suites** (`testkit/varco_conformance/`), so the person writing a
  conformance subclass sees it without knowing it exists.
- ✅ The mandatory finding ID makes the register and the markers **mutually verifying**: the `rg`
  command lists every live marker, and every marker names a row.
- ❌ `COVERAGE.md` grows a second responsibility. Accepted — it is one appended section with its
  own heading, and the two responsibilities share an audience and a lifetime.

**Alternatives considered:**

- **Keep it in `BACKLOG.md`** (status quo). ❌ **Rejected — this is the exact failure being
  repaired.** `BACKLOG.md:5-13` states outright that completed-work tables *are* trimmed and warns
  about the `cae7f33` wholesale loss. A register of *fixed* findings is, by construction, a table
  of completed work, so it is guaranteed to be trimmed again. ✅ Zero new files. Not enough.
- **A new `testkit/varco_conformance/KNOWN-ISSUES.md`.** ✅ Single responsibility; ✅ unambiguous
  name. ❌ A **third** conformance doc (suites, `COVERAGE.md`, this) splits one audit question
  across two files that must be read together, and a file with no other reason to be opened is
  precisely the kind that goes stale unnoticed — the failure mode being repaired. ❌ Rejected.
- **The xfail markers *are* the register; CLAUDE.md points at an `rg` command, no file.** ✅ Very
  strong on liveness — a `strict=True` xfail cannot rot silently; it turns red the moment the bug
  is fixed. ✅ Zero carrying cost. ❌ **Rejected as the sole mechanism, for one decisive reason: it
  structurally cannot record a *fixed* finding.** All five recovered findings are fixed or worked
  around and therefore have no marker to live in — an xfail-only register would have recorded
  exactly nothing today, which is the history this row exists to stop losing. ❌ Also fails
  discoverability: `rg` answers *"what is broken now?"* but never *"was this ever broken, and how
  was it resolved?"*, which is the question that cost this cycle a backlog row.
  **Its liveness property is kept** — adopted as the *cross-check* (Phase 3's `rg` command), not
  the register.

### §D-kinds — three things a red conformance run can mean; the convention currently conflates them

CLAUDE.md today gives one instruction for one situation. There are three, and the difference
decides whether you may edit `testkit/`.

| Kind | What it is | Action | May you edit `testkit/`? | In-tree precedent |
|---|---|---|---|---|
| **A — backend ABC violation** | The backend genuinely breaks the ABC's documented contract | `@pytest.mark.xfail(reason="BUG: KI-N …", strict=True)` on the subclass's override + a register row. **Never** an in-place production fix in the same pass | ❌ **Never** — weakening a shared assertion to accommodate one backend silently un-tests every other | KI-2/KI-7 (`varco_kafka/varco_kafka/dlq.py:575`, `varco_nats/varco_nats/dlq.py:558`) |
| **B — conformance-suite gap** | The suite's assertion is missing, or too weak to fail on a real violation. The backend is fine; the *guard* is not | **Fix in place in `testkit/`.** No xfail (there is no bug to pin), no register row required | ✅ **Yes — this is the one case you should** | `varco_kafka/tests/test_kafka_conformance.py:80-88` — `test_count_reflects_pushed_entries` asserts only `after >= before`, which trivially holds at `KafkaDLQ.count()`'s constant `-1` (`varco_kafka/varco_kafka/dlq.py:544`) |
| **C — legitimate backend capability divergence** | Both the backend and the suite are correct; the transport genuinely cannot express what the suite assumes | **Override the single test in the subclass** with a docstring arguing why, **or** a `COVERAGE.md` "Stated absences" bullet. Never loosen the shared suite | `varco_memcached/tests/test_memcached_conformance.py:33-62` — Memcached `exptime` is whole-seconds at the wire protocol, so the subclass overrides `test_ttl_expiry`'s 0.3s window rather than relaxing it for every backend |

- ✅ Each kind now has a distinct, verifiable action, so "fix the suite" stops looking like a
  violation of "never fix in place".
- ✅ The B/C distinction is what protects the shared suite: **C overrides one test in one
  subclass; B strengthens the suite for everyone.** Neither ever weakens it.
- ❌ Three rules where there was one. Accepted — the table is five lines and each row carries a
  real in-tree example, so it is checkable rather than aspirational.

### §D-fixed — what to do about five findings that are already resolved

**No xfail for any of them.** `strict=True` inverts the result: a passing test marked `xfail(strict=True)`
reports `XPASS(strict)` = **failure**, so adding markers here would turn `make test` red
immediately. This is not a subtlety to rediscover — it is the property the convention is built on
(`CLAUDE.md:1219`).

Each recovered finding instead gets:

1. **A register row** in `COVERAGE.md` carrying ID, symptom, status, the fix's `file:line`, and
   the `file:line` of the regression guard that keeps it fixed.
2. **A verified regression guard.** §D-fast establishes that three of five already have one and
   two do not.

- ✅ The register records resolved history — the thing an xfail-only design cannot do (§D-register).
- ✅ Every row is falsifiable: each cites a line a reader can open.
- ❌ A row can go stale if the cited line moves. Mitigated by citing the *guard test's* name
  alongside its line — a renamed test is greppable, a moved line is not.

### §D-fast — the fast-leg audit: two findings have no Docker-free guard

CLAUDE.md's own rule (`:1076-1079`) prefers *"a fast, dependency-free reproduction that runs in
`make test` over one gated behind Docker and `-m integration` — a nightly-only guard will not warn
you while you work."* Audited against source:

| ID | Docker-free guard | Evidence |
|---|---|---|
| KI-3 | ✅ **Exists** — `test_set_with_subsecond_ttl_preserves_precision`, asserts `_ttls_ms["k"] == 50` against `FakeRedis` | `varco_redis/tests/test_redis_cache.py:196-202` |
| KI-5 | ✅ **Exists** — `test_set_with_subsecond_ttl_rounds_up_to_one`, asserts `exptime == 1` against `FakeMemcached` (plus two neighbours pinning `ttl=1.2 → 2` and `ttl=0 → 0`) | `varco_memcached/tests/test_cache.py:215-252` |
| KI-6 | ✅ n/a — an upstream-driver workaround, not an ABC violation; covered by the Beanie DLQ's own tests | `technical_docs/features/dead-letter-queues.md:335-349` |
| **KI-2** | ❌ **MISSING** — the only guard is the inherited Docker-backed conformance test (`varco_kafka/tests/test_kafka_conformance.py:71-78`, `pytestmark = integration`). The Docker-free `TestKafkaDLQDeleteWhereRaises` covers **only** the *with-predicate* → `NotImplementedError` path | `varco_kafka/tests/test_kafka_dlq.py:502-508` |
| **KI-7** | ❌ **MISSING** — identical shape: `TestNatsDLQDeleteWhereRaises` covers only the with-predicate path | `varco_nats/tests/test_nats_dlq.py:202-206` |

**Both gaps are cheap to close, and this is verifiable from source rather than assumed:**
`KafkaDLQ.delete_where()` (`varco_kafka/varco_kafka/dlq.py:575`) and `NatsDLQ.delete_where()`
(`varco_nats/varco_nats/dlq.py:558`) each raise on their **first statement**, before touching a
producer, consumer, or connection. A no-predicate call therefore needs **no broker** — the
existing Docker-free test files already construct these DLQs directly and are not marked
`integration` (`varco_kafka/tests/test_kafka_dlq.py:29-34` imports only, no `pytestmark`).

- ✅ Two four-line tests move KI-2/KI-7's regression guard from nightly-only into `make test`.
- ✅ They sit beside the sibling `NotImplementedError` tests, so the pair reads as one contract.
- ❌ Duplicates an assertion the Docker-backed conformance suite already makes. Accepted, and
  deliberate: the conformance run proves it *against a real broker*; the fast test proves it
  *while you work*. CLAUDE.md's rule asks for exactly this.

### §D-claude — the CLAUDE.md edit, and two adjacent stale counts

CLAUDE.md's own rules apply: *link, don't inline*; *one home per fact*; *keep only what changes an
agent's behaviour*. The replacement is **shorter** than what it replaces and inlines no finding.

**Current text — `CLAUDE.md:1217-1223`, to be replaced verbatim:**

> **A conformance failure that reveals a genuine backend ABC-contract violation becomes
> `@pytest.mark.xfail(reason="BUG: ...", strict=True)` plus a one-line BACKLOG.md entry — never an
> in-place production-code fix.** `strict=True` means the xfail itself fails loudly if the
> underlying bug is ever fixed, so the marker doesn't silently rot. See BACKLOG.md's "Known issues
> found while implementing Plan 012" table for the accumulated findings (e.g. `RedisCache`/
> `MemcachedCache` truncating a sub-second `ttl` to `int()`, `KafkaDLQ`/`NatsDLQ.delete_where()`
> never reaching the ABC's "no predicate → `ValueError`" check).

**Exact replacement:**

> **A red conformance run means one of three things, and they take different actions** — a genuine
> backend ABC violation (`@pytest.mark.xfail(reason="BUG: KI-N …", strict=True)` + a register row,
> **never** an in-place production fix and **never** a weakened shared assertion), a gap in the
> suite itself (fix it in `testkit/`, no marker), or a legitimate backend capability divergence
> (override the one test in the subclass, with a docstring). The decision table and an in-tree
> example of each: `testkit/varco_conformance/COVERAGE.md`'s **Conformance findings register**,
> which is also where accumulated findings live — **not BACKLOG.md**, which is trimmed by design.
> `strict=True` means the marker fails loudly the moment the bug is fixed, so it cannot rot;
> `rg -n 'BUG:' varco_*/tests/ testkit/` lists every live marker and each must name a register row.

- ✅ Names all three cases (the behaviour-changing fact), links the detail, inlines no finding.
- ✅ States *why* not BACKLOG.md in six words, so the pointer is not "fixed" back later.
- ✅ Ships the `rg` liveness command — §D-register's rejected alternative kept as a cross-check.
- ❌ Slightly longer than a bare pointer. Justified: the three-way split is exactly the
  "changes an agent's behaviour" test, and it was the conflation causing the problem.

**Two adjacent stale counts, in scope by class** (this plan's deliverable *is* documentation
integrity; both are dangling facts of the same kind, in the same paragraph block):

| Location | Says | Truth |
|---|---|---|
| `CLAUDE.md:1203-1204` | `test_conformance_inmemory.py` runs *"the other **four** suites (`event_bus`, `cache`, `job_store`, `dlq`)"* | **Five** — `token_revocation` was added by Plan 034 / S13b (`varco_core/tests/test_conformance_inmemory.py:46,165`) |
| `CLAUDE.md:1210,1214` | *"every implementation of one of the **five** ABCs"* (×2) | **Eight** — `COVERAGE.md:3-7,15` already says eight; CLAUDE.md was not updated when 029/031/034 added theirs |
| `testkit/varco_conformance/__init__.py:5-7` | lists **five** ABCs | **Eight** |
| `testkit/varco_conformance/__init__.py:24-28` | *"plus a BACKLOG entry"* | The **same dangling pointer** as `CLAUDE.md:1220` — the module docstring carries a second copy |

### §D-changelog — no CHANGELOG entry, stated rather than invented

`CHANGELOG.md` records production behaviour changes. This plan changes **no** production source:
its edits are two test files, `COVERAGE.md`, `CLAUDE.md`, and a `testkit/` module docstring —
`testkit/` is never packaged (`COVERAGE.md:3`), and no shipped wheel's bytes change.

- ✅ No entry is the correct outcome, not an omission.
- ✅ `scripts/api_surface.py` regeneration is **not applicable** — no `__all__` in any distribution
  package is touched, so the `--check` gate cannot trip. Nothing to regenerate, nothing to commit.

## Steps

### Phase 1 — the register (`COVERAGE.md`) — independently verifiable, no test change

1. [ ] `testkit/varco_conformance/COVERAGE.md` — append a `## Conformance findings register`
       section after "What Plan 024 filled" and before the trailing audit-date line. Open with one
       sentence stating this is the durable home the convention points at, and that
       **BACKLOG.md is not** (it is trimmed by design — `BACKLOG.md:5-13`). Table columns:
       `ID | Suite | Backend | Symptom | Kind (A/B/C) | Status | Fix | Guard`.
2. [ ] Same file — populate the five recovered rows from §D-premise, each with the `file:line`
       citations from §D-premise **and** the guard citations from §D-fast (KI-2 and KI-7's guard
       cells reference the tests Phase 2 adds — write them as the final names):

       | ID | Backend | Kind | Status |
       |---|---|---|---|
       | KI-2 | `KafkaDLQ.delete_where()` | A | FIXED |
       | KI-3 | `RedisCache.set()` sub-second ttl | A | FIXED |
       | KI-5 | `MemcachedCache.set()` sub-second ttl | A | FIXED |
       | KI-6 | `BeanieDeadLetterQueue.count_by_channel()` | A (upstream) | WORKED AROUND |
       | KI-7 | `NatsDLQ.delete_where()` | A | FIXED |

3. [ ] Same file — add a short "How to file a new finding" subsection carrying §D-kinds' three-row
       decision table verbatim (A/B/C, action, may-you-edit-`testkit/`, in-tree precedent), plus:
       *the next free ID is `KI-13`* (KI-8…KI-12 were non-conformance BACKLOG rows resolved by
       Plans 020/024 — recorded so the series is never restarted at a colliding number), and the
       rule that every `BUG:` xfail `reason=` must name its `KI-N`.
4. [ ] Same file — under "Stated absences", add one bullet recording §D-kinds **Kind B**'s live
       example: `test_count_reflects_pushed_entries` asserts only `after >= before`, which
       trivially holds at `KafkaDLQ.count()`'s constant `-1`
       (`varco_kafka/varco_kafka/dlq.py:544`; noted at
       `varco_kafka/tests/test_kafka_conformance.py:80-88`). **Do not strengthen the assertion in
       this plan** — record it as the worked Kind-B candidate and leave it; see Open questions Q1.

**Verify:** `rg -n 'KI-2|KI-3|KI-5|KI-6|KI-7' testkit/varco_conformance/COVERAGE.md` → five rows
present. Then open each cited `file:line` and confirm it says what the row claims.

### Phase 2 — close the two fast-leg gaps (TDD: both tests fail if the fix is reverted)

5. [ ] `varco_kafka/tests/test_kafka_dlq.py` — in `TestKafkaDLQDeleteWhereRaises` (`:502`), add
       `test_delete_where_with_no_predicate_raises_value_error`: construct `KafkaDLQ(settings)`
       and assert `pytest.raises(ValueError)` on a bare `await dlq.delete_where()`. Comment: *KI-2
       regression, Docker-free — the ABC's no-predicate refusal must be reached BEFORE the
       backend-support `NotImplementedError` (`varco_kafka/varco_kafka/dlq.py:575-580`). The
       inherited conformance guard is `-m integration` only.* **No container, no `pytestmark`** —
       the method raises on its first statement.
6. [ ] `varco_nats/tests/test_nats_dlq.py` — in `TestNatsDLQDeleteWhereRaises` (`:202`), add the
       identical `test_delete_where_with_no_predicate_raises_value_error`, citing
       `varco_nats/varco_nats/dlq.py:558-563` and KI-7.
7. [ ] Confirm both new tests are collected **without** the `integration` marker (neither file
       declares `pytestmark`; do not add one).

**Verify (must pass with no Docker running):**
```bash
uv run pytest varco_kafka/tests/test_kafka_dlq.py varco_nats/tests/test_nats_dlq.py -q
uv run pytest varco_kafka/tests/test_kafka_dlq.py -k no_predicate -v
uv run pytest varco_nats/tests/test_nats_dlq.py -k no_predicate -v
```
**Falsification check** (do not commit): temporarily delete the no-predicate `if` block at
`varco_kafka/varco_kafka/dlq.py:575-580`; the new test must fail with `NotImplementedError`.
Restore.

### Phase 3 — repoint CLAUDE.md and the testkit docstring

8. [ ] `CLAUDE.md:1217-1223` — replace the quoted paragraph with §D-claude's exact replacement.
9. [ ] `CLAUDE.md:1203-1204` — *"the other four suites (`event_bus`, `cache`, `job_store`, `dlq`)"*
       → **five**, adding `token_revocation` (`varco_core/tests/test_conformance_inmemory.py:46,165`).
10. [ ] `CLAUDE.md:1210` and `:1214` — *"one of the five ABCs"* → *"one of the eight ABCs"* (both
        occurrences), matching `COVERAGE.md:6,15`.
11. [ ] `testkit/varco_conformance/__init__.py:5-7` — extend the ABC list from five to eight
        (`AbstractIdempotencyStore`, `WebhookSubscriptionRepository`,
        `AbstractTokenRevocationStore`).
12. [ ] `testkit/varco_conformance/__init__.py:24-28` — replace *"plus a BACKLOG entry"* with a
        pointer to `COVERAGE.md`'s findings register, and add the one-line A/B/C summary. This is
        the second copy of the dangling pointer; both must move together.

**Verify:**
```bash
rg -n 'Known issues found while implementing Plan 012' .        # → zero hits
rg -n 'BACKLOG' testkit/varco_conformance/                      # → zero hits
rg -n 'one of the five ABCs|other four suites' CLAUDE.md        # → zero hits
rg -n 'BUG:' varco_*/tests/ testkit/                            # → exactly 1 (the providify upstream gap)
```
The last command is the register's liveness cross-check (§D-register). Its single hit is
`varco_redis/tests/test_redis_cache_disposes.py:94` — an **upstream** gap filed under
UPSTREAM-GAPS, correctly **not** in the conformance register. Record that expectation next to the
command in `COVERAGE.md` so a future reader does not "fix" it into the register.

### Phase 4 — full green

13. [ ] `make lint` — `ruff check` + `ruff format --check` + `api-check` + `asyncapi-check` +
        `import-budget`. `api-check` cannot trip (§D-changelog: no `__all__` touched).
14. [ ] `make test` — all eleven suites, including the two new Docker-free tests.
15. [ ] Optional, Docker required — prove the conformance path itself is still green end-to-end:
        `make integration-test-clean PKG=varco_kafka` and `make integration-test-clean PKG=varco_nats`.
        Not required to merge (integration is not a required check); run if Docker is available.
16. [ ] Confirm **no** CHANGELOG edit and **no** `scripts/api_surface.py` regeneration were needed
        (§D-changelog). If either turns out to be required, this plan's scope was exceeded — stop
        and re-check.

## Edge cases

- **A reviewer asks for xfails on the four "bugs"** → they are fixed; `xfail(strict=True)` on a
  passing test reports `XPASS(strict)` = failure and reddens `make test`. §D-fixed; register rows
  carry the history instead.
- **`KafkaDLQ`/`NatsDLQ.delete_where()` acquires a connection before the predicate check** →
  Phase 2's Docker-free tests would hang or error. Verified false today (both raise on the first
  statement, `dlq.py:575` / `dlq.py:558`); if a future refactor moves I/O earlier, the new test
  fails loudly and *that* is a Kind-A finding against the ABC's ordering contract.
- **A finding is discovered that is both A and B** (backend violates the ABC *and* the suite was
  too weak to catch it) → file **both**: an xfail for the backend violation (Kind A) *and* a
  `testkit/` assertion strengthening (Kind B). They are independent and the B fix is what makes
  the A marker meaningful.
- **A sibling plan lands a `COVERAGE.md` row first** → this plan's section appends below;
  no conflict in the matrix table. Whichever lands last rebases (§Scope and siblings).
- **Someone adds a `BUG:` xfail without a `KI-N`** → Phase 3's `rg` cross-check surfaces a marker
  with no register row. The register's "How to file" subsection is the fix; no automation.
- **`NoopEventBus`/`NullTokenRevocationStore` fail a suite** → not a finding at all. They are
  Null Objects with existing stated absences (`COVERAGE.md:35-40,128-132`) — Kind C, already
  answered.

## Verification

```bash
# Phase 1 — register exists and is populated
rg -n 'Conformance findings register' testkit/varco_conformance/COVERAGE.md
rg -n 'KI-2|KI-3|KI-5|KI-6|KI-7'      testkit/varco_conformance/COVERAGE.md

# Phase 2 — the two new fast-leg guards, no Docker
uv run pytest varco_kafka/tests/test_kafka_dlq.py varco_nats/tests/test_nats_dlq.py -q
uv run pytest varco_kafka/tests/ varco_nats/tests/ -q          # unit legs, integration deselected

# Phase 3 — every dangling pointer gone; marker census matches the register
rg -n 'Known issues found while implementing Plan 012' .        # → 0
rg -n 'BACKLOG' testkit/varco_conformance/                      # → 0
rg -n 'one of the five ABCs|other four suites' CLAUDE.md        # → 0
rg -n 'BUG:' varco_*/tests/ testkit/                            # → 1 (providify upstream gap)

# Phase 4 — full green
make lint
make test

# Optional, Docker required — the conformance path end-to-end
make integration-test-clean PKG=varco_kafka
make integration-test-clean PKG=varco_nats
```

## Risks

| Risk | Likelihood | Mitigation |
|---|---|---|
| **`COVERAGE.md` edit collides with Plans 038/039/040** | Medium | Named in §Scope and siblings. This plan appends a **new section**, never edits the matrix table or existing absence bullets, so a merge conflict is textual at worst; whichever lands last rebases. 039 owns its own `RetentionTarget` row (`plans/039:812`) — this plan must not write it |
| **The register becomes the next thing to go stale** | Medium | This is the failure being repaired, so it cannot be waved away. Three defences: (a) it lives in a file with an existing, exercised no-trim norm (`COVERAGE.md`, survived every trim that took the BACKLOG table); (b) Phase 3's `rg` cross-check makes marker-vs-register drift mechanically visible; (c) `strict=True` on any live Kind-A marker still fails loudly on its own. ⚠️ Not automated — accepted for an S-sized row; see Open questions Q2 |
| **Phase 2's tests duplicate the conformance suite** | Certain, by design | §D-fast: the Docker-backed run proves it against a real broker, the fast test proves it while you work. CLAUDE.md `:1076-1079` asks for exactly this |
| **`CLAUDE.md` grows rather than shrinks** | Low | The replacement is shorter than the text it replaces and inlines no finding; §D-claude checks it against CLAUDE.md's own three editing rules |
| ⚠️ **ASSUMPTION — `make test` currently passes on `main`** | — | Not run while writing this plan (no command execution). If Phase 4 finds pre-existing red unrelated to this work, it is **not** this plan's to fix (CODING_STANDARD.md:818-824) — file it and proceed |
| ⚠️ **ASSUMPTION — no `conftest.py` marks `test_kafka_dlq.py`/`test_nats_dlq.py` as `integration` at the directory level** | — | Verified absent *in the files themselves* (no `pytestmark`); the package `conftest.py` files were **not** read. Phase 2 Step 7 verifies collection explicitly — if a directory-level marker exists, the new tests move to a Docker-free module instead |
| ⚠️ **ASSUMPTION — `KI-13` is the next free ID** | — | Derived from `rg 'KI-\d+'` across the tree (highest seen: KI-12, `plans/024:43`). If a higher ID surfaces, bump; the register's "next free ID" line is the single place to correct |

## Open questions

**Q1 — Should `test_count_reflects_pushed_entries` be strengthened now?** It asserts only
`after >= before`, trivially true at `KafkaDLQ.count()`'s constant `-1`
(`varco_kafka/varco_kafka/dlq.py:544`). It is the cleanest live Kind-B example, which is *why*
Phase 1 Step 4 records it rather than fixing it: strengthening it may turn `KafkaDLQ` red, which
would be a Kind-A finding needing its own xfail and register row — real scope, not an S row's
worth. **Recommendation: record now, fix in a follow-up.** Filed below.

**Q2 — Should the register↔marker cross-check be automated?** Phase 3's `rg` is manual. A ~20-line
`scripts/` check (every `BUG:` marker names a `KI-N`; every `KI-N` marker has a register row) could
join `make lint`'s no-`PKG` path beside `api-check`. **Not in this plan** — S-sized, and CLAUDE.md
is explicit that a new gate needs justification, not enthusiasm. Filed below.

**Q3 — Do the other six suites have Kind-B gaps like Q1's?** This plan audited `cache` and `dlq`
(the two the row named) plus the counted stale facts. `event_bus`, `job_store`, `channel_manager`,
`idempotency_store`, `webhook_subscription`, `token_revocation` were **not** assertion-audited.
Stated plainly rather than implied complete. Filed below.

## BACKLOG entries to add when this plan lands

| ID | Row | Severity | Complexity |
|---|---|---|---|
| `CONF-COUNT` | **Strengthen `DeadLetterQueueConformance.test_count_reflects_pushed_entries`** — `after >= before` trivially holds at `KafkaDLQ.count()`'s constant `-1`, so the assertion cannot fail on a real regression. The live Kind-B example recorded by Plan 042 (`COVERAGE.md`'s findings register). Strengthening it may surface a Kind-A `KafkaDLQ` finding needing its own xfail + register row — that is the work, and why it was not done inline | 🟢 nice | S |
| `CONF-RG` | **Automate the register↔marker cross-check** — a `scripts/` check asserting every `BUG:` xfail names a `KI-N` and every `KI-N` marker has a `COVERAGE.md` register row, wired into `make lint`'s no-`PKG` path beside `api-check`. Plan 042 Q2 left it manual deliberately | 🟢 nice | S |
| `CONF-AUDIT6` | **Assertion-audit the six unaudited conformance suites** — Plan 042 audited `cache` and `dlq` only. `event_bus`, `job_store`, `channel_manager`, `idempotency_store`, `webhook_subscription`, `token_revocation` have not been checked for Kind-B gaps (assertions too weak to fail on a real violation) | 🟡 should | M |

---

**Standing note for whoever executes this plan:** if any step reveals a *new* backend ABC
violation, it takes the standing rule — `xfail(reason="BUG: KI-13 …", strict=True)` + a register
row — **not** a production-code fix. This plan's licence covers two test files, `COVERAGE.md`,
`CLAUDE.md`, and one `testkit/` module docstring. Nothing else.
