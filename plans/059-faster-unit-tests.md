# Plan 059 — Faster unit tests: pytest-xdist per suite, a bounded suite pool, randomized order, no needless conformance waits (TEST-1)

Covers BACKLOG 4.0.0 feature-extension row **`TEST-1`** (`BACKLOG.md:55` — *"Faster unit tests —
`pytest-xdist` per suite, suites in parallel in `scripts/unit_tests.sh`, fake loop clock (looptime)
for sleep-bound tests, remove the 2 s in-memory conformance waits"*). One of eleven plans
(059–069); index `plans/000-index-4-0-feature-extension.md`. **Built FIRST** of the set
(`BACKLOG.md:37` — user override: *"Every other row's build loop runs the suites; faster tests pay
back inside this cycle"*). Touches **test infrastructure and tests only — zero product code.**

**Sources of record read for this plan (2026-10-07):** `BACKLOG.md:37,55,191`;
`scripts/unit_tests.sh` (all 176 lines); `scripts/integration_tests.sh:27,176-230` (its two pytest
invocations at `:212,:218`); `Makefile:13-16,321-330`; `.github/workflows/test.yml:1-127`; root
`pyproject.toml:24-75` (PEP 735 groups; `lint` included into `dev` at `:61`); `uv.lock` (pytest
9.0.2 `:2308`, pytest-asyncio 1.3.0 `:2324`; **no** pytest-xdist / pytest-randomly / looptime /
execnet); every package's `[tool.pytest.ini_options]`; `testkit/varco_conformance/{event_bus,cache,
job_store}.py`; `varco_core/tests/test_conformance_inmemory.py`; `varco_core/tests/conftest.py`;
`testkit/tls_fixtures.py:142-151`; the five subprocess snapshot tests;
`varco_core/tests/test_repo_ci_invariants.py:179-191`; `varco_core/varco_core/cache/invalidation.py:130-131`;
`varco_core/varco_core/job/memory.py:302,387,430`; `varco_core/varco_core/job/base.py:1072-1075`;
`CONTRIBUTING.md:19-32`; `technical_docs/common-pitfalls.md:1-30`; research
`design/research/020-pytest-parallelism-and-fake-clocks.md` (all) and
`design/research/019-streaming-orchestration-and-test-performance-2026.md` §C (`:56-80,102-108,160`).

> ⚠️ **Five premises corrected or sharpened from source before anything was decided.**
> (1) **`unit_tests.sh` does not parse pytest output.** The pass/fail/skip summary
> (`:158-175`) is built purely from each suite's **exit code** (`:144-155`: 0 → passed, 5 → no
> tests, else failed). Parallel suites therefore cannot break the *status*; only the *log* can
> interleave. The new per-suite counts (§D-script) are display-only and never decide status.
> (2) **The 2 s settle is not "free for synchronous backends"**, as `event_bus.py:39-41` claims —
> `asyncio.sleep(2.0)` (`:100,:122,:177`) always costs 2 s of wall clock. That is the measured
> 3 × 2.00 s in `TestInMemoryEventBusConformance`. In-process buses need no settle at all.
> (3) **The two other conformance waits are wall-clock waits, not loop-clock waits.** The cache
> TTL test (`cache.py:58`, 0.3 s) depends on `TTLStrategy`, which reads `time.time()`
> (`invalidation.py:130-131`); the lease reap test (`job_store.py:122`, 0.2 s) depends on
> `datetime.now(UTC)` (`job/memory.py:387,430`). looptime fakes neither (brief 020 §looptime:
> "`time.time()` and `time.monotonic()` — these remain real-wall time"). Faking them would make
> the tests **fail**, not run faster. They cost 0.5 s total and stay.
> (4) **xdist workers are separate processes**, so the scout's process-global list (providify
> `current()`/`di_global`, OTel providers, the global attribute registry, `set_profiling_enabled`,
> `install_cache_metrics`, env vars) **cannot race across workers**. What xdist changes *inside*
> a process is **which tests run and in what order** — order-dependence, which is what
> pytest-randomly flushes (§D-randomly). The only resources genuinely shared across workers and
> across pool suites are the **filesystem, TCP ports, and the parent's repo tree** (§D-isolation).
> (5) **The five subprocess snapshot tests do not write into the repo.** `test_api_surface_snapshot`,
> `test_di_catalogue_script`, `test_settings_reference_script`, `test_entry_points_script` run
> `--check` (read-only) or write doctored snapshots to `tmp_path`; `test_import_budget_script`
> monkeypatches `BUDGET_PATH` to `tmp_path` (`:173`) before any `--update`. No `xdist_group` is
> needed for them; Step 9's `git status --porcelain` gate proves it mechanically for every suite.

## Goal

After this plan: **`make test` runs the big suites under pytest-xdist (`-n $(nproc)`), one at a
time with the whole machine, then the remaining suites concurrently in a pool of at most `nproc`
lanes** — never more than `nproc` busy pytest processes; **every unit test is order-independent**
(pytest-randomly is on for unit runs, its seed printed in the summary); **the in-process event-bus
conformance class no longer sleeps 2 s three times** (`EventBusConformance.subscribe_settle_seconds`,
`0.0` in-process, `2.0` default for brokers, unchanged); **`make test PKG=x` stays one command**
with live output; the looptime question is **answered by measurement**; and
`design/test-performance/measurements/unit-test-timing.md` records **before/after wall time for
`make test` and every suite** (local, same machine, median of 3) plus CI before/after per matrix
leg. CI's `unit` job is green on 3.12 and 3.13 and **`all-green` remains the only required check**.

## Non-goals

- **Parallelising integration or chaos tests.** Under xdist every worker would boot its own
  session-scoped container (brief 020 §Distribution: "session-scoped fixtures run once per worker"),
  defeating Plan 012's shared-container model; brief 020 §Evidence gaps: no documented safe pattern
  for async session fixtures under `asyncio_mode="auto"` + xdist. `integration_tests.sh` gains
  only `-p no:randomly` (§D-randomly).
- **Any product-code change** — none is needed; every wait removed is in `testkit/` or `tests/`.
- **Rewriting the wall-clock conformance waits** (cache TTL 0.3 s, job lease 0.2 s) — premise (3).
  Passing `now=` to `reap_expired_leases()` (the ABC has it, `job/base.py:1072-1075`) would test a
  different contract on SA/Redis/Beanie for 0.2 s of gain and risks a conformance finding.
- **Reducing the broker settle** for Kafka/NATS/Redis conformance subclasses — integration-only,
  not on the unit path; the default stays `2.0` (byte-identical integration behaviour).
- **A CI fan-out per package** — rejected by §RL-5-shape (`test.yml:3-15`), not reopened.
- **Coverage (`COVERAGE_CORE=sysmon`)** — the unit job runs no coverage (`test.yml:110-111`).
- **`testpaths` tuning** (brief 019 §C's collection win) — already taken: every package sets
  `testpaths = ["tests"]`.
- **pytest-timeout** — not adopted; a CI `timeout-minutes` bounds a hang (§D-ci).
- **`benchmarks/`** — untouched (never collected by `unit_tests.sh`, `Makefile:304-305`).
- **Editing `BACKLOG.md`** — the roll-up is the coordinator's; parks land in the measurement file
  and §Done.

## Design

### §D-lanes — one primary strategy: xdist inside the big suites, a bounded pool for the rest

```
make test (no PKG)
  │ uv sync --all-packages --all-extras           (once — RL-21, unchanged)
  │ classify every suite (derived, no hand list):
  │   serial-only  ← its pyproject sets asyncio_default_test_loop_scope = "session"
  │                  (varco_ws, examples/00-full-stack-post-api — background uvicorn, brief 020 gap)
  │   xdist        ← W ≥ 2 and non-integration test-file count ≥ XDIST_MIN_FILES (calibrated, Step 6)
  │   pool         ← everything else (incl. serial-only)
  ▼
Phase X — xdist lane: one suite at a time, foreground, LIVE output (tee'd to its log)
  varco_core   -n W --dist <calibrated>     ← owns all W cores
  varco_fastapi -n W ...
  varco_sa ...                              (largest first)
  ▼
Phase P — pool lane: ≤ J suites concurrently, no -n, output to per-suite log files,
          each log printed whole, as one block, the moment its suite finishes;
          started longest-first (file count desc.); heartbeat every 60 s names running suites
  ▼
Summary: per suite  ✔/○/✘  lane  counts (display-only)  wall s ; total wall; seed; W; J
Exit 1 iff any suite exit ∉ {0, 5}          (exit-code-based, exactly as today)
```

`W = J = nproc` by default (`nproc` honours CPU affinity/cgroups). Invariant: **at no time are
more than `max(W, J)` pytest worker processes busy** — the two phases never overlap. `W ≤ 1`
means "no xdist" (one worker only adds overhead), so a 1-vCPU box degrades to today's behaviour.

**Why this split** (all from the 2026-10-07 baseline + file counts, Step 1 re-measures):
- The critical path is `varco_core` — 283 of 677 test files, **56.5 s wall / 31 s user CPU**.
  Suite-level parallelism alone can never finish faster than `varco_core`'s serial wall
  (≈ 50 s after §D-settle). xdist attacks exactly that suite: both its CPU (31 s / W) and its idle
  waits run in parallel. Brief 019 §C: xdist `-n auto` took PyPI's suite 191 s → 63 s.
- xdist is also the only lever that speeds up **`make test PKG=varco_core`** — the inner loop of
  the other ten 4.0 rows. A suite pool does nothing for one suite.
- The small suites (kafka 22, nats 21, casbin 13, memcached 10, ws 9, example 6 files) are
  dominated by interpreter + `uv run` + collection start-up; xdist would *add* a start-up per
  worker (brief 020: "~250 ms per test file … single-file runs may not benefit"; every xdist worker
  re-collects the whole suite). Running them **concurrently** hides that start-up instead.
- Two non-overlapping phases avoid oversubscription — the source of timing flakes CLAUDE.md's
  rule would otherwise convert into margin edits one by one.

**Knobs** (env, read by the script only; `VARCO_UNIT_*` deliberately **not** `VARCO_TEST_*`, which
is the `VARCO_TEST_<SERVICE>_URL` override namespace `integration_tests.sh:81-88` scans):

| Variable | Default | Meaning |
|---|---|---|
| `VARCO_UNIT_WORKERS` | `nproc` | `-n` for the xdist lane; `0`/`1` = no xdist anywhere |
| `VARCO_UNIT_JOBS` | `nproc` | pool width; `1` = today's sequential order |
| `VARCO_UNIT_XDIST_MIN_FILES` | Step 6's calibrated value (tentative 40) | xdist-lane threshold |
| `VARCO_UNIT_DIST` | Step 6's winner (tentative `worksteal`) | `--dist` mode |
| `VARCO_UNIT_SEED` | random, printed | `--randomly-seed` for every suite |
| `VARCO_UNIT_LOG_DIR` | `mktemp -d` (printed) | per-suite logs; never inside the repo |
| `VARCO_UNIT_SYNC` | `1` | `0` skips the up-front `uv sync` (also used by Step 7's test) |
| `VARCO_UNIT_PYTEST` | `uv run --no-sync pytest` | command prefix (Step 7's fake-pytest test) |

`VARCO_UNIT_WORKERS=0 VARCO_UNIT_JOBS=1` reproduces today's fully serial run — the escape hatch
for `-s`, `--pdb`, and bisecting (xdist does not relay worker stdout and has no `--pdb`).
`PYTEST_EXTRA_ARGS` keeps its meaning; a later `-n0` in it overrides the script's `-n` (argparse:
last wins). `make test PKG=x` → one suite, foreground, live, xdist iff classified so.

**File-count rule, not a hand list.** `find <suite>/tests -name 'test_*.py' ! -name '*integration*'
! -name '*chaos*' | wc -l` — derived at run time, so a new package can never be *dropped*
(RL-18's failure mode); at worst it lands in the pool lane, which is always correct. Integration
files are excluded from the count because they are deselected in a unit run.

### §D-randomly — pytest-randomly on for unit runs, off for integration/chaos

pytest-randomly (brief 020 §pytest-randomly: shuffles module → class → function, reseeds `random`
per test, "best added from the start") is installed via the new root `test` group and is therefore
active in every pytest invocation. Decision: **it stays enabled for unit runs; `integration_tests.sh`
passes `-p no:randomly`.**
- ✅ xdist's `worksteal`/`load` already make per-worker order vary run to run; randomizing it
  *deliberately, with a printed seed*, turns an unreproducible flake into `VARCO_UNIT_SEED=N`.
- ✅ The seed is generated **once per `make test`** and passed to every suite, printed in the
  summary; xdist requires every worker to collect the identical test order, so a seed that did
  not propagate to workers fails loudly ("Different tests were collected between gw0 and gw1") —
  self-verifying.
- ✅ Disabled for integration/chaos: chaos modules sequence container restarts within a module
  (`*_container_chaos` is module-scoped), integration reproduction needs Docker, and a reseeded
  `random` could make two tests' random names collide on a **shared** session container — the
  namespacing rule assumes `uuid4` (unaffected: `os.urandom`), but a test using `random` would not
  be. Order auditing of integration tests is out of scope.
- ❌ A latent order-dependence can turn one CI leg red on an unrelated PR. Mitigated by Step 3
  (flush them first, serially, three seeds per suite) and the printed seed.
- ❌ One more dev dependency. Exact `==` pin, same reasoning as `lint` (`pyproject.toml:25-28`).

### §D-settle — delete the needless wait, do not fake it

`EventBusConformance` gains `subscribe_settle_seconds: ClassVar[float] = 2.0` (same pattern as the
existing `supports_lifecycle` ClassVar, `event_bus.py:75-77`) and a `_settle()` helper that returns
immediately when it is `0`. The three `asyncio.sleep(_SUBSCRIBE_SETTLE)` calls become
`await self._settle()`; the module constant is removed; the false "Free for backends …" comment is
corrected. `TestInMemoryEventBusConformance` sets `0.0`. Broker subclasses
(`varco_kafka`/`varco_nats`/`varco_redis` ×2 conformance files) are **not edited** — they inherit
`2.0`, integration behaviour byte-identical. No shared assertion is weakened: delivery is still
proven by `_wait_until` polling (`:45-52`), which exits on the first check for a synchronous bus.

### §D-looptime — adopt only where measurement shows value (expected: park)

looptime 0.7 fakes **loop time only** — `asyncio.sleep`, `wait_for`, `call_later` — not
`time.time()`/`time.monotonic()`, not threads, not real I/O, and it injects `IdleTimeoutError`
after 1 s of real inactivity (brief 020 §looptime). Source reading of every unit-path
`asyncio.sleep(≥0.1)` in the 2026-10-07 tree: the dominant sink (the 6 s settle) is deleted by
§D-settle; the remaining sub-second sleeps are mostly wall-clock-coupled (TTL/SWR/lease/watch-poll/
TLS reload) or cancelled before they elapse (e.g. `test_cache_singleflight.py:123` `sleep(1.0)` is
cancelled at `:131`). **Gate (Step 10):** adopt looptime iff tests whose wait is *loop-time-only*
add **≥ 3 s, or ≥ 10 %**, to any suite's post-xdist wall. If adopted: `looptime==0.7` in the
`test` group and `@pytest.mark.looptime` on the named tests only — **never `--looptime` globally**.
If not: parked in the measurement file + §Done with that trigger.

### §D-isolation — what xdist and the pool actually share

Cross-process-shared resources only: **(a) TCP ports** — every in-tree server binds port 0
(`varco_ws/tests/conftest.py:60`, `examples/.../conftest.py:125`, `test_issuer_source_ssl.py:91`,
`test_tls_pkcs12.py:253`, `test_tls_clients_integration.py:43`); the 76 fixed-port literals in
unit-test files are URL strings in settings/connection tests, never `bind()`ed (Step 6 re-greps).
**(b) Filesystem** — `tmp_path`/`tmp_path_factory` get a per-worker basetemp from xdist;
`tls_fixtures.pki_bundle` (session) is therefore generated once **per worker** (correct, small CPU
cost); `tempfile.mkdtemp()` (casbin) is unique per call. **(c) The repo tree** — premise (5);
gated by Step 9. Process-global Python state is per worker (premise (4)) and is an *order* concern
handled by §D-randomly; the audit set for Step 3 is the 16 files that touch it
(`rg -l 'set_tracer_provider|set_meter_provider|set_profiling_enabled|install_cache_metrics|register_global_attribute|os\.environ\[.*\]\s*=|DIContainer\.current|di_global'`
over unit-test dirs: `test_observability.py`, `test_cache_layered_metrics.py`,
`test_observability_di.py`, `test_providify_pytest_plugin.py`, `test_no_service_locator.py`,
`test_cache_metrics.py`, `test_profiling.py`, `test_retention_di.py`, `test_entry_points_script.py`,
`test_settings_no_loose_env_reads.py`, `varco_memcached/tests/test_di.py`, the example `conftest.py`,
`varco_fastapi/tests/{test_profiling_middleware,test_composite,test_metrics_span_context}.py`,
`varco_redis/tests/test_redis_di_wiring.py`).

### §D-script — `scripts/unit_tests.sh` shape

- Keeps: the `packages.sh` derivation (`:53`), `EXTRA_SUITES` (`:59`), validation (`:79-87`),
  one up-front sync (`:89-107`, now skippable via `VARCO_UNIT_SYNC=0`), `-m "not integration"`,
  `-v`, `PYTEST_EXTRA_ARGS`, the exit-5 rule, exit codes.
- Adds: `--plan` (print `suite=<label> lane=<xdist|pool> workers=<n> files=<n>` per suite +
  `seed=` `workers=` `jobs=` and exit 0 **before** syncing); the two-phase scheduler; a polling
  pool (`kill -0` + `wait "$pid" || status=$?` — `set -e`-safe, no `wait -n` needed); a
  `trap` on `INT TERM` that kills every child (Ctrl-C must not orphan pytest workers) and exits 130;
  a heartbeat; per-suite wall time and a display-only counts parse of each log's final pytest
  summary line (`?` if absent); the seed line.
- ⛔ The literal substring `-m integration` must not appear anywhere in the file, comments
  included — `test_repo_ci_invariants.py:188` asserts it.

### §D-ci — CI stays one job shape

`.github/workflows/test.yml`'s `unit` job keeps `bash scripts/unit_tests.sh` verbatim, its matrix,
and `all-green`'s `needs: [lint, unit]`. Only addition: `timeout-minutes: 20` on `unit` — buffered
pool logs make a hang invisible, and the default job timeout is 6 h. No new job, no new required
check, no `services:`. On a public repo `ubuntu-latest` has 4 vCPU (brief 020 §GitHub Actions) →
`W = J = 4`; on 2 vCPU the derivation adapts. `-n $(nproc)` rather than `-n auto`: brief 020
§Distribution — `auto` counts **physical** cores (psutil is installed, a `varco_core` dependency),
which on a hyper-threaded 4-vCPU runner may be 2.

### Alternatives considered

- **Suites in parallel only, no xdist**: ✅ zero isolation work ❌ floor = `varco_core` serial
  (≈ 50 s) ❌ no gain for `make test PKG=x`, the dev loop this row exists for.
- **xdist via each package's `addopts`**: ✅ IDE and ad-hoc runs get it too ❌ `-m integration`
  runs would spawn W× session containers (Plan 012's shared model) ❌ a single-test run pays W
  worker start-ups ❌ `-s`/`--pdb` broken by default.
- **xdist in every suite AND all suites concurrently**: ✅ simplest script ❌ up to 11 × W
  processes on 4 vCPU → timing flakes + memory ❌ no measured upside over two phases.
- **One pytest invocation over all suites**: ❌ eleven rootdirs with different `pythonpath`,
  conftests and loop scopes — impossible without restructuring every package.
- **looptime globally (`--looptime`)**: ❌ breaks wall-clock tests (premise (3)) and real-I/O tests
  (`IdleTimeoutError`), and fixture setup/teardown is not compacted (brief 020 §looptime).
  **time-machine**: ❌ does not fake `loop.time()`; brief 019 §C: competes with looptime.
- **looptime for the 2 s settle**: ❌ a fake clock for a wait that should not exist — delete it.
- **Per-suite `--junitxml`** (brief 020's pattern): ❌ no consumer — status is exit-code-based and
  counts are display-only; log files alone solve interleaving.
- **pytest-randomly as a one-shot audit, then uninstalled**: ✅ deterministic CI ❌ xdist already
  varies order, so order bugs would still surface — just without a seed to reproduce them.
- **`--dist loadscope` fixed up front**: ❌ unmeasured; module-scoped fixtures
  (`test_jwt_authority.py:29`, `test_entry_points_script.py:38`, `test_registry_revocation.py:223`)
  favour it, uneven test durations favour `worksteal` (brief 020: 53.85 s → 28.71 s loadfile →
  worksteal). Step 6 measures both.

## Steps

All commands from `/home/edoardo/projects/varco`. Strictly sequential: every later measurement
assumes the earlier fixes. `M` = `design/test-performance/measurements/unit-test-timing.md`.

1. [x] **Baseline — measure all eleven suites before touching anything** (only `varco_core` was
       measured). Create `M` with a "Machine" section (`nproc`; `lscpu | rg 'Model name|Thread|Core'`;
       `uv run python -c "import os,psutil;print(os.cpu_count(),psutil.cpu_count(logical=False))"`;
       Python version; "WSL2" note) and a "Baseline" section. For each suite run the script's exact
       command shape with `TIMEFORMAT='%R %U %S'; time (cd <dir> && uv run --no-sync pytest tests/
       -m "not integration" -q --durations=30)` (the example from root, as `:130-135`), **3 runs,
       record median** wall/user/sys + passed/skipped/deselected. Then `time make test` × 3, median.
       From the durations, list every call ≥ 0.5 s across suites and classify each by reading its
       source: `loop-sleep` (pure `asyncio.sleep`/`wait_for`), `wall-clock wait`, `subprocess`,
       `real I/O/thread`, `CPU`. Record `git status --porcelain` before/after a run.
       **Verify**: `M` has 11 suite rows × 3 runs + medians, a `make test` median, the
       classification table.
2. [x] `pyproject.toml` (root) — new PEP 735 group `test = ["pytest-xdist==3.8.0",
       "pytest-randomly==<version uv resolves>"]` with a comment block citing brief 020 (xdist 3.8.0
       = pytest-9 support; exact pins for the `lint` reason), and `{ include-group = "test" }` in
       `dev`. `uv lock && uv sync --all-packages --all-extras`. `scripts/integration_tests.sh:212,218`
       — add `-p no:randomly` to both invocations with a DESIGN comment (§D-randomly ✅/❌).
       **Verify**: `uv run python -c "import xdist, pytest_randomly"`; `uv lock --locked`;
       `rg -c 'no:randomly' scripts/integration_tests.sh` → 2; record the resolved randomly version
       in `M` (brief 020 cites 3.14.0 — ⚠️ ASSUMPTION it is still the current line).
3. [x] **Order-dependence audit, serial.** For every suite: `uv run --no-sync pytest tests/
       -m "not integration" -q -p randomly --randomly-seed={1,2,3}` (no `-n`). Every failure is fixed
       **in the test or its conftest** — a `monkeypatch`, a reset fixture, an autouse reset beside
       `_reset_jwt_globals` (`varco_core/tests/conftest.py:25`) — never `-p no:randomly`, never
       an ordering marker. Start from §D-isolation's 16-file audit set. Each fix → a row in `M`
       ("Order fixes": test id, leaked state, fix). **Verify**: three seeds green per suite.
       ⚠️ If > 10 independent fixes are needed, stop and report (Open question 3).
4. [x] `varco_core/tests/test_conformance_inmemory.py` — **failing first**:
       `test_in_process_bus_needs_no_settle` asserting
       `EventBusConformance.subscribe_settle_seconds == 2.0` and
       `TestInMemoryEventBusConformance.subscribe_settle_seconds == 0.0` (a structural guard so the
       wait cannot silently return; no timing assertion). **Verify**: fails with `AttributeError`.
5. [x] `testkit/varco_conformance/event_bus.py` — §D-settle: ClassVar + `_settle()`; replace
       `:100,:122,:177`; drop `_SUBSCRIBE_SETTLE`; correct `:38-41`'s comment; document the ClassVar
       in the class docstring (`Edge cases:` in-process = 0). `test_conformance_inmemory.py` —
       `subscribe_settle_seconds = 0.0` on `TestInMemoryEventBusConformance` with a one-line why.
       **Verify**: Step 4's test passes; `uv run pytest varco_core/tests/test_conformance_inmemory.py
       --durations=5` shows the three bus tests < 0.1 s; `rg -n '_SUBSCRIBE_SETTLE' testkit varco_*/tests`
       → 0; `git diff --stat` touches no `varco_{kafka,nats,redis}/tests/` file.
6. [x] **xdist audit + calibration** (records "Calibration" in `M`). (a) Every non-serial-only
       suite under `-n $(nproc) --dist worksteal -p randomly --randomly-seed={1,2,3}`; fix failures
       per §D-isolation (port 0, `tmp_path`, no repo writes) or as Step 3; re-grep fixed ports:
       `rg -n '\.bind\(\(' varco_*/tests examples/00-full-stack-post-api/example/tests` → port 0 only.
       `varco_ws` and the example are **not** run under `-n` (brief 020 evidence gap). (b) Matrix,
       3 runs, median: `varco_core` and `varco_fastapi` × `--dist {worksteal, loadscope, load}` at
       `W=nproc`; `varco_core` at `W=nproc/2`; every suite serial vs `-n nproc`. (c) Decide:
       `VARCO_UNIT_DIST` = fastest for `varco_core` (tie → `worksteal`); `XDIST_MIN_FILES` = smallest
       non-integration file count among suites where xdist wall ≤ 0.8 × serial wall, rounded down
       to a multiple of 5 (if the cut is non-monotone, the cut minimising summed wall).
       **Verify**: `M` has the matrix, both decisions, and the fixes list.
7. [x] `varco_core/tests/test_unit_tests_script.py` (**new, failing first**; precedent
       `test_bump_script.py`) — drives `bash scripts/unit_tests.sh` as a subprocess:
       (a) `--plan`: every name from `scripts/packages.sh` plus the example appears exactly once
       (RL-18); (b) `varco_ws` and the example are `lane=pool` even with
       `VARCO_UNIT_XDIST_MIN_FILES=1` (the loop-scope rule, not size, decides); (c)
       `VARCO_UNIT_WORKERS=0` and `=1` → no `lane=xdist`; (d) `--plan varco_redis` → only it;
       (e) pool order is file-count-descending; (f) `--plan bogus_pkg` → exit 1 (validation kept);
       (g) `VARCO_UNIT_SEED=42` → `seed=42`; (h) with `VARCO_UNIT_SYNC=0` and `VARCO_UNIT_PYTEST`
       pointing at a fake pytest in `tmp_path` (sleeps 0.2 s, appends `start/end <label> <ns>` to a
       file, exits 1 for `varco_casbin`, 5 for `varco_ws`, else 0, prints a pytest-style summary):
       exit 1; summary lists `varco_casbin` failed, `varco_ws` no-tests, all others passed, each
       once; `varco_casbin`'s log block is printed; max concurrent pool suites ≤ `VARCO_UNIT_JOBS=2`
       computed from the start/end file (the oversubscription invariant, mechanically).
       **Verify**: fails (no `--plan`, no knobs).
8. [x] `scripts/unit_tests.sh` — implement §D-lanes/§D-script with the Step 6 defaults; rewrite
       the header (usage, knobs table, the `-s`/`--pdb` → `VARCO_UNIT_WORKERS=0` note, a DESIGN
       block with ✅/❌ for two phases vs. alternatives). `Makefile:13-16,326-327` comments and the
       `help` line for `test` — describe lanes in one line. **Verify**: Step 7 green;
       `bash -n scripts/unit_tests.sh`; `rg -c -- '-m integration' scripts/unit_tests.sh` → 0;
       `uv run pytest varco_core/tests/test_repo_ci_invariants.py` green; a hand check — drop a
       `def test_x(): assert False` into `varco_casbin/tests/test_zz_tmp.py`, `make test` exits 1 and
       names `varco_casbin`, delete it.
9. [x] **Flake + isolation gate.** 5 consecutive `make test` runs green; 1 oversubscribed probe
       `VARCO_UNIT_WORKERS=$((2*$(nproc))) VARCO_UNIT_JOBS=$((2*$(nproc))) make test` green; after
       each, `git status --porcelain` identical to before. A timing failure → **increase its margin**
       (CLAUDE.md Test Conventions), comment citing Plan 059, never xfail, never a per-test `-n0`;
       an isolation failure → fix as Step 3/6. Every edit → `M` ("Margin/isolation fixes").
       **Verify**: `M` lists 6 green runs with their seeds.
10. [x] **looptime gate** (§D-looptime). Re-run `--durations=30` per suite in its lane; sum the
        `loop-sleep` tests (Step 1's classification) on each suite's post-xdist wall. Gate met →
        add `looptime==0.7` to the `test` group, mark exactly those tests `@pytest.mark.looptime`,
        re-measure, keep only if the suite gets faster. Gate not met → record "parked" + trigger in
        `M`. **Verify**: `M` has the sums and the decision; if adopted, `rg -n 'looptime' pyproject.toml`
        → 1 and `rg -n -- '--looptime' .` → 0.
11. [x] **CI + docs.** `.github/workflows/test.yml` — `timeout-minutes: 20` on `unit` + a comment
        (§D-ci); nothing else. `CLAUDE.md` — Commands: the `make test` sentence gains one clause
        (lanes, `nproc`, seed printed, `VARCO_UNIT_WORKERS=0 VARCO_UNIT_JOBS=1` = serial); Test
        Conventions: one bullet ("unit tests run under xdist + pytest-randomly: order-independent,
        no fixed port, no write outside `tmp_path`; reproduce with the printed seed") and one
        clause on `subscribe_settle_seconds` in the conformance paragraph; one workflow line
        ("`-s`/`--pdb` need `VARCO_UNIT_WORKERS=0`"). `CONTRIBUTING.md:19-32` — the knob table.
        `technical_docs/common-pitfalls.md` — two rows: *passes alone, fails in `make test`*
        (leaked global/order → seed + `VARCO_UNIT_WORKERS=0`, fix with a reset fixture) and *fixed
        port / fixed path in a unit test* (→ port 0 / `tmp_path`). `CHANGELOG.md` `[Unreleased]` —
        one `### Changed` bullet "(Plan 059)". README unchanged (contributor-facing only).
        **Verify**: `rg -n 'VARCO_UNIT_WORKERS' CLAUDE.md CONTRIBUTING.md` ≥ 2;
        `rg -n 'needs: \[lint, unit\]' .github/workflows/test.yml` → 1; `make lint` green.
12. [x] **After-measurement + done.** Repeat Step 1's procedure (3 runs, median, same machine) on
        the finished tree: per-suite wall in its lane, and `make test`. In the same session, also
        time `VARCO_UNIT_WORKERS=0 VARCO_UNIT_JOBS=1 make test` × 3 as a **same-day serial
        control**, so machine drift between Step 1 and Step 12 is visible rather than credited to
        this plan. CI: the "Run all unit test suites" step duration for both legs from the last
        `main` run before this branch and from this PR's run (Actions UI — `gh` is not installed),
        recorded in `M`. Fill §Done's numbers. **Verify**: §Verification all green.

## Edge cases

- `make test PKG=varco_ws` → one suite, pool-classified, foreground, live output, no `-n`.
- `make test PKG=varco_core` → xdist with W workers, live; `PYTEST_EXTRA_ARGS=-n0` makes it serial.
- `nproc` = 1 (or `VARCO_UNIT_WORKERS=1`) → no xdist anywhere; `J=1` → today's order.
- `PYTEST_EXTRA_ARGS="-x"` → works under xdist (in-flight tests may finish); `-s`/`--pdb` → need
  `VARCO_UNIT_WORKERS=0` (documented, Step 11).
- A suite exits 5 → "no tests", not a failure (unchanged).
- Two pool suites fail → both logs printed whole, in completion order, never interleaved.
- A pool suite hangs → heartbeat names it every 60 s; CI's `timeout-minutes: 20` ends the job.
- Ctrl-C during `make test` → `trap` kills every child; exit 130; no orphaned workers.
- `VARCO_UNIT_SEED=N` → same shuffle; exact replay also needs `VARCO_UNIT_WORKERS=0` (xdist
  distribution is timing-dependent).
- A new workspace package → appears via `packages.sh`; lane derived from its own pyproject and
  file count; Step 7(a) fails if any suite is missing or duplicated.
- An integration test run (`make integration-test*`) → unchanged except `-p no:randomly`.
- `make bench` → pytest-randomly shuffles benchmarks; harmless (no ordering assumption, no
  timing assertion — `benchmarks/README.md` rule).

## Verification

```bash
uv lock --locked
uv run pytest varco_core/tests/test_unit_tests_script.py varco_core/tests/test_conformance_inmemory.py varco_core/tests/test_repo_ci_invariants.py -q
bash -n scripts/unit_tests.sh && bash scripts/unit_tests.sh --plan
make test                                                     # green; summary shows lanes, seed, W, J
for i in 1 2 3 4 5; do make test || break; done               # Step 9 flake gate
VARCO_UNIT_WORKERS=$((2*$(nproc))) VARCO_UNIT_JOBS=$((2*$(nproc))) make test
git status --porcelain                                        # unchanged by any run above
VARCO_UNIT_WORKERS=0 VARCO_UNIT_JOBS=1 make test              # the serial escape hatch still works
make lint && make type-check                                  # docs/scripts touched; mypy unaffected
rg -n '_SUBSCRIBE_SETTLE' testkit varco_*/tests               # → 0
rg -n 'needs: \[lint, unit\]' .github/workflows/test.yml      # → 1 (all-green unchanged)
```
CI: the PR's `Tests` run — `Unit tests (py3.12)` and `Unit tests (py3.13)` green, `All tests
passed` green, branch protection still selects only `Tests / All tests passed`.

## Risks

| Risk | Invariant that must hold | Mitigation |
|---|---|---|
| ⚠️ ASSUMPTION — pytest-xdist 3.8.0 + pytest 9.0.2 + pytest-asyncio 1.3.0 work on *these* suites (brief 020's table is a compatibility claim, not a varco run) | Every suite green under `-n` | Step 6(a) on every non-serial-only suite, three seeds |
| ⚠️ ASSUMPTION — the repo is public → 4-vCPU `ubuntu-latest` (brief 020) | Correctness independent of core count | W/J derive from `nproc`; Step 12 records CI's actual numbers |
| ⚠️ ASSUMPTION — concurrent `uv run --no-sync` processes do not serialise on a uv lock | Pool start-up stays parallel | Step 6/12 timings; fallback: `VARCO_UNIT_PYTEST="$ROOT/.venv/bin/python -m pytest"` as the default |
| ⚠️ ASSUMPTION — pytest-randomly 3.14.0 (brief 020) is still the current line | Exact pin resolves and loads under pytest 9 | Step 2 records the resolved version; xdist's identical-collection check proves seed propagation |
| ⚠️ ASSUMPTION — the order audit finds a bounded number (≤ 10) of fixes | The plan stays S–M | Step 3 stop rule → Open question 3 |
| ⚠️ ASSUMPTION — `varco_ws`/example gain nothing from xdist (9 + 6 files) | They stay correct | Derived serial-only rule; brief 020 evidence gap on async session fixtures |
| **Timing-sensitive tests flake under parallel load** | CLAUDE.md: margin up, never xfail | Step 9's 5 runs + oversubscribed probe; two non-overlapping phases cap load at `nproc` |
| **A test writes into the repo tree or binds a fixed port** | Workers/pool suites share only fs + ports | Premise (5); §D-isolation greps; `git status --porcelain` gate (Step 9) |
| **Order-dependence surfaces on an unrelated PR** | The seed reproduces it | Step 3 flush; seed in every summary; pitfall row |
| **Buffered pool logs hide a hang or a failure** | Every failed suite's log reaches stdout | Print-on-completion; heartbeat; `timeout-minutes: 20`; Step 7(h) asserts the failed log is printed |
| **The new script drops a suite** (RL-18 failure mode) | Every `packages.sh` name + example runs exactly once | Step 7(a); exit-code-based status unchanged |
| **A comment in the new script contains `-m integration`** | `test_repo_ci_invariants.py:188` stays green | Step 8 `rg -c` check |
| **Integration/chaos behaviour drifts** | Nightly signal unchanged | Settle default stays 2.0; `-p no:randomly` in `integration_tests.sh`; no broker test file edited |
| **`all-green` or branch protection changes** | `all-green` is the only required check | Step 11 touches only `timeout-minutes`; Verification greps `needs:` |
| **Memory: W workers each import the full stack** | Fits a 16 GB runner | 4 workers; Step 12 notes any OOM; fallback `VARCO_UNIT_WORKERS` lower in CI env |

## Out of scope / Parked

- **looptime** — parked unless Step 10's gate fires; trigger: a unit test whose wait is
  loop-time-only adds ≥ 3 s (or ≥ 10 %) to a suite's post-xdist wall.
- **Integration/chaos parallelism** — needs an async FileLock pattern for session containers
  (brief 020 §Evidence gaps); separate research brief.
- **Broker-settle reduction** for Kafka/NATS/Redis conformance — integration-only; measure first.

## Done — this plan

- [x] `M` written (not committed — the coordinator commits): Machine; Baseline (11 suites × 3 runs + medians, `make test` median,
      ≥ 0.5 s classification); Order fixes; Calibration (dist, threshold); Margin/isolation fixes;
      looptime decision; After (same procedure + same-day serial control); CI before/after per leg.
      ⏳ CI *after* is pending until the PR's `Tests` run exists.
- [x] No suite is slower after than before (same machine, medians) — with one noise-level
      exception recorded in `M`: `varco_nats` 2.09 s vs 2.03 s (+3 %, inside both run ranges).
- [x] `make test` after ≤ **0.5 ×** baseline locally — **0.39 ×** (53.21 s / 134.83 s; 0.42 × of
      the same-day serial control 128.12 s) — if missed, analysed in `M` and raised with
      the user before closing (Open question 2).
- [x] `varco_core` suite (`make test PKG=varco_core`) after ≤ 0.4 × 56.5 s on a ≥ 4-core box —
      14.26 s = 0.25 ×.
- [x] Six consecutive green full runs (Step 9) with `git status --porcelain` clean.
- [ ] ⏳ (pending the PR run) CI `unit` green on 3.12 and 3.13; `all-green` still the only required check; `needs:` unchanged.
- [x] CLAUDE.md / CONTRIBUTING.md / common-pitfalls.md / CHANGELOG.md updated (Step 11).

## Open questions (for the user, before Step 3)

1. **pytest-randomly permanently on for unit runs** (this plan's default) vs. a one-shot audit
   then removed — confirm.
2. **`make test` ≤ 0.5 × baseline as a closing gate** (expected ≈ 0.3–0.4 ×) — accept, or record
   numbers only?
3. **If Step 3 finds > 10 order-dependence fixes** — fix in-plan (scope grows) or split into a
   follow-up plan and land the rest with those suites temporarily `-p no:randomly`?
4. **Is the GitHub repo public** (4-vCPU runners)? Only CI numbers depend on it.
