# Unit-test timing — Plan 059 (TEST-1)

Before/after wall time for `make test` and every unit suite, recorded by
[plans/059-faster-unit-tests.md](../../../plans/059-faster-unit-tests.md). All local numbers are
from **one machine, one session per phase, median of 3 runs**. CI numbers are per matrix leg.

## Machine

| Fact | Value |
|---|---|
| `nproc` | 28 |
| CPU (`lscpu`) | Intel(R) Core(TM) i7-14700HX — 14 cores/socket (as reported to the VM), 2 threads/core |
| `os.cpu_count()` / `psutil.cpu_count(logical=False)` | 28 / 14 |
| RAM | 23 GiB (+ 6 GiB swap) |
| Python | 3.12.13 (uv-managed venv) |
| uv | 0.10.12 |
| Kernel | 6.18.40.1-microsoft-standard-**WSL2** — a VM; wall clock and core counts are those the WSL2 VM reports, and background Windows load is not controlled |

## Baseline (2026-10-11, tree before any Plan 059 change)

Command shape per suite (the script's own shape, `-q --durations=30` added):
`TIMEFORMAT='%R %U %S'; time (cd <dir> && uv run --no-sync pytest tests/ -m "not integration" -q --durations=30)`
(the example from the repo root with `examples/00-full-stack-post-api/example/tests/`). Seconds.

| Suite | run 1 wall | run 2 wall | run 3 wall | **median wall** | median user | median sys | result (run 1) |
|---|---|---|---|---|---|---|---|
| varco_core | 58.65 | 56.94 | 58.00 | **58.00** | 34.13 | 4.13 | 11 failed, 4614 passed, 14 skipped, 10 deselected, 6 xfailed, 5 warnings |
| varco_kafka | 2.52 | 2.52 | 2.89 | **2.52** | 1.97 | 0.16 | 170 passed, 3 skipped, 34 deselected |
| varco_nats | 2.09 | 2.03 | 2.00 | **2.03** | 1.88 | 0.14 | 155 passed, 1 skipped, 32 deselected |
| varco_redis | 5.23 | 4.47 | 4.55 | **4.55** | 4.12 | 0.17 | 395 passed, 7 skipped, 118 deselected |
| varco_beanie | 2.93 | 2.68 | 2.76 | **2.76** | 2.63 | 0.19 | 468 passed, 2 skipped, 98 deselected, 1 warning |
| varco_sa | 8.81 | 10.36 | 10.28 | **10.28** | 7.82 | 1.40 | 771 passed, 1 skipped, 99 deselected, 12 warnings |
| varco_memcached | 1.66 | 1.64 | 1.67 | **1.66** | 1.53 | 0.12 | 104 passed, 23 deselected |
| varco_ws | 1.80 | 1.78 | 1.95 | **1.80** | 1.64 | 0.14 | 81 passed, 5 skipped, 8 deselected |
| varco_fastapi | 53.13 | 52.08 | 52.38 | **52.38** | 48.90 | 1.15 | 1434 passed, 4 skipped, 10 deselected, 13 warnings |
| varco_casbin | 2.65 | 2.85 | 2.84 | **2.84** | 2.65 | 0.16 | 99 passed, 4 skipped, 12 deselected |
| examples/00-full-stack-post-api | 4.05 | 3.86 | 3.67 | **3.86** | 3.70 | 0.20 | 22 passed, 35 deselected, 1 warning |

⚠️ `varco_core`'s 11 failures are the Plan 059 red tests already in the tree
(`test_unit_tests_script.py` ×10, `test_conformance_inmemory.py::test_in_process_bus_needs_no_settle`
×1) — expected red before Steps 5/8; they cost < 1 s together.

**Sum of suite medians: 142.7 s.** `varco_core` + `varco_fastapi` are 77 % of it.

### `make test` (sequential, includes the up-front `uv sync`)

| Run | wall | user | sys |
|---|---|---|---|
| 1 | 144.61 | 114.61 | 7.32 |
| 2 | 134.76 | 105.60 | 7.36 |
| 3 | 134.83 | 105.67 | 6.82 |
| **median** | **134.83** | 105.67 | 7.32 |

`git status --porcelain` before and after the three runs: **identical**.

### Every call/setup/teardown ≥ 0.5 s (run 1's `--durations=30`), classified from source

| Suite | Duration | Phase | Test | Class | Why |
|---|---|---|---|---|---|
| varco_fastapi | 20.19 s | call | `test_di_binding_health.py::test_regression_no_function_local_provider_with_unresolvable_return_type` | CPU | `ast.walk` over every nested function of every `.py` in ten packages; one test → the xdist floor for this suite |
| varco_fastapi | 3.75 s | call | `test_fastapi_di_wiring.py::…::test_every_default_is_overridable_after_bootstrap` | CPU | bootstraps the container once per default binding |
| varco_core | 3.48 s | call | `test_di_catalogue_script.py::test_check_flag_is_deterministic_across_two_successful_runs` | subprocess | two `scripts/di_catalogue.py --check` runs |
| varco_core | 2.00 s ×3 | call | `test_conformance_inmemory.py::TestInMemoryEventBusConformance::{test_publish_subscribe_round_trip, test_multiple_subscribers_on_one_channel, test_event_payload_fidelity_round_trip}` | **loop-sleep** | `asyncio.sleep(_SUBSCRIBE_SETTLE=2.0)` — deleted by §D-settle (Step 5) |
| varco_core | 1.81 s | call | `test_di_catalogue_script.py::test_check_flag_exits_zero_on_a_clean_snapshot` | subprocess | |
| varco_core | 1.74 / 1.46 / 1.44 s | call | `test_api_surface_snapshot.py::TestCheckMode::*` (3 tests) | subprocess | `scripts/api_surface.py --check` |
| varco_core | 1.06 s | setup | `test_bump_script.py::TestSetVersion::test_set_rewrites_all_ten_project_versions` | subprocess | fixture copies the tree + runs the script |
| varco_fastapi | 1.00 s | call | `milestone_c/test_client.py::test_job_handle_stream_progress_fallback_to_polling` | **loop-sleep** | `JobHandle.stream_progress()` polls with `asyncio.sleep(1.0)` (`client/handle.py:259`) |
| examples/00-full-stack-post-api | 0.84 s | call | `test_diagnose_static.py::test_diagnose_static_runs_without_env_or_docker` | subprocess | `varco diagnose` CLI |
| varco_core | 0.77 / 0.63 s | call | `test_webhook_dispatcher.py::{test_exhaustion_lands_in_dlq_never_raises, test_subscription_auto_disabled_after_n_consecutive_failures}` | **loop-sleep** | `RetryPolicy` backoff via `asyncio.sleep` |
| varco_redis | 0.76 s | call | `test_redis_di_wiring.py::…::test_every_default_is_overridable_after_bootstrap` | CPU | |
| varco_core | 0.71 s | call | `test_watch_watchfiles.py::…::test_three_rapid_writes_coalesce_into_one_notification` | real I/O/thread | watchfiles' Rust notify thread + debounce |
| varco_core | 0.64 s | call | `test_settings_no_loose_env_reads.py::…::test_loose_reads_equal_the_frozen_allowlist` | CPU | AST scan of the tree |
| varco_core | 0.55 s | call | `test_cli_diagnose.py::test_a_modules_source_exits_0_and_prints_seams_section` | CPU | in-process `main()`, builds a wiring report |
| varco_fastapi | 0.54 s | call | `milestone_f/test_mcp_adapter.py::test_to_mcp_server_returns_a_low_level_server_and_does_not_raise` | CPU | import of the `mcp` SDK |
| varco_fastapi | 0.52 / 0.51 s | call | `test_layer_boundaries.py::…`, `test_trust_store_deprecation.py::…` | subprocess | fresh-interpreter import checks |
| varco_core | 0.50 s | call | `test_tls_reloading_store.py::test_mid_rotation_unreadable_file_keeps_last_good` | wall-clock wait | stat-poll watcher interval |
| varco_core | 0.50 s | call | `test_watch_poll.py::test_digest_true_detects_same_stat_content_rewrite` | wall-clock wait | stat-poll watcher interval |
| varco_core | 0.50 s ×4 | teardown | `test_issuer_source_ssl.py::*` (4 tests) | real I/O/thread | `HTTPServer.shutdown()`'s 0.5 s `serve_forever` poll interval |

Loop-sleep total on the unit path: **6.0 s** (settle, removed by Step 5) + **2.4 s** remaining
(1.00 + 0.77 + 0.63) — the input to Step 10's looptime gate.

## Dependencies (Step 2)

Root `pyproject.toml` PEP 735 group `test`, included into `dev`:

| Package | Pin | Note |
|---|---|---|
| pytest-xdist | `==3.8.0` | latest on PyPI 2026-10-11; brief 020's pytest-9 release |
| pytest-randomly | `==5.0.0` | ⚠️ brief 020 cited **3.14.0** — that line is gone. 4.0.0 (2025-09) → 4.1.0 (2026-04) → **5.0.0 (2026-09-01)**, latest on PyPI 2026-10-11. 5.0.0 requires pytest ≥ 8 and moves the shuffle into a `pytest_collection_modifyitems` wrapper so the order is reproducible from the seed alone — what this plan relies on |
| execnet | 2.1.2 | transitive (xdist) |

`scripts/integration_tests.sh` passes `-p no:randomly` on both pytest invocations.

## Order fixes (Step 3)

Every suite, serial (no `-n`), `-p randomly --randomly-seed={1,2,3}`. First pass: 9 suites green on
all three seeds; `varco_sa` red on seeds 1 and 3, `varco_core` red on all three (beyond the 11
expected-red Plan 059 tests). **5 independent fixes** (stop rule: > 10) — all in tests, none in
product code, no `-p no:randomly`, no ordering marker:

| Test id | Leaked state | Fix |
|---|---|---|
| `varco_sa/tests/test_framework_metadata.py::test_framework_table_names_matches_expected_literal_set` and `::test_framework_metadata_tables_match_expected_literal_set` | `test_register_framework_metadata_adds_a_new_metadata_source` registers `a_brand_new_framework_table` into the process-global `varco_sa.metadata._FRAMEWORK_METADATA` | that test `monkeypatch.setattr`s the registry to a copy first |
| `varco_core/tests/test_providify_pytest_plugin.py::test_requesting_no_providify_fixture_observes_no_container_related_global_state` | `test_retention_di.py::…::test_never_touches_di_container_current` called a bare `DIContainer.current()`, which lazily **creates** the process-global container and never removes it | the leaker runs inside `with DIContainer.scoped():` (providify's own restore-previous context manager) |
| `varco_core/tests/test_providify_pytest_plugin.py::test_di_container_fixture_is_function_scoped` | asserted a sibling test had already run (definition-order dependence) | the pair is symmetric: each asserts its container is not one already seen, then records it; containers kept (not `id()`s) so an id can never be reused |
| `varco_core/tests/test_tenant_membership.py::TestClaimTenantMembershipMissingClaim::test_missing_claim_with_allow_policy_allows_and_warns_once` | once-per-process flag `varco_core.tenancy.membership._missing_claim_warned` already consumed by an earlier ALLOW-path test (`test_tenant_posture.py`) | `monkeypatch.setattr(mod, "_missing_claim_warned", False)` in the test |
| `varco_core/tests/test_webhook_settings_wiring.py::{test_settings_none_constructs_from_environment, test_get_verifier_with_settings_rejects_a_90s_old_delivery_at_60s_tolerance, test_explicit_tolerance_seconds_kwarg_wins_over_settings}` | `test_webhook_verifier_registry.py::test_register_webhook_verifier_replace_true_swaps_binding` replaced the built-in `"stripe"` in the process-global `_PROVIDERS` | autouse fixture in `test_webhook_verifier_registry.py` monkeypatches `_PROVIDERS` to a per-test copy |

Second pass (`varco_core`, `varco_sa`, seeds 1–3): green except the 11 expected-red Plan 059
tests (Steps 5/8 turn them green).

## Calibration (Step 6, 2026-10-11)

### (a) xdist audit — `-n 28 --dist worksteal -p randomly --randomly-seed={1,2,3}`

Every non-serial-only suite (`varco_ws` and the example are serial-only — their pyproject sets
`asyncio_default_test_loop_scope = "session"` — and were not run under `-n`). All green on all
three seeds after **one** fix (the only failure was order-dependence surfaced by a fresh worker,
not a cross-process race):

| Test id | Leaked state | Fix |
|---|---|---|
| `varco_core/tests/test_cli_diagnose.py::test_check_ignores_info_only_findings` (and the same pattern in `::test_c_check_exits_0_on_a_clean_profile`) | the test emptied the process-global `varco_core.di.select._REGISTERED_VALUES`, but a backend module (re-)imported *during* `main()` registered `dlq`/`idempotency`/`job_store` values (`beanie`, `sa`) into the fresh dict → `di.bootstrapped_unselected` WARNING → `--check` exit 1. Whether that import happens depends on what ran earlier in the process | shared helper `_isolate_backend_selection(monkeypatch)`: empties the registry **and** `monkeypatch.setenv`s every `SEAMS` env var to its own default value, which silences the finding whatever is registered |

Running total of order/isolation fixes: **6** (5 in Step 3 + 1 here) — under the stop rule (> 10).

Fixed-port re-grep `rg -n '\.bind\(\(' varco_*/tests examples/00-full-stack-post-api/example/tests`
→ `test_tls_pkcs12.py:253`, `varco_ws/tests/conftest.py:60`, example `conftest.py:125` — all
port `0`. The other `port=NNNN` literals are settings/URL strings, never bound.

### (b) Matrix — wall seconds, 3 runs, median (`-p randomly --randomly-seed=1`)

`--dist` at `W = nproc = 28`:

| Suite | worksteal | loadscope | load |
|---|---|---|---|
| varco_core | 16.51 / 17.04 / 18.58 → **17.04** | 16.65 / 17.22 / 17.61 → **17.22** | 16.65 / 15.50 / 16.58 → **16.58** |
| varco_fastapi | 28.30 / 30.03 / 28.59 → **28.59** | 29.14 / 27.80 / 27.01 → **27.80** | 29.70 / 27.34 / 27.44 → **27.44** |

Worker count, `varco_core`, `--dist worksteal` (`W = nproc/2 = 14` is the plan's row; the
others were added because the 14 result was faster than 28):

| W | runs | median |
|---|---|---|
| 4 | 17.96 / 18.69 / 16.97 | 17.96 |
| 6 | 15.24 / 15.34 / 15.93 | 15.34 |
| 8 | 13.18 / 14.21 / 13.35 | 13.35 |
| 10 | 12.30 / 13.10 / 12.91 | **12.91** |
| 14 | 14.13 / 14.73 / 14.96 | 14.73 |
| 28 | (above) | 17.04 |
| `varco_fastapi` at 14 | 26.14 / 26.96 / 26.04 | 26.14 |

Serial vs `-n 28 --dist worksteal`, every non-serial-only suite (same session):

| Suite | files | serial median | xdist median | xdist / serial |
|---|---|---|---|---|
| varco_core | 282 | 47.27 | 17.49 | **0.37** |
| varco_fastapi | 129 | 49.33 | 25.47 | **0.52** |
| varco_sa | 75 | 8.62 | 8.36 | 0.97 |
| varco_beanie | 40 | 2.84 | 4.72 | 1.66 |
| varco_redis | 36 | 4.62 | 5.51 | 1.19 |
| varco_nats | 15 | 1.99 | 4.06 | 2.04 |
| varco_kafka | 14 | 2.48 | 3.97 | 1.60 |
| varco_casbin | 11 | 2.46 | 5.53 | 2.25 |
| varco_memcached | 9 | 1.48 | 3.47 | 2.34 |

### (c) Decisions

- **`VARCO_UNIT_DIST = worksteal`.** For `varco_core` the three medians are within 4 %
  (16.58–17.22 s) and the run ranges overlap; read as the plan's *tie → worksteal*. (Strictly
  by median `load` is 0.46 s faster — interpretation recorded, not hidden.) `varco_fastapi` is
  floored at ≈ 20 s by one CPU-bound test (`test_di_binding_health.py`, Baseline table), so its
  `--dist` spread is noise around that floor.
- **`VARCO_UNIT_XDIST_MIN_FILES = 125`.** Only `varco_core` (282) and `varco_fastapi` (129) meet
  xdist ≤ 0.8 × serial; smallest qualifying count 129 → rounded down to a multiple of 5. The cut
  is monotone (every suite ≤ 75 files fails it).
- ⚠️ **Finding, not acted on: `W = nproc` is not the fastest worker count on this box.** This
  WSL2 VM reports 28 logical / 14 physical cores; `varco_core` is fastest at `W = 10` (12.9 s),
  ≈ 25 % faster than `W = 28` (17.0 s) — each worker re-collects all 282 files, and 28 collections
  contend for 14 physical cores. The plan fixes `W = nproc` (§D-ci, chosen for the 4-vCPU CI
  runner where `nproc` and the sweet spot coincide), so the default is unchanged;
  `VARCO_UNIT_WORKERS=10` is the local override. Raised in the plan report as a decision.

## Script hand check (Step 8)

`def test_x(): assert False` in `varco_casbin/tests/test_zz_tmp.py` → `make test` exit 2 (make's
own code; the script exited 1), summary `✘  varco_casbin  FAILED (exit 1)  lane=pool …
[1 failed, 99 passed, 4 skipped, 12 deselected]`, every other suite `✔`; the file was then
deleted. SIGTERM to the script mid-pool (fake 47 s pytest, `VARCO_UNIT_JOBS=4`): exit 130, all
four children gone (each pool suite runs in its own process group and the trap kills the group).

## Flake + isolation gate (Step 9)

`git status --porcelain` captured once before the first run and diffed after every run.

| Run | Command | Exit | Wall (s) | Seed | porcelain unchanged |
|---|---|---|---|---|---|
| 1 | `make test` | 0 | 55.40 | 762661652 | yes |
| 2 | `make test` | 0 | 54.76 | 591552485 | yes |
| 3 | `make test` | 0 | 50.97 | 857016730 | yes |
| 4 | `make test` | 0 | 51.27 | 713625222 | yes |
| 5 | `make test` | 0 | 56.29 | 441383544 | yes |
| probe | `VARCO_UNIT_WORKERS=56 VARCO_UNIT_JOBS=56 make test` (2 × nproc) | 0 | 68.22 | 643239233 | yes |

**Margin/isolation fixes: none needed.** (The hand-check run before these six also stayed
green apart from the deliberate failure, seed 261223168.)

## looptime gate (Step 10)

Re-run in-lane (`varco_core`/`varco_fastapi` at `-n 28 --dist worksteal --randomly-seed=1
--durations=30`; the pool suites had no loop-sleep call ≥ 0.5 s in the Baseline table and run
without `-n`, so their numbers are the Baseline ones):

| Suite | post-change wall | loop-sleep tests (in lane) | sum | share | gate (≥ 3 s or ≥ 10 %) |
|---|---|---|---|---|---|
| varco_core | ≈ 17 s (xdist) | `test_webhook_dispatcher.py::test_subscription_auto_disabled_after_n_consecutive_failures` 0.95 s, `::test_exhaustion_lands_in_dlq_never_raises` 0.69 s | 1.64 s | 9.6 % summed (≤ 0.95 s ≈ 5.6 % on the critical path — they run on different workers) | **not met** |
| varco_fastapi | ≈ 25.7 s (xdist) | `milestone_c/test_client.py::test_job_handle_stream_progress_fallback_to_polling` 1.01 s | 1.01 s | 3.9 % | **not met** |
| every pool suite | Baseline | none ≥ 0.5 s | 0 | 0 % | **not met** |

**Decision: looptime parked.** Not added to the `test` group, no `@pytest.mark.looptime`
anywhere. Trigger to reopen: a unit test whose wait is loop-time-only adds ≥ 3 s, or ≥ 10 % of
its suite's post-xdist wall (on `varco_core`, the summed share is already 9.6 % — the closest
candidate; a third ~0.7 s `RetryPolicy`-backoff test there would cross it). The dominant
in-lane costs are now subprocess/CPU tests (`test_di_catalogue_script.py` 4.97 s,
`test_di_binding_health.py` 19.66 s), which no fake clock can shorten.

## After (Step 12, 2026-10-11, finished tree, same machine)

Per suite, Step 1's command shape in the suite's own lane (`varco_core`/`varco_fastapi` add
`-n 28 --dist worksteal`; pool suites run as before, no `-n`), pytest-randomly on with a fresh
random seed per run, 3 runs, median. Seconds.

| Suite | lane | run 1 | run 2 | run 3 | **median wall** | median user | median sys | baseline median | after / before |
|---|---|---|---|---|---|---|---|---|---|
| varco_core | xdist | 14.26 | 15.86 | 14.26 | **14.26** | 208.92 | 18.36 | 58.00 | **0.25** |
| varco_kafka | pool | 2.33 | 2.22 | 2.25 | **2.25** | 1.71 | 0.13 | 2.52 | 0.89 |
| varco_nats | pool | 2.36 | 2.09 | 1.90 | **2.09** | 1.92 | 0.13 | 2.03 | 1.03 ⚠️ |
| varco_redis | pool | 4.19 | 4.41 | 4.23 | **4.23** | 3.77 | 0.19 | 4.55 | 0.93 |
| varco_beanie | pool | 2.58 | 2.65 | 2.59 | **2.59** | 2.41 | 0.19 | 2.76 | 0.94 |
| varco_sa | pool | 8.53 | 8.02 | 8.15 | **8.15** | 6.21 | 1.05 | 10.28 | 0.79 |
| varco_memcached | pool | 1.49 | 1.59 | 1.42 | **1.49** | 1.41 | 0.08 | 1.66 | 0.90 |
| varco_ws | pool | 1.62 | 1.65 | 1.86 | **1.65** | 1.50 | 0.13 | 1.80 | 0.92 |
| varco_fastapi | xdist | 26.43 | 26.27 | 27.34 | **26.43** | 235.69 | 13.64 | 52.38 | **0.50** |
| varco_casbin | pool | 2.41 | 2.54 | 2.77 | **2.54** | 2.33 | 0.19 | 2.84 | 0.89 |
| examples/00-full-stack-post-api | pool | 4.12 | 3.49 | 3.51 | **3.51** | 3.34 | 0.17 | 3.86 | 0.91 |

Results identical to the baseline counts, now with zero failures (`varco_core`: 4625 passed, 14
skipped, 6 xfailed — the 11 Plan 059 red tests are green).

⚠️ `varco_nats` median is 0.06 s (3 %) above baseline — inside both phases' run-to-run spread
(baseline 2.00–2.09 s, after 1.90–2.36 s); read as noise, not a regression (the suite's code path
is unchanged; pytest-randomly's per-test reseed is the only new work on it).

### `make test`

| Run | wall | user | sys | seed |
|---|---|---|---|---|
| 1 | 53.34 | 488.66 | 34.86 | 616396379 |
| 2 | 52.10 | 484.52 | 35.58 | 37828243 |
| 3 | 53.21 | 508.36 | 37.55 | 80771586 |
| **median** | **53.21** | 488.66 | 35.58 | |

In-lane suite walls inside run 2's summary: `varco_core` 16.0 s, `varco_fastapi` 26.5 s, then the
pool (longest `varco_sa` 9.3 s). Critical path ≈ 16 + 26.5 + 9.3 + sync ≈ 53 s.

### Same-day serial control — `VARCO_UNIT_WORKERS=0 VARCO_UNIT_JOBS=1 make test`

| Run | wall | user | sys | seed |
|---|---|---|---|---|
| 1 | 128.79 | 103.79 | 7.18 | 298565736 |
| 2 | 128.12 | 103.85 | 6.79 | 394497034 |
| 3 | 124.33 | 99.61 | 6.97 | 440711826 |
| **median** | **128.12** | 103.79 | 6.97 | |

### Ratios

| Comparison | Ratio | Gate |
|---|---|---|
| `make test` after / Step 1 baseline (53.21 / 134.83) | **0.39 ×** | ≤ 0.5 × — **met** |
| `make test` after / same-day serial control (53.21 / 128.12) | **0.42 ×** | (drift check: the control is 6.7 s faster than Step 1, of which 6 s is the deleted settle) |
| `varco_core` after / 56.5 s (14.26 / 56.5) | **0.25 ×** | ≤ 0.4 × on a ≥ 4-core box — **met** |

⚠️ **CPU cost went up, not down:** `make test` user CPU 105.7 s → 488.7 s. Each of the 28 xdist
workers imports the stack and re-collects its whole suite. Wall time is what this plan buys; on a
shared or battery-powered machine, `VARCO_UNIT_WORKERS=10` gives a faster *and* cheaper
`varco_core` here (Calibration table).

`git status --porcelain` after all six Step 12 runs: identical to before them.

## CI (Step 12)

`gh` is not installed; numbers read from the public GitHub REST API
(`/repos/edoardoscarpaci/varco/actions/runs/<id>/jobs`), step "Run all unit test suites".

| | Run | py3.12 | py3.13 |
|---|---|---|---|
| **Before** — last `main` run before this branch | `Tests` #34700797817, `6093e05` (3.2.0 plan, #19), 2026-09-12, success | 98 s (job 110 s) | 86 s (job 105 s) |
| **After** — this plan's PR run | ⏳ **pending** — nothing is committed or pushed yet, so no PR run exists | — | — |

Fill the After row from the PR's `Tests` run once it exists.
