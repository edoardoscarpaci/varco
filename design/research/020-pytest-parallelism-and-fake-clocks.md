# Research 020 — pytest parallelism and fake asyncio clocks
Date: 2026-10-07 · Freshness matters: yes — tooling and xdist modes evolve; pytest 9.x async semantics are new.

## Question
For varco's 11-package test suite (pytest 9.0.2, pytest-asyncio 1.3.0, session-scoped containers, 3.12/3.13): Can pytest-xdist parallelize safely with asyncio_mode="auto" and session fixtures? What does looptime fake, and which fake-clock alternatives exist? How should we run concurrent suite invocations from a bash script to avoid interleaved output? Real-world speedup evidence?

## Findings

### pytest-xdist 3.8.0 & pytest 9.0.2 compatibility

- **Latest stable**: pytest-xdist 3.8.0 (adds Pytest 9 support patch) — [python-pytest-xdist Arch Linux](https://archlinux.org/packages/extra/any/python-pytest-xdist/) (2025)
- **Requires**: pytest ≥7.0.0; depends on execnet ≥2.1 — [PyPI pytest-xdist](https://pypi.org/project/pytest-xdist/)
- **Compatibility**: xdist 3.8.0 explicitly patched for pytest 9 changes; pytest-asyncio 1.3.0 released 2025-11-10 adds "Support for pytest 9" — [pytest-asyncio 1.3.0 changelog](https://pytest-asyncio.readthedocs.io/en/v1.3.0/reference/changelog.html)
- **asyncio_mode="auto" with xdist**: Each worker runs its own mini pytest collection. asyncio_mode="auto" automatically marks async functions without requiring `@pytest.mark.asyncio`. **Known caveat**: each worker is a separate process, so session-scoped fixtures run once per worker, not once per session — [paylogic: pytest-xdist and session-scoped fixtures](https://developer.paylogic.com/articles/pytest-xdist-and-session-scoped-fixtures.html)

### Distribution modes & fixture scoping

- **--dist modes**:
  - `load` (default): distribute individual tests as they complete; no grouping
  - `loadfile`: group by file; distribute entire files to workers
  - `loadscope`: group by test class/module scope; guarantees all tests in a scope run in same worker — ideal for expensive module/class fixtures
  - `loadgroup`: group by `@pytest.mark.xdist_group("name")` mark; distribute groups as units
  - `worksteal`: workers steal test groups from others when idle (reduces idle time) — [pytest-xdist 3.8 distribution.rst](https://pytest-xdist.readthedocs.io/en/latest/distribution.html)

- **-n auto vs -n logical**: `-n auto` uses physical CPU cores; `-n logical` uses logical cores (including hyperthreads) — [pytest-xdist 3.8 docs](https://pytest-xdist.readthedocs.io/en/latest/distribution.html)

- **Session-scoped fixture pattern**: each worker runs the fixture once. Coordination via `tmp_path_factory.getbasetemp().parent` + `filelock.FileLock` — first worker to acquire lock produces data, others read from file — [developer.paylogic](https://developer.paylogic.com/articles/pytest-xdist-and-session-scoped-fixtures.html)

- **PYTEST_XDIST_WORKER env var**: set in each worker process (e.g., "gw0", "gw1"); PYTEST_XDIST_WORKER_COUNT is total worker count — [pytest-xdist docs](https://pytest-xdist.readthedocs.io/en/latest/)

- **Worker startup overhead**: ~250ms per test file; total overhead scales with file count and worker count. Single-file test runs may not benefit from -n auto if overhead exceeds speedup — [Medium: 5x faster testing](https://techbytesdispatch.medium.com/unleashing-pytest-power-how-parallel-testing-with-xdist-made-python-tests-5x-faster-035c04e77187)

- **--durations, -p no:cacheprovider, -W error filterwarnings**: No documented xdist-specific interaction issues; all work in worker processes — [pytest 9.0.0 docs](https://docs.pytest.org/en/9.0.x/announce/release-9.0.0.html)

### looptime 0.7 (released 2026-01-03)

- **What it fakes**: Mocks the event loop's foundation — advances loop.time() instantly when event loop requests I/O sleep. Affects asyncio.sleep(), wait_for(), timeouts, and "later" callbacks — all execute in near-zero real time — [looptime PyPI](https://pypi.org/project/looptime/)
- **What it does NOT fake**:
  - `time.time()` and `time.monotonic()` — these remain real-wall time
  - Thread executors — synchronous code in thread pools runs on real-time; "small steps" workaround needed for sync-async sync — [looptime.readthedocs.io/nuances](https://looptime.readthedocs.io/nuances/)
  - Real I/O operations — external network requests, file ops proceed at actual speed; looptime injects `IdleTimeoutError` after 1s of real-time inactivity to prevent hangs
  
- **Marker and CLI**: `@pytest.mark.looptime` enables fast-forwarding for that test; `@pytest.mark.looptime(False)` disables it. `--looptime` CLI flag enables for all unmarked tests as if implicitly marked — [looptime readthedocs configuration](https://looptime.readthedocs.io/configuration/)

- **Known limitations**:
  - Premature finalization: loop fast-forwards before scheduled tasks run; looptime does 42 dummy cycles by default to prevent this
  - Floating-point precision errors; looptime converts to 1-microsecond resolution integers internally
  - Fixture setup/teardown not compacted (pytest-asyncio 1.0.0+ uses multiple event loops per fixture scope) — [looptime nuances](https://looptime.readthedocs.io/nuances/)

### Alternative fake-clock libraries

- **aiotools.VirtualClock** (aiotools ≥1.0.0): Patches event loop selector and time() methods on UNIX only; `with vclock.patch_loop()` makes asyncio.sleep() instant. Works for deterministic timing tests. Less ecosystem adoption than looptime — [aiotools 1.5.9 PyPI](https://pypi.org/project/aiotools/1.5.9/); [Aiotools v1.0.0 discussion](https://discuss.python.org/t/aiotools-v1-0-0-release-with-virtual-clock-for-asyncio/5469)

- **time-machine**: Fakes wall-clock time (time.time(), datetime); does NOT fake loop.time() or asyncio.sleep(). Orthogonal to looptime (one for wall time, one for loop time). No tight asyncio integration — [time-machine PyPI](https://pypi.org/project/time-machine/)

- **asyncio test utilities**: Python 3.11+ offers no built-in fake loop time. The `asyncio` module itself has no loop clock mocking — custom implementations or third-party libraries required.

### pytest-randomly & pytest-repeat

- **pytest-randomly 3.14.0**: Randomly shuffles test order at module → class → function levels; resets global `random.seed()` at start of every test to a fixed number (default `time.time()` from run start, overridable with `--randomly-seed`). Detects hidden inter-test dependencies (module-level state, temp files, DB rows from prior test) — best added from the start when suite is small — [pytest-randomly PyPI 3.14.0](https://pypi.org/project/pytest-randomly/3.14.0/)

- **pytest-repeat**: Runs each test N times (`--count=N`); combined with pytest-randomly finds flaky tests. Research: 2026 paper "JS-TOD: Detecting order-dependent flaky tests in Jest" shows continued academic interest — [JS-TOD 2026](https://mro.massey.ac.nz/items/01263ea5-7616-4e00-9ed6-96e789fa83f9/full)

### Concurrent bash script pytest runs

- **Bash pattern**: Run `pytest ... &` for each suite in background, collect with `wait` (or `wait -n` to process completions as they occur)
- **Per-suite isolation**: Use `--junitxml=<suite>.xml` for each run; `-q` for quiet mode; redirect `stdout`/`stderr` to per-suite log files (e.g., `pytest varco_core/tests/ --junitxml=junitxml/core.xml -q > logs/core.log 2>&1 &`)
- **Exit code handling**: Capture exit codes from background jobs (`wait $pid; code=$?`) and aggregate at end
- **Avoiding output interleave**: Log files + XML reports eliminate terminal output race conditions — [bash parallel patterns](https://download.hermesedi.duckdns.org/.hermes/hermes-agent/scripts/run_tests_parallel.py)

### Real-world speedups with sources

- **5x speedup**: Test suite with 186 tests, high I/O, 8 cores via xdist — nearly 5x improvement — [Medium article](https://techbytesdispatch.medium.com/unleashing-pytest-power-how-parallel-testing-with-xdist-made-python-tests-5x-faster-035c04e77187)
- **8.5x overall**: 17 minutes → 2 minutes by combining xdist with balanced load distribution (loadfile → worksteal) and proper test setup — [exante.eu blog](https://exante.eu/press/blog/2925-how-we-made-python-pytest-suites-8-5x-faster/)
- **47% improvement**: Updating from loadfile to worksteal on same suite: 53.85s → 28.71s — [Medium 5x article](https://techbytesdispatch.medium.com/unleashing-pytest-power-how-parallel-testing-with-xdist-made-python-tests-5x-faster-035c04e77187)
- **Real benchmark pairs** (serial → parallel @ -n 8):
  - Django-allauth: 22s → 8s
  - Pandas: 182s → 61s
  - Aiohttp: 197s → 160s
  (gains depend on test I/O patterns and suite structure) — [How to run tests in parallel with pytest-xdist](https://pydevtools.com/handbook/how-to/how-to-run-tests-in-parallel-with-pytest-xdist/)

### GitHub Actions ubuntu-latest 2026

- **Public repos**: 4 vCPUs, 16 GB RAM, 14 GB SSD (x64)
- **Private repos**: 2 vCPUs, 8 GB RAM, 14 GB SSD (x64)
- **Alternate**: ubuntu-slim (1 vCPU, 5 GB RAM, 15-min timeout) for light jobs — [GitHub blog 1 vCPU runner Jan 2026](https://github.blog/changelog/2026-01-22-1-vcpu-linux-runner-now-generally-available-in-github-actions/); [Supported runners](https://docs.github.com/en/enterprise-cloud@latest/actions/reference/runners/github-hosted-runners)

## Version/compatibility notes

| Tool | Latest | Tested combo | Status |
|---|---|---|---|
| pytest | 9.0.2 (2025-12-06) | pytest 9.0.2 + asyncio 1.3.0 + xdist 3.8.0 | ✅ Working |
| pytest-asyncio | 1.3.0 (2025-11-10) | Adds pytest 9 support; drops Python 3.9 | ✅ Working |
| pytest-xdist | 3.8.0 (2025) | pytest ≥7.0; adds --no-loadscope-reorder | ✅ Working |
| looptime | 0.7 (2026-01-03) | No breaking changes in 0.7 vs 0.6 | ✅ Maintained |
| pytest-randomly | 3.14.0 (2025) | Stable, no asyncio-specific issues | ✅ Maintained |

**EOL / deprecation**: pytest-asyncio 1.3.0 removed Python 3.9 support — [changelog](https://pytest-asyncio.readthedocs.io/en/v1.3.0/reference/changelog.html); pytest 9 removed some deprecated marks but does NOT affect asyncio. varco's 3.12/3.13 matrix stays current.

## Evidence gaps

- **Known issue**: pytest-xdist + asyncio event loop per worker requires manual event loop setup in some pytest-asyncio configurations. **No documented safe pattern yet for session-scoped async fixtures under asyncio_mode="auto" + -n auto** — the FileLock pattern exists for sync fixtures; async version unclear. Worth a separate brief: testcontainers async fixture coordination under xdist.
- **looptime + real I/O**: No published case study of looptime with testcontainers (real broker containers) in parallel tests. No documented guidance on whether looptime should run during integration tests (likely: no). 
- **Overhead measurement**: The 250ms/file figure is anecdotal; no peer-reviewed benchmark for pytest-xdist startup cost on modern hardware (2025+).
- **Alternative fake clocks**: time-machine does not integrate asyncio.sleep; no published syncpoint between time-machine + looptime (if desired together). aiotools.VirtualClock is UNIX-only; Windows compatibility unclear.

## Librarian's note

**Evidence favors**:
- **Immediate win**: xdist 3.8.0 is pytest-9 compatible; use `--dist loadscope` for varco's session-scoped containers (each suite file is one scope, each runs its own fixture once — testcontainers already isolate via run ID). Start with `-n auto` on CI (4 vCPU public runners), measure wall time.
- **Flaky detection first**: Add pytest-randomly to all 11 suites now (no overhead, detects order deps). Use `--count=2` for high-value suites before xdist; 5×–8.5× speedups exist for I/O-heavy tests but require clean fixture isolation.
- **looptime later**: looptime 0.7 does NOT fake wall-clock time; safe to combine with time-machine if needed. Do NOT use during integration tests (real I/O pending). Keep as opt-in marker on unit tests only; document the 42-cycle premature-finalization workaround if observed.
- **Bash script pattern**: Emit per-suite `--junitxml` and log files; aggregate exit codes at end via bash arrays (`exit_codes=(${!}) ; ... [[ ${exit_codes[@]} =~ 1 ]] && exit 1`). No special concurrency primitive needed — `wait -n` is not required for 11 independent suites.
