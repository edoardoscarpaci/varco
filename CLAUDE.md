# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

**Quick reference**: [ARCHITECTURE.md](ARCHITECTURE.md) — package map, dependency graph, and
type hierarchies (navigate the codebase without reading files one-by-one). [README.md](README.md)
— runnable usage snippets and env-var reference tables for every subsystem.
[technical_docs/features/](technical_docs/features/) — per-feature design rationale and
operator Pitfalls tables.
[technical_docs/common-pitfalls.md](technical_docs/common-pitfalls.md) — the cross-cutting
pitfall catalogue.

---

## What this file is (and is not)

**CLAUDE.md is agent guidance, not documentation.** It exists to tell an agent *how to work
in this repo*: which command to run, which layer a change belongs in, which rule must not be
broken, and where the real documentation lives. It is a routing table and a rulebook.

**It is not the product's documentation, and content must not accumulate here.** Anything a
*human user of varco* would want to read — how a subsystem works, worked usage examples,
env-var reference tables, design rationale, API surface — belongs in README.md,
ARCHITECTURE.md, or `technical_docs/`, and is linked from here rather than restated.

Rules for editing this file:

- **Link, don't inline.** A section here should be short enough to read in one screen and
  should end by pointing at the doc that carries the detail. If a section grows into a
  reference table or a tutorial, that is the signal to move it out and leave a pointer —
  exactly what happened to the Common Pitfalls catalogue.
- **One home per fact.** Never restate in this file something README.md,
  ARCHITECTURE.md, `technical_docs/`, or CHANGELOG.md already says. Duplicated prose drifts,
  and the copy here is the one that silently goes stale.
- **Keep only what changes an agent's behaviour.** A rule ("never `uvx ruff`", "services
  never hold `AbstractEventBus`"), a non-obvious command, a layer boundary, a decision tree.
  If removing a paragraph would not change what an agent *does*, it belongs elsewhere.
- **New feature docs go to `technical_docs/`**, and get at most a one-line pointer here.

---

## Commands

All commands run from the **workspace root** (`/home/edoardo/projects/varco`) using a single shared virtual environment managed by `uv`.

```bash
# Install everything (all workspace members + dev deps, including ruff/mypy — see below)
uv sync --all-packages --all-extras

# One-time, per clone: makes `git blame` skip the mechanical ruff sweep commits (Plan 017 / RL-6)
git config blame.ignoreRevsFile .git-blame-ignore-revs

# Run all tests for one package (every workspace member has its own tests/ dir:
# varco_core, varco_kafka, varco_nats, varco_redis, varco_sa, varco_beanie,
# varco_memcached, varco_ws, varco_fastapi, varco_casbin)
uv run pytest varco_core/tests/
uv run pytest varco_kafka/tests/
uv run pytest varco_redis/tests/
uv run pytest varco_sa/tests/

# Run a single test file
uv run pytest varco_core/tests/test_event.py

# Run a single test by name
uv run pytest varco_core/tests/test_event.py::TestInMemoryEventBus::test_subscribe

# Run integration tests (require Docker — Kafka, NATS, Redis, Memcached, or MongoDB broker)
uv run pytest varco_kafka/tests/ -m integration
uv run pytest varco_redis/tests/ -m integration

# Import any workspace package directly (no install step needed)
uv run python -c "from varco_core.event import AbstractEventBus"
```

The `Makefile` (workspace root) wraps the above plus lint/type-check/build/docs targets across
every package in one call — `make lint` (`ruff check` **+** `ruff format --check` — the
formatter is a CI gate as of Plan 020 / RL-17, adopted at zero `.py` churn), `make format` (`ruff
format` + `ruff check --fix`), `make type-check` (`mypy`), `make test` (now delegates to `scripts/unit_tests.sh`
— runs **all eleven** workspace suites (ten packages + `examples/00-full-stack-post-api`) and
*accumulates* pass/fail/skip into one summary instead of aborting at the first red package; `make
test PKG=varco_redis` narrows to one), `make integration-test` / `make integration-test-clean`,
`make build`, `make publish`, `make docs` / `make docs-serve`. Run `make help` for the full list.
`pytest-asyncio` is installed with `asyncio_mode = "auto"` in every package — all `async def
test_*` functions run automatically without `@pytest.mark.asyncio`.

`make integration-test` / `make integration-test-clean` now **exclude chaos tests by default**
(Plan 018 / RT7) — `scripts/integration_tests.sh` defaults `MARKER_EXPR` to `"integration and not
chaos"`. Chaos tests (`@pytest.mark.chaos`, always paired with `@pytest.mark.integration`)
kill/pause/restart a real container mid-test and are strictly noisier and slower than a plain
broker round-trip. Run them with `make chaos-test` / `make chaos-test PKG=varco_redis` /
`make chaos-test-clean` — the same script with `MARKER_EXPR="integration and chaos"`, and the
same six-`VARCO_TEST_*_URL`-unset clean-room contract `integration-test-clean` uses.

`ruff` and `mypy` are **pinned dev dependencies**, declared in the root `pyproject.toml`'s
`[dependency-groups] lint` group (`ruff==0.16.4`, `mypy==2.3.1`) and pulled in by the `dev` group
via PEP 735 `{ include-group = "lint" }`. `make lint`/`make type-check` invoke them as `uv run
ruff`/`uv run mypy` — resolving the exact pin recorded in `uv.lock`. **Never invoke linting via
`uvx ruff`** — `uvx` resolves whatever the newest ruff release is at the moment you run it, which
can silently diverge from the pin CI enforces (see
[technical_docs/common-pitfalls.md](technical_docs/common-pitfalls.md)).

**mypy strictness ramp — complete** (Plan 020 / RL-14 → Plan 021, RL-14/RL-14b/RL-14c/RL-14d all
CLOSED; full design: `plans/021-mypy-strict-full-ramp.md`). `[tool.mypy]` is `strict = true`
(mypy 2.3.1's 13-flag bundle — see the `pyproject.toml` comment block for the exact enumeration,
sourced from `mypy --help`'s own `--strict` description, not brief prose) plus
`disallow_any_unimported = true`, landed separately because it is not part of `--strict`. The
`[[tool.mypy.overrides]]` section is empty — `check_untyped_defs` was hoisted from ten per-package
override blocks to one global flag (Plan 021 Phase 1). Two metrics, two different roles, both
still meaningful post-ramp: **M1** (`rg -o 'type: ignore' varco_*/varco_* | wc -l`) is a
directional suppression-debt gauge only, never a gate; **M2** (`uv run mypy <ten dirs>`) is the
actual gate — CI's `lint` job running plain `mypy` (no flags — the ten dirs' own `[tool.mypy]`
already carries `strict`) now **is** M2, trivially.

`disallow_any_expr` is the **one permanent exclusion** — confirmed not part of `--strict` in mypy
2.3.1 (research brief 004 §1), and untargeted by any brief's "Skip these entirely" reversal
evidence. `warn_unreachable` is also not in `--strict` and stays out, unmeasured, out of scope.
Every other flag once filed as "Stopped"/"Decided never"/"Out of scope" (`disallow_any_generics`,
`warn_return_any`, `disallow_untyped_calls`, `disallow_any_unimported`, `disallow_incomplete_defs`,
`disallow_untyped_defs`) was re-opened on a fresh measurement (research briefs 004/005) and landed
— see Plan 021 §D1–§D6 for the full per-flag remediation pattern and U-8 evidence discipline
applied to each re-opened "never" verdict.

### Public API surface snapshot (`scripts/api_surface.py`)

`scripts/api_surface.py` (Plan 022 / Phase 0, §D-AUDIT) records the public API surface — for every
name in each distribution package's top-level `__all__`, its kind (`class`/`function`/`constant`),
defining module, and (functions only) `inspect.signature()`. It derives its package list by
*executing* `scripts/packages.sh`, so it structurally cannot drift the way a hand-written list
would (Plan 020 / RL-18).

```bash
uv run python scripts/api_surface.py          # regenerate the committed snapshot
uv run python scripts/api_surface.py --check  # diff live tree vs. snapshot; exit 1 on a break
```

Outputs `design/api-freeze-and-standards/measurements/api-surface.json` (machine-readable, what
`--check` diffs) and a sibling `.md` (sorted, human-diffable in a PR). `--snapshot PATH` and
`--packages PKG…` narrow the run.

✅ **This is a gate, as of Plan 024 / C5.** `--check` is wired into `make lint`'s no-`PKG` path
(`make lint PKG=<one package>` stays narrow and fast, deliberately skipping it — §D-C5), a
standalone `make api-check`, and CI's `lint` job (`.github/workflows/test.yml`, a step after
`mypy`) — so removing a name from any `__all__`, or narrowing an exported function's signature,
now fails CI. Regenerating and committing the snapshot alongside the change is now a **hard
requirement**, not a courtesy — `--check` will fail the very next `make lint`/CI run otherwise.

⚠️ **Known limitation, deliberate:** class signatures are **not** recorded. They are synthesised
from `__init__`/`__new__` and, for pydantic models and dataclasses, from generated code whose
rendering is not guaranteed identical across the 3.12/3.13 unit-test matrix — a snapshot that
differed by interpreter would make `--check` unrunnable in CI. So `--check` catches **removals and
*function* signature changes only**; a narrowed class `__init__` is invisible to it. (Relatedly,
heap addresses in sentinel default values are stripped, or every run would report a spurious
change — see `_ADDRESS_RE`.) Additions and module moves are reported as notes and never fail.

### AsyncAPI snapshot gate (`varco export-asyncapi --check`)

`make asyncapi` regenerates `design/api-freeze-and-standards/measurements/asyncapi-example.json`
from the example app's live consumers; `make asyncapi-check` diffs it and fails on drift. Wired
into `make lint`'s no-`PKG` path only (the same §D-C5 rule as `api-check`), and into **no new CI
job** — it rides in `test.yml`'s existing `lint` job. Regenerate and commit whenever
`examples/00-full-stack-post-api`'s consumer wiring moves. Details:
`technical_docs/features/asyncapi-export.md`.

### Import-time budget (`scripts/import_budget.py`)

`scripts/import_budget.py` (Plan 028 / P1b) measures each distribution package's
`python -X importtime` cost **above a bare-interpreter baseline measured in the same job**
(best-of-5, fresh subprocesses, self-times summed) and compares it against a hard ceiling
committed in `design/async-performance-patterns/measurements/import-budget.json`. Same
RL-18 package derivation as `api_surface.py`/`bump.py` — it executes `scripts/packages.sh`.

```bash
uv run python scripts/import_budget.py --check --warn-only   # what make lint / CI run today
uv run python scripts/import_budget.py --check               # the same, as a gate
uv run python scripts/import_budget.py --update              # rewrite measured_ms, never ceilings
make import-budget                                           # the warn-only form
```

**Rule: a new top-level `import` in a `varco_*` `__init__.py` needs a budget check, not a hunch.**
`varco_core/__init__.py` is PEP 562 lazy as of 3.1 (289.6 ms → 6.6 ms); re-eagerising even one of
the four measured contributors (`lark`, `jwt`, `psutil`, `opentelemetry.sdk`) undoes it. Run the
script before assuming an import is free.

⚠️ **Warn-only today, a gate tomorrow.** It is wired into `make lint`'s no-`PKG` path (beside
`api-check` — `make lint PKG=<one>` stays narrow, the §D-C5 rule) and into `test.yml`'s `lint` job,
in both cases **with `--warn-only`**: a breach prints loudly and exits 0. The flip to a real gate
is Plan 028's Phase 2 (Steps 13-14) and is deliberately blocked on ≥10 real CI observations
recorded in each entry's `observations` array, because the ~2× ceiling headroom is an assumption
about GitHub-runner variance that no source quantifies. Until those exist, **do not drop
`--warn-only`** — and never "fix" a breach by raising a ceiling silently; a ceiling only moves in a
reviewed diff with the observations as justification.

⚠️ **Contrast with the benchmark harness, which is never a gate** (see below). Import time is a
structural property measured in a fresh subprocess with best-of-N, whose failure mode is "someone
added a top-level import" — reproducible and actionable. A microbenchmark on a shared GitHub
runner is neither. The asymmetry is deliberate; do not "unify" it in either direction.

### Benchmarks (`make bench`, `benchmarks/`, CodSpeed)

`benchmarks/` (Plan 028 / P2) holds seven in-process, Docker-free, deterministic benchmarks over
the paths varco pays per request — query parse, AST build + SA compile, DTO roundtrip,
`AsyncService.create()`, event publish, cache get/set, and a subprocess `import varco_core`. They
are collected by their own `benchmarks/pytest.ini` (`python_files = bench_*.py`), so
`scripts/unit_tests.sh` — which iterates an explicit suite list — never picks them up and the unit
legs never slow down. `pytest-codspeed` lives in a root `bench` dependency group deliberately
**excluded from `dev`**, so a normal `uv sync` never installs it.

```bash
make bench                                   # plain pytest, uninstrumented, no token needed
uv run --group bench pytest benchmarks/ -q   # the same thing
```

⛔ **Rule: `bench` is never a required check and must never appear in `all-green`'s `needs:`.**
`.github/workflows/bench.yml` is a separate workflow, comment-only, and skips (never fails) on a
fork PR with no `CODSPEED_TOKEN`. Adding it to branch protection, or to `test.yml`'s `needs:`,
converts an unquantified-noise signal into a merge blocker. Full rules: `benchmarks/README.md`.

⛔ **Rule: a benchmark must never import a backend that needs a container**, and must never assert
anything about time. Timing is CodSpeed's job; correctness assertions belong in a package's
`tests/`.

### Lockstep version bump (`scripts/bump.py`)

`scripts/bump.py` (Plan 023 / Phase 1, §RL-9-bump) is the **only** mechanism that writes a version
number into a `pyproject.toml` in this workspace. It is a tomlkit-based, style-preserving rewriter
— not `uv version` (which cannot touch sibling requirement strings) and not hatch-vcs (unsuitable
for a hand-chosen, not CI-derived, version). Same package-list derivation discipline as
`scripts/api_surface.py`: it executes `scripts/packages.sh` rather than hand-listing the ten names
(Plan 020 / RL-18).

```bash
uv run python scripts/bump.py --set 3.0.0            # write + `uv lock`
uv run python scripts/bump.py --bump minor           # arithmetic bump relative to current
uv run python scripts/bump.py --set 3.0.0 --dry-run  # print the diff, write nothing
uv run python scripts/bump.py --check                # verify coherence; write nothing
```

⚠️ **Contrast with `scripts/api_surface.py --check`: this one IS a CI gate.** It is deterministic
and hermetic — it parses TOML and imports no `varco_*` module, so none of the three reasons
`api_surface.py --check` is excluded from CI (interpreter-dependent rendering, heap addresses,
import cost) apply here. It is wired as a unit test
(`varco_core/tests/test_bump_script.py::test_workspace_versions_are_coherent`), so it runs in
`make test` and in CI's existing `unit` job with no new CI surface. Sibling requirements are
pinned `~=<major>.0` (compatible release), never `==<exact>` — see `CONTRIBUTING.md`'s versioning
policy for why.

### CI

Two GitHub Actions workflows gate `main` (`.github/workflows/`), plus three release/supply-chain
workflows that never gate a PR (Plan 023 / Phase 5):

- **`test.yml`** — runs on every push/PR to `main`. Three jobs: `lint` (`ruff check .` + `mypy`
  over the ten source dirs, Python 3.12 only — mypy is version-independent given a pinned
  `python_version` in `[tool.mypy]`), `unit` (matrix `[3.12, 3.13]`, `fail-fast: false`, runs
  `scripts/unit_tests.sh`), and `all-green` (`needs: [lint, unit]`, `if: always()`, asserts both
  results are literally `'success'` — a *skipped* leg fails it too). **`all-green` is the only
  required status check** — never select an individual matrix leg in branch protection, or a
  skipped leg leaves the check permanently pending.
- **`integration.yml`** — `push: [main]` + nightly `schedule` + `workflow_dispatch` only (not on
  PRs). Runs `make integration-test-clean` via testcontainers, no `services:` blocks. Not a
  required check — a nightly failure is a BACKLOG/Phase-3 signal, not a merge blocker.
  A second, independent `chaos` job in the same file (Plan 018 / RT7-ci) runs `make
  chaos-test-clean`, gated `if: github.event_name != 'push'` — chaos never runs on the `push:
  main` trigger, only nightly `schedule` and `workflow_dispatch`, so a chaos flake can never
  appear on the one trigger a human is watching land on `main`. It is not in either job's
  `needs:` and must **never** become a required check, on any schedule — unlike `integration`
  (whose eventual promotion is a possibility after a measured flake rate), `chaos` exists to find
  real bugs under deliberate container failure, not to gate merges.
  Since Plan 024 the workflow also carries a `concurrency` group scoped by `github.event_name` as
  well as `github.ref` (`${{ github.workflow }}-${{ github.event_name }}-${{ github.ref }}`,
  `cancel-in-progress: true`) — several merges landing in quick succession leave only the newest
  commit's run running, while the nightly `schedule` and any `workflow_dispatch` run sit in
  separate groups and can never be cancelled by, or cancel, a merge (which would take the `chaos`
  job down with it). It deliberately does **not** use `test.yml`'s simpler `${{ github.workflow
  }}-${{ github.ref }}` group: this workflow has three triggers that all report `github.ref ==
  refs/heads/main`, so a ref-only group would collide across them (`design/research/001-github-actions-concurrency-semantics.md`
  §3). This is also a new, independent, mechanical reason `integration`/`chaos` must never become
  a required check while `cancel-in-progress: true` is set — a cancelled run resolves as neither
  success nor failure, so a required check that can be cancelled can leave a PR permanently stuck
  "waiting for status to be reported" (research 001 §8).

- **`release.yml`** — `push: tags: ["v*"]` + `workflow_dispatch` only. Three jobs: `packages`
  (derives the `{dir, name}` matrix from `scripts/packages.sh`, RL-18 compliance), `build` (one
  `uv build --package <dir>` leg per package, one artifact per package), `publish` (matrix over
  the ten names, `environment: pypi-<name>` + `id-token: write` scoped to that job only, PyPI
  trusted publishing via `pypa/gh-action-pypi-publish` with PEP 740 attestations on by default).
  Top-level `permissions: {}`. Never a required check — it never runs on `pull_request`.
- **`scorecard.yml`** — weekly `schedule` + `push: [main]` + `branch_protection_rule`. Publishes
  an OpenSSF Scorecard result and SARIF upload. One score covers all ten distributions; never a
  required check, and `Branch-Protection`/`Signed-Releases` are expected to score low until Plan
  023's Phase 9 ruleset lands (and PEP 740 attestations do not by themselves satisfy
  `Signed-Releases`).
- **`docs.yml`** — versioned docs via `mike`, see the "Versioned documentation" section below.
  Never a required check.
- **`bench.yml`** — CodSpeed benchmarks (Plan 028 / P2). ⛔ **Currently DISABLED —
  `workflow_dispatch` only**: the `pull_request` + `push: [main]` triggers are commented out in
  the file because no CodSpeed account / `CODSPEED_TOKEN` exists yet, so an automatic run would
  upload nothing. Re-enable by uncommenting them (runbook §3b). Otherwise unchanged:
  `permissions: {}` at top level, a `concurrency` group scoped by `github.event_name` as well as
  `github.ref`, and an `if:` that **skips** a fork PR (no `CODSPEED_TOKEN`) rather than failing it.
  Comment-only. Not in `test.yml`'s `needs:`; ⛔ must never become a required check.
  `make bench` runs the same directory locally, uninstrumented, and is unaffected.

**Branch protection (repository setting, not in the repo tree) — APPLIED.** Plan 023's Phase 9 +
Appendix A ruleset shape is live: branch ruleset `main-branch-protection` (Settings → Branches →
rule for `main` → Require status checks to pass → select **only** `Tests / All tests passed`, the
`all-green` job) plus tag ruleset `release-tags`, both with an admin bypass actor, applied after
the `v3.0.0` tag shipped (Plan 024 / C1, reported complete by the operator). The required check is
still, and will remain, only `Tests / All tests passed` — `release`, `docs`, `scorecard`, and
`chaos` must never be selected. `design/varco-1-0-release/release-runbook.md` remains the
step-by-step reference for re-applying this shape (e.g. onto a future ruleset target), not a
to-do list.

**Manual, out-of-repo operator steps for `release.yml`** (cannot be performed by any file in this
repo — full detail in `design/varco-1-0-release/release-runbook.md`) — DONE. Ten GitHub
Environments (`pypi-varco-core` … `pypi-varco-casbin`, no deployment-branch restriction), ten PyPI
trusted-publisher configs (owner `edoardoscarpaci`, repo `varco`, workflow `release.yml`,
environment `pypi-<name>`), and the GitHub Pages publishing source for `docs.yml` are all
configured (Plan 024 / C1, reported complete by the operator). None of these were scriptable with
`gh` (not installed on the maintainer's machine), so the runbook is the durable record of how they
were done and how to redo them (e.g. for an eleventh package).

---

## Architecture

Varco is a **uv workspace monorepo** of ten packages (`pyproject.toml`'s `[tool.uv.workspace]`,
plus the `examples` workspace member). Each package is independently installable from PyPI.
`varco_core` has no sibling dependencies; every other package depends on it. Package roles,
per-module listings, and the dependency graph live in ARCHITECTURE.md's *Package Overview*.

---

## Key Abstractions and Layer Rules

### Event system (varco_core.event)

Three concentric layers — never skip a layer. Type hierarchy: ARCHITECTURE.md's "Event
System". Usage: README's "Consumer — EventConsumer + @listen".

**Rule**: services must never hold or call `AbstractEventBus` directly. They inject `AbstractEventProducer` and call `_produce()` / `_produce_many()`. The only accepted exceptions are `OutboxRelay` (infrastructure), `EventConsumer.register_to()` (wiring-time only), and `DlqRedriver` (`varco_core.event.redrive`, Plan 009 — publishes a dead letter back onto the bus on operator command; it is infrastructure, not application logic, same reasoning as `OutboxRelay`).

**`@listen` is declarative / `register_to` is imperative.** The decorator stores metadata on the function object at class-definition time. No subscription is created until `consumer.register_to(bus)` is called (typically in a `@PostConstruct` method). This separation makes the consumer bus-agnostic and testable.

**`ChannelManager` implementations must satisfy `declare_channel(c)` ⟹ `channel_exists(c)` is `True` until `delete_channel(c)`** — declared-or-present, not "carries data". Enforced by `testkit/varco_conformance/channel_manager.py`, one of **eight** conformance modules (Plan 019 / RT2-C — see §Test Conventions' conformance paragraph).

### Service layer (varco_core.service)

`AsyncService[D, PK, C, R, U]` — generic type parameters and the `_get_repo` abstract method:
ARCHITECTURE.md's "Service Layer". Usage: README's "Service Layer".

Authorization is enforced at the service layer (not HTTP), via the injected `AbstractAuthorizer`.

**Mixin composition pattern** — `ValidatorServiceMixin`, `TenantAwareService`, `SoftDeleteService`, `EventConsumer` all compose via MRO. Chain hooks (`_scoped_params`, `_check_entity`, `_prepare_for_create`) with `super()` so every mixin in the chain runs.

### DI wiring (providify)

Each backend package ships a `di.py` with a `bootstrap()` helper that runs `container.scan(...)` to discover its `@Singleton` classes. Some packages also expose an opt-in `@Configuration` for resources that need imperative async setup (e.g. `RedisCacheConfiguration`). The DI module is the only place that knows concrete types — application code always injects interfaces (`AbstractEventBus`, `AsyncRepository[D]`, `IUoWProvider`).

```python
# Typical app bootstrap
container = DIContainer()
container.scan("varco_kafka", recursive=True)  # discovers the Kafka bus @Singletons
container.install(SAModule)
bind_repositories(container, User, Post)
```

### DI wiring verb taxonomy

The DI wiring verbs above (`bootstrap`, `bind_*`, `enable_*`, `mount_*`, `install_*`, plus
`async_bootstrap`) look like one family but are six distinct shapes. This table is the index —
it does not restate any individual function's reasoning; follow the example path to that
function's own docstring for the "why".

| Verb | Shape | Meaning | Example |
|---|---|---|---|
| `bootstrap(container=None, ...)` | sync, returns container or `None` | one per package; wraps `container.scan(pkg)`; returns `None` if providify is absent | `varco_kafka.di.bootstrap` |
| `async_bootstrap(...)` | async, returns container | `bootstrap()` + an `await container.ainstall(SomeConfiguration)` step, only where an async connection must open before the singleton is usable | `varco_redis.di.async_bootstrap(setup_cache=True)`, `varco_memcached.di.async_bootstrap` |
| `bind_*(container, ...)` | sync, mutates container | registers N *typed, per-item* generic bindings unknowable before app startup | `varco_sa.di.bind_repositories`, `varco_fastapi.client.bind_clients_from`, `varco_ws.di.bind_websocket_adapter` |
| `enable_*(container)` | sync, mutates container | flips on an opt-in DI **binding** that would shadow an app default if auto-registered | `varco_casbin.di.enable_policy_authorizer`, `varco_core.tenancy.di.enable_tenant_membership` |
| `mount_*(app, ...)` | sync, mutates the ASGI app | flips on an opt-in privileged **HTTP surface**, always behind an explicit acknowledgement kwarg | `varco_fastapi.tenancy.mount_tenant_admin`, `varco_fastapi.admin.mount_reliability_admin` |
| `install_*(...)` | sync, **container-free**; **three shapes** | ⚠️ one verb, three shapes (Plan 022 / AB-3, extended Plan 037 / §D-S12-hook). **(a)** a process-global side effect (OTel instrument registration), taking no argument at all; **(b)** an ASGI-app mutation, taking and modifying an `app`; **(c)** a mutation of a caller-supplied SQLAlchemy object (a `Session`/`sessionmaker`/`async_sessionmaker`), container-free, returning an uninstall callable. Neither (a) nor (b) nor (c) takes a container — all are unrelated to `container.install(SomeConfiguration)` | (a) `install_cache_metrics`, `install_reliability_metrics` · (b) `install_middleware_stack`, `install_cors` · (c) `varco_sa.tenancy.rls_session.install_rls_tenant_hook` |

Name collisions this table exists specifically to call out (audited at Plan 022's RL-8
checkpoint — see `design/api-freeze-and-standards/api-break-candidates.md` for each verdict):
- `install_*` in this taxonomy takes **no container** — `container.install(X)` is providify's
  unrelated `@Configuration`-install verb. **AB-3 verdict: `leave-and-document`** — renaming four
  functions to fix a naming *adjacency* this row already resolves is a poor use of the 3.0.0
  window. What the audit did fix is the row itself, which used to claim `install_*` was uniformly
  "a process-global side effect": that is false of `install_middleware_stack` and `install_cors`,
  which take and mutate an ASGI `app`. Do **not** move that pair into the `mount_*` family —
  `mount_*` is "an opt-in privileged HTTP surface, always behind an explicit acknowledgement
  kwarg", which middleware installation is not.
- ~~`enable_rls_ddl()`~~ → **`render_rls_ddl()`** (`varco_sa/varco_sa/rls.py`). **AB-1 verdict:
  `rename+alias`, landed in 3.0.0.** It was never in the `enable_*` family — it is a pure
  DDL-string generator, touches no container, performs no I/O — and `render_*` now says so. The
  old name remains as a deprecated alias until 4.0.0.

Several `bind_*` factories above register a binding whose interface is only known at call time
(a generic alias like `AsyncRepository[User]`, or a plain class captured in a closure) —
providify needs the *real* return type, not the placeholder `from __future__ import
annotations` leaves on a closure. Framework code doing this uses providify's native
`container.provide(Provider(singleton=...)(factory), returns=...)` / `@Provider(returns=...)`
(providify ≥ 2.0.0, Plan 016 / RL-2) — the `returns=` override is applied at
decoration/registration time, so no `factory.__annotations__["return"] = ...` patching is
needed. (Prior to providify 2.0.0 this went through a since-deleted `varco_core`
compat shim — see UPSTREAM-GAPS.md U-20.)

### Resilience (varco_core.resilience)

Standalone decorators composable with any callable — usage: README's "Resilience" section.
Types: ARCHITECTURE.md's "Resilience".

**`CircuitBreaker` must be a shared instance per external dependency** — a per-call instance will never accumulate enough failures to open. Use `@circuit_breaker(config)` for per-function breakers, or `breaker.protect(fn)` for a shared breaker across multiple functions.

`@retry` is also integrated into `@listen` via `retry_policy=` and `dlq=` parameters. The wrapper is built at `register_to()` time (not decoration time) so the resolved channel string and bound `self` are available.

### Dead Letter Queue (varco_core.event.dlq)

`AbstractDeadLetterQueue` is the interface; `InMemoryDeadLetterQueue`/`KafkaDLQ`/`RedisDLQ`/
`SADeadLetterQueue`/`BeanieDeadLetterQueue` are the implementations. Dead letters must never
be silently deleted (no TTL index by default).

**Contract**: `push()` must never raise — the retry wrapper in `_make_retry_wrapper` cannot recover from DLQ failures, and neither can `OutboxRelay` or `JobRunner` (Plan 005 Phase 3/4). Implementations must log errors and swallow them.

Usage: README's "Dead Letter Queue" section. Full detail (redrive policy, retention, tenancy,
REST admin): `technical_docs/features/dead-letter-queues.md`.

### Idempotency-Key middleware (varco_core.idempotency + varco_fastapi.middleware.idempotency, Plan 029 / D1)

`AbstractIdempotencyStore` (`reserve`/`complete`/`get`/`release`/`delete_expired`) is the seam —
contract + `InMemoryIdempotencyStore` in `varco_core`, the HTTP adapter
(`IdempotencyMiddleware`) in `varco_fastapi`, four backends (`InMemoryIdempotencyStore`,
`RedisIdempotencyStore`, `SAIdempotencyStore`, `BeanieIdempotencyStore`).

**Rule**: `reserve()` is the one atomic primitive every implementation must offer — never add a
set-if-absent method to `AsyncCache` for this (Plan 011 D-11 forbids it); the atomicity
requirement is pushed up into this ABC instead, and each backend uses its own native atomic
primitive (`SET NX PX` / `UNIQUE` + `IntegrityError` / `DuplicateKeyError` / a lazily-created
`asyncio.Lock`).

Usage: README's "Idempotency-Key middleware" section. Full design (fingerprint construction,
header replay allowlist, tenant/subject scoping's fail-closed rule, streaming/over-ceiling
handling, a Pitfalls table): `technical_docs/features/idempotency-key.md`.

### CloudEvents envelope (varco_core.event.cloudevents, Plan 030 / N2)

`CloudEventsJsonSerializer` is a **second** `Serializer[Event]` — CloudEvents v1.0.2 *structured*
mode. `Event` does not change, no bus changes, and nothing happens unless an app opts in with
`bind_cloudevents_serializer(container, CloudEventsSettings(source=...))`.

**Rule**: never put a module-level `@Singleton` or `@Provider` in `varco_core.event.cloudevents` —
providify's scanner auto-registers *both* shapes, and `container.scan("varco_core",
recursive=True)` is a documented, in-use pattern, so a decorator there would silently change the
wire bytes of every app that scans `varco_core`.

**Rule**: `tenantid` comes from `current_tenant()` and is **best-effort** — absent under an
`OutboxRelay`-driven publish. Do not "fix" that by adding a tenant field to `Event`.

**Rule**: structured mode only. `AbstractEventBus.publish()` never gains `headers=` (RS-2), so
CloudEvents *binary* mode is out of scope until a separate `MessageEncoder` Protocol lands.

**Rule**: a `@Provider`-produced bus or DLQ must **declare `serializer` on the provider method** —
providify injects only what the method itself declares, so an undeclared `Serializer[Event]` binding
silently never arrives (wrong wire format, no error). This bit `varco_redis`'s bus selector and all
five DLQ backends; `RedisEventBusSelectorConfiguration.bus()` is the worked example. Guarded by
`varco_redis/tests/test_redis_cloudevents_di.py`. Never "fix" a missing binding by decorating
`cloudevents.py`.

⚠️ `SADeadLetterQueue` and `OutboxRelay` are hand-constructed, not DI-wired — pass `serializer=`
explicitly or they keep the `JsonEventSerializer` default. A DLQ backlog stored before a serializer
swap is never converted; drain it first.

Usage: README's "CloudEvents envelope" section. Full design (attribute mapping, the named Redis
Streams `ce`-field convention, the Kafka `content-type` limitation, the three-phase dual-emit
migration, a Pitfalls table): `technical_docs/features/cloudevents-envelope.md`.

### AsyncAPI export (varco_core.asyncapi + `varco export-asyncapi`, Plan 030 / N3)

`generate_asyncapi(consumers_or_container, *, title, version, ...)` emits an AsyncAPI 3.1.0
document as a plain `dict`; `varco export-asyncapi --check` gates a committed snapshot inside
`make lint`'s no-`PKG` path (beside `api-check` and `import-budget`, skipped by `make lint
PKG=<one>`).

**Rule**: generation is **runtime, from live registered consumers — never a static import walk**.
A `@listen` channel may be `Callable[[Any], str]` resolved at `register_to()` time against a bound
`self`, which a static scan gets silently wrong. An unregistered consumer is deliberately absent.

**Rule**: no new dependency for this — plain `dict` + `json` + `model_json_schema()`. Output is
**JSON only**; `pyyaml` is not a `varco_core` runtime dependency and must not become one.

**Rule**: ⛔ no Node in CI. `npx @asyncapi/cli validate` is run by hand, once, and the result is
recorded in `design/api-freeze-and-standards/measurements/asyncapi-validate.txt`.

Usage + the local `npx` invocation: `technical_docs/features/asyncapi-export.md`.

### Outbound and inbound webhooks (varco_core.webhook[.inbound], Plan 031 / D4, Plan 038 / S19)

`varco_core.webhook` holds everything portable — the `WebhookSubscription`/`WebhookDelivery`
entities, `WebhookSubscriptionRepository` ABC (+ `InMemoryWebhookSubscriptionRepository`), the
`WebhookSigner` ABC (`StandardWebhooksSigner` default, `Rfc9421Signer` opt-in), the SSRF guard
(`ssrf.validate_target()`), and `WebhookDispatcher`. `varco_sa`/`varco_beanie` hold repositories;
`varco_fastapi.webhook.mount_webhook_admin` holds only the admin mount.

**Rule**: `WebhookDispatcher` never holds `AbstractEventBus` — it is an `EventConsumer`, wired via
`register_to()`. It deliberately does NOT use the generic `@listen(retry_policy=..., dlq=...)`
wrapper (that retries/DLQs one handler call as a whole); it runs its own per-subscription retry
loop with the existing `RetryPolicy` instead, because one event can match many subscriptions that
must each retry/DLQ/auto-disable independently.

**Rule**: every delivery target goes through `varco_core.webhook.ssrf.validate_target()` —
resolve-then-validate-then-**pin** to the first resolved address (never a later re-resolution —
that is the DNS-rebinding bypass), `https` only unless `allow_insecure_http` is set at the
deployment level (never per-tenant), and no redirect following.

**Rule**: `WebhookSettings` (env `VARCO_WEBHOOK_`) is the single configuration source —
`WebhookDispatcher` constructs it when `settings=` is omitted and forwards every knob to the call
site that needs it (SSRF knobs → `validate_target()`, `signature_tolerance_seconds` → the signer,
retry/timeout/disable → its own defaults). A new knob goes on `WebhookSettings` and is threaded
through; never add a constructor-keyword-only or `validate_target`-only option, or the env var
becomes a lie. Explicit constructor keywords remain per-instance overrides and always win.

**Rule**: `active_secrets` are encrypted via the existing `FieldEncryptor` when a repository is
constructed with `encryptor=` — no new crypto path. `encryptor=None` is a documented dev/test-only
default; production wiring must pass a real encryptor.

**Rule (Plan 038 / S19)**: inbound webhook verification reuses `StandardWebhooksSigner.verify()`
(via delegation, for the scheme varco already ships) and `AbstractIdempotencyStore` (via
`WebhookReplayGuard`) — never a second HMAC path for Standard Webhooks and never a new
replay-cache ABC. `varco_core.webhook.inbound` (`WebhookVerifier` ABC + four provider adapters +
`get_verifier`) is fully portable; `varco_fastapi.webhook.verify_webhook` is a **route
dependency**, never a middleware (the secret and algorithm are per-route facts) — see
`technical_docs/features/inbound-webhooks.md`.

Full design (signing-scheme choice, the five-layer SSRF model, retry-schedule convention, a
Pitfalls table): `technical_docs/features/outbound-webhooks.md`. Usage: README's "Outbound
webhooks" section.

### Feature flags (varco_core.flags, Plan 032 / D7)

`AbstractFeatureFlags` is a varco-shaped seam (bool/string/numeric/object resolution) — **not** a
transcription of OpenFeature's `AbstractProvider`. `NullFeatureFlags` is the scanned `@Singleton`
default (always returns the caller's own default); `enable_feature_flags(container)` opts in
`InMemoryFeatureFlags`, same `enable_*` verb shape as `varco_casbin.di.enable_policy_authorizer`.

**Rule**: `FlagEvaluationContext.tenant_id` comes from `current_tenant()`, never
`RequestContext` — same tenant single-source-of-truth rule as everywhere else in this file.

**No OpenFeature provider yet — deliberately.** The un-park trigger (`openfeature-sdk`, the SDK
not the spec, reaching 1.0.0) had not fired as of the 2026-09-04 check (SDK 0.10.0, spec 0.9.0).
An `OpenFeatureFlags` adapter is purely additive once it does. Full version evidence and design:
`technical_docs/features/feature-flags.md`. Usage: README's "Feature flags" section.

### HTTP edge hardening (Plan 035 / S3, S7, S8, S10)

Security headers (`varco_fastapi.middleware.security_headers`, on by default), request body limits
(`varco_fastapi.middleware.body_limit`, on by default at 10 MiB), and HTTP rate limiting
(`varco_fastapi.middleware.rate_limit`, one opt-in `create_varco_app(rate_limit=RateLimitBundle(...))`
row) — plus the unmapped-exception `str(exc)` leak fix (S3) and the `inspect_http_edge()` seam Plan
036 consumes. Full design (the normative middleware-ordering table, every header/limit default
argued, a Pitfalls table): `technical_docs/features/http-edge-hardening.md`. Usage: README's
"Security headers"/"Request body limits"/"HTTP rate limiting" sections.

**Rule**: never register `SecurityHeadersMiddleware`/`BodyLimitMiddleware`/`RateLimitMiddleware`
via `create_varco_app(extra_middleware=...)` — verified to land at the wrong stack position
(outside `ErrorMiddleware`, and outside `RequestContextMiddleware`) for all three. Use the
dedicated `security_headers=`/`body_limit=`/`rate_limit=` keywords instead.

**Rule**: never add a `remaining()`/quota-reporting method to `RateLimiter` to emit
`X-RateLimit-*`/`RateLimit` headers — the ABC has no such method by design (adding one breaks
every out-of-tree implementation, the same `BulkCache`-off-`AsyncCache` rule). A limiter that
cannot report remaining quota must never emit a header that lies about it; the parked design is an
optional `RateLimitIntrospection` Protocol.

**S17 outcome (Plan 041)**: `MetricsMiddleware` now executes INSIDE `TracingMiddleware` — see
"Metrics inside tracing" in `http-edge-hardening.md` for the decision and its operator-facing
latency-series step-change note.

### Admin surface tenancy, security posture preflight, authorization-decision audit (Plan 036 / S4, S9, S11)

Cross-tenant write guard on the webhook and reliability admin surfaces (S4), the startup
`SecurityPosture` preflight aggregating every sibling plan's exported inspector (S9), and
`AuditingAuthorizer` recording every authorization denial plus every delegated allow (S11). Full
design + Pitfalls tables: `technical_docs/features/admin-surface-tenancy.md`,
`technical_docs/features/security-posture.md`, `technical_docs/features/authorization-audit.md`.
Usage: README's "Security posture preflight"/"Authorization-decision audit" sections and the
`mount_webhook_admin`/`mount_reliability_admin` snippets.

**Rule**: `mount_webhook_admin` and `mount_reliability_admin` are now tenant-bound
(`cross_tenant_role=`, default `"cross-tenant-admin"`) — `mount_tenant_admin` is deliberately
**not**, because it *is* the tenant control plane (every route addresses a tenant that is by
definition not the caller's own); see `admin-surface-tenancy.md`'s §D-S4-control for the full
argument. Do not add a tenant guard to `mount_tenant_admin` without revisiting that decision.

### Recurring schedules (varco_core.schedule, Plan 032 / D6)

`Schedule` (a cron expression + IANA timezone + `GapPolicy`/`OverlapPolicy`/`CatchUpPolicy`) is
materialized into ordinary `Job` rows by `ScheduleMaterializer` — **no second execution path**;
the existing `AbstractJobRunner` runs the produced jobs unchanged. Cron parsing is a hand-rolled,
zero-dependency 5-field parser (`varco_core.schedule.cron`); RRULE/RFC 5545 remains parked (a
complete implementation needs `dateutil.rrule`, a new runtime dependency).

**Rule**: the materializer does **not** use the job store's fenced-lease primitives
(`try_claim`/`save(expected_epoch=)`) — a synthetic lease row would corrupt every
`list_by_status()`/`all_jobs()` caller downstream. Cross-process double-materialization safety is
a deterministic `uuid5` occurrence `Job.job_id` (idempotent-upsert convergence via
`AbstractJobStore.save()`) plus `UNIQUE(schedule_id)` on the `Schedule` row; in-process
exclusivity is a lazily-created, per-schedule `asyncio.Lock`. Never "fix" this by adding a lease
to `Schedule` to match the job store's model.

Full design (the fenced-lease deviation in detail, a Pitfalls table for DST gaps/catch-up
surprise/materializer downtime): `technical_docs/features/recurring-schedules.md`. Usage:
README's "Recurring schedules" section.

**Note (Plan 039 / S20)**: `Schedule.task_name: str | None = None` — set it and `_build_job`
emits a `TaskPayload`, making a materialized `Job` reachable through `JobRunner.recover()`;
`None` (the default) stays byte-identical to every pre-3.2 `Schedule` row. This is what
`varco_core.retention`'s `RetentionScheduler` rides — see below.

### Retention & purge automation (varco_core.retention, Plan 039 / S20)

A `RetentionPolicy` registry materialized onto the cron→`Job` path above — adapters over five
shipped bulk-delete verbs (`AbstractDeadLetterQueue.delete_where`, `AuditRepository.delete_where`,
`AbstractIdempotencyStore.delete_expired`, `AbstractTokenRevocationStore.delete_expired`,
`AbstractJobStore.delete_where`), a scheduler that materializes+dispatches, an opt-in DI binding,
a posture inspector, and a `varco retention --policy`/`list` CLI. Full design + a Pitfalls table:
`technical_docs/features/retention-and-purge.md`. Usage: README's "Retention & purge automation"
section.

**Rules**:
- Retention is **adapters over shipped verbs, never a new abstract method** — not on
  `AbstractDeadLetterQueue`, `AbstractIdempotencyStore`, `AbstractTokenRevocationStore`,
  `AuditRepository`, or `AbstractJobStore`. Same standing rule that keeps `BulkCache` off
  `AsyncCache` and `remaining()` off `RateLimiter`.
- `RetentionPolicy.dry_run` has **no default and never will** — a policy that does not state its
  own blast radius, in the source, greppable, cannot be constructed at all.
- The scheduler adds **no lease** — `varco_core/varco_core/schedule/materializer.py:24-30`'s
  DESIGN block is why: a synthetic lease row saved through `AbstractJobStore.save()` would be
  indistinguishable from a real job to `JobRetentionTarget`'s own `delete_where()`, i.e. the
  feature would delete its own coordination row.

### SBOM and regulatory posture (scripts/sbom.py, Plan 030 / D5)

`scripts/sbom.py` generates **one CycloneDX 1.6 SBOM per distribution** (never workspace-wide —
that over-reports ~6× and misleads the regulated consumer it exists to serve), attaches them to the
GitHub Release, and embeds them in each wheel at `.dist-info/sboms/` per PEP 770. Release-time
only: the documents and the `sbom-files` pyproject key are **never committed** (`.gitignore`).

**Rule**: never claim CRA compliance or certification anywhere. `docs/regulatory-posture.md` states
a *position* — the non-commercial FOSS exemption, why we believe it applies, and the funding facts
that would void it — and says explicitly that it is not legal advice.

### Observability (varco_core.observability)

`@span`/`@counter`/`@histogram` decorators, `TracingServiceMixin`/`TracingRepositoryMixin`,
`Metric`/`register_gauge`, and `OtelConfig`/`OtelConfiguration` provide OpenTelemetry tracing
and metrics, with automatic parameter capture and a process-wide global-attribute registry on
top (both opt-out). Usage: README's "Observability" section. Full design (decision table, PII
section, Kubernetes Downward-API recipe): `technical_docs/features/observability-attributes.md`.

**Rule — Resource attribute vs. global attribute registry**: static process identity
(`k8s.pod.name`, `deployment.environment`, a Helm release) belongs in
`OtelConfig.extra_resource_attrs` (free — exported once per batch, never multiplies metric
series). The global attribute registry is for values not known at bootstrap or that must be
filterable/`group by`-able as a metric **label** — every key in the registry becomes a label on
every metric series it touches.

⚠️ **`TracingServiceMixin`/`TracingRepositoryMixin` do NOT auto-capture `pk`/`dto`/`params`** —
only `@span`-decorated functions and `create_span(..., params=...)` get automatic parameter
capture; CRUD spans only carry global attributes + `SpanConfig.attributes` + `correlation_id`.

### Ambient request context (varco_core.context, Plan 011 / X1)

`AmbientVar[T]` (`context/ambient.py`) is the generic request-scoped ambient-value primitive
`RequestContext`/`resolve_precedence()` build on. Full design narrative:
`technical_docs/features/i18n-and-localization.md`'s "`varco_core.context`" section.

**Rule: `RequestContext` never holds the tenant.** `current_tenant()` (`service/tenant.py`) stays
the single source of truth for "who is the tenant" — `TenantAwareService`, RLS, `tenancy_cache_key()`,
the DLQ tenant stamp, and the audit trail all read it directly. Composition with the tenant is by
*ordering* (`TenantResolutionMiddleware` runs before `LocalizationMiddleware`), never by
containment — see the pitfall table below.

**Note on `ContextVar` construction**: module-scope `ContextVar()` construction (as `AmbientVar`
does internally, and as `_request_context` does at `context/request.py` module scope) is
**correct**, not an exception to the lazy-`asyncio.Lock` rule — PEP 567 requires a `ContextVar` be
created once, typically at module scope, to behave correctly across `asyncio` tasks.
`ContextVar()` construction has no running-event-loop requirement, unlike `asyncio.Lock()`.

### Internationalization (varco_core.i18n, Plan 011 / I2)

Off by default (`I18nSettings.enabled=False`) — no catalog constructed, no middleware, no `.mo`
read, no `Content-Language` header. `MessageCatalog` (ABC) with three implementations:
`NullMessageCatalog` (DI default, zero I/O), `DictMessageCatalog` (in-memory), `GettextMessageCatalog`
(production default — stdlib `gettext` only, zero new runtime dependency). Full design
(precedence chain, `Accept-Language` negotiation, `TenantDefaultsProvider`):
`technical_docs/features/i18n-and-localization.md`.

**Rule**: `localization_cache_key(base, locale=True)` (`i18n/cache_key.py`) fails closed
(`RuntimeError`) with no ambient locale — locale is never an implicit cache-key component
(RD-6), same rule as `tenancy_cache_key()`.

### Timezones (varco_core.tz, Plan 011 / T1 / T2 / T3)

Off by default (`TimezoneSettings.enabled=False`) — no resolution, `current_timezone()` is `None`,
storage is unaffected. Five-source precedence chain, startup tzdata validation, and RFC 9557
output formatting: `technical_docs/features/timezone-handling.md`.

**Rule**: **varco never changes what it stores** — everything is still written aware-UTC; this
is a rendering/interpretation layer only (`to_user_tz()`, `now_local()`). RFC 9557 (IXDTF) is an
**output-only** format — no parser ships.

### Error taxonomy — `message_key`, `params`, i18n (varco_core.exception, Plan 011 / I1)

Every built-in `ServiceException` carries a `message_key: ClassVar[str | None]` alongside its
existing stable `code`. Full design (D-4 wire delta, `VarcoErrorCodes` alias reasoning, RFC 9457
opt-in, `error_message_for()` wiring): `technical_docs/features/error-taxonomy-and-i18n.md`.

**Rule**: `code` is the machine identifier, `message_key` is the i18n key — a prior docstring
claiming `code` itself was the i18n key was wrong and is corrected.

⚠️ `error_params()` (default `{}`) returns structured interpolation data — treat it as a **new
exfiltration surface**: `ServiceAuthorizationError` deliberately excludes `reason` from its
params, and any override must apply the same scrutiny, never `vars(exc)`. **As of Plan 040 / S21,
two of the three leak shapes this warns about are mechanical, not just advisory**:
`error_message_for()` routes `error_params()` through `varco_core.redaction.redact_mapping()`
(secret-named key -> `"[REDACTED]"`) and `json_safe()` (non-JSON value -> `"<TypeName>"`) by
default (`ErrorEnvelopeSettings.redact_params=True`). **Still advisory** — no mechanism catches
it — a secret *value* under a key that does not match a redaction pattern (redaction in 3.2 is
key-name-based only, never value scanning).

### Redaction (varco_core.redaction, Plan 040 / S21)

One small, key-name-based seam shared by span capture, `error_params()`, the audit trail, and
request logging (`Redactor`, `PolicyRedactor`, `RedactionPolicy`, `redact_mapping()`). Full
design + a Pitfalls table: `technical_docs/features/redaction.md`. Usage: README's "Redaction"
section.

**Rules**:
- Redaction is **key-name-based only, never value scanning** — no card/JWT/`whsec_`-prefix
  detection. A denylist over payload strings is wrong-once-is-a-leak territory; parked with a
  trigger (a real leak key-name matching structurally cannot catch, plus a false-positive budget).
- ⛔ **Never redact on the audit read path** (`list()`/`list_for_entity()`/an admin router) —
  `AuditRepository.verify_chain()` recomputes `entry_hash()` from returned fields; mutating a
  returned entry's `diff` produces a spurious `HashMismatch` on every row. Redact only inside
  `AuditLogMixin._audit_diff()`, before `_produce()`.
- ⛔ **Never add a scanned `@Singleton`/`@Provider`/`@Configuration` to `varco_core.redaction`** —
  the standing `varco_core.tls`/`cloudevents` scan rule; `container.scan("varco_core",
  recursive=True)` is a documented, in-use pattern.
- **A redactor that raises redacts** — any failure anywhere in `redact_mapping()`'s walk degrades
  the whole result to `{k: "[REDACTED]" for k in data}`, never a partial pass-through and never
  the original input.
- Audit redaction is **opt-in** (`AuditLogMixin._audit_redactor`) — the incumbent substring
  matcher has real false positives on domain-named fields (`"pin"` matches `shipping_address`;
  `"auth"` matches `author`), so an on-by-default flip would corrupt audit data.

### Profiling (varco_core.profiling)

Diagnostic CPU + memory profiler, off by default (zero overhead when disabled). Usage
(decorator/context-manager forms, FastAPI middleware, custom-backend registration): README's
"Profiling" section.

**Rules:**
- `cProfile` and `tracemalloc` are **process-global** — one session at a time. The FastAPI
  middleware serialises with a process-wide `asyncio.Lock`; concurrent requests pass through
  unprofiled rather than blocking.
- `cProfile` across an `await` captures all coroutines on the event loop thread. Use it for
  CPU-bound or isolated async work; use a sampling backend (pyinstrument) for busy loops.
- `tracemalloc` prior state is always **restored** on session exit — safe to use in apps
  that already enable it.
- **Never leave profiling always-on in production** — `cProfile`/`tracemalloc` add 20–100%
  overhead. Use the kill-switch (`set_profiling_enabled`) or `VARCO_PROFILING_ENABLED`.

### Cache system (varco_core.cache)

`AsyncCache[K, V]` is a `runtime_checkable` Protocol; `CacheBackend[K, V]` is the ABC backends
subclass. Hierarchy: ARCHITECTURE.md's "Cache System". Usage: README's "Cache System".

**Rule**: never instantiate `InvalidationStrategy` outside its backend's `start()`/`stop()` lifecycle — it may hold subscriptions or background tasks.

Stampede protection (`Singleflight`, per-process only) and bulk operations (`BulkCache`, a
**separate** Protocol from `AsyncCache` — see the pitfall table) are covered in
`technical_docs/features/cache-hardening.md`.

### File watching and hot reload (varco_core.watch / varco_core.reload, Plan 025 / T1, T2)

`AbstractPathWatcher` (`StatPollWatcher` default, `WatchfilesWatcher` opt-in via
`varco-core[watch]`) watches a set of directories and notifies subscribers; `ReloadableResource[T]`
loads a value from a watcher (or any manual trigger) and swaps it under a lock with keep-last-good
semantics. Usage: README's "File watching and hot reload". Type hierarchy: ARCHITECTURE.md's
"File watching".

**Rule**: a watcher's fingerprint is `(st_mtime_ns, st_size, st_ino)` of the **resolved** path,
and enumeration skips `..`-prefixed names — because Kubernetes delivers rotated
Secrets/ConfigMaps as a `..data` symlink swap, and a watcher that stats the symlink itself (or a
hand-rolled mtime-only dict) never sees it change. Never "fix" a watcher to stat the symlink
itself.

### TLS trust store (varco_core.tls, Plan 026 / T3, T5, T7; client injection + mTLS hardening Plan 027 / T4, T6)

`TrustStore` unifies the two pre-3.1 TLS models (`SSLConfig`'s `verify=False` escape hatch +
the old `varco_fastapi.auth.TrustStore`'s `include_system_cas`/`bytes`-CA support) into one
frozen-dataclass superset, reachable from any backend; `ReloadingTrustStore` makes it hot-
reloadable on top of Plan 025's `watch`/`reload` primitives. `varco_core.tls.clients` adds four
zero-hard-dependency adapters (`to_httpx_verify`/`to_aiohttp_connector`/
`to_urllib3_poolmanager`/`to_requests_adapter`) and `varco_core.tls.install` adds the opt-in
`install_process_trust()`; `TrustStore.key_password`/`pkcs12_file` add encrypted-key and
PKCS#12 mTLS support. Usage: README's "TLS trust store" section (including the mTLS/client-
injection/`install_process_trust` subsections). Type hierarchy + module listing:
ARCHITECTURE.md's "TLS trust". Full design (mutate-vs-swap, the additive `SSL_CERT_FILE`/
`SSL_CERT_DIR` divergence, the deprecation-subclass asymmetry, the PKCS#12 temp-file
discipline, a Pitfalls table): `technical_docs/features/tls-trust-and-hot-reload.md`.

**Rule**: TLS trust lives in `varco_core.tls` — `varco_fastapi` may import it (the deprecated
`varco_fastapi.auth.TrustStore` shim does), never the reverse. Same seam rule as
`AbstractEventBus`/`AbstractMigrator`.

**Rule**: never add a scanned `@Configuration` to `varco_core.tls` — `container.scan(
"varco_core", recursive=True)` is a documented, in-use pattern that auto-activates every scanned
`@Configuration`, which would start a filesystem watcher in every app that scans `varco_core`.
`varco_core.tls.bind_trust_store(container, store)` registers an already-constructed,
already-owned store instead — no lifecycle side effect.

**Rule**: never add a hard dependency on httpx/aiohttp/urllib3/requests to any `varco_*`
package's `[project.dependencies]`/extras for the adapters' sake — `varco_core.tls.clients`
imports each of the four inside the function body that needs it, never at module scope, guarded
mechanically by `varco_core/tests/test_tls_no_hard_client_deps.py`.

**Rule**: never call `install_process_trust()` from library code — it is an application-level,
explicit, `acknowledge_global_mutation=True`-gated decision only (§D-T4-install). varco itself
never calls it; `rg -n "install_process_trust" varco_*/varco_*` should only ever hit the
definition and its export.

### Query system (varco_core.query)

The query system builds a typed AST over filter/sort/pagination parameters and applies it to
backends. Pipeline diagram: ARCHITECTURE.md's "Query System". Usage: README's "Query System".

**Rule**: all AST nodes are `@dataclass(frozen=True)` — immutable, hashable, safe to cache. The SQLAlchemy applicator lives in `varco_core.query.applicator.sqlalchemy` (not in `varco_sa`) so the query system stays backend-agnostic.

Datetime coercion policy (`DatetimeCoercionPolicy`, Plan 011 / T3), including the ⚠️
`ASTTypeCoercion`-has-no-`policy=` caveat, is covered in
`technical_docs/features/timezone-handling.md`'s T3 section.

### Transactional Outbox (varco_core.service.outbox)

Services must **not** publish events directly after a DB commit — a broker failure will silently drop the event. Use the outbox pattern instead. Mechanism + usage: README's "Transactional Outbox". Types: ARCHITECTURE.md's "Outbox Pattern".

**Rule**: `OutboxRelay` is the only place allowed to call `AbstractEventBus` directly (besides `EventConsumer.register_to()`).

### Background jobs — time, lease, fencing (varco_core.job / Plan 005 Phase 4)

`AbstractJobStore`/`AbstractJobRunner` (`varco_core.job.base`) support a time dimension, bounded
retry (reuses `varco_core.resilience.RetryPolicy`, no second retry model), and a fenced lease
(`try_claim`/`renew`/`reap_expired_leases`/`save(expected_epoch=)` → `StaleLeaseError` on a stale
write). **`run_at` is materialized, not replaced** by the zoned-schedule fields
(`run_at_wall`/`run_at_tz`/`run_at_fold`) — it keeps its exact current meaning as the UTC claim
predicate; the three new fields are the *intent* it was computed from.

Usage: README's "Background Jobs" section. Full detail (TTL/heartbeat sizing, retry-binding
decisions, zoned-schedule DST resolution): `technical_docs/features/job-scheduling-and-leases.md`
and `technical_docs/features/timezone-handling.md`'s T2 section.

### Database auditing (varco_core.service.audit)

An append-only audit trail for `create`/`update`/`delete` mutations, event-driven like the
outbox pattern but persisted by a dedicated consumer rather than a relay. **`AuditLogMixin`
composes to the LEFT of `AsyncService`**, and the consumer must be wired from `@PostConstruct`
(same rule as any other `EventConsumer`).

Usage: README's "Database Auditing" section. Full detail (idempotency per backend, retention,
tenancy, REST admin, tamper evidence via `hash_chain=True`):
`technical_docs/features/database-auditing.md`.

### Field-level encryption & crypto-shredding (varco_core.encryption / encryption_store)

`FieldEncryptor` (Protocol) → `FernetFieldEncryptor` / `MultiKeyEncryptorRegistry`
(rotation) / `TenantAwareEncryptorRegistry` (per-tenant) / `ScopedEncryptorRegistry`
(per-arbitrary-scope). `EncryptionKeyManager` persists DEKs via an `EncryptionKeyStore`.
Full design (scope-vs-tenant backfill requirement, destroy-vs-retire model, capability-shim
rule): `technical_docs/features/crypto-shredding.md`.

**Rule**: never embed personal data in a scope string — varco does not parse it.

**Rule**: `destroy(kid)`/`manager.destroy_scope(scope)` crypto-shred (tombstone); `retire(kid)`
only removes a key from rotation — decrypt of existing ciphertext still works after `retire`,
but raises `KeyDestroyedError` after `destroy`.

### A2A protocol surface — SkillAdapter + SkillSource (varco_fastapi.router.a2a / router.skill)

`SkillAdapter` exposes an agent over the Google A2A protocol, mounted at both the v1.0.0
surface and (while `legacy_paths=True`, the default) the pre-v1.0.0 paths. Full design
(path/method table, legacy-path deprecation timeline, async-A2A provenance):
`technical_docs/features/a2a-surface.md`. Usage: README's "A2A — exposing a non-router
subject" subsection.

**Rule**: `router_cls` and `source=` are mutually exclusive — `ValueError` otherwise.

**`ctx` is the U-3 auth-passthrough contract**: `SkillSource.invoke(skill_id, payload, *,
ctx=)` receives the verified caller's `AuthContext` (or `None` when no auth middleware
populated one) so the three caller classes — end user, another agent, an integrating
platform — are distinguishable in the audit trail.

### Authority / JWT system (varco_core.authority)

`JwtAuthority` signs tokens with a private key; `TrustedIssuerRegistry` verifies tokens from multiple trusted issuers. Key rotation is zero-downtime via `MultiKeyAuthority`:

```python
# Signing
authority = JwtAuthority.from_pem(pem_bytes, kid="svc:A", issuer="my-svc", algorithm="RS256")
token = authority.sign(authority.token().subject("usr_1").expires_in(timedelta(hours=1)))

# Rotation
multi = MultiKeyAuthority(authority)
multi.rotate(JwtAuthority.from_pem(new_pem, kid="svc:B", ...))
multi.retire("svc:A")   # only after all tokens signed with svc:A have expired

# Verification (multi-issuer)
registry = TrustedIssuerRegistry.from_env()
await registry.load_all()
payload = await registry.verify(raw_token)
```

Key sources (`varco_core.authority.sources`): `PemFile`, `PemFolder`, `JwksUrl`, `OidcDiscovery`. `TrustedIssuerRegistry.from_env()` reads issuer config from environment variables.

**JWKS caching knobs**: `TrustedIssuerRegistry(min_refresh_interval=..., ttl_seconds=...)`
(env: `VARCO_JWKS_MIN_REFRESH_SECONDS` default `10.0`, `VARCO_JWKS_TTL_SECONDS` default `0.0` =
disabled) tune when the in-memory keyset cache refreshes. `ttl_seconds` makes `get_key()`
proactively reload once the cache is stale, without waiting for a `kid` miss.

**Background JWKS refresh (Plan 041 / S22)**: `TrustedIssuerRegistry.start_refresh()`/
`stop_refresh()` run a background task on the same `ttl_seconds`-derived period, so a registry
can refresh without ever receiving a `verify()` call. **Off unless** `VARCO_JWKS_TTL_SECONDS`/
`interval=` is positive, and it must be started explicitly — via
`create_varco_app(jwks_refresh=JwksRefreshLifecycle(registry))` or a direct
`await registry.start_refresh()` call — **never** by a scanned `@Configuration` (the same
`varco_core.tls`/`cloudevents` rule: `container.scan("varco_core", recursive=True)` must never
auto-start a background HTTPS-fetching task). `varco_core.authority.inspect_jwks_posture()`
reports whether one is actually running. Full design:
`technical_docs/features/jwt-claim-transformer.md`'s "JWKS caching knobs, and the background
refresher" section.

#### Claim transformation + token profiles (varco_core.jwt.transform / varco_core.jwt.profile)

Real-world issuers (Keycloak, Cognito, Auth0, a bespoke internal claim) rarely name their
roles/scopes/tenant claims the way varco expects. `varco_core.jwt.transform` maps a foreign
claim shape onto the canonical names, and `varco_core.jwt.profile.TokenProfile` replaces the
single `JwtUtil.SYSTEM_ISSUER` class variable with named, composable profiles. Usage: README's
"Consume a foreign-shaped JWT" / "Gate a route on a named token profile" worked examples. Full
detail: `technical_docs/features/jwt-claim-transformer.md` and
`technical_docs/features/token-profiles.md`.

**Rule**: `JwtParser._from_raw_claims` is the single funnel both `JwtParser.parse()` and
`TrustedIssuerRegistry.verify()` (and therefore `varco_fastapi`'s `JwtBearerAuth`/
`PassthroughAuth`) go through — this is what makes claim transformation zero-code-change.

**Two BREAKING security defaults**: `VARCO_JWT_AUDIENCE` is required unless
`VARCO_JWT_ALLOW_ANY_AUDIENCE=true` (`JwtBearerAuth` refuses to construct otherwise); `iss` is
enforced by default (`VARCO_JWT_ENFORCE_ISS=true`). Full `VARCO_JWT_*` env-var reference:
README's "Verification hardening (VARCO_JWT_*)" subsection.

**A third BREAKING security default (Plan 034 / S1)**: `JwtParser.parse()` requires
`algorithms=` — there is no silent `["HS256"]` default any more, and never will be (no env-var
escape hatch). `JwtBearerAuth`/`PassthroughAuth`/`TrustedIssuerRegistry.verify()` are structurally
unaffected (they never went through the default). Fix: `algorithms=["HS256"]` (or whatever you
sign with) at the call site.

#### Credential and token lifecycle (Plan 034 — required `algorithms=`, `?api_key=` off by
default, token revocation, hashed API keys)

`varco_core.revocation` (`AbstractTokenRevocationStore`, `RevocationScope`/`RevocationEntry`) lets
an app invalidate a JWT before its `exp` — per token/subject/tenant/issuer —
through one ABC with three in-tree implementations (`NullTokenRevocationStore` the scanned DI
default, `InMemoryTokenRevocationStore`, `varco_redis.revocation.RedisTokenRevocationStore` the
production backend). `varco_core.auth.api_key.hash_api_key()`/`verify_api_key()` are the
stdlib-only (`hashlib`/`hmac`) primitives behind `ApiKeyAuth`'s `hashed_keys=` path. Full design,
the revocation scope table, and a Pitfalls table:
`technical_docs/features/credential-and-token-lifecycle.md`.

**Rule**: revoke a credential before its natural expiry → `varco_core.revocation`, never a second
verification path. Store an API key → `varco_core.auth.api_key.hash_api_key()`, never a raw dict.

**Rule**: binding a revocation store in DI (`enable_token_revocation`/
`enable_redis_token_revocation`) does **not** by itself turn checking on — the store must also be
passed to `TrustedIssuerRegistry(revocation_store=...)`. This two-step is the single most likely
way to think revocation is on when it is not (`inspect_revocation_posture()`'s
`registry_wired` field exists specifically to surface it).

**Rule**: `ApiKeyAuth`'s `?api_key=` query fallback and `WebSocketAuth`'s `?token=` fallback are
both **off by default** — a credential in a URL is already in the access/proxy/`Referer` log by
the time anything could warn about it. Name `param="api_key"` / `token_query_param="token"`
explicitly to re-enable either, one line, greppable.

### Authorization — policy engine (varco_core.auth.policy + varco_casbin)

Two layers of authorization coexist: static, token-derived (`varco_core.auth.base`) and
dynamic, engine-driven (`varco_core.auth.policy`, a pluggable `PolicyEngine` evaluating
ACL/RBAC/ABAC rules held outside the token). Bridge diagram + type hierarchy: ARCHITECTURE.md's
"Authorization — policy engine". Full design (`RequestMapper` keying, per-adapter durability
trade-offs): `technical_docs/features/casbin-authorization.md`.

**Wiring** (`varco_casbin.di`):
```python
container = bootstrap(DIContainer())  # binds CasbinPolicyEngine → PolicyEngine + PolicyManagement
enable_policy_authorizer(container)  # OPT-IN: binds PolicyEngineAuthorizer → AbstractAuthorizer
```

**Rules**:
- The authorizer is **opt-in** via `enable_policy_authorizer(container)` — it is NOT a scanned
  `@Configuration` (scan auto-activates those), so importing/bootstrapping `varco_casbin` never
  silently shadows an app's own authorizer.
- `CasbinPolicyEngine` must be a **shared singleton** (DI handles this) — a per-call engine reloads
  policy every request.
- `CasbinSettings` (pydantic `BaseSettings`) is registered via a `@Provider` in `bootstrap`, NOT
  `@Singleton` — providify cannot inject pydantic's `**values` constructor.
- The REST admin API is `build_policy_router(engine, server_auth=..., admin_role="admin")` (requires
  the `varco-casbin[fastapi]` extra) — a plain FastAPI `APIRouter` rather than a `VarcoRouter`
  (a standalone admin surface with its own JSON-body handlers; it predates `@route`'s full
  FastAPI-parameter support and there is no need to migrate it).
- Persisted dynamic CRUD needs `adapter="sqlalchemy"` (the `varco-casbin[sqlalchemy]` extra) or
  `adapter="beanie"` (the `varco-casbin[beanie]` extra, MongoDB via Beanie — also requires
  `VARCO_CASBIN_DB_NAME`); the default `memory` adapter is non-durable, and `adapter="file"`
  is durable but single-process only (concurrent writers can corrupt the CSV).

### Schema migrations (varco_core.migration + varco_sa/varco_beanie/varco_fastapi)

One backend-agnostic contract, two engines, one lifespan component, one CLI. Type diagram:
ARCHITECTURE.md's "Schema migrations" (if present) or `technical_docs/features/schema-migrations.md`
for the full picture (held-open-transaction locking mechanism, the ten-framework-table branch
story, `ensure_table()` reconciliation, Mongo index-mode).

**Rule**: `varco_fastapi` imports **only** `varco_core.migration` — never `varco_sa`,
`varco_beanie`, or `alembic`. Same seam as `AbstractEventBus`/`AbstractJobStore`.

**Default is `off` — nothing runs.** `MigrationSettings.mode` (`VARCO_MIGRATE_MODE`):
`off` (default, nothing registered) / `check` (fail startup if behind, never writes DDL —
**the recommended production posture**) / `upgrade` (lock → apply → release; for
single-instance, dev, and PaaS-without-a-pre-deploy-hook).

**Renamed in 3.0.0 (Plan 022 / AB-2): `SchemaMigrationError` / `SchemaMigrationPlan`.** The
schema-migration pair used to be called `MigrationError`/`MigrationPlan`, colliding at the
`varco_core` top level with the unrelated, older `varco_core.migrator` (domain data/field
migration) pair of the same names — so the schema pair was deliberately *not* re-exported and had
to be imported from `varco_core.migration` explicitly. The rename closes that hole: **the entire
schema-migration surface is now on `varco_core` directly**, and an import site says which concept
it means. `varco_core.migration.MigrationError` / `.MigrationPlan` still resolve, to the identical
objects (so `except` and `isinstance` are unaffected), emit a `DeprecationWarning`, and are removed
in 4.0.0. `varco_core.MigrationError`/`.MigrationPlan` still mean the **domain-migration** pair and
are unchanged.

`alembic` is an optional extra: `pip install "varco-sa[migrations]"`. See
`technical_docs/features/schema-migrations.md`.

### Multitenancy — isolation strategies, control plane, global scope (Plan 007, Plan 008)

Tenant data isolation is a **selectable deployment strategy**, not one hard-coded shape.
Three `TenantIsolation` values (`SHARED` — default, unchanged; `SCHEMA` — Postgres only;
`DATABASE` — Postgres + Mongo), `enforce_rls: bool` as an additive hardening flag on
`SHARED` rather than a fourth enum value, and an orthogonal `TenantScope`
(`TENANT`/`GLOBAL`) for shared reference data under every strategy. Type diagram + full design
(RD-4/RD-7/RD-9…RD-18 reasoning, the command/fact DAG rule, readiness-coordinator semantics,
`schema_translate_map`-vs-`search_path`, fan-out supervisor narrative, new env vars, the
connection-budget sizing worksheet, all six wiring recipes): `technical_docs/features/multitenancy.md`.
Usage: README's "Multi-tenancy (DB-level)" section.

**Rule**: `varco_fastapi.tenancy` imports **only** `varco_core.tenancy` — never `varco_sa`,
`varco_beanie`, `sqlalchemy`, or `pymongo`. Same seam rule as `AbstractEventBus`/
`AbstractMigrator`.

**Default is byte-identical to pre-Plan-007 behaviour.** `TenancySettings()` defaults:
`isolation=SHARED`, `enforce_rls=False`, every model `TenantScope.TENANT`,
`fanout_framework_tables=False`. No pool, no extra engine/client, no symbolic schema, no
control-plane surface constructed. `create_varco_app(tenancy=None)` (the default)
registers nothing.

**Rule**: `mount_tenant_admin(app, control_service, acknowledge_bundled_admin=True,
server_auth=..., admin_role="tenant-admin")` is the **only** way to expose the admin
surface — there is deliberately **no** `VARCO_TENANCY_MOUNT_ADMIN` env var, ever.

**Rule**: never ship a varco-owned Alembic revision that enables RLS (Plan 037 / §D-S12-oq4) —
varco ships the generator (`varco_sa.rls_autogen`), the ordering guarantee, and the posture
check; enabling RLS on any table, including varco's own framework tables, stays an
application-authored, reviewed revision. Full detail: `technical_docs/features/postgres-rls.md`.

### Tenant identity provenance & delegation (varco_core.tenancy.source, Plan 033 / S6, S5, S16)

**Rule**: tenant provenance — *where a request's tenant identity is allowed to come from* — is
`varco_core.tenancy.source` (`TenantSource`/`TenantSourceChain`/`TenantProvenance`).
`current_tenant()` stays the single source of truth for *who* the tenant is; this plan changes
only what feeds it. `TenantProvenance` is its own `AmbientVar`
(`varco_core.tenancy.provenance`) and must never move into `RequestContext` — same rule as
`current_tenant()` itself never living there.

**Rule**: `TenantResolutionMiddleware(chain=None)` is byte-identical to pre-3.2 header-only
behaviour (one `DeprecationWarning` at construction) — nothing flips by default. Full design
(trust ranking, cross-check modes, the subdomain algorithm, membership binding, act-as
delegation, the 4.0 flip list, a Pitfalls table): `technical_docs/features/tenant-provenance.md`.
Usage: README's "Tenant identity provenance" section.

---

## Planning & Development Workflows

### Before Adding a Feature

1. **Check ARCHITECTURE.md type hierarchies** — Find existing abstractions that apply. For example:
   - Adding authentication? → Look at `authority/jwt_authority.py` and `TrustedIssuerRegistry`
   - Adding caching? → Extend `CacheBackend` and pick an `InvalidationStrategy`
   - Adding event handling? → Extend `EventConsumer`, use `@listen`, wire with `register_to()`

2. **Check if a backend implementation already exists** — Don't implement the same interface twice:
   - Event bus? → Kafka and Redis backends exist; add a new one only if truly needed
   - Cache? → In-memory, Redis, and layered exist
   - ORM? → SQLAlchemy and Beanie exist
   - Query filtering? → AST + visitor pattern handles this; extend `ASTVisitor` if needed
   - CPU profiling? → `"cprofile"` backend exists; add pyinstrument/py-spy by implementing `CpuProfilerBackend`
   - Memory profiling? → `"tracemalloc"` backend exists; add memray by implementing `MemoryProfilerBackend`

3. **Identify the layer boundary** — Where does this feature live?
   - Protocol/ABC in `varco_core`? → Used by app code
   - Concrete impl in a backend (`varco_kafka`, `varco_redis`, `varco_sa`)? → Backend-specific
   - Service mixin? → If it composes via MRO with other mixins

Worked usage examples for every subsystem live in README.md — see its Table of Contents.

---

## When you hit a `providify` limitation or bug

**Do not work around it in application code.** The user owns the `providify` library
(`/home/edoardo/projects/providify` when present locally). If something inside `providify`
itself is missing, wrong, or forces a hand-rolled workaround (private-attribute access,
copy-pasted patch-before-register dances, silent behavior differences across versions),
**stop and report it — don't paper over it.**

**The report is a file; the ledger is only an index.** Two artifacts, and the split matters:

1. **`design/upstream-gaps/<library>-<short-slug>.md` — the durable report.** This is the real
   deliverable and it is never deleted. Cover: what upstream does today, why it is a gap, a
   minimal reproduction, the ask (with ✅/❌ per candidate fix, so the maintainer is choosing
   between options rather than reading a complaint), and any interim workaround.
2. **[UPSTREAM-GAPS.md](UPSTREAM-GAPS.md) — a one-row pointer into that file.**
   ⚠️ **This ledger is cleared from time to time, and its absence is expected, not an error.**
   If it is missing it was wiped to clear resolved rows — recreate it from the template in its
   own header and add your row. Never skip filing because the ledger is not there, and never go
   hunting for what happened to it. Nothing is lost in a clearing because the ledger holds no
   content of its own; it can always be rebuilt with `ls design/upstream-gaps/*.md`.
   (An earlier incarnation inlined every entry's full body, was deleted in `cae7f33`, and took
   all of them with it. That is the failure this split prevents.)

- **Verify the claim in providify's own source, citing `file:line`** — not from memory, not from
  `varco`'s docs about it, not from a docstring. The register carries a standing lesson (U-8)
  about entries filed off documentation that did not survive contact with source. A docstring
  that *contradicts* the source is itself good evidence, but quote both.
- **Guard every gap with a `strict=True` xfail** so the fix cannot land unnoticed and untested.
  Prefer a fast, dependency-free reproduction that runs in `make test` over one gated behind
  Docker and `-m integration` — a nightly-only guard will not warn you while you work.
- **Say so plainly if the gap is partly ours.** If varco can fix its own symptom today with a
  supported mechanism, that belongs in its own section of the report. A report that blames
  upstream for something we control is worse than no report — it turns our bug into a wait.
  (Worked example: `P22-PROVIDER-PREDESTROY` §5, where a `@Disposes` closes varco's leak with
  no upstream change at all.)
- If a workaround is genuinely unavoidable in the short term (e.g. the now-deleted
  `varco_core` compat shim filed under U-20 — six independent hand-rolled annotation
  patches consolidated into one shared, documented, deletable helper until providify 2.0.0
  shipped `@Provider(returns=...)` natively), centralize it in exactly one place, name it as a
  shim intended for deletion, and still write the report — the shim is not a substitute for it.
- This mirrors the rule for downstream consumers of `varco_*` — inside this repo, `providify` is
  the upstream and the same discipline applies to it.

---

## Coding Standards

All code in this repo follows the **coding-practice** skill. Key non-obvious rules specific to this codebase:

- `from __future__ import annotations` at the top of every file.
- `asyncio.Lock` is always created **lazily** (never at module level or `__init__`) — locks must be created inside a running event loop.
- Frozen `@dataclass(frozen=True)` for all value objects and config. Mutable dataclasses are a red flag.
- `TYPE_CHECKING` guards for cross-package type hints that would create circular imports at runtime (e.g. `consumer.py` importing from `dlq.py`).
- Every design decision gets a `DESIGN:` block with `✅` benefits and `❌` drawbacks.
- Docstrings include `Args:`, `Returns:`, `Raises:`, `Edge cases:`, `Thread safety:` / `Async safety:` where relevant.

---

## Test Conventions

- All tests are `async def` — no `@pytest.mark.asyncio` needed (auto mode).
- Integration tests require a real broker via Docker and are tagged `@pytest.mark.integration`. They are skipped by default; run with `-m integration`.
- `InMemoryEventBus` is the standard bus for unit tests. Use `bus.drain()` after publishes when `DispatchMode.BACKGROUND` is active.
- `InMemoryDeadLetterQueue` is the standard DLQ for unit tests.
- If a timing-sensitive test becomes flaky, increase its sleep margin rather than marking it xfail.

**Shared, session-scoped integration containers** (Plan 012 / RT1) — each package's
`tests/conftest.py` exposes ONE session-scoped fixture per external service (`redis_url`,
`mongo_url`, `postgres_url` + `postgres_container`, `kafka_bootstrap`, `memcached_host_port`,
`nats_url`), started once per test session instead of once per test file. **Per-test namespacing
rule**: because the container is shared, every test must confine itself to a key/topic/stream/
database/schema name it owns exclusively (a `uuid4().hex[:8]` run id is the established
convention) — never assume the server starts empty. A test that genuinely needs a pristine
server declares its own function-scoped `*_container_fresh` fixture instead, paying the full
container-boot cost explicitly and rarely.

**`VARCO_TEST_<SERVICE>_URL` override contract** (Open Question 1) — each session-scoped
fixture honors a namespaced override (`VARCO_TEST_REDIS_URL`, `VARCO_TEST_POSTGRES_URL`, …): when
set, no container is started and the value is used as-is, reported via `request.config.stash` and
in `scripts/integration_tests.sh`'s summary as "NOT a clean-room run". Bare names
(`REDIS_URL`/`DATABASE_URL`/…) are deliberately **never** honored — a developer with an unrelated
`DATABASE_URL` exported in their shell must never silently run destructive tests (schema
creates/drops) against their own dev database. `make integration-test-clean` unsets every
`VARCO_TEST_*` name first, guaranteeing fresh containers regardless of the calling shell's
environment.

**Integration tests run in CI too, now** (`.github/workflows/integration.yml`, Plan 017 / RL-5)
— push to `main`, nightly `schedule`, and `workflow_dispatch`. It always invokes `make
integration-test-clean`, never plain `make integration-test`, so every CI integration run is a
genuine clean-room run — the same guarantee described above, just automated. This is exactly why
CI provisions brokers via testcontainers rather than GitHub Actions `services:` blocks: a
`services:` block can only be wired to the conftests through the **bare** env var names
(`REDIS_URL`, `DATABASE_URL`, …) that the `VARCO_TEST_<SERVICE>_URL` contract deliberately never
honors — so a `services:`-backed workflow would either be silently ignored by the fixtures (each
one boots its own container anyway, doubling cost) or would require breaking the "bare names are
never honored" invariant just to make CI convenient. Testcontainers-only keeps the "NOT a
clean-room run" signal meaningful on the one class of run — CI — where it matters most, and keeps
local and CI runs on byte-identical code paths.

**Chaos tests** (`testkit/varco_chaos`, Plan 018 / RT7) — the `chaos` marker is **additive** to
`integration` (`pytestmark = [pytest.mark.integration, pytest.mark.chaos]`), never a replacement:
a chaos test always also carries `integration`. `scripts/integration_tests.sh` defaults
`MARKER_EXPR` to `"integration and not chaos"`, so `make integration-test` never runs one; `make
chaos-test` / `make chaos-test-clean` flip it to `"integration and chaos"`.

There are now **three** container-scope conventions, each solving a different isolation need:

| Scope | Fixture name pattern | When |
|---|---|---|
| `session` (shared) | `redis_url`, `kafka_bootstrap`, `postgres_url`, … | The default — every non-chaos, non-pristine-requiring test |
| `function` (fresh) | `*_container_fresh` | A test needs a pristine server (e.g. asserting the full topic/key list) but must not break it for anyone else |
| `module` (chaos) | `*_container_chaos` | A test is **allowed to break** the container (restart/pause) — declared **inside the chaos test module itself, never in `conftest.py`**, so no non-chaos test can accidentally depend on a container that gets restarted under it |

`ChaosContainer` (`testkit/varco_chaos/containers.py`) is the **only** sanctioned caller of
`DockerContainer.get_wrapped_container()` in the repo — every chaos test goes through its
surface (`restart()`, `paused()`, `wait_ready()`, and its `url` property, Plan 019 / §RT7b-port)
instead of reaching for the raw docker-py handle. `restart()` always uses docker-py's own
`Container.restart()`, **never** `DockerContainer.stop()` + `.start()` — the latter deletes and
recreates the container on testcontainers' side, losing even the container ID.
⚠️ `restart()` does **not** guarantee the host port survives — research 006 §A/§B/§F: Docker's
own Engine API reference documents that the allocated port "might be changed when restarting the
container", and this is platform-independent and version-stable (moby v1.3.0 → v29.1), including
GitHub Actions' native-Linux dockerd. This is documented Docker behaviour, not a WSL2-specific
flake (research 002 §1's original "port survives" claim is superseded — see its in-tree banner).
See [technical_docs/common-pitfalls.md](technical_docs/common-pitfalls.md)'s chaos
`restart()` port-instability row — the fix is
`ChaosContainer.url`, never a captured string.

Chaos tests run nightly + `workflow_dispatch` only (`.github/workflows/integration.yml`'s
`chaos` job, `if: github.event_name != 'push'`), never on `push: main`, and are never a required
check — a chaos scenario is designed to provoke a real race under container failure, and an
occasional red run is a BACKLOG/operator-triage signal, not a merge blocker.

**Conformance suite opt-in** (`testkit/varco_conformance`, Plan 012 / RT6, plus
`channel_manager.py` added by Plan 019 / RT2-C) — a shared, never-packaged suite of behavioral
contract tests, **eight** modules, one per `varco_core` ABC (`event_bus.py`, `cache.py`,
`job_store.py`, `dlq.py`, `channel_manager.py`, `idempotency_store.py`, `webhook_subscription.py`,
`token_revocation.py` — the last added by Plan 034 / S13b). Reached via one `pythonpath =
["../testkit"]` line in a package's `[tool.pytest.ini_options]`; a backend opts in with a thin
subclass overriding the abstract fixture:

```python
from varco_conformance.event_bus import EventBusConformance


class TestRedisEventBusConformance(EventBusConformance):
    @pytest.fixture
    async def bus(self, redis_url: str):
        async with RedisEventBus(RedisEventBusSettings(url=redis_url)) as bus:
            yield bus
```

The base classes are deliberately not named `Test*` — pytest never collects them standalone, so
an unimplemented fixture fails loudly (`NotImplementedError`) instead of silently passing.
`varco_core/tests/test_conformance_inmemory.py` runs the other five suites (`event_bus`, `cache`,
`job_store`, `dlq`, `token_revocation`) against every in-process implementation with no Docker
required — the fast feedback loop. `channel_manager.py` has no in-process implementation to run
there (there is no `InMemoryChannelManager` — `ChannelManager` is inherently a broker-admin
concern) and is subclassed only by the three real-broker backends (`varco_kafka`, `varco_redis`,
`varco_nats`).

**`testkit/varco_conformance/COVERAGE.md`** (Plan 024 / C7) is the authoritative, audited coverage
matrix — for every implementation of one of the eight ABCs, whether it subclasses the matching
suite and, if not, the written reason (`NoopEventBus`'s Null Object shape, `varco_ws`'s push-adapter
resolution, `varco_memcached`/`varco_casbin`'s legitimate partial/zero-ABC surface,
`channel_manager`'s lack of an in-process implementation). **Rule**: a new implementation of one of
the eight ABCs either subclasses its suite or gets a row in `COVERAGE.md` explaining why not — a
future absence must be argued against a written record, not rediscovered.

**A red conformance run means one of three things, and they take different actions** — a genuine
backend ABC violation (`@pytest.mark.xfail(reason="BUG: KI-N …", strict=True)` + a register row,
**never** an in-place production fix and **never** a weakened shared assertion), a gap in the
suite itself (fix it in `testkit/`, no marker), or a legitimate backend capability divergence
(override the one test in the subclass, with a docstring). The decision table and an in-tree
example of each: `testkit/varco_conformance/COVERAGE.md`'s **Conformance findings register**,
which is also where accumulated findings live — **not BACKLOG.md**, which is trimmed by design.
`strict=True` means the marker fails loudly the moment the bug is fixed, so it cannot rot;
`rg -n 'BUG:' varco_*/tests/ testkit/` lists every live marker and each must name a register row.

**providify's `pytest11` plugin fixtures** (providify ≥ 2.0.0, Plan 016 / RL-3d) — installing
`providify` activates its own `pytest11` entry point (`providify/pytest_plugin.py`) in every
project, with **four** function-scoped, yield-based, non-autouse fixtures:

| Fixture | Yields |
|---|---|
| `di_container` | A fresh, empty `DIContainer` — a new instance per test. |
| `di_acontainer` | The async counterpart — same fresh-per-test contract, usable directly under this repo's `asyncio_mode = "auto"` with no extra marker; `container.ashutdown()` is awaited automatically at teardown. |
| `di_overrides` | A `ContainerOverrides` bound to that test's `di_container`; any override made through it is undone automatically at teardown. |
| `di_global` | Makes `DIContainer.current()` return the test's container for the duration of the test, then restores whatever `current()` returned before. |

varco **deliberately does not re-export or wrap any of these** — see Design §RL-3d in Plan 016:
`testkit/varco_conformance` is never packaged, so it cannot deliver fixtures to a downstream
consumer anyway, and a second name for an identical fixture is pure confusion. Use them directly
via `providify`'s own names. A project's own `conftest.py` **can** redefine `di_container` (or any
of the other three) and that consumer definition wins over the plugin default
(`providify/pytest_plugin.py:33-37`) — ordinary pytest fixture-override semantics, no varco-side
opt-out mechanism needed. A test that requests none of the four sees zero behavioural difference
from providify's plugin not being installed at all — verified by
`varco_core/tests/test_providify_pytest_plugin.py`, which exercises all four plus this inertness
guarantee with no conftest edits anywhere in the repo.

---

## Common Pitfalls

The pitfall catalogue lives in **[technical_docs/common-pitfalls.md](technical_docs/common-pitfalls.md)**
— a cross-cutting table of coding-pattern traps that have actually bitten someone here
(per-call `CircuitBreaker`/`Bulkhead`/`Singleflight`, quoted `@Provider` return
annotations, `uvx ruff` vs. the pin, BSON millisecond truncation on a Mongo
exclusive-upper-bound sweep, …). **Read it before writing code that touches any of those
primitives**, and add a row when you find a new one.

Two neighbours, so you file a new pitfall in the right place:
- Feature-specific *operational* pitfalls (wrong env var → wrong runtime behaviour) go in
  that feature's own `technical_docs/features/*.md` **Pitfalls** section.
- Agent/contributor *workflow* rules stay in this file.

---

## Decision Tree: What to Implement Where?

```
Am I adding a new capability?
├─ Event system feature (new event type, new consumer pattern)?
│  └─ → varco_core.event (protocol) + varco_kafka/redis (backend)
│
├─ Cache feature (new invalidation strategy, new backend)?
│  └─ → varco_core.cache (ABC) + varco_redis/sa (impl)
│     ↳ a bulk/batch capability? → BulkCache with a portable CacheBackend
│       default, NEVER a new method on AsyncCache (breaks isinstance() for
│       out-of-tree caches, Plan 011 D-11)
│
├─ Query filtering (new comparison operator, new visitor)?
│  └─ → varco_core.query (parser + visitor) + varco_core.query.applicator.sqlalchemy
│
├─ Request-scoped ambient value (locale, timezone, anything else per-request)?
│  └─ → varco_core.context (AmbientVar + RequestContext + resolve_precedence)
│     ↳ tenant? → NO, use current_tenant() — never add it to RequestContext
│     ↳ HTTP resolution? → varco_fastapi.middleware.LocalizationMiddleware
│       (one middleware, two independent toggles — RD-3)
│
├─ Internationalization / localized output?
│  └─ → varco_core.i18n (MessageCatalog ABC + negotiation)
│     ↳ a new catalog format (ICU, MF2, Fluent)? → implement the ABC, do
│       NOT add a runtime dependency to varco_core
│     ↳ translatable entity data? → app side, a Non-goal (RD-7)
│
├─ Timezone / scheduling?
│  └─ → varco_core.tz
│     ↳ per-request user zone? → tz/resolve.py
│     ↳ DST-safe one-shot schedule? → tz/schedule.py + the three Job
│       columns (D-7)
│     ↳ recurring cron schedule (cron → Job materialization)?
│       → varco_core.schedule (Plan 032 / D6) — ScheduleMaterializer,
│         never a second AbstractJobRunner
│     ↳ RRULE/RFC 5545? → still parked — needs dateutil.rrule, a new
│       runtime dependency (technical_docs/features/recurring-schedules.md)
│
├─ Scheduled cleanup of a framework table (DLQ, audit log, idempotency
│  store, revocation store, job store)?
│  └─ → varco_core.retention (Plan 039 / S20) — bind_retention_registry(container,
│         registry) + create_varco_app(retention=RetentionLifecycle(...)), never a
│         hand-written job and never a second scheduler
│     ↳ Outbox? → NOT a target — deleting an unpublished event is event
│       loss by construction (technical_docs/features/retention-and-purge.md)
│
├─ Feature flag / runtime toggle?
│  └─ → varco_core.flags.AbstractFeatureFlags (Plan 032 / D7) —
│       NullFeatureFlags is the DI default; enable_feature_flags()
│       opts in InMemoryFeatureFlags
│     ↳ OpenFeature provider? → deferred — un-park trigger not yet
│       fired (technical_docs/features/feature-flags.md)
│
├─ Resilience pattern (new retry/timeout/breaker variant)?
│  └─ → varco_core.resilience (decorator + config)
│
├─ Browser security header (CSP, HSTS, frame options, …)?
│  └─ → varco_fastapi.middleware.security_headers (SecurityHeadersMiddleware, on by default)
├─ Request too big / memory-exhaustion ceiling on a request body?
│  └─ → varco_fastapi.middleware.body_limit (BodyLimitMiddleware, on by default at 10 MiB)
├─ HTTP rate limit (per IP/subject/tenant/global)?
│  └─ → varco_fastapi.middleware.rate_limit (RateLimitMiddleware + RateLimitBundle) —
│       never a new limiter; imports RateLimiter/RateLimitConfig from
│       varco_core.resilience.rate_limit as-is
├─ "Is my HTTP edge (headers/body-limit/rate-limit) actually configured?"
│  └─ → varco_fastapi.middleware.introspect.inspect_http_edge() — a pure read,
│       never a preflight (Plan 036's SecurityPosture is the preflight)
│
├─ File/dir change detection (config reload, cert rotation, anything watching a path)?
│  └─ → varco_core.watch — never a hand-rolled mtime dict (misses the K8s `..data` rotation)
│     ↳ Load → swap → notify subscribers on change? → varco_core.reload.ReloadableResource[T]
│
├─ TLS/CA/mTLS trust config, or a hot-reloading trust store?
│  └─ → varco_core.tls.TrustStore (+ ReloadingTrustStore for hot reload)
│     ↳ a settings-embedded fragment (nested in a ConnectionSettings)?
│                            → varco_core.connection.SSLConfig, which converts via
│                              to_trust_store()/TrustStore.to_ssl_config() (lossy the second way)
│     ↳ Need a varco trust store in httpx/aiohttp/urllib3/requests?
│                            → varco_core.tls.clients, never a hand-built context
│
├─ Profiling / performance diagnostic?
│  ├─ New CPU backend (pyinstrument, py-spy)?
│  │  └─ → implement CpuProfilerBackend + register_cpu_backend()
│  ├─ New memory backend (memray)?
│  │  └─ → implement MemoryProfilerBackend + register_memory_backend()
│  └─ New profiling primitive (service mixin, consumer wrapper)?
│     └─ → varco_core.profiling (use ProfileSession as the engine)
│
├─ Event wire format / interop envelope (CloudEvents, a partner's schema)?
│  └─ → a new Serializer[Event] implementation, NEVER a change to Event
│     ↳ CloudEvents? → already exists: varco_core.event.cloudevents (structured mode)
│     ↳ needs transport headers? → blocked on the MessageEncoder Protocol (RS-2),
│       never a headers= parameter on AbstractEventBus.publish()
│
├─ Describing the event surface to another team/tool (AsyncAPI, a schema registry)?
│  └─ → varco_core.asyncapi (runtime introspection of wired consumers)
│     + varco_core/cli/asyncapi.py for a CLI verb
│     ↳ ⛔ never a static import walk, and never a new AsyncAPI dependency
│
├─ Hiding a secret from a span/log/audit payload/error body?
│  └─ → varco_core.redaction (Redactor / PolicyRedactor / redact_mapping()), never a
│       second pattern list
│     ↳ Need the value back later (crypto-shredding, key rotation)? → that is
│       varco_core.encryption, NOT redaction — redaction destroys
│
├─ Authentication/JWT feature?
│  └─ → varco_core.authority (protocol) + varco_core.authority.sources (key sources)
│
├─ JWT claim shape (foreign IdP roles/scopes/tenant naming)?
│  └─ → varco_core.jwt.transform (ClaimMapping / ClaimTransformer) — env-driven or code-configured
│
├─ Named internal/system token recognition (replacing SYSTEM_ISSUER)?
│  └─ → varco_core.jwt.profile (TokenProfile / TokenProfileRegistry) + varco_fastapi's require_token_profile
│
├─ Revoke a credential before its natural expiry (logout, compromise, kill switch)?
│  └─ → varco_core.revocation (AbstractTokenRevocationStore) — never a second
│         verification path; wire the store into TrustedIssuerRegistry(revocation_store=...)
│
├─ Store an API key (in-memory dict, config, DB column)?
│  └─ → varco_core.auth.api_key.hash_api_key() / ApiKeyAuth(hashed_keys=...) —
│         never a raw dict of plaintext keys
│
├─ Service layer feature (mixin, hook, outbox)?
│  └─ → varco_core.service (ABC + mixin) + varco_sa/beanie (repository impl)
│
├─ Migration / schema-upgrade feature?
│  └─ → varco_core.migration (AbstractMigrator contract + MigrationSettings)
│       + the backend migrator (varco_sa.migration.AlembicMigrator /
│         varco_beanie.migration.BeanieMigrator)
│       ↳ Startup wiring? → varco_fastapi.migrate.MigrationLifecycle
│       ↳ New CLI verb?   → varco_core.cli.migrate (shared verbs) or the backend's
│                            own migration/cli.py via the "varco.commands" group
│       ↳ New framework table? → register_framework_metadata() in its owning module
│                                 + a revision in varco_sa/migrations/versions/
│
├─ Multitenancy / isolation-strategy feature?
│  └─ → varco_core.tenancy (contracts: TenantIsolation/TenantScope/TenancySettings,
│         AbstractTenantCatalog, TenantResourcePool, DynamicTenantUoWProvider,
│         GlobalUoWProvider, AbstractTenantProvisioner, TenantFanoutSupervisor)
│       + the backend implementation (varco_sa.tenancy.SASchemaRouter/SAEngineRegistry/
│         SATenantCatalog / varco_beanie.tenancy.BeanieTenantPool/BeanieTenantCatalog)
│       ↳ Startup wiring? → varco_fastapi.tenancy.TenancyLifecycle +
│                            create_varco_app(tenancy=...)
│       ↳ Request-scoped tenant resolution? → varco_fastapi.middleware.
│                            TenantResolutionMiddleware
│       ↳ Admin/provisioning surface? → varco_fastapi.tenancy.mount_tenant_admin()
│                            (never a create_varco_app kwarg — RD-9)
│       ↳ New global/shared entity? → Meta.tenant_scope = TenantScope.GLOBAL,
│                            never a new mixin (validate_service_scope() guards it)
│     ↳ Where may a tenant come from (header/subdomain/JWT claim, and what
│       if two disagree)? → varco_core.tenancy.source (TenantSourceChain)
│     ↳ Is this subject allowed that tenant? → varco_core.tenancy.membership
│     ↳ May this service act for that tenant (RFC 8693 act claim)?
│                            → varco_core.auth.delegation
│     ↳ Is my deployment still header-only? → inspect_tenant_provenance()
│     ↳ RLS DDL for tenant tables? → `varco_sa.rls_autogen` (the generated-for-you
│       path) or `varco_sa.rls.render_rls_ddl` (the per-table escape hatch); the
│       per-transaction GUC? → `varco_sa.tenancy.rls_session.install_rls_tenant_hook`;
│       is my deployment actually protected? → `inspect_rls_posture()`
│       (`technical_docs/features/postgres-rls.md`)
│
├─ Cross-repo service integration (calling a peer whose Python package is
│  not importable from this repo)?
│  └─ → varco_fastapi.contract (ServiceContract, build_contract, `varco
│         export-contract`) + varco_fastapi.client.method
│         (build_client_method, ImportedTypeResolver/SynthesizedTypeResolver)
│       ↳ Runtime, no generated file? → varco_fastapi.contract.runtime.contract_client
│       ↳ Checked-in typed client module? → `varco gen-client` /
│                            varco_fastapi.contract.codegen.render_client_module
│       ↳ Just IDE/mypy types for an existing client_for() call site?
│                            → `varco gen-client-stubs [--check]`
│       ↳ Fleet of peers, one env var each? → varco_fastapi.client.peer.PeerRegistry
│                            + bind_peers()
│       ⚠️ NOT client_for()'s custom-route methods — those are not wired
│         through build_client_method yet (see the pitfall table)
│
├─ Reliability feature (DLQ redrive/retention, audit retention/tamper
│  evidence, "opt into durability once")?
│  └─ → varco_core.event.redrive (DlqRedriver) / varco_core.event.dlq
│         (delete/delete_where/count_by_channel) / varco_core.service.audit
│         (list/delete_where/verify_chain) for the primitives
│       ↳ Bundling retry+DLQ+outbox+audit+metrics behind one object?
│                            → varco_core.reliability.ReliabilityPreset
│       ↳ FastAPI startup wiring? → varco_fastapi.reliability.ReliabilityLifecycle
│                            + create_varco_app(reliability=...)
│       ↳ REST admin/query surface? → varco_fastapi.admin.mount_reliability_admin()
│                            (never a create_varco_app kwarg — RD-9, same rule
│                            as mount_tenant_admin())
│       ↳ New CLI verb? → varco_core.cli.dlq / varco_core.cli.retention
│
├─ HTTP idempotency / dedup-a-retried-request feature (Plan 029 / D1)?
│  └─ → varco_core.idempotency (AbstractIdempotencyStore, reserve/complete/
│         get/release/delete_expired — reserve() MUST be atomic)
│       + varco_fastapi.middleware.idempotency.IdempotencyMiddleware (opt-in,
│         never create_varco_app default — install via install_middleware_stack
│         INSIDE ErrorMiddleware, INSIDE RequestContextMiddleware)
│       ↳ New backend? → implement AbstractIdempotencyStore using that
│                            backend's own atomic set-if-absent primitive;
│                            never emulate one with exists()+set()
│
├─ Outbound webhook feature (subscription, signing, SSRF, delivery, admin) (Plan 031 / D4)?
│  └─ → varco_core.webhook (WebhookSubscription/WebhookDelivery, the
│         WebhookSubscriptionRepository ABC, WebhookSigner ABC, ssrf.validate_target(),
│         WebhookDispatcher — an EventConsumer, never holds AbstractEventBus)
│       + varco_sa.webhook / varco_beanie.webhook for repositories
│       ↳ New signing scheme? → implement WebhookSigner; register in get_signer()
│       ↳ Admin/replay/rotation surface? → varco_fastapi.webhook.mount_webhook_admin()
│                            (never a create_varco_app kwarg, never an env var — RD-9)
│       ⚠️ Never weaken ssrf.validate_target()'s resolve-then-pin behaviour — a
│         validate-the-URL-string-only shortcut reopens DNS rebinding
│
├─ Receiving a signed webhook (verify a Stripe/GitHub/Slack/Svix/Standard
│  Webhooks delivery) (Plan 038 / S19)?
│  └─ → varco_core.webhook.inbound (WebhookVerifier ABC, four provider
│         adapters, get_verifier(), WebhookReplayGuard over the existing
│         AbstractIdempotencyStore — never a second HMAC path and never a
│         new replay-cache ABC)
│       + varco_fastapi.webhook.verify_webhook — a route dependency,
│         never a middleware (the secret/algorithm are per-route facts)
│       ↳ New provider? → subclass HmacWebhookVerifier; register in
│                            get_verifier()
│       ⚠️ GitHub ships no timestamp — GitHubWebhookVerifier refuses
│         construction without replay_guard= or
│         acknowledge_no_replay_protection=True
│
├─ Startup security check (is BaseAuthorizer still bound, is an admin mount
│  unauthenticated, is RLS actually enforced, ...)?
│  └─ → varco_fastapi.posture (SecurityPostureLifecycle) — never a second
│       preflight; it aggregates the sibling plans' own exported inspectors
│       (technical_docs/features/security-posture.md)
│
├─ Authorization decision logging (who was allowed/denied what, including
│  delegated/impersonated access)?
│  └─ → AuditingAuthorizer via varco_core.auth.di.enable_authorization_audit()
│       — never a middleware (authorize() is called from the service layer,
│       not HTTP) (technical_docs/features/authorization-audit.md)
│
└─ ORM/database feature?
   └─ → varco_sa (SQLAlchemy) and/or varco_beanie (MongoDB)
        ↳ Models auto-generated from varco_core.model.DomainModel
        ↳ Implement backend-specific Repository, OutboxRepository

---

Should I create a new backend implementation?
├─ Only if you're supporting a genuinely different transport/storage:
│  ├─ New event bus (e.g., RabbitMQ, AWS SNS)? → new package varco_[backend]
│  ├─ New cache backend (e.g., Memcached)? → add to varco_redis or new package
│  ├─ New ORM (e.g., Tortoise)? → new package varco_[backend]
│  └─ New DLQ (e.g., S3-based dead letters)? → new package or existing
│
└─ Do NOT create a new backend just for:
   ├─ A different config (use the existing backend with new settings)
   ├─ A new feature (extend the existing backend's interface)
   └─ Convenience (keep it simple; fewer backends = fewer bugs)

---

Should I add it to varco_core or a backend?
├─ varco_core if:
│  ├─ ✅ It's a protocol/ABC that backends implement
│  ├─ ✅ It's used by application code (services, handlers)
│  ├─ ✅ It's transport/storage agnostic (event types, domain model)
│  └─ ✅ All backends need it (caching, query, resilience)
│
└─ Backend (varco_kafka/redis/sa/beanie) if:
   ├─ ✅ It's a concrete implementation of a varco_core interface
   ├─ ✅ It depends on third-party libraries specific to that backend (aiokafka, redis, sqlalchemy)
   └─ ✅ It only makes sense for one transport/storage system
```

---

## Pre-Implementation Checklist

Before writing code, ask yourself:

- [ ] **Is this already implemented elsewhere?** → Search ARCHITECTURE.md type hierarchies, check `varco_*/` for similar patterns
- [ ] **Does this belong in varco_core or a backend?** → Use decision tree above
- [ ] **Am I respecting layer boundaries?** → Services inject protocols, not concrete implementations; only DI knows concrete types
- [ ] **Will this compose via MRO if it's a mixin?** → Does it call `super()` on every hook?
- [ ] **Is my event consumer testable?** → Decorated with `@listen`, wired in `@PostConstruct`, no bus reference in `__init__`?
- [ ] **If I'm publishing events, am I using the outbox pattern?** → Events saved in same DB transaction, relayed asynchronously?
- [ ] **If I'm caching, is my key namespaced?** → Includes tenant_id, user_id, or other scope identifier?
- [ ] **If I'm using external APIs, do I have resilience?** → Timeout + retry + circuit breaker + bulkhead (shared instances), with optional rate limiting?
- [ ] **If I'm rate-limiting, is my limiter appropriate for the deployment?** → `InMemoryRateLimiter` for single-process; `RedisRateLimiter` for multi-pod.
- [ ] **If I'm using `@hedge`, is the operation truly idempotent?** → Hedging non-idempotent writes causes duplicate side-effects.
- [ ] **Are my dataclasses frozen?** → `@dataclass(frozen=True)` for value objects, configs, AST nodes?
- [ ] **Am I creating locks lazily?** → Never at module level or `__init__`, always inside methods?
- [ ] **Did I add docstrings with Args/Returns/Raises/Edge cases?** → Especially for new abstractions and non-obvious code
- [ ] **Did I test with the right bus?** → `InMemoryEventBus` for unit tests, real broker (Docker) for integration tests?
