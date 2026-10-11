# Makefile — varco monorepo
# All commands run from the workspace root. Requires: uv (https://docs.astral.sh/uv/)
#
# Quick reference:
#   make install          — sync all workspace deps
#   make lint              — ruff check (whole repo; PKG= narrows to one package's source dirs)
#   make format             — ruff format + fix (same PKG= narrowing as lint)
#   make asyncapi-check   — AsyncAPI 3.1.0 snapshot gate (Plan 030 / N3b)
#   make entry-points-check — generated DI entry-point table gate (Plan 058 / DI-14)
#   make import-budget    — -X importtime budget per package (warn-only today;
#                            also run by `make lint` with no PKG=)
#   make type-check        — mypy (all ten source dirs; PKG= narrows to one package)
#   make test              — unit tests, all ten packages + the example suite
#                            (scripts/unit_tests.sh — accumulates pass/fail/skip
#                            instead of aborting on the first red package; big
#                            suites under xdist -n $(nproc), the rest in a
#                            bounded pool, random order with a printed seed —
#                            VARCO_UNIT_WORKERS=0 VARCO_UNIT_JOBS=1 = serial)
#   make test PKG=varco_core — unit tests for one package
#   make integration-test — integration tests (requires Docker; honors any
#                            VARCO_TEST_*_URL override present in the shell)
#   make integration-test PKG=varco_redis — integration tests for one package
#   make integration-test-clean — integration tests, guaranteed clean-room
#                            (unsets every VARCO_TEST_*_URL override first)
#   `.github/workflows/integration.yml` runs `make integration-test-clean`
#   (push:main + nightly + workflow_dispatch) — always the clean-room entry
#   point, so every CI integration run is a genuine clean-room run. It is
#   NOT a required check; `.github/workflows/test.yml`'s `all-green` job is
#   the only one (RL-5, plans/017-ci-green-workflows-and-lint-type-gates.md).
#   make chaos-test        — chaos tests: kills/pauses/restarts a real
#                            container mid-test (requires Docker; honors any
#                            VARCO_TEST_*_URL override present in the shell).
#                            Excluded from `make integration-test` by default.
#   make chaos-test PKG=varco_redis — chaos tests for one package
#   make chaos-test-clean  — chaos tests, guaranteed clean-room (unsets every
#                            VARCO_TEST_*_URL override first)
#   `.github/workflows/integration.yml`'s `chaos` job runs
#   `make chaos-test-clean` (nightly + workflow_dispatch only, never on
#   push:main) — it is NOT a required check, same disposition as the
#   `integration` job (Plan 018 / RT7-ci).
#   make build            — build wheels for all packages
#   make build PKG=varco_redis — build one package
#   make publish          — publish all dist/* to PyPI (requires UV_PUBLISH_TOKEN)
#   make docs-deps        — install documentation tooling (mkdocs + mkdocstrings)
#   make docs             — build the static HTML docs site into ./site
#   make docs-serve       — live-reload docs preview at http://127.0.0.1:8000
#   make bench            — run benchmarks/ uninstrumented (never a gate; see
#                            benchmarks/README.md and .github/workflows/bench.yml)
#   make clean            — remove all dist/ directories and the built docs site

.DEFAULT_GOAL := help

# ── Package list ──────────────────────────────────────────────────────────────
# Derived from [tool.uv.workspace] members (root pyproject.toml) via
# scripts/packages.sh — single source of truth, Plan 020 / RL-18. Do NOT
# hand-edit this list; edit the workspace members instead.
PACKAGES := $(shell $(CURDIR)/scripts/packages.sh)

# Optional single-package override: make test PKG=varco_redis
PKG ?=

# ── Helpers ───────────────────────────────────────────────────────────────────
# Resolve actual target list: single package when PKG is set, all otherwise.
ifeq ($(PKG),)
  _TARGETS := $(PACKAGES)
else
  _TARGETS := $(PKG)
endif

# Derive source dirs (package-name/package-name) for lint/format/type-check.
_SRC_DIRS := $(foreach p,$(_TARGETS),$(p)/$(p))

# ── Formatting ────────────────────────────────────────────────────────────────
# RL-6: `uv run ruff` resolves the pinned version from uv.lock (root
# `[dependency-groups] lint`) — the previous ephemeral-resolve invocation
# picked up whatever ruff release was newest at invocation time, so a local
# green said nothing about CI.
# `--all-packages --all-extras` is NOT optional here (see the mypy note below).
RUFF  := uv run --all-packages --all-extras ruff
MYPY  := uv run --all-packages --all-extras mypy

# DESIGN: why both carry --all-packages --all-extras, even though ruff never
# imports anything.  `uv run` syncs the environment before executing, and a
# bare `uv run` syncs to the DEFAULT set — which UNINSTALLS optional extras a
# previous `uv sync --all-extras` had put there.  With `mcp` and
# `prometheus-client` gone, `ignore_missing_imports = true` silently degrades
# their types to Any and mypy reports two errors that have nothing to do with
# the code under check:
#     router/mcp.py     no-any-unimported  (_MCPTool became Any)
#     router/metrics.py unused-ignore      (a call on Any is not untyped)
# So a bare `uv run ruff` immediately before `make type-check` was enough to
# break it.  ✅ Both vars now request the same environment CI builds with
# `uv sync --locked --all-packages --all-extras`, so no target can strip the
# extras out from under another.  ❌ Each invocation re-checks the sync
# (fast, and a no-op once the env matches).

# ─────────────────────────────────────────────────────────────────────────────
.PHONY: help
help:
	@echo ""
	@echo "  varco — available targets"
	@echo ""
	@echo "  make install                 sync all workspace deps"
	@echo "  make sync                    alias for install"
	@echo "  make print-packages          print the derived package list (RL-18)"
	@echo "  make lint                    ruff check + format --check + api-check (whole repo)"
	@echo "  make lint PKG=varco_redis    ruff check (one package's source dirs; no api-check)"
	@echo "  make api-check               api_surface.py --check (removals + fn signature changes)"
	@echo "  make asyncapi                regenerate the committed AsyncAPI 3.1.0 snapshot"
	@echo "  make asyncapi-check          diff the AsyncAPI snapshot against live consumer wiring"
	@echo "  make di-catalogue            regenerate the committed DI default-implementation catalogue"
	@echo "  make di-catalogue-check      diff the DI catalogue against live bootstrapped containers"
	@echo "  make settings-reference      regenerate the committed VARCO_* settings reference"
	@echo "  make settings-check          diff the settings reference against the live tree"
	@echo "  make entry-points            regenerate the generated DI entry-point table (3 outputs)"
	@echo "  make entry-points-check      diff the entry-point table + ARCHITECTURE.md block vs the live tree"
	@echo "  make format                  ruff format + fix (whole repo)"
	@echo "  make format PKG=varco_redis  ruff format + fix (one package's source dirs)"
	@echo "  make type-check              mypy (all ten source dirs)"
	@echo "  make test                    unit tests, all ten packages + example suite (xdist + pool lanes, seed printed)"
	@echo "                                (scripts/unit_tests.sh; same script CI runs)"
	@echo "  make test PKG=varco_core     unit tests (one package)"
	@echo "  make integration-test        integration tests (requires Docker; honors"
	@echo "                                VARCO_TEST_*_URL overrides if set)"
	@echo "  make integration-test PKG=varco_redis  integration tests (one package)"
	@echo "  make integration-test-clean  integration tests, clean-room (unsets every"
	@echo "                                VARCO_TEST_*_URL override first)"
	@echo "                                — .github/workflows/integration.yml runs"
	@echo "                                exactly this target (push:main + nightly +"
	@echo "                                workflow_dispatch); not a required check"
	@echo "  make chaos-test              chaos tests: kills/pauses/restarts a real"
	@echo "                                container mid-test (requires Docker; honors"
	@echo "                                VARCO_TEST_*_URL overrides if set); excluded"
	@echo "                                from integration-test by default"
	@echo "  make chaos-test PKG=varco_redis  chaos tests (one package)"
	@echo "  make chaos-test-clean        chaos tests, clean-room (unsets every"
	@echo "                                VARCO_TEST_*_URL override first) — the"
	@echo "                                integration.yml 'chaos' job runs exactly"
	@echo "                                this target (nightly + workflow_dispatch"
	@echo "                                only); never a required check"
	@echo "  make build                   build wheels + sdists (all packages)"
	@echo "  make build PKG=varco_redis   build one package"
	@echo "  make publish                 publish dist/* to PyPI"
	@echo "  make docs-deps               install documentation tooling"
	@echo "  make docs                    build static HTML docs into ./site"
	@echo "  make docs-serve              live-reload docs preview"
	@echo "  make clean                   remove all dist/ directories + ./site"
	@echo ""

# ── Install / sync ────────────────────────────────────────────────────────────
.PHONY: install sync
install sync:
	uv sync

# print-packages — prints the derived PACKAGES list, space-separated. Used by
# varco_core/tests/test_repo_package_lists.py's drift guard, and useful for
# humans (`make -s print-packages` for scripting).
.PHONY: print-packages
print-packages:
	@echo $(PACKAGES)

# ── Lint ──────────────────────────────────────────────────────────────────────
# §RL-6-ruff: whole repo (`.`) by default — covers tests/, testkit/, examples/,
# scripts/ and varco_casbin too, none of which the old $(_SRC_DIRS)-only scope
# reached. `PKG=` still narrows to that one package's source dirs for local
# iteration speed.
_LINT_TARGET := $(if $(PKG),$(_SRC_DIRS),.)

.PHONY: lint
lint:
	$(RUFF) check $(_LINT_TARGET)
	$(RUFF) format --check $(_LINT_TARGET)
ifeq ($(strip $(PKG)),)
	$(MAKE) api-check
	$(MAKE) import-budget
	$(MAKE) asyncapi-check
	$(MAKE) di-catalogue-check
	$(MAKE) settings-check
	$(MAKE) entry-points-check
endif

# ── API surface gate ──────────────────────────────────────────────────────────
# §D-C5 (Plan 024 / C5): `--check` diffs the live tree against the committed
# `design/api-freeze-and-standards/measurements/api-surface.json` snapshot and
# exits non-zero on a removal or a *function* signature change (never a class
# `__init__` narrowing — see CLAUDE.md's "Public API surface snapshot" section
# for the documented scope). Runs `uv run python` directly (not through $(RUFF)/
# $(MYPY)) because it imports every package live, same as `make type-check`.
.PHONY: api-check
api-check:
	uv run --all-packages --all-extras python scripts/api_surface.py --check

# ── Import-time budget ────────────────────────────────────────────────────────
# Plan 028 / Phase 1 (P1b), §D-P1-oq4. Measures each distribution package's
# `-X importtime` cost above a bare-interpreter baseline (best-of-5, fresh
# subprocesses) and compares it against the hard ceiling committed in
# design/async-performance-patterns/measurements/import-budget.json.
#
# ⚠️ `--warn-only` is deliberate and temporary: the ~2x ceiling headroom is an
# assumption about runner variance that no source quantifies, so Plan 028's
# Steps 13-14 collect >= 10 real CI observations before the flag is dropped
# here and in .github/workflows/test.yml. Until then a breach prints loudly and
# exits 0.
#
# Wired into `make lint`'s no-PKG path only, beside `api-check` — `make lint
# PKG=<one>` stays narrow and fast (the §D-C5 rule Plan 024 set for api-check).
# Runs `uv run python` directly because it imports every package live.
.PHONY: import-budget
import-budget:
	uv run --all-packages --all-extras python scripts/import_budget.py --check --warn-only

# ── AsyncAPI document gate ────────────────────────────────────────────────────
# Plan 030 / Phase 2 (N3b), §D-AA4. Regenerates the AsyncAPI 3.1.0 document from
# the example app's LIVE, registered consumers and diffs it against the committed
# snapshot, exiting non-zero on drift — the same snapshot-plus-`--check` shape
# `api-check` uses.
#
# The subject is deliberately ONE example app's consumers, not the whole repo, so
# the snapshot moves only when that app's wiring moves — a gate that churned on
# unrelated changes would train contributors to regenerate blindly.
#
# ⚠️ No Node in CI (§D-AA4): this checks OUR document against OUR snapshot. Spec
# conformance was validated once, by hand, with `npx @asyncapi/cli validate` —
# see design/api-freeze-and-standards/measurements/asyncapi-validate.txt.
#
# Wired into `make lint`'s no-PKG path only, beside `api-check`/`import-budget`
# — `make lint PKG=<one>` stays narrow and fast (the §D-C5 rule).
ASYNCAPI_SNAPSHOT := design/api-freeze-and-standards/measurements/asyncapi-example.json
ASYNCAPI_ARGS := --title "varco example — full-stack post API" --version 0.1.0 \
	--path examples/00-full-stack-post-api \
	--consumer example.consumer:PostEventConsumer \
	-o $(ASYNCAPI_SNAPSHOT)

.PHONY: asyncapi-check
asyncapi-check:
	uv run --all-packages --all-extras varco export-asyncapi $(ASYNCAPI_ARGS) --check

.PHONY: asyncapi
asyncapi:
	uv run --all-packages --all-extras varco export-asyncapi $(ASYNCAPI_ARGS)

# ── DI catalogue gate ─────────────────────────────────────────────────────────
# Plan 047 §D-script (DI-17). Same snapshot-plus-`--check` shape as
# `api-check`: regenerates the default-implementation catalogue from every
# package's own bootstrapped container and diffs it against the committed
# design/api-freeze-and-standards/measurements/di-catalogue.json.
#
# Wired into `make lint`'s no-PKG path only, beside `api-check`/
# `import-budget`/`asyncapi-check` — `make lint PKG=<one>` stays narrow.
.PHONY: di-catalogue-check
di-catalogue-check:
	uv run --all-packages --all-extras python scripts/di_catalogue.py --check

.PHONY: di-catalogue
di-catalogue:
	uv run --all-packages --all-extras python scripts/di_catalogue.py

# ── Settings reference gate ───────────────────────────────────────────────────
# Plan 048 / DI-6, §D-script. Same snapshot-plus-`--check` shape as
# `api-check`/`di-catalogue-check`: regenerates the VARCO_* settings reference
# (every bind_settings()-eligible class's env vars, defaults, secrecy) from the
# live tree and diffs it against the committed
# design/api-freeze-and-standards/measurements/settings-reference.json.
#
# Wired into `make lint`'s no-PKG path only, beside `api-check`/
# `import-budget`/`asyncapi-check`/`di-catalogue-check` — `make lint PKG=<one>`
# stays narrow.
.PHONY: settings-check
settings-check:
	uv run --all-packages --all-extras python scripts/settings_reference.py --check

.PHONY: settings-reference
settings-reference:
	uv run --all-packages --all-extras python scripts/settings_reference.py

# ── Entry-point table gate ────────────────────────────────────────────────────
# Plan 058 / DI-14, §D-entrypoints. Same snapshot-plus-`--check` shape as
# `api-check`: walks every module of every package (scripts/packages.sh) and
# classifies every bootstrap/bind_*/enable_*/mount_*/install_* function,
# VarcoModule and @Configuration. Three outputs — the JSON snapshot,
# technical_docs/entry-points.md, and the marked block in ARCHITECTURE.md —
# and `--check` diffs the snapshot AND the ARCHITECTURE.md block.
#
# Wired into `make lint`'s no-PKG path only, beside `api-check`/
# `di-catalogue-check`/`settings-check` — `make lint PKG=<one>` stays narrow.
.PHONY: entry-points-check
entry-points-check:
	uv run --all-packages --all-extras python scripts/entry_points.py --check

.PHONY: entry-points
entry-points:
	uv run --all-packages --all-extras python scripts/entry_points.py

# ── Benchmarks ────────────────────────────────────────────────────────────────
# Plan 028 / Phase 3 (P2), §D-P2-harness. Runs benchmarks/ as plain pytest
# tests with no instrumentation and no CODSPEED_TOKEN — the local, uninstrumented
# run that stops an ungated benchmark from rotting.
#
# ⚠️ These are NEVER a gate. .github/workflows/bench.yml is a separate workflow,
# is not in test.yml's `needs:`, must never be added to `all-green`'s `needs:`,
# and must never be selected as a required status check. The asymmetry with
# `make import-budget` (which IS wired into `make lint`) is deliberate and is
# argued in §D-P1-oq4 and benchmarks/README.md.
#
# `--group bench` rather than the `dev` group: pytest-codspeed must not be
# installed by a normal `uv sync`, so its plugin can never affect the unit legs.
# scripts/unit_tests.sh iterates an explicit suite list (each package's tests/
# plus examples/00-full-stack-post-api) and so never collects benchmarks/.
.PHONY: bench
bench:
	uv run --group bench --all-packages --all-extras pytest benchmarks/

# ── Format ────────────────────────────────────────────────────────────────────
.PHONY: format
format:
	$(RUFF) format $(_LINT_TARGET)
	$(RUFF) check --fix $(_LINT_TARGET)

# ── Type check ────────────────────────────────────────────────────────────────
.PHONY: type-check
type-check:
	$(MYPY) $(_SRC_DIRS)

# ── Unit tests ────────────────────────────────────────────────────────────────
# §RL-5-parity: delegates to scripts/unit_tests.sh, which accumulates
# pass/fail/skip across every suite instead of aborting on the first red
# package — the same script CI runs (bash scripts/unit_tests.sh), so a green
# `make test` means a green CI `unit` leg. `PKG=` narrows to one package
# (forwarded as a positional arg; one suite, foreground, live); unset runs all
# ten packages + the example suite in two lanes — xdist for the big suites,
# then a pool of at most $(nproc) concurrent suites (Plan 059).
.PHONY: test
test:
	@bash scripts/unit_tests.sh $(PKG)

# ── Integration tests ─────────────────────────────────────────────────────────
# `integration-test` honors any VARCO_TEST_*_URL override present in the
# environment (see scripts/integration_tests.sh's header and Open Question 1
# in plans/012-r3-reliability-and-regression-proofing.md). This is the
# developer-facing, override-honouring target — CI instead always calls
# `integration-test-clean` below via .github/workflows/integration.yml
# (push:main + nightly + workflow_dispatch), so a stray shell env var can
# never point a CI run at a non-clean-room broker.
.PHONY: integration-test
integration-test:
	@if [ -n "$(PKG)" ]; then \
		bash scripts/integration_tests.sh $(PKG); \
	else \
		bash scripts/integration_tests.sh; \
	fi

# `integration-test-clean` is the guaranteed clean-room entry point: every
# VARCO_TEST_*_URL override name is explicitly unset first, so the run always
# exercises fresh testcontainers-managed brokers/databases regardless of what
# is set in the calling shell.
.PHONY: integration-test-clean
integration-test-clean:
	@if [ -n "$(PKG)" ]; then \
		env -u VARCO_TEST_REDIS_URL -u VARCO_TEST_MONGO_URL -u VARCO_TEST_POSTGRES_URL \
			-u VARCO_TEST_KAFKA_URL -u VARCO_TEST_MEMCACHED_URL -u VARCO_TEST_NATS_URL \
			bash scripts/integration_tests.sh $(PKG); \
	else \
		env -u VARCO_TEST_REDIS_URL -u VARCO_TEST_MONGO_URL -u VARCO_TEST_POSTGRES_URL \
			-u VARCO_TEST_KAFKA_URL -u VARCO_TEST_MEMCACHED_URL -u VARCO_TEST_NATS_URL \
			bash scripts/integration_tests.sh; \
	fi

# ── Chaos tests (Plan 018 / RT7b) ──────────────────────────────────────────────
# `chaos-test` / `chaos-test-clean` mirror `integration-test` / `integration-test-clean`
# above exactly, differing only in MARKER_EXPR: "integration and chaos" instead
# of the script's own default "integration and not chaos". Chaos tests kill,
# pause, or restart a real container mid-test (§RT7-shape) — never run as part
# of the default `integration-test` target, and never a required CI check
# (.github/workflows/integration.yml's `chaos` job, nightly + dispatch only).
.PHONY: chaos-test
chaos-test:
	@if [ -n "$(PKG)" ]; then \
		MARKER_EXPR="integration and chaos" bash scripts/integration_tests.sh $(PKG); \
	else \
		MARKER_EXPR="integration and chaos" bash scripts/integration_tests.sh; \
	fi

# `chaos-test-clean` is the guaranteed clean-room entry point for chaos tests —
# same six VARCO_TEST_*_URL names unset first as `integration-test-clean`, so a
# stray shell env var can never point a chaos run at a container it does not
# own (and is therefore not allowed to restart/pause).
.PHONY: chaos-test-clean
chaos-test-clean:
	@if [ -n "$(PKG)" ]; then \
		env -u VARCO_TEST_REDIS_URL -u VARCO_TEST_MONGO_URL -u VARCO_TEST_POSTGRES_URL \
			-u VARCO_TEST_KAFKA_URL -u VARCO_TEST_MEMCACHED_URL -u VARCO_TEST_NATS_URL \
			MARKER_EXPR="integration and chaos" bash scripts/integration_tests.sh $(PKG); \
	else \
		env -u VARCO_TEST_REDIS_URL -u VARCO_TEST_MONGO_URL -u VARCO_TEST_POSTGRES_URL \
			-u VARCO_TEST_KAFKA_URL -u VARCO_TEST_MEMCACHED_URL -u VARCO_TEST_NATS_URL \
			MARKER_EXPR="integration and chaos" bash scripts/integration_tests.sh; \
	fi

# ── Build ─────────────────────────────────────────────────────────────────────
.PHONY: build
build:
	$(foreach pkg,$(_TARGETS),uv build --package $(pkg) --out-dir $(pkg)/dist;)

# ── Publish ───────────────────────────────────────────────────────────────────
# ⚠️ BREAK-GLASS MANUAL PATH ONLY (Plan 023 / §RL-10-publish). The sanctioned
# release path is a `v*` git tag + `.github/workflows/release.yml`, which
# publishes over OIDC trusted publishing (no token, PEP 740 attestations) —
# see design/varco-1-0-release/release-runbook.md. This target requires
# UV_PUBLISH_TOKEN to be set (or --token flag) and stores no token anywhere in
# this repo or CI; it exists only for a genuine break-glass scenario where the
# tag-triggered workflow itself cannot be used.
# Publishes wheels from every package's dist/ directory.
.PHONY: publish
publish:
	$(foreach pkg,$(_TARGETS), \
		if [ -d $(pkg)/dist ] && ls $(pkg)/dist/*.whl 1>/dev/null 2>&1; then \
			uv publish $(pkg)/dist/*; \
		fi; \
	)

# ── Docs ──────────────────────────────────────────────────────────────────────
# MkDocs Material + mkdocstrings. The API reference is generated from package
# docstrings at build time (scripts/gen_ref_pages.py); hand-written feature docs
# live in technical_docs/features/. Output goes to ./site (gitignored).
#
# DOCS_ENV silences the MkDocs-2.0 banners injected by mkdocs-material
# (NO_MKDOCS_2_WARNING) and by the properdocs fork pulled in transitively by the
# gen-files/literate-nav/section-index plugins (DISABLE_MKDOCS_2_WARNING). We pin
# mkdocs<2 in the docs dependency group so a future v2 can't silently break the build.
DOCS_ENV := NO_MKDOCS_2_WARNING=1 DISABLE_MKDOCS_2_WARNING=true

.PHONY: docs-deps
docs-deps:
	uv sync --all-packages --all-extras --group docs

# Normal build — produces ./site even while docstring coverage is still improving.
.PHONY: docs
docs: docs-deps
	$(DOCS_ENV) uv run mkdocs build

# Completeness gate — fails on any docstring/reference warning. This is the target
# the api-docs-maintainer agent drives toward (clean strict build = docs complete).
.PHONY: docs-strict
docs-strict: docs-deps
	$(DOCS_ENV) uv run mkdocs build --strict

.PHONY: docs-serve
docs-serve: docs-deps
	$(DOCS_ENV) uv run mkdocs serve

# ── Clean ─────────────────────────────────────────────────────────────────────
.PHONY: clean
clean:
	rm -rf $(foreach pkg,$(_TARGETS),$(pkg)/dist) dist/ site/
