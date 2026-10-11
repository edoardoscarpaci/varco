#!/usr/bin/env bash
# integration_tests.sh — Run integration tests for one package or all packages.
#
# Tests spin up their own Docker containers via pytest fixtures — no manual
# service setup required.  The only prerequisite is a running Docker daemon.
# This script IS what CI runs: .github/workflows/integration.yml's
# integration job calls `make integration-test-clean`, which is this script
# with every VARCO_TEST_*_URL override unset first (Makefile) — so a run
# invoked directly by a developer (with overrides honoured) and a CI run
# (always clean-room) share this one entry point
# (RL-5, plans/017-ci-green-workflows-and-lint-type-gates.md).
#
# Usage:
#   scripts/integration_tests.sh                  # run all packages + example
#   scripts/integration_tests.sh varco_redis       # run one package
#   scripts/integration_tests.sh varco_kafka varco_redis  # run specific packages
#   make integration-test                          # same as the bare invocation
#   make integration-test PKG=varco_redis           # same as passing one package
#   make integration-test-clean                     # clean-room run: every
#                                                     # VARCO_TEST_*_URL override
#                                                     # is unset first, guaranteeing
#                                                     # fresh testcontainers for
#                                                     # every backend
#
# Environment:
#   VARCO_RUN_INTEGRATION  — automatically set to "1"; do NOT set it yourself.
#   PYTEST_EXTRA_ARGS      — passed verbatim to every pytest invocation,
#                            e.g. PYTEST_EXTRA_ARGS="-x -s" ./scripts/integration_tests.sh
#   VARCO_TEST_<SERVICE>_URL — per-service opt-out from the default
#                            testcontainers-managed broker/database (e.g.
#                            VARCO_TEST_REDIS_URL, VARCO_TEST_KAFKA_URL,
#                            VARCO_TEST_POSTGRES_URL, VARCO_TEST_MONGO_URL,
#                            VARCO_TEST_MEMCACHED_URL, VARCO_TEST_NATS_URL).
#                            Deliberately namespaced (never bare REDIS_URL /
#                            DATABASE_URL — see Open Question 1 in
#                            plans/012-r3-reliability-and-regression-proofing.md).
#                            A run with any override set is NOT a clean-room
#                            run and is called out below and in the summary.
#
# Exit codes:
#   0 — all selected suites passed
#   1 — one or more suites failed, or Docker is not available

set -euo pipefail

# ── Resolve workspace root regardless of where the script is called from ──────
ROOT="$(cd "$(dirname "$0")/.." && pwd)"

# ── Marker expression (Plan 018 / RT7 — chaos scaffolding) ────────────────────
# Chaos tests (`@pytest.mark.chaos`, always also carrying `integration`) kill,
# pause, or restart a real container mid-test. They are additive to the
# `integration` marker, not a replacement for it, and are excluded from the
# default developer/CI `integration` run so `make integration-test` never
# becomes flaky just because a chaos scenario is red. `make chaos-test` /
# `make chaos-test-clean` (Makefile) override this to
# "integration and chaos" — the only other value this script is exercised
# with.
MARKER_EXPR="${MARKER_EXPR:-integration and not chaos}"

# ── Colour helpers (fall back gracefully when stdout is not a tty) ────────────
if [[ -t 1 ]]; then
  RED="\033[0;31m"; GREEN="\033[0;32m"; YELLOW="\033[1;33m"
  CYAN="\033[0;36m"; BOLD="\033[1m"; RESET="\033[0m"
else
  RED=""; GREEN=""; YELLOW=""; CYAN=""; BOLD=""; RESET=""
fi

# ── Docker availability check ─────────────────────────────────────────────────
# Tests manage their own containers; we only need to confirm the daemon is up.
echo -e "\n${BOLD}── Docker check ───────────────────────────────────────────────────────────${RESET}"
if ! command -v docker &>/dev/null; then
  echo -e "  ${RED}✘${RESET}  docker not found — install Docker and retry." >&2
  exit 1
fi
if ! docker info &>/dev/null; then
  echo -e "  ${RED}✘${RESET}  Docker daemon is not running — start it and retry." >&2
  exit 1
fi
echo -e "  ${GREEN}✔${RESET}  Docker daemon is running"

# ── VARCO_TEST_*_URL override report (Open Question 1) ────────────────────────
# A run with any VARCO_TEST_*_URL override set is NOT a clean-room run — the
# override skips the session-scoped testcontainers fixture entirely for that
# service. Reported loudly, up front and again in the summary, so a green run
# is never silently mistaken for one that exercised fresh containers.
OVERRIDES_FOUND=()
while IFS='=' read -r name value; do
  [[ "$name" == VARCO_TEST_*_URL ]] || continue
  OVERRIDES_FOUND+=("$name=$value")
done < <(env)

if [[ ${#OVERRIDES_FOUND[@]} -gt 0 ]]; then
  echo -e "\n${YELLOW}${BOLD}⚠ Override(s) active — this is NOT a clean-room run:${RESET}"
  for override in "${OVERRIDES_FOUND[@]}"; do
    echo -e "  ${YELLOW}⚠ Override active: ${override}${RESET}"
  done
fi

echo -e "\n${BOLD}Marker expression:${RESET} ${MARKER_EXPR}"
if [[ "$MARKER_EXPR" == "integration and not chaos" ]]; then
  echo -e "  ${YELLOW}chaos tests excluded — run 'make chaos-test'${RESET}"
fi

# ── Known integration-test packages ───────────────────────────────────────────
# Derived from scripts/packages.sh (single source of truth, Plan 020 / RL-18),
# minus a locally-declared, named exclusion list — the derivation intentionally
# does not decide who gets excluded; that stays visible and reasoned here.
#
# INTEGRATION_EXCLUDE: packages with no broker-facing integration tests.
#   - varco_core: has no broker of its own to integration-test against (it is
#     the transport/storage-agnostic core); running it here would report a
#     spurious "no tests collected" pass forever, not a genuine skip.
INTEGRATION_EXCLUDE=("varco_core")

mapfile -t _ALL_PACKAGES < <("$ROOT/scripts/packages.sh")
ALL_INTEGRATION_PACKAGES=()
for pkg in "${_ALL_PACKAGES[@]}"; do
  excluded=0
  for ex in "${INTEGRATION_EXCLUDE[@]}"; do
    [[ "$pkg" == "$ex" ]] && excluded=1 && break
  done
  [[ $excluded -eq 0 ]] && ALL_INTEGRATION_PACKAGES+=("$pkg")
done

# ── Extra suites (RT8, Step 33) ────────────────────────────────────────────────
# Suites that do not fit the "$pkg/tests" shape assumed above — declared as
# "<dir relative to ROOT>:<testpath relative to dir>". Only run in the
# default (no-args) invocation, so `scripts/integration_tests.sh varco_redis`
# is unaffected and ALL_INTEGRATION_PACKAGES stays a pure package list.
EXTRA_SUITES=("examples/00-full-stack-post-api:example/tests")

# ── Determine which suites to run ──────────────────────────────────────────────
# Each entry is "<dir>:<testpath>:<label>:<run_from>". For a plain package,
# dir == label, testpath is always "tests", and run_from="cd" (invoke
# `uv run pytest` from inside <dir>, so it resolves that package's own
# workspace-member pyproject.toml/venv).
#
# The example suite is NOT a workspace member (root pyproject.toml lists
# "examples", not "examples/00-full-stack-post-api") — `uv run` invoked
# from inside that directory resolves an entirely separate, non-workspace
# environment with no varco_core installed (`uv run pytest` there fails
# collection with `ModuleNotFoundError: No module named 'varco_core'`,
# verified locally). run_from="root" instead runs `uv run pytest
# <dir>/<testpath>/` from the WORKSPACE ROOT, so `uv run` resolves the
# shared workspace venv (varco_core et al. installed) while pytest's own
# rootdir/ini discovery still walks up from the given testpath and finds
# examples/00-full-stack-post-api/pyproject.toml's own asyncio settings
# regardless of cwd (verified: identical collection result either way).
SUITES=()
if [[ $# -eq 0 ]]; then
  for pkg in "${ALL_INTEGRATION_PACKAGES[@]}"; do
    SUITES+=("$pkg:tests:$pkg:cd")
  done
  for extra in "${EXTRA_SUITES[@]}"; do
    extra_dir="${extra%%:*}"
    extra_testpath="${extra#*:}"
    SUITES+=("$extra_dir:$extra_testpath:$extra_dir:root")
  done
else
  for pkg in "$@"; do
    SUITES+=("$pkg:tests:$pkg:cd")
  done
fi

# ── Validate suite paths ────────────────────────────────────────────────────────
for suite in "${SUITES[@]}"; do
  suite_dir="${suite%%:*}"
  rest="${suite#*:}"
  suite_testpath="${rest%%:*}"
  if [[ ! -d "$ROOT/$suite_dir/$suite_testpath" ]]; then
    echo -e "${RED}ERROR: '$suite_dir' does not have a '$suite_testpath' directory under $ROOT/${RESET}" >&2
    exit 1
  fi
done

# ── Run pytest for each suite ───────────────────────────────────────────────────
# VARCO_RUN_INTEGRATION=1 activates the integration suite inside each test
# module (they check this env var and skip when absent).  We also pass
# -m "$MARKER_EXPR" so pytest's marker filter applies — belt-and-suspenders.
FAILED_SUITES=()
PASSED_SUITES=()
SKIPPED_SUITES=()

echo -e "\n${BOLD}── Running integration tests ──────────────────────────────────────────────${RESET}\n"

for suite in "${SUITES[@]}"; do
  suite_dir="${suite%%:*}"
  rest="${suite#*:}"
  suite_testpath="${rest%%:*}"
  rest2="${rest#*:}"
  suite_label="${rest2%%:*}"
  suite_run_from="${rest2#*:}"

  echo -e "${CYAN}▶  $suite_label${RESET}"
  # DESIGN: cd into each suite's own directory before invoking pytest
  # (run_from="cd", every workspace-member package).
  #   pytest anchors rootdir by walking up from the test path to find pyproject.toml.
  #   Running from the workspace root makes pytest use the workspace pyproject.toml,
  #   which lacks per-package pythonpath/asyncio_mode settings — tests fail to collect.
  #   cd-ing into the suite's dir makes pytest use its own pyproject.toml.
  #   ✅ Correct rootdir → correct config → tests collect and markers work.
  #   ❌ Changes the working directory inside the subprocess — transparent to the parent shell.
  # run_from="root" (the example suite — see the SUITES-building comment
  # above for why) instead runs `uv run pytest` from $ROOT with the full
  # relative path as the testpath argument, so `uv run` resolves the
  # workspace venv while pytest still finds the suite's own pyproject.toml.
  # shellcheck disable=SC2086  # PYTEST_EXTRA_ARGS intentionally word-splits
  # `set -e` must not abort the loop on a non-zero pytest exit — we want to run
  # every suite and summarise at the end, so the status is captured explicitly.
  # DESIGN: pytest-randomly disabled below (Plan 059 / §D-randomly). It is
  # installed via the root `test` group and so is active in every pytest run;
  # unit runs keep it on (scripts/unit_tests.sh), integration/chaos runs opt out.
  #   ✅ Chaos modules sequence container restarts within one module
  #      (`*_container_chaos` is module-scoped) — a shuffle would reorder them.
  #   ✅ A reseeded `random` could make two tests' random names collide on a
  #      SHARED session container (the namespacing rule assumes uuid4, which
  #      reads os.urandom and is unaffected — a test using `random` would not be).
  #   ❌ Order-dependence among integration tests stays unaudited (out of
  #      scope: reproducing it needs Docker).
  status=0
  if [[ "$suite_run_from" == "root" ]]; then
    (cd "$ROOT" && VARCO_RUN_INTEGRATION=1 uv run pytest \
        "$suite_dir/$suite_testpath/" \
        -m "$MARKER_EXPR" \
        -p no:randomly \
        -v \
        ${PYTEST_EXTRA_ARGS:-}) || status=$?
  else
    (cd "$ROOT/$suite_dir" && VARCO_RUN_INTEGRATION=1 uv run pytest \
        "$suite_testpath/" \
        -m "$MARKER_EXPR" \
        -p no:randomly \
        -v \
        ${PYTEST_EXTRA_ARGS:-}) || status=$?
  fi

  # DESIGN: pytest exit code 5 (EXIT_NOTESTSCOLLECTED) means "this suite has
  # no @pytest.mark.integration test", which is not a failure — several
  # workspace members legitimately have none yet.
  #   ✅ A suite that later GAINS an integration test is picked up
  #      automatically, with no edit to ALL_INTEGRATION_PACKAGES/EXTRA_SUITES.
  #   ✅ A genuinely failing suite still exits 1 and is still reported failed.
  #   ❌ A test file whose marker is accidentally deleted degrades silently to
  #      "no tests" instead of failing loudly — mitigated by printing the
  #      skipped suites in the summary so the drift stays visible.
  if [[ $status -eq 0 ]]; then
    PASSED_SUITES+=("$suite_label")
    echo -e "${GREEN}✔  $suite_label passed${RESET}\n"
  elif [[ $status -eq 5 ]]; then
    SKIPPED_SUITES+=("$suite_label")
    echo -e "${YELLOW}○  $suite_label has no integration tests — skipped${RESET}\n"
  else
    FAILED_SUITES+=("$suite_label")
    echo -e "${RED}✘  $suite_label FAILED${RESET}\n"
  fi
done

# ── Summary ───────────────────────────────────────────────────────────────────
echo -e "${BOLD}── Summary ────────────────────────────────────────────────────────────────${RESET}"
echo -e "${BOLD}Marker expression:${RESET} ${MARKER_EXPR}"
if [[ "$MARKER_EXPR" == "integration and not chaos" ]]; then
  echo -e "  ${YELLOW}chaos tests excluded — run 'make chaos-test'${RESET}"
fi
for suite_label in "${PASSED_SUITES[@]+"${PASSED_SUITES[@]}"}"; do
  echo -e "  ${GREEN}✔  $suite_label${RESET}"
done
for suite_label in "${SKIPPED_SUITES[@]+"${SKIPPED_SUITES[@]}"}"; do
  echo -e "  ${YELLOW}○  $suite_label (no integration tests)${RESET}"
done
for suite_label in "${FAILED_SUITES[@]+"${FAILED_SUITES[@]}"}"; do
  echo -e "  ${RED}✘  $suite_label${RESET}"
done

if [[ ${#OVERRIDES_FOUND[@]} -gt 0 ]]; then
  echo -e "\n${YELLOW}⚠ This run used ${#OVERRIDES_FOUND[@]} override(s) — NOT a clean-room run:${RESET}"
  for override in "${OVERRIDES_FOUND[@]}"; do
    echo -e "  ${YELLOW}⚠ ${override}${RESET}"
  done
fi

if [[ ${#FAILED_SUITES[@]} -gt 0 ]]; then
  echo -e "\n${RED}${BOLD}${#FAILED_SUITES[@]} suite(s) failed.${RESET}" >&2
  exit 1
fi

echo -e "\n${GREEN}${BOLD}All integration tests passed.${RESET}"
