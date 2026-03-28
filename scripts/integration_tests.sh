#!/usr/bin/env bash
# integration_tests.sh — Run integration tests for one package or all packages.
#
# Tests spin up their own Docker containers via pytest fixtures — no manual
# service setup required.  The only prerequisite is a running Docker daemon.
#
# Usage:
#   scripts/integration_tests.sh                  # run all packages
#   scripts/integration_tests.sh varco_redis       # run one package
#   scripts/integration_tests.sh varco_kafka varco_redis  # run specific packages
#
# Environment:
#   VARCO_RUN_INTEGRATION  — automatically set to "1"; do NOT set it yourself.
#   PYTEST_EXTRA_ARGS      — passed verbatim to every pytest invocation,
#                            e.g. PYTEST_EXTRA_ARGS="-x -s" ./scripts/integration_tests.sh
#
# Exit codes:
#   0 — all selected suites passed
#   1 — one or more suites failed, or Docker is not available

set -euo pipefail

# ── Resolve workspace root regardless of where the script is called from ──────
ROOT="$(cd "$(dirname "$0")/.." && pwd)"

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

# ── Known integration-test packages ───────────────────────────────────────────
# DESIGN: plain array — no per-package host/port config needed since tests
# manage their own containers.
#   ✅ Simple to extend: add the package name here, nothing else.
#   ❌ bash 4+ only (arrays with expansion); not sh-portable.
ALL_INTEGRATION_PACKAGES=("varco_redis" "varco_kafka" "varco_beanie" "varco_memcached")

# ── Determine which packages to test ──────────────────────────────────────────
if [[ $# -eq 0 ]]; then
  SELECTED_PACKAGES=("${ALL_INTEGRATION_PACKAGES[@]}")
else
  SELECTED_PACKAGES=("$@")
fi

# ── Validate package names ─────────────────────────────────────────────────────
for pkg in "${SELECTED_PACKAGES[@]}"; do
  if [[ ! -d "$ROOT/$pkg/tests" ]]; then
    echo -e "${RED}ERROR: '$pkg' does not have a tests/ directory under $ROOT/${RESET}" >&2
    exit 1
  fi
done

# ── Run pytest for each package ───────────────────────────────────────────────
# VARCO_RUN_INTEGRATION=1 activates the integration suite inside each test
# module (they check this env var and skip when absent).  We also pass
# -m integration so pytest's marker filter applies — belt-and-suspenders.
FAILED_PACKAGES=()
PASSED_PACKAGES=()

echo -e "\n${BOLD}── Running integration tests ──────────────────────────────────────────────${RESET}\n"

for pkg in "${SELECTED_PACKAGES[@]}"; do
  echo -e "${CYAN}▶  $pkg${RESET}"
  # DESIGN: cd into each package directory before invoking pytest.
  #   pytest anchors rootdir by walking up from the test path to find pyproject.toml.
  #   Running from the workspace root makes pytest use the workspace pyproject.toml,
  #   which lacks per-package pythonpath/asyncio_mode settings — tests fail to collect.
  #   cd-ing into the package dir makes pytest use the package's own pyproject.toml.
  #   ✅ Correct rootdir → correct config → tests collect and markers work.
  #   ❌ Changes the working directory inside the subprocess — transparent to the parent shell.
  # shellcheck disable=SC2086  # PYTEST_EXTRA_ARGS intentionally word-splits
  if (cd "$ROOT/$pkg" && VARCO_RUN_INTEGRATION=1 uv run pytest \
      tests/ \
      -m integration \
      -v \
      ${PYTEST_EXTRA_ARGS:-}); then
    PASSED_PACKAGES+=("$pkg")
    echo -e "${GREEN}✔  $pkg passed${RESET}\n"
  else
    FAILED_PACKAGES+=("$pkg")
    echo -e "${RED}✘  $pkg FAILED${RESET}\n"
  fi
done

# ── Summary ───────────────────────────────────────────────────────────────────
echo -e "${BOLD}── Summary ────────────────────────────────────────────────────────────────${RESET}"
for pkg in "${PASSED_PACKAGES[@]+"${PASSED_PACKAGES[@]}"}"; do
  echo -e "  ${GREEN}✔  $pkg${RESET}"
done
for pkg in "${FAILED_PACKAGES[@]+"${FAILED_PACKAGES[@]}"}"; do
  echo -e "  ${RED}✘  $pkg${RESET}"
done

if [[ ${#FAILED_PACKAGES[@]} -gt 0 ]]; then
  echo -e "\n${RED}${BOLD}${#FAILED_PACKAGES[@]} package(s) failed.${RESET}" >&2
  exit 1
fi

echo -e "\n${GREEN}${BOLD}All integration tests passed.${RESET}"
