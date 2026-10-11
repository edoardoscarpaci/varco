#!/usr/bin/env bash
# unit_tests.sh — Run unit tests for one package or all workspace members,
# accumulating pass/fail/skip across all of them instead of aborting on the
# first red package (§RL-5-parity, plans/017-ci-green-workflows-and-lint-type-gates.md),
# in two non-overlapping lanes (Plan 059 / TEST-1, plans/059-faster-unit-tests.md):
#
#   Phase X — xdist lane: the big suites, one at a time, each under
#             `pytest -n W` with the whole machine, LIVE output (tee'd to a log).
#   Phase P — pool lane: every other suite, up to J concurrently, no `-n`,
#             each one's output buffered to its own log file and printed whole,
#             as one block, the moment that suite finishes.
#
# `Makefile:test`'s previous `foreach ... || exit 1` loop aborted on the first
# failing package, hiding every package after it — exactly the failure-isolation
# property §RL-5-shape depends on to justify NOT fanning out per-package CI jobs.
#
# The package list is derived from [tool.uv.workspace] members via
# scripts/packages.sh — single source of truth, Plan 020 / RL-18. Do not
# hand-edit a package list here; edit the workspace members instead. The lane
# a suite runs in is derived too (its pyproject + its test-file count), never
# a hand list, so a new package can never be dropped — at worst it lands in
# the pool lane, which is always correct.
#
# Usage:
#   scripts/unit_tests.sh                  # run all ten packages + example
#   scripts/unit_tests.sh varco_redis      # run one package (foreground, live)
#   scripts/unit_tests.sh varco_kafka varco_redis  # run specific packages
#   scripts/unit_tests.sh --plan [PKG...]  # print the lane plan and exit 0
#                                          # (before any `uv sync`)
#   make test                              # same as the bare invocation
#   make test PKG=varco_redis              # same as passing one package
#
# Environment (`VARCO_UNIT_*`, deliberately NOT `VARCO_TEST_*` — that prefix is
# the `VARCO_TEST_<SERVICE>_URL` override namespace integration_tests.sh scans):
#
#   VARCO_UNIT_WORKERS         `-n` for the xdist lane (default: `nproc`).
#                              0 or 1 = no xdist anywhere.
#   VARCO_UNIT_JOBS            pool width (default: `nproc`). 1 = sequential,
#                              foreground, live — the pre-Plan-059 order.
#   VARCO_UNIT_XDIST_MIN_FILES non-integration test-file count at which a suite
#                              joins the xdist lane (default: 125, Plan 059
#                              Step 6 calibration).
#   VARCO_UNIT_DIST            xdist `--dist` mode (default: worksteal).
#   VARCO_UNIT_SEED            `--randomly-seed` for every suite (default:
#                              random, printed in the summary).
#   VARCO_UNIT_LOG_DIR         per-suite logs (default: a fresh `mktemp -d`,
#                              printed; never inside the repo).
#   VARCO_UNIT_SYNC            1 (default) = one up-front `uv sync`; 0 = skip it.
#   VARCO_UNIT_PYTEST          pytest command prefix (default:
#                              `uv run --no-sync pytest`), word-split.
#   PYTEST_EXTRA_ARGS          passed verbatim, LAST, to every pytest
#                              invocation — so a `-n0` in it overrides the
#                              script's `-n` (argparse: last wins).
#
# ⚠️ `-s` and `--pdb` need `VARCO_UNIT_WORKERS=0` (xdist neither relays worker
# stdout nor supports `--pdb`), and `VARCO_UNIT_JOBS=1` (or a single PKG) so the
# suite runs in the foreground with the terminal's stdin:
#   VARCO_UNIT_WORKERS=0 VARCO_UNIT_JOBS=1 make test   # fully serial, live
# Exact replay of an order-dependent failure: the printed seed plus
# `VARCO_UNIT_WORKERS=0` (xdist's distribution is timing-dependent).
#
# DESIGN: two non-overlapping phases (xdist inside the big suites, then a
# bounded pool for the rest) over the alternatives:
#   ✅ xdist attacks the critical path (varco_core) and is the only lever that
#      speeds up `make test PKG=varco_core` — the inner dev loop.
#   ✅ Small suites are dominated by interpreter + collection start-up, which
#      xdist would multiply per worker; running them concurrently hides it.
#   ✅ The phases never overlap, so at most max(W, J) pytest processes are busy —
#      no oversubscription, hence no load-induced timing flakes.
#   ❌ Pool output is buffered: a hung pool suite is silent except for the 60 s
#      heartbeat (CI's `timeout-minutes` bounds it).
#   ❌ A SIGTERM during a foreground (xdist / single-suite) run is handled only
#      after that suite ends — bash defers traps while a foreground child runs.
#      Ctrl-C is immediate (the terminal signals the whole process group).
#   Rejected: suites in parallel only (floor = varco_core's serial wall, no gain
#   for `make test PKG=x`); xdist in every suite AND all suites concurrently
#   (up to 11 × W processes); xdist via `addopts` (integration runs would boot
#   W× session containers, `-s`/`--pdb` broken by default).
#
# Status is decided ONLY by each suite's exit code (0 → passed, 5 → no tests,
# anything else → failed); the per-suite counts shown in the summary are parsed
# from each log's final pytest line for display only.
#
# This script does NOT select the integration marker — integration tests are
# opt-in (guarded by VARCO_RUN_INTEGRATION, see varco_kafka/tests/conftest.py)
# and already skip themselves when that guard is absent. The "not integration"
# marker expression is passed explicitly anyway, belt-and-braces, so a marker
# misconfiguration can never accidentally pull a slow/Docker-requiring test
# into this suite.
#
# Exit codes:
#   0   — all selected suites passed (or had no tests to collect)
#   1   — one or more suites failed, or an unknown package was named
#   130 — interrupted (INT/TERM); every child pytest is killed first

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

# ── Arguments ─────────────────────────────────────────────────────────────────
PLAN_ONLY=0
ARGS=()
for arg in "$@"; do
  if [[ "$arg" == "--plan" ]]; then
    PLAN_ONLY=1
  else
    ARGS+=("$arg")
  fi
done

# ── Knobs ─────────────────────────────────────────────────────────────────────
_nproc() {
  nproc 2>/dev/null || getconf _NPROCESSORS_ONLN 2>/dev/null || echo 1
}
_require_int() {  # name value
  if ! [[ "$2" =~ ^[0-9]+$ ]]; then
    echo -e "${RED}ERROR: $1 must be a non-negative integer, got '$2'${RESET}" >&2
    exit 1
  fi
}
NPROC="$(_nproc)"
WORKERS="${VARCO_UNIT_WORKERS:-$NPROC}"
JOBS="${VARCO_UNIT_JOBS:-$NPROC}"
# Calibrated in Plan 059 Step 6 (design/test-performance/measurements/
# unit-test-timing.md): xdist only beat 0.8 × serial for varco_core (282
# files) and varco_fastapi (129); varco_sa (75) broke even, smaller suites
# got slower. 129 rounded down to a multiple of 5.
MIN_FILES="${VARCO_UNIT_XDIST_MIN_FILES:-125}"
DIST="${VARCO_UNIT_DIST:-worksteal}"
# Two RANDOMs → a 30-bit seed; any non-negative integer is a valid seed.
SEED="${VARCO_UNIT_SEED:-$(( (RANDOM << 15) | RANDOM ))}"
_require_int VARCO_UNIT_WORKERS "$WORKERS"
_require_int VARCO_UNIT_JOBS "$JOBS"
_require_int VARCO_UNIT_XDIST_MIN_FILES "$MIN_FILES"
_require_int VARCO_UNIT_SEED "$SEED"
[[ "$JOBS" -ge 1 ]] || JOBS=1
read -r -a PYTEST_CMD <<< "${VARCO_UNIT_PYTEST:-uv run --no-sync pytest}"

# ── All workspace member packages ───────────────────────────────────────────
mapfile -t ALL_PACKAGES < <("$ROOT/scripts/packages.sh")

# ── Extra suites — the example app is not a workspace member (see
# scripts/integration_tests.sh's DESIGN comment for why run_from="root" is
# required for it: `uv run` from inside examples/00-full-stack-post-api
# resolves a separate, non-workspace environment with no varco_core installed).
EXTRA_SUITES=("examples/00-full-stack-post-api:example/tests")

# ── Determine which suites to run — "dir:testpath:label:run_from" ────────────
SUITES=()
if [[ ${#ARGS[@]} -eq 0 ]]; then
  for pkg in "${ALL_PACKAGES[@]}"; do
    SUITES+=("$pkg:tests:$pkg:cd")
  done
  for extra in "${EXTRA_SUITES[@]}"; do
    extra_dir="${extra%%:*}"
    extra_testpath="${extra#*:}"
    SUITES+=("$extra_dir:$extra_testpath:$extra_dir:root")
  done
else
  for pkg in "${ARGS[@]}"; do
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

# ── Classify every suite into a lane ─────────────────────────────────────────
# serial-only ← its pyproject sets asyncio_default_test_loop_scope = "session":
#   those suites drive a background server on one session-wide loop, and there
#   is no documented safe pattern for async session fixtures under xdist
#   (research brief 020 §Evidence gaps). They always go to the pool.
# files ← non-integration, non-chaos test files (those are deselected anyway).
declare -A S_DIR S_TESTPATH S_RUN_FROM S_FILES S_LANE
XDIST_LABELS=()
POOL_LABELS=()
_by_files_desc() {  # label... → labels sorted by S_FILES descending (stable)
  local label
  for label in "$@"; do
    printf '%s\t%s\n' "${S_FILES[$label]}" "$label"
  done | sort -s -t $'\t' -k1,1nr | cut -f2
}
for suite in "${SUITES[@]}"; do
  suite_dir="${suite%%:*}"
  rest="${suite#*:}"
  suite_testpath="${rest%%:*}"
  rest2="${rest#*:}"
  label="${rest2%%:*}"
  S_DIR[$label]="$suite_dir"
  S_TESTPATH[$label]="$suite_testpath"
  S_RUN_FROM[$label]="${rest2#*:}"
  S_FILES[$label]="$(find "$ROOT/$suite_dir/$suite_testpath" -name 'test_*.py' \
      ! -name '*integration*' ! -name '*chaos*' | wc -l | tr -d ' ')"
  serial_only=0
  if grep -Eq '^[[:space:]]*asyncio_default_test_loop_scope[[:space:]]*=[[:space:]]*"session"' \
      "$ROOT/$suite_dir/pyproject.toml" 2>/dev/null; then
    serial_only=1
  fi
  if [[ "$WORKERS" -ge 2 && $serial_only -eq 0 && "${S_FILES[$label]}" -ge "$MIN_FILES" ]]; then
    S_LANE[$label]="xdist"
  else
    S_LANE[$label]="pool"
  fi
done
for suite in "${SUITES[@]}"; do
  rest2="${suite#*:}"; rest2="${rest2#*:}"; label="${rest2%%:*}"
  if [[ "${S_LANE[$label]}" == "xdist" ]]; then XDIST_LABELS+=("$label"); else POOL_LABELS+=("$label"); fi
done
# Largest first in both lanes: the xdist lane front-loads the critical path;
# the pool starts its longest suites first so its tail stays short.
if [[ ${#XDIST_LABELS[@]} -gt 0 ]]; then mapfile -t XDIST_LABELS < <(_by_files_desc "${XDIST_LABELS[@]}"); fi
if [[ ${#POOL_LABELS[@]} -gt 0 ]]; then mapfile -t POOL_LABELS < <(_by_files_desc "${POOL_LABELS[@]}"); fi

_print_plan() {
  local label
  echo "seed=$SEED"
  echo "workers=$WORKERS"
  echo "jobs=$JOBS"
  echo "dist=$DIST"
  echo "xdist_min_files=$MIN_FILES"
  for label in "${XDIST_LABELS[@]+"${XDIST_LABELS[@]}"}"; do
    echo "suite=$label lane=xdist workers=$WORKERS files=${S_FILES[$label]}"
  done
  for label in "${POOL_LABELS[@]+"${POOL_LABELS[@]}"}"; do
    echo "suite=$label lane=pool workers=1 files=${S_FILES[$label]}"
  done
}

if [[ $PLAN_ONLY -eq 1 ]]; then
  _print_plan
  exit 0
fi

# ── Sync the workspace ONCE, up front ──────────────────────────────────────────
# RL-21: each suite below runs its own `uv run`, and `uv run` will re-resolve
# and re-sync the environment if it thinks anything is stale.  Eleven of those,
# interleaved with pytest collection, produced intermittent false failures on
# trees that were green on the very next run (seen on both the examples suite
# and varco_fastapi).  A false red here is expensive: this script is CI's `unit`
# job, and `all-green` — the single required status check — sits downstream.
#
# So: sync deliberately once, then forbid every per-suite `uv run` from touching
# the environment again via --no-sync.  A suite now either runs against a fully
# prepared venv or fails loudly at this step, instead of racing a sync. With
# the pool lane this matters twice over: concurrent syncs would race each other.
#
#   ✅ One sync instead of up to eleven; removes the race entirely rather than
#      papering over it with a retry (which would hide real failures).
#   ✅ Faster — the per-suite staleness check is skipped.
#   ❌ A dependency changed mid-run is not picked up. Correct: a unit run should
#      test one fixed environment, not a moving one.
if [[ "${VARCO_UNIT_SYNC:-1}" != "0" ]]; then
  echo -e "${BOLD}── Syncing workspace ──────────────────────────────────────────────────────${RESET}"
  uv sync --all-packages --all-extras
  echo
fi

LOG_DIR="${VARCO_UNIT_LOG_DIR:-$(mktemp -d "${TMPDIR:-/tmp}/varco-unit-XXXXXX")}"
mkdir -p "$LOG_DIR"

# ── Per-suite bookkeeping ─────────────────────────────────────────────────────
declare -A S_STATUS S_WALL
POOL_PIDS=()      # pids (= process-group ids) of running pool suites
declare -A PID_LABEL PID_START

_now_us() {  # wall clock in microseconds, no subprocess
  local t="${EPOCHREALTIME/[.,]/}"
  echo "$t"
}
_fmt_secs() {  # microseconds → "12.3"
  local us="$1"
  printf '%d.%d' $(( us / 1000000 )) $(( (us % 1000000) / 100000 ))
}
_log_path() {  # label → log file (slashes flattened)
  echo "$LOG_DIR/${1//\//_}.log"
}

_run_pytest() {  # label [color] — runs the suite's pytest in the caller's foreground
  local label="$1" color="${2:-no}"
  local dir="${S_DIR[$label]}" testpath="${S_TESTPATH[$label]}"
  local -a xdist_args=()
  if [[ "${S_LANE[$label]}" == "xdist" ]]; then
    xdist_args=(-n "$WORKERS" --dist "$DIST")
  fi
  # A live run is piped through `tee`, so pytest would see a pipe and drop
  # colour; ask for it explicitly when the script's own stdout is a terminal.
  if [[ "$color" == "yes" ]]; then
    xdist_args+=(--color=yes)
  fi
  # shellcheck disable=SC2086  # PYTEST_EXTRA_ARGS intentionally word-splits
  if [[ "${S_RUN_FROM[$label]}" == "root" ]]; then
    cd "$ROOT" && "${PYTEST_CMD[@]}" "$dir/$testpath/" \
        -m "not integration" -v --randomly-seed="$SEED" \
        "${xdist_args[@]+"${xdist_args[@]}"}" ${PYTEST_EXTRA_ARGS:-}
  else
    cd "$ROOT/$dir" && "${PYTEST_CMD[@]}" "$testpath/" \
        -m "not integration" -v --randomly-seed="$SEED" \
        "${xdist_args[@]+"${xdist_args[@]}"}" ${PYTEST_EXTRA_ARGS:-}
  fi
}

_record() {  # label status start_us
  local label="$1" status="$2" start="$3"
  S_STATUS[$label]="$status"
  S_WALL[$label]="$(_fmt_secs $(( $(_now_us) - start )))"
  # pytest exit code 5 (EXIT_NOTESTSCOLLECTED) means "no tests matched the
  # filter" — not a failure (mirrors scripts/integration_tests.sh).
  if [[ $status -eq 0 ]]; then
    echo -e "${GREEN}✔  $label passed (${S_WALL[$label]}s)${RESET}\n"
  elif [[ $status -eq 5 ]]; then
    echo -e "${YELLOW}○  $label has no tests — skipped${RESET}\n"
  else
    echo -e "${RED}✘  $label FAILED (exit $status)${RESET}\n"
  fi
}

_run_foreground() {  # label — live output, tee'd to its log
  local label="$1" start status=0
  echo -e "${CYAN}▶  $label  [lane=${S_LANE[$label]}]${RESET}"
  start="$(_now_us)"
  # `set -e` must not abort on a red suite; with `pipefail` the pipeline's
  # status is pytest's (tee succeeds), captured explicitly.
  local color="no"
  [[ -t 1 ]] && color="yes"
  ( _run_pytest "$label" "$color" ) 2>&1 | tee "$(_log_path "$label")" || status=$?
  _record "$label" "$status" "$start"
}

_start_pool_suite() {  # label — background, own process group, output to its log
  local label="$1" pid
  echo -e "${CYAN}▶  $label  [lane=pool, log: $(_log_path "$label")]${RESET}"
  # `set -m` only around the launch: the job gets its OWN process group, so
  # the INT/TERM trap can kill pytest *and* its children with one
  # `kill -- -pgid` (Ctrl-C must not orphan pytest processes). stdin is
  # /dev/null — a background group reading the tty would be stopped.
  set -m
  ( _run_pytest "$label" ) </dev/null >"$(_log_path "$label")" 2>&1 &
  pid=$!
  set +m
  POOL_PIDS+=("$pid")
  PID_LABEL[$pid]="$label"
  PID_START[$pid]="$(_now_us)"
}

_finish_pool_suite() {  # pid — reap, print its log as one block, record
  local pid="$1" status=0 label="${PID_LABEL[$1]}"
  wait "$pid" || status=$?
  echo -e "${BOLD}── $label (pool) ───────────────────────────────────────────${RESET}"
  cat "$(_log_path "$label")"
  _record "$label" "$status" "${PID_START[$pid]}"
}

_on_interrupt() {
  trap '' INT TERM
  local pid
  echo -e "\n${RED}${BOLD}Interrupted — stopping every running suite.${RESET}" >&2
  for pid in "${POOL_PIDS[@]+"${POOL_PIDS[@]}"}"; do
    kill -TERM -- "-$pid" 2>/dev/null || true
  done
  wait 2>/dev/null || true
  exit 130
}
trap _on_interrupt INT TERM

# ── Phase X — xdist lane: one suite at a time, the whole machine, live ───────
echo -e "\n${BOLD}── Running unit tests (seed=$SEED workers=$WORKERS jobs=$JOBS logs=$LOG_DIR) ──${RESET}\n"
for label in "${XDIST_LABELS[@]+"${XDIST_LABELS[@]}"}"; do
  _run_foreground "$label"
done

# ── Phase P — pool lane: ≤ JOBS suites concurrently ──────────────────────────
if [[ ${#POOL_LABELS[@]} -gt 0 ]]; then
  if [[ "$JOBS" -le 1 || ${#POOL_LABELS[@]} -eq 1 ]]; then
    # Sequential: run in the foreground so output is live and stdin is the
    # terminal's (`-s`/`--pdb` with VARCO_UNIT_WORKERS=0 VARCO_UNIT_JOBS=1).
    for label in "${POOL_LABELS[@]}"; do
      _run_foreground "$label"
    done
  else
    # A polling pool rather than `wait -n`: `kill -0` + `wait "$pid" ||
    # status=$?` is `set -e`-safe and needs no bash-5.1 `wait -p`.
    next=0
    last_beat="$SECONDS"
    while [[ $next -lt ${#POOL_LABELS[@]} || ${#POOL_PIDS[@]} -gt 0 ]]; do
      while [[ $next -lt ${#POOL_LABELS[@]} && ${#POOL_PIDS[@]} -lt $JOBS ]]; do
        _start_pool_suite "${POOL_LABELS[$next]}"
        next=$(( next + 1 ))
      done
      still=()
      for pid in "${POOL_PIDS[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then
          still+=("$pid")
        else
          _finish_pool_suite "$pid"
        fi
      done
      POOL_PIDS=("${still[@]+"${still[@]}"}")
      if [[ ${#POOL_PIDS[@]} -gt 0 ]]; then
        if (( SECONDS - last_beat >= 60 )); then
          running=()
          for pid in "${POOL_PIDS[@]}"; do running+=("${PID_LABEL[$pid]}"); done
          echo -e "${YELLOW}…  still running: ${running[*]}${RESET}"
          last_beat="$SECONDS"
        fi
        sleep 0.2
      fi
    done
  fi
fi

# ── Summary ───────────────────────────────────────────────────────────────────
_counts() {  # label → the final pytest summary line's body, or "?"
  local line
  # ANSI colour codes are stripped first (a live, tty run asks for colour).
  line="$(sed -E $'s/\x1b\\[[0-9;]*m//g' "$(_log_path "$1")" 2>/dev/null \
      | grep -E '^=+ .+ in [0-9.]+s' | tail -n 1 || true)"
  if [[ -z "$line" ]]; then
    echo "?"
    return
  fi
  line="$(sed -E 's/^=+ //; s/ =+$//; s/ in [0-9.]+s.*$//' <<< "$line")"
  echo "$line"
}

FAILED=0
echo -e "${BOLD}── Summary ────────────────────────────────────────────────────────────────${RESET}"
for label in "${XDIST_LABELS[@]+"${XDIST_LABELS[@]}"}" "${POOL_LABELS[@]+"${POOL_LABELS[@]}"}"; do
  status="${S_STATUS[$label]}"
  detail="lane=${S_LANE[$label]}  ${S_WALL[$label]}s  [$(_counts "$label")]"
  if [[ $status -eq 0 ]]; then
    echo -e "  ${GREEN}✔  $label  passed  $detail${RESET}"
  elif [[ $status -eq 5 ]]; then
    echo -e "  ${YELLOW}○  $label  no tests  $detail${RESET}"
  else
    echo -e "  ${RED}✘  $label  FAILED (exit $status)  $detail${RESET}"
    FAILED=$(( FAILED + 1 ))
  fi
done
echo "  seed=$SEED  workers=$WORKERS  jobs=$JOBS  dist=$DIST  logs=$LOG_DIR"
echo "  reproduce: VARCO_UNIT_SEED=$SEED (exact replay: also VARCO_UNIT_WORKERS=0)"

if [[ $FAILED -gt 0 ]]; then
  echo -e "\n${RED}${BOLD}$FAILED suite(s) failed.${RESET}" >&2
  exit 1
fi

echo -e "\n${GREEN}${BOLD}All unit tests passed.${RESET}"
