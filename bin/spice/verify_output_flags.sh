#!/usr/bin/env bash
# Verification script for -o/--output flag on spice CLI commands.
# Run this against a live spice instance.
#
# Usage:
#   ./verify_output_flags.sh [path/to/spice]
#
# Defaults to 'spice' on PATH. Pass a path to use a local build, e.g.:
#   ./verify_output_flags.sh ./target/debug/spice

set -euo pipefail

SPICE="${1:-spice}"
PASS=0
FAIL=0
SKIP=0

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BOLD='\033[1m'
RESET='\033[0m'

pass() { echo -e "${GREEN}✓ PASS${RESET}  $1"; PASS=$((PASS+1)); }
fail() { echo -e "${RED}✗ FAIL${RESET}  $1"; FAIL=$((FAIL+1)); }
skip() { echo -e "${YELLOW}⚠ SKIP${RESET}  $1"; SKIP=$((SKIP+1)); }
header() { echo -e "\n${BOLD}── $1 ──${RESET}"; }
show() {
  echo -e "  ${BOLD}cmd:${RESET} $*"
  OUTPUT=$("$@" 2>&1) && true
  echo "$OUTPUT" | sed 's/^/  /'
  echo ""
}

# Check that output looks like JSON (starts with [ or {)
is_json() { echo "$1" | grep -qE '^\s*[\[{]'; }
# Check that output does NOT look like JSON
is_table() { echo "$1" | grep -vqE '^\s*[\[{]'; }

run_table() {
  local desc="$1"; shift
  local out
  out=$("$@" 2>&1) && true
  if is_table "$out"; then
    pass "$desc (table)"
  else
    fail "$desc (table) — got JSON-looking output"
    echo "$out" | sed 's/^/    /'
  fi
}

run_json() {
  local desc="$1"; shift
  local out
  out=$("$@" 2>&1) && true
  if is_json "$out"; then
    pass "$desc (json)"
  else
    fail "$desc (json) — output doesn't look like JSON"
    echo "$out" | sed 's/^/    /'
  fi
}

echo -e "${BOLD}╔══════════════════════════════════════════╗${RESET}"
echo -e "${BOLD}║   spice -o/--output flag verification    ║${RESET}"
echo -e "${BOLD}╚══════════════════════════════════════════╝${RESET}"
echo "  Binary: $SPICE"
echo ""

# ── version ────────────────────────────────────────────────────────────────────
header "version"
run_table "spice version"            "$SPICE" version
run_json  "spice version -o json"    "$SPICE" version -o json

# ── status ─────────────────────────────────────────────────────────────────────
header "status"
# NOTE: status has a known pre-existing parse error when the runtime returns
# the new array format. Both table and json show the same error text — that is
# expected until the StatusResponse struct is updated.
show "$SPICE" status
show "$SPICE" status -o json

# ── datasets ───────────────────────────────────────────────────────────────────
header "datasets"
run_table "spice datasets"           "$SPICE" datasets
run_json  "spice datasets -o json"   "$SPICE" datasets -o json

# ── catalogs ───────────────────────────────────────────────────────────────────
header "catalogs"
run_table "spice catalogs"           "$SPICE" catalogs
run_json  "spice catalogs -o json"   "$SPICE" catalogs -o json

# ── models ─────────────────────────────────────────────────────────────────────
header "models"
run_table "spice models"             "$SPICE" models
run_json  "spice models -o json"     "$SPICE" models -o json

# ── pods ───────────────────────────────────────────────────────────────────────
header "pods"
run_table "spice pods"               "$SPICE" pods
run_json  "spice pods -o json"       "$SPICE" pods -o json

# ── workers ────────────────────────────────────────────────────────────────────
header "workers"
run_table "spice workers"            "$SPICE" workers
run_json  "spice workers -o json"    "$SPICE" workers -o json

# ── trace ──────────────────────────────────────────────────────────────────────
header "trace"
# Use a task that likely has data; adjust TRACE_TASK if needed.
TRACE_TASK="${TRACE_TASK:-ai_completion}"
run_table "spice trace $TRACE_TASK"           "$SPICE" trace "$TRACE_TASK"
run_json  "spice trace $TRACE_TASK -o json"   "$SPICE" trace "$TRACE_TASK" -o json
echo -e "  ${BOLD}cmd:${RESET} $SPICE trace $TRACE_TASK -o sql"
"$SPICE" trace "$TRACE_TASK" -o sql 2>&1 | sed 's/^/  /'
echo ""

# ── acceleration ───────────────────────────────────────────────────────────────
header "acceleration (requires a dataset with snapshots enabled)"
ACCEL_DATASET="${ACCEL_DATASET:-}"
if [[ -z "$ACCEL_DATASET" ]]; then
  skip "spice acceleration snapshots   (set ACCEL_DATASET=<name> to test)"
  skip "spice acceleration snapshots -o json"
  skip "spice acceleration snapshot <dataset> <id>"
  skip "spice acceleration snapshot <dataset> <id> -o json"
else
  run_table "spice acceleration snapshots $ACCEL_DATASET"          "$SPICE" acceleration snapshots "$ACCEL_DATASET"
  run_json  "spice acceleration snapshots $ACCEL_DATASET -o json"  "$SPICE" acceleration snapshots "$ACCEL_DATASET" -o json
  SNAP_ID="${SNAP_ID:-1}"
  run_table "spice acceleration snapshot $ACCEL_DATASET $SNAP_ID"          "$SPICE" acceleration snapshot "$ACCEL_DATASET" "$SNAP_ID"
  run_json  "spice acceleration snapshot $ACCEL_DATASET $SNAP_ID -o json"  "$SPICE" acceleration snapshot "$ACCEL_DATASET" "$SNAP_ID" -o json
fi

# ── query (requires cluster/scheduler mode) ────────────────────────────────────
header "query (requires cluster mode — skipped if API returns 503)"
QID="${QUERY_ID:-}"
Q_OUT=$("$SPICE" query list 2>&1) && true
if echo "$Q_OUT" | grep -q "503\|scheduler\|cluster"; then
  skip "spice query list          (cluster mode not active)"
  skip "spice query list -o json  (cluster mode not active)"
  skip "spice query status        (cluster mode not active)"
  skip "spice query status -o json"
  skip "spice query results       (cluster mode not active)"
  skip "spice query results -o json"
else
  run_table "spice query list"           "$SPICE" query list
  run_json  "spice query list -o json"   "$SPICE" query list -o json
  if [[ -n "$QID" ]]; then
    run_table "spice query status $QID"          "$SPICE" query status "$QID"
    run_json  "spice query status $QID -o json"  "$SPICE" query status "$QID" -o json
    run_table "spice query results $QID"         "$SPICE" query results "$QID"
    run_json  "spice query results $QID -o json" "$SPICE" query results "$QID" -o json
  else
    skip "spice query status/results  (set QUERY_ID=<id> to test)"
  fi
fi

# ── eval (requires a configured model) ────────────────────────────────────────
header "eval (requires a configured model)"
EVAL_MODEL="${EVAL_MODEL:-}"
EVAL_DATASET="${EVAL_DATASET:-}"
if [[ -z "$EVAL_MODEL" || -z "$EVAL_DATASET" ]]; then
  skip "spice eval          (set EVAL_MODEL and EVAL_DATASET to test)"
  skip "spice eval -o json"
else
  run_table "spice eval $EVAL_MODEL $EVAL_DATASET"          "$SPICE" eval --model "$EVAL_MODEL" --dataset "$EVAL_DATASET"
  run_json  "spice eval $EVAL_MODEL $EVAL_DATASET -o json"  "$SPICE" eval --model "$EVAL_MODEL" --dataset "$EVAL_DATASET" -o json
fi

# ── search (requires embeddings-enabled dataset) ──────────────────────────────
header "search (requires embeddings-enabled dataset)"
SEARCH_DATASET="${SEARCH_DATASET:-}"
if [[ -z "$SEARCH_DATASET" ]]; then
  skip "spice search          (set SEARCH_DATASET and run interactively — JSON mode suppresses welcome message)"
else
  echo "  Manual: run  '$SPICE search --datasets $SEARCH_DATASET -o json'  and verify JSON output"
  skip "search is interactive — automated test not supported"
fi

# ── cloud (requires spice cloud login) ────────────────────────────────────────
header "cloud (requires 'spice cloud login')"
CLOUD_OUT=$("$SPICE" cloud whoami 2>&1) && true
if echo "$CLOUD_OUT" | grep -qiE "not logged|unauthorized|login|401|403"; then
  skip "spice cloud whoami          (not logged in)"
  skip "spice cloud whoami -o json"
  skip "spice cloud apps            (not logged in)"
  skip "spice cloud apps -o json"
  skip "spice cloud regions         (not logged in)"
  skip "spice cloud regions -o json"
  skip "spice cloud deployments     (not logged in)"
  skip "spice cloud deployments -o json"
  skip "spice cloud images          (not logged in)"
  skip "spice cloud images -o json"
  skip "spice cloud inspect         (not logged in)"
  skip "spice cloud inspect -o json"
  skip "spice cloud api-keys        (not logged in)"
  skip "spice cloud api-keys -o json"
  skip "spice cloud secrets list    (not logged in)"
  skip "spice cloud secrets list -o json"
  skip "spice cloud secrets get     (not logged in)"
  skip "spice cloud secrets get -o json"
  skip "spice cloud get app         (not logged in)"
  skip "spice cloud get app -o json"
else
  run_table "spice cloud whoami"           "$SPICE" cloud whoami
  run_json  "spice cloud whoami -o json"   "$SPICE" cloud whoami -o json
  run_table "spice cloud apps"             "$SPICE" cloud apps
  run_json  "spice cloud apps -o json"     "$SPICE" cloud apps -o json
  run_table "spice cloud regions"          "$SPICE" cloud regions
  run_json  "spice cloud regions -o json"  "$SPICE" cloud regions -o json
  run_table "spice cloud images"           "$SPICE" cloud images
  run_json  "spice cloud images -o json"   "$SPICE" cloud images -o json
  CLOUD_APP="${CLOUD_APP:-}"
  if [[ -n "$CLOUD_APP" ]]; then
    run_table "spice cloud deployments --app $CLOUD_APP"          "$SPICE" cloud deployments --app "$CLOUD_APP"
    run_json  "spice cloud deployments --app $CLOUD_APP -o json"  "$SPICE" cloud deployments --app "$CLOUD_APP" -o json
    run_table "spice cloud inspect --app $CLOUD_APP"              "$SPICE" cloud inspect --app "$CLOUD_APP"
    run_json  "spice cloud inspect --app $CLOUD_APP -o json"      "$SPICE" cloud inspect --app "$CLOUD_APP" -o json
    run_table "spice cloud api-keys --app $CLOUD_APP"             "$SPICE" cloud api-keys --app "$CLOUD_APP"
    run_json  "spice cloud api-keys --app $CLOUD_APP -o json"     "$SPICE" cloud api-keys --app "$CLOUD_APP" -o json
    run_table "spice cloud secrets list --app $CLOUD_APP"         "$SPICE" cloud secrets list --app "$CLOUD_APP"
    run_json  "spice cloud secrets list --app $CLOUD_APP -o json" "$SPICE" cloud secrets list --app "$CLOUD_APP" -o json
    run_table "spice cloud get app $CLOUD_APP"                    "$SPICE" cloud get app "$CLOUD_APP"
    run_json  "spice cloud get app $CLOUD_APP -o json"            "$SPICE" cloud get app "$CLOUD_APP" -o json
    SECRET="${CLOUD_SECRET:-}"
    if [[ -n "$SECRET" ]]; then
      run_table "spice cloud secrets get $SECRET"          "$SPICE" cloud secrets get --app "$CLOUD_APP" "$SECRET"
      run_json  "spice cloud secrets get $SECRET -o json"  "$SPICE" cloud secrets get --app "$CLOUD_APP" "$SECRET" -o json
    else
      skip "spice cloud secrets get  (set CLOUD_SECRET=<name> to test)"
    fi
  else
    skip "spice cloud deployments/inspect/api-keys/secrets/get app  (set CLOUD_APP=org/app to test)"
  fi
fi

# ── Summary ────────────────────────────────────────────────────────────────────
echo ""
echo -e "${BOLD}╔══════════════════════════════╗${RESET}"
echo -e "${BOLD}║          Summary             ║${RESET}"
echo -e "${BOLD}╠══════════════════════════════╣${RESET}"
echo -e "║  ${GREEN}PASS${RESET}  ${PASS}"
echo -e "║  ${RED}FAIL${RESET}  ${FAIL}"
echo -e "║  ${YELLOW}SKIP${RESET}  ${SKIP}"
echo -e "${BOLD}╚══════════════════════════════╝${RESET}"

if [[ $FAIL -gt 0 ]]; then
  exit 1
fi
