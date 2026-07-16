#!/usr/bin/env bash
# Ensure the CH-benCH source DB `chbench` is ready WITHOUT re-seeding every run,
# by keeping a pristine tablespace snapshot on the long-lived CI MySQL pod and
# restoring from it with a fast local file copy. The physical file work is done
# by the file-agent SIDECAR in the MySQL pod (chbench-mysql chart, sidecar.enabled);
# this script only DRIVES it over the ordinary MySQL connection — it INSERTs a
# command row into `chbench._file_cmd` and polls for completion. MySQL has no
# server-side DB-clone primitive and the runner has no filesystem access to the
# pod, so the sidecar (co-located, sharing the data volume) performs the
# `FLUSH ... FOR EXPORT` + copy (snapshot) and `DISCARD`/copy/`IMPORT` (restore).
#
# Only meaningful on the dedicated, persistent MySQL pod (xlarge runner): its
# node-local data dir and the sidecar's cache both survive between runs (wiped
# together only on pod restart). On an ephemeral per-run docker MySQL there is
# no sidecar and nothing to cache, so the caller gates this step to the pod path
# and the main run seeds normally.
#
# Flow (mirrors chbench_template.sh, but the "copy" is the sidecar's job):
#   fingerprint = sf=<N> driver=<chbench-driver tree hash> mysql<major>
#   HIT  (sidecar reports a cached snapshot with a matching fingerprint):
#        issue a `restore` command and wait. On restore failure, fall back to a
#        full reseed so the run is never blocked by a bad cache.
#   MISS: seed once via `testoperator run htap --prepare-only`, then issue a
#        `snapshot` command so the next run can HIT. `chbench` is already freshly
#        seeded, so no restore is needed on a miss.
# Either way `chbench` ends up seeded, so the caller runs with `--skip-prepare`.
#
# Env (from the workflow step):
#   SCALE_FACTOR (req), TERMINALS (opt), SPICEPOD_PATH (req), SPICED_BIN (req),
#   CHBENCH_MYSQL_HOST/PORT/USER/PASS (req), CHBENCH_MYSQL_DB (opt, default
#   chbench), TESTOP_PREFIX (opt), REPO_ROOT (default: $GITHUB_WORKSPACE) for the
#   chbench-driver fingerprint, CMD_TIMEOUT_SECONDS (opt, default 3600) and
#   SIDECAR_WAIT_SECONDS (opt, default 180) for the poll bounds.
set -euo pipefail

SF="${SCALE_FACTOR:?}"
MYH="${CHBENCH_MYSQL_HOST:?}"; MYP="${CHBENCH_MYSQL_PORT:-3306}"
MYU="${CHBENCH_MYSQL_USER:-bench}"; MYDB="${CHBENCH_MYSQL_DB:-chbench}"
export MYSQL_PWD="${CHBENCH_MYSQL_PASS:-bench}"
REPO_ROOT="${REPO_ROOT:-${GITHUB_WORKSPACE:-$PWD}}"
TESTOP_PREFIX="${TESTOP_PREFIX:-}"
CMD_TIMEOUT="${CMD_TIMEOUT_SECONDS:-3600}"
SIDECAR_WAIT="${SIDECAR_WAIT_SECONDS:-180}"

# The mysql client may not be preinstalled on the runner.
if ! command -v mysql >/dev/null 2>&1; then
  sudo apt-get update -qq && sudo apt-get install -y -qq default-mysql-client || true
fi

# Scalar/one-row query against `chbench` (-N no headers, -B tab-separated).
my() { mysql -h "$MYH" -P "$MYP" -u "$MYU" -D "$MYDB" -N -B -e "$1"; }

mysqlmajor=$(my "SELECT SUBSTRING_INDEX(VERSION(), '.', 1)")
driver=$(git -C "$REPO_ROOT" rev-parse "HEAD:tools/chbench-driver" 2>/dev/null || echo nogit)
fp="sf=${SF} driver=${driver} mysql${mysqlmajor}"
echo "template fingerprint: $fp"

# Wait for the sidecar to have created its control table (it does so shortly
# after the pod starts). Absent after the bound => the sidecar isn't running.
table_exists() {
  local n
  n=$(my "SELECT COUNT(*) FROM information_schema.tables
          WHERE table_schema='${MYDB}' AND table_name='$1'")
  [ "${n:-0}" != "0" ]
}
wait_for_control_table() {
  local waited=0
  until table_exists _file_cmd; do
    if [ "$waited" -ge "$SIDECAR_WAIT" ]; then
      echo "ERROR: chbench._file_cmd not present after ${SIDECAR_WAIT}s — is the MySQL pod's file-agent sidecar enabled (sidecar.enabled)?" >&2
      return 1
    fi
    sleep 3; waited=$((waited + 3))
  done
}

# SQL-escape a single-quoted literal.
esc() { printf '%s' "$1" | sed "s/'/''/g"; }

# Issue a command to the sidecar and wait for it to finish. Echoes the final
# state ("done"/"error"/"timeout") on stdout; the sidecar's message goes to the
# log. INSERT + LAST_INSERT_ID() run in one session so we get our own row's id.
issue_and_wait() {
  local cmd="$1" id state waited=0
  id=$(my "INSERT INTO _file_cmd (cmd, fp) VALUES ('$(esc "$cmd")', '$(esc "$fp")'); SELECT LAST_INSERT_ID()")
  echo "  issued $cmd command id=$id, waiting (timeout ${CMD_TIMEOUT}s)..." >&2
  while :; do
    state=$(my "SELECT state FROM _file_cmd WHERE id=$id" || echo "")
    case "$state" in
      done|error)
        echo "  $cmd id=$id -> $state: $(my "SELECT COALESCE(msg,'') FROM _file_cmd WHERE id=$id" | tr '\n' ' ')" >&2
        echo "$state"; return 0 ;;
    esac
    if [ "$waited" -ge "$CMD_TIMEOUT" ]; then
      echo "  $cmd id=$id -> timeout after ${CMD_TIMEOUT}s (last state '${state:-?}')" >&2
      echo "timeout"; return 0
    fi
    sleep 2; waited=$((waited + 2))
  done
}

seed_via_prepare_only() {
  echo "seeding chbench via testoperator --prepare-only (SF$SF)"
  CHBENCH_MYSQL_HOST="$MYH" CHBENCH_MYSQL_PORT="$MYP" CHBENCH_MYSQL_USER="$MYU" \
  CHBENCH_MYSQL_PASS="$MYSQL_PWD" CHBENCH_MYSQL_DB="$MYDB" \
    $TESTOP_PREFIX testoperator run htap \
      -s "$SPICED_BIN" -p "$SPICEPOD_PATH" --query-set chbench --source-type mysql \
      --scale-factor "$SF" ${TERMINALS:+--terminals $TERMINALS} \
      --duration 1 --concurrency 1 --ready-wait 60 \
      --prepare-only --disable-progress-bars
}

do_miss() {
  echo "template MISS for SF$SF -> seed once, then snapshot for future runs"
  seed_via_prepare_only
  if [ "$(issue_and_wait snapshot)" != "done" ]; then
    # Non-fatal: chbench is already freshly seeded, so the run can proceed with
    # --skip-prepare; only the cache for the *next* run is missing.
    echo "WARNING: snapshot did not complete; next run will reseed (MISS again)" >&2
  fi
}

wait_for_control_table

# HIT if the sidecar reports a cached snapshot whose fingerprint matches.
cached=""
if table_exists _file_cache; then
  cached=$(my "SELECT COALESCE(MAX(fp),'') FROM _file_cache WHERE k=1")
fi

if [ "$cached" = "$fp" ]; then
  echo "template HIT for SF$SF -> restoring chbench from sidecar snapshot"
  if [ "$(issue_and_wait restore)" = "done" ]; then
    echo "restore complete — run with --skip-prepare"
  else
    echo "WARNING: restore failed/timed out; falling back to a full reseed" >&2
    do_miss
  fi
else
  [ -n "$cached" ] && echo "cached fingerprint '$cached' != '$fp' -> reseeding"
  do_miss
fi

echo "chbench is ready — run with --skip-prepare"
