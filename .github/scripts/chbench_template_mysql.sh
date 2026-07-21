#!/usr/bin/env bash
# Ensure the CH-benCH source DB `chbench` is ready WITHOUT re-seeding every run,
# by keeping a pristine snapshot of the whole data dir on the long-lived CI MySQL
# pod and restoring from it with a fast local file copy. The physical file work is
# done by the SUPERVISOR in the MySQL pod (chbench-mysql chart): it runs mysqld as
# a child and performs COLD, whole-data-dir snapshot/restore while mysqld is
# cleanly stopped. This script only DRIVES it over the ordinary MySQL connection —
# it INSERTs a command row into `chbench._filed_cmd` and polls for completion.
# MySQL has no server-side DB-clone primitive and the runner has no filesystem
# access to the pod, so the co-located supervisor (sharing the data volume) does
# the copy; because it acts only on a stopped server, a reset can never corrupt
# InnoDB (the failure mode of the earlier online tablespace-swap design).
#
# Only meaningful on the dedicated, persistent MySQL pod (xlarge runner): its
# node-local data dir and the supervisor's snapshot cache both survive between
# runs (wiped together only on pod restart). On an ephemeral per-run docker MySQL
# there is no supervisor and nothing to cache, so the caller gates this step to
# the pod path and the main run seeds normally.
#
# Flow (mirrors chbench_template.sh, but the "copy" is the supervisor's job):
#   fingerprint = sf<N>-driver<chbench-driver tree hash>-mysql<major>  (fs-safe)
#   HIT  (supervisor's snapshot registry lists a matching fingerprint):
#        issue a `restore` command and wait. NB: a restore RESTARTS mysqld, so the
#        connection drops mid-command — we tolerate that while polling. On restore
#        failure, fall back to a full reseed so the run is never blocked.
#   MISS: seed once via `testoperator run htap --prepare-only`, then issue a
#        `snapshot` command so the next run can HIT. `chbench` is already freshly
#        seeded, so no restore is needed on a miss.
# Either way `chbench` ends up seeded, so the caller runs with `--skip-prepare`.
#
# A restore resets the binlog (supervisor RESET MASTER); spiced is started fresh
# by the subsequent testoperator run and re-establishes CDC from the current
# position, so no extra downstream reset is needed here.
#
# Env (from the workflow step):
#   SCALE_FACTOR (req), TERMINALS (opt), SPICEPOD_PATH (req), SPICED_BIN (req),
#   CHBENCH_MYSQL_HOST/PORT/USER/PASS (req), CHBENCH_MYSQL_DB (opt, default
#   chbench), TESTOP_PREFIX (opt), REPO_ROOT (default: $GITHUB_WORKSPACE) for the
#   chbench-driver fingerprint, CMD_TIMEOUT_SECONDS (opt, default 3600) and
#   SUPERVISOR_WAIT_SECONDS (opt, default 180) for the poll bounds.
set -euo pipefail

SF="${SCALE_FACTOR:?}"
MYH="${CHBENCH_MYSQL_HOST:?}"; MYP="${CHBENCH_MYSQL_PORT:-3306}"
MYU="${CHBENCH_MYSQL_USER:-bench}"; MYDB="${CHBENCH_MYSQL_DB:-chbench}"
export MYSQL_PWD="${CHBENCH_MYSQL_PASS:-bench}"
REPO_ROOT="${REPO_ROOT:-${GITHUB_WORKSPACE:-$PWD}}"
TESTOP_PREFIX="${TESTOP_PREFIX:-}"
CMD_TIMEOUT="${CMD_TIMEOUT_SECONDS:-3600}"
# Back-compat: accept the old SIDECAR_WAIT_SECONDS name if a caller still sets it.
SUPERVISOR_WAIT="${SUPERVISOR_WAIT_SECONDS:-${SIDECAR_WAIT_SECONDS:-180}}"

# The mysql client may not be preinstalled on the runner.
if ! command -v mysql >/dev/null 2>&1; then
  sudo apt-get update -qq && sudo apt-get install -y -qq default-mysql-client || true
fi

# Scalar/one-row query against `chbench` (-N no headers, -B tab-separated).
my() { mysql -h "$MYH" -P "$MYP" -u "$MYU" -D "$MYDB" -N -B -e "$1"; }

mysqlmajor=$(my "SELECT SUBSTRING_INDEX(VERSION(), '.', 1)")
driver=$(git -C "$REPO_ROOT" rev-parse "HEAD:tools/chbench-driver" 2>/dev/null || echo nogit)
# Filesystem-safe fingerprint: the supervisor uses it verbatim as a cache dir name.
fp="sf${SF}-driver${driver}-mysql${mysqlmajor}"
echo "template fingerprint: $fp"

# SQL-escape a single-quoted literal.
esc() { printf '%s' "$1" | sed "s/'/''/g"; }

table_exists() {
  local n
  n=$(my "SELECT COUNT(*) FROM information_schema.tables
          WHERE table_schema='${MYDB}' AND table_name='$1'" 2>/dev/null || echo 0)
  [ "${n:-0}" != "0" ]
}

# Wait for the supervisor to have created its control table (it does so shortly
# after mysqld first becomes ready). Absent after the bound => it isn't running.
wait_for_control_table() {
  local waited=0
  until table_exists _filed_cmd; do
    if [ "$waited" -ge "$SUPERVISOR_WAIT" ]; then
      echo "ERROR: chbench._filed_cmd not present after ${SUPERVISOR_WAIT}s — is the MySQL pod running the chbench-mysql supervisor (chbench-mysql chart)?" >&2
      return 1
    fi
    sleep 3; waited=$((waited + 3))
  done
}

# A restore restarts mysqld, so generate a caller-side token and key the command
# by it (the row briefly disappears across the restart; we recreate/poll by token).
new_token() {
  if [ -r /proc/sys/kernel/random/uuid ]; then cat /proc/sys/kernel/random/uuid
  else echo "tok-$(date +%s)-${RANDOM}${RANDOM}"; fi
}

# Issue a command to the supervisor and wait for it to finish. Echoes the final
# state ("done"/"error"/"timeout") on stdout; the supervisor's message goes to the
# log. Tolerates the connection dropping mid-command (a restore bounces mysqld):
# query failures and a briefly-absent row are treated as "still running".
issue_and_wait() {
  local cmd="$1" token state msg waited=0
  token=$(new_token)
  my "INSERT INTO _filed_cmd (token, cmd, fp) VALUES ('$(esc "$token")', '$(esc "$cmd")', '$(esc "$fp")')"
  echo "  issued $cmd token=$token, waiting (timeout ${CMD_TIMEOUT}s)..." >&2
  while :; do
    # `|| echo ''` swallows connection errors while mysqld restarts for a restore.
    state=$(my "SELECT state FROM _filed_cmd WHERE token='$(esc "$token")'" 2>/dev/null || echo "")
    case "$state" in
      done|error)
        msg=$(my "SELECT COALESCE(msg,'') FROM _filed_cmd WHERE token='$(esc "$token")'" 2>/dev/null | tr '\n' ' ' || echo "")
        echo "  $cmd token=$token -> $state: $msg" >&2
        echo "$state"; return 0 ;;
    esac
    if [ "$waited" -ge "$CMD_TIMEOUT" ]; then
      echo "  $cmd token=$token -> timeout after ${CMD_TIMEOUT}s (last state '${state:-?}')" >&2
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

# HIT if the supervisor's registry lists a snapshot with this fingerprint.
hit=0
if table_exists _filed_snapshots; then
  n=$(my "SELECT COUNT(*) FROM _filed_snapshots WHERE fp='$(esc "$fp")'" 2>/dev/null || echo 0)
  [ "${n:-0}" != "0" ] && hit=1
fi

if [ "$hit" = 1 ]; then
  echo "template HIT for SF$SF -> restoring chbench from the supervisor snapshot"
  if [ "$(issue_and_wait restore)" = "done" ]; then
    echo "restore complete — run with --skip-prepare"
  else
    echo "WARNING: restore failed/timed out; falling back to a full reseed" >&2
    do_miss
  fi
else
  echo "no cached snapshot for '$fp' -> reseeding"
  do_miss
fi

echo "chbench is ready — run with --skip-prepare"
