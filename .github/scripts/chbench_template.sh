#!/usr/bin/env bash
# Ensure the CH-benCH source DB `chbench` is ready WITHOUT re-seeding every run,
# by caching a pristine seed as a Postgres *template database* on the long-lived
# CI Postgres pod. Restoring a template (CREATE DATABASE ... STRATEGY=FILE_COPY)
# is seconds; seeding SF1000 is ~tens of minutes.
#
# Only meaningful on the dedicated, persistent PG pod (xlarge runner) — on an
# ephemeral per-run docker Postgres there is nothing to cache, so the caller
# gates this step to the pod path and the main run keeps seeding normally.
#
# Flow (mirrors the local harness `do_reset`):
#   fingerprint = sf=<N> driver=<chbench-driver tree hash> pg<major>
#   HIT  (template exists + fingerprint matches): restore chbench via FILE_COPY.
#   MISS: seed once into the template via `testoperator run htap --prepare-only`,
#         freeze it (IS_TEMPLATE + fingerprint comment), then restore chbench.
# Either way chbench ends up seeded, so the caller runs with `--skip-prepare`.
#
# Env (from the workflow step):
#   SCALE_FACTOR (req), TERMINALS (opt), SPICEPOD_PATH (req), SPICED_BIN (req),
#   CHBENCH_PG_HOST/PORT/USER/PASS (req), TESTOP_PREFIX (opt),
#   REPO_ROOT (default: $GITHUB_WORKSPACE) for the chbench-driver fingerprint.
set -euo pipefail
SF="${SCALE_FACTOR:?}"; TMPL="chbench_tmpl_sf${SF}"
PGH="${CHBENCH_PG_HOST:?}"; PGP="${CHBENCH_PG_PORT:-5432}"
PGU="${CHBENCH_PG_USER:-bench}"; export PGPASSWORD="${CHBENCH_PG_PASS:-bench}"
REPO_ROOT="${REPO_ROOT:-${GITHUB_WORKSPACE:-$PWD}}"
TESTOP_PREFIX="${TESTOP_PREFIX:-}"

# psql may not be preinstalled on the runner (the cleanup step installs it too).
if ! command -v psql >/dev/null 2>&1; then
  sudo apt-get update -qq && sudo apt-get install -y -qq postgresql-client || true
fi

psql_pg() { psql -h "$PGH" -p "$PGP" -U "$PGU" -d postgres -v ON_ERROR_STOP=1 -tAc "$1"; }
pgmajor=$(psql_pg "SHOW server_version" | cut -d. -f1)
# src/ only: docs/, scripts/, and tests/ cannot affect seeded data, so their
# commits must not invalidate the cached template (a false miss = 40-min reseed).
driver=$(git -C "$REPO_ROOT" rev-parse "HEAD:tools/chbench-driver/src" 2>/dev/null || echo nogit)
fp="sf=${SF} driver=${driver} pg${pgmajor}"
echo "template fingerprint: $fp"

drop_template() {  # un-freeze (IS_TEMPLATE blocks DROP) then drop
  psql_pg "ALTER DATABASE $1 WITH IS_TEMPLATE false ALLOW_CONNECTIONS true" >/dev/null 2>&1 || true
  psql_pg "DROP DATABASE IF EXISTS $1 WITH (FORCE)" >/dev/null 2>&1 || true
}

# A crashed/cancelled prior run can leave a logical replication slot on chbench.
# A slot pins its database, so DROP DATABASE fails even WITH (FORCE) until the
# slot is gone; and an *active* slot won't drop until its walsender is killed.
# Terminate holders, then drop, retrying until none remain. Scoped to Spice's
# own chbench slots (matching the cleanup step's 'spice_%' convention) so we
# never disturb unrelated slots on this shared, long-lived CI pod.
SLOT_FILTER="slot_name LIKE 'spice_%' AND database = 'chbench'"
drop_chbench_replication_slots() {
  for _ in 1 2 3 4 5; do
    n=$(psql_pg "SELECT count(*) FROM pg_replication_slots WHERE $SLOT_FILTER" 2>/dev/null || echo 0)
    [ "${n:-0}" = 0 ] && return 0
    psql_pg "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots WHERE $SLOT_FILTER AND active_pid IS NOT NULL" >/dev/null 2>&1 || true
    psql_pg "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE $SLOT_FILTER AND NOT active" >/dev/null 2>&1 || true
    sleep 1
  done
  stuck=$(psql_pg "SELECT string_agg(slot_name || '(active_pid=' || coalesce(active_pid::text,'none') || ')', ', ') FROM pg_replication_slots WHERE $SLOT_FILTER" 2>/dev/null || true)
  [ -n "$stuck" ] && echo "WARNING: chbench replication slots still present after cleanup: $stuck"
}

# Terminate lingering backends then drop; fail loud (no error suppression) so a
# genuine failure surfaces here instead of masking into a later CREATE
# 'database already exists'.
drop_working_db() {  # $1 = dbname
  psql_pg "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname='$1' AND pid <> pg_backend_pid()" >/dev/null 2>&1 || true
  psql_pg "DROP DATABASE IF EXISTS $1 WITH (FORCE)"
}

drop_chbench_replication_slots

exists=$(psql_pg "SELECT 1 FROM pg_database WHERE datname='$TMPL'" 2>/dev/null)
hit=0
if [ -n "$exists" ]; then
  cur=$(psql_pg "SELECT shobj_description(oid,'pg_database') FROM pg_database WHERE datname='$TMPL'" 2>/dev/null)
  if [ "$cur" = "$fp" ]; then hit=1
  else echo "template $TMPL STALE (fingerprint '$cur' != '$fp') -> dropping"; drop_template "$TMPL"; fi
fi

if [ "$hit" = 1 ]; then
  echo "template HIT: $TMPL -> restoring chbench (FILE_COPY)"
else
  echo "template MISS for SF$SF -> seeding once into $TMPL via --prepare-only"
  drop_template "$TMPL"
  psql_pg "CREATE DATABASE $TMPL OWNER $PGU" >/dev/null
  CHBENCH_PG_HOST="$PGH" CHBENCH_PG_PORT="$PGP" CHBENCH_PG_USER="$PGU" \
  CHBENCH_PG_PASS="$PGPASSWORD" CHBENCH_PG_DB="$TMPL" \
    $TESTOP_PREFIX testoperator run htap \
      -s "$SPICED_BIN" -p "$SPICEPOD_PATH" --query-set chbench \
      --scale-factor "$SF" ${TERMINALS:+--terminals $TERMINALS} \
      --duration 1 --concurrency 1 --ready-wait 60 \
      --prepare-only --disable-progress-bars
  psql -h "$PGH" -p "$PGP" -U "$PGU" -d "$TMPL" -v ON_ERROR_STOP=1 -tAc "CHECKPOINT" >/dev/null 2>&1 || true
  psql_pg "ALTER DATABASE $TMPL WITH IS_TEMPLATE true ALLOW_CONNECTIONS false" >/dev/null
  psql_pg "COMMENT ON DATABASE $TMPL IS '$fp'" >/dev/null
  tmpl_size=$(psql_pg "SELECT pg_size_pretty(pg_database_size('$TMPL'))")
  echo "seeded + froze template $TMPL ($tmpl_size)"
fi

# Restore the working chbench from the template (fast physical copy).
drop_working_db chbench
psql_pg "CREATE DATABASE chbench TEMPLATE $TMPL OWNER $PGU STRATEGY=FILE_COPY" >/dev/null
psql_pg "ALTER DATABASE chbench WITH ALLOW_CONNECTIONS true" >/dev/null 2>&1 || true
echo "chbench restored from $TMPL — run with --skip-prepare"
