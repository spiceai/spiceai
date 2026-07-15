#!/usr/bin/env bash
# Ensure the CH-benCH source DB `chbench` is ready WITHOUT re-seeding every run,
# by caching a pristine seed as a set of `<table>_tmpl` template tables ON THE
# SAME long-lived CI MySQL pod, then resetting the working tables from them with
# `TRUNCATE` + `INSERT ... SELECT`. A reset is a server-internal row copy; a
# fresh SF1000 seed is generation + `LOAD DATA` (many minutes).
#
# This is the MySQL counterpart of chbench_template.sh (Postgres). The two
# differ because MySQL has no server-side database-clone primitive like
# Postgres's `CREATE DATABASE ... TEMPLATE ... STRATEGY=FILE_COPY`, and — unlike
# the Postgres flow — the runner has NO filesystem access to the MySQL pod (they
# speak only over the MySQL connection). So the cache cannot be a physical file
# copy; it is a logical copy kept as template tables inside the `chbench`
# database, which the `bench` user already fully owns (no extra privilege, no
# CREATE DATABASE, no server filesystem access needed).
#
# Only meaningful on the dedicated, persistent MySQL pod (xlarge runner): its
# node-local data dir survives between runs (wiped only on pod restart), so the
# template tables persist run-to-run. On an ephemeral per-run docker MySQL there
# is nothing to cache, so the caller gates this step to the pod path and the main
# run keeps seeding normally.
#
# Flow (mirrors chbench_template.sh):
#   fingerprint = sf=<N> driver=<chbench-driver tree hash> mysql<major>
#   HIT  (marker table present + fingerprint matches + template tables exist):
#        reset each working table via TRUNCATE + INSERT ... SELECT from <t>_tmpl.
#   MISS: seed once via `testoperator run htap --prepare-only`, snapshot every
#        base table into <t>_tmpl, then write the fingerprint marker. On a miss
#        `chbench` is already freshly seeded, so no reset is needed.
# Either way `chbench` ends up seeded, so the caller runs with `--skip-prepare`.
#
# Env (from the workflow step):
#   SCALE_FACTOR (req), TERMINALS (opt), SPICEPOD_PATH (req), SPICED_BIN (req),
#   CHBENCH_MYSQL_HOST/PORT/USER/PASS (req), CHBENCH_MYSQL_DB (opt, default
#   chbench), TESTOP_PREFIX (opt), REPO_ROOT (default: $GITHUB_WORKSPACE) for the
#   chbench-driver fingerprint.
set -euo pipefail

SF="${SCALE_FACTOR:?}"
MYH="${CHBENCH_MYSQL_HOST:?}"; MYP="${CHBENCH_MYSQL_PORT:-3306}"
MYU="${CHBENCH_MYSQL_USER:-bench}"; MYDB="${CHBENCH_MYSQL_DB:-chbench}"
# MYSQL_PWD is read by the mysql client, so the password never appears on a
# command line (no ps/`--password` insecure-warning exposure).
export MYSQL_PWD="${CHBENCH_MYSQL_PASS:-bench}"
REPO_ROOT="${REPO_ROOT:-${GITHUB_WORKSPACE:-$PWD}}"
TESTOP_PREFIX="${TESTOP_PREFIX:-}"

MARKER="_chbench_tmpl_fp"

# SF flows into the fingerprint that is written as a single-quoted SQL literal;
# it comes from a workflow_dispatch input, so reject anything non-numeric before
# it reaches SQL (guards against malformed input / quoting surprises).
case "$SF" in
  ''|*[!0-9]*) echo "error: SCALE_FACTOR must be a positive integer, got '$SF'" >&2; exit 1;;
esac

# The mysql client may not be preinstalled on the runner (mirrors the Postgres
# script installing psql). default-mysql-client provides the `mysql` CLI. Fail
# fast with a clear message if it is still missing after the install attempt,
# rather than letting a later `mysql` call error with an opaque not-found.
if ! command -v mysql >/dev/null 2>&1; then
  sudo apt-get update -qq && sudo apt-get install -y -qq default-mysql-client || true
fi
if ! command -v mysql >/dev/null 2>&1; then
  echo "error: mysql client not found and could not be installed (default-mysql-client)" >&2
  exit 1
fi

# Scalar query against `chbench`: -N (no column names) -B (tab-separated, no box).
my_scalar() { mysql -h "$MYH" -P "$MYP" -u "$MYU" -D "$MYDB" -N -B -e "$1"; }
# Batch of statements against `chbench`, stop on first error.
my_batch()  { mysql -h "$MYH" -P "$MYP" -u "$MYU" -D "$MYDB"; }

mysqlmajor=$(my_scalar "SELECT SUBSTRING_INDEX(VERSION(), '.', 1)")
driver=$(git -C "$REPO_ROOT" rev-parse "HEAD:tools/chbench-driver" 2>/dev/null || echo nogit)
fp="sf=${SF} driver=${driver} mysql${mysqlmajor}"
echo "template fingerprint: $fp"

# Bulk-load session preamble reused by the snapshot and reset batches: disable
# unique/FK checks for speed, and skip binlogging the multi-GB copy when the
# session is allowed to (SESSION_VARIABLES_ADMIN). Probe sql_log_bin once and
# degrade gracefully if the privilege is absent — losing it only makes the copy
# slower (it is binlogged), never wrong, exactly like the seed loader.
LOGBIN=""
if mysql -h "$MYH" -P "$MYP" -u "$MYU" -D "$MYDB" -e "SET sql_log_bin=0" >/dev/null 2>&1; then
  LOGBIN="SET sql_log_bin=0;"
  echo "bulk-copy session: sql_log_bin=0 (copy not binlogged)"
else
  echo "bulk-copy session: no SESSION_VARIABLES_ADMIN; copy is binlogged"
fi
PREAMBLE="${LOGBIN} SET unique_checks=0; SET foreign_key_checks=0;"

# Base (non-template) tables in `chbench`: everything except the <t>_tmpl cache
# tables and the fingerprint marker. Discovered from information_schema rather
# than hardcoded, so the set can never drift from the driver's schema.
list_base_tables() {
  my_scalar "SELECT table_name FROM information_schema.tables
             WHERE table_schema = DATABASE()
               AND RIGHT(table_name, 5) <> '_tmpl'
               AND table_name <> '${MARKER}'"
}

# Decide HIT vs MISS: the marker table must exist, its stored fingerprint must
# match, and the template set must be COMPLETE — one <t>_tmpl for every base
# table. Requiring completeness (not merely "some template exists") means a
# partially-dropped cache on the long-lived pod is treated as a MISS and
# reseeded, instead of failing mid-reset on an `INSERT ... SELECT` from a
# missing <t>_tmpl.
hit=0
marker_exists=$(my_scalar "SELECT COUNT(*) FROM information_schema.tables
                           WHERE table_schema = DATABASE() AND table_name = '${MARKER}'")
if [ "${marker_exists:-0}" != "0" ]; then
  cur=$(my_scalar "SELECT fp FROM \`${MARKER}\` LIMIT 1" || true)
  base_count=$(my_scalar "SELECT COUNT(*) FROM information_schema.tables
                          WHERE table_schema = DATABASE()
                            AND RIGHT(table_name, 5) <> '_tmpl'
                            AND table_name <> '${MARKER}'")
  tmpl_count=$(my_scalar "SELECT COUNT(*) FROM information_schema.tables
                          WHERE table_schema = DATABASE() AND RIGHT(table_name, 5) = '_tmpl'")
  if [ "$cur" = "$fp" ] && [ "${base_count:-0}" != "0" ] && [ "${tmpl_count:-0}" = "${base_count:-0}" ]; then
    hit=1
  elif [ "$cur" = "$fp" ]; then
    echo "template INCOMPLETE (${tmpl_count:-0} template(s) for ${base_count:-0} base table(s)) -> reseeding"
  elif [ -n "${cur:-}" ]; then
    echo "template STALE (fingerprint '$cur' != '$fp') -> reseeding"
  fi
fi

if [ "$hit" = 1 ]; then
  echo "template HIT for SF$SF -> resetting working tables from <table>_tmpl"
  tables=$(list_base_tables)
  {
    echo "$PREAMBLE"
    for t in $tables; do
      # TRUNCATE (fast, resets auto-increment to a fresh-seed state; no FKs in
      # this schema so no ordering constraint) then refill from the template.
      # The working table keeps its indexes and _bench_ts triggers, so no DDL is
      # reproduced here. The BEFORE INSERT trigger re-stamps _bench_ts to the
      # reset time — benchmark-neutral: _bench_ts is instrumentation only, and
      # the correctness gates compare source vs Spice relatively.
      echo "TRUNCATE TABLE \`${t}\`;"
      echo "INSERT INTO \`${t}\` SELECT * FROM \`${t}_tmpl\`;"
    done
  } | my_batch
  echo "reset complete — run with --skip-prepare"
  exit 0
fi

echo "template MISS for SF$SF -> seeding once via --prepare-only, then snapshotting"

# Seed the working `chbench` tables once (schema + data + indexes + triggers).
CHBENCH_MYSQL_HOST="$MYH" CHBENCH_MYSQL_PORT="$MYP" CHBENCH_MYSQL_USER="$MYU" \
CHBENCH_MYSQL_PASS="$MYSQL_PWD" CHBENCH_MYSQL_DB="$MYDB" \
  $TESTOP_PREFIX testoperator run htap \
    -s "$SPICED_BIN" -p "$SPICEPOD_PATH" --query-set chbench --source-type mysql \
    --scale-factor "$SF" ${TERMINALS:+--terminals $TERMINALS} \
    --duration 1 --concurrency 1 --ready-wait 60 \
    --prepare-only --disable-progress-bars

# Snapshot each freshly-seeded base table into a <table>_tmpl template table.
# CREATE ... LIKE copies columns, defaults and indexes but NOT triggers, so the
# INSERT ... SELECT copies _bench_ts verbatim (no trigger fires) — the template
# holds the pristine seed-time values.
tables=$(list_base_tables)
{
  echo "$PREAMBLE"
  for t in $tables; do
    echo "DROP TABLE IF EXISTS \`${t}_tmpl\`;"
    echo "CREATE TABLE \`${t}_tmpl\` LIKE \`${t}\`;"
    echo "INSERT INTO \`${t}_tmpl\` SELECT * FROM \`${t}\`;"
  done
} | my_batch

# Record the fingerprint so the next run can decide HIT/MISS.
{
  echo "DROP TABLE IF EXISTS \`${MARKER}\`;"
  echo "CREATE TABLE \`${MARKER}\` (fp VARCHAR(255) NOT NULL);"
  echo "INSERT INTO \`${MARKER}\` (fp) VALUES ('${fp}');"
} | my_batch

echo "seeded + snapshotted template tables for SF$SF — run with --skip-prepare"
