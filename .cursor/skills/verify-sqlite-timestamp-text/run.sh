#!/usr/bin/env bash
# Verify SQLite acceleration timestamp handling against a running spiced:
#   1. time retention deletes every expired row, including the cutoff's own day;
#   2. a filter on now() - INTERVAL runs and matches the spelled-out instant.
set -euo pipefail

SPICED=${SPICED:-$(command -v spiced || true)}
[[ -x "$SPICED" ]] || { echo "set SPICED to a spiced binary" >&2; exit 2; }
SPICED=$(realpath "$SPICED")
PY=${PY:-python3}
HTTP=${HTTP:-127.0.0.1:18690}
FLIGHT=${FLIGHT:-127.0.0.1:18691}
METRICS=${METRICS:-127.0.0.1:18692}
WORKDIR=${WORKDIR:-$(mktemp -d /tmp/verify-sqlite-timestamp-text.XXXXXX)}
cd "$WORKDIR"
echo "spiced: $("$SPICED" --version 2>&1 | head -1)"
echo "workdir: $WORKDIR"

# One row a minute over the last 48 hours, each with a fraction of a second.
"$PY" - <<'PY'
import datetime as dt, pyarrow as pa, pyarrow.parquet as pq
now = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)
ts = [now - dt.timedelta(minutes=m, milliseconds=250) for m in range(2880)]
pq.write_table(pa.table({"id": list(range(len(ts))), "ts": pa.array(ts, pa.timestamp("us"))}), "events.parquet")
PY

# x_retention loads every row with no retention first, so the retention check below
# deletes rows already stored rather than filtering them out of the load.
pod() {
  cat <<Y
version: v1
kind: Spicepod
name: verify-sqlite-timestamp-text
datasets:
  - from: file:$WORKDIR/events.parquet
    name: x_retention
    time_column: ts
    acceleration:
      enabled: true
      engine: sqlite
      mode: file
      refresh_mode: append
      params:
        sqlite_file: $WORKDIR/retention.db
Y
  [[ $1 == retention ]] && printf '      retention_check_enabled: true\n      retention_period: 24h\n      retention_check_interval: 5s\n'
  cat <<Y
  - from: file:$WORKDIR/events.parquet
    name: x_now
    acceleration:
      enabled: true
      engine: sqlite
      mode: file
      params:
        sqlite_file: $WORKDIR/now.db
Y
}

PID=
stop() { [[ -n $PID ]] && kill "$PID" 2>/dev/null && wait "$PID" 2>/dev/null; PID=; }
trap '[[ ${KEEP:-0} == 1 ]] || stop' EXIT
start() {
  pod "$1" > spicepod.yaml
  "$SPICED" --http "$HTTP" --flight "$FLIGHT" --metrics "$METRICS" >> spiced.log 2>&1 &
  PID=$!
  for _ in $(seq 1 60); do curl -sf "http://$HTTP/v1/ready" >/dev/null && return; sleep 1; done
  echo "spiced not ready after 60s" >&2; exit 1
}

start load
stop
start retention
sleep 15  # at least two retention checks

q() { curl -s -XPOST "http://$HTTP/v1/sql" -H 'Content-Type: text/plain' -H 'Accept: text/csv' --data "$1" | grep -v '^$' | tail -n +2; }

fail=0
# 1. Retention. Rows older than a cutoff a minute before the retention cutoff must be gone.
cutoff=$(date -u -d '-24 hours -1 minute' +'%F %T')
expired=$(q "SELECT count(*) FROM x_retention WHERE ts < TIMESTAMP '$cutoff'")
kept=$(q "SELECT count(*) FROM x_retention")
echo "retention: rows=$kept  older than $cutoff (expected 0)=$expired"
[[ $expired == 0 && $kept -gt 0 ]] || { echo "FAIL: retention kept expired rows"; fail=1; }

# 2. now() - INTERVAL pushed to SQLite. Compare with the same instant spelled out.
literal=$(date -u -d '-24 hours' +'%F %T')
by_literal=$(q "SELECT count(*) FROM x_now WHERE ts > TIMESTAMP '$literal'")
by_now=$(q "SELECT count(*) FROM x_now WHERE ts > now() - INTERVAL '24 hours'")
echo "now(): by now()=$by_now  by literal=$by_literal (a row a minute, so they may differ by 1)"
if ! [[ $by_now =~ ^[0-9]+$ ]] || (( by_now - by_literal > 1 || by_literal - by_now > 1 )); then
  echo "FAIL: now() - INTERVAL did not match the literal"; fail=1
fi

[[ $fail == 0 ]] && echo "PASS" || { grep -iE "no such function|retention" spiced.log | head -5 | cut -c1-240; exit 1; }
