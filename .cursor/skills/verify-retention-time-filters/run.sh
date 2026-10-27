#!/usr/bin/env bash
# Verify retention / refresh_data_window time filters against a running spiced.
# Exits non-zero on any FAIL. See SKILL.md.
set -u
HERE=$(cd "$(dirname "$0")" && pwd)
SPICED=${SPICED:-spiced}
PY=${PY:-python3}                      # needs pyarrow
HP=${HP:-18190}; FP=${FP:-50161}
ENGINES=${ENGINES:-"arrow duckdb cayenne"}
W=$(mktemp -d /tmp/verify-retention-XXXX)
FAIL=0
pass() { echo "PASS $*"; }
fail() { echo "FAIL $*"; FAIL=1; }
$PY "$HERE/gen.py" "$W/data" >/dev/null || { echo "gen.py failed (pip install pyarrow)"; exit 2; }
DATA=$W/data/events.parquet
# gen.py: 2880 rows 1/min over 48h back from now, 151 rows at 1/s around now-24h,
# 3 future rows, 4 NULL-time rows; every string column encodes the same instant as `ts`.
q() { curl -s -XPOST 127.0.0.1:$HP/v1/sql -H 'Content-Type: text/plain' -H 'Accept: text/csv' --data "$1" | grep -v '^$' | tail -n +2; }
PID=
stop() { [ -n "$PID" ] || return 0; kill -TERM $PID 2>/dev/null; for _ in $(seq 1 30); do kill -0 $PID 2>/dev/null || break; sleep 1; done; kill -KILL $PID 2>/dev/null; PID=; }
cleanup() { stop; [ "${KEEP:-0}" = 1 ] && echo "kept $W" || rm -rf "$W"; }
trap cleanup EXIT
pod() { mkdir -p "$W/$1"; sed "s#__DATA__#$DATA#g" > "$W/$1/spicepod.yaml"; } # $1 = name; spicepod on stdin
run_pod() { # $1 = name; not in a pipeline, so PID stays in this shell
  stop; local dir=$W/$1
  ( cd "$dir" && exec "$SPICED" --http 127.0.0.1:$HP --flight 127.0.0.1:$FP --metrics 127.0.0.1:$((HP+1000)) > spiced.log 2>&1 ) &
  PID=$!
  for _ in $(seq 1 60); do curl -sf 127.0.0.1:$HP/v1/ready >/dev/null && return 0; sleep 1; done
  echo "(spiced not ready after 60s: $dir/spiced.log)"; tail -5 "$dir/spiced.log"
}
mode() { [ "$1" = arrow ] && echo memory || echo file; }
echo "spiced: $($SPICED --version 2>&1 | head -1)  workdir: $W"

# 1+2: ISO 8601 string time columns (Z, offsets, space separator) with retention.
{ printf 'version: v1\nkind: Spicepod\nname: verify\ndatasets:\n'
  for e in $ENGINES; do for c in ts_iso ts_iso_off ts_iso_naive; do cat <<Y
  - from: file:__DATA__
    name: ${c}_$e
    time_column: $c
    time_format: iso8601
    acceleration: {enabled: true, engine: $e, mode: $(mode $e), retention_check_enabled: true, retention_period: 24h, retention_check_interval: 1h}
Y
  done; done; } | pod iso
run_pod iso
for e in $ENGINES; do for c in ts_iso ts_iso_off ts_iso_naive; do
  t=${c}_$e
  out=$(q "select count(*) filter (where ts < now() - interval '24 hours' - interval '5 seconds'), count(*) filter (where ts >= now() - interval '24 hours' + interval '5 seconds'), count(*) filter (where ts is null) from $t")
  IFS=, read -r expired live nulls <<<"$out"
  want_live=$($PY - "$DATA" <<'P'
import sys, datetime as dt, pyarrow.parquet as pq
t = pq.read_table(sys.argv[1]).column('ts').to_pylist()
now = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)
print(sum(1 for v in t if v is not None and v >= now - dt.timedelta(hours=24) + dt.timedelta(seconds=5)))
P
)
  if [ "$expired" = 0 ] && [ -n "$live" ] && [ $((want_live - live)) -le 10 ] && [ $((want_live - live)) -ge -10 ] && [ "$nulls" = 4 ]; then
    pass "$t: expired=0 live=$live (expected ~$want_live) null=4"
  else
    fail "$t: expired=$expired live=$live (expected ~$want_live) null=$nulls raw=[$out]"
  fi
done; done

# 3: NULL-time rows survive the load when retention drives the window.
{ printf 'version: v1\nkind: Spicepod\nname: verify\ndatasets:\n'
  for e in $ENGINES; do cat <<Y
  - from: file:__DATA__
    name: ret_$e
    time_column: ts
    acceleration: {enabled: true, engine: $e, mode: $(mode $e), retention_check_enabled: true, retention_period: 24h, retention_check_interval: 1h}
Y
  done; } | pod nulls
run_pod nulls
for e in $ENGINES; do
  n=$(q "select count(*) from ret_$e where ts is null")
  [ "$n" = 4 ] && pass "ret_$e: 4 NULL-time rows kept" || fail "ret_$e: NULL-time rows=$n (expected 4)"
done

# 4: DATE time_partition_column with refresh_data_window 24h loads the whole window.
{ printf 'version: v1\nkind: Spicepod\nname: verify\ndatasets:\n'
  for e in $ENGINES; do cat <<Y
  - from: file:__DATA__
    name: w_$e
    time_column: ts
    time_partition_column: ts_date
    time_partition_format: date
    acceleration: {enabled: true, engine: $e, mode: $(mode $e), refresh_data_window: 24h}
Y
  done; cat <<Y
  - from: file:__DATA__
    name: w_ctl
    time_column: ts
    acceleration: {enabled: true, engine: arrow, refresh_data_window: 24h}
Y
} | pod window
run_pod window
ctl=$(q "select count(*) from w_ctl")
for e in $ENGINES; do
  n=$(q "select count(*) from w_$e")
  # Loads start a few seconds apart and the boundary has 1 row/s, so allow a small skew.
  if [ -n "$n" ] && [ -n "$ctl" ] && [ $((n - ctl)) -le 10 ] && [ $((ctl - n)) -le 10 ]; then
    pass "w_$e: rows=$n ~ w_ctl=$ctl"
  else
    fail "w_$e: rows=$n, w_ctl=$ctl"
  fi
done

[ $FAIL = 0 ] && echo "ALL PASS" || echo "SOME CHECKS FAILED"
exit $FAIL
