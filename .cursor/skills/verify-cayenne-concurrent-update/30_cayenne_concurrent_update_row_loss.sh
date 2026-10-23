#!/usr/bin/env bash
# Concurrent same-key autocommit UPDATE against a running or freshly launched
# spiced. Success: id=50 present, count(*)=1000, amount = start + (# of
# count=1 responses), and no silent count=0.
#
# Usage: 30_cayenne_concurrent_update_row_loss.sh [threads] [updates]
# Env: MODE=file|memory (default file)
#      SPICED=path-to-spiced (default: first of $PATH, then $REPO/target/debug/spiced)
#      PYTHON=python with requests (and pyarrow if parquet must be generated)
set -u
SKILL_DIR=$(cd "$(dirname "$0")" && pwd)
REPO=$(cd "$SKILL_DIR/../../.." && pwd)
TH=${1:-16}
N=${2:-200}
MODE=${MODE:-file}
PYTHON=${PYTHON:-python3}
SPICED=${SPICED:-}
if [ -z "$SPICED" ]; then
  if command -v spiced >/dev/null 2>&1; then
    SPICED=$(command -v spiced)
  elif [ -x "$REPO/target/debug/spiced" ]; then
    SPICED=$REPO/target/debug/spiced
  elif [ -x "$REPO/target/release/spiced" ]; then
    SPICED=$REPO/target/release/spiced
  else
    echo "spiced not found; set SPICED= or build -p spiced" >&2
    exit 1
  fi
fi

DATA_SRC=$SKILL_DIR/data/orders.parquet
if [ ! -f "$DATA_SRC" ]; then
  (cd "$SKILL_DIR" && "$PYTHON" 00_setup_data.py)
fi

D=$(mktemp -d /tmp/cay-upd-XXXX)
mkdir -p "$D/data"
cp "$DATA_SRC" "$D/data/orders.parquet"
cat > "$D/spicepod.yaml" <<Y
version: v1
kind: Spicepod
name: cay-upd
datasets:
  - from: file:data/orders.parquet
    name: o_cayenne
    access: read_write
    acceleration: {enabled: true, engine: cayenne, mode: ${MODE}, primary_key: id, write_mode: acceleration}
Y

echo "spiced=$SPICED"
echo "sha=$(git -C "$REPO" rev-parse HEAD)"
echo "mode=$MODE threads=$TH updates=$N"
"$SPICED" --version 2>/dev/null | head -1

cd "$D"
"$SPICED" --http 127.0.0.1:8393 --flight 127.0.0.1:50393 --metrics 127.0.0.1:9393 >"$D/spiced.log" 2>&1 &
P=$!
ready=0
for _ in $(seq 1 180); do
  if curl -sf 127.0.0.1:8393/v1/ready >/dev/null; then
    ready=1
    break
  fi
  if ! kill -0 "$P" 2>/dev/null; then
    echo "spiced exited before ready" >&2
    tail -50 "$D/spiced.log" >&2
    rm -rf "$D"
    exit 1
  fi
  sleep 0.5
done
if [ "$ready" != 1 ]; then
  echo "spiced was not ready within 90s" >&2
  tail -50 "$D/spiced.log" >&2
  kill "$P" 2>/dev/null || true
  wait "$P" 2>/dev/null || true
  rm -rf "$D"
  exit 1
fi

set +e
"$PYTHON" - "$TH" "$N" <<'P'
import concurrent.futures as cf
import json
import sys

import requests

th, n = int(sys.argv[1]), int(sys.argv[2])
H = "http://127.0.0.1:8393/v1/sql"

def q(sql):
    return requests.post(H, data=sql.encode(), timeout=120)

before_row = q("SELECT id,amount FROM o_cayenne WHERE id=50")
before_count = q("SELECT count(*) c FROM o_cayenne")
print("before:", before_row.text, before_count.text)
before_amount = json.loads(before_row.text)[0]["amount"]

with cf.ThreadPoolExecutor(th) as ex:
    res = list(ex.map(lambda _i: q("UPDATE o_cayenne SET amount = amount + 1 WHERE id = 50"), range(n)))

codes = {}
bodies = []
count1 = 0
count0 = 0
errors = 0
for r in res:
    codes[r.status_code] = codes.get(r.status_code, 0) + 1
    bodies.append(r.text.strip())
    if r.status_code != 200:
        errors += 1
        continue
    try:
        payload = json.loads(r.text)
        c = int(payload[0]["count"]) if isinstance(payload, list) else int(payload.get("count", -1))
    except (json.JSONDecodeError, KeyError, IndexError, TypeError, ValueError):
        c = -1
    if c == 1:
        count1 += 1
    elif c == 0:
        count0 += 1

print(f"{n} UPDATEs with {th} threads: HTTP codes", codes, "bodies sample:", set(bodies[:50]))
print(f"count1={count1} count0={count0} non200={errors}")

after_row = q("SELECT id,amount FROM o_cayenne WHERE id=50")
after_count = q("SELECT count(*) c FROM o_cayenne")
print("after :", after_row.text, after_count.text, "(expected id=50 present with amount=before+successes, count=1000)")

ok = True
if after_row.status_code != 200 or not after_row.text.strip() or after_row.text.strip() in ("[]", ""):
    print("FAIL: id=50 missing")
    ok = False
else:
    after = json.loads(after_row.text)
    if not after:
        print("FAIL: id=50 missing")
        ok = False
    else:
        amount = after[0]["amount"]
        expected = before_amount + count1
        if abs(amount - expected) > 1e-9:
            print(f"FAIL: amount {amount} != start {before_amount} + count1 {count1} = {expected}")
            ok = False
        else:
            print(f"OK amount {amount} = {before_amount} + {count1}")
total = json.loads(after_count.text)[0]["c"]
if int(total) != 1000:
    print(f"FAIL: count(*)={total} expected 1000")
    ok = False
else:
    print("OK count(*)=1000")
if count0 and errors == 0:
    print(f"FAIL: {count0} silent count=0 responses with no error")
    ok = False
if errors:
    print(f"FAIL: {errors} non-200 responses")
    ok = False
sys.exit(0 if ok else 2)
P
RC=$?
set -e
kill "$P" 2>/dev/null || true
wait "$P" 2>/dev/null || true
grep -E 'WARN|ERROR' "$D/spiced.log" | grep -v -i 'free space\|preview\|over-commit' | cut -c1-300 | tail -5 || true
rm -rf "$D"
exit "$RC"
