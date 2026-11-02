#!/bin/bash
# INSERT semantics on accelerated read_write datasets with primary_key (no on_conflict):
# existing key, duplicate key in one batch, NULL key.
# Self-contained: fresh pod in /tmp, ports 8394/50394/9394.
set -u
SKILL=$(cd "$(dirname "$0")" && pwd)
PY=${PY:-python3}
D=$(mktemp -d /tmp/pk-XXXX)
mkdir -p "$D/data" "$D/acc"
if ! "$PY" -c 'import pyarrow' >/dev/null 2>&1; then
  echo "pyarrow is required: pip install pyarrow" >&2
  exit 1
fi
(cd "$D" && "$PY" "$SKILL/00_setup_data.py")
cat > "$D/spicepod.yaml" <<Y
version: v1
kind: Spicepod
name: pk
datasets:
  - {from: file:data/orders.parquet, name: o_duckdb, access: read_write, acceleration: {enabled: true, engine: duckdb, mode: file, primary_key: id, write_mode: acceleration, params: {duckdb_file: ./acc/d.db}}}
  - {from: file:data/orders.parquet, name: o_sqlite, access: read_write, acceleration: {enabled: true, engine: sqlite, mode: file, primary_key: id, write_mode: acceleration, params: {sqlite_file: ./acc/s.db}}}
  - {from: file:data/orders.parquet, name: o_cayenne, access: read_write, acceleration: {enabled: true, engine: cayenne, mode: file, primary_key: id, write_mode: acceleration}}
  - {from: file:data/orders.parquet, name: o_arrow, access: read_write, acceleration: {enabled: true, engine: arrow, write_mode: acceleration}}
Y
cd "$D"
spiced --http 127.0.0.1:8394 --flight 127.0.0.1:50394 --metrics 127.0.0.1:9394 > spiced.log 2>&1 &
P=$!
trap 'kill $P; wait $P 2>/dev/null; rm -rf $D' EXIT
ready=0
for _ in $(seq 1 120); do
  if curl -sf 127.0.0.1:8394/v1/ready >/dev/null; then
    ready=1
    break
  fi
  sleep 0.5
done
if [ "$ready" -ne 1 ]; then
  echo "spiced did not become ready; last log:" >&2
  tail -n 80 spiced.log >&2
  exit 1
fi
echo "=== $(spiced --version | head -1)  dir=$D  sha=${VERIFY_SHA:-unknown} ==="
q() {
  printf '%-90s => ' "$1"
  curl -s -X POST 127.0.0.1:8394/v1/sql -H 'Content-Type: text/plain' --data-binary "$1" -w ' [HTTP %{http_code}]\n' | cut -c1-260
}
for t in o_duckdb o_sqlite o_cayenne; do
  echo "### $t"
  q "INSERT INTO $t (id,customer,amount,status) VALUES (1,'dup',0.0,'new')"
  q "SELECT count(*) c, count(DISTINCT id) d FROM $t"
  q "SELECT id,customer,amount FROM $t WHERE id=1"
  echo "  -- duplicate key inside one batch:"
  q "INSERT INTO $t (id,customer,amount,status) VALUES (5001,'a',1.0,'new'),(5001,'b',2.0,'new')"
  q "SELECT id,customer FROM $t WHERE id=5001"
  echo "  -- NULL primary key:"
  q "INSERT INTO $t (id,customer,amount,status) VALUES (NULL,'nullpk',1.0,'new')"
  q "SELECT count(*) FROM $t WHERE id IS NULL"
done
echo "### o_arrow (no PK; UPDATE)"
q "UPDATE o_arrow SET amount=0 WHERE id=1"
