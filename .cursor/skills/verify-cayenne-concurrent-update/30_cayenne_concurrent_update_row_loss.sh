#!/usr/bin/env bash
# Concurrent same-key autocommit UPDATE against a running or freshly launched
# spiced. Success: id=50 present, count(*)=1000, amount = start + (# of
# count=1 responses), no silent count=0, HTTP 409 is a retryable conflict
# (not an increment and not a failure), and every HTTP 200 UPDATE body is
# a 0 or 1 affected-row count (malformed replies fail the harness).
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
# The launch below runs from a temp dir, so pin a relative SPICED now.
case "$SPICED" in
  /*) ;;
  */*) SPICED="$(cd "$(dirname "$SPICED")" && pwd)/$(basename "$SPICED")" ;;
  *) SPICED=$(command -v "$SPICED") || { echo "spiced not found: $SPICED" >&2; exit 1; } ;;
esac
# A runtime already answering on the harness port would take the UPDATEs meant
# for the spiced launched below.
if curl -sf 127.0.0.1:8393/v1/ready >/dev/null 2>&1; then
  echo "127.0.0.1:8393 already serves a runtime; stop it so the harness tests the spiced it launches" >&2
  exit 1
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
    if ! kill -0 "$P" 2>/dev/null; then
      echo "the launched spiced exited, but 127.0.0.1:8393 reports ready: another process holds the port" >&2
      tail -50 "$D/spiced.log" >&2
      rm -rf "$D"
      exit 1
    fi
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
"$PYTHON" "$SKILL_DIR/30_cayenne_concurrent_update_row_loss.py" "$TH" "$N"
RC=$?
set -e
kill "$P" 2>/dev/null || true
wait "$P" 2>/dev/null || true
grep -E 'WARN|ERROR' "$D/spiced.log" | grep -v -i 'free space\|preview\|over-commit' | cut -c1-300 | tail -5 || true
rm -rf "$D"
exit "$RC"
