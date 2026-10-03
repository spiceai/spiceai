#!/usr/bin/env bash
# Self-contained reproduction: a `refresh_mode: caching` HTTP dataset returns an
# Internal error when a query filters on an integer column. The same query on the
# same dataset works when acceleration is removed, and a filter on a text column
# works with caching enabled.
#
# Observed on spiced v2.3.1 (build f3ca9d17dc).
#
# Usage:  ./repro_caching_int_filter.sh
# Needs:  python3, curl, and spiced on PATH or at ~/.spice/bin/spiced.
set -euo pipefail

SPICED_BIN="${SPICED_BIN:-$HOME/.spice/bin/spiced}"
command -v "$SPICED_BIN" >/dev/null 2>&1 || SPICED_BIN="spiced"
WORK="$(mktemp -d)"
ORIGIN_PORT=9009
HTTP_PORT=8093

# 1. A minimal origin: one JSON row with an integer `id`/`version` and a text `origin`.
cat > "$WORK/origin.py" <<'PY'
import json, sys
from http.server import BaseHTTPRequestHandler, HTTPServer
ROW = {"id": 1, "version": 42, "origin": "p1"}
class H(BaseHTTPRequestHandler):
    def do_GET(self):
        b = (json.dumps(ROW) + "\n").encode()
        self.send_response(200); self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(b))); self.end_headers(); self.wfile.write(b)
    def log_message(self, *a): pass
HTTPServer(("127.0.0.1", int(sys.argv[1])), H).serve_forever()
PY

# 2. A caching dataset over that origin. The `json_object: "*"` column decomposes
#    the JSON body into the typed top-level columns.
cat > "$WORK/spicepod.yaml" <<YAML
version: v1
kind: Spicepod
name: caching-int-filter-repro
datasets:
  - from: http://127.0.0.1:${ORIGIN_PORT}/data
    name: d1
    params:
      file_format: json
      allowed_request_paths: "/data"
    columns:
      - name: id
        type: bigint
      - name: version
        type: bigint
      - name: origin
        type: text
      - name: extra
        type: text
        metadata:
          json_object: "*"
    acceleration:
      enabled: true
      engine: duckdb
      refresh_mode: caching
      params:
        caching_ttl: "3s"
        caching_stale_while_revalidate_ttl: "6s"
      retention_period: "10m"
YAML

cleanup() { kill "${OPID:-}" "${SPID:-}" 2>/dev/null || true; }
trap cleanup EXIT

python3 "$WORK/origin.py" "$ORIGIN_PORT" >/dev/null 2>&1 & OPID=$!
sleep 0.5
"$SPICED_BIN" "$WORK/spicepod.yaml" --http "127.0.0.1:${HTTP_PORT}" >"$WORK/spiced.log" 2>&1 & SPID=$!
for _ in $(seq 1 120); do grep -q "Spice runtime is ready" "$WORK/spiced.log" 2>/dev/null && break; sleep 0.3; done
grep -q "Spice runtime is ready" "$WORK/spiced.log" || { echo "spiced did not become ready:"; tail -20 "$WORK/spiced.log"; exit 1; }

q() { curl -s -X POST "http://127.0.0.1:${HTTP_PORT}/v1/sql" -H 'Content-Type: text/plain' -d "$1"; echo; }

echo "== text-column filter (works) =="
echo "   SELECT version FROM d1 WHERE origin = 'p1'"
q "SELECT version FROM d1 WHERE origin = 'p1'"

echo "== integer-column filter (fails) =="
echo "   SELECT version FROM d1 WHERE id = 1"
q "SELECT version FROM d1 WHERE id = 1"

echo "   SELECT id FROM d1 WHERE version > 0"
q "SELECT id FROM d1 WHERE version > 0"
