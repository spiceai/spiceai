#!/usr/bin/env bash
# Copyright 2024-2026 The Spice.ai OSS Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# The HTTP rate-control scenario catalog.
#
#   ./run_ratecontrol.sh --list
#   ./run_ratecontrol.sh all
#   ./run_ratecontrol.sh cluster
#   ./run_ratecontrol.sh multi-origin-isolation
#
# `spiced` must be built with the `rate-control` cargo feature, or
# `state_location` is ignored and every cluster scenario silently measures a
# process-local limiter:
#   cargo build --release -p spiced --features "rate-control,duckdb"

set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"

BRANCH_BUILD="$HOME/Github/spiceai/target/release/spiced"
if [[ -n "${SPICED_BIN:-}" ]]; then
  :
elif [[ -x "$BRANCH_BUILD" ]]; then
  SPICED_BIN="$BRANCH_BUILD"
else
  SPICED_BIN="$HOME/.spice/bin/spiced"
fi

PY="${PY:-$HERE/.venv/bin/python}"
if [[ ! -x "$PY" ]]; then
  echo "no harness venv at $PY; run: python3 -m venv .venv && ./.venv/bin/pip install -r requirements.txt" >&2
  exit 1
fi

exec "$PY" "$HERE/loadgen/run_ratecontrol.py" \
  --spiced "$SPICED_BIN" \
  --rustfs "${RUSTFS_BIN:-$HOME/.spice/bin/rustfs}" \
  --python "$PY" \
  --harness-dir "$HERE" \
  --run-root "${RUN_ROOT:-/tmp/http_ratecontrol_run}" \
  "$@"
