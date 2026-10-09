#!/usr/bin/env bash
# Copyright 2024-2026 The Spice.ai OSS Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Run the TPC-H suite through spiced (Mode B) for one acceleration engine, held
# to two baselines:
#
#   1. Mode A, the suite's plans in plain DataFusion over the same generated
#      tables. Spice's default layout must pass every case Mode A passes.
#   2. That default run. Each layout must pass every case the default passes.
#
# Usage: run-mode-b.sh <harness> <spiced> <engine> <mode> <out-dir> [layout ...]
#
#   harness  the spice-substrait-compliance binary
#   spiced   the spiced binary that serves the tables
#   engine   the acceleration engine: cayenne, duckdb, arrow, sqlite, ..., or
#            `none` to serve the parquet files federated
#   mode     file or memory
#   out-dir  where the JSON and CSV reports go
#   layout   acceleration layouts (`--layout`), e.g. primary_key,indexes
#
# Environment: SCALE_FACTOR (default 1), ITERATIONS, the times each plan runs
# (default 3), and SUITE, the IBM TPC-H suite directory (default
# tools/substrait-compliance/.ibm/test-suites/tpch).
#
# Every layout runs even after an earlier run fails, so one red layout cannot
# hide the others. The exit status is 1 when any run failed.
set -uo pipefail

if [[ $# -lt 5 ]]; then
  echo "Usage: $0 <harness> <spiced> <engine> <mode> <out-dir> [layout ...]" >&2
  exit 2
fi

harness=$1
spiced=$2
engine=$3
mode=$4
out=$5
shift 5

scale_factor=${SCALE_FACTOR:-1}
iterations=${ITERATIONS:-3}
suite=${SUITE:-tools/substrait-compliance/.ibm/test-suites/tpch}
mkdir -p "$out"

failed=()

run() {
  local name=$1
  shift
  echo "::group::${name}"
  "$harness" --suite "$suite" --scale-factor "$scale_factor" \
    --out-json "$out/${name}.json" --out-csv "$out/${name}.csv" "$@"
  local status=$?
  echo "::endgroup::"
  if [[ $status -ne 0 ]]; then
    echo "::error::${name} failed (exit ${status})"
    failed+=("$name")
  fi
  return $status
}

if ! run mode-a --mode mode-a; then
  echo "Mode A did not produce a baseline, so no Mode B run can be held to it." >&2
  exit 1
fi

serve=(--mode mode-b --spiced-path "$spiced" --acceleration-engine "$engine"
  --acceleration-mode "$mode" --iterations "$iterations")

run "mode-b-${engine}-${mode}" "${serve[@]}" --baseline "$out/mode-a.json"
if [[ ! -f "$out/mode-b-${engine}-${mode}.json" ]]; then
  echo "The default layout produced no report, so no layout can be held to it." >&2
  exit 1
fi
if [[ $# -gt 0 && "$(jq '.passed' "$out/mode-b-${engine}-${mode}.json")" == 0 ]]; then
  echo "::error::The default layout passed no case, so the $# layout run(s) would compare nothing; skipped them." >&2
  exit 1
fi

for layout in "$@"; do
  run "mode-b-${engine}-${mode}-${layout//,/+}" "${serve[@]}" --layout "$layout" \
    --baseline "$out/mode-b-${engine}-${mode}.json"
done

if [[ ${#failed[@]} -gt 0 ]]; then
  echo "Failed: ${failed[*]}" >&2
  exit 1
fi
echo "Every run passed what its baseline passed."
