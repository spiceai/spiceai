#!/usr/bin/env bash
# Copyright 2024-2026 The Spice.ai OSS Authors
#
# Unit tests for `.github/scripts/approve_bot_pr_runs.sh`, which approves the
# held `pull_request` runs on a commit a workflow just pushed with GITHUB_TOKEN.
#
# Approving a run starts CI on code nobody reviewed, so the cases that must NOT
# be approved matter as much as the ones that must: another commit, a fork, a
# run a person triggered, a run that is not held.
#
# No network: a stub `gh` on PATH answers each listing from a fixture, applying
# the script's own `--jq` filter with the real `jq`, and records every approval.
#
# Usage: .github/scripts/test_approve_bot_pr_runs.sh

set -uo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
subject="$script_dir/approve_bot_pr_runs.sh"

command -v jq >/dev/null || { echo "jq is required to run these tests" >&2; exit 2; }

tests_run=0
failures=0

fail_test() {
  failures=$((failures + 1))
  echo "  FAIL: $1"
}

stub_dir="$(mktemp -d)"
work_dir="$(mktemp -d)"
trap 'rm -rf "$stub_dir" "$work_dir"' EXIT

# Listing N (1-based) is answered from $work_dir/listing.N.json, the last one
# repeating; $work_dir/listing.fail makes every listing fail. An approval
# appends the run ID to $work_dir/approved and fails for IDs in $work_dir/refuse.
cat >"$stub_dir/gh" <<'STUB'
#!/usr/bin/env bash
set -euo pipefail
dir="$STUB_WORK_DIR"
args=("$@")
if [ "${args[0]}" = api ] && [ "${args[1]}" = -X ] && [ "${args[2]}" = POST ]; then
  id="$(sed -E 's|.*/runs/([0-9]+)/approve$|\1|' <<<"${args[3]}")"
  if grep -qx "$id" "$dir/refuse" 2>/dev/null; then
    echo "HTTP 403: Resource not accessible by integration" >&2
    exit 1
  fi
  echo "$id" >>"$dir/approved"
  echo '{}'
  exit 0
fi
if [ -e "$dir/listing.fail" ]; then
  echo "HTTP 502: Bad Gateway" >&2
  exit 1
fi
filter=""
for ((i = 0; i < ${#args[@]}; i++)); do
  if [ "${args[$i]}" = --jq ]; then filter="${args[$((i + 1))]}"; fi
done
n=$(( $(cat "$dir/polls" 2>/dev/null || echo 0) + 1 ))
echo "$n" >"$dir/polls"
fixture="$dir/listing.$n.json"
while [ ! -e "$fixture" ] && [ "$n" -gt 1 ]; do
  n=$((n - 1))
  fixture="$dir/listing.$n.json"
done
# `tr` drops the CR a Windows jq ends lines with; gh on a runner prints LF.
jq -r "$filter" "$fixture" | tr -d '\r'
STUB
chmod +x "$stub_dir/gh"

SHA=1111111111111111111111111111111111111111
OTHER_SHA=2222222222222222222222222222222222222222
BRANCH=snapshot-update/test-hf-123
REPO=spiceai/spiceai

# run_json <id> <conclusion> [sha] [head_repo] [actor] [event] [branch]
run_json() {
  printf '{"id":%s,"conclusion":"%s","head_sha":"%s","head_repository":{"full_name":"%s"},"triggering_actor":{"login":"%s"},"event":"%s","head_branch":"%s"}' \
    "$1" "$2" "${3:-$SHA}" "${4:-$REPO}" "${5:-github-actions[bot]}" "${6:-pull_request}" "${7:-$BRANCH}"
}

listing() {
  local file="$work_dir/listing.$1.json"
  shift
  local IFS=,
  printf '{"workflow_runs":[%s]}' "$*" >"$file"
}

reset() {
  rm -f "$work_dir"/*
}

run_subject() {
  STUB_WORK_DIR="$work_dir" GITHUB_REPOSITORY="$REPO" \
    APPROVE_WAIT_SECONDS="${WAIT:-3}" APPROVE_POLL_SECONDS=0.05 \
    PATH="$stub_dir:$PATH" bash "$subject" "$@" 2>&1
}

approved() {
  if [ -e "$work_dir/approved" ]; then tr '\n' ' ' <"$work_dir/approved" | sed 's/ $//'; fi
}

expect_eq() {
  if [ "$2" != "$3" ]; then fail_test "$1: expected '$3', got '$2'"; fi
}

expect_contains() {
  if ! grep -qF -- "$3" <<<"$2"; then fail_test "$1: output lacks '$3':"$'\n'"$2"; fi
}

begin() {
  tests_run=$((tests_run + 1))
  echo "- $1"
  reset
}

begin "approves only the held runs on the pushed commit"
listing 1 \
  "$(run_json 10 action_required)" \
  "$(run_json 11 action_required)" \
  "$(run_json 20 success)" \
  "$(run_json 30 action_required "$OTHER_SHA")" \
  "$(run_json 31 action_required "$SHA" someone/fork)" \
  "$(run_json 32 action_required "$SHA" "$REPO" a-person)" \
  "$(run_json 33 action_required "$SHA" "$REPO" 'github-actions[bot]' pull_request_target)" \
  "$(run_json 34 action_required "$SHA" "$REPO" 'github-actions[bot]' pull_request other/branch)"
out="$(run_subject "$BRANCH" "$SHA")"; rc=$?
expect_eq "exit code" "$rc" 0
expect_eq "approved runs" "$(approved)" "10 11"
expect_contains "summary" "$out" "Approved 2 held run(s)"

begin "waits for runs created after the push and approves each once"
listing 1
listing 2 "$(run_json 10 action_required)"
listing 3 "$(run_json 10 action_required)" "$(run_json 11 action_required)"
out="$(run_subject "$BRANCH" "$SHA")"; rc=$?
expect_eq "exit code" "$rc" 0
expect_eq "approved runs" "$(approved)" "10 11"

begin "a refused approval warns and does not fail the job"
listing 1 "$(run_json 10 action_required)" "$(run_json 11 action_required)"
echo 10 >"$work_dir/refuse"
out="$(run_subject "$BRANCH" "$SHA")"; rc=$?
expect_eq "exit code" "$rc" 0
expect_eq "approved runs" "$(approved)" "11"
expect_contains "warning" "$out" "::warning::GitHub refused to approve held run 10"
expect_contains "summary" "$out" "1 refused"

begin "a failed listing warns, approves nothing, and does not fail the job"
touch "$work_dir/listing.fail"
out="$(run_subject "$BRANCH" "$SHA")"; rc=$?
expect_eq "exit code" "$rc" 0
expect_eq "approved runs" "$(approved)" ""
expect_contains "warning" "$out" "::warning::Could not list the pull_request runs"
expect_contains "cause" "$out" "HTTP 502"

begin "no run within the wait is a notice, not an error"
listing 1
out="$(WAIT=1 run_subject "$BRANCH" "$SHA")"; rc=$?
expect_eq "exit code" "$rc" 0
expect_eq "approved runs" "$(approved)" ""
expect_contains "notice" "$out" "::notice::No pull_request run appeared"

begin "rejects a SHA that is not a full commit SHA"
listing 1 "$(run_json 10 action_required)"
out="$(run_subject "$BRANCH" "abc123")"; rc=$?
expect_eq "exit code" "$rc" 2
expect_eq "approved runs" "$(approved)" ""

begin "approves on a branch named after a Spicepod path with '+'"
PLUS_BRANCH='testoperator-cluster-snapshot-update/123-accelerated-file-parquet-mixed-duckdb-file-+sqlite-file-.yaml'
listing 1 \
  "$(run_json 10 action_required "$SHA" "$REPO" 'github-actions[bot]' pull_request "$PLUS_BRANCH")" \
  "$(run_json 11 action_required "$SHA" "$REPO" 'github-actions[bot]' pull_request "$BRANCH")"
out="$(run_subject "$PLUS_BRANCH" "$SHA")"; rc=$?
expect_eq "exit code" "$rc" 0
expect_eq "approved runs" "$(approved)" "10"

begin "rejects a branch that could break out of the filter's quotes"
listing 1 "$(run_json 10 action_required)"
out="$(run_subject 'x" or true or "' "$SHA")"; rc=$?
expect_eq "exit code" "$rc" 2
expect_eq "approved runs" "$(approved)" ""

echo
if [ "$failures" -ne 0 ]; then
  echo "$failures failure(s) in $tests_run test(s)"
  exit 1
fi
echo "All $tests_run tests passed"
