#!/usr/bin/env bash
# Copyright 2024-2026 The Spice.ai OSS Authors
#
# Unit tests for `.github/scripts/save_unpushable_snapshots.sh`.
#
# The script must recognise exactly one refusal: GitHub timing out on whether a
# new branch adds workflow files. The refusals next to it (a real workflow-file
# change, a merge-queue lock, a lost race) mean something else, so they must be
# left to the caller, and are tested as carefully as the one that matches.
#
# No network: each case runs against a throwaway repository whose HEAD is a
# snapshot commit, with push logs copied from real runs.
#
# Usage: .github/scripts/test_save_unpushable_snapshots.sh

set -uo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
subject="$script_dir/save_unpushable_snapshots.sh"

tests_run=0
failures=0

fail_test() {
  failures=$((failures + 1))
  echo "  FAIL: $1"
}

work_dir="$(mktemp -d)"
trap 'rm -rf "$work_dir"' EXIT

repo="$work_dir/repo"
git init -q "$repo"
git -C "$repo" -c user.name=t -c user.email=t@example.invalid commit -q --allow-empty -m base
mkdir -p "$repo/snapshots"
echo "q1 plan" >"$repo/snapshots/q1.snap"
git -C "$repo" add snapshots/q1.snap
git -C "$repo" -c user.name=t -c user.email=t@example.invalid commit -q -m "fix: Update snapshots"
base_sha="$(git -C "$repo" rev-parse HEAD~1)"

# Runs the subject with the given push log; leaves its exit status in $status,
# its stdout+stderr in $output, and its temp dir and step summary under $case_dir.
run_case() {
  local log_text="$1"
  case_dir="$(mktemp -d "$work_dir/case.XXXXXX")"
  printf '%s\n' "$log_text" >"$case_dir/push.log"
  : >"$case_dir/summary.md"
  output="$(cd "$repo" && RUNNER_TEMP="$case_dir/tmp" GITHUB_RUN_ID=4242 GITHUB_REPOSITORY=spiceai/spiceai \
    GITHUB_STEP_SUMMARY="$case_dir/summary.md" \
    bash "$subject" "$case_dir/push.log" testoperator-snapshot-update/abc release/2.4 2>&1)"
  status=$?
}

expect_ignored() {
  local name="$1"
  tests_run=$((tests_run + 1))
  if [ "$status" -ne 1 ]; then
    fail_test "$name: expected exit 1 (not this refusal), got $status: $output"
  elif [ -e "$case_dir/tmp/unpushed-snapshots/snapshots.patch" ]; then
    fail_test "$name: saved a patch for a refusal it should leave to the caller"
  elif [ -s "$case_dir/summary.md" ]; then
    fail_test "$name: wrote a step summary for a refusal it should leave to the caller"
  else
    echo "  ok: $name"
  fi
}

echo "save_unpushable_snapshots.sh"

# Positive: the refusal from run 42580263706 (a release/2.4-based dispatch).
run_case ' ! [remote rejected]       testoperator-snapshot-update/abc -> testoperator-snapshot-update/abc (Unable to determine if workflow can be created or updated due to timeout; `workflows` scope may be required.)
error: failed to push some refs to '"'"'https://github.com/spiceai/spiceai'"'"''
tests_run=$((tests_run + 1))
patch="$case_dir/tmp/unpushed-snapshots/snapshots.patch"
if [ "$status" -ne 0 ]; then
  fail_test "workflow-check timeout: expected exit 0, got $status: $output"
elif [ ! -s "$patch" ]; then
  fail_test "workflow-check timeout: no patch at $patch"
elif ! grep -q 'snapshots/q1.snap' "$patch" || ! grep -q '+q1 plan' "$patch"; then
  fail_test "workflow-check timeout: the patch does not carry the snapshot change"
elif ! grep -q '^::error::GitHub refused to let this workflow' <<<"$output"; then
  fail_test "workflow-check timeout: no ::error naming the cause: $output"
elif ! grep -qF 'gh run download 4242 --repo spiceai/spiceai --name unpushed-snapshots' "$case_dir/summary.md" \
  || ! grep -qF "git switch -c testoperator-snapshot-update/abc $base_sha" "$case_dir/summary.md" \
  || ! grep -qF -- '--base release/2.4' "$case_dir/summary.md"; then
  fail_test "workflow-check timeout: the summary does not say how to land the patch: $(cat "$case_dir/summary.md")"
else
  echo "  ok: workflow-check timeout saves the commit and names the cause"
fi

# The patch must apply onto the commit the summary tells people to branch from.
tests_run=$((tests_run + 1))
clone="$work_dir/clone"
git clone -q "$repo" "$clone"
if ! git -C "$clone" switch -q -c landed "$base_sha" \
  || ! git -C "$clone" -c user.name=t -c user.email=t@example.invalid am -q "$patch" \
  || [ "$(cat "$clone/snapshots/q1.snap")" != "q1 plan" ]; then
  fail_test "the saved patch does not apply onto its parent commit"
else
  echo "  ok: the saved patch applies onto its parent commit"
fi

# Negative: the commit really changes a workflow file. Same family, different cause.
run_case ' ! [remote rejected] HEAD -> testoperator-snapshot-update/abc (refusing to allow a GitHub App to create or update workflow `.github/workflows/x.yml` without `workflows` permission)'
expect_ignored "a real workflow-file change is not this refusal"

# Negative: the branch's PR is in the merge queue.
run_case ' ! [remote rejected] testoperator-snapshot-update/abc -> testoperator-snapshot-update/abc (protected branch hook declined)
remote: error: GH006: Protected branch update failed for refs/heads/testoperator-snapshot-update/abc.
remote: error: Changes must be made through the merge queue: this branch is queued for merging'
expect_ignored "a merge-queue lock is not this refusal"

# Negative: another run pushed first.
run_case ' ! [rejected]        testoperator-snapshot-update/abc -> testoperator-snapshot-update/abc (fetch first)'
expect_ignored "a lost push race is not this refusal"

# Usage: an unreadable log is an error, not a silent "not this refusal".
tests_run=$((tests_run + 1))
output="$(cd "$repo" && RUNNER_TEMP="$work_dir/usage" bash "$subject" "$work_dir/missing.log" b release/2.4 2>&1)"
status=$?
if [ "$status" -ne 2 ]; then
  fail_test "a missing push log: expected exit 2, got $status: $output"
else
  echo "  ok: a missing push log is a usage error"
fi

echo
echo "$tests_run tests, $failures failed"
[ "$failures" -eq 0 ]
