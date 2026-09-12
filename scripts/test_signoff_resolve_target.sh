#!/usr/bin/env bash
#
# Unit tests for the `Resolve sign-off target` step of
# `.github/workflows/signoff.yml` — the step whose `head_sha` output keys the
# `sign-off` job's concurrency group.
#
# That key is the whole point. `concurrency` is evaluated before any step runs,
# so a workflow-level group can only see the raw dispatch input: `-f branch=X`
# and `-f pr_number=N` for the same branch produced `signoff-X` and `signoff-N`,
# two different groups, so neither cancelled the other. Two 1-4h runs then held
# two slots of the shared self-hosted pool for one commit and both posted the
# `signoff` status on it, last write winning (#12472). Resolving in its own job
# lets the group key on the commit instead, which both input forms reduce to —
# so the load-bearing assertion here is that the two forms agree on `head_sha`
# for the same branch.
#
# The step is inline YAML rather than a function in scripts/signoff, so this
# extracts the real `run:` block from the workflow and executes it. Extracting
# (rather than copying it here) is what makes the test bind to the shipping
# definition: a change to the step that breaks resolution fails this test.
#
# No network and no credentials: a stub `gh` on PATH answers the two lookups.
#
# Usage: scripts/test_signoff_resolve_target.sh

set -uo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/.." && pwd)"
workflow="$repo_root/.github/workflows/signoff.yml"

tests_run=0
failures=0

fail_test() {
  failures=$((failures + 1))
  echo "  FAIL: $1"
}

command -v python3 >/dev/null 2>&1 || { echo "python3 not found — required to extract the step"; exit 1; }
command -v jq >/dev/null 2>&1 || { echo "jq not found — the step under test uses it"; exit 1; }
# Checked separately from python3 itself: without this the extraction below fails
# as an ImportError traceback, which reads like a bug in the test rather than a
# missing dependency.
python3 -c 'import yaml' 2>/dev/null \
  || { echo "python3 cannot import yaml — required to extract the step (pip install pyyaml)"; exit 1; }

stub_dir="$(mktemp -d)"
trap 'rm -rf "$stub_dir"' EXIT

subject="$stub_dir/resolve_step.sh"

# Pull the step's `run:` script out of the workflow by name, and with it the
# `env:` keys it declares — asserting those are exactly the three this test
# supplies, so a new input added to the step cannot be silently untested.
#
# The script embeds `${{ github.actor }}`, which bash cannot expand, so the two
# recognised expressions are substituted with fixed values. Any *other* `${{ }}`
# left in the body is a hard error: it would mean the step grew a template this
# test does not model, and letting it through would exercise a script that is
# not the one CI runs.
python3 - "$workflow" "$subject" <<'PY' || exit 1
import io, re, sys
import yaml

workflow_path, out_path = sys.argv[1], sys.argv[2]
doc = yaml.safe_load(io.open(workflow_path, encoding="utf-8"))

steps = doc["jobs"]["resolve"]["steps"]
matches = [s for s in steps if s.get("name") == "Resolve branch/PR input"]
if len(matches) != 1:
    sys.exit(f"expected exactly 1 'Resolve branch/PR input' step in the resolve job, found {len(matches)}")
step = matches[0]

declared = sorted(step.get("env", {}))
expected = ["BASE_REPO", "BRANCH", "PR_NUMBER"]
if declared != expected:
    sys.exit(f"step env keys changed: expected {expected}, found {declared} — update this test")

body = step["run"]
body = body.replace("${{ github.actor }}", "test-actor")
leftover = re.findall(r"\$\{\{.*?\}\}", body)
if leftover:
    sys.exit(f"step body has unmodelled template expressions: {leftover} — update this test")

io.open(out_path, "w", encoding="utf-8", newline="\n").write("#!/usr/bin/env bash\n" + body)
PY
chmod +x "$subject"

# A `gh` covering exactly the two calls the step makes:
#
#   pr view <N> --repo <repo> --json …   -> STUB_PR_JSON, or exit STUB_PR_RC
#   api repos/<repo>/commits/<ref> --jq .sha -> STUB_BRANCH_SHA, or exit STUB_API_RC
#
# Any other invocation is a hard error: several cases assert the step rejects an
# input *before* reaching the network, and a stub that quietly served an
# unexpected call would let a regression pass.
cat >"$stub_dir/gh" <<'STUB'
#!/usr/bin/env bash
set -uo pipefail

if [[ "${1:-}" == "pr" && "${2:-}" == "view" ]]; then
  [[ "${STUB_PR_RC:-0}" == "0" ]] || exit "${STUB_PR_RC}"
  printf '%s' "${STUB_PR_JSON:-}"
  exit 0
fi

if [[ "${1:-}" == "api" && "${2:-}" == repos/*/commits/* ]]; then
  printf '%s' "$2" >"${STUB_LOG_API_ENDPOINT:-/dev/null}"
  [[ "${STUB_API_RC:-0}" == "0" ]] || exit "${STUB_API_RC}"
  printf '%s\n' "${STUB_BRANCH_SHA:-}"
  exit 0
fi

echo "stub gh: unexpected invocation: $*" >&2
exit 99
STUB
chmod +x "$stub_dir/gh"

PR_SHA="1111111111111111111111111111111111111111"
BRANCH_SHA="2222222222222222222222222222222222222222"

# A `gh pr view --json …` payload for the fields the step reads.
pr_json() {
  local state="$1" cross="$2" owner="$3" name="$4" ref="$5" oid="$6"
  jq -nc --arg s "$state" --argjson x "$cross" --arg o "$owner" \
    --arg n "$name" --arg r "$ref" --arg oid "$oid" \
    '{state:$s, isCrossRepository:$x, headRepositoryOwner:{login:$o},
      headRepository:{name:$n}, headRefName:$r, headRefOid:$oid}'
}

# Run the extracted step with the given dispatch inputs, capturing its exit
# status, its combined output, and the step outputs it wrote to $GITHUB_OUTPUT.
# Sets: rc, out, and out_<name> for each resolved output.
run_step() {
  local branch="$1" pr_number="$2"
  local outfile="$stub_dir/github_output"
  : >"$outfile"

  : >"$stub_dir/api_endpoint"
  out=$(
    PATH="$stub_dir:$PATH" \
    BRANCH="$branch" \
    PR_NUMBER="$pr_number" \
    BASE_REPO="spiceai/spiceai" \
    GITHUB_OUTPUT="$outfile" \
    STUB_PR_JSON="${STUB_PR_JSON:-}" \
    STUB_PR_RC="${STUB_PR_RC:-0}" \
    STUB_BRANCH_SHA="${STUB_BRANCH_SHA:-}" \
    STUB_API_RC="${STUB_API_RC:-0}" \
    STUB_LOG_API_ENDPOINT="$stub_dir/api_endpoint" \
    bash "$subject" 2>&1
  )
  rc=$?
  api_endpoint="$(cat "$stub_dir/api_endpoint" 2>/dev/null || true)"

  local key
  for key in checkout_repo checkout_ref is_fork head_sha; do
    printf -v "out_$key" '%s' "$(sed -n "s/^${key}=//p" "$outfile" | tail -1)"
  done
}

# `printf -v` is a bashism for indirect assignment; keep a portable fallback for
# shells that reject it so a failure here is loud rather than a silent empty.
if ! printf -v _probe '%s' ok 2>/dev/null || [[ "${_probe:-}" != "ok" ]]; then
  echo "this test needs a bash whose printf supports -v"; exit 1
fi

assert_eq() {
  local label="$1" want="$2" got="$3"
  [[ "$got" == "$want" ]] || fail_test "$label: expected '$want', got '$got'"
}

echo "Testing the sign-off target resolution step"
echo

# --- the regression test for #12472 -----------------------------------------
# Both dispatch forms for the same branch must land on one commit, because that
# commit is the concurrency key. If these two disagree, the two forms sit in
# different groups again and neither cancels the other.
tests_run=$((tests_run + 1))
echo "both dispatch forms resolve the same branch to the same commit"
STUB_PR_JSON="$(pr_json OPEN false spiceai spiceai fix/my-branch "$BRANCH_SHA")" \
  STUB_PR_RC=0 STUB_BRANCH_SHA="$BRANCH_SHA" STUB_API_RC=0 \
  run_step "" "1234"
via_pr="$out_head_sha"
[[ $rc -eq 0 ]] || fail_test "pr_number form exited $rc: $out"

STUB_PR_JSON="" STUB_PR_RC=0 STUB_BRANCH_SHA="$BRANCH_SHA" STUB_API_RC=0 \
  run_step "fix/my-branch" ""
via_branch="$out_head_sha"
[[ $rc -eq 0 ]] || fail_test "branch form exited $rc: $out"

assert_eq "head_sha via pr_number" "$BRANCH_SHA" "$via_pr"
assert_eq "head_sha via branch" "$BRANCH_SHA" "$via_branch"
assert_eq "the two forms agree" "$via_pr" "$via_branch"

# --- the pr_number form ------------------------------------------------------
tests_run=$((tests_run + 1))
echo "a same-repo PR pins checkout_ref to the head commit"
STUB_PR_JSON="$(pr_json OPEN false spiceai spiceai fix/branch "$PR_SHA")" \
  run_step "" "99"
[[ $rc -eq 0 ]] || fail_test "exited $rc: $out"
assert_eq "checkout_repo" "spiceai/spiceai" "$out_checkout_repo"
assert_eq "checkout_ref" "$PR_SHA" "$out_checkout_ref"
assert_eq "is_fork" "false" "$out_is_fork"
assert_eq "head_sha" "$PR_SHA" "$out_head_sha"

tests_run=$((tests_run + 1))
echo "a fork PR reports is_fork and still pins the commit"
STUB_PR_JSON="$(pr_json OPEN true contributor spiceai fix/theirs "$PR_SHA")" \
  run_step "" "99"
[[ $rc -eq 0 ]] || fail_test "exited $rc: $out"
assert_eq "checkout_repo" "contributor/spiceai" "$out_checkout_repo"
assert_eq "checkout_ref" "$PR_SHA" "$out_checkout_ref"
assert_eq "is_fork" "true" "$out_is_fork"
assert_eq "head_sha" "$PR_SHA" "$out_head_sha"
grep -q "FORK" <<<"$out" || fail_test "expected a fork warning in: $out"

tests_run=$((tests_run + 1))
echo "a closed PR warns but still resolves"
STUB_PR_JSON="$(pr_json CLOSED false spiceai spiceai fix/old "$PR_SHA")" \
  run_step "" "99"
[[ $rc -eq 0 ]] || fail_test "exited $rc: $out"
assert_eq "head_sha" "$PR_SHA" "$out_head_sha"
grep -q "not OPEN" <<<"$out" || fail_test "expected a state warning in: $out"

tests_run=$((tests_run + 1))
echo "a failed PR lookup fails the step"
STUB_PR_RC=1 run_step "" "99"
[[ $rc -ne 0 ]] || fail_test "expected a non-zero exit, got 0: $out"
grep -q "could not look up PR" <<<"$out" || fail_test "expected a lookup error in: $out"

# `jq -r` renders a missing field as "null", which is what a PR whose head
# repository has been deleted actually returns. Unvalidated, that string becomes
# the concurrency key, so every such PR would share one group.
tests_run=$((tests_run + 1))
echo "a PR whose head commit does not resolve is rejected, not keyed on 'null'"
STUB_PR_JSON='{"state":"OPEN","isCrossRepository":true,"headRepositoryOwner":null,"headRepository":null,"headRefName":null,"headRefOid":null}' \
  run_step "" "99"
[[ $rc -ne 0 ]] || fail_test "expected a non-zero exit, got 0: $out"
grep -q "not a commit SHA" <<<"$out" || fail_test "expected a SHA-shape error in: $out"

tests_run=$((tests_run + 1))
echo "a non-numeric pr_number is rejected before any lookup"
run_step "" "12; rm -rf /"
[[ $rc -ne 0 ]] || fail_test "expected a non-zero exit, got 0: $out"
grep -q "positive integer" <<<"$out" || fail_test "expected a validation error in: $out"

# --- the branch form ---------------------------------------------------------
tests_run=$((tests_run + 1))
echo "a branch keeps checkout_ref as the branch name, and resolves head_sha"
STUB_BRANCH_SHA="$BRANCH_SHA" run_step "feature/foo.bar-1" ""
[[ $rc -eq 0 ]] || fail_test "exited $rc: $out"
assert_eq "checkout_repo" "spiceai/spiceai" "$out_checkout_repo"
assert_eq "checkout_ref" "feature/foo.bar-1" "$out_checkout_ref"
assert_eq "is_fork" "false" "$out_is_fork"
assert_eq "head_sha" "$BRANCH_SHA" "$out_head_sha"

tests_run=$((tests_run + 1))
echo "an unpushed branch fails with a legible error, not a late checkout failure"
STUB_API_RC=1 run_step "fix/never-pushed" ""
[[ $rc -ne 0 ]] || fail_test "expected a non-zero exit, got 0: $out"
grep -q "could not resolve branch" <<<"$out" || fail_test "expected a resolve error in: $out"

tests_run=$((tests_run + 1))
echo "a lookup that answers with something other than a commit SHA fails"
STUB_BRANCH_SHA="not-a-sha" run_step "fix/weird" ""
[[ $rc -ne 0 ]] || fail_test "expected a non-zero exit, got 0: $out"
grep -q "not a commit SHA" <<<"$out" || fail_test "expected a SHA-shape error in: $out"

# `#` is a legal git ref character (e.g. the issue-#123-* branches this repo's
# worktree convention produces) but also a URL fragment delimiter — unencoded,
# gh's commits-API call silently truncates the ref at it and 404s on the wrong
# thing. checkout_ref (what gets attested and checked out) must stay the raw
# branch name; only the lookup URL needs encoding.
tests_run=$((tests_run + 1))
echo "a branch containing '#' is accepted and percent-encoded for the lookup"
STUB_BRANCH_SHA="$BRANCH_SHA" run_step "issue-#13038-20260818163736" ""
[[ $rc -eq 0 ]] || fail_test "exited $rc: $out"
assert_eq "checkout_ref" "issue-#13038-20260818163736" "$out_checkout_ref"
assert_eq "head_sha" "$BRANCH_SHA" "$out_head_sha"
[[ "$api_endpoint" == *"%23"* ]] || fail_test "expected the lookup endpoint to percent-encode '#', got: $api_endpoint"
[[ "$api_endpoint" != *"#"* ]] || fail_test "raw '#' reached gh api unencoded, would truncate as a URL fragment: $api_endpoint"

# A branch name that would break the dispatch or the checkout must be rejected
# by the step's own validation, before the stub `gh` is ever reached — the stub
# hard-errors on an unexpected call, so reaching it shows up as a failure here.
for bad in "fix/../etc" "fix/\$(id)" 'fix/`id`' "-fix/leading-dash" "/abs/path" "fix/with space" "fix/quote'x" 'fix/dquote"x' "fix/semi;x"; do
  tests_run=$((tests_run + 1))
  echo "rejects the branch name '$bad'"
  run_step "$bad" ""
  [[ $rc -ne 0 ]] || fail_test "expected a non-zero exit, got 0: $out"
  grep -q "invalid branch name" <<<"$out" || fail_test "expected a validation error in: $out"
done

# --- mutually exclusive inputs ----------------------------------------------
tests_run=$((tests_run + 1))
echo "both inputs at once is rejected"
run_step "fix/branch" "99"
[[ $rc -ne 0 ]] || fail_test "expected a non-zero exit, got 0: $out"
grep -q "not both" <<<"$out" || fail_test "expected an exclusivity error in: $out"

tests_run=$((tests_run + 1))
echo "neither input is rejected"
run_step "" ""
[[ $rc -ne 0 ]] || fail_test "expected a non-zero exit, got 0: $out"
grep -q "is required" <<<"$out" || fail_test "expected a missing-input error in: $out"

# Whitespace-only inputs are the same as absent: the step tests "${VAR// }".
tests_run=$((tests_run + 1))
echo "whitespace-only inputs count as absent"
run_step "   " "  "
[[ $rc -ne 0 ]] || fail_test "expected a non-zero exit, got 0: $out"
grep -q "is required" <<<"$out" || fail_test "expected a missing-input error in: $out"

echo
if [[ $failures -eq 0 ]]; then
  echo "All $tests_run test group(s) passed."
  exit 0
fi
echo "$failures assertion(s) failed across $tests_run test group(s)."
exit 1
