#!/usr/bin/env bash
#
# Unit tests for the `Save trusted CI helpers` step of
# `.github/workflows/signoff.yml` — the step that materialises
# `.trusted-ci/signoff` and `.trusted-ci/actions/`, the copies every later step
# in the sign-off job runs instead of the working tree's.
#
# Two properties are load-bearing, and both are about *which commit* the copies
# come from:
#
#   1. Version agreement. The call sites for these helpers live in the workflow
#      file, which always comes from `github.sha`. The same-repo checkout is at
#      the branch under test, so copying from the tree let a branch present an
#      older helper to newer call sites: a branch predating
#      `scripts/signoff correct-cancelled` (#12432) supplied a script without
#      it, the `if: cancelled()` step died on "unknown command", and the
#      cancelled run kept the `signoff=failure` it had already posted — a red
#      Attestation on code nothing judged (#12657).
#
#   2. Trust. The copies must not be reachable by the code under test. For a
#      fork PR that is the whole point of `.trusted-ci/`; for a same-repo PR it
#      means the branch cannot supply the script that decides its own sign-off.
#
# The step is inline YAML rather than a function in scripts/signoff, and it has
# to stay that way: extracting it to a file in the repo would make the *saver*
# itself come from the branch under test, which is the bug. So this extracts the
# real `run:` block from the workflow and executes it. Extracting (rather than
# copying it here) is what makes the test bind to the shipping definition.
#
# No network and no credentials: the fixtures are local git repositories, and
# `origin` is a second local repository on disk.
#
# Usage: scripts/test_signoff_trusted_helpers.sh

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
command -v git >/dev/null 2>&1 || { echo "git not found — the step under test uses it"; exit 1; }
command -v tar >/dev/null 2>&1 || { echo "tar not found — the step under test uses it"; exit 1; }
# Checked separately from python3 itself: without this the extraction below
# fails as an ImportError traceback, which reads like a bug in the test rather
# than a missing dependency.
python3 -c 'import yaml' 2>/dev/null \
  || { echo "python3 cannot import yaml — required to extract the step (pip install pyyaml)"; exit 1; }

work_dir="$(mktemp -d)"
trap 'rm -rf "$work_dir"' EXIT

subject="$work_dir/save_trusted_helpers.sh"

# Pull the step's `run:` script out of the workflow by name, and with it the
# `env:` keys it declares — asserting those are exactly the one this test
# supplies, so a new input added to the step cannot be silently untested.
#
# Any `${{ }}` left in the body is a hard error: it would mean the step reads a
# template this test does not model, and letting it through would exercise a
# script that is not the one CI runs.
python3 - "$workflow" "$subject" <<'PY' || exit 1
import io, re, sys
import yaml

workflow_path, out_path = sys.argv[1], sys.argv[2]
doc = yaml.safe_load(io.open(workflow_path, encoding="utf-8"))

steps = doc["jobs"]["sign-off"]["steps"]
matches = [s for s in steps if s.get("name") == "Save trusted CI helpers"]
if len(matches) != 1:
    sys.exit(f"expected exactly 1 'Save trusted CI helpers' step in the sign-off job, found {len(matches)}")
step = matches[0]

declared = sorted(step.get("env", {}))
expected = ["TRUSTED_SHA"]
if declared != expected:
    sys.exit(f"step env keys changed: expected {expected}, found {declared} — update this test")

body = step["run"]
leftover = re.findall(r"\$\{\{.*?\}\}", body)
if leftover:
    sys.exit(f"step body has unmodelled template expressions: {leftover} — update this test")

io.open(out_path, "w", encoding="utf-8", newline="\n").write("#!/usr/bin/env bash\n" + body)
PY
chmod +x "$subject"

git_quiet() { git "$@" >/dev/null 2>&1; }

# Build the pair of repositories the step sees on a runner:
#
#   origin/   a bare-ish repo standing in for spiceai/spiceai, holding both the
#             OLD commit (a branch predating the helper change) and the NEW one
#             (the workflow file's own commit).
#   clone/    the workspace, checked out at OLD — i.e. actions/checkout at
#             `ref: <branch under test>`, which does not fetch other refs.
#
# Returns the two SHAs in $old_sha / $new_sha and the workspace in $clone.
#
# `$1` selects what the clone fetches, so the "already present" and "must be
# fetched" paths can both be exercised: `shallow-refs` clones only the old
# branch (the realistic same-repo case), `all-refs` clones everything (the fork
# case, where github.sha is the ref already checked out).
build_fixture() {
  local fetch_mode="$1"
  # How the NEW (trusted) commit's setup-rust names its nested action. Defaults
  # to the one spelling every action in this repo actually uses; the guard cases
  # below pass spellings the step's rewrite does not handle, so that the guard
  # is what has to stop the run.
  local nested_ref="${2:-./.github/actions/configure-apt}"
  local root="$work_dir/fixture"
  rm -rf "$root"
  mkdir -p "$root"
  origin="$root/origin"
  clone="$root/clone"

  mkdir -p "$origin"
  (
    cd "$origin" || exit 1
    git_quiet init -b trunk
    git_quiet config user.email test@example.com
    git_quiet config user.name "Test"
    mkdir -p scripts .github/actions/setup-rust .github/actions/configure-apt

    # The OLD helper: no `correct-cancelled`, exactly the shape that produced
    # #12657.
    printf '#!/usr/bin/env bash\necho OLD-SIGNOFF\n' >scripts/signoff
    chmod +x scripts/signoff
    printf 'name: setup-rust\n# OLD-ACTION\n' >.github/actions/setup-rust/action.yml
    printf 'name: configure-apt\n# OLD-NESTED\n' >.github/actions/configure-apt/action.yml
    git_quiet add -A
    git_quiet commit -m "old"
    git_quiet branch branch-under-test

    # The NEW helper, on trunk: this is what github.sha points at. Its action
    # nests another action the way setup-cc/setup-make/setup-sccache nest
    # configure-apt — the shape whose `uses: ./…` resolves from the workspace
    # root rather than the action's own directory.
    printf '#!/usr/bin/env bash\ncase "${1:-}" in correct-cancelled) echo NEW-SIGNOFF;; esac\n' >scripts/signoff
    printf 'name: setup-rust\n# NEW-ACTION\nruns:\n  using: composite\n  steps:\n    - uses: %s\n' \
      "$nested_ref" >.github/actions/setup-rust/action.yml
    printf 'name: configure-apt\n# NEW-NESTED\n' >.github/actions/configure-apt/action.yml
    git_quiet add -A
    git_quiet commit -m "new"
  ) || return 1

  old_sha="$(git -C "$origin" rev-parse branch-under-test)"
  new_sha="$(git -C "$origin" rev-parse trunk)"

  if [[ "$fetch_mode" == "all-refs" ]]; then
    git_quiet clone "$origin" "$clone"
  else
    git_quiet clone --branch branch-under-test --single-branch "$origin" "$clone"
  fi
  git -C "$clone" checkout --detach "$old_sha" >/dev/null 2>&1
  git -C "$clone" config user.email test@example.com
  git -C "$clone" config user.name "Test"
}

# Run the extracted step in the fixture workspace with the given TRUSTED_SHA.
# Sets: rc, out.
run_step() {
  local trusted_sha="$1"
  out=$(cd "$clone" && TRUSTED_SHA="$trusted_sha" bash "$subject" 2>&1)
  rc=$?
}

assert_contains() {
  local label="$1" needle="$2" file="$3"
  tests_run=$((tests_run + 1))
  if [[ ! -f "$file" ]]; then
    fail_test "$label: $file does not exist"
    return
  fi
  grep -q -- "$needle" "$file" || fail_test "$label: expected '$needle' in $file, got: $(cat "$file")"
}

assert_absent() {
  local label="$1" needle="$2" file="$3"
  tests_run=$((tests_run + 1))
  if [[ ! -f "$file" ]]; then
    fail_test "$label: $file does not exist"
    return
  fi
  ! grep -q -- "$needle" "$file" || fail_test "$label: '$needle' must not appear in $file"
}

echo "Testing the sign-off trusted-helper materialisation step"
echo

# --- the regression test for #12657 -----------------------------------------
# The workspace is at a branch whose scripts/signoff has no `correct-cancelled`.
# The trusted copy must still be the one the workflow's call sites expect.
echo "the trusted script comes from the workflow's commit, not the checked-out branch"
build_fixture shallow-refs || { echo "fixture setup failed"; exit 1; }
run_step "$new_sha"
tests_run=$((tests_run + 1))
[[ $rc -eq 0 ]] || fail_test "step exited $rc; output: $out"
assert_contains "trusted signoff" "NEW-SIGNOFF" "$clone/.trusted-ci/signoff"
assert_contains "trusted signoff carries the subcommand the workflow calls" \
  "correct-cancelled" "$clone/.trusted-ci/signoff"
assert_absent "the branch's script must not be what runs" "OLD-SIGNOFF" "$clone/.trusted-ci/signoff"

# The composite actions run with this job's secrets in scope, so they are held
# to the same rule as the script.
echo "the trusted composite actions come from the same commit"
assert_contains "trusted action" "NEW-ACTION" "$clone/.trusted-ci/actions/setup-rust/action.yml"
assert_absent "the branch's action must not be what runs" "OLD-ACTION" \
  "$clone/.trusted-ci/actions/setup-rust/action.yml"

# A composite action's nested `uses: ./…` resolves from the *workspace root*,
# not from the action's own directory. So a trusted copy that still names
# `./.github/actions/x` runs the checked-out x — fork-authored, for a fork
# dispatch — with this job's GH_TOKEN and cache credentials in scope, which is
# the boundary `.trusted-ci/` exists to draw. The step repoints those
# references, and the copies must show it.
echo "nested action references are repointed at the trusted tree"
assert_contains "nested reference repointed" \
  "uses: ./.trusted-ci/actions/configure-apt" \
  "$clone/.trusted-ci/actions/setup-rust/action.yml"
assert_absent "no trusted copy may reach back into the checked-out tree" \
  "uses: ./.github/actions/" \
  "$clone/.trusted-ci/actions/setup-rust/action.yml"

# The rewrite is only useful if what it points at was itself materialised from
# the trusted commit.
assert_contains "the nested action is itself trusted" "NEW-NESTED" \
  "$clone/.trusted-ci/actions/configure-apt/action.yml"
assert_absent "the branch's nested action must not be what runs" "OLD-NESTED" \
  "$clone/.trusted-ci/actions/configure-apt/action.yml"

# Later steps invoke `.trusted-ci/signoff` directly, so the bit has to survive
# the extraction — `git show` writes a plain file, unlike the `cp` this replaced.
echo "the trusted script is executable"
tests_run=$((tests_run + 1))
[[ -x "$clone/.trusted-ci/signoff" ]] || fail_test "expected .trusted-ci/signoff to be executable"

# The working tree is deliberately left alone: the fork overlay that follows
# checks out tracked paths over it, and the sign-off run itself lints and tests
# the branch's code. Only the .trusted-ci/ copies are pinned.
echo "the working tree is left at the branch under test"
assert_contains "working tree untouched" "OLD-SIGNOFF" "$clone/scripts/signoff"

# --- the fork case: github.sha is the ref already checked out ----------------
# Nothing to fetch, and the same copies must land. Asserting the step does not
# depend on the fetch succeeding keeps the common path honest.
echo "it works when the trusted commit is already present"
build_fixture all-refs || { echo "fixture setup failed"; exit 1; }
run_step "$new_sha"
tests_run=$((tests_run + 1))
[[ $rc -eq 0 ]] || fail_test "step exited $rc; output: $out"
assert_contains "trusted signoff (no fetch needed)" "NEW-SIGNOFF" "$clone/.trusted-ci/signoff"

# --- the guard is wider than the rewrite -------------------------------------
# The rewrite matches the one spelling every action in this repo uses; the guard
# matches any `uses:` line naming that tree at all, so a spelling the rewrite
# does not handle stops the run instead of escaping into the checked-out tree
# with this job's secrets in scope. These are the two the guard's comment claims
# to cover.
#
# Each spelling goes into the *trusted* commit and the real step runs over it,
# so what is asserted is the step's own exit status. Re-running a copy of the
# guard's expression here instead would only confirm that the expression matches
# the string this test wrote — it would still pass with the step's `exit 1`
# removed, or with the guard deleted outright.
echo "a reference the rewrite does not handle fails the step rather than escaping"
for nested_ref in '"./.github/actions/configure-apt"' '.github/actions/configure-apt'; do
  build_fixture shallow-refs "$nested_ref" || { echo "fixture setup failed"; exit 1; }
  run_step "$new_sha"
  tests_run=$((tests_run + 1))
  [[ $rc -ne 0 ]] \
    || fail_test "expected a non-zero exit for the reference $nested_ref; output: $out"
  # And it has to fail *for that reason*: the guard names the copy it found, so
  # an unrelated failure cannot stand in for the one under test.
  tests_run=$((tests_run + 1))
  grep -q '\.trusted-ci/actions/setup-rust/action\.yml' <<<"$out" \
    || fail_test "the step failed for $nested_ref without naming the offending copy; output: $out"
done

# --- no silent fallback ------------------------------------------------------
# An unresolvable trusted commit has to stop the run. Falling back to the
# working tree would restore exactly the skew this step exists to prevent, and
# it would do it invisibly — the run would go on to sign the branch off with the
# branch's own script.
echo "an unresolvable trusted commit fails the step rather than falling back"
build_fixture shallow-refs || { echo "fixture setup failed"; exit 1; }
run_step "0000000000000000000000000000000000000000"
tests_run=$((tests_run + 1))
[[ $rc -ne 0 ]] || fail_test "expected a non-zero exit for an unresolvable trusted commit; output: $out"
tests_run=$((tests_run + 1))
if [[ -f "$clone/.trusted-ci/signoff" ]]; then
  fail_test "expected no .trusted-ci/signoff after a failed resolve, found: $(cat "$clone/.trusted-ci/signoff")"
fi

# --- a stale .trusted-ci/ from a previous run on the same workspace ----------
# Self-hosted runners reuse the workspace. actions/checkout's clean removes
# untracked files, but the step must not depend on that having happened: a
# leftover copy of a *different* commit's helpers is the same skew by another
# route.
echo "a leftover .trusted-ci/ from a previous run is replaced, not merged into"
build_fixture shallow-refs || { echo "fixture setup failed"; exit 1; }
mkdir -p "$clone/.trusted-ci/actions/setup-rust"
printf '#!/usr/bin/env bash\necho STALE-SIGNOFF\n' >"$clone/.trusted-ci/signoff"
printf 'name: setup-rust\n# STALE-ACTION\n' >"$clone/.trusted-ci/actions/setup-rust/action.yml"
printf 'stale\n' >"$clone/.trusted-ci/actions/leftover.yml"
run_step "$new_sha"
tests_run=$((tests_run + 1))
[[ $rc -eq 0 ]] || fail_test "step exited $rc; output: $out"
assert_absent "stale script replaced" "STALE-SIGNOFF" "$clone/.trusted-ci/signoff"
assert_absent "stale action replaced" "STALE-ACTION" "$clone/.trusted-ci/actions/setup-rust/action.yml"
tests_run=$((tests_run + 1))
[[ ! -e "$clone/.trusted-ci/actions/leftover.yml" ]] \
  || fail_test "expected the leftover .trusted-ci/actions/leftover.yml to be removed"

echo
if [[ $failures -eq 0 ]]; then
  echo "All $tests_run assertions passed"
  exit 0
fi
echo "$failures of $tests_run assertions failed"
exit 1
