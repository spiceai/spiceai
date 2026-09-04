#!/usr/bin/env bash
#
# Unit tests for the `Show sccache stats` step of `.github/workflows/signoff.yml`.
#
# That step is the only report of whether the sign-off gate compiled with the
# compiler cache. `setup-sccache`'s pre-flight degrades rather than fails, so
# three states are load-bearing and must not be collapsed:
#
#   true  — cache was live: print `sccache --show-stats`
#   false — pre-flight cleared it: warn that the gate compiled uncached
#   unset — setup never ran: a plain line, not a warning that claims uncached
#
# Collapsing unset into false annotates an early failure as a cache outage
# (Copilot on #13838, env -u SCCACHE_SETUP). The step is inline YAML, so this
# extracts the real `run:` block and executes it.
#
# Usage: scripts/test_signoff_sccache_stats.sh

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
python3 -c 'import yaml' 2>/dev/null \
  || { echo "python3 cannot import yaml — required to extract the step (pip install pyyaml)"; exit 1; }

work_dir="$(mktemp -d)"
trap 'rm -rf "$work_dir"' EXIT

subject="$work_dir/show_sccache_stats.sh"

python3 - "$workflow" "$subject" <<'PY' || exit 1
import io, re, sys
import yaml

workflow_path, out_path = sys.argv[1], sys.argv[2]
doc = yaml.safe_load(io.open(workflow_path, encoding="utf-8"))

steps = doc["jobs"]["sign-off"]["steps"]
matches = [s for s in steps if s.get("name") == "Show sccache stats"]
if len(matches) != 1:
    sys.exit(f"expected exactly 1 'Show sccache stats' step in the sign-off job, found {len(matches)}")
step = matches[0]

if step.get("env"):
    sys.exit(f"step env keys appeared: {sorted(step['env'])} — this test does not model them")

body = step["run"]
leftover = re.findall(r"\$\{\{.*?\}\}", body)
if leftover:
    sys.exit(f"step body has unmodelled template expressions: {leftover} — update this test")

io.open(out_path, "w", encoding="utf-8", newline="\n").write(body)
PY

run_step() {
  # GitHub-hosted Linux default shell is bash -eo pipefail. `env -u` first so
  # an omitted SCCACHE_SETUP is actually unset, not inherited from the test
  # process; later NAME=VALUE arguments re-set it for the false/true cases.
  env -u SCCACHE_SETUP "$@" bash -eo pipefail "$subject"
}

assert_contains() {
  local haystack="$1" needle="$2" label="$3"
  tests_run=$((tests_run + 1))
  if [[ "$haystack" == *"$needle"* ]]; then
    echo "  ok    $label"
  else
    fail_test "$label — expected to contain $(printf %q "$needle")"
  fi
}

assert_not_contains() {
  local haystack="$1" needle="$2" label="$3"
  tests_run=$((tests_run + 1))
  if [[ "$haystack" == *"$needle"* ]]; then
    fail_test "$label — must not contain $(printf %q "$needle")"
  else
    echo "  ok    $label"
  fi
}

echo "Show sccache stats"

out="$(run_step 2>&1)"
assert_contains "$out" "never configured" "unset: plain 'never configured' line"
assert_not_contains "$out" "::warning::" "unset: not a warning (job may not have reached the gate)"
assert_not_contains "$out" "compiled without the compiler cache" "unset: does not claim the gate compiled uncached"

out="$(run_step SCCACHE_SETUP=false 2>&1)"
assert_contains "$out" "::warning::" "false: warning"
assert_contains "$out" "compiled without the compiler cache" "false: says the gate compiled uncached"
assert_not_contains "$out" "never configured" "false: not the unset message"

stub_dir="$work_dir/stub"
mkdir -p "$stub_dir"
cat >"$stub_dir/sccache" <<'STUB'
#!/usr/bin/env bash
if [[ "${1:-}" == "--show-stats" ]]; then
  [[ "${STUB_SCCACHE_RC:-0}" == "0" ]] || exit "${STUB_SCCACHE_RC}"
  printf '%s\n' "${STUB_SCCACHE_OUT:-STUB_STATS}"
  exit 0
fi
echo "stub sccache: unexpected invocation: $*" >&2
exit 99
STUB
chmod +x "$stub_dir/sccache"

out="$(run_step PATH="$stub_dir:$PATH" SCCACHE_SETUP=true STUB_SCCACHE_OUT=STUB_STATS 2>&1)"
assert_contains "$out" "STUB_STATS" "true: prints sccache --show-stats"
assert_not_contains "$out" "::warning::" "true: no warning when stats succeed"
assert_not_contains "$out" "never configured" "true: not the unset message"

out="$(run_step PATH="$stub_dir:$PATH" SCCACHE_SETUP=true STUB_SCCACHE_RC=1 2>&1)"
assert_contains "$out" "::warning::sccache --show-stats failed" "true: stats failure is a warning, not a verdict"

if [ "$failures" -ne 0 ]; then
  echo
  echo "$failures of $tests_run assertions failed"
  exit 1
fi
echo
echo "All $tests_run assertions passed"
