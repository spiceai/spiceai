#!/usr/bin/env bash
#
# Unit tests for `scripts/preflight_build_disk.sh`, the check that refuses a
# build on a runner whose volumes cannot hold it.
#
# Both directions are load-bearing. Under-reporting leaves the condition to
# surface from inside the compiler as `os error 28` on an unrelated crate, which
# is how #12794 came to be triaged as a compiler-cache fault. Over-reporting is
# worse: it would refuse builds that complete today — on a GitHub-hosted image,
# or on any volume the script could not actually measure — and a floor tuned for
# the self-hosted pool must never do that.
#
# No network, no runner, no real disk: a stub `df` on PATH reports whatever each
# case needs, keyed by the path it is asked about, so a case can put two paths
# on one volume or on two.
#
# Usage: scripts/test_preflight_build_disk.sh

set -uo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
subject="$script_dir/preflight_build_disk.sh"

tests_run=0
failures=0

fail_test() {
  failures=$((failures + 1))
  echo "  FAIL: $1"
}

stub_dir="$(mktemp -d)"
work_dir="$(mktemp -d)"
trap 'rm -rf "$stub_dir" "$work_dir"' EXIT

# A `df` whose answers come from $STUB_MAP: one `path|device|free_kb` line per
# path, with STUB_DEFAULT used for anything unlisted. STUB_DF_RC makes it fail
# outright and STUB_DF_GARBAGE makes it print something unparseable — both are
# "unknown free space", which must never read as "full".
cat >"$stub_dir/df" <<'STUB'
#!/usr/bin/env bash
set -uo pipefail
[[ "${STUB_DF_RC:-0}" == "0" ]] || exit "${STUB_DF_RC}"
if [[ -n "${STUB_DF_GARBAGE:-}" ]]; then
  echo "df: something went sideways"
  exit 0
fi
if [[ -n "${STUB_DF_NONNUMERIC:-}" ]]; then
  echo "Filesystem 1024-blocks      Used Available Capacity Mounted on"
  echo "map:autofs            0         0         -       -  /net"
  exit 0
fi
target="${*: -1}"
device="/dev/disk1"
free_kb="${STUB_DEFAULT:-104857600}"
if [[ -n "${STUB_MAP:-}" ]]; then
  while IFS='|' read -r map_path map_device map_free; do
    [[ -n "$map_path" ]] || continue
    if [[ "$target" == "$map_path" ]]; then
      device="$map_device"
      free_kb="$map_free"
      break
    fi
  done <<<"${STUB_MAP}"
fi
echo "Filesystem 1024-blocks      Used Available Capacity Mounted on"
echo "${device}   1000000000 500000000 ${free_kb}       50% /"
STUB
chmod +x "$stub_dir/df"

gib_to_kb() { echo $(( $1 * 1048576 )); }

# Runs the subject in a fresh bash with the stub `df` first on PATH. Every
# variable the case wants set is passed in the environment, so no case can leak
# state into the next. Echoes "<rc>|<output>", with stderr folded in because the
# diagnostic the operator reads is written there.
run_subject() {
  local output rc
  output=$(PATH="$stub_dir:$PATH" bash "$subject" "$@" 2>&1)
  rc=$?
  printf '%s|%s' "$rc" "$output"
}

expect_rc() {
  local label="$1" want="$2" got="$3"
  tests_run=$((tests_run + 1))
  [[ "${got%%|*}" == "$want" ]] || fail_test "$label: expected rc ${want}, got ${got%%|*} (${got#*|})"
}

expect_contains() {
  local label="$1" needle="$2" got="$3"
  tests_run=$((tests_run + 1))
  case "${got#*|}" in
    *"$needle"*) ;;
    *) fail_test "$label: expected output to contain '${needle}', got: ${got#*|}" ;;
  esac
}

expect_not_contains() {
  local label="$1" needle="$2" got="$3"
  tests_run=$((tests_run + 1))
  case "${got#*|}" in
    *"$needle"*) fail_test "$label: expected output NOT to contain '${needle}', got: ${got#*|}" ;;
  esac
}

echo "Testing scripts/preflight_build_disk.sh"

# --- The refusal, and who it applies to ------------------------------------

echo "- a self-hosted runner below the floor refuses the build"
result=$(STUB_DEFAULT="$(gib_to_kb 3)" RUNNER_ENVIRONMENT=self-hosted RUNNER_NAME=mbp-runner-04-01 \
  run_subject "$work_dir")
expect_rc "self-hosted below floor" 70 "$result"
expect_contains "self-hosted below floor" "3 GiB free" "$result"
expect_contains "self-hosted names the runner" "mbp-runner-04-01" "$result"
# The whole point of the diagnostic: an operator must not read this as a broken
# branch, which is the mistake the raw compiler error invites.
expect_contains "self-hosted says the branch was not evaluated" "the branch was not evaluated" "$result"
expect_contains "self-hosted annotates the run" "::error title=Runner out of disk::" "$result"

echo "- a GitHub-hosted runner below the floor reports and builds anyway"
result=$(STUB_DEFAULT="$(gib_to_kb 3)" RUNNER_ENVIRONMENT=github-hosted \
  run_subject "$work_dir")
expect_rc "github-hosted below floor" 0 "$result"
expect_contains "github-hosted warns" "building anyway" "$result"
expect_not_contains "github-hosted does not annotate" "::error" "$result"

echo "- an unset RUNNER_ENVIRONMENT reports and builds anyway"
result=$(STUB_DEFAULT="$(gib_to_kb 3)" run_subject "$work_dir")
expect_rc "no runner environment" 0 "$result"
expect_contains "no runner environment warns" "building anyway" "$result"

echo "- a self-hosted runner above the floor passes"
result=$(STUB_DEFAULT="$(gib_to_kb 400)" RUNNER_ENVIRONMENT=self-hosted run_subject "$work_dir")
expect_rc "self-hosted above floor" 0 "$result"
expect_contains "self-hosted above floor reports the reading" "400 GiB" "$result"

echo "- exactly at the floor passes"
result=$(STUB_DEFAULT="$(gib_to_kb 10)" RUNNER_ENVIRONMENT=self-hosted run_subject "$work_dir")
expect_rc "at the floor" 0 "$result"

# --- Unknown is not full ----------------------------------------------------

echo "- a df that fails does not refuse the build"
result=$(STUB_DF_RC=1 RUNNER_ENVIRONMENT=self-hosted run_subject "$work_dir")
expect_rc "df fails" 0 "$result"
expect_contains "df fails says so" "could not measure" "$result"

echo "- a df printing something unparseable does not refuse the build"
result=$(STUB_DF_GARBAGE=1 RUNNER_ENVIRONMENT=self-hosted run_subject "$work_dir")
expect_rc "df garbage" 0 "$result"
expect_contains "df garbage says so" "could not measure" "$result"

echo "- a df whose Available column is not a number does not refuse the build"
# The dangerous shape: awk coerces a non-numeric field to 0, and a 0 that came
# from garbage is indistinguishable downstream from a volume genuinely at zero.
result=$(STUB_DF_NONNUMERIC=1 RUNNER_ENVIRONMENT=self-hosted run_subject "$work_dir")
expect_rc "df non-numeric" 0 "$result"
expect_contains "df non-numeric says so" "could not measure" "$result"
expect_not_contains "df non-numeric is not read as zero" "0 GiB free" "$result"

echo "- no df at all does not refuse the build"
tests_run=$((tests_run + 1))
empty_path_dir="$(mktemp -d)"
# An absolute bash, because emptying PATH takes `bash` with it and the case is
# about the subject finding no `df`, not about the harness finding no shell.
bash_path="$(command -v bash)"
output=$(PATH="$empty_path_dir" RUNNER_ENVIRONMENT=self-hosted "$bash_path" "$subject" "$work_dir" 2>&1)
rc=$?
rm -rf "$empty_path_dir"
[[ "$rc" == "0" ]] || fail_test "no df: expected rc 0, got ${rc} (${output})"

# --- Which volumes get measured --------------------------------------------

echo "- two paths on one volume are reported once"
result=$(STUB_MAP="${work_dir}|/dev/disk3s5|$(gib_to_kb 400)
${work_dir}/tmp|/dev/disk3s5|$(gib_to_kb 400)" RUNNER_ENVIRONMENT=self-hosted \
  run_subject "$work_dir" "$work_dir/tmp")
expect_rc "one volume twice" 0 "$result"
tests_run=$((tests_run + 1))
lines=$(printf '%s' "${result#*|}" | grep -c "free space on" || true)
[[ "$lines" == "1" ]] || fail_test "one volume twice: expected 1 reading, got ${lines}"

echo "- a short second volume is caught even when the first is fine"
# #12794 saw the work volume and the system temp directory fill separately, so
# measuring only the target directory would have missed half the condition.
mkdir -p "$work_dir/tmp"
result=$(STUB_MAP="${work_dir}|/dev/disk3s5|$(gib_to_kb 400)
${work_dir}/tmp|/dev/disk4s1|$(gib_to_kb 2)" RUNNER_ENVIRONMENT=self-hosted \
  run_subject "$work_dir" "$work_dir/tmp")
expect_rc "second volume short" 70 "$result"
expect_contains "second volume named" "${work_dir}/tmp has 2 GiB free" "$result"
expect_not_contains "healthy volume not blamed" "${work_dir} has 400 GiB free" "$result"

echo "- a target directory that does not exist yet measures its nearest existing parent"
# The first build on a fresh runner has no target/ — measuring nothing there
# would silently disable the check exactly when the volume is most suspect.
result=$(STUB_MAP="${work_dir}|/dev/disk3s5|$(gib_to_kb 1)" RUNNER_ENVIRONMENT=self-hosted \
  run_subject "$work_dir/not/created/yet")
expect_rc "absent target dir" 70 "$result"
expect_contains "absent target dir measured the parent" "1 GiB free" "$result"

echo "- an absent bare relative path falls back to the working directory"
# `target` strips to itself, so walking parents is a loop. Giving up there would
# silently measure nothing, which is the check quietly disabling itself.
result=$(cd "$work_dir" && STUB_MAP=".|/dev/disk3s5|$(gib_to_kb 1)" RUNNER_ENVIRONMENT=self-hosted \
  PATH="$stub_dir:$PATH" bash "$subject" "no-such-target" 2>&1; printf '|%s' "$?")
tests_run=$((tests_run + 1))
case "$result" in
  *"free space on .: 1 GiB"*) ;;
  *) fail_test "bare relative path: expected a reading for '.', got: ${result}" ;;
esac

# --- The floor --------------------------------------------------------------

echo "- BUILD_MIN_FREE_GIB raises the floor"
result=$(STUB_DEFAULT="$(gib_to_kb 20)" BUILD_MIN_FREE_GIB=50 RUNNER_ENVIRONMENT=self-hosted \
  run_subject "$work_dir")
expect_rc "raised floor" 70 "$result"
expect_contains "raised floor named" "50 GiB floor" "$result"

echo "- BUILD_MIN_FREE_GIB lowers the floor"
result=$(STUB_DEFAULT="$(gib_to_kb 4)" BUILD_MIN_FREE_GIB=2 RUNNER_ENVIRONMENT=self-hosted \
  run_subject "$work_dir")
expect_rc "lowered floor" 0 "$result"

echo "- a non-integer BUILD_MIN_FREE_GIB falls back to the default"
result=$(STUB_DEFAULT="$(gib_to_kb 3)" BUILD_MIN_FREE_GIB=lots RUNNER_ENVIRONMENT=self-hosted \
  run_subject "$work_dir")
expect_rc "non-integer floor" 70 "$result"
expect_contains "non-integer floor warns" "is not an integer" "$result"
expect_contains "non-integer floor uses the default" "10 GiB floor" "$result"

echo "- a zero floor reports the readings and refuses nothing"
# The escape hatch: if the floor is ever wrong for a host, an operator turns the
# refusal off from the workflow rather than waiting on a change to this script.
result=$(STUB_DEFAULT="$(gib_to_kb 0)" BUILD_MIN_FREE_GIB=0 RUNNER_ENVIRONMENT=self-hosted \
  run_subject "$work_dir")
expect_rc "zero floor" 0 "$result"
expect_contains "zero floor still reports" "free space on" "$result"

echo "- a zero-padded BUILD_MIN_FREE_GIB is decimal, not octal"
result=$(STUB_DEFAULT="$(gib_to_kb 7)" BUILD_MIN_FREE_GIB=08 RUNNER_ENVIRONMENT=self-hosted \
  run_subject "$work_dir")
expect_rc "zero-padded floor" 70 "$result"
expect_contains "zero-padded floor is 8" "8 GiB floor" "$result"

# --- Surfaces ---------------------------------------------------------------

echo "- a refusal writes the step summary when one is available"
tests_run=$((tests_run + 1))
summary_file="$work_dir/summary.md"
: >"$summary_file"
PATH="$stub_dir:$PATH" STUB_DEFAULT="$(gib_to_kb 1)" RUNNER_ENVIRONMENT=self-hosted \
  GITHUB_STEP_SUMMARY="$summary_file" bash "$subject" "$work_dir" >/dev/null 2>&1
case "$(cat "$summary_file")" in
  *"not enough disk to build"*) ;;
  *) fail_test "step summary: expected the refusal to be written, got: $(cat "$summary_file")" ;;
esac

echo "- a passing preflight writes no step summary"
tests_run=$((tests_run + 1))
: >"$summary_file"
PATH="$stub_dir:$PATH" STUB_DEFAULT="$(gib_to_kb 400)" RUNNER_ENVIRONMENT=self-hosted \
  GITHUB_STEP_SUMMARY="$summary_file" bash "$subject" "$work_dir" >/dev/null 2>&1
[[ ! -s "$summary_file" ]] || fail_test "step summary: expected nothing on a pass, got: $(cat "$summary_file")"

echo "- no paths is a usage error, not a refusal"
result=$(RUNNER_ENVIRONMENT=self-hosted run_subject)
expect_rc "no arguments" 2 "$result"
expect_contains "no arguments explains" "usage:" "$result"

echo "- an empty path argument is skipped rather than measured"
# The composite action passes TMPDIR straight through, and it is routinely unset.
result=$(STUB_DEFAULT="$(gib_to_kb 400)" RUNNER_ENVIRONMENT=self-hosted run_subject "$work_dir" "")
expect_rc "empty path" 0 "$result"

echo
if [[ "$failures" -eq 0 ]]; then
  echo "All ${tests_run} assertions passed."
  exit 0
fi
echo "${failures} of ${tests_run} assertions failed."
exit 1
