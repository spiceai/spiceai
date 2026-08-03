#!/usr/bin/env bash
#
# Unit tests for the sign-off disk guard in `scripts/signoff`: the preflight
# that refuses to start a run the work volume cannot hold, and the failure
# classification that tells "the runner is out of disk" apart from "the branch
# is broken". Both exist because an out-of-disk sign-off otherwise reports as a
# code failure — the suite passes, then the linker dies with errno=28 on a crate
# the branch never touched — so getting the distinction wrong in either
# direction is the bug. No network and no credentials: a stub `df` on PATH
# reports whatever free space a case needs.
#
# Usage: scripts/test_signoff_disk_guard.sh

set -uo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
subject="$script_dir/signoff"

tests_run=0
failures=0

fail_test() {
  failures=$((failures + 1))
  echo "  FAIL: $1"
}

stub_dir="$(mktemp -d)"
trap 'rm -rf "$stub_dir"' EXIT

# A `df` reporting STUB_FREE_KB 1K-blocks available, in the POSIX `-P` layout
# the subject parses. STUB_DF_RC makes it fail outright, and STUB_DF_GARBAGE
# makes it print something unparseable — both are "unknown free space", which
# must never read as "full".
cat >"$stub_dir/df" <<'STUB'
#!/usr/bin/env bash
set -uo pipefail
[[ "${STUB_DF_RC:-0}" == "0" ]] || exit "${STUB_DF_RC}"
if [[ -n "${STUB_DF_GARBAGE:-}" ]]; then
  echo "df: something went sideways"
  exit 0
fi
echo "Filesystem 1024-blocks      Used Available Capacity Mounted on"
echo "/dev/disk3s5   1000000000 500000000 ${STUB_FREE_KB:-0}       50% /"
STUB
chmod +x "$stub_dir/df"

gib_to_kb() { echo $(( $1 * 1048576 )); }

# Runs one function from the subject in a fresh bash with the stub `df` first on
# PATH. Sourcing calls the single function under test without running the checks
# a real `signoff` invocation would. Echoes "<rc>|<output>".
call_subject() {
  local snippet="$1"
  shift
  local output rc
  # `env` rather than a bare assignment prefix: these come from "$@", and words
  # that only look like assignments after expansion are read as the command.
  output="$(env "PATH=$stub_dir:$PATH" "$@" \
    bash -c 'source "$1"; shift; '"$snippet" _ "$subject" 2>&1)"
  rc=$?
  printf '%s|%s' "$rc" "$output"
}

assert_free_disk_gib() {
  local name="$1" want_rc="$2" want_out="$3"
  shift 3
  tests_run=$((tests_run + 1))

  local result rc output
  result="$(call_subject 'free_disk_gib' "$@")"
  rc="${result%%|*}"
  output="${result#*|}"

  if [[ "$rc" -ne "$want_rc" ]]; then
    fail_test "$name: expected exit ${want_rc}, got ${rc} (output: ${output})"
    return
  fi
  if [[ "$want_rc" -eq 0 && "$output" != "$want_out" ]]; then
    fail_test "$name: expected '${want_out}' GiB, got '${output}'"
    return
  fi
  echo "  ok: $name"
}

# $2 is the exit status preflight_disk should return: 0 to proceed, 70 to stop.
assert_preflight() {
  local name="$1" want_rc="$2" want_output="${3:-}"
  shift 3
  tests_run=$((tests_run + 1))

  local summary="$stub_dir/summary"
  : >"$summary"

  local result rc output
  result="$(call_subject 'preflight_disk' GITHUB_STEP_SUMMARY="$summary" "$@")"
  rc="${result%%|*}"
  output="${result#*|}"

  if [[ "$rc" -ne "$want_rc" ]]; then
    fail_test "$name: expected exit ${want_rc}, got ${rc} (output: ${output})"
    return
  fi
  if [[ -n "$want_output" ]]; then
    # The step summary is where a remote run explains itself; the terminal is
    # where a local run does. Accept the phrase in either.
    if [[ "$output" != *"$want_output"* ]] && ! grep -qF "$want_output" "$summary"; then
      fail_test "$name: expected '${want_output}' in the output or step summary, got '${output}' / '$(cat "$summary")'"
      return
    fi
  fi
  echo "  ok: $name"
}

assert_failure_kind() {
  local name="$1" check_status="$2" want="$3"
  shift 3
  tests_run=$((tests_run + 1))

  local result rc output
  result="$(call_subject "failure_kind ${check_status}" "$@")"
  rc="${result%%|*}"
  output="${result#*|}"

  if [[ "$rc" -ne 0 ]]; then
    fail_test "$name: expected exit 0, got ${rc} (output: ${output})"
    return
  fi
  if [[ "$output" != "$want" ]]; then
    fail_test "$name: expected kind '${want}', got '${output}'"
    return
  fi
  echo "  ok: $name"
}

echo "free_disk_gib"
assert_free_disk_gib "reports whole GiB available" 0 "40" \
  STUB_FREE_KB="$(gib_to_kb 40)"
assert_free_disk_gib "truncates a partial GiB rather than rounding up" 0 "1" \
  STUB_FREE_KB=2000000
assert_free_disk_gib "reports a full volume as zero" 0 "0" \
  STUB_FREE_KB=0
assert_free_disk_gib "signals unknown when df fails" 1 "" \
  STUB_DF_RC=1
assert_free_disk_gib "signals unknown when df output does not parse" 1 "" \
  STUB_DF_GARBAGE=1

echo "preflight_disk"
assert_preflight "proceeds when the volume has room" 0 "" \
  SIGNOFF_REMOTE_RUN=1 STUB_FREE_KB="$(gib_to_kb 60)"
assert_preflight "stops a remote run below the floor" 70 "not evaluated" \
  SIGNOFF_REMOTE_RUN=1 STUB_FREE_KB="$(gib_to_kb 3)"
assert_preflight "calls a remote stop an infrastructure failure, not a code one" 70 "infrastructure failure" \
  SIGNOFF_REMOTE_RUN=1 STUB_FREE_KB="$(gib_to_kb 3)"
# A developer's own disk is theirs to manage: warn, never block.
assert_preflight "only warns locally below the floor" 0 "warning" \
  STUB_FREE_KB="$(gib_to_kb 3)"
assert_preflight "honors a raised SIGNOFF_MIN_FREE_GIB" 70 "not evaluated" \
  SIGNOFF_REMOTE_RUN=1 SIGNOFF_MIN_FREE_GIB=100 STUB_FREE_KB="$(gib_to_kb 60)"
assert_preflight "honors a lowered SIGNOFF_MIN_FREE_GIB" 0 "" \
  SIGNOFF_REMOTE_RUN=1 SIGNOFF_MIN_FREE_GIB=1 STUB_FREE_KB="$(gib_to_kb 3)"
assert_preflight "falls back to the default floor on a non-integer override" 70 "not evaluated" \
  SIGNOFF_REMOTE_RUN=1 SIGNOFF_MIN_FREE_GIB=lots STUB_FREE_KB="$(gib_to_kb 3)"
# Unknown free space must never block a run: a runner whose df we cannot read
# is not a runner we know to be full.
assert_preflight "proceeds when free space is unknown" 0 "" \
  SIGNOFF_REMOTE_RUN=1 STUB_DF_RC=1

echo "run_make_step + build_hit_disk_full"
# The recorder has to notice the linker's death without swallowing make's own
# exit status, and without eating the output the Actions log shows the reader.
assert_recorder() {
  local name="$1" make_body="$2" want_rc="$3" want_marked="$4" want_passthrough="${5:-}"
  tests_run=$((tests_run + 1))

  local marker="$stub_dir/marker" fake_make="$stub_dir/make"
  : >"$marker"
  printf '#!/usr/bin/env bash\n%s\n' "$make_body" >"$fake_make"
  chmod +x "$fake_make"

  local output rc
  output="$(env "PATH=$stub_dir:$PATH" SIGNOFF_DISK_MARKER="$marker" \
    bash -c 'source "$1"; run_make_step some-target' _ "$subject" 2>&1)"
  rc=$?

  if [[ "$rc" -ne "$want_rc" ]]; then
    fail_test "$name: expected make's status ${want_rc} to survive the recorder, got ${rc}"
    rm -f "$fake_make"
    return
  fi
  if [[ -n "$want_passthrough" && "$output" != *"$want_passthrough"* ]]; then
    fail_test "$name: expected make's output to pass through carrying '${want_passthrough}', got '${output}'"
    rm -f "$fake_make"
    return
  fi

  local marked="no"
  [[ -s "$marker" ]] && marked="yes"
  if [[ "$marked" != "$want_marked" ]]; then
    fail_test "$name: expected marked=${want_marked}, got ${marked} (marker: '$(cat "$marker")')"
    rm -f "$fake_make"
    return
  fi
  rm -f "$fake_make"
  echo "  ok: $name"
}

# Verbatim from run 30831204417, where this went unrecognised for three hours.
assert_recorder "records the linker's out-of-disk death" \
  'echo "          ld: write() failed, errno=28 (No space left on device)"; exit 101' \
  101 yes "errno=28"
assert_recorder "records it when only the prose spelling appears" \
  'echo "error: No space left on device"; exit 101' \
  101 yes
# The other direction: an ordinary compile error must leave the marker empty,
# or every real failure would be excused as infrastructure.
assert_recorder "leaves an ordinary compile failure unmarked" \
  'echo "error[E0308]: mismatched types"; echo "  --> crates/runtime/src/lib.rs:1:1"; exit 101' \
  101 no "E0308"
assert_recorder "leaves a passing step unmarked and keeps its status" \
  'echo "Finished dev profile"; exit 0' \
  0 no "Finished dev profile"
# The recorder sits in a pipeline; make's status must not be replaced by the
# recorder's own success, which is what a naive `make | tee` would report.
assert_recorder "reports make's failure, not the recorder's success" \
  'echo "some output"; exit 42' \
  42 no "some output"

echo "failure_kind"
# The regression: run 30831204417 passed 8809 tests, then died at the linker
# with errno=28 and reported as a plain check failure. run_checks exits non-zero
# for that (not the preflight code), so the classification has to come from what
# the build said, or from the volume.
assert_failure_kind "calls a failure on a near-empty volume a disk failure" 101 "disk" \
  STUB_FREE_KB="$(gib_to_kb 1)"
# The authoritative signal, and the one that matters most: the volume can look
# healthy again by the time we measure it, because cargo unlinks the partial
# binaries it was writing on its way out. What the build *said* still stands.
marker_with_disk_error="$stub_dir/marker-disk"
echo "ld: write() failed, errno=28 (No space left on device)" >"$marker_with_disk_error"
assert_failure_kind "trusts what the build said over free space measured after it" 101 "disk" \
  SIGNOFF_DISK_MARKER="$marker_with_disk_error" STUB_FREE_KB="$(gib_to_kb 200)"
marker_empty="$stub_dir/marker-empty"
: >"$marker_empty"
assert_failure_kind "an empty marker does not itself imply a disk failure" 101 "checks" \
  SIGNOFF_DISK_MARKER="$marker_empty" STUB_FREE_KB="$(gib_to_kb 200)"
assert_failure_kind "calls the preflight's own refusal a disk failure" 70 "disk" \
  STUB_FREE_KB="$(gib_to_kb 60)"
# The other direction matters just as much: a real defect on a tight disk must
# not be excused as infrastructure, or a broken branch signs off as "re-dispatch
# me". 10 GiB is under the 25 GiB preflight floor and well over the critical bar.
assert_failure_kind "calls a failure on a tight but workable volume a check failure" 101 "checks" \
  STUB_FREE_KB="$(gib_to_kb 10)"
assert_failure_kind "calls a failure on a roomy volume a check failure" 101 "checks" \
  STUB_FREE_KB="$(gib_to_kb 200)"
assert_failure_kind "calls a failure with unknown free space a check failure" 101 "checks" \
  STUB_DF_RC=1

echo
if [[ "$failures" -gt 0 ]]; then
  echo "${failures} of ${tests_run} tests failed"
  exit 1
fi
echo "all ${tests_run} tests passed"
