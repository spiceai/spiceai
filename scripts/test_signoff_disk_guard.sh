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
# `failure_kind` names a third cause with the same consequence — a run that was
# signalled and so judged nothing at all — so its cases live here too, alongside
# `describe_check_failure`, which turns any of the three into what the run
# publishes.
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
# The watcher has to notice the linker's death without swallowing make's own
# exit status, and without eating the output the Actions log shows the reader.
#
# The verdict is read back out of `SIGNOFF_DISK_HIT` in the same shell that ran
# the step, which is exactly how failure_kind reads it in production. It has to
# be: a file could not be written at true ENOSPC, which is the whole point of
# #12427, so there is nothing on disk for a later shell to inspect.
assert_recorder() {
  local name="$1" make_body="$2" want_rc="$3" want_marked="$4" want_passthrough="${5:-}"
  tests_run=$((tests_run + 1))

  local fake_make="$stub_dir/make"
  printf '#!/usr/bin/env bash\n%s\n' "$make_body" >"$fake_make"
  chmod +x "$fake_make"

  local output rc
  # `if run_make_step` rather than a bare call: sourcing the subject turns on
  # `set -e`, which would abort before the verdict could be reported.
  output="$(env "PATH=$stub_dir:$PATH" SIGNOFF_DISK_WATCH=1 \
    bash -c 'source "$1"
      if run_make_step some-target; then step_rc=0; else step_rc=$?; fi
      echo "VERDICT=${SIGNOFF_DISK_HIT:+yes}"
      exit "$step_rc"' _ "$subject" 2>&1)"
  rc=$?

  if [[ "$rc" -ne "$want_rc" ]]; then
    fail_test "$name: expected make's status ${want_rc} to survive the watcher, got ${rc}"
    rm -f "$fake_make"
    return
  fi
  if [[ -n "$want_passthrough" && "$output" != *"$want_passthrough"* ]]; then
    fail_test "$name: expected make's output to pass through carrying '${want_passthrough}', got '${output}'"
    rm -f "$fake_make"
    return
  fi

  local marked="no"
  [[ "$output" == *"VERDICT=yes"* ]] && marked="yes"
  if [[ "$output" != *"VERDICT="* ]]; then
    fail_test "$name: the step never reported a verdict — output: '${output}'"
    rm -f "$fake_make"
    return
  fi
  if [[ "$marked" != "$want_marked" ]]; then
    fail_test "$name: expected marked=${want_marked}, got ${marked} (output: '${output}')"
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

# Stickiness: a step that merely mentions running out of disk and then succeeds
# must not leave the verdict blaming the volume for a later, genuine failure.
tests_run=$((tests_run + 1))
printf '#!/usr/bin/env bash\necho "error[E0308]: mismatched types"; exit 101\n' >"$stub_dir/make"
chmod +x "$stub_dir/make"
sticky_output="$(env "PATH=$stub_dir:$PATH" SIGNOFF_DISK_WATCH=1 SIGNOFF_DISK_HIT=1 \
  bash -c 'source "$1"
    if run_make_step some-target; then :; fi
    echo "VERDICT=${SIGNOFF_DISK_HIT:+yes}"' _ "$subject" 2>&1)"
if [[ "$sticky_output" == *"VERDICT=yes"* ]]; then
  fail_test "each step resets the verdict: a previous step's ENOSPC still marks a later compile failure"
else
  echo "  ok: each step resets the verdict, so only the failing step speaks"
fi
rm -f "$stub_dir/make"

echo "run_make_step when the watcher itself fails"
# A watcher that exits for its own reasons — no awk on the runner, a syntax error
# in its program, a signal — has not reported "no out-of-disk line went past". It
# has reported nothing. Leaving the watch armed on that would let failure_kind
# assert "not disk" on the authority of a reading nobody took, mislabelling a
# genuine ENOSPC death as the branch's fault: #12427 in the other direction.
#
# The stub fails *only* the watcher invocation and delegates everything else to
# the real awk, so this isolates a dead watcher from a host with no awk at all —
# and lets the same shell go on to check that the free-space backstop takes over.
tests_run=$((tests_run + 1))
broken_dir="$stub_dir/broken-watcher"
mkdir -p "$broken_dir"
real_awk="$(command -v awk)"
cat >"$broken_dir/awk" <<STUB
#!/usr/bin/env bash
set -uo pipefail
# The watcher is the only awk call carrying \`-v pat=…\`; free_disk_gib's df
# parsing has no such argument and must keep working.
for arg in "\$@"; do
  # Exit 2, awk's own error status — deliberately not the hit status 3.
  [[ "\$arg" == pat=* ]] && exit 2
done
exec "${real_awk}" "\$@"
STUB
chmod +x "$broken_dir/awk"
printf '#!/usr/bin/env bash\necho "ld: write() failed, errno=28"; exit 101\n' >"$broken_dir/make"
chmod +x "$broken_dir/make"

broken_output="$(env "PATH=$broken_dir:$stub_dir:$PATH" SIGNOFF_DISK_WATCH=1 \
  STUB_FREE_KB="$(gib_to_kb 1)" \
  bash -c 'source "$1"
    if run_make_step some-target; then step_rc=0; else step_rc=$?; fi
    echo "RC=${step_rc}"
    echo "VERDICT=${SIGNOFF_DISK_HIT:+yes}"
    echo "ARMED=${SIGNOFF_DISK_WATCH:+yes}"
    echo "KIND=$(failure_kind "$step_rc")"' _ "$subject" 2>&1)"

if [[ "$broken_output" != *"RC=101"* ]]; then
  fail_test "a dead watcher still reports make's own status: expected RC=101, got '${broken_output}'"
elif [[ "$broken_output" == *"VERDICT=yes"* ]]; then
  fail_test "a dead watcher must not be read as an out-of-disk hit: '${broken_output}'"
elif [[ "$broken_output" == *"ARMED=yes"* ]]; then
  fail_test "a dead watcher must disarm the watch, not leave it asserting 'not disk': '${broken_output}'"
elif [[ "$broken_output" != *"watcher exited 2"* ]]; then
  fail_test "a dead watcher is reported, not silent: expected 'watcher exited 2', got '${broken_output}'"
elif [[ "$broken_output" != *"KIND=disk"* ]]; then
  fail_test "with the watch disarmed, a near-empty volume must classify as disk: '${broken_output}'"
else
  echo "  ok: a watcher that died disarms the watch and lets free space classify"
fi
rm -rf "$broken_dir"

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
assert_failure_kind "trusts what the build said over free space measured after it" 101 "disk" \
  SIGNOFF_DISK_WATCH=1 SIGNOFF_DISK_HIT=1 STUB_FREE_KB="$(gib_to_kb 200)"
assert_failure_kind "a watch that saw no ENOSPC does not itself imply a disk failure" 101 "checks" \
  SIGNOFF_DISK_WATCH=1 STUB_FREE_KB="$(gib_to_kb 200)"
# A watched run's verdict is final in BOTH directions. On a shared pool another
# run can drag the volume under any threshold while this branch fails for its
# own reasons; calling that "infrastructure" tells the author to re-dispatch a
# branch that will just fail again.
assert_failure_kind "a watched build that did not hit ENOSPC stays a check failure on an empty volume" 101 "checks" \
  SIGNOFF_DISK_WATCH=1 STUB_FREE_KB="$(gib_to_kb 1)"
# ...and free space is still consulted when nothing watched, which is how a
# local run gets any classification at all.
assert_failure_kind "falls back to free space when nothing watched the build" 101 "disk" \
  STUB_FREE_KB="$(gib_to_kb 1)"
# A hit with no armed watch is not a watch: only a run that armed the watch can
# have produced the flag, so an inherited one must not classify on its own.
assert_failure_kind "a hit without an armed watch falls back to free space" 101 "checks" \
  SIGNOFF_DISK_HIT=1 STUB_FREE_KB="$(gib_to_kb 200)"
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

# A signalled run reached no verdict at all, which is a different statement from
# either disk kind: both of those describe a `make` that returned. The statuses
# here are what bash reports for a killed foreground child — 128+N — which is how
# a `Sign off` step whose 353-minute budget expires reads (#12518).
assert_failure_kind "calls a SIGTERM'd run signalled, not a check failure" 143 "signalled" \
  STUB_FREE_KB="$(gib_to_kb 200)"
assert_failure_kind "calls an interrupted run signalled" 130 "signalled" \
  STUB_FREE_KB="$(gib_to_kb 200)"
assert_failure_kind "calls a SIGKILL'd run signalled" 137 "signalled" \
  STUB_FREE_KB="$(gib_to_kb 200)"
assert_failure_kind "treats 128 itself as signalled" 128 "signalled" \
  STUB_FREE_KB="$(gib_to_kb 200)"
# "Signalled" must win over the disk backstop. A budget that expires while the
# volume happens to be tight is still a run that judged nothing, and telling the
# author it was disk sends them to reclaim space that was never the problem.
assert_failure_kind "keeps a signalled run signalled on a near-empty volume" 143 "signalled" \
  STUB_FREE_KB="$(gib_to_kb 1)"
# The boundary in the other direction is the one that protects real verdicts: 2
# is what make exits when a recipe fails, and 101 is cargo's own. Neither may be
# excused as "nothing ran".
assert_failure_kind "leaves make's own recipe failure a check failure" 2 "checks" \
  STUB_FREE_KB="$(gib_to_kb 200)"
assert_failure_kind "leaves status 127 a check failure" 127 "checks" \
  STUB_FREE_KB="$(gib_to_kb 200)"

# 128+N is a sufficient test for "signalled", not a necessary one. Cancelling a
# job signals the whole process group; when make handles that and exits with a
# small status of its own, the status alone is indistinguishable from the
# recipe failure directly above — and reading it as one published
# `signoff=failure` about a branch nothing judged (#12710). The recorded signal
# is what tells the two apart, so these cases pair with that one by design:
# same status, opposite verdict, and the flag is the only difference.
assert_failure_kind "calls a signalled run signalled even when make exited 2" 2 "signalled" \
  SIGNOFF_SIGNALLED=1 STUB_FREE_KB="$(gib_to_kb 200)"
assert_failure_kind "calls a signalled run signalled on cargo's own status" 101 "signalled" \
  SIGNOFF_SIGNALLED=1 STUB_FREE_KB="$(gib_to_kb 200)"
# "Reached no verdict" outranks naming a cause, so the recorded signal wins over
# both disk signals — the free-space backstop and the preflight's own refusal.
# A run signalled during the preflight judged nothing either, and sending its
# author to reclaim space describes a problem it did not have.
assert_failure_kind "keeps a signalled run signalled on a near-empty volume, whatever make returned" 2 "signalled" \
  SIGNOFF_SIGNALLED=1 STUB_FREE_KB="$(gib_to_kb 1)"
assert_failure_kind "keeps a signalled run signalled over the preflight's refusal" 70 "signalled" \
  SIGNOFF_SIGNALLED=1 STUB_FREE_KB="$(gib_to_kb 1)"
# An armed watch that saw no ENOSPC line is the strongest "this was the branch"
# signal the script has, and it must still not overrule "nothing judged it".
assert_failure_kind "keeps a signalled run signalled under an armed watch" 2 "signalled" \
  SIGNOFF_SIGNALLED=1 SIGNOFF_DISK_WATCH=1 STUB_FREE_KB="$(gib_to_kb 200)"
# Status 0 included, and it is the case that matters most. A trapping recipe can
# end the checks early and still return 0, which reads as a clean pass — so this
# is the classification behind cmd_signoff's refusal to publish a `success` a
# signalled run did not earn. Every other case here withholds a `failure`; this
# one withholds the verdict that would let an unjudged branch merge.
assert_failure_kind "calls a signalled run signalled even when make exited 0" 0 "signalled" \
  SIGNOFF_SIGNALLED=1 STUB_FREE_KB="$(gib_to_kb 200)"

# The flag has to come from somewhere, and a classification test can only show
# that the reading is right once it is set. This is the other half: arm the
# handler the way the verify path does, signal the process the way a cancelled
# job does, and let a child that traps the signal return a small status — the
# exact shape run 31144830314 died in. Asserts on failure_kind's answer rather
# than on the variable, so it fails if either the trap or the reading regresses.

# Stands in for a `make` caught by a cancellation. Signals its parent the way a
# process-group kill reaches every member, then handles its own signal and exits
# 2 — so the status the caller sees carries no trace of a signal at all.
cat >"$stub_dir/trapping_make" <<'STUB'
#!/usr/bin/env bash
set -uo pipefail
trap 'exit 2' TERM
kill -TERM "$PPID" 2>/dev/null
kill -TERM $$ 2>/dev/null
# Only reached if neither signal was delivered, which is the test failing.
sleep 5
exit 0
STUB
chmod +x "$stub_dir/trapping_make"

tests_run=$((tests_run + 1))
signalled_result="$(call_subject '
    watch_for_signals
    # `set +e` around the call and back, exactly as the verify path captures
    # run_checks: an `if` would disable set -e for the whole condition.
    set +e
    trapping_make
    status=$?
    set -e
    echo "STATUS=${status} KIND=$(failure_kind "$status")"' \
  STUB_FREE_KB="$(gib_to_kb 200)")"
signalled_output="${signalled_result#*|}"
if [[ "$signalled_output" != *"STATUS=2"* ]]; then
  fail_test "the signal test did not reproduce a trapped-and-returned status: '${signalled_output}'"
elif [[ "$signalled_output" != *"KIND=signalled"* ]]; then
  fail_test "a signalled run whose make exited 2 must classify as signalled: '${signalled_output}'"
else
  echo "  ok: the armed handler makes a trapped-and-returned status read as signalled"
fi

# The mirror image, and the reason the handler is armed on the verify path only:
# with no signal, the same status must still be the branch's failure. Without
# this, a handler that set the flag unconditionally would pass everything above.
assert_failure_kind "an unsignalled run with the same status is still a check failure" 2 "checks" \
  STUB_FREE_KB="$(gib_to_kb 200)"

echo
echo "describe_check_failure"
# Asserts what a failed run publishes. An empty SIGNOFF_FAILURE_STATUS_DESC is
# the contract for "publish no verdict", so it is checked as a value, not as an
# accident of an unset variable.
assert_describe() {
  local name="$1" check_status="$2" want_desc="$3" want_message="$4"
  shift 4
  tests_run=$((tests_run + 1))

  # The status is interpolated into the snippet, as assert_failure_kind does:
  # call_subject's trailing arguments are environment assignments for `env`, so
  # there is no positional left to pass it through.
  local result rc output
  result="$(call_subject \
    "describe_check_failure ${check_status} 21195 someone
     printf 'DESC[%s]\nMSG[%s]\nSUM[%s]\n' \
       \"\$SIGNOFF_FAILURE_STATUS_DESC\" \"\$SIGNOFF_FAILURE_MESSAGE\" \"\$SIGNOFF_FAILURE_SUMMARY\"" \
    "$@")"
  rc="${result%%|*}"
  output="${result#*|}"

  if [[ "$rc" -ne 0 ]]; then
    fail_test "$name: expected exit 0, got ${rc} (output: ${output})"
    return
  fi
  if [[ "$output" != *"DESC[${want_desc}]"* ]]; then
    fail_test "$name: expected description '${want_desc}', got: ${output}"
    return
  fi
  if [[ "$output" != *"${want_message}"* ]]; then
    fail_test "$name: expected '${want_message}' in the output, got: ${output}"
    return
  fi
  echo "  ok: $name"
}

# The regression, stated as the contract: run 30942941645 on PR #12448 timed out
# with the suite still passing, and published "Sign-off checks failed after
# 21195s" — a code failure that had not happened, on top of a `signoff=success`
# posted seven hours earlier. Publishing nothing is what makes both impossible.
assert_describe "publishes no verdict for a signalled run" 143 "" \
  "the checks reached no verdict" STUB_FREE_KB="$(gib_to_kb 200)"
assert_describe "says the run was signalled rather than that checks failed" 143 "" \
  "was signalled after 21195s" STUB_FREE_KB="$(gib_to_kb 200)"
# The two kinds that *did* reach a verdict still publish one. Without these, the
# guard above could silence every failure and the tests would not notice.
assert_describe "still publishes the out-of-disk verdict" 101 \
  "Runner out of disk after 21195s — checks did not complete, re-dispatch (triggered by someone)" \
  "ran out of disk" SIGNOFF_DISK_WATCH=1 SIGNOFF_DISK_HIT=1 STUB_FREE_KB="$(gib_to_kb 200)"
assert_describe "still publishes a genuine check failure" 101 \
  "Sign-off checks failed after 21195s (triggered by someone)" \
  "sign-off checks failed" STUB_FREE_KB="$(gib_to_kb 200)"
# The published half of #12710, and the one an operator actually reads: a
# cancelled run whose make returned 2 must publish no description at all. The
# case directly above is the same status with no signal recorded and does
# publish one, so this pins the difference to the signal and not to the status.
assert_describe "publishes no verdict when a signalled run's make returned an ordinary status" 2 "" \
  "the checks reached no verdict" SIGNOFF_SIGNALLED=1 STUB_FREE_KB="$(gib_to_kb 200)"
echo

# `08` passes the digit regex, but bash arithmetic reads a leading zero as
# octal — untreated, the floor becomes an arithmetic error rather than 8.
assert_preflight "reads a leading-zero floor as base 10, not octal" 0 "" \
  SIGNOFF_REMOTE_RUN=1 SIGNOFF_MIN_FREE_GIB=08 STUB_FREE_KB="$(gib_to_kb 9)"
assert_preflight "still stops below a leading-zero floor" 70 "not evaluated" \
  SIGNOFF_REMOTE_RUN=1 SIGNOFF_MIN_FREE_GIB=08 STUB_FREE_KB="$(gib_to_kb 7)"

echo
echo "preflight-disk subcommand"
# The workflow runs this as its own step, before the toolchain setup, so it has
# to be reachable through the dispatcher and not just as an internal function —
# and it has to carry the same verdict, since that step's exit status is the
# only thing the job sees.
assert_preflight_cmd() {
  local name="$1" want_rc="$2" want_out="$3"
  shift 3
  tests_run=$((tests_run + 1))

  local output rc
  output="$(env "PATH=$stub_dir:$PATH" "$@" \
    bash "$subject" preflight-disk 2>&1)"
  rc=$?

  if [[ "$rc" -ne "$want_rc" ]]; then
    fail_test "$name: expected exit ${want_rc}, got ${rc} (output: '${output}')"
  elif [[ -n "$want_out" && "$output" != *"$want_out"* ]]; then
    fail_test "$name: expected '${want_out}' in the output, got '${output}'"
  else
    echo "  ok: $name"
  fi
}
assert_preflight_cmd "proceeds when the volume has room" 0 "" \
  SIGNOFF_REMOTE_RUN=1 STUB_FREE_KB="$(gib_to_kb 200)"
assert_preflight_cmd "stops a remote run below the floor" 70 "not evaluated" \
  SIGNOFF_REMOTE_RUN=1 STUB_FREE_KB="$(gib_to_kb 5)"
# The step summary is not visible on the run page when the step itself is what
# failed, so the annotation is what an operator actually reads.
assert_preflight_cmd "annotates the stop as a runner problem" 70 "::error title=Runner out of disk::" \
  SIGNOFF_REMOTE_RUN=1 STUB_FREE_KB="$(gib_to_kb 5)"
# Locally it must stay advisory: a developer's own disk is theirs to manage, and
# this subcommand is reachable by hand.
assert_preflight_cmd "only warns locally below the floor" 0 "warning" \
  STUB_FREE_KB="$(gib_to_kb 5)"
assert_preflight_cmd "proceeds when free space is unknown" 0 "" \
  SIGNOFF_REMOTE_RUN=1 STUB_DF_RC=1

echo
if [[ "$failures" -gt 0 ]]; then
  echo "${failures} of ${tests_run} tests failed"
  exit 1
fi
echo "all ${tests_run} tests passed"
