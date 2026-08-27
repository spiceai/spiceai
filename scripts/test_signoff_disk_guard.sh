#!/usr/bin/env bash
#
# Unit tests for the sign-off runner guards in `scripts/signoff`: the preflight
# that refuses to start a run the work volume cannot hold, and the failure
# classification that tells "the runner broke" apart from "the branch is broken".
# Both exist because a run the runner broke otherwise reports as a code failure —
# the suite passes, then the linker dies with errno=28 on a crate the branch never
# touched, or sccache loses its storage and nothing compiles at all — so getting
# the distinction wrong in either direction is the bug. No network and no
# credentials: a stub `df` on PATH reports whatever free space a case needs, and a
# stub `make` prints whatever a case needs the watcher to read.
#
# `failure_kind` names two further causes with the same consequence — a run that
# was signalled and so judged nothing at all, and a branch whose Makefile has no
# rule for a target the gate invokes, so nothing was compiled — so their cases
# live here too, alongside `describe_check_failure`, which turns any of them into
# what the run publishes. The make-target preflight is here for the same reason
# the disk one is: it decides whether a run gets to start, and getting it wrong
# either way is the bug.
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

echo "make_target_exists + preflight_make_targets"
# These cases run the *real* `make` against a real Makefile: the question is
# what GNU make itself answers for a target it cannot resolve, so stubbing make
# would only test the stub. That also means they must not put $stub_dir first on
# PATH — assert_recorder below leaves a fake `make` there.
fixture_dir="$(mktemp -d)"
trap 'rm -rf "$stub_dir" "$fixture_dir"' EXIT

# A Makefile defining exactly the named targets, each a no-op. From the probe's
# point of view this is what a branch based before a target was added looks
# like: every other target resolves, that one does not.
write_makefile() {
  local dir="$1"
  shift
  : >"$dir/Makefile"
  local target
  for target in "$@"; do
    printf '.PHONY: %s\n%s:\n\t@true\n\n' "$target" "$target" >>"$dir/Makefile"
  done
}

# Runs one function from the subject with DIR as the working directory, so the
# probe reads DIR's Makefile. Echoes "<rc>|<output>" like call_subject.
call_subject_in() {
  local dir="$1" snippet="$2"
  shift 2
  local output rc
  output="$(cd "$dir" && env "$@" \
    bash -c 'source "$1"; shift; '"$snippet" _ "$subject" 2>&1)"
  rc=$?
  printf '%s|%s' "$rc" "$output"
}

assert_target_exists() {
  local name="$1" dir="$2" target="$3" want_rc="$4"
  shift 4
  tests_run=$((tests_run + 1))

  local result rc output
  result="$(call_subject_in "$dir" "make_target_exists ${target}" "$@")"
  rc="${result%%|*}"
  output="${result#*|}"

  if [[ "$rc" -ne "$want_rc" ]]; then
    fail_test "$name: expected exit ${want_rc}, got ${rc} (output: ${output})"
    return
  fi
  echo "  ok: $name"
}

# $4 is the exit status preflight_make_targets should return: 0 to proceed, 71
# to stop.
assert_preflight_targets() {
  local name="$1" dir="$2" want_rc="$3" want_output="${4:-}"
  shift 4
  tests_run=$((tests_run + 1))

  local summary="$fixture_dir/summary"
  : >"$summary"

  local result rc output
  result="$(call_subject_in "$dir" 'preflight_make_targets' \
    GITHUB_STEP_SUMMARY="$summary" "$@")"
  rc="${result%%|*}"
  output="${result#*|}"

  if [[ "$rc" -ne "$want_rc" ]]; then
    fail_test "$name: expected exit ${want_rc}, got ${rc} (output: ${output})"
    return
  fi
  if [[ -n "$want_output" ]]; then
    if [[ "$output" != *"$want_output"* ]] && ! grep -qF "$want_output" "$summary"; then
      fail_test "$name: expected '${want_output}' in the output or step summary, got '${output}' / '$(cat "$summary")'"
      return
    fi
  fi
  echo "  ok: $name"
}

complete_dir="$fixture_dir/complete"
mkdir -p "$complete_dir"
write_makefile "$complete_dir" lint-rust nextest-packages nextest verify-cli

# The #12813 branch exactly: everything the gate needs except the target added
# to trunk in f7c9485b1.
predates_dir="$fixture_dir/predates-verify-cli"
mkdir -p "$predates_dir"
write_makefile "$predates_dir" lint-rust nextest-packages nextest

assert_target_exists "resolves a target the Makefile defines" "$complete_dir" verify-cli 0
assert_target_exists "reports a target the Makefile does not define" "$predates_dir" verify-cli 1

# A missing prerequisite reports the same "No rule to make target" phrase. The
# target itself exists, so the run must proceed and let the step report it in
# its own words — not be relabelled as a branch that predates the gate.
prereq_dir="$fixture_dir/missing-prereq"
mkdir -p "$prereq_dir"
printf '.PHONY: verify-cli\nverify-cli: some-absent-file\n\t@true\n' >"$prereq_dir/Makefile"
assert_target_exists "a missing prerequisite is not a missing target" "$prereq_dir" verify-cli 0

# No Makefile at all reports the same "No rule to make target" phrase, and that
# is the honest answer: the gate cannot run a single target in such a checkout.
empty_dir="$fixture_dir/no-makefile"
mkdir -p "$empty_dir"
assert_target_exists "reports a missing target when there is no Makefile at all" "$empty_dir" verify-cli 1

# A make that fails for its *own* reasons is a different matter: that reading is
# not one to refuse a branch on, and the step that hits it reports it in its own
# words. An unparseable Makefile says "missing separator", never "No rule".
broken_dir="$fixture_dir/broken-makefile"
mkdir -p "$broken_dir"
printf 'this is not a makefile\n' >"$broken_dir/Makefile"
assert_target_exists "treats a Makefile make cannot parse as target present" "$broken_dir" verify-cli 0

# ...as is a runner with no make at all, which says "command not found". A PATH
# holding bash and nothing else: emptying it outright would take `bash` with it
# and test nothing.
no_make_path="$fixture_dir/path-without-make"
mkdir -p "$no_make_path"
ln -sf "$(command -v bash)" "$no_make_path/bash"
assert_target_exists "treats a runner with no make as target present" \
  "$complete_dir" verify-cli 0 PATH="$no_make_path"

assert_preflight_targets "proceeds when every target resolves" "$complete_dir" 0 ""
assert_preflight_targets "stops a branch whose Makefile predates a target" "$predates_dir" 71 "verify-cli"
assert_preflight_targets "says the branch was not evaluated" "$predates_dir" 71 "not evaluated"
assert_preflight_targets "names the remedy rather than only the symptom" "$predates_dir" 71 "Merge trunk into the branch"
# Unlike the disk floor, this is fatal locally too: it is not a threshold a
# developer may reasonably run under, it is a step certain to fail.
assert_preflight_targets "stops a local run as well as a remote one" "$predates_dir" 71 "no rule for"
assert_preflight_targets "annotates a remote stop for the run page" "$predates_dir" 71 \
  "::error title=Sign-off cannot run on this branch::" SIGNOFF_REMOTE_RUN=1
# Reporting only the first would send the author round the loop once per target.
several_dir="$fixture_dir/predates-several"
mkdir -p "$several_dir"
write_makefile "$several_dir" lint-rust
assert_preflight_targets "lists every missing target, not just the first" "$several_dir" 71 \
  "nextest verify-cli nextest-packages"

# Requiring a target the run will not invoke refuses a branch that could have
# completed every step selected for it. `nextest-packages` is reached only
# through run_targeted_tests, and remote sign-off turns targeted work off by
# default — so on that run the target is never invoked and must not be probed.
targeted_off_dir="$fixture_dir/predates-nextest-packages"
mkdir -p "$targeted_off_dir"
write_makefile "$targeted_off_dir" lint-rust nextest verify-cli
assert_preflight_targets "probes nextest-packages when targeted work is on" \
  "$targeted_off_dir" 71 "nextest-packages"
assert_preflight_targets "proceeds without nextest-packages when targeted work is off" \
  "$targeted_off_dir" 0 "" SIGNOFF_SKIP_TARGETED_LINT=1 SIGNOFF_SKIP_TARGETED_TESTS=1
# Either switch alone leaves the targeted path unreachable, so either alone is
# enough to stop asking for the target.
assert_preflight_targets "proceeds when only targeted tests are off" \
  "$targeted_off_dir" 0 "" SIGNOFF_SKIP_TARGETED_TESTS=1
# The unconditional targets stay required whatever the targeted switches say.
assert_preflight_targets "still stops on an unconditional target with targeted work off" \
  "$predates_dir" 71 "verify-cli" SIGNOFF_SKIP_TARGETED_LINT=1 SIGNOFF_SKIP_TARGETED_TESTS=1

# A preflight run_checks never calls is a preflight that does nothing, and no
# case above would notice — so assert the wiring, not just the function. Called
# with no revision, run_checks skips the Rust-changes check and reaches the
# preflight directly, so this needs neither git nor a build. Only the refusing
# direction is exercised: letting it proceed would run the real gate.
tests_run=$((tests_run + 1))
wiring_result="$(call_subject_in "$predates_dir" 'run_checks')"
if [[ "${wiring_result%%|*}" -ne 71 ]]; then
  fail_test "run_checks stops on a missing make target before running any step: got ${wiring_result}"
elif [[ "${wiring_result#*|}" != *"verify-cli"* ]]; then
  fail_test "run_checks stops on a missing make target before running any step: expected 'verify-cli' in ${wiring_result}"
else
  echo "  ok: run_checks stops on a missing make target before running any step"
fi

# The list is only worth anything if it matches the Makefile the gate actually
# runs: rename a target in the root Makefile without updating
# SIGNOFF_MAKE_TARGETS and every sign-off refuses to start. Asserted with
# targeted work on, so the conditional entry is covered here too.
assert_preflight_targets "every sign-off make target resolves in the repo's own Makefile" \
  "$(cd "$script_dir/.." && pwd)" 0 ""

echo
echo "preflight_lockfile"
# The third preflight, and the third way to reach "the checks reached no
# verdict": Cargo.lock no longer describes the manifests, so cargo has to rewrite
# it before anything can build. A stub `cargo` stands in for the real resolution —
# the question here is what the guard does with each answer cargo can give, and
# resolving a 700-crate workspace for real would turn these tests into a build.
lock_dir="$(mktemp -d)"
trap 'rm -rf "$stub_dir" "$fixture_dir" "$lock_dir"' EXIT

cargo_stub_dir="$lock_dir/bin"
mkdir -p "$cargo_stub_dir"
# STUB_CARGO_ERR is what cargo prints on stderr; STUB_CARGO_RC is its status.
# Both default to a clean pass, so a case only states what it changes.
cat >"$cargo_stub_dir/cargo" <<'STUB'
#!/usr/bin/env bash
set -uo pipefail
if [[ -n "${STUB_CARGO_ERR:-}" ]]; then
  printf '%s\n' "${STUB_CARGO_ERR}" >&2
fi
exit "${STUB_CARGO_RC:-0}"
STUB
chmod +x "$cargo_stub_dir/cargo"

# $3 is the exit status preflight_lockfile should return: 0 to proceed, 72 to stop.
assert_preflight_lock() {
  local name="$1" dir="$2" want_rc="$3" want_output="${4:-}"
  shift 4
  tests_run=$((tests_run + 1))

  local summary="$lock_dir/summary"
  : >"$summary"

  local result rc output
  result="$(call_subject_in "$dir" 'preflight_lockfile' \
    "PATH=$cargo_stub_dir:$PATH" GITHUB_STEP_SUMMARY="$summary" "$@")"
  rc="${result%%|*}"
  output="${result#*|}"

  if [[ "$rc" -ne "$want_rc" ]]; then
    fail_test "$name: expected exit ${want_rc}, got ${rc} (output: ${output})"
    return
  fi
  if [[ -n "$want_output" ]]; then
    if [[ "$output" != *"$want_output"* ]] && ! grep -qF "$want_output" "$summary"; then
      fail_test "$name: expected '${want_output}' in the output or step summary, got '${output}' / '$(cat "$summary")'"
      return
    fi
  fi
  echo "  ok: $name"
}

# Asserts an explanation is *absent*. For the "cargo broke for another reason"
# cases the failure worth guarding against is not a wrong status but a wrong
# story: a guard that blames the lockfile for every cargo failure sends the
# reader to regenerate a file that was never the problem.
assert_preflight_lock_silent_on() {
  local name="$1" dir="$2" unwanted="$3"
  shift 3
  tests_run=$((tests_run + 1))

  local summary="$lock_dir/summary"
  : >"$summary"

  local result output
  result="$(call_subject_in "$dir" 'preflight_lockfile' \
    "PATH=$cargo_stub_dir:$PATH" GITHUB_STEP_SUMMARY="$summary" "$@")"
  output="${result#*|}"

  if [[ "$output" == *"$unwanted"* ]] || grep -qF "$unwanted" "$summary"; then
    fail_test "$name: did not expect '${unwanted}', got '${output}' / '$(cat "$summary")'"
    return
  fi
  echo "  ok: $name"
}

with_lock_dir="$lock_dir/with-lock"
mkdir -p "$with_lock_dir"
printf 'version = 4\n' >"$with_lock_dir/Cargo.lock"
no_lock_dir="$lock_dir/no-lock"
mkdir -p "$no_lock_dir"

# The wording cargo 1.96 uses, and the wording earlier releases used. The guard
# matches on `--locked was passed` rather than either whole sentence: pinned to
# one phrasing it would silently stop guarding on a toolchain bump, and both
# spellings name the flag.
readonly LOCKED_REFUSAL='error: cannot update the lock file /w/Cargo.lock because --locked was passed to prevent this'
readonly LOCKED_REFUSAL_OLD='error: the lock file /w/Cargo.lock needs to be updated but --locked was passed to prevent this'

assert_preflight_lock "proceeds when the lockfile still matches" "$with_lock_dir" 0 ""
# A directory with no lockfile is not one this guard has an opinion about.
assert_preflight_lock "proceeds when there is no Cargo.lock" "$no_lock_dir" 0 "" \
  STUB_CARGO_RC=101 STUB_CARGO_ERR="$LOCKED_REFUSAL"

# The shape this exists for: one PR bumps the version in [workspace.package]
# while another adds a member, git merges both cleanly because they touch
# different regions of the lockfile, and the combination is stale. Neither author
# can see it, because on a pull request `Attestation` is the only job that runs.
assert_preflight_lock "stops a branch whose lockfile is stale" "$with_lock_dir" 72 \
  "no longer matches the workspace manifests" \
  STUB_CARGO_RC=101 STUB_CARGO_ERR="$LOCKED_REFUSAL"
assert_preflight_lock "recognises the older cargo wording too" "$with_lock_dir" 72 \
  "no longer matches the workspace manifests" \
  STUB_CARGO_RC=101 STUB_CARGO_ERR="$LOCKED_REFUSAL_OLD"
assert_preflight_lock "says the branch was not evaluated" "$with_lock_dir" 72 \
  "not evaluated" \
  STUB_CARGO_RC=101 STUB_CARGO_ERR="$LOCKED_REFUSAL"
assert_preflight_lock "names the remedy rather than only the symptom" "$with_lock_dir" 72 \
  "commit it, and sign off again" \
  STUB_CARGO_RC=101 STUB_CARGO_ERR="$LOCKED_REFUSAL"
# Fatal locally as well as remotely: unlike the disk floor this is not a
# threshold someone may reasonably run under, it is a step certain to fail after
# the whole gate has run.
assert_preflight_lock "stops a local run as well as a remote one" "$with_lock_dir" 72 \
  "no longer matches the workspace manifests" \
  STUB_CARGO_RC=101 STUB_CARGO_ERR="$LOCKED_REFUSAL"
assert_preflight_lock "annotates a remote stop for the run page" "$with_lock_dir" 72 \
  "::error title=Sign-off cannot run on this branch::" \
  SIGNOFF_REMOTE_RUN=1 STUB_CARGO_RC=101 STUB_CARGO_ERR="$LOCKED_REFUSAL"

# The other direction, and the one that matters more. An unreachable registry, a
# manifest cargo cannot parse, a missing rustc — the gate itself reports all of
# those minutes later with context this step does not have.
assert_preflight_lock "proceeds when cargo failed for another reason" "$with_lock_dir" 0 \
  "could not check whether Cargo.lock is current" \
  STUB_CARGO_RC=101 STUB_CARGO_ERR="error: failed to get 'serde' as a dependency: network unreachable"
assert_preflight_lock_silent_on "does not call an unrelated cargo failure a stale lockfile" \
  "$with_lock_dir" "no longer matches the workspace manifests" \
  STUB_CARGO_RC=101 STUB_CARGO_ERR="error: failed to get 'serde' as a dependency: network unreachable"
# `--locked` appearing in unrelated output is not cargo refusing to rewrite the
# lock; the refusal names the flag as *passed*.
assert_preflight_lock "does not read a mention of --locked as a refusal" "$with_lock_dir" 0 \
  "could not check whether Cargo.lock is current" \
  STUB_CARGO_RC=101 STUB_CARGO_ERR="error: unexpected argument '--locked-x' found"

# No cargo at all is the same class of answer as no lockfile: the guard has
# nothing to read, and refusing a sign-off over its own missing tool would be a
# check deciding a verdict it never computed.
#
# The PATH below keeps bash reachable and drops cargo. It cannot simply be
# emptied: call_subject_in runs the subject through `env PATH=... bash -c`, so a
# PATH without bash fails to start the shell at all and the case would pass for
# the wrong reason. Naming the directory bash itself came from keeps that
# independent of where this runs, and cargo lives under ~/.cargo/bin rather than
# beside bash.
bash_dir="$(dirname "$(command -v bash)")"
assert_preflight_lock "proceeds when cargo is unavailable" "$with_lock_dir" 0 "" \
  "PATH=$bash_dir"

echo
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
  # The watcher reads two signatures off one stream, so every case asserts both
  # verdicts. Defaulting this to "no" means the existing disk cases now also
  # prove the cache signature does not cross-fire on them.
  local want_cache_marked="${6:-no}"
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
      echo "CACHEHIT=${SIGNOFF_CACHE_HIT:+yes}"
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

  local cache_marked="no"
  [[ "$output" == *"CACHEHIT=yes"* ]] && cache_marked="yes"
  if [[ "$output" != *"CACHEHIT="* ]]; then
    fail_test "$name: the step never reported a cache verdict — output: '${output}'"
    rm -f "$fake_make"
    return
  fi
  if [[ "$cache_marked" != "$want_cache_marked" ]]; then
    fail_test "$name: expected cache_marked=${want_cache_marked}, got ${cache_marked} (output: '${output}')"
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

# Verbatim from run 30978407363, where sccache's storage endpoint stopped
# answering 98 minutes into a sign-off and the run reported "Sign-off checks
# failed" about a branch nothing had compiled.
assert_recorder "records sccache failing to reach its storage" \
  'echo "sccache: error: Server startup failed: cache storage failed to read: Unexpected (temporary) at read => send http request"
   echo "error: process didn'"'"'t exit successfully: \`sccache /Users/runner/.rustup/toolchains/1.96.1-aarch64-apple-darwin/bin/rustc -vV\` (exit status: 2)"
   exit 101' \
  101 no "Server startup failed" yes
assert_recorder "records a storage failure reported without the startup prefix" \
  'echo "sccache: error: cache storage failed to read: Unexpected (temporary)"; exit 101' \
  101 no "" yes
# The third spelling (#12622). sccache reaches this one when the server it spawns
# never comes up inside the startup timeout, which is how an endpoint that stops
# answering during a run reads once the previous server has exited. Wording is
# verbatim from sccache's `connect_or_start_server`; the second line is the same
# bail's own continuation, unprefixed, and must not be what the guard matches.
assert_recorder "records sccache timing out waiting for its server to start" \
  'echo "sccache: error: Timed out waiting for server startup. Maybe the remote service is unreachable?"
   echo "Run with SCCACHE_LOG=debug SCCACHE_NO_DAEMON=1 to get more information"
   echo "error: process didn'"'"'t exit successfully: \`sccache /Users/runner/.rustup/toolchains/1.96.1-aarch64-apple-darwin/bin/rustc -vV\` (exit status: 2)"
   exit 101' \
  101 no "Timed out waiting for server startup" yes
# `sccache --start-server` bails with the same sentence and no trailing advice,
# so the guard anchors on the prefix the two share rather than on either whole
# sentence — this case fails if the pattern ever pins the "Maybe the remote
# service is unreachable?" suffix.
assert_recorder "records the bare startup-timeout wording too" \
  'echo "sccache: error: Timed out waiting for server startup"; exit 101' \
  101 no "" yes
# The other direction, as for the storage spellings: the sentence only counts on
# sccache's own error channel. A build that merely prints it — a test asserting
# on the wording, or a log echoed back — is still a failure about the branch.
assert_recorder "leaves a failure unmarked when the timeout wording is only quoted" \
  'echo "Timed out waiting for server startup"
   echo "error[E0308]: mismatched types"
   exit 101' \
  101 no "E0308" no
# The direction that matters: a genuine defect must not be excused because the
# word sccache appeared. Only sccache's own `error:` channel counts.
assert_recorder "leaves a compile failure unmarked when sccache merely ran" \
  'echo "Compiling runtime v2.3.0"; echo "sccache: Starting the server..."; echo "error[E0308]: mismatched types"; exit 101' \
  101 no "E0308" no
# Disk wins when both appear: a volume at zero can break the cache endpoint too,
# and reclaiming space is the remedy that fixes both.
assert_recorder "reports disk, not cache, when the volume filled and took sccache with it" \
  'echo "sccache: error: Server startup failed: cache storage failed to read"
   echo "ld: write() failed, errno=28 (No space left on device)"
   exit 101' \
  101 yes "errno=28" no

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

# The case above asserts make's status survives a dead watcher, but its stub
# make writes a single short line, so whether make finishes writing before the
# watcher closes the pipe is a race — it reported 141 (SIGPIPE) in roughly one
# run in fifteen (#12734). The race was the test's, the loss was not: a watcher
# placed downstream of make and exiting at once takes make's status with it,
# and the more the step writes, the likelier that is. So this case pins the
# same guarantee under a make that keeps writing, where the old ordering fails
# every time rather than occasionally.
tests_run=$((tests_run + 1))
chatty_dir="$stub_dir/chatty-writer"
mkdir -p "$chatty_dir"
cat >"$chatty_dir/awk" <<STUB
#!/usr/bin/env bash
set -uo pipefail
for arg in "\$@"; do
  [[ "\$arg" == pat=* ]] && exit 2
done
exec "${real_awk}" "\$@"
STUB
chmod +x "$chatty_dir/awk"
# Enough output that a closed reader is certain to be noticed, and an explicit
# status afterwards so anything other than 101 means make never got to return.
cat >"$chatty_dir/make" <<'STUB'
#!/usr/bin/env bash
set -uo pipefail
for i in $(seq 1 2000); do
  echo "compiling crate number ${i} with a line long enough to fill a pipe buffer"
done
exit 101
STUB
chmod +x "$chatty_dir/make"

chatty_output="$(env "PATH=$chatty_dir:$stub_dir:$PATH" SIGNOFF_DISK_WATCH=1 \
  STUB_FREE_KB="$(gib_to_kb 200)" \
  bash -c 'source "$1"
    if run_make_step some-target >/dev/null; then step_rc=0; else step_rc=$?; fi
    echo "RC=${step_rc}"
    echo "ARMED=${SIGNOFF_DISK_WATCH:+yes}"' _ "$subject" 2>&1)"

if [[ "$chatty_output" == *"RC=141"* ]]; then
  fail_test "a watcher that cannot run must not cost make its status to SIGPIPE: '${chatty_output}'"
elif [[ "$chatty_output" != *"RC=101"* ]]; then
  fail_test "a chatty step under a dead watcher still reports make's own status: expected RC=101, got '${chatty_output}'"
elif [[ "$chatty_output" == *"ARMED=yes"* ]]; then
  fail_test "a watcher that never ran must leave the watch disarmed: '${chatty_output}'"
else
  echo "  ok: a step that keeps writing still reports its own status when the watcher cannot run"
fi
rm -rf "$chatty_dir"
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

# The third way to reach "the checks reached no verdict": the branch's Makefile
# had no rule for a target the gate invokes, so nothing was compiled at all.
# It has to stay distinct from "checks" — a verdict worded like a lint denial is
# what sent authors to read a 14k-line log of passing tests (#12813).
assert_failure_kind "calls a missing make target its own kind, not a check failure" 71 "missing-target" \
  STUB_FREE_KB="$(gib_to_kb 200)"
# The preflight refused before anything ran, so no later reading may overrule
# it — neither a volume that happens to be empty nor a stale cache flag.
assert_failure_kind "keeps a missing make target distinct on a near-empty volume" 71 "missing-target" \
  STUB_FREE_KB="$(gib_to_kb 1)"
assert_failure_kind "keeps a missing make target distinct with a cache hit recorded" 71 "missing-target" \
  SIGNOFF_DISK_WATCH=1 SIGNOFF_CACHE_HIT=1 STUB_FREE_KB="$(gib_to_kb 200)"
# ...and it must not swallow a genuinely signalled run, which is decided first.
assert_failure_kind "a signalled run stays signalled, not a missing target" 143 "signalled" \
  STUB_FREE_KB="$(gib_to_kb 200)"

# The fourth way, and the newest: the lockfile preflight refused to start because
# Cargo.lock no longer describes the manifests. Same requirement as
# missing-target — it must stay distinct from "checks", or the status reads as a
# lint denial for a run that compiled nothing at all.
assert_failure_kind "calls a stale lockfile its own kind, not a check failure" 72 "stale-lockfile" \
  STUB_FREE_KB="$(gib_to_kb 200)"
# The preflight refused before anything ran, so no later reading may overrule it.
assert_failure_kind "keeps a stale lockfile distinct on a near-empty volume" 72 "stale-lockfile" \
  STUB_FREE_KB="$(gib_to_kb 1)"
assert_failure_kind "keeps a stale lockfile distinct with a cache hit recorded" 72 "stale-lockfile" \
  SIGNOFF_DISK_WATCH=1 SIGNOFF_CACHE_HIT=1 STUB_FREE_KB="$(gib_to_kb 200)"
# ...and it must not swallow a genuinely signalled run, which is decided first.
assert_failure_kind "a signalled run stays signalled, not a stale lockfile" 72 "signalled" \
  SIGNOFF_SIGNALLED=1 STUB_FREE_KB="$(gib_to_kb 200)"
# The other direction matters just as much: a real defect on a tight disk must
# not be excused as infrastructure, or a broken branch signs off as "re-dispatch
# me". 10 GiB is under the 25 GiB preflight floor and well over the critical bar.
assert_failure_kind "calls a failure on a tight but workable volume a check failure" 101 "checks" \
  STUB_FREE_KB="$(gib_to_kb 10)"
assert_failure_kind "calls a failure on a roomy volume a check failure" 101 "checks" \
  STUB_FREE_KB="$(gib_to_kb 200)"
assert_failure_kind "calls a failure with unknown free space a check failure" 101 "checks" \
  STUB_DF_RC=1

# The cache verdict, same shape as the disk one: authoritative when the watch was
# armed, and worth nothing without it.
assert_failure_kind "calls an unreachable compiler cache an infrastructure failure" 101 "cache" \
  SIGNOFF_DISK_WATCH=1 SIGNOFF_CACHE_HIT=1 STUB_FREE_KB="$(gib_to_kb 200)"
# Disk outranks cache: an out-of-disk volume can break sccache's storage, and
# "reclaim space" is then the remedy that fixes both.
assert_failure_kind "prefers the disk verdict when the build reported both" 101 "disk" \
  SIGNOFF_DISK_WATCH=1 SIGNOFF_DISK_HIT=1 SIGNOFF_CACHE_HIT=1 STUB_FREE_KB="$(gib_to_kb 200)"
# A cache hit with no armed watch is not a reading, exactly as for disk — and
# there is no after-the-fact backstop for the cache, so it falls through to the
# free-space one and lands on the branch.
assert_failure_kind "a cache hit without an armed watch does not classify" 101 "checks" \
  SIGNOFF_CACHE_HIT=1 STUB_FREE_KB="$(gib_to_kb 200)"
# The preflight's own refusal is about disk and must not be recoloured by an
# inherited cache flag.
assert_failure_kind "the preflight refusal stays a disk failure with a cache flag set" 70 "disk" \
  SIGNOFF_DISK_WATCH=1 SIGNOFF_CACHE_HIT=1 STUB_FREE_KB="$(gib_to_kb 60)"
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
# And over the cache verdict, for the same reason: a run that was killed judged
# nothing, whatever the build had printed about sccache before it died.
assert_failure_kind "keeps a signalled run signalled with a cache hit recorded" 143 "signalled" \
  SIGNOFF_DISK_WATCH=1 SIGNOFF_CACHE_HIT=1 STUB_FREE_KB="$(gib_to_kb 200)"
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
assert_describe "still publishes the unreachable-cache verdict" 101 \
  "Compiler cache unreachable after 21195s — checks did not complete, re-dispatch (triggered by someone)" \
  "sccache could not reach its storage" \
  SIGNOFF_DISK_WATCH=1 SIGNOFF_CACHE_HIT=1 STUB_FREE_KB="$(gib_to_kb 200)"
assert_describe "still publishes a genuine check failure" 101 \
  "Sign-off checks failed after 21195s (triggered by someone)" \
  "sign-off checks failed" STUB_FREE_KB="$(gib_to_kb 200)"
# The published half of #12710, and the one an operator actually reads: a
# cancelled run whose make returned 2 must publish no description at all. The
# case directly above is the same status with no signal recorded and does
# publish one, so this pins the difference to the signal and not to the status.
assert_describe "publishes no verdict when a signalled run's make returned an ordinary status" 2 "" \
  "the checks reached no verdict" SIGNOFF_SIGNALLED=1 STUB_FREE_KB="$(gib_to_kb 200)"

# A branch that predates a gate target has to say so in the commit status
# itself: that description is all most readers see, and worded as a check
# failure it is indistinguishable from a lint denial. It also has to carry the
# remedy, because the reader who needs it is not going to open the log.
assert_describe "says a missing make target could not run, not that checks failed" 71 \
  "Sign-off could not run after 21195s — branch predates a make target the gate needs; merge trunk in (triggered by someone)" \
  "the checks did not run" STUB_FREE_KB="$(gib_to_kb 200)"
assert_describe "tells the author to merge trunk in" 71 \
  "Sign-off could not run after 21195s — branch predates a make target the gate needs; merge trunk in (triggered by someone)" \
  "merge trunk in and sign off again" STUB_FREE_KB="$(gib_to_kb 200)"

# The two above use a status the signal reading must not claim. A run signalled
# while that same status is recorded reaches no verdict, and "no verdict"
# outranks naming a cause — the missing-target wording would assert the gate
# reached one.
assert_describe "declines the missing-target verdict for a signalled run" 71 "" \
  "the checks reached no verdict" SIGNOFF_SIGNALLED=1 STUB_FREE_KB="$(gib_to_kb 200)"

# A branch whose lockfile is stale has the same problem as one that predates a
# gate target: the commit-status description is all most readers see, and worded
# as a check failure it sends them looking for a lint denial in a log containing
# no compilation. The remedy has to be in the description itself.
assert_describe "says a stale lockfile could not run, not that checks failed" 72 \
  "Sign-off could not run after 21195s — Cargo.lock does not match the manifests; regenerate and commit it (triggered by someone)" \
  "the checks did not run" STUB_FREE_KB="$(gib_to_kb 200)"
assert_describe "tells the author to regenerate and commit the lockfile" 72 \
  "Sign-off could not run after 21195s — Cargo.lock does not match the manifests; regenerate and commit it (triggered by someone)" \
  "regenerate and commit it, then sign off again" STUB_FREE_KB="$(gib_to_kb 200)"
# And, as for missing-target, "no verdict" outranks naming a cause.
assert_describe "declines the stale-lockfile verdict for a signalled run" 72 "" \
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
