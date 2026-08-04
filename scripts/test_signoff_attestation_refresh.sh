#!/usr/bin/env bash
#
# Unit tests for `refresh_attestation_check` in `scripts/signoff` — the step that
# re-runs the PR's `pr.yml` run so `Attestation` re-reads the `signoff` status the
# sign-off just posted.
#
# The refresh selects that run by commit, and a remote sign-off can run for hours
# (the job budget is 358 minutes), so the branch may have advanced underneath it.
# `pr.yml`'s concurrency group collapses its SHA term to the literal `any-sha` on
# a `pull_request` event, so every attempt for a PR shares one group: re-running a
# stale commit's run cancels the *current* head's in-flight one, and the re-run
# reports a verdict computed from its own original payload — the stale commit
# (#12360). So the refresh must fire when the SHA is still the PR head and skip
# when it is not, without regressing the ordinary cases (no PR yet, lookup down).
#
# No network and no credentials: a stub `gh` on PATH answers the two lookups and
# records whether the rerun was attempted.
#
# Usage: scripts/test_signoff_attestation_refresh.sh

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

reruns="$stub_dir/reruns"

# The SHA under sign-off, and a later one standing in for "the branch moved".
SIGNED_SHA="1111111111111111111111111111111111111111"
NEWER_SHA="2222222222222222222222222222222222222222"

# A `gh` covering exactly the three calls this function makes:
#
#   api repos/<repo>/commits/<sha>/pulls  -> STUB_PR_HEADS (space-separated), or
#                                            exit STUB_PULLS_RC to fail the lookup
#   run list …                            -> STUB_RUN ("<id> <status> <conclusion>")
#   run rerun <id>                        -> recorded to STUB_RERUNS
#
# Any other invocation is a hard error — several cases assert that *no* rerun
# happens, and a stub that quietly accepted an unexpected call could hide one.
cat >"$stub_dir/gh" <<'STUB'
#!/usr/bin/env bash
set -uo pipefail

if [[ "${1:-}" == "api" && "${2:-}" == */pulls ]]; then
  [[ "${STUB_PULLS_RC:-0}" == "0" ]] || exit "${STUB_PULLS_RC}"
  # The subject asks for a joined list, which is what the stub is configured with.
  printf '%s' "${STUB_PR_HEADS:-}"
  exit 0
fi

if [[ "${1:-}" == "run" && "${2:-}" == "list" ]]; then
  printf '%s' "${STUB_RUN:-}"
  exit 0
fi

if [[ "${1:-}" == "run" && "${2:-}" == "rerun" ]]; then
  printf '%s\n' "rerun ${3:-}" >>"${STUB_RERUNS}"
  exit "${STUB_RERUN_RC:-0}"
fi

echo "stub gh: unexpected invocation: $*" >&2
exit 99
STUB
chmod +x "$stub_dir/gh"

# Calls the one function under test in a fresh bash with the stub `gh` first on
# PATH. Echoes "<rc>|<output>".
call_refresh() {
  local sha="$1"
  shift
  local output rc
  # `env` rather than an assignment prefix: these come from "$@", and words that
  # only look like assignments after expansion are read as the command name.
  # STUB_FORCE feeds the optional third argument (the failed-sign-off force path),
  # so a case can exercise it the same way it sets any other stub input.
  output="$(env "PATH=$stub_dir:$PATH" "STUB_RERUNS=$reruns" "$@" \
    bash -c 'source "$1"; refresh_attestation_check "$2" "$3" "${STUB_FORCE:-}"' \
    _ "$subject" "$sha" "spiceai/spiceai" 2>&1)"
  rc=$?
  printf '%s|%s' "$rc" "$output"
}

# $3: 'rerun' if the Attestation run must be re-run, 'norerun' if it must not be.
assert_refresh() {
  local name="$1" want_output="$2" want_rerun="$3" sha="$4"
  shift 4
  tests_run=$((tests_run + 1))

  : >"$reruns"

  local result rc output
  result="$(call_refresh "$sha" "$@")"
  rc="${result%%|*}"
  output="${result#*|}"

  # The refresh is a convenience on top of a sign-off that already succeeded;
  # nothing it discovers may fail the sign-off.
  if [[ "$rc" -ne 0 ]]; then
    fail_test "$name: expected exit 0 (must not fail the sign-off), got ${rc} (output: ${output})"
    return
  fi

  if [[ "$output" != *"$want_output"* ]]; then
    fail_test "$name: expected '${want_output}' in the output, got '${output}'"
    return
  fi

  local rerun_count
  rerun_count=$(grep -c 'rerun' "$reruns" 2>/dev/null || true)
  rerun_count="${rerun_count:-0}"

  case "$want_rerun" in
    rerun)
      if [[ "$rerun_count" -ne 1 ]]; then
        fail_test "$name: expected exactly 1 rerun, got ${rerun_count}"
        return
      fi
      ;;
    norerun)
      if [[ "$rerun_count" -ne 0 ]]; then
        fail_test "$name: expected NO rerun, got ${rerun_count}: '$(cat "$reruns")'"
        return
      fi
      ;;
    *)
      fail_test "$name: bad want_rerun '${want_rerun}'"
      return
      ;;
  esac

  echo "  ok: $name"
}

echo "Testing refresh_attestation_check in ${subject}"
echo

echo "The SHA is still the PR head — refresh, as before:"
assert_refresh "a failed Attestation run on the current head is re-run" \
  "Re-ran the 'Attestation' check" rerun "$SIGNED_SHA" \
  STUB_PR_HEADS="$SIGNED_SHA" STUB_RUN="4242 completed failure"
# Several open PRs can contain one commit; being the head of any of them is
# enough for the refresh to be aimed at the right run.
assert_refresh "still refreshes when it is one of several PR heads" \
  "Re-ran the 'Attestation' check" rerun "$SIGNED_SHA" \
  STUB_PR_HEADS="$NEWER_SHA $SIGNED_SHA" STUB_RUN="4242 completed failure"
echo

echo "The branch advanced — skip, and say why (#12360):"
# The regression this file exists for: re-running the stale commit's run would
# cancel the current head's in-flight Attestation.
assert_refresh "a stale SHA does not re-run anything" \
  "no longer the head of its PR" norerun "$SIGNED_SHA" \
  STUB_PR_HEADS="$NEWER_SHA" STUB_RUN="4242 completed failure"
assert_refresh "the skip names the current head" \
  "now ${NEWER_SHA:0:12}" norerun "$SIGNED_SHA" \
  STUB_PR_HEADS="$NEWER_SHA" STUB_RUN="4242 completed failure"
assert_refresh "the skip explains the cancellation risk" \
  "would cancel the current head's" norerun "$SIGNED_SHA" \
  STUB_PR_HEADS="$NEWER_SHA" STUB_RUN="4242 completed failure"
echo

echo "Cases that must NOT be read as stale — the guard has to fall through:"
# A commit signed off before its PR exists is the ordinary first-sign-off case.
assert_refresh "no open PR yet still refreshes" \
  "Re-ran the 'Attestation' check" rerun "$SIGNED_SHA" \
  STUB_PR_HEADS="" STUB_RUN="4242 completed failure"
# A lookup that fails is no reason to skip a refresh that would have worked.
assert_refresh "a failed PR lookup still refreshes" \
  "Re-ran the 'Attestation' check" rerun "$SIGNED_SHA" \
  STUB_PULLS_RC=1 STUB_RUN="4242 completed failure"
echo

echo "The pre-existing run-state behaviour is unchanged:"
assert_refresh "an in-progress run is left to finish" \
  "still in_progress" norerun "$SIGNED_SHA" \
  STUB_PR_HEADS="$SIGNED_SHA" STUB_RUN="4242 in_progress "
assert_refresh "an already-green run is not re-run" \
  "" norerun "$SIGNED_SHA" \
  STUB_PR_HEADS="$SIGNED_SHA" STUB_RUN="4242 completed success"
assert_refresh "no run found falls back to the manual hint" \
  "Re-run the PR's 'Attestation' check from the Checks tab" norerun "$SIGNED_SHA" \
  STUB_PR_HEADS="$SIGNED_SHA" STUB_RUN=""
assert_refresh "a rerun that fails reports the fallback" \
  "Could not auto-refresh" rerun "$SIGNED_SHA" \
  STUB_PR_HEADS="$SIGNED_SHA" STUB_RUN="4242 completed failure" STUB_RERUN_RC=1
echo

echo "The force path (a failed sign-off, #12346) gets the same guard:"
# `force` exists so a *failed* sign-off re-runs a run that concluded success,
# overturning an attestation it just invalidated. The staleness question is
# orthogonal: a commit that is no longer the PR head must not be re-run either
# way, or the force path reintroduces the same cancellation.
assert_refresh "force still refreshes on the current head" \
  "Re-ran the 'Attestation' check" rerun "$SIGNED_SHA" \
  STUB_PR_HEADS="$SIGNED_SHA" STUB_RUN="4242 completed success" STUB_FORCE=1
assert_refresh "force does not re-run a stale commit's check" \
  "no longer the head of its PR" norerun "$SIGNED_SHA" \
  STUB_PR_HEADS="$NEWER_SHA" STUB_RUN="4242 completed success" STUB_FORCE=1
echo

echo "A missing gh is a no-op, not a crash:"
tests_run=$((tests_run + 1))
: >"$reruns"
# A PATH with no gh at all; bash is invoked by absolute path.
output="$(env "PATH=$stub_dir/empty" "STUB_RERUNS=$reruns" \
  "$(command -v bash)" -c 'source "$1"; refresh_attestation_check "$2" "$3"' \
  _ "$subject" "$SIGNED_SHA" "spiceai/spiceai" 2>&1)"
rc=$?
if [[ "$rc" -ne 0 ]]; then
  fail_test "missing gh exits 0: got ${rc} (output: ${output})"
elif [[ "$output" != *"Install the GitHub CLI"* ]]; then
  fail_test "missing gh says so: expected 'Install the GitHub CLI', got '${output}'"
else
  echo "  ok: a missing gh is a no-op"
fi
echo

echo "----------------------------------------"
if [[ "$failures" -eq 0 ]]; then
  echo "PASS: ${tests_run} assertions"
  exit 0
fi
echo "FAIL: ${failures} of ${tests_run} assertions failed"
exit 1
