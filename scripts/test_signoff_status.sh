#!/usr/bin/env bash
#
# Unit tests for the `signoff` commit status: which states `scripts/signoff` may
# post over which — the in-progress `pending` a remote run posts on the way in,
# the `clear-pending` that resolves one left behind, and the failing verdict —
# plus whether each of those reaches the `Attestation` check that gates the PR.
# No network and no credentials: a stub `gh` on PATH serves a canned commit
# status and workflow run, and records every status posted and run re-run.
#
# Usage: scripts/test_signoff_status.sh

set -uo pipefail

# Every assertion below that exercises a failing post wants one call and no
# waiting; the retry is covered on its own by assert_post_retry, which sets the
# backoff it needs.
export SIGNOFF_STATUS_POST_BACKOFF=""

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
subject="$script_dir/signoff"

tests_run=0
failures=0

fail_test() {
  failures=$((failures + 1))
  echo "  FAIL: $1"
}

# A `gh` that answers the four calls the subject makes: read the combined status
# for a commit, post a new one, list the PR's workflow run, and re-run it.
# STUB_STATE is the `signoff` state the read reports ("none" for a commit that
# has none); STUB_READ_RC makes the read fail and STUB_POST_RC makes the post
# fail; every post that lands is appended to STUB_POSTS as
# `state<TAB>context<TAB>description`. STUB_RUN is the `<id> <status>
# <conclusion>` line the run list reports (empty for no run yet) and every
# re-run id is appended to STUB_RERUNS.
write_gh_stub() {
  local dir="$1"
  cat >"$dir/gh" <<'STUB'
#!/usr/bin/env bash
set -uo pipefail

case "${1:-}" in
  auth) exit 0 ;;
  repo) echo "spiceai/spiceai"; exit 0 ;;
  run)
    case "${2:-}" in
      # The subject asks gh's own jq for one flattened line, so answer with the
      # value that expression would have produced.
      list)
        [[ "${STUB_RUN_LIST_RC:-0}" == "0" ]] || exit "${STUB_RUN_LIST_RC}"
        [[ -z "${STUB_RUN:-}" ]] || printf '%s\n' "${STUB_RUN}"
        exit 0 ;;
      rerun)
        printf '%s\n' "${3:-}" >>"${STUB_RERUNS:-/dev/null}"
        exit "${STUB_RERUN_RC:-0}" ;;
    esac
    echo "stub gh: unexpected run subcommand: $*" >&2
    exit 64 ;;
esac

if [[ "${1:-}" != "api" ]]; then
  echo "stub gh: unexpected command: $*" >&2
  exit 64
fi

if [[ "$*" == *"--method POST"* ]]; then
  # A post that fails records nothing: the status never landed on the commit.
  # STUB_POST_FAILURES names a file holding how many of the next posts must
  # fail; each failing attempt decrements it, so a caller that retries gets
  # through once it reaches zero. The body is the one that stranded the verdict
  # the retry exists for.
  if [[ -n "${STUB_POST_FAILURES:-}" ]]; then
    remaining="$(cat "$STUB_POST_FAILURES" 2>/dev/null)"
    if (( "${remaining:-0}" > 0 )); then
      printf '%s\n' "$(( remaining - 1 ))" >"$STUB_POST_FAILURES"
      echo "gh: Bad credentials (HTTP 401)" >&2
      exit 1
    fi
  fi
  if [[ "${STUB_POST_RC:-0}" != "0" ]]; then
    echo "gh: Bad credentials (HTTP 401)" >&2
    exit "${STUB_POST_RC}"
  fi
  state="" context="" description=""
  while [[ $# -gt 0 ]]; do
    case "$1" in
      -f) case "$2" in
            state=*)       state="${2#state=}" ;;
            context=*)     context="${2#context=}" ;;
            description=*) description="${2#description=}" ;;
          esac
          shift 2 ;;
      *) shift ;;
    esac
  done
  printf '%s\t%s\t%s\n' "$state" "$context" "$description" >>"$STUB_POSTS"
  exit 0
fi

# The read. The subject asks gh's own jq for the state, so answer with the value
# the expression would have produced rather than re-implementing it.
if [[ "${STUB_READ_RC:-0}" != "0" ]]; then
  echo "stub gh: canned read failure" >&2
  exit "${STUB_READ_RC}"
fi
printf '%s\n' "$STUB_STATE"
exit 0
STUB
  chmod +x "$dir/gh"
}

# Runs `clear-pending` against a commit whose signoff status is $2, then checks
# the posted statuses against the remaining arguments (none = expect no post).
assert_clear_pending() {
  local name="$1" state="$2" want_state="${3:-}" want_description="${4:-}"
  tests_run=$((tests_run + 1))

  local posts="$stub_dir/posts"
  : >"$posts"

  local output rc
  output="$(PATH="$stub_dir:$PATH" STUB_STATE="$state" STUB_POSTS="$posts" \
    "$subject" clear-pending "a canned reason" 2>&1)"
  rc=$?

  if [[ "$rc" -ne 0 ]]; then
    fail_test "$name: expected exit 0, got ${rc} (output: ${output})"
    return
  fi

  local posted
  posted="$(cat "$posts")"

  if [[ -z "$want_state" ]]; then
    if [[ -n "$posted" ]]; then
      fail_test "$name: expected no status to be posted, got '${posted}'"
      return
    fi
    echo "  ok: $name"
    return
  fi

  local lines
  lines="$(printf '%s\n' "$posted" | grep -c .)"
  if [[ "$lines" -ne 1 ]]; then
    fail_test "$name: expected exactly one posted status, got ${lines} ('${posted}')"
    return
  fi
  if [[ "$posted" != "${want_state}"$'\t'"signoff"$'\t'* ]]; then
    fail_test "$name: expected a '${want_state}' status in the 'signoff' context, got '${posted}'"
    return
  fi
  if [[ "$posted" != *"${want_description}"* ]]; then
    fail_test "$name: expected the description to carry '${want_description}', got '${posted}'"
    return
  fi
  echo "  ok: $name"
}

# Calls post_pending_status against a commit whose signoff status is $2, then
# checks the posted statuses against $3 (empty = expect no post). Sourcing the
# subject in a fresh bash calls the one function under test without running the
# checks a real `signoff` invocation would; $4, when set, is the read's exit code.
assert_pending_post() {
  local name="$1" state="$2" want_state="${3:-}" read_rc="${4:-0}"
  tests_run=$((tests_run + 1))

  local posts="$stub_dir/posts"
  : >"$posts"

  local output rc
  output="$(PATH="$stub_dir:$PATH" STUB_STATE="$state" STUB_POSTS="$posts" \
    STUB_READ_RC="$read_rc" \
    bash -c 'source "$1"; post_pending_status spiceai/spiceai '"$fake_sha"' someone' \
    _ "$subject" 2>&1)"
  rc=$?

  # A failure to report progress must never abort the sign-off it precedes.
  if [[ "$rc" -ne 0 ]]; then
    fail_test "$name: expected exit 0, got ${rc} (output: ${output})"
    return
  fi

  local posted
  posted="$(cat "$posts")"

  if [[ -z "$want_state" ]]; then
    if [[ -n "$posted" ]]; then
      fail_test "$name: expected no status to be posted, got '${posted}'"
      return
    fi
    echo "  ok: $name"
    return
  fi

  local lines
  lines="$(printf '%s\n' "$posted" | grep -c .)"
  if [[ "$lines" -ne 1 ]]; then
    fail_test "$name: expected exactly one posted status, got ${lines} ('${posted}')"
    return
  fi
  if [[ "$posted" != "${want_state}"$'\t'"signoff"$'\t'* ]]; then
    fail_test "$name: expected a '${want_state}' status in the 'signoff' context, got '${posted}'"
    return
  fi
  echo "  ok: $name"
}

# Calls post_failure_status against a commit whose Attestation run is described
# by $2 (`<id> <status> <conclusion>`, empty for no run yet), then checks that
# the failing verdict was posted ($3, empty = expect no post) and that the check
# was re-run ($4, empty = expect no re-run). $5, when set, fails the post.
assert_failure_post() {
  local name="$1" run="$2" want_state="${3:-}" want_rerun="${4:-}" post_rc="${5:-0}"
  tests_run=$((tests_run + 1))

  local posts="$stub_dir/posts" reruns="$stub_dir/reruns"
  : >"$posts"
  : >"$reruns"

  local output rc
  output="$(PATH="$stub_dir:$PATH" STUB_POSTS="$posts" STUB_RERUNS="$reruns" \
    STUB_RUN="$run" STUB_POST_RC="$post_rc" \
    bash -c 'source "$1"; post_failure_status spiceai/spiceai "$2" "Sign-off checks failed after 42s (triggered by someone)"' \
    _ "$subject" "$fake_sha" 2>&1)"
  rc=$?

  # Reporting the verdict must not pre-empt the caller's own failure path.
  if [[ "$rc" -ne 0 ]]; then
    fail_test "$name: expected exit 0, got ${rc} (output: ${output})"
    return
  fi

  local posted
  posted="$(cat "$posts")"
  if [[ -z "$want_state" ]]; then
    if [[ -n "$posted" ]]; then
      fail_test "$name: expected no status to be posted, got '${posted}'"
      return
    fi
  elif [[ "$posted" != "${want_state}"$'\t'"signoff"$'\t'* ]]; then
    fail_test "$name: expected a '${want_state}' status in the 'signoff' context, got '${posted}'"
    return
  fi

  assert_reruns "$name" "$reruns" "$want_rerun" "$output"
}

# Calls refresh_attestation_check directly against the run described by $2, with
# force set from $3, and checks which run id (if any) was re-run. $6, when set,
# is a substring the output must carry.
assert_attestation_refresh() {
  local name="$1" run="$2" force="$3" want_rerun="${4:-}" list_rc="${5:-0}" want_output="${6:-}"
  tests_run=$((tests_run + 1))

  local reruns="$stub_dir/reruns"
  : >"$reruns"

  local output rc
  output="$(PATH="$stub_dir:$PATH" STUB_RERUNS="$reruns" STUB_RUN="$run" \
    STUB_RUN_LIST_RC="$list_rc" \
    bash -c 'source "$1"; refresh_attestation_check "$2" spiceai/spiceai "$3"' \
    _ "$subject" "$fake_sha" "$force" 2>&1)"
  rc=$?

  # Never fatal: the sign-off's own verdict has already been recorded.
  if [[ "$rc" -ne 0 ]]; then
    fail_test "$name: expected exit 0, got ${rc} (output: ${output})"
    return
  fi
  if [[ -n "$want_output" && "$output" != *"$want_output"* ]]; then
    fail_test "$name: expected the output to carry '${want_output}', got '${output}'"
    return
  fi

  assert_reruns "$name" "$reruns" "$want_rerun" "$output"
}

# The description post_failure_status is handed is what the commit status must
# carry: the caller distinguishes an out-of-disk runner from a failing branch,
# and that wording is all most readers ever see. Asserts it round-trips verbatim.
assert_failure_description() {
  local name="$1" description="$2"
  tests_run=$((tests_run + 1))

  local posts="$stub_dir/posts" reruns="$stub_dir/reruns"
  : >"$posts"
  : >"$reruns"

  PATH="$stub_dir:$PATH" STUB_POSTS="$posts" STUB_RERUNS="$reruns" \
    STUB_RUN="4242 completed success" STUB_POST_RC=0 \
    bash -c 'source "$1"; post_failure_status spiceai/spiceai "$2" "$3"' \
    _ "$subject" "$fake_sha" "$description" >/dev/null 2>&1

  local posted
  posted="$(cat "$posts")"
  if [[ "$posted" != "failure"$'\t'"signoff"$'\t'"$description"* ]]; then
    fail_test "$name: expected the posted description to be '${description}', got '${posted}'"
    return
  fi
  echo "  ok: $name"
}

# Shared tail of the two helpers above: exactly the expected run was re-run.
assert_reruns() {
  local name="$1" reruns="$2" want_rerun="$3" output="$4"
  local rerun
  rerun="$(tr -d '\n' <"$reruns")"

  if [[ "$rerun" != "$want_rerun" ]]; then
    if [[ -z "$want_rerun" ]]; then
      fail_test "$name: expected no 'Attestation' re-run, got run ${rerun} (output: ${output})"
    else
      fail_test "$name: expected run ${want_rerun} to be re-run, got '${rerun}' (output: ${output})"
    fi
    return
  fi
  echo "  ok: $name"
}

# Calls post_commit_status against an endpoint that fails its next $2 posts,
# under a backoff of $3 (empty = one attempt). Checks the return code against
# $4, that exactly $5 statuses landed on the commit, and that the output — the
# log plus the job summary, which is where a reader who is not tailing the run
# looks — carries $6.
assert_post_retry() {
  local name="$1" fail_times="$2" backoff="$3" want_rc="$4" want_posts="$5" want_output="${6:-}"
  local state="${7:-failure}" unwanted_output="${8:-}"
  tests_run=$((tests_run + 1))

  # Not named `failures`: that is the suite's own counter, and shadowing it here
  # makes fail_test's increment an arithmetic error, so a failing assertion
  # reports nothing and the suite still claims every test passed.
  local posts="$stub_dir/posts" failures_left="$stub_dir/failures" summary="$stub_dir/summary"
  : >"$posts"
  : >"$summary"
  printf '%s\n' "$fail_times" >"$failures_left"

  local output rc
  output="$(PATH="$stub_dir:$PATH" STUB_POSTS="$posts" STUB_POST_FAILURES="$failures_left" \
    SIGNOFF_STATUS_POST_BACKOFF="$backoff" GITHUB_STEP_SUMMARY="$summary" \
    bash -c 'source "$1"; post_commit_status spiceai/spiceai "$2" "$3" "a canned verdict"' \
    _ "$subject" "$fake_sha" "$state" 2>&1)"
  rc=$?
  output+=$'\n'"$(cat "$summary")"

  if [[ "$rc" -ne "$want_rc" ]]; then
    fail_test "$name: expected exit ${want_rc}, got ${rc} (output: ${output})"
    return
  fi

  local landed
  landed="$(grep -c . <"$posts")"
  if [[ "$landed" -ne "$want_posts" ]]; then
    fail_test "$name: expected ${want_posts} status(es) to land, got ${landed} (output: ${output})"
    return
  fi
  if [[ -n "$want_output" && "$output" != *"$want_output"* ]]; then
    fail_test "$name: expected the output to carry '${want_output}', got '${output}'"
    return
  fi
  if [[ -n "$unwanted_output" && "$output" == *"$unwanted_output"* ]]; then
    fail_test "$name: expected the output not to carry '${unwanted_output}', got '${output}'"
    return
  fi
  echo "  ok: $name"
}

# The BASH_SOURCE guard that makes the subject sourceable must not stop it from
# dispatching when it is executed — every other caller runs it that way.
assert_dispatches_when_executed() {
  tests_run=$((tests_run + 1))
  local output rc
  output="$("$subject" version 2>&1)"
  rc=$?
  if [[ "$rc" -ne 0 || "$output" != signoff\ * ]]; then
    fail_test "executing the script still dispatches: got rc=${rc}, output '${output}'"
    return
  fi
  echo "  ok: executing the script still dispatches a command"
}

readonly fake_sha="0123456789abcdef0123456789abcdef01234567"

stub_dir="$(mktemp -d)" || {
  echo "could not create a temp dir for the gh stub" >&2
  exit 1
}
trap 'rm -rf "$stub_dir"' EXIT
write_gh_stub "$stub_dir"

echo "scripts/signoff — the in-progress 'pending' status"

# The bug: a re-dispatch against an already-attested commit replaced a success a
# multi-hour run earned with `pending`, so a run that never reached a verdict of
# its own left the commit worse off than before it started.
assert_pending_post "an existing success is left alone" success

# Nothing of value to lose in any other state, and the in-progress marker is
# what `status` and the PR's status list read while the run is going.
assert_pending_post "a commit with no sign-off status gets a pending" none pending
assert_pending_post "a failed sign-off is overwritten with pending" failure pending
assert_pending_post "an errored sign-off is overwritten with pending" error pending
assert_pending_post "a stale pending is refreshed" pending pending

# Fail safe when the current state cannot be read: not posting costs a progress
# marker, posting blind could destroy an attestation.
assert_pending_post "an unreadable status posts nothing" success "" 1
assert_pending_post "an unreadable status posts nothing even with none cached" none "" 1

assert_dispatches_when_executed

echo
echo "scripts/signoff — the failing verdict and the gate that reads it"

# The bug: `Attestation` is the required check and pr.yml never runs on a
# commit-status change, so a re-sign-off that failed left the green attestation
# it had just overturned in place and the branch could still enter the queue.
assert_failure_post "a failed re-sign-off re-runs an already-green Attestation" \
  "4242 completed success" failure 4242

# Same post, and the refresh is what it always was for a check that is not green.
assert_failure_post "a failed sign-off re-runs a red Attestation" \
  "4242 completed failure" failure 4242

# Nothing to re-read when the status never landed — and the previous
# attestation, which the check is still reporting, is what the commit still has.
assert_failure_post "a post that fails leaves the check alone" \
  "4242 completed success" "" "" 1

# A run still going will read the verdict when it finishes; there is no run at
# all before the PR's first pr.yml run.
assert_failure_post "an in-flight Attestation is not re-run" \
  "4242 in_progress null" failure
assert_failure_post "a commit with no Attestation run is not re-run" \
  "" failure

# A run the runner broke and a failing branch reach this helper by the same path
# and are told apart only by the description, so it must not be rewritten en route.
assert_failure_description "an out-of-disk verdict keeps its own wording" \
  "Runner out of disk after 42s — checks did not complete, re-dispatch (triggered by someone)"
assert_failure_description "an unreachable-cache verdict keeps its own wording" \
  "Compiler cache unreachable after 42s — checks did not complete, re-dispatch (triggered by someone)"
assert_failure_description "an ordinary check failure keeps its own wording" \
  "Sign-off checks failed after 42s (triggered by someone)"

# The success path's short-circuit is the behaviour the failure path inverts:
# both directions are asserted so neither can drift into the other.
assert_attestation_refresh "an unforced refresh skips a green Attestation" \
  "4242 completed success" "" ""
assert_attestation_refresh "a forced refresh re-runs a green Attestation" \
  "4242 completed success" force 4242
assert_attestation_refresh "an unforced refresh re-runs a red Attestation" \
  "4242 completed failure" "" 4242

# Malformed and unavailable run lists must not re-run something arbitrary.
assert_attestation_refresh "a null run id is not re-run" \
  "null null null" force ""
assert_attestation_refresh "an unreadable run list is not re-run" \
  "4242 completed success" force "" 1

# There is nothing to re-run while a run is still going, and it may already have
# read the status this verdict replaced — so the forced path must say the check
# might land green rather than promise it will pick the verdict up.
assert_attestation_refresh "an in-flight Attestation is flagged, not promised, on the forced path" \
  "4242 in_progress null" force "" 0 "may have read the previous sign-off already"
assert_attestation_refresh "an in-flight Attestation is not flagged on the success path" \
  "4242 in_progress null" "" "" 0 "it will evaluate the sign-off once it finishes"

echo
echo "scripts/signoff — recording the verdict"

# The bug: post_commit_status made one unretried call, so a single transient
# response discarded a verdict that had cost hours of runner time, and the
# `pending` posted on the way in stayed on the commit as the only thing there —
# a state no reader can tell from a run still in flight.
assert_post_retry "a verdict survives a transient failure" 2 "0 0 0" 0 1
assert_post_retry "a verdict survives failures up to the last attempt" 3 "0 0 0" 0 1

# An expired token is beyond what retrying can fix. What it must not do is fail
# where only the log of a finished job can see it.
assert_post_retry "a verdict that never lands says the pending is stale" 9 "0 0 0" 1 0 \
  "Verdict not recorded"

# Which failure it was decides whether the answer is more attempts or a fresh
# token, and only the response body tells them apart.
assert_post_retry "the response body is reported, not just the failure" 9 "0 0 0" 1 0 \
  "Bad credentials"

# An empty backoff is the single call the endpoint used to get, so a caller that
# opts out is not silently retried.
assert_post_retry "an empty backoff posts exactly once" 1 "" 1 0 "attempt 1/1"

# A pending that never posts strands nothing — the commit keeps what it had and
# the run still has its verdict to post — so it must not raise the alarm that
# belongs to a lost verdict.
assert_post_retry "a lost pending does not claim a stale status" 9 "0 0 0" 1 0 \
  "" pending "Verdict not recorded"

echo
echo "scripts/signoff clear-pending"

# The bug: a run terminated mid-check leaves the `pending` it posted on the
# commit for good, so `status` and `mine` report a sign-off still in progress.
assert_clear_pending "a pending status is replaced with a failure" \
  pending failure "a canned reason"

# The run reached a verdict — that verdict is the one that counts.
assert_clear_pending "a successful sign-off is left alone" success
assert_clear_pending "a failed sign-off is left alone" failure
assert_clear_pending "an errored sign-off is left alone" error

# Safe to run unconditionally at the end of a job, including one that never
# posted anything.
assert_clear_pending "a commit with no sign-off status is left alone" none

echo
if [[ "$failures" -gt 0 ]]; then
  echo "${failures} of ${tests_run} tests failed"
  exit 1
fi
echo "all ${tests_run} tests passed"
