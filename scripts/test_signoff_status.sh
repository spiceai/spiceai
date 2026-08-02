#!/usr/bin/env bash
#
# Unit tests for the `signoff` commit status: which states `scripts/signoff` may
# post over which, in both directions — the in-progress `pending` a remote run
# posts on the way in, and the `clear-pending` that resolves one left behind. No
# network and no credentials: a stub `gh` on PATH serves a canned commit status
# and records every status the subject posts back.
#
# Usage: scripts/test_signoff_status.sh

set -uo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
subject="$script_dir/signoff"

tests_run=0
failures=0

fail_test() {
  failures=$((failures + 1))
  echo "  FAIL: $1"
}

# A `gh` that answers the two calls the subject makes: read the combined status
# for a commit, and post a new one. STUB_STATE is the `signoff` state the read
# reports ("none" for a commit that has none); STUB_READ_RC makes the read fail;
# every post is appended to STUB_POSTS as `state<TAB>context<TAB>description`.
write_gh_stub() {
  local dir="$1"
  cat >"$dir/gh" <<'STUB'
#!/usr/bin/env bash
set -uo pipefail

case "${1:-}" in
  auth) exit 0 ;;
  repo) echo "spiceai/spiceai"; exit 0 ;;
esac

if [[ "${1:-}" != "api" ]]; then
  echo "stub gh: unexpected command: $*" >&2
  exit 64
fi

if [[ "$*" == *"--method POST"* ]]; then
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
