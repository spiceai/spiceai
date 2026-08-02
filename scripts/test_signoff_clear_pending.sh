#!/usr/bin/env bash
#
# Unit tests for `scripts/signoff clear-pending`. No network and no
# credentials: a stub `gh` on PATH serves a canned commit status and records
# every status the subject posts back.
#
# Usage: scripts/test_signoff_clear_pending.sh

set -uo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
subject="$script_dir/signoff"

tests_run=0
failures=0

fail_test() {
  failures=$((failures + 1))
  echo "  FAIL: $1"
}

# A `gh` that answers the two calls clear-pending makes: read the combined
# status for a commit, and post a new one. STUB_STATE is the `signoff` state the
# read reports ("none" for a commit that has none); every post is appended to
# STUB_POSTS as `state<TAB>context<TAB>description`.
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

# The read. clear-pending asks gh's own jq for the state, so answer with the
# value the expression would have produced rather than re-implementing it.
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

stub_dir="$(mktemp -d)" || {
  echo "could not create a temp dir for the gh stub" >&2
  exit 1
}
trap 'rm -rf "$stub_dir"' EXIT
write_gh_stub "$stub_dir"

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
