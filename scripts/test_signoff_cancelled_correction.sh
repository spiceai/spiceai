#!/usr/bin/env bash
#
# Unit tests for `scripts/signoff correct-cancelled`: the repair a cancelled
# Remote Sign-off run applies to the `signoff` commit status it left behind.
#
# Both directions are load-bearing. A cancelled job is killed by signal, so the
# dying `run_checks` posts `signoff=failure` and Attestation reports a code
# failure that never happened (#12424) — so the correction has to fire. But it
# must fire *only* on that: overwriting a `success` throws away checks that
# genuinely passed and forces a 1-4 hour re-dispatch (#12428). A run can find a
# legitimate success on the commit either because its own `Sign off` step
# completed just before the cancel signal landed, or because a second run —
# `-f branch=x` and `-f pr_number=N` are different concurrency groups, so they
# do not cancel each other — already signed the same SHA off.
#
# No network and no credentials: a stub `gh` on PATH serves whatever combined
# status a case needs and records any status POST to a file.
#
# Usage: scripts/test_signoff_cancelled_correction.sh

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

posted="$stub_dir/posted"

# A `gh` that answers the two calls this command makes and nothing else.
#
#   api repos/<repo>/commits/<sha>/status  -> STUB_COMBINED, or exit STUB_GH_READ_RC
#   api --method POST repos/<repo>/statuses/<sha> ... -> appended to STUB_POSTED
#
# Any other invocation is a hard error: the point of several cases is that *no*
# write happens, and a stub that silently accepted an unexpected call would let
# a regression pass as a success.
cat >"$stub_dir/gh" <<'STUB'
#!/usr/bin/env bash
set -uo pipefail

if [[ "${1:-}" == "api" && "${2:-}" == "--method" && "${3:-}" == "POST" ]]; then
  printf '%s\n' "$*" >>"${STUB_POSTED}"
  exit "${STUB_GH_POST_RC:-0}"
fi

if [[ "${1:-}" == "api" && "${2:-}" == */commits/*/status ]]; then
  [[ "${STUB_GH_READ_RC:-0}" == "0" ]] || exit "${STUB_GH_READ_RC}"
  printf '%s' "${STUB_COMBINED:-\{\}}"
  exit 0
fi

echo "stub gh: unexpected invocation: $*" >&2
exit 99
STUB
chmod +x "$stub_dir/gh"

# A combined-status payload whose `signoff` entry has state $1, wrapped in the
# neighbours a real commit carries. `Attestation` is deliberately given the
# opposite verdict in every case: the command must key on the context, not on
# "some status on this commit failed".
combined_with_signoff() {
  local state="$1"
  cat <<JSON
{
  "state": "pending",
  "statuses": [
    { "context": "enforce-pull-with-spice", "state": "success", "description": "ok" },
    { "context": "signoff", "state": "${state}", "description": "from the test" },
    { "context": "Attestation", "state": "failure", "description": "not signed off" }
  ]
}
JSON
}

# Runs `correct-cancelled` against the stub gh. Echoes "<rc>|<output>".
# `git rev-parse HEAD` is never reached: every case passes an explicit SHA, so
# the test does not depend on the checkout it runs in.
run_correction() {
  local sha="$1" repo="$2"
  shift 2
  local output rc
  # `env` rather than an assignment prefix: these come from "$@", and words that
  # only look like assignments after expansion are read as the command name.
  output="$(env "PATH=$stub_dir:$PATH" "STUB_POSTED=$posted" "$@" \
    bash "$subject" correct-cancelled "$sha" "$repo" 2>&1)"
  rc=$?
  printf '%s|%s' "$rc" "$output"
}

# $2: substring the output must contain. $3: 'post' if a status write is
# expected, 'nopost' if the command must leave the status untouched.
assert_correction() {
  local name="$1" want_output="$2" want_post="$3"
  shift 3
  tests_run=$((tests_run + 1))

  : >"$posted"

  local result rc output
  result="$(run_correction "deadbeefcafe1234" "spiceai/spiceai" "$@")"
  rc="${result%%|*}"
  output="${result#*|}"

  # The command runs while a cancelled job is being torn down. Whatever it finds,
  # it must not add a second failure to the run — every path exits 0.
  if [[ "$rc" -ne 0 ]]; then
    fail_test "$name: expected exit 0 (must never mask the cancellation), got ${rc} (output: ${output})"
    return
  fi

  if [[ "$output" != *"$want_output"* ]]; then
    fail_test "$name: expected '${want_output}' in the output, got '${output}'"
    return
  fi

  local post_count
  post_count=$(grep -c 'POST' "$posted" 2>/dev/null || true)
  post_count="${post_count:-0}"

  case "$want_post" in
    post)
      if [[ "$post_count" -ne 1 ]]; then
        fail_test "$name: expected exactly 1 status POST, got ${post_count}"
        return
      fi
      if ! grep -q 'state=pending' "$posted"; then
        fail_test "$name: expected the POST to set state=pending, got '$(cat "$posted")'"
        return
      fi
      if ! grep -q 'context=signoff' "$posted"; then
        fail_test "$name: expected the POST to target context=signoff, got '$(cat "$posted")'"
        return
      fi
      ;;
    nopost)
      if [[ "$post_count" -ne 0 ]]; then
        fail_test "$name: expected NO status POST, got ${post_count}: '$(cat "$posted")'"
        return
      fi
      ;;
    *)
      fail_test "$name: bad want_post '${want_post}'"
      return
      ;;
  esac

  echo "  ok: $name"
}

echo "Testing the cancelled-run sign-off correction in ${subject}"
echo

echo "The bogus verdict a dying run posts — the case the correction exists for (#12424):"
assert_correction "failure is rewritten to pending" \
  "Reset signoff=failure" post \
  STUB_COMBINED="$(combined_with_signoff failure)"
assert_correction "error is rewritten to pending" \
  "Reset signoff=error" post \
  STUB_COMBINED="$(combined_with_signoff error)"
echo

echo "A sign-off that genuinely completed — must survive (#12428):"
# The regression this whole file exists for: the run's own Sign off step posted
# success, then the cancel signal landed before the correction ran.
assert_correction "success is left alone" \
  "is 'success', not a failure this run posted" nopost \
  STUB_COMBINED="$(combined_with_signoff success)"
# The cross-run case: another run (different concurrency group) signed this SHA
# off, and this run is cancelled afterwards.
assert_correction "success posted by another run is left alone" \
  "leaving it alone" nopost \
  STUB_COMBINED="$(combined_with_signoff success)"
echo

echo "Every other state is somebody else's to write:"
assert_correction "pending is left alone" \
  "is 'pending', not a failure this run posted" nopost \
  STUB_COMBINED="$(combined_with_signoff pending)"
assert_correction "unset signoff context is left alone" \
  "is 'unset', not a failure this run posted" nopost \
  STUB_COMBINED='{"state":"pending","statuses":[{"context":"Attestation","state":"failure"}]}'
assert_correction "no statuses at all is left alone" \
  "is 'unset'" nopost \
  STUB_COMBINED='{"state":"pending","statuses":[]}'
# Keys on the context, not on "something on this commit is red": every payload
# above carries a failing `Attestation`, and this one carries nothing else.
assert_correction "a failure on a different context does not trigger a rewrite" \
  "is 'unset'" nopost \
  STUB_COMBINED='{"state":"failure","statuses":[{"context":"Rust Lint","state":"failure"},{"context":"Build and Test","state":"error"}]}'
echo

echo "Degraded environments — never write over a status that could not be read:"
# The distinction that makes this safe: "could not read" is not "unset". Reading
# a failure as absence would license exactly the overwrite being fixed.
assert_correction "an unreadable status is left alone, not treated as unset" \
  "could not read" nopost \
  STUB_GH_READ_RC=1
assert_correction "malformed JSON is not treated as a failure to rewrite" \
  "not a failure this run posted" nopost \
  STUB_COMBINED='not json at all'
echo

echo "A failed write is reported, not escalated:"
tests_run=$((tests_run + 1))
: >"$posted"
result="$(run_correction "deadbeefcafe1234" "spiceai/spiceai" \
  STUB_COMBINED="$(combined_with_signoff failure)" STUB_GH_POST_RC=1)"
rc="${result%%|*}"
output="${result#*|}"
if [[ "$rc" -ne 0 ]]; then
  fail_test "a failed POST still exits 0: got ${rc} (output: ${output})"
elif [[ "$output" != *"could not reset"* ]]; then
  fail_test "a failed POST says so: expected 'could not reset', got '${output}'"
else
  echo "  ok: a failed POST is reported and still exits 0"
fi
echo

echo "Missing tools are a no-op, not a crash:"
for tool in gh jq; do
  tests_run=$((tests_run + 1))
  # A PATH holding only the *other* tool, so the guard for this one is what
  # decides. bash itself is invoked by absolute path.
  #
  # The tool that stays is a placeholder, not a symlink to the host's copy: the
  # host is not guaranteed to have both, and symlinking what it happens to have
  # made each case depend on the *other* tool being installed — a box with `gh`
  # but no `jq` would leave the jq-missing case with an empty PATH and report
  # "gh unavailable", failing an assertion about a guard that worked correctly.
  # A placeholder is enough because the subject exits at the missing guard
  # without ever invoking the surviving tool; it fails loudly if that ever stops
  # being true rather than passing on a path this case does not mean to test.
  empty_dir="$stub_dir/without-$tool"
  mkdir -p "$empty_dir"
  for keep in gh jq; do
    [[ "$keep" == "$tool" ]] && continue
    cat >"$empty_dir/$keep" <<PLACEHOLDER
#!/usr/bin/env bash
echo "placeholder ${keep}: should never be invoked — the missing-${tool} guard runs first" >&2
exit 97
PLACEHOLDER
    chmod +x "$empty_dir/$keep"
  done
  : >"$posted"
  output="$(env "PATH=$empty_dir" "STUB_POSTED=$posted" \
    "$(command -v bash)" "$subject" correct-cancelled "deadbeefcafe1234" "spiceai/spiceai" 2>&1)"
  rc=$?
  if [[ "$rc" -ne 0 ]]; then
    fail_test "missing ${tool} exits 0: got ${rc} (output: ${output})"
  elif [[ "$output" != *"${tool} unavailable"* ]]; then
    fail_test "missing ${tool} says so: expected '${tool} unavailable', got '${output}'"
  else
    echo "  ok: a missing ${tool} is a no-op"
  fi
done
echo

echo "The repo comes from the environment when the caller does not pass one:"
# The workflow passes github.repository explicitly, but GITHUB_REPOSITORY is what
# a bare invocation inside Actions has to fall back to.
tests_run=$((tests_run + 1))
: >"$posted"
output="$(env "PATH=$stub_dir:$PATH" "STUB_POSTED=$posted" \
  STUB_COMBINED="$(combined_with_signoff failure)" GITHUB_REPOSITORY="spiceai/spiceai" \
  bash "$subject" correct-cancelled "deadbeefcafe1234" 2>&1)"
rc=$?
if [[ "$rc" -ne 0 ]]; then
  fail_test "GITHUB_REPOSITORY fallback exits 0: got ${rc} (output: ${output})"
elif ! grep -q 'repos/spiceai/spiceai/statuses/deadbeefcafe1234' "$posted"; then
  fail_test "GITHUB_REPOSITORY fallback targets the right repo: got '$(cat "$posted")'"
else
  echo "  ok: the repo falls back to GITHUB_REPOSITORY"
fi
echo

echo "The status reader agrees with every other reader in the script:"
# cmd_status, cmd_mine and the correction all pick the current state through
# combined_status_state, so they cannot disagree about what the commit says.
assert_reader() {
  local name="$1" payload="$2" want="$3"
  tests_run=$((tests_run + 1))
  local got
  got="$(bash -c 'source "$1"; combined_status_state "$2"' _ "$subject" "$payload" 2>&1)"
  if [[ "$got" != "$want" ]]; then
    fail_test "$name: expected '${want}', got '${got}'"
  else
    echo "  ok: $name"
  fi
}
assert_reader "picks the signoff entry out of many" "$(combined_with_signoff success)" "success"
assert_reader "empty when the context is absent" \
  '{"statuses":[{"context":"Attestation","state":"failure"}]}' ""
assert_reader "empty when there are no statuses" '{"statuses":[]}' ""
# The combined endpoint collapses each context to its latest status, so the
# first matching entry is the current one — assert that ordering explicitly.
assert_reader "takes the first matching entry" \
  '{"statuses":[{"context":"signoff","state":"success"},{"context":"signoff","state":"failure"}]}' \
  "success"
echo

echo "----------------------------------------"
if [[ "$failures" -eq 0 ]]; then
  echo "PASS: ${tests_run} assertions"
  exit 0
fi
echo "FAIL: ${failures} of ${tests_run} assertions failed"
exit 1
