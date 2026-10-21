#!/usr/bin/env bash
#
# Unit tests for token_response.sh. No network and no credentials: every case
# feeds a canned token-endpoint response through the real script.
#
# Usage: test_token_response.sh

set -uo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
subject="$script_dir/token_response.sh"

tests_run=0
failures=0

fail_test() {
  failures=$((failures + 1))
  echo "  FAIL: $1"
}

# Asserts the script accepts the response and prints exactly the given token.
assert_token() {
  local name="$1" status="$2" body="$3" want="$4"
  tests_run=$((tests_run + 1))
  local got rc
  got="$(printf '%s' "$body" | "$subject" "$status" https://token.example 2>/dev/null)"
  rc=$?
  if [ "$rc" -ne 0 ]; then
    fail_test "$name: expected success, got exit $rc"
    return
  fi
  if [ "$got" != "$want" ]; then
    fail_test "$name: expected token '$want', got '$got'"
    return
  fi
  echo "  ok: $name"
}

# Asserts the script rejects the response, and that its annotation contains
# every given substring (so a failure is actually diagnosable).
assert_rejected() {
  local name="$1" status="$2" body="$3"
  shift 3
  tests_run=$((tests_run + 1))
  local stderr rc
  stderr="$(printf '%s' "$body" | "$subject" "$status" https://token.example 2>&1 >/dev/null)"
  rc=$?
  if [ "$rc" -eq 0 ]; then
    fail_test "$name: expected a non-zero exit"
    return
  fi
  local want
  for want in "$@"; do
    case "$stderr" in
      *"$want"*) ;;
      *)
        fail_test "$name: annotation is missing '$want' (was: $stderr)"
        return
        ;;
    esac
  done
  case "$stderr" in
    *"::error::"*) ;;
    *)
      fail_test "$name: annotation is not a GitHub error annotation (was: $stderr)"
      return
      ;;
  esac
  echo "  ok: $name"
}

# Asserts the script rejects the response without echoing the given text.
assert_body_not_logged() {
  local name="$1" status="$2" body="$3" secret="$4"
  tests_run=$((tests_run + 1))
  local stderr
  stderr="$(printf '%s' "$body" | "$subject" "$status" https://token.example 2>&1 >/dev/null)"
  case "$stderr" in
    *"$secret"*)
      fail_test "$name: the response body leaked into the annotation"
      return
      ;;
  esac
  echo "  ok: $name"
}

echo "token_response.sh"

assert_token "a successful exchange yields the token" \
  200 '{"access_token":"tok-abc123","token_type":"Bearer","expires_in":3600}' 'tok-abc123'

assert_token "extra unknown fields are ignored" \
  200 '{"scope":"management","access_token":"tok-xyz"}' 'tok-xyz'

assert_token "a token with punctuation survives verbatim" \
  200 '{"access_token":"eyJ0.eyJz-_9=.sig+/"}' 'eyJ0.eyJz-_9=.sig+/'

# The failure that had been showing up as a bare "exit code 1": the endpoint
# says exactly what is wrong and the old code discarded it.
assert_rejected "an OAuth error reports the code and description" \
  401 '{"error":"invalid_client","error_description":"client authentication failed"}' \
  '401' 'invalid_client' 'client authentication failed'

assert_rejected "an OAuth error without a description still reports the code" \
  400 '{"error":"unsupported_grant_type"}' \
  '400' 'unsupported_grant_type'

assert_rejected "a 200 with no access_token is reported" \
  200 '{"token_type":"Bearer"}' \
  '200' 'access_token'

assert_rejected "a null access_token is reported" \
  200 '{"access_token":null}' \
  'access_token'

# An empty token would be exported and then make cloud_integration.rs skip every
# test, reporting green with no coverage.
assert_rejected "an empty access_token is reported" \
  200 '{"access_token":""}' \
  'access_token'

# A multi-line value appended to $GITHUB_ENV as NAME=value would inject
# arbitrary variables into the remaining steps.
assert_rejected "a multi-line access_token is refused" \
  200 '{"access_token":"tok-abc\ninjected=1"}' \
  'multiple lines'

# `jq -r` renders any JSON scalar or container as text, so a non-string
# access_token would otherwise be exported as though it were a credential.
assert_rejected "a numeric access_token is refused" \
  200 '{"access_token":12345}' \
  'number' 'not a string'

assert_rejected "an object access_token is refused" \
  200 '{"access_token":{"jwt":"tok-abc"}}' \
  'object' 'not a string'

assert_rejected "an array access_token is refused" \
  200 '{"access_token":["tok-abc"]}' \
  'array' 'not a string'

assert_rejected "a non-JSON body reports the status and its size" \
  502 '<html><body>Bad Gateway</body></html>' \
  '502' 'non-JSON' 'chars'

assert_rejected "an empty body reports the status" \
  500 '' \
  '500'

assert_body_not_logged "a form-encoded body is never echoed" \
  200 'access_token=tok-leaked&token_type=Bearer' 'tok-leaked'

assert_body_not_logged "an HTML error page is never echoed" \
  502 '<html><body>Bad Gateway</body></html>' 'Bad Gateway'

echo
if [ "$failures" -ne 0 ]; then
  echo "FAILED: $failures of $tests_run checks"
  exit 1
fi
echo "OK: $tests_run checks"
