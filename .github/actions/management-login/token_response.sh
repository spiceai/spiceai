#!/usr/bin/env bash
#
# Extracts the access token from an OAuth token-endpoint response.
#
# Usage: token_response.sh <http-status> <token-url> < response-body
#
# Writes the token to stdout on success. On failure writes a GitHub Actions
# error annotation to stderr and exits 1, so a broken token exchange names its
# own cause instead of surfacing as a bare "exit code 1".
#
# The body is read from stdin rather than argv so it never lands in a process
# listing.

set -euo pipefail

http_status="${1:?usage: token_response.sh <http-status> <token-url>}"
token_url="${2:?usage: token_response.sh <http-status> <token-url>}"

body="$(cat)"

fail() {
  echo "::error::Management login failed: $1" >&2
  exit 1
}

# Only the two standard OAuth 2.0 error fields are echoed. The rest of the body
# is deliberately never printed: a non-JSON token endpoint may answer in
# form-encoded form, which carries the token itself.
if error_code="$(printf '%s' "$body" | jq -er '.error' 2>/dev/null)"; then
  error_description="$(printf '%s' "$body" | jq -r '.error_description // ""' 2>/dev/null)"
  detail="$error_code"
  if [ -n "$error_description" ]; then
    detail="$error_code: $error_description"
  fi
  fail "$token_url returned HTTP $http_status ($detail). Check the client id and secret passed to this action."
fi

if ! token="$(printf '%s' "$body" | jq -er '.access_token // empty' 2>/dev/null)"; then
  if ! printf '%s' "$body" | jq -e . >/dev/null 2>&1; then
    fail "$token_url returned HTTP $http_status with a non-JSON body (${#body} chars). The body is not logged because it may contain a token."
  fi
  fail "$token_url returned HTTP $http_status with no 'access_token' field."
fi

# OAuth 2.0 specifies 'access_token' as a string. `jq -r` renders a number,
# array or object as text just the same, so without this the action would
# export something that is not a credential and the failure would only surface
# later as an opaque 401.
token_type="$(printf '%s' "$body" | jq -r '.access_token | type' 2>/dev/null || echo unknown)"
if [ "$token_type" != "string" ]; then
  fail "$token_url returned an 'access_token' of JSON type $token_type, not a string."
fi

# An empty token must never be exported: the management API tests skip
# themselves when the token variable is unset, so exporting one would turn this
# into a green run with no tests.
if [ -z "$token" ]; then
  fail "$token_url returned HTTP $http_status with an empty 'access_token'."
fi

# A value spanning lines would inject arbitrary variables when appended to
# $GITHUB_ENV as NAME=value.
case "$token" in
  *$'\n'*) fail "$token_url returned an 'access_token' spanning multiple lines." ;;
esac

printf '%s\n' "$token"
