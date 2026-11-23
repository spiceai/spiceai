#!/usr/bin/env bash
# Exchange this job's GitHub Actions OIDC token for temporary credentials of the CI object
# store (RustFS), with STS AssumeRoleWithWebIdentity, and print them on stdout, one per
# line: the access key ID, the secret access key, the session token, and when they expire.
#
# The store decides what the credentials may do from the token's claims, by the repository
# the job runs in. A job has a token only with `permissions: id-token: write`, which puts
# ACTIONS_ID_TOKEN_REQUEST_URL and ACTIONS_ID_TOKEN_REQUEST_TOKEN in its environment; a pull
# request from a fork never gets one.
#
# The `rustfs-credentials` action and cached_fetch.sh both get their credentials here, so
# the audience and the request bounds are set in one place.
#
# Usage: bash .github/scripts/rustfs_credentials.sh ENDPOINT [DURATION_SECONDS [AUDIENCE]]
#   ENDPOINT          the store's S3 endpoint, which answers STS as well
#   DURATION_SECONDS  the credentials' lifetime, 900 to 43200 seconds (default 3600)
#   AUDIENCE          the OIDC token audience the store trusts (default below)
# On failure it prints nothing on stdout, one line saying why on stderr, and exits 1.
set -euo pipefail

default_audience=rustfs-dev-teraswitch

fail() {
  echo "$*" >&2
  exit 1
}

if [ "$#" -lt 1 ] || [ "$#" -gt 3 ]; then
  fail "rustfs_credentials.sh needs ENDPOINT, and optionally DURATION_SECONDS and AUDIENCE; got $# arguments."
fi
endpoint="${1%/}"
duration="${2:-3600}"
audience="${3:-$default_audience}"
if [ -z "$endpoint" ]; then
  fail "rustfs_credentials.sh needs the store's endpoint; it was empty."
fi
if ! [[ "$duration" =~ ^[0-9]+$ ]]; then
  fail "DURATION_SECONDS must be a whole number of seconds, got '$duration'."
fi
if [ -z "${ACTIONS_ID_TOKEN_REQUEST_URL:-}" ] || [ -z "${ACTIONS_ID_TOKEN_REQUEST_TOKEN:-}" ]; then
  fail "No GitHub OIDC token is available. The job needs 'permissions: id-token: write'; pull requests from forks never get one."
fi
if ! command -v jq >/dev/null 2>&1; then
  fail "jq is not installed, so GitHub's OIDC token response cannot be read."
fi

# Bearer tokens go to curl on stdin, so neither is ever on a command line.
#
# Every request is bounded. Without `--max-time`, a server that accepts the connection and
# never answers holds curl, and with it the calling job, until the job's own bound cancels
# it, which for a build is hours. `--retry` treats a timeout as transient, so a stall costs
# at most four 30-second attempts per call.
status=0
response="$(printf 'Authorization: Bearer %s\n' "$ACTIONS_ID_TOKEN_REQUEST_TOKEN" |
  curl --silent --retry 3 --connect-timeout 10 --max-time 30 -H @- --get \
    --data-urlencode "audience=$audience" --write-out '\n%{http_code}' \
    "$ACTIONS_ID_TOKEN_REQUEST_URL")" || status=$?
code="${response##*$'\n'}"
body="${response%$'\n'*}"
if [ "$status" -ne 0 ] || [ "$code" != 200 ]; then
  fail "GitHub did not issue an OIDC token (curl exit $status, HTTP $code)."
fi
oidc_token="$(jq -r '.value // empty' <<<"$body" 2>/dev/null)" || oidc_token=''
if [ -z "$oidc_token" ]; then
  fail "GitHub returned no OIDC token."
fi

status=0
response="$(printf '%s' "$oidc_token" |
  curl --silent --retry 3 --connect-timeout 10 --max-time 30 -X POST "$endpoint/" \
    -H 'Content-Type: application/x-www-form-urlencoded' \
    --data-urlencode 'Action=AssumeRoleWithWebIdentity' \
    --data-urlencode 'Version=2011-06-15' \
    --data-urlencode "DurationSeconds=$duration" \
    --data-urlencode 'WebIdentityToken@-' \
    --write-out '\n%{http_code}')" || status=$?
code="${response##*$'\n'}"
body="${response%$'\n'*}"
xml_value() { sed -n "s:.*<$1>\(.*\)</$1>.*:\1:p" <<<"$body"; }

if [ "$status" -ne 0 ]; then
  fail "Could not exchange the OIDC token with RustFS (curl exit $status, HTTP $code)."
fi
if [ "$code" != 200 ]; then
  # The store's own reason, with the token cut out should the store repeat it: nothing this
  # script prints may carry a credential, so no caller has to mask one.
  reason="$(xml_value Code): $(xml_value Message)"
  reason="${reason//"$oidc_token"/***}"
  fail "RustFS refused the OIDC token (HTTP $code): $reason"
fi

access_key="$(xml_value AccessKeyId)"
secret_key="$(xml_value SecretAccessKey)"
session_token="$(xml_value SessionToken)"
expiration="$(xml_value Expiration)"
if [ -z "$access_key" ] || [ -z "$secret_key" ] || [ -z "$session_token" ]; then
  fail "RustFS returned HTTP 200 without credentials."
fi
# Callers write these into `$GITHUB_OUTPUT` and curl configuration lines, where a newline, a
# quote or a colon would add entries or fields of its own. STS credentials are base64 or
# token characters, and the expiration an ISO 8601 time.
for value in "$access_key" "$secret_key" "$session_token"; do
  if ! [[ "$value" =~ ^[A-Za-z0-9+/=._~-]+$ ]]; then
    fail "RustFS returned credentials with characters an access key, secret or session token never has."
  fi
done
if ! [[ "$expiration" =~ ^[0-9A-Za-z:.+-]*$ ]]; then
  fail "RustFS returned an expiration that is not a time."
fi
printf '%s\n' "$access_key" "$secret_key" "$session_token" "$expiration"
