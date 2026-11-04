#!/usr/bin/env bash
# Writes the BigQuery tests' workload identity federation credential. See action.yml.
#
# The credential holds no key: it tells Google's client libraries to fetch this job's GitHub OIDC
# token, exchange it at Google's Security Token Service, and act as the service account. Before
# exporting it, this script makes the same exchange once, so a job Google refuses fails here with
# Google's reason rather than later inside a test. Bearer tokens reach curl only through stdin or
# a process substitution, never a command line.
set -euo pipefail

fail() {
  echo "::error::$*"
  exit 1
}

if [[ -z "${WIF_PROVIDER:-}" || -z "${SERVICE_ACCOUNT:-}" ]]; then
  fail "BigQuery credentials are not configured. Set the BIGQUERY_WIF_PROVIDER and BIGQUERY_SERVICE_ACCOUNT secrets in the job's environment (ci)."
fi
[[ "${TOKEN_LIFETIME_SECONDS}" =~ ^[0-9]+$ ]] && ((TOKEN_LIFETIME_SECONDS >= 600 && TOKEN_LIFETIME_SECONDS <= 3600)) ||
  fail "token-lifetime-seconds must be from 600 to 3600, got '${TOKEN_LIFETIME_SECONDS}'."
if [[ -z "${ACTIONS_ID_TOKEN_REQUEST_URL:-}" || -z "${ACTIONS_ID_TOKEN_REQUEST_TOKEN:-}" ]]; then
  fail "No GitHub OIDC token is available. The job needs 'permissions: id-token: write' and 'environment: ci'; pull requests from forks never get one."
fi
echo "::add-mask::${ACTIONS_ID_TOKEN_REQUEST_TOKEN}"

oidc_audience="https://iam.googleapis.com/${WIF_PROVIDER}"
sts_audience="//iam.googleapis.com/${WIF_PROVIDER}"
impersonation_url="https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/${SERVICE_ACCOUNT}:generateAccessToken"

# Google APIs report `{"error": {"message": …}}`; STS reports `{"error": "…", "error_description": "…"}`.
# The trailing period is dropped because the message is quoted mid-sentence.
google_error() {
  jq -r '(if (.error | type) == "object" then .error.message else (.error_description // .error) end // "no error message") | sub("\\.$"; "")' \
    <<<"$1" 2>/dev/null || printf '%s' "$1"
}

# Sets `status` and `body` from a curl response written with `-w '\n%{http_code}'`.
split_response() {
  status="${1##*$'\n'}"
  body="${1%$'\n'*}"
}

# 1. The job's GitHub OIDC token, issued for the provider.
oidc_token="$(printf 'Authorization: Bearer %s\n' "${ACTIONS_ID_TOKEN_REQUEST_TOKEN}" |
  curl -sSf --retry 3 -H @- --get --data-urlencode "audience=${oidc_audience}" \
    "${ACTIONS_ID_TOKEN_REQUEST_URL}" | jq -r '.value // empty')"
[[ -n "${oidc_token}" ]] || fail "GitHub returned no OIDC token."
echo "::add-mask::${oidc_token}"

# 2. Google's Security Token Service checks the token against the provider's condition.
split_response "$(jq -n --arg audience "${sts_audience}" --arg subject "${oidc_token}" '{
    grant_type: "urn:ietf:params:oauth:grant-type:token-exchange",
    audience: $audience,
    scope: "https://www.googleapis.com/auth/cloud-platform",
    requested_token_type: "urn:ietf:params:oauth:token-type:access_token",
    subject_token_type: "urn:ietf:params:oauth:token-type:jwt",
    subject_token: $subject
  }' | curl -sS --retry 3 -X POST -H 'Content-Type: application/json' --data-binary @- \
    -w '\n%{http_code}' https://sts.googleapis.com/v1/token)"
[[ "${status}" == 200 ]] || fail "Google Cloud refused the job's GitHub OIDC token (HTTP ${status}): $(google_error "${body}"). The provider accepts only jobs in the ci environment of spiceai/spiceai and spicehq/spiceai."
federated_token="$(jq -r '.access_token // empty' <<<"${body}")"
[[ -n "${federated_token}" ]] || fail "Google Cloud STS returned HTTP 200 without a token."
echo "::add-mask::${federated_token}"

# 3. Acting as the service account, with the lifetime the tests will get.
split_response "$(jq -n --arg lifetime "${TOKEN_LIFETIME_SECONDS}s" \
  '{scope: ["https://www.googleapis.com/auth/cloud-platform"], lifetime: $lifetime}' |
  curl -sS --retry 3 -X POST -H @<(printf 'Authorization: Bearer %s\n' "${federated_token}") \
    -H 'Content-Type: application/json' --data-binary @- -w '\n%{http_code}' "${impersonation_url}")"
[[ "${status}" == 200 ]] || fail "Could not act as ${SERVICE_ACCOUNT} (HTTP ${status}): $(google_error "${body}")"
echo "::add-mask::$(jq -r '.accessToken // empty' <<<"${body}")"
echo "Google Cloud accepted the job; access tokens for ${SERVICE_ACCOUNT} last ${TOKEN_LIFETIME_SECONDS} s."

# The credential the driver and the tests use: the same three steps, repeated by Google's client
# libraries before each token expires, for as long as the job runs.
credential="$(jq -cn \
  --arg audience "${sts_audience}" \
  --arg impersonation_url "${impersonation_url}" \
  --argjson lifetime "${TOKEN_LIFETIME_SECONDS}" \
  --arg source_url "${ACTIONS_ID_TOKEN_REQUEST_URL}&audience=$(jq -rn --arg a "${oidc_audience}" '$a | @uri')" \
  --arg request_token "${ACTIONS_ID_TOKEN_REQUEST_TOKEN}" '{
    type: "external_account",
    audience: $audience,
    subject_token_type: "urn:ietf:params:oauth:token-type:jwt",
    token_url: "https://sts.googleapis.com/v1/token",
    service_account_impersonation_url: $impersonation_url,
    service_account_impersonation: {token_lifetime_seconds: $lifetime},
    credential_source: {
      url: $source_url,
      headers: {Authorization: ("Bearer " + $request_token)},
      format: {type: "json", subject_token_field_name: "value"}
    }
  }')"
echo "::add-mask::${credential}"
echo "BIGQUERY_SERVICE_ACCOUNT_JSON=${credential}" >>"${GITHUB_ENV}"
