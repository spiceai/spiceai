#!/usr/bin/env bash
# Creates (`create`) or deletes (`delete`) the job's BigQuery service account key. See action.yml.
#
# Every bearer token and the key itself reach curl through stdin or a process substitution, never
# a command line, and are masked before anything could print them.
set -euo pipefail

key_file=""
remove_key_file() {
  if [[ -n "${key_file}" ]]; then rm -f -- "${key_file}"; fi
}
trap remove_key_file EXIT

fail() {
  echo "::error::$*"
  exit 1
}

mode="${1:-}"
[[ "${mode}" == create || "${mode}" == delete ]] || fail "bigquery-service-account-key: mode must be 'create' or 'delete', got '${mode}'."

if [[ -z "${WIF_PROVIDER:-}" || -z "${MINTER_SERVICE_ACCOUNT:-}" || -z "${SERVICE_ACCOUNT:-}" ]]; then
  fail "BigQuery credentials are not configured. Set the BIGQUERY_WIF_PROVIDER, BIGQUERY_KEY_MINTER_SERVICE_ACCOUNT and BIGQUERY_SERVICE_ACCOUNT secrets in the job's environment (ci)."
fi

# Sends a JSON request; sets `status` and `body`. The bearer token goes in a process substitution.
request() {
  local method="$1" url="$2" token="$3" data="${4:-}"
  local response
  response="$(printf '%s' "${data}" | curl -sS --retry 3 -X "${method}" \
    -H @<(printf 'Authorization: Bearer %s\n' "${token}") \
    -H 'Content-Type: application/json' \
    ${data:+--data-binary @-} \
    -w '\n%{http_code}' "${url}")"
  status="${response##*$'\n'}"
  body="${response%$'\n'*}"
}

# Google APIs report `{"error": {"message": …}}`; STS reports `{"error": "…", "error_description": "…"}`.
google_error() {
  jq -r 'if (.error | type) == "object" then .error.message else (.error_description // .error) end // "no error message"' \
    <<<"${body}" 2>/dev/null || printf '%s' "${body}"
}

# Exchanges the job's GitHub OIDC token for a short-lived token of the minter service account; sets `token`.
sign_in() {
  if [[ -z "${ACTIONS_ID_TOKEN_REQUEST_URL:-}" || -z "${ACTIONS_ID_TOKEN_REQUEST_TOKEN:-}" ]]; then
    fail "No GitHub OIDC token is available. The job needs 'permissions: id-token: write' and 'environment: ci'; pull requests from forks never get one."
  fi

  local oidc_token federated_token
  oidc_token="$(printf 'Authorization: Bearer %s\n' "${ACTIONS_ID_TOKEN_REQUEST_TOKEN}" |
    curl -sSf --retry 3 -H @- --get --data-urlencode "audience=https://iam.googleapis.com/${WIF_PROVIDER}" \
      "${ACTIONS_ID_TOKEN_REQUEST_URL}" | jq -r '.value // empty')"
  [[ -n "${oidc_token}" ]] || fail "GitHub returned no OIDC token."
  echo "::add-mask::${oidc_token}"

  local response
  response="$(jq -n --arg audience "//iam.googleapis.com/${WIF_PROVIDER}" --arg subject "${oidc_token}" '{
      grant_type: "urn:ietf:params:oauth:grant-type:token-exchange",
      audience: $audience,
      scope: "https://www.googleapis.com/auth/cloud-platform",
      requested_token_type: "urn:ietf:params:oauth:token-type:access_token",
      subject_token_type: "urn:ietf:params:oauth:token-type:jwt",
      subject_token: $subject
    }' | curl -sS --retry 3 -X POST -H 'Content-Type: application/json' --data-binary @- \
      -w '\n%{http_code}' https://sts.googleapis.com/v1/token)"
  status="${response##*$'\n'}"
  body="${response%$'\n'*}"
  [[ "${status}" == 200 ]] || fail "Google Cloud refused the job's GitHub OIDC token (HTTP ${status}): $(google_error). The provider accepts only jobs in the ci environment of spiceai/spiceai and spicehq/spiceai."
  federated_token="$(jq -r '.access_token // empty' <<<"${body}")"
  [[ -n "${federated_token}" ]] || fail "Google Cloud STS returned HTTP 200 without a token."
  echo "::add-mask::${federated_token}"

  request POST "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/${MINTER_SERVICE_ACCOUNT}:generateAccessToken" \
    "${federated_token}" '{"scope":["https://www.googleapis.com/auth/cloud-platform"],"lifetime":"600s"}'
  [[ "${status}" == 200 ]] || fail "Could not act as ${MINTER_SERVICE_ACCOUNT} (HTTP ${status}): $(google_error)"
  token="$(jq -r '.accessToken // empty' <<<"${body}")"
  [[ -n "${token}" ]] || fail "Google Cloud returned HTTP 200 without an access token for ${MINTER_SERVICE_ACCOUNT}."
  echo "::add-mask::${token}"
}

base64url() {
  openssl base64 -A | tr '+/' '-_' | tr -d '='
}

# A new key can take a minute before Google accepts it. Signs a token request with the key until
# Google issues a token, so the tests never start with a key that is not usable yet.
wait_until_usable() {
  local key_json="$1" email key_id attempt now header claims signature jwt code
  key_file="$(mktemp "${RUNNER_TEMP:-/tmp}/bigquery-key.XXXXXX")"
  jq -r '.private_key' <<<"${key_json}" >"${key_file}"
  email="$(jq -r '.client_email' <<<"${key_json}")"
  key_id="$(jq -r '.private_key_id' <<<"${key_json}")"

  for attempt in $(seq 1 30); do
    now="$(date +%s)"
    header="$(jq -cn --arg kid "${key_id}" '{alg: "RS256", typ: "JWT", kid: $kid}' | base64url)"
    claims="$(jq -cn --arg iss "${email}" --argjson iat "${now}" '{
        iss: $iss, scope: "https://www.googleapis.com/auth/bigquery",
        aud: "https://oauth2.googleapis.com/token", iat: $iat, exp: ($iat + 300)
      }' | base64url)"
    signature="$(printf '%s.%s' "${header}" "${claims}" | openssl dgst -sha256 -sign "${key_file}" -binary | base64url)"
    jwt="${header}.${claims}.${signature}"
    code="$(printf '%s' "${jwt}" | curl -sS -o /dev/null -w '%{http_code}' \
      --data-urlencode 'grant_type=urn:ietf:params:oauth:grant-type:jwt-bearer' \
      --data-urlencode 'assertion@-' https://oauth2.googleapis.com/token)"
    if [[ "${code}" == 200 ]]; then
      echo "Google accepted the key after ${attempt} attempt(s)."
      return 0
    fi
    sleep 5
  done
  fail "Google did not accept the new key ${key_id} within 150 s (last token request: HTTP ${code})."
}

create() {
  sign_in
  request POST "https://iam.googleapis.com/v1/projects/-/serviceAccounts/${SERVICE_ACCOUNT}/keys" "${token}" '{}'
  [[ "${status}" == 200 ]] || fail "Could not create a key for ${SERVICE_ACCOUNT} (HTTP ${status}): $(google_error)"

  local key_name key_json line
  key_name="$(jq -r '.name // empty' <<<"${body}")"
  key_json="$(jq -r '.privateKeyData // empty' <<<"${body}" | base64 -d | jq -c .)"
  [[ -n "${key_name}" && -n "${key_json}" ]] || fail "Google Cloud returned HTTP 200 without a key for ${SERVICE_ACCOUNT}."
  echo "::add-mask::${key_json}"
  echo "::add-mask::$(jq -r '.private_key | tojson | .[1:-1]' <<<"${key_json}")"
  while IFS= read -r line; do
    [[ -n "${line}" && "${line}" != -----* ]] && echo "::add-mask::${line}"
  done < <(jq -r '.private_key' <<<"${key_json}")

  # The name goes out first, so the delete step can remove the key even if the wait below fails.
  echo "BIGQUERY_SERVICE_ACCOUNT_KEY_NAME=${key_name}" >>"${GITHUB_ENV}"
  echo "Created key ${key_name##*/} for ${SERVICE_ACCOUNT}."

  wait_until_usable "${key_json}"
  echo "BIGQUERY_SERVICE_ACCOUNT_JSON=${key_json}" >>"${GITHUB_ENV}"
}

delete() {
  local key_name="${BIGQUERY_SERVICE_ACCOUNT_KEY_NAME:-}"
  if [[ -z "${key_name}" ]]; then
    echo "This job created no BigQuery service account key; nothing to delete."
    return 0
  fi
  [[ "${key_name}" =~ ^projects/[^/]+/serviceAccounts/[^/]+/keys/[0-9a-f]+$ ]] || fail "Refusing to delete '${key_name}': not a service account key name."

  sign_in
  request DELETE "https://iam.googleapis.com/v1/${key_name}" "${token}"
  case "${status}" in
    200) echo "Deleted key ${key_name##*/}." ;;
    404) echo "Key ${key_name##*/} was already deleted." ;;
    *) fail "Could not delete key ${key_name##*/} of ${SERVICE_ACCOUNT} (HTTP ${status}): $(google_error). Delete it with: gcloud iam service-accounts keys delete ${key_name##*/} --iam-account=${SERVICE_ACCOUNT}" ;;
  esac
}

"${mode}"
