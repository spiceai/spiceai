#!/usr/bin/env bash
# Exchanges the job's GitHub OIDC token for a Databricks token and prints it. The job needs
# `id-token: write`, and DATABRICKS_HOST, DATABRICKS_OIDC_CLIENT_ID and DATABRICKS_OIDC_AUDIENCE.
set -euo pipefail

oidc_token="$(curl --silent --show-error --fail --max-time 30 --retry 3 --retry-all-errors --get \
  --data-urlencode "audience=${DATABRICKS_OIDC_AUDIENCE}" \
  --header "Authorization: bearer ${ACTIONS_ID_TOKEN_REQUEST_TOKEN}" \
  "${ACTIONS_ID_TOKEN_REQUEST_URL}" | jq -er .value)"
echo "::add-mask::${oidc_token}" >&2

token="$(curl --silent --show-error --fail --max-time 30 --retry 3 --retry-all-errors \
  "https://${DATABRICKS_HOST}/oidc/v1/token" \
  --data-urlencode "grant_type=urn:ietf:params:oauth:grant-type:token-exchange" \
  --data-urlencode "subject_token_type=urn:ietf:params:oauth:token-type:jwt" \
  --data-urlencode "subject_token=${oidc_token}" \
  --data-urlencode "client_id=${DATABRICKS_OIDC_CLIENT_ID}" \
  --data-urlencode "scope=all-apis" | jq -er .access_token)"
echo "::add-mask::${token}" >&2
printf '%s' "${token}"
