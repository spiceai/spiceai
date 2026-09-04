#!/usr/bin/env bash
# Fetch IBM/substrait-compliance at the pinned tag into a local directory.
# Default destination is .data/substrait-compliance (gitignored via .data/).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
# shellcheck source=/dev/null
TAG="$(awk -F '"' '/^ibm_substrait_compliance_tag / { print $2; exit }' "${ROOT}/tools/substrait-compliance/pin.toml")"
DEST="${1:-${ROOT}/.data/substrait-compliance}"

if [[ -z "${TAG}" ]]; then
  echo "failed to read ibm_substrait_compliance_tag from pin.toml" >&2
  exit 1
fi

if [[ -d "${DEST}/.git" ]]; then
  git -C "${DEST}" fetch --tags --depth 1 origin "refs/tags/${TAG}:refs/tags/${TAG}"
  git -C "${DEST}" checkout --detach "${TAG}"
else
  mkdir -p "$(dirname "${DEST}")"
  git clone --depth 1 --branch "${TAG}" https://github.com/IBM/substrait-compliance.git "${DEST}"
fi

echo "IBM/substrait-compliance ${TAG} @ $(git -C "${DEST}" rev-parse HEAD)"
echo "TPC-H suite: ${DEST}/test-suites/tpch"
