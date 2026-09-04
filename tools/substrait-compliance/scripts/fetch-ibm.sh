#!/usr/bin/env bash
# Clone IBM/substrait-compliance at the pinned tag into tools/substrait-compliance/.ibm
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DEST="${ROOT}/.ibm"
TAG="${IBM_SUBSTRAIT_COMPLIANCE_TAG:-v0.1.1}"
REPO="${IBM_SUBSTRAIT_COMPLIANCE_REPO:-https://github.com/IBM/substrait-compliance.git}"

if [[ -d "${DEST}/.git" ]]; then
  git -C "${DEST}" fetch --tags --depth 1 origin "refs/tags/${TAG}:refs/tags/${TAG}" || \
    git -C "${DEST}" fetch --tags origin tag "${TAG}"
  git -C "${DEST}" checkout -q "${TAG}"
else
  rm -rf "${DEST}"
  git clone --depth 1 --branch "${TAG}" "${REPO}" "${DEST}"
fi

echo "IBM/substrait-compliance ${TAG} at ${DEST}"
echo "TPC-H suite: ${DEST}/test-suites/tpch"
