#!/usr/bin/env bash
# Fetch the compliance suite into tools/substrait-compliance/.ibm at the pinned commit.
#
# The default is the `spiceai` branch of spiceai/substrait-compliance: IBM's
# `main` (suite files identical to the `v0.1.1` release) plus the TPC-H q01
# shipdate-cutoff correction, kept upstreamable. Pin a commit, not the branch,
# so a run is reproducible; move the pin here, in the workflow, and in `SUITE_REF`
# (src/main.rs) together.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DEST="${ROOT}/.ibm"
REF="${SUBSTRAIT_COMPLIANCE_REF:-43d31411c69ef7594887c7d759037bcf8244eeed}"
REPO="${SUBSTRAIT_COMPLIANCE_REPO:-https://github.com/spiceai/substrait-compliance.git}"

if [[ ! -d "${DEST}/.git" ]]; then
  rm -rf "${DEST}"
  git init -q "${DEST}"
  git -C "${DEST}" remote add origin "${REPO}"
else
  git -C "${DEST}" remote set-url origin "${REPO}"
fi
# A commit, branch, or tag; GitHub serves a reachable commit by hash.
git -C "${DEST}" fetch -q --depth 1 origin "${REF}"
git -C "${DEST}" checkout -q --detach FETCH_HEAD

echo "substrait-compliance ${REF} ($(git -C "${DEST}" rev-parse --short HEAD)) from ${REPO} at ${DEST}"
echo "TPC-H suite: ${DEST}/test-suites/tpch"
