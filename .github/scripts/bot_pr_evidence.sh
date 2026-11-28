#!/usr/bin/env bash
# Copyright 2024-2026 The Spice.ai OSS Authors
#
# The Evidence section for a pull request a workflow opens or updates with a
# commit it generated (snapshots, schemas): that commit, and the run that
# produced it. Attestation's evidence check (.github/scripts/pr_evidence.mjs)
# needs a commit from the PR with a link to its run; whether the run supports the
# change is for review.
#
#   bot_pr_evidence.sh section "<what the run did>"        print the section for HEAD
#   bot_pr_evidence.sh update <pr> "<what the run did>"   replace it in an open PR's description
#
# Call `update` before approve_bot_pr_runs.sh releases the runs GitHub holds on
# the push, so the Attestation run they start reads the new description: an edit
# made with GITHUB_TOKEN starts no `edited` run of its own.
set -euo pipefail

section() {
  local sha run_url
  sha="$(git rev-parse HEAD)"
  run_url="${GITHUB_SERVER_URL:?}/${GITHUB_REPOSITORY:?}/actions/runs/${GITHUB_RUN_ID:?}"
  printf '## 🧪 Evidence\n\nGenerated at %s by %s: %s\n' "${sha:0:12}" "$1" "${run_url}"
}

case "${1:-}" in
  section)
    section "${2:?what the run did}"
    ;;
  update)
    pr="${2:?pull request}"
    what="${3:?what the run did}"
    body="$(gh pr view "${pr}" --json body --jq .body)"
    new_section="$(section "${what}")"
    file="$(mktemp)"
    # Drop the old section, from its level-2 heading to the next one, and append
    # the new section.
    {
      printf '%s\n' "${body}" | awk '/^## /{ skip = /^## .*[Ee]vidence/ } !skip'
      printf '\n%s\n' "${new_section}"
    } >"${file}"
    gh pr edit "${pr}" --body-file "${file}"
    rm -f "${file}"
    ;;
  *)
    echo 'usage: bot_pr_evidence.sh section "<what the run did>" | update <pr> "<what the run did>"' >&2
    exit 2
    ;;
esac
