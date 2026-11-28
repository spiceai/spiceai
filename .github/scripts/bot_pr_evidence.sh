#!/usr/bin/env bash
# Copyright 2024-2026 The Spice.ai OSS Authors
#
# The Evidence section for a pull request a workflow opens or updates with a
# commit it generated (snapshots, schemas): an entry naming that commit and
# linking the run that produced it. Attestation's evidence check
# (.github/scripts/pr_evidence.mjs) needs each entry to name a PR commit and carry
# output; whether the run supports the change is for review. What the run did is
# the entry's code span, so a later run of the same generation re-runs "the same
# command" and keeps the earlier entry from going stale.
#
#   bot_pr_evidence.sh section "<what the run did>"        print the section for HEAD
#   bot_pr_evidence.sh update <pr> "<what the run did>"   add an entry to an open PR's section
#
# `update` keeps every earlier entry and every other part of the description. It
# reads headings as the evidence check does: outside fenced blocks, with the
# section ending at the next heading of the same or a higher level.
#
# Call `update` before approve_bot_pr_runs.sh releases the runs GitHub holds on
# the push, so the Attestation run they start reads the new description: an edit
# made with GITHUB_TOKEN starts no `edited` run of its own. Two runs updating one
# PR at once can lose one entry; the check then fails rather than passes.
set -euo pipefail

entry() {
  local sha run_url
  sha="$(git rev-parse HEAD)"
  run_url="${GITHUB_SERVER_URL:?}/${GITHUB_REPOSITORY:?}/actions/runs/${GITHUB_RUN_ID:?}"
  printf 'Generated at %s by `%s`: %s\n' "${sha:0:12}" "$1" "${run_url}"
}

case "${1:-}" in
  section)
    printf '## 🧪 Evidence\n\n%s' "$(entry "${2:?what the run did}")"
    printf '\n'
    ;;
  update)
    pr="${2:?pull request}"
    what="${3:?what the run did}"
    body="$(gh pr view "${pr}" --json body --jq .body)"
    new_entry="$(entry "${what}")"
    file="$(mktemp)"
    printf '%s\n' "${body}" | ENTRY="${new_entry}" awk '
      function heading_level(line) { match(line, /^#+ /); return RLENGTH - 1 }
      /^[[:space:]]*(```|~~~)/ { fenced = !fenced; print; next }
      !fenced && /^#+ / {
        level = heading_level($0)
        if (in_section && level <= section_level) { print ENVIRON["ENTRY"]; print ""; in_section = 0; added = 1 }
        if (!added && !in_section && $0 ~ /^#+ [^A-Za-z0-9]*[Ee]vidence[^A-Za-z0-9]*$/) { in_section = 1; section_level = level }
      }
      { print }
      END {
        if (in_section) { print ""; print ENVIRON["ENTRY"] }
        else if (!added) { print ""; print "## 🧪 Evidence"; print ""; print ENVIRON["ENTRY"] }
      }' >"${file}"
    gh pr edit "${pr}" --body-file "${file}"
    rm -f "${file}"
    ;;
  *)
    echo 'usage: bot_pr_evidence.sh section "<what the run did>" | update <pr> "<what the run did>"' >&2
    exit 2
    ;;
esac
