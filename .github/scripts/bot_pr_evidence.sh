#!/usr/bin/env bash
# Copyright 2024-2026 The Spice.ai OSS Authors
#
# The Evidence section for a pull request a workflow opens or updates with a
# commit it generated (snapshots, schemas): an entry naming that commit and
# linking the run that produced it. Attestation's evidence check
# (.github/scripts/pr_evidence.mjs) needs each entry to name a PR commit and carry
# output; whether the run supports the change is for review. What the run did,
# workload included, is the entry's code span: a later run of the same workload
# re-runs "the same command" and keeps the earlier entry current, while another
# workload's run does not.
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
# made with GITHUB_TOKEN starts no `edited` run of its own. GitHub has no
# conditional edit, so a run updating the same PR at the same moment can drop this
# entry, and the evidence check cannot see an entry that is missing. `update`
# therefore reads the description back, puts the entry back if it is gone, and
# exits non-zero if it still is after three tries, so a lost entry fails the run
# that wrote it instead of passing unseen. EVIDENCE_RECHECK_DELAY sets the pause
# before each read-back (default: 5 to 14 seconds, jittered).
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
    new_entry="$(entry "${what}")"
    file="$(mktemp)"
    add_entry() {
      gh pr view "${pr}" --json body --jq .body | ENTRY="${new_entry}" awk '
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
    }
    has_entry() { gh pr view "${pr}" --json body --jq .body | grep -qxF -- "${new_entry}"; }
    add_entry
    for _ in 1 2 3; do
      sleep "${EVIDENCE_RECHECK_DELAY:-$((RANDOM % 10 + 5))}"
      if has_entry; then
        rm -f "${file}"
        exit 0
      fi
      echo "The evidence entry for ${what} is missing from ${pr} after a concurrent update; adding it again." >&2
      add_entry
    done
    rm -f "${file}"
    if has_entry; then exit 0; fi
    echo "The evidence entry for ${what} could not be kept in ${pr}'s description; add it by hand: ${new_entry}" >&2
    exit 1
    ;;
  *)
    echo 'usage: bot_pr_evidence.sh section "<what the run did>" | update <pr> "<what the run did>"' >&2
    exit 2
    ;;
esac
