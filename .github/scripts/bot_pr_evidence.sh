#!/usr/bin/env bash
# Copyright 2024-2026 The Spice.ai OSS Authors
#
# The Evidence section for a pull request a workflow opens or updates with a
# commit it generated (snapshots, schemas). Attestation's evidence check
# (.github/scripts/pr_evidence.mjs) needs an entry naming a PR commit with output.
# The run that generated the commit ran at the commit it was dispatched on, not at
# the generated one, so its link is not output for that commit; the entry's output
# is a fenced block with the workload's command and the commit it produced, and
# the run link stays beside it so a reviewer can open the run. Whether the run
# supports the change is for review.
#
#   bot_pr_evidence.sh section "<workload command>"        print the section for HEAD
#   bot_pr_evidence.sh update <pr> "<workload command>"   replace the section in an open PR
#
# The command names the workload (query set, scale, Spicepod, options). The
# workflows that call this key each snapshot branch, and so each PR, on one
# workload, so the section only ever holds that workload's entry, and `update`
# replaces it. Everything else in the description is kept; headings are read as
# the evidence check reads them, outside fenced blocks, with the section ending at
# the next heading of the same or a higher level.
#
# Call `update` before approve_bot_pr_runs.sh releases the runs GitHub holds on
# the push, so the Attestation run they start reads the new description: an edit
# made with GITHUB_TOKEN starts no `edited` run of its own. GitHub has no
# conditional edit, so two runs of the same workload updating one PR at once can
# leave the older one's entry. The check then finds it stale and fails, rather
# than passes. `update` also reads the description back and writes its entry
# again if a later write replaced it, and exits non-zero if it is still gone after
# three tries. EVIDENCE_RECHECK_DELAY sets the pause before each read-back
# (default: 5 to 14 seconds, jittered).
set -euo pipefail

sha="$(git rev-parse HEAD)"
run_url="${GITHUB_SERVER_URL:?}/${GITHUB_REPOSITORY:?}/actions/runs/${GITHUB_RUN_ID:?}"
first_line="Generated at ${sha:0:12} by ${run_url}:"

entry() {
  printf '%s\n\n```\n$ %s\ncommitted %s\n```\n' "${first_line}" "$1" "${sha:0:12}"
}

case "${1:-}" in
  section)
    printf '## 🧪 Evidence\n\n'
    entry "${2:?workload command}"
    ;;
  update)
    pr="${2:?pull request}"
    new_entry="$(entry "${3:?workload command}")"
    file="$(mktemp)"
    write_section() {
      gh pr view "${pr}" --json body --jq .body | ENTRY="${new_entry}" awk '
        function heading_level(line) { match(line, /^#+ /); return RLENGTH - 1 }
        /^[[:space:]]*(```|~~~)/ { fenced = !fenced; if (!in_section) print; next }
        !fenced && /^#+ / {
          level = heading_level($0)
          if (in_section && level <= section_level) { print ENVIRON["ENTRY"]; print ""; in_section = 0; done = 1 }
          if (!done && !in_section && $0 ~ /^#+ [^A-Za-z0-9]*[Ee]vidence[^A-Za-z0-9]*$/) {
            print; print ""; in_section = 1; section_level = level; next
          }
        }
        !in_section { print }
        END {
          if (in_section) { print ENVIRON["ENTRY"] }
          else if (!done) { print ""; print "## 🧪 Evidence"; print ""; print ENVIRON["ENTRY"] }
        }' >"${file}"
      gh pr edit "${pr}" --body-file "${file}"
    }
    has_entry() { gh pr view "${pr}" --json body --jq .body | grep -qxF -- "${first_line}"; }
    write_section
    for _ in 1 2 3; do
      sleep "${EVIDENCE_RECHECK_DELAY:-$((RANDOM % 10 + 5))}"
      if has_entry; then
        rm -f "${file}"
        exit 0
      fi
      echo "The evidence entry for ${sha:0:12} is missing from ${pr} after a concurrent update; writing it again." >&2
      write_section
    done
    rm -f "${file}"
    if has_entry; then exit 0; fi
    echo "The evidence entry could not be kept in ${pr}'s description; add it by hand: ${first_line}" >&2
    exit 1
    ;;
  *)
    echo 'usage: bot_pr_evidence.sh section "<workload command>" | update <pr> "<workload command>"' >&2
    exit 2
    ;;
esac
