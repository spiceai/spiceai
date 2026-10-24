#!/usr/bin/env bash
# Copyright 2024-2026 The Spice.ai OSS Authors
#
# Release the `pull_request` workflow runs GitHub holds for approval on a branch
# a workflow just pushed with `GITHUB_TOKEN`.
#
# A pull request that `github-actions[bot]` opens or pushes to gets its
# `pull_request` runs created as `completed` / `action_required`: they never
# start until someone approves them. Nobody does for the automated snapshot
# PRs, so those PRs never report a check, and when a newer PR supersedes one
# and closes it, GitHub rewrites every held run to `failure` with zero jobs.
# Approving them from the workflow that pushed the commit makes them run like
# any other PR's checks.
#
# Only runs that satisfy every one of these are approved:
#   * triggered by `pull_request`,
#   * on <branch> at exactly <head-sha>, the commit the caller just pushed,
#   * whose head repository is this repository (never a fork),
#   * triggered by `github-actions[bot]`,
#   * with conclusion `action_required`.
#
# GitHub creates the runs a few seconds after the push or `gh pr create`, so
# this polls until no new run has appeared for a few polls or the wait runs out.
#
# Usage:
#   .github/scripts/approve_bot_pr_runs.sh <branch> <head-sha>
#
# Reads GH_TOKEN (needs the `actions: write` scope) and GITHUB_REPOSITORY.
# APPROVE_WAIT_SECONDS (default 60) bounds the wait and APPROVE_POLL_SECONDS
# (default 5) sets the poll interval. Needs only `gh`, which every caller
# already uses to open the PR.
#
# The commit the caller produced is already pushed when this runs, so an API
# error or a refused approval is reported as a warning and does not fail the
# job: the PR is then left exactly as it was before this script existed.

set -euo pipefail

if [ "$#" -ne 2 ]; then
  echo "usage: $0 <branch> <head-sha>" >&2
  exit 2
fi
branch="$1"
head_sha="$2"
repo="${GITHUB_REPOSITORY:?GITHUB_REPOSITORY must be set}"
wait_seconds="${APPROVE_WAIT_SECONDS:-60}"
poll_seconds="${APPROVE_POLL_SECONDS:-5}"
# Polls with no new run, once the first one has appeared, before the set counts as settled.
settle_polls=2

# The values are spliced into the jq filter below as string literals (`gh --jq`
# takes no variables), so refuse anything that could break out of the quotes.
if ! [[ "${head_sha}" =~ ^[0-9a-f]{40}$ ]]; then
  echo "::error::approve_bot_pr_runs.sh: '${head_sha}' is not a full commit SHA." >&2
  exit 2
fi
for value in "${branch}" "${repo}"; do
  if ! [[ "${value}" =~ ^[A-Za-z0-9._/-]+$ ]]; then
    echo "::error::approve_bot_pr_runs.sh: '${value}' contains characters outside [A-Za-z0-9._/-]." >&2
    exit 2
  fi
done

# The query already narrows to the branch and SHA; the filter re-checks both so
# a run the API returned for any other reason is never approved.
filter=".workflow_runs[]
  | select(.event == \"pull_request\")
  | select(.head_branch == \"${branch}\" and .head_sha == \"${head_sha}\")
  | select(.head_repository.full_name == \"${repo}\")
  | select(.triggering_actor.login == \"github-actions[bot]\")
  | \"\\(.id) \\(.conclusion // \"none\")\""

seen=" "
approved=0
refused=0
quiet=0
deadline=$((SECONDS + wait_seconds))
while :; do
  if ! listing="$(gh api -X GET "repos/${repo}/actions/runs" \
      -f branch="${branch}" -f event=pull_request -f head_sha="${head_sha}" -f per_page=100 \
      --jq "${filter}" 2>&1)"; then
    echo "::warning::Could not list the pull_request runs on ${branch} at ${head_sha}, so none were approved: ${listing}"
    exit 0
  fi

  new=0
  while read -r id conclusion; do
    [ -n "${id}" ] || continue
    case "${seen}" in
      *" ${id} "*) continue ;;
    esac
    seen="${seen}${id} "
    new=1
    if [ "${conclusion}" != "action_required" ]; then
      continue
    fi
    # stdin is the listing this loop reads, so keep gh off it.
    if output="$(gh api -X POST "repos/${repo}/actions/runs/${id}/approve" 2>&1 </dev/null)"; then
      echo "Approved held run ${id}."
      approved=$((approved + 1))
    else
      echo "::warning::GitHub refused to approve held run ${id} on ${branch}: ${output}"
      refused=$((refused + 1))
    fi
  done <<<"${listing}"

  if [ "${seen}" != " " ]; then
    if [ "${new}" -eq 0 ]; then
      quiet=$((quiet + 1))
    else
      quiet=0
    fi
    [ "${quiet}" -lt "${settle_polls}" ] || break
  fi
  [ "${SECONDS}" -lt "${deadline}" ] || break
  sleep "${poll_seconds}"
done

if [ "${seen}" = " " ]; then
  echo "::notice::No pull_request run appeared on ${branch} at ${head_sha} within ${wait_seconds}s; nothing to approve."
else
  echo "Approved ${approved} held run(s) on ${branch} at ${head_sha}; ${refused} refused."
fi
