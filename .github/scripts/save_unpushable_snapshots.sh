#!/usr/bin/env bash
# Copyright 2024-2026 The Spice.ai OSS Authors
#
# Keep a snapshot commit that GitHub would not let this workflow push, and say
# why, instead of failing with "see the git output above".
#
# The snapshot workflows push a new `testoperator-snapshot-update/...` branch
# with GITHUB_TOKEN, which has no `workflows` permission. Before GitHub accepts
# a new branch from such a token it must confirm the branch adds no workflow
# file changes. For a branch that starts from a release-branch commit (far from
# trunk) that check runs out of time, and GitHub refuses the push with:
#
#   ! [remote rejected] HEAD -> <branch> (Unable to determine if workflow can be
#     created or updated due to timeout; `workflows` scope may be required.)
#
# Retrying does not help: the same push is refused every time, and creating the
# ref through the REST API is refused as well ("Resource not accessible by
# integration"). The snapshots themselves are fine, so this saves the commit as
# a patch under $RUNNER_TEMP/unpushed-snapshots/ (the caller uploads it as the
# `unpushed-snapshots` artifact) and prints how to land it by hand.
#
# Only that one rejection is handled. Every other refusal (a protected branch,
# a merge-queue lock, a real workflow-file change, a non-fast-forward) is left
# to the caller, because each means something different.
#
# Usage:
#   .github/scripts/save_unpushable_snapshots.sh <push-log-file> <branch> <base-ref>
#
#   <push-log-file>  the combined output of the failed `git push`
#   <branch>         the branch the push tried to create
#   <base-ref>       the branch the run was dispatched on (the PR's base)
#
# Run from the repository with the snapshot commit at HEAD. Reads RUNNER_TEMP,
# GITHUB_REPOSITORY and GITHUB_RUN_ID, and appends to GITHUB_STEP_SUMMARY when
# it is set.
#
# Exit status: 0 when the log is this rejection and the patch was saved (the
# caller should still fail the step: the snapshots were not published); 1 when
# the log is some other rejection; 2 on a usage error or when the patch could
# not be written.

set -uo pipefail

if [ "$#" -ne 3 ]; then
  echo "usage: $0 <push-log-file> <branch> <base-ref>" >&2
  exit 2
fi

push_log_file="$1"
branch="$2"
base_ref="$3"

if [ ! -r "${push_log_file}" ]; then
  echo "::error::save_unpushable_snapshots.sh cannot read the push log ${push_log_file}." >&2
  exit 2
fi

# The exact text GitHub prints for this refusal. A plain "workflows" match would
# also catch "refusing to allow a GitHub App to create or update workflow ...",
# which means the commit really does change a workflow file.
if ! grep -qF 'Unable to determine if workflow can be created or updated due to timeout' "${push_log_file}"; then
  exit 1
fi

out_dir="${RUNNER_TEMP:-/tmp}/unpushed-snapshots"
patch="${out_dir}/snapshots.patch"
if ! mkdir -p "${out_dir}" || ! git format-patch -1 HEAD --stdout >"${patch}" || [ ! -s "${patch}" ]; then
  echo "::error::GitHub refused to create ${branch}, and the snapshot commit could not be saved as a patch either; the snapshots from this run are lost."
  exit 2
fi
parent="$(git rev-parse HEAD~1)"
files="$(git diff-tree --no-commit-id --name-only -r HEAD | wc -l | tr -d ' ')"

run="${GITHUB_RUN_ID:-<run-id>}"
repo="${GITHUB_REPOSITORY:-spiceai/spiceai}"
message="GitHub refused to let this workflow's GITHUB_TOKEN create ${branch}: the branch starts from a ${base_ref} commit, and GitHub could not confirm in time that it adds no workflow files, which a token without the 'workflows' permission must show. Retrying will not help. The snapshot commit (${files} files) is saved as this run's 'unpushed-snapshots' artifact; to land it, see the job summary."

echo "::error::${message}"
echo "Saved the snapshot commit to ${patch}."

if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then
  cat >>"${GITHUB_STEP_SUMMARY}" <<EOF
### Snapshots not pushed: GitHub refused to create \`${branch}\`

${message}

To land the snapshots with your own credentials:

\`\`\`bash
gh run download ${run} --repo ${repo} --name unpushed-snapshots
git fetch origin ${base_ref}
git switch -c ${branch} ${parent}
git am snapshots.patch
git push origin ${branch}
gh pr create --base ${base_ref} --head ${branch} --label kind/bug --label area/tests --fill
\`\`\`
EOF
fi

exit 0
