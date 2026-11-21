// Copyright 2024-2026 The Spice.ai OSS Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Whether a pull request is a pure revert: every commit it introduces is a Git
// revert, and every commit it reverts is already on the base branch.
//
// Two required checks use this one rule: Attestation fast-tracks a pure revert
// past the developer sign-off (`.github/workflows/pr.yml`), and Trunk Health
// lets a pure revert merge while trunk is broken (`trunk_health.mjs`). Both
// jobs import it from a checkout of the *base* commit, never the PR's, so a PR
// cannot change the rule that judges it without changing the workflows that
// load it, which review covers.

// The footer Git writes for each reverted commit (full 40-char SHA). `git
// revert` creates one commit per reverted commit; a single --no-commit batch
// can carry several footers, so every match counts.
// GitHub's list-commits endpoint for a pull request returns at most this many.
const MAX_LISTED_COMMITS = 250;

const REVERT_FOOTER = /^This reverts commit ([0-9a-f]{40})\.$/gm;

/**
 * The SHAs a commit message reverts, or an empty list when it carries no
 * revert footer.
 */
export function revertedShas(message) {
  return [...(message ?? '').matchAll(REVERT_FOOTER)].map((match) => match[1]);
}

/**
 * Decide whether pull request `pullNumber` is a pure revert onto `baseSha`.
 *
 * Returns `{ pure, reason, commitCount, revertedShas }`. Any API error makes
 * the answer "not pure", with the error in `reason`: callers fall back to their
 * normal path rather than trusting an unproven revert.
 */
export async function checkPureRevert({ github, owner, repo, pullNumber, baseSha }) {
  const notPure = (reason, commitCount = 0) => ({
    pure: false,
    reason,
    commitCount,
    revertedShas: [],
  });

  let commits;
  try {
    commits = await github.paginate(github.rest.pulls.listCommits, {
      owner,
      repo,
      pull_number: pullNumber,
      per_page: 100,
    });
  } catch (error) {
    return notPure(`could not list the PR's commits (${error.message})`);
  }
  if (commits.length === 0) {
    return notPure('the PR introduces no commits');
  }
  // The API lists at most MAX_LISTED_COMMITS of a PR's commits, so a PR that
  // reaches the cap may have more that were never examined.
  if (commits.length >= MAX_LISTED_COMMITS) {
    return notPure(
      `the PR has at least ${MAX_LISTED_COMMITS} commits, more than the API lists, so not all could be checked`,
      commits.length
    );
  }

  const reverted = new Set();
  for (const commit of commits) {
    const shas = revertedShas(commit.commit?.message);
    if (shas.length === 0) {
      return notPure(
        `commit ${commit.sha} is not a Git revert (no "This reverts commit ..." footer)`,
        commits.length
      );
    }
    for (const sha of shas) reverted.add(sha);
  }

  // Each reverted commit must already be on the base branch: only undoing
  // something that actually landed counts. compare base...sha reports
  // ahead_by === 0 exactly when sha carries no commits the base lacks, that is,
  // when sha is an ancestor of (or equal to) the base tip.
  for (const sha of reverted) {
    let aheadBy;
    try {
      const { data } = await github.rest.repos.compareCommitsWithBasehead({
        owner,
        repo,
        basehead: `${baseSha}...${sha}`,
      });
      aheadBy = data.ahead_by;
    } catch (error) {
      return notPure(
        `could not confirm reverted commit ${sha} is on the base branch (${error.message})`,
        commits.length
      );
    }
    if (aheadBy !== 0) {
      return notPure(`reverted commit ${sha} is not on the base branch`, commits.length);
    }
  }

  return {
    pure: true,
    reason: `every one of ${commits.length} commit(s) reverts a commit on the base branch`,
    commitCount: commits.length,
    revertedShas: [...reverted],
  };
}
