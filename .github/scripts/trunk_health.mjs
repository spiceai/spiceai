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

// The Trunk Health gate (`.github/workflows/trunk_health.yml`).
//
// The comprehensive tests run after merge, on trunk. A failure they find is
// tracked as an open issue labeled `trunk-broken` or an open PR labeled
// `fixes-trunk`. This gate keeps those from being ignored: once one has carried
// its label for more than GRACE_HOURS, every merge that does not fix it is
// blocked. As a backstop, when the last BACKSTOP_RUNS post-merge runs failed and
// nothing is labeled at all, merging is blocked until someone files the failure.
//
// A PR is exempt when it is labeled `fixes-trunk`, closes (by a closing link) an
// issue labeled `trunk-broken`, or is a pure revert from a branch of this
// repository. Removing the label from a
// blocking item is the only override. See docs/dev/ci_signoff.md.
//
// The merge-queue run enforces. The pull_request run is advisory: it never
// fails, and posts or updates one PR comment on each of the PR's events
// instead, so a PR is not left with a stale red check after trunk recovers.

import { checkPureRevert } from './pure_revert.mjs';

export const BROKEN_LABEL = 'trunk-broken';
export const FIX_LABEL = 'fixes-trunk';
export const GRACE_HOURS = 24;
export const BACKSTOP_RUNS = 2;
export const POST_MERGE_WORKFLOW = 'nightly_tests.yml';
export const COMMENT_MARKER = '<!-- trunk-health -->';

const COMMENT_AUTHOR = 'github-actions[bot]';
const HOUR_MS = 60 * 60 * 1000;
const RUNS_PER_PAGE = 20;
// Conclusions that say nothing about whether trunk is healthy.
const IGNORED_CONCLUSIONS = new Set(['cancelled', 'skipped', 'neutral']);

/** The PR number a merge-queue ref was built for, or null when it does not match. */
export function parseQueueRef(headRef) {
  const match = /^refs\/heads\/gh-readonly-queue\/.+\/pr-(\d+)-[0-9a-f]+$/.exec(headRef ?? '');
  return match ? Number(match[1]) : null;
}

/** When `label` was most recently added to issue or PR `number`, or null if never. */
async function labeledAt(github, owner, repo, number, label) {
  const events = await github.paginate(github.rest.issues.listEvents, {
    owner,
    repo,
    issue_number: number,
    per_page: 100,
  });
  let latest = null;
  for (const event of events) {
    if (event.event !== 'labeled' || event.label?.name !== label) continue;
    const at = new Date(event.created_at);
    if (latest === null || at > latest) latest = at;
  }
  return latest;
}

/**
 * Open issues labeled BROKEN_LABEL and open PRs labeled FIX_LABEL, each with
 * how long it has carried its label.
 */
export async function blockingItems({ github, owner, repo, now }) {
  const items = [];
  for (const [label, wantPr] of [
    [BROKEN_LABEL, false],
    [FIX_LABEL, true],
  ]) {
    const found = await github.paginate(github.rest.issues.listForRepo, {
      owner,
      repo,
      state: 'open',
      labels: label,
      per_page: 100,
    });
    for (const entry of found) {
      if (Boolean(entry.pull_request) !== wantPr) continue;
      const at = await labeledAt(github, owner, repo, entry.number, label);
      if (at === null) {
        throw new Error(`could not find when ${label} was added to #${entry.number}`);
      }
      const ageHours = (now - at) / HOUR_MS;
      items.push({
        kind: wantPr ? 'PR' : 'issue',
        number: entry.number,
        title: entry.title,
        url: entry.html_url,
        label,
        ageHours,
        overdue: ageHours > GRACE_HOURS,
      });
    }
  }
  return items;
}

/**
 * The newest BACKSTOP_RUNS completed post-merge runs on `branch` that reached a
 * verdict, and whether all of them failed. A post-merge workflow that does not
 * exist on the branch yet leaves the backstop inactive.
 */
export async function postMergeRuns({ github, owner, repo, branch }) {
  const runs = [];
  // Page until BACKSTOP_RUNS runs with a verdict are found, however many
  // cancelled ones sit between them, or the history runs out.
  for (let page = 1; runs.length < BACKSTOP_RUNS; page += 1) {
    let data;
    try {
      ({ data } = await github.rest.actions.listWorkflowRuns({
        owner,
        repo,
        workflow_id: POST_MERGE_WORKFLOW,
        branch,
        status: 'completed',
        per_page: RUNS_PER_PAGE,
        page,
      }));
    } catch (error) {
      if (error.status === 404) return { missing: true, failing: false, runs: [] };
      throw error;
    }
    const pageRuns = data.workflow_runs ?? [];
    for (const run of pageRuns) {
      if (IGNORED_CONCLUSIONS.has(run.conclusion) || runs.length === BACKSTOP_RUNS) continue;
      runs.push({ url: run.html_url, conclusion: run.conclusion, sha: run.head_sha });
    }
    if (pageRuns.length < RUNS_PER_PAGE) break;
  }
  const failing = runs.length === BACKSTOP_RUNS && runs.every((run) => run.conclusion !== 'success');
  return { missing: false, failing, runs };
}

/** Why PR `pr` may merge while trunk is broken, or null when it may not. */
export async function exemption({ github, owner, repo, pr, baseSha }) {
  const labels = (pr.labels ?? []).map((label) => label.name);
  if (labels.includes(FIX_LABEL)) return `it is labeled \`${FIX_LABEL}\``;

  // Only this repository's issues count: a closing link to another
  // repository's issue, which anyone could create and label, earns nothing.
  const thisRepo = `${owner}/${repo}`.toLowerCase();
  let cursor = null;
  do {
    const { repository } = await github.graphql(
      `query($owner: String!, $repo: String!, $number: Int!, $cursor: String) {
        repository(owner: $owner, name: $repo) {
          pullRequest(number: $number) {
            closingIssuesReferences(first: 100, after: $cursor) {
              pageInfo { hasNextPage endCursor }
              nodes {
                number
                repository { nameWithOwner }
                labels(first: 100) { nodes { name } }
              }
            }
          }
        }
      }`,
      { owner, repo, number: pr.number, cursor }
    );
    const references = repository.pullRequest.closingIssuesReferences;
    for (const issue of references.nodes) {
      if (issue.repository.nameWithOwner.toLowerCase() !== thisRepo) continue;
      if (issue.labels.nodes.some((label) => label.name === BROKEN_LABEL)) {
        return `it closes #${issue.number}, which is labeled \`${BROKEN_LABEL}\``;
      }
    }
    cursor = references.pageInfo.hasNextPage ? references.pageInfo.endCursor : null;
  } while (cursor !== null);

  // A revert footer is the author's claim, and the check does not prove the
  // changes undo the commit. From a branch of this repository that claim comes
  // from someone with write access, who could label the PR `fixes-trunk`
  // anyway; from a fork it costs nothing, so a fork's revert needs the label.
  const sameRepo =
    pr.head?.repo?.full_name !== undefined && pr.head.repo.full_name === pr.base?.repo?.full_name;
  if (!sameRepo) return null;
  const revert = await checkPureRevert({ github, owner, repo, pullNumber: pr.number, baseSha });
  if (revert.pure) return 'it is a pure revert';
  return null;
}

/** Whether PR `prNumber` may merge into `branch` now, and why. */
/**
 * The PRs a merge-queue entry would merge: its own and those of the entries
 * ahead of it that have not merged yet. With grouping, GitHub merges a batch
 * when the last entry's checks pass, so the entries ahead of it merge on this
 * run's verdict too. They are the commits on the group's head that `branch`
 * does not have yet, each a squash commit whose subject ends `(#N)`.
 */
export async function queuedPrs({ github, owner, repo, branch, headSha, prNumber }) {
  const { data } = await github.rest.repos.compareCommitsWithBasehead({
    owner,
    repo,
    basehead: `${branch}...${headSha}`,
  });
  const numbers = new Set();
  for (const commit of data.commits ?? []) {
    const subject = (commit.commit?.message ?? '').split('\n', 1)[0];
    const match = /\(#(\d+)\)$/.exec(subject.trim());
    if (!match) {
      throw new Error(`could not tell which PR merge-queue commit ${commit.sha} belongs to`);
    }
    numbers.add(Number(match[1]));
  }
  numbers.add(prNumber);
  return [...numbers];
}

/** Whether the PRs `prNumbers` may merge into `branch` now, and why. */
export async function evaluate({ github, owner, repo, prNumbers, baseSha, branch, now }) {
  const items = await blockingItems({ github, owner, repo, now });
  const backstop = await postMergeRuns({ github, owner, repo, branch });
  const overdue = items.filter((item) => item.overdue);
  const backstopFires = backstop.failing && items.length === 0;

  // Every PR that would merge must be exempt, not only the one this run is for.
  const exemptions = [];
  if (overdue.length > 0 || backstopFires) {
    for (const number of prNumbers) {
      const { data: pr } = await github.rest.pulls.get({ owner, repo, pull_number: number });
      exemptions.push({ number, reason: await exemption({ github, owner, repo, pr, baseSha }) });
    }
  }
  const exempt = exemptions.length > 0 && exemptions.every((e) => e.reason !== null);
  const blocked = (overdue.length > 0 || backstopFires) && !exempt;
  return { blocked, items, overdue, backstop, backstopFires, exempt, exemptions, prNumbers };
}

const hours = (value) => `${Math.floor(value)}h`;

/** The Markdown report of an evaluation. */
export function render(result) {
  const lines = [];
  if (result.blocked) {
    lines.push('### Trunk Health: merging is blocked', '');
    if (result.overdue.length > 0) {
      lines.push(
        `Trunk has a post-merge failure that has gone unaddressed for more than ${GRACE_HOURS}h:`,
        ''
      );
    } else {
      lines.push(
        `The last ${BACKSTOP_RUNS} post-merge runs on trunk failed, and no open issue is ` +
          `labeled \`${BROKEN_LABEL}\` and no open PR \`${FIX_LABEL}\`:`,
        ''
      );
    }
  } else if (result.exempt) {
    if (result.exemptions.length === 1) {
      lines.push(`### Trunk Health: this PR may merge because ${result.exemptions[0].reason}`, '');
    } else {
      lines.push('### Trunk Health: every PR in this merge group may merge', '');
      for (const e of result.exemptions) lines.push(`- #${e.number}: ${e.reason}`);
      lines.push('');
    }
  } else if (result.items.length > 0) {
    lines.push(
      '### Trunk Health: trunk is broken',
      '',
      `Merging is not blocked yet. It will be once an item below has carried its label for ${GRACE_HOURS}h.`,
      ''
    );
  } else {
    lines.push('### Trunk Health: trunk is healthy', '');
  }

  for (const item of result.items) {
    const state = item.overdue ? '**overdue**' : `blocks merging in ${hours(GRACE_HOURS - item.ageHours)}`;
    lines.push(
      `- ${item.kind} [#${item.number}](${item.url}) ${item.title} — \`${item.label}\` for ${hours(item.ageHours)}, ${state}`
    );
  }
  if (result.backstopFires) {
    for (const run of result.backstop.runs) {
      lines.push(`- [${POST_MERGE_WORKFLOW} run](${run.url}) at ${run.sha.slice(0, 12)}: ${run.conclusion}`);
    }
  }

  if (result.blocked && result.prNumbers.length > 1) {
    const notExempt = result.exemptions.filter((e) => e.reason === null).map((e) => `#${e.number}`);
    lines.push(
      '',
      `This merge group would also merge ${result.prNumbers.length - 1} PR(s) queued ahead of this one; ` +
        `each must be exempt. Not exempt: ${notExempt.join(', ')}.`
    );
  }
  if (result.blocked) {
    lines.push(
      '',
      'To unblock:',
      '',
      result.overdue.length > 0
        ? '- fix the item above, or'
        : `- open an issue labeled \`${BROKEN_LABEL}\` for the failure (it then has ${GRACE_HOURS}h before it blocks), or fix it, or`,
      `- label this PR \`${FIX_LABEL}\` if it is the fix, or link the issue with a closing keyword (\`Fixes #N\`), or`,
      '- ask a maintainer to remove the label if the item should not block merging.',
      '',
      'See [Trunk Health](https://github.com/spiceai/spiceai/blob/trunk/docs/dev/ci_signoff.md#trunk-health).'
    );
  }
  return lines.join('\n');
}

const UNBLOCKED_BODY =
  `${COMMENT_MARKER}\n### Trunk Health: merging is no longer blocked\n\n` +
  'The post-merge failure this comment reported has been addressed or exempted.';

/** Create, update or retire the one Trunk Health comment on a PR. */
async function syncComment({ github, owner, repo, prNumber, blocked, body, core }) {
  const comments = await github.paginate(github.rest.issues.listComments, {
    owner,
    repo,
    issue_number: prNumber,
    per_page: 100,
  });
  const existing = comments.find(
    (comment) => comment.user?.login === COMMENT_AUTHOR && comment.body?.includes(COMMENT_MARKER)
  );
  const wanted = blocked ? `${COMMENT_MARKER}\n${body}` : UNBLOCKED_BODY;
  if (existing) {
    if (existing.body === wanted) return;
    await github.rest.issues.updateComment({ owner, repo, comment_id: existing.id, body: wanted });
    core.info(`Updated Trunk Health comment ${existing.id}.`);
  } else if (blocked) {
    await github.rest.issues.createComment({ owner, repo, issue_number: prNumber, body: wanted });
    core.info('Posted a Trunk Health comment.');
  }
}

/** The workflow step: enforce in the merge queue, advise on a pull request. */
export async function run({ github, context, core, now = new Date() }) {
  const { owner, repo } = context.repo;
  const event = context.eventName;
  const enforce = event === 'merge_group';
  if (!enforce && event !== 'pull_request') {
    core.setFailed(`Trunk Health runs on pull_request and merge_group, not ${event}.`);
    return;
  }

  let prNumber;
  let baseSha;
  let branch;
  let headSha;
  if (enforce) {
    const group = context.payload.merge_group;
    prNumber = parseQueueRef(group?.head_ref);
    baseSha = group?.base_sha;
    headSha = group?.head_sha;
    branch = (group?.base_ref ?? '').replace(/^refs\/heads\//, '');
    if (prNumber === null) {
      core.setFailed(
        `Could not read the PR number from merge-queue ref ${group?.head_ref}, so Trunk Health ` +
          'cannot tell whether this PR may merge. Failing closed; re-run the check.'
      );
      return;
    }
  } else {
    const pr = context.payload.pull_request;
    prNumber = pr.number;
    baseSha = pr.base.sha;
    branch = pr.base.ref;
  }

  let result;
  try {
    const prNumbers = enforce
      ? await queuedPrs({ github, owner, repo, branch, headSha, prNumber })
      : [prNumber];
    result = await evaluate({ github, owner, repo, prNumbers, baseSha, branch, now });
  } catch (error) {
    const message =
      `Could not determine trunk health (${error.message}), so Trunk Health cannot tell ` +
      'whether this PR may merge.';
    if (enforce) {
      core.setFailed(`${message} Failing closed; re-run the check.`);
    } else {
      core.warning(`${message} The merge-queue run decides; this pull_request run is advisory.`);
    }
    return;
  }

  if (result.backstop.missing) {
    core.notice(`${POST_MERGE_WORKFLOW} does not exist on ${branch} yet, so the backstop is inactive.`);
  }
  const body = render(result);
  await core.summary.addRaw(body).write();

  if (enforce) {
    if (result.blocked) core.setFailed(body);
    else core.info(body);
    return;
  }

  if (result.blocked) {
    core.warning('Trunk Health: merging is blocked. The merge queue will reject this PR; see the job summary.');
  }
  const pr = context.payload.pull_request;
  if (pr.head.repo?.full_name !== pr.base.repo?.full_name) {
    core.info('Fork PR: its token cannot comment, so the report is in the job summary only.');
    return;
  }
  try {
    await syncComment({ github, owner, repo, prNumber, blocked: result.blocked, body, core });
  } catch (error) {
    core.warning(`Could not update the Trunk Health comment (${error.message}).`);
  }
}
