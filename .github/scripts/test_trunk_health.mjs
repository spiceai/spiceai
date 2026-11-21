// Copyright 2024-2026 The Spice.ai OSS Authors
//
// Unit tests for trunk_health.mjs, the Trunk Health gate. Each runs the real
// `run()` the workflow calls against an in-memory GitHub API.
// Run: node .github/scripts/test_trunk_health.mjs

import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';

import { COMMENT_MARKER, parseQueueRef, run } from './trunk_health.mjs';
import { fakeCore, fakeGithub, testRunner } from './fake_github.mjs';

const NOW = new Date('2026-11-20T12:00:00Z');
const hoursAgo = (h) => new Date(NOW.getTime() - h * 3600 * 1000).toISOString();
const BASE = 'f'.repeat(40);
const PR = 42;

const mergeGroup = (pr = PR) => ({
  eventName: 'merge_group',
  repo: { owner: 'o', repo: 'r' },
  payload: {
    merge_group: {
      head_ref: `refs/heads/gh-readonly-queue/trunk/pr-${pr}-${'1'.repeat(40)}`,
      base_ref: 'refs/heads/trunk',
      base_sha: BASE,
      head_sha: 'e'.repeat(40),
    },
  },
});
const pullRequest = ({ fork = false } = {}) => ({
  eventName: 'pull_request',
  repo: { owner: 'o', repo: 'r' },
  payload: {
    pull_request: {
      number: PR,
      base: { sha: BASE, ref: 'trunk', repo: { full_name: 'o/r' } },
      head: { repo: { full_name: fork ? 'someone/r' : 'o/r' } },
    },
  },
});

/** An open issue labeled trunk-broken `age` hours ago. */
const brokenIssue = (number, age) => ({
  issues: [{ number, labels: ['trunk-broken'] }],
  events: { [number]: [{ event: 'labeled', label: 'trunk-broken', created_at: hoursAgo(age) }] },
});

async function gate(context, state) {
  const { github, writes, calls } = fakeGithub(state);
  const { core, out } = fakeCore();
  await run({ github, context, core, now: NOW });
  return { ...out, writes, calls };
}

const { test, runAll } = testRunner();

// --- Enforcement in the merge queue ----------------------------------------

test('no blocking items and no failing runs: passes', async () => {
  const r = await gate(mergeGroup(), { runs: [{ conclusion: 'success' }] });
  assert.deepEqual(r.failed, []);
  assert.match(r.summary, /trunk is healthy/);
});

test('an item inside the grace period does not block', async () => {
  const r = await gate(mergeGroup(), brokenIssue(7, 23.9));
  assert.deepEqual(r.failed, []);
  assert.match(r.summary, /Merging is not blocked yet/);
  assert.match(r.summary, /blocks merging in 0h/);
});

test('an issue past the grace period blocks, naming it and how to unblock', async () => {
  const r = await gate(mergeGroup(), brokenIssue(7, 24.5));
  assert.equal(r.failed.length, 1);
  assert.match(r.failed[0], /merging is blocked/);
  assert.match(r.failed[0], /issue \[#7\]\(https:\/\/github.com\/o\/r\/issues\/7\) item 7 — `trunk-broken` for 24h, \*\*overdue\*\*/);
  assert.match(r.failed[0], /label this PR `fixes-trunk`/);
  assert.match(r.failed[0], /remove the label/);
});

test('an open fixes-trunk PR past the grace period blocks other PRs', async () => {
  const r = await gate(mergeGroup(), {
    issues: [{ number: 9, labels: ['fixes-trunk'], pr: true }],
    events: { 9: [{ event: 'labeled', label: 'fixes-trunk', created_at: hoursAgo(30) }] },
  });
  assert.equal(r.failed.length, 1);
  assert.match(r.failed[0], /PR \[#9\]/);
});

test('a trunk-broken label on a PR, or fixes-trunk on an issue, is not a blocking item', async () => {
  const r = await gate(mergeGroup(), {
    issues: [
      { number: 9, labels: ['trunk-broken'], pr: true },
      { number: 10, labels: ['fixes-trunk'] },
    ],
    events: {
      9: [{ event: 'labeled', label: 'trunk-broken', created_at: hoursAgo(100) }],
      10: [{ event: 'labeled', label: 'fixes-trunk', created_at: hoursAgo(100) }],
    },
  });
  assert.deepEqual(r.failed, []);
});

test('age counts from the most recent time the label was added', async () => {
  const r = await gate(mergeGroup(), {
    issues: [{ number: 7, labels: ['trunk-broken'] }],
    events: {
      7: [
        { event: 'labeled', label: 'trunk-broken', created_at: hoursAgo(100) },
        { event: 'unlabeled', label: 'trunk-broken', created_at: hoursAgo(90) },
        { event: 'labeled', label: 'other', created_at: hoursAgo(2) },
        { event: 'labeled', label: 'trunk-broken', created_at: hoursAgo(5) },
      ],
    },
  });
  assert.deepEqual(r.failed, []);
  assert.match(r.summary, /for 5h/);
});

test('an item whose label event cannot be found fails closed', async () => {
  const r = await gate(mergeGroup(), { issues: [{ number: 7, labels: ['trunk-broken'] }], events: {} });
  assert.equal(r.failed.length, 1);
  assert.match(r.failed[0], /could not find when trunk-broken was added to #7/);
  assert.match(r.failed[0], /Failing closed/);
});

test('any API error fails closed in the merge queue', async () => {
  const r = await gate(mergeGroup(), { fail: { 'issues.listForRepo': new Error('API rate limit') } });
  assert.equal(r.failed.length, 1);
  assert.match(r.failed[0], /Could not determine trunk health \(API rate limit\).*Failing closed; re-run the check/);
});

test('an unparseable merge-queue ref fails closed', async () => {
  const context = mergeGroup();
  context.payload.merge_group.head_ref = 'refs/heads/some-branch';
  const r = await gate(context, {});
  assert.equal(r.failed.length, 1);
  assert.match(r.failed[0], /Could not read the PR number from merge-queue ref refs\/heads\/some-branch/);
});

test('merge-queue refs parse to the PR they were built for', () => {
  assert.equal(parseQueueRef(`refs/heads/gh-readonly-queue/trunk/pr-16584-${'a'.repeat(40)}`), 16584);
  assert.equal(parseQueueRef(`refs/heads/gh-readonly-queue/release/2.4/pr-3-${'a'.repeat(40)}`), 3);
  assert.equal(parseQueueRef('refs/heads/trunk'), null);
  assert.equal(parseQueueRef(undefined), null);
});

// --- The backstop ----------------------------------------------------------

test('two failed post-merge runs with nothing labeled block', async () => {
  const r = await gate(mergeGroup(), { runs: [{ conclusion: 'failure' }, { conclusion: 'failure' }, { conclusion: 'success' }] });
  assert.equal(r.failed.length, 1);
  assert.match(r.failed[0], /last 2 post-merge runs on trunk failed/);
  assert.match(r.failed[0], /open an issue labeled `trunk-broken`/);
});

test('one failure after a success does not trip the backstop', async () => {
  const r = await gate(mergeGroup(), { runs: [{ conclusion: 'failure' }, { conclusion: 'success' }] });
  assert.deepEqual(r.failed, []);
});

test('cancelled and skipped runs are not verdicts and are passed over', async () => {
  const tripped = await gate(mergeGroup(), {
    runs: [{ conclusion: 'cancelled' }, { conclusion: 'failure' }, { conclusion: 'skipped' }, { conclusion: 'timed_out' }],
  });
  assert.equal(tripped.failed.length, 1);

  const notTripped = await gate(mergeGroup(), {
    runs: [{ conclusion: 'failure' }, { conclusion: 'cancelled' }, { conclusion: 'success' }],
  });
  assert.deepEqual(notTripped.failed, []);
});

test('the backstop pages past a full page of cancelled runs to find its verdicts', async () => {
  const runs = [...Array(25).fill({ conclusion: 'cancelled' }), { conclusion: 'failure' }, { conclusion: 'failure' }];
  const r = await gate(mergeGroup(), { runs });
  assert.equal(r.failed.length, 1);
  assert.match(r.failed[0], /last 2 post-merge runs on trunk failed/);
  assert.deepEqual(r.calls.filter((c) => c.startsWith('actions.listWorkflowRuns page')), [
    'actions.listWorkflowRuns page 1',
    'actions.listWorkflowRuns page 2',
  ]);
});

test('the backstop stops paging once it has its verdicts', async () => {
  const runs = [{ conclusion: 'success' }, { conclusion: 'failure' }, ...Array(40).fill({ conclusion: 'failure' })];
  const r = await gate(mergeGroup(), { runs });
  assert.deepEqual(r.failed, []);
  assert.deepEqual(r.calls.filter((c) => c.startsWith('actions.listWorkflowRuns page')), [
    'actions.listWorkflowRuns page 1',
  ]);
});

test('fewer completed runs than the backstop needs do not trip it', async () => {
  const r = await gate(mergeGroup(), { runs: [{ conclusion: 'failure' }] });
  assert.deepEqual(r.failed, []);
});

test('the backstop yields once the failure is filed, starting its grace period', async () => {
  const r = await gate(mergeGroup(), { runs: [{ conclusion: 'failure' }, { conclusion: 'failure' }], ...brokenIssue(7, 1) });
  assert.deepEqual(r.failed, []);
});

test('a post-merge workflow not yet on trunk leaves the backstop inactive', async () => {
  const r = await gate(mergeGroup(), { runs: 'missing' });
  assert.deepEqual(r.failed, []);
  assert.match(r.notices[0], /nightly_tests\.yml does not exist on trunk yet/);
});

// --- Exemptions ------------------------------------------------------------

test('a PR labeled fixes-trunk may merge while blocked', async () => {
  const r = await gate(mergeGroup(), { ...brokenIssue(7, 48), pulls: { [PR]: { labels: ['fixes-trunk'] } } });
  assert.deepEqual(r.failed, []);
  assert.match(r.summary, /may merge because it is labeled `fixes-trunk`/);
});

test('a PR closing a trunk-broken issue may merge', async () => {
  const r = await gate(mergeGroup(), { ...brokenIssue(7, 48), closing: { [PR]: [{ number: 7, labels: ['trunk-broken'] }] } });
  assert.deepEqual(r.failed, []);
  assert.match(r.summary, /it closes #7, which is labeled `trunk-broken`/);
});

test('closing another repository\'s labeled issue earns nothing', async () => {
  const r = await gate(mergeGroup(), {
    ...brokenIssue(7, 48),
    closing: { [PR]: [{ number: 999, labels: ['trunk-broken'], repo: 'someone/sandbox' }] },
  });
  assert.equal(r.failed.length, 1);
});

test('a labeled closing issue past the first page of references still exempts', async () => {
  const closing = Array.from({ length: 150 }, (_, i) => ({ number: 1000 + i, labels: [] }));
  closing[120].labels = ['trunk-broken'];
  const r = await gate(mergeGroup(), { ...brokenIssue(7, 48), closing: { [PR]: closing } });
  assert.deepEqual(r.failed, []);
  assert.match(r.summary, /it closes #1120, which is labeled `trunk-broken`/);
});

test('closing an issue without the label earns nothing', async () => {
  const r = await gate(mergeGroup(), { ...brokenIssue(7, 48), closing: { [PR]: [{ number: 8, labels: ['bug'] }] } });
  assert.equal(r.failed.length, 1);
});

test('a pure revert may merge', async () => {
  const reverted = 'a'.repeat(40);
  const r = await gate(mergeGroup(), {
    ...brokenIssue(7, 48),
    commits: { [PR]: [{ sha: 'c'.repeat(40), message: `Revert "x"\n\nThis reverts commit ${reverted}.` }] },
    onBase: new Set([reverted]),
  });
  assert.deepEqual(r.failed, []);
  assert.match(r.summary, /it is a pure revert/);
});

test('a revert of a commit not on trunk is not exempt', async () => {
  const r = await gate(mergeGroup(), {
    ...brokenIssue(7, 48),
    commits: { [PR]: [{ sha: 'c'.repeat(40), message: `Revert "x"\n\nThis reverts commit ${'a'.repeat(40)}.` }] },
    onBase: new Set(),
  });
  assert.equal(r.failed.length, 1);
});

test('exemptions are not looked up when nothing blocks', async () => {
  const r = await gate(mergeGroup(), {});
  assert.ok(!r.calls.includes('pulls.get') && !r.calls.includes('graphql'), r.calls.join(', '));
});

// --- Merge groups ----------------------------------------------------------

// With HEADGREEN grouping, the entries queued ahead merge on the last entry's
// run, so every PR the group carries must be exempt.
test('a group whose earlier PR is not exempt is blocked, though this PR is', async () => {
  const r = await gate(mergeGroup(), {
    ...brokenIssue(7, 48),
    queue: [{ message: 'feat: unrelated (#41)' }, { message: 'fix: the trunk failure (#42)' }],
    pulls: { 42: { labels: ['fixes-trunk'] } },
  });
  assert.equal(r.failed.length, 1);
  assert.match(r.failed[0], /would also merge 1 PR\(s\) queued ahead of this one; each must be exempt\. Not exempt: #41\./);
});

test('a group of exempt PRs may merge, each named with its reason', async () => {
  const r = await gate(mergeGroup(), {
    ...brokenIssue(7, 48),
    queue: [{ message: 'fix: part one (#41)' }, { message: 'fix: part two (#42)' }],
    pulls: { 41: { labels: ['fixes-trunk'] }, 42: { labels: ['fixes-trunk'] } },
  });
  assert.deepEqual(r.failed, []);
  assert.match(r.summary, /every PR in this merge group may merge\n\n- #41: it is labeled `fixes-trunk`\n- #42: it is labeled `fixes-trunk`/);
});

test('a queue commit that names no PR fails closed', async () => {
  const r = await gate(mergeGroup(), { queue: [{ sha: 'd'.repeat(40), message: 'a direct commit' }] });
  assert.equal(r.failed.length, 1);
  assert.match(r.failed[0], new RegExp(`could not tell which PR merge-queue commit ${'d'.repeat(40)} belongs to`));
});

test('the PR of the queue ref is judged even when the compare lists nothing', async () => {
  const blocked = await gate(mergeGroup(), { ...brokenIssue(7, 48), queue: [] });
  assert.equal(blocked.failed.length, 1);
  assert.match(blocked.failed[0], /merging is blocked/);

  const exempt = await gate(mergeGroup(), {
    ...brokenIssue(7, 48),
    queue: [],
    pulls: { 42: { labels: ['fixes-trunk'] } },
  });
  assert.deepEqual(exempt.failed, []);
  assert.match(exempt.summary, /this PR may merge because it is labeled `fixes-trunk`/);
});

// --- Advisory pull_request runs --------------------------------------------

test('a blocked PR run passes, warns and posts one marked comment', async () => {
  const r = await gate(pullRequest(), brokenIssue(7, 48));
  assert.deepEqual(r.failed, []);
  assert.match(r.warnings[0], /merging is blocked/);
  assert.equal(r.writes.length, 1);
  assert.equal(r.writes[0].op, 'create');
  assert.ok(r.writes[0].body.startsWith(`${COMMENT_MARKER}\n### Trunk Health: merging is blocked`));
});

test('a second blocked run updates the existing comment instead of adding one', async () => {
  const r = await gate(pullRequest(), {
    ...brokenIssue(7, 48),
    comments: { [PR]: [{ id: 5, body: `${COMMENT_MARKER}\nold`, login: 'github-actions[bot]' }] },
  });
  assert.deepEqual(r.writes.map((w) => [w.op, w.comment_id]), [['update', 5]]);
});

test('a marker comment by someone else is not taken over', async () => {
  const r = await gate(pullRequest(), {
    ...brokenIssue(7, 48),
    comments: { [PR]: [{ id: 5, body: `${COMMENT_MARKER}\nquoted`, login: 'a-human' }] },
  });
  assert.deepEqual(r.writes.map((w) => w.op), ['create']);
});

test('once unblocked, the comment says so', async () => {
  const r = await gate(pullRequest(), {
    comments: { [PR]: [{ id: 5, body: `${COMMENT_MARKER}\nblocked`, login: 'github-actions[bot]' }] },
  });
  assert.equal(r.writes.length, 1);
  assert.equal(r.writes[0].op, 'update');
  assert.match(r.writes[0].body, /merging is no longer blocked/);
});

test('a PR that was never blocked gets no comment', async () => {
  const r = await gate(pullRequest(), brokenIssue(7, 2));
  assert.deepEqual(r.writes, []);
  assert.deepEqual(r.warnings, []);
});

test('a fork PR gets no comment attempt', async () => {
  const r = await gate(pullRequest({ fork: true }), brokenIssue(7, 48));
  assert.deepEqual(r.failed, []);
  assert.ok(!r.calls.includes('issues.listComments'));
  assert.match(r.summary, /merging is blocked/);
});

test('API errors only warn on a PR run', async () => {
  const r = await gate(pullRequest(), { fail: { 'issues.listForRepo': new Error('boom') } });
  assert.deepEqual(r.failed, []);
  assert.match(r.warnings[0], /Could not determine trunk health \(boom\).*advisory/);
});

test('a failing comment write only warns', async () => {
  const r = await gate(pullRequest(), { ...brokenIssue(7, 48), fail: { 'issues.createComment': new Error('403') } });
  assert.deepEqual(r.failed, []);
  assert.match(r.warnings.at(-1), /Could not update the Trunk Health comment \(403\)/);
});

test('any other event is refused', async () => {
  const r = await gate({ eventName: 'push', repo: { owner: 'o', repo: 'r' }, payload: {} }, {});
  assert.deepEqual(r.failed, ['Trunk Health runs on pull_request and merge_group, not push.']);
});

// --- The workflow ----------------------------------------------------------

test('the workflow runs the base commit\'s gate, never the PR\'s', () => {
  const workflow = readFileSync(fileURLToPath(new URL('../workflows/trunk_health.yml', import.meta.url)), 'utf8');
  assert.match(workflow, /ref: \$\{\{ github\.event\.pull_request\.base\.sha \|\| github\.event\.merge_group\.base_sha \}\}/);
  assert.match(workflow, /\/trusted\/\.github\/scripts\/trunk_health\.mjs/);
  assert.match(workflow, /persist-credentials: false/);
  assert.match(workflow, /name: Trunk Health\n/);
});

await runAll();
