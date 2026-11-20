// Copyright 2024-2026 The Spice.ai OSS Authors
//
// Unit tests for pure_revert.mjs, the rule Attestation (pr.yml) and Trunk
// Health share. Run: node .github/scripts/test_pure_revert.mjs

import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';

import { checkPureRevert, revertedShas } from './pure_revert.mjs';
import { fakeGithub, testRunner } from './fake_github.mjs';

const A = 'a'.repeat(40);
const B = 'b'.repeat(40);
const C = 'c'.repeat(40);
const BASE = 'f'.repeat(40);
const revertOf = (sha) => `Revert "something"\n\nThis reverts commit ${sha}.\n`;

const { test, runAll } = testRunner();

const check = (state) =>
  checkPureRevert({ github: fakeGithub(state).github, owner: 'o', repo: 'r', pullNumber: 7, baseSha: BASE });

test('one revert of a commit on the base is pure', async () => {
  const result = await check({ commits: { 7: [{ sha: C, message: revertOf(A) }] }, onBase: new Set([A]) });
  assert.deepEqual(result, {
    pure: true,
    reason: 'every one of 1 commit(s) reverts a commit on the base branch',
    commitCount: 1,
    revertedShas: [A],
  });
});

test('a --no-commit batch with several footers is pure when all are on the base', async () => {
  const message = `Revert two\n\nThis reverts commit ${A}.\nThis reverts commit ${B}.\n`;
  const result = await check({ commits: { 7: [{ sha: C, message }] }, onBase: new Set([A, B]) });
  assert.equal(result.pure, true);
  assert.deepEqual(result.revertedShas, [A, B]);
});

test('a commit without a revert footer makes the PR not pure', async () => {
  const result = await check({
    commits: { 7: [{ sha: C, message: revertOf(A) }, { sha: B, message: 'fix: follow-up' }] },
    onBase: new Set([A]),
  });
  assert.equal(result.pure, false);
  assert.equal(result.reason, `commit ${B} is not a Git revert (no "This reverts commit ..." footer)`);
});

test('reverting a commit that is not on the base is not pure', async () => {
  const result = await check({ commits: { 7: [{ sha: C, message: revertOf(A) }] }, onBase: new Set() });
  assert.equal(result.pure, false);
  assert.equal(result.reason, `reverted commit ${A} is not on the base branch`);
});

test('a PR with no commits is not pure', async () => {
  const result = await check({ commits: { 7: [] } });
  assert.equal(result.pure, false);
  assert.equal(result.reason, 'the PR introduces no commits');
});

test('an API error makes the answer "not pure", never a throw', async () => {
  const listFails = await check({ fail: { 'pulls.listCommits': new Error('boom') } });
  assert.equal(listFails.pure, false);
  assert.equal(listFails.reason, "could not list the PR's commits (boom)");

  const compareFails = await check({
    commits: { 7: [{ sha: C, message: revertOf(A) }] },
    fail: { 'repos.compareCommitsWithBasehead': new Error('down') },
  });
  assert.equal(compareFails.pure, false);
  assert.equal(compareFails.reason, `could not confirm reverted commit ${A} is on the base branch (down)`);
});

test('the footer must be the whole line, with a full SHA', () => {
  assert.deepEqual(revertedShas(`This reverts commit ${A.slice(0, 12)}.`), []);
  assert.deepEqual(revertedShas(`Mentions: This reverts commit ${A}.`), []);
  assert.deepEqual(revertedShas(`This reverts commit ${A}.`), [A]);
  assert.deepEqual(revertedShas(undefined), []);
});

// Attestation and Trunk Health must judge with the base commit's copy of this
// rule, so a PR cannot change the rule that judges it.
test('Attestation imports the rule from the base-commit checkout', () => {
  const workflow = readFileSync(fileURLToPath(new URL('../workflows/pr.yml', import.meta.url)), 'utf8');
  assert.match(workflow, /\/trusted\/\.github\/scripts\/pure_revert\.mjs/);
  const checkout = workflow.slice(workflow.indexOf("- name: Check out the base commit's scripts"));
  assert.match(checkout.slice(0, 600), /ref: \$\{\{ github\.event\.pull_request\.base\.sha \}\}/);
  assert.match(checkout.slice(0, 600), /persist-credentials: false/);
  assert.ok(
    workflow.indexOf("- name: Check out the base commit's scripts") < workflow.indexOf('- name: Fast-track pure reverts'),
    'the checkout must come before the step that imports from it'
  );
});

await runAll();
