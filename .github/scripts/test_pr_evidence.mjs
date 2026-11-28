// Copyright 2024-2026 The Spice.ai OSS Authors
//
// Unit tests for pr_evidence.mjs, the evidence rule Attestation (pr.yml) applies to
// a pull request's description. Run: node .github/scripts/test_pr_evidence.mjs

import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';

import {
  RUST_AFFECTING,
  checkEvidence,
  describeProblems,
  evidenceSection,
  parseEvidence,
} from './pr_evidence.mjs';
import { testRunner } from './fake_github.mjs';

const { test, runAll } = testRunner();

const FIRST = '1a'.repeat(20);
const SECOND = '2b'.repeat(20);
const MERGE = '3c'.repeat(20);
const HEAD = '4d'.repeat(20);
const BASE = 'f'.repeat(40);
const INTRODUCED = '2026-12-01T00:00:00Z';

const TEMPLATE_COMMENT = '<!--\nList every check in scope you did not run as `Not run — <reason>`.\n-->';

/** A description with the given Evidence section text between Summary and Docs. */
const describe = (evidence) =>
  `## 📝 Summary\n\nFixes the thing.\n\n## 🧪 Evidence\n\n${TEMPLATE_COMMENT}\n\n${evidence}\n\n## 📚 Docs\n\nNone.\n`;

const ran = (sha) => `Ran at ${sha.slice(0, 12)}:\n\n\`\`\`\n$ make test\n12 passed\n\`\`\``;

/**
 * A stub of the Octokit calls checkEvidence makes. `commits` are listed oldest
 * first as { sha, merge? }; `files` maps a commit SHA to the paths it changes;
 * `history` is the module's commit dates on the base, newest first.
 */
function stubGithub({
  body, commits = [{ sha: HEAD }], files = {}, createdAt = '2026-12-02T00:00:00Z',
  history = [INTRODUCED], fail,
}) {
  const reads = [];
  const github = {
    paginate: async (method, params) => (await method(params)).data,
    rest: {
      pulls: {
        get: async () => {
          if (fail) throw fail;
          return { data: { body, created_at: createdAt, head: { sha: commits.at(-1).sha }, base: { sha: BASE } } };
        },
        listCommits: async () => ({
          data: commits.map(({ sha, merge }) => ({
            sha,
            parents: merge ? [{ sha: BASE }, { sha: FIRST }] : [{ sha: BASE }],
          })),
        }),
      },
      repos: {
        listCommits: async ({ path, sha }) => {
          assert.equal(path, '.github/scripts/pr_evidence.mjs');
          assert.equal(sha, BASE, 'the check took effect on the base branch, not on the PR');
          return { data: history.map((date) => ({ commit: { committer: { date } } })) };
        },
        getCommit: async ({ ref }) => {
          reads.push(ref);
          return { data: { files: (files[ref] ?? []).map((filename) => ({ filename })) } };
        },
      },
    },
  };
  return { github, reads };
}

const check = (state) => checkEvidence({ github: stubGithub(state).github, owner: 'o', repo: 'r', pullNumber: 7 });

// --- Finding the section -----------------------------------------------------

test('the section runs to the next heading at the same level, without template comments', () => {
  assert.equal(evidenceSection(describe('Ran it.')), 'Ran it.');
});

test('a deeper heading inside the section stays in it', () => {
  const body = '## Evidence\n\nRan it.\n\n### Benchmarks\n\nP99 4 ms\n\n## Docs\n\nNone.';
  assert.equal(evidenceSection(body), 'Ran it.\n\n### Benchmarks\n\nP99 4 ms');
});

test('a description without the section has none, and an untouched template is empty', () => {
  assert.equal(evidenceSection('## Summary\n\nFixes the thing.'), null);
  assert.equal(evidenceSection(null), null);
  assert.equal(evidenceSection(describe('')), '');
});

test('the parse finds SHA tokens, output, and not-run lines that lack a reason', () => {
  const parsed = parseEvidence(
    `Ran at ${HEAD.slice(0, 7).toUpperCase()}: https://github.com/o/r/actions/runs/1\n` +
      'Not run — needs an S3 bucket\nnot run: no GPU here\nThe TPC-DS bench was not run because it needs SF100\n' +
      'Integration tests: not run\n- [ ] Not run —\nFuzzing: **not run**'
  );
  assert.deepEqual(parsed.tokens, [HEAD.slice(0, 7)]);
  assert.equal(parsed.hasOutput, true);
  assert.deepEqual(parsed.notRunWithoutReason, ['Integration tests: not run', '- [ ] Not run —', 'Fuzzing: **not run**']);
});

// --- Verdicts ----------------------------------------------------------------

test('complete evidence at the head passes', async () => {
  assert.deepEqual(await check({ body: describe(ran(HEAD)) }), { problems: [], enforced: true, cited: HEAD });
});

test('a description without the section fails with that alone', async () => {
  const result = await check({ body: '## 📝 Summary\n\nFixes the thing.' });
  assert.deepEqual(result.problems, [
    'The description has no Evidence section. Add the `🧪 Evidence` section from .github/PULL_REQUEST_TEMPLATE.md.',
  ]);
});

test('an untouched template fails as empty', async () => {
  const result = await check({ body: describe('') });
  assert.deepEqual(result.problems, ['The Evidence section is empty (its template comment does not count).']);
});

test('a claim without output or a link fails', async () => {
  const result = await check({ body: describe(`Tests pass at ${HEAD.slice(0, 12)}.`) });
  assert.deepEqual(result.problems, [
    'The Evidence section has no output: paste it in a fenced code block, or link the CI run.',
  ]);
});

test('a link to the run counts as output', async () => {
  const result = await check({ body: describe(`Green at ${HEAD.slice(0, 12)}: https://github.com/o/r/actions/runs/42`) });
  assert.deepEqual(result.problems, []);
});

test('a check marked not run needs a reason', async () => {
  const result = await check({ body: describe(`${ran(HEAD)}\n\nIntegration tests: not run\nBenchmarks: Not run — no change to a shipped binary`) });
  assert.deepEqual(result.problems, [
    'A check is marked not run without a reason: "Integration tests: not run". Write `Not run — <reason>`.',
  ]);
});

test('"not run" inside pasted output is output, not a claim', async () => {
  const output = '```\n$ ./run-checks\nunit           12 passed\nintegration    not run\n```';
  const result = await check({ body: describe(`${ran(HEAD)}\n\n${output}\n\nIntegration tests: not run`) });
  assert.deepEqual(result.problems, [
    'A check is marked not run without a reason: "Integration tests: not run". Write `Not run — <reason>`.',
  ]);
});

test('a sentence that mentions "not run" is not a check marked not run', async () => {
  const prose = 'Queries that do not run on DuckDB are skipped by the oracle, as before.\n**Benchmarks**: not run.';
  const result = await check({ body: describe(`${ran(HEAD)}\n\n${prose}`) });
  assert.deepEqual(result.problems, [
    'A check is marked not run without a reason: "**Benchmarks**: not run.". Write `Not run — <reason>`.',
  ]);
});

test('citing only a commit outside the PR fails, naming the head', async () => {
  const result = await check({ body: describe(ran(BASE)) });
  assert.deepEqual(result.problems, [
    `The Evidence section names no commit from this PR. Name the commit each result ran at (the head is ${HEAD.slice(0, 12)}).`,
  ]);
  assert.equal(result.cited, null);
});

test('a later commit that changes shipped code makes the evidence stale', async () => {
  const result = await check({
    body: describe(ran(FIRST)),
    commits: [{ sha: FIRST }, { sha: SECOND }, { sha: HEAD }],
    files: { [SECOND]: ['docs/dev/testing.md'], [HEAD]: ['crates/cayenne/src/lib.rs', 'Cargo.lock'] },
  });
  assert.deepEqual(result.problems, [
    `The evidence ran at ${FIRST.slice(0, 12)}, but later commits change shipped code: ` +
      `${HEAD.slice(0, 12)} (crates/cayenne/src/lib.rs, Cargo.lock). Re-run it at the new head and update the SHA.`,
  ]);
  assert.equal(result.cited, FIRST);
});

test('later commits that change no shipped code, and base merges, keep the evidence current', async () => {
  const { github, reads } = stubGithub({
    body: describe(ran(FIRST)),
    commits: [{ sha: FIRST }, { sha: SECOND }, { sha: MERGE, merge: true }],
    files: { [SECOND]: ['docs/dev/testing.md'], [MERGE]: ['crates/runtime/src/lib.rs'] },
  });
  const result = await checkEvidence({ github, owner: 'o', repo: 'r', pullNumber: 7 });
  assert.deepEqual(result.problems, []);
  assert.deepEqual(reads, [SECOND], 'a merge commit is not read: it brings in the base, not new work');
});

test('the newest cited commit decides what is later', async () => {
  const result = await check({
    body: describe(`${ran(FIRST)}\n\nRe-ran at ${SECOND.slice(0, 12)}, same output.`),
    commits: [{ sha: FIRST }, { sha: SECOND }],
    files: { [SECOND]: ['crates/runtime/src/lib.rs'] },
  });
  assert.deepEqual(result.problems, []);
  assert.equal(result.cited, SECOND);
});

test('a PR opened before the check took effect is not enforced', async () => {
  const before = await check({ body: describe(''), createdAt: '2026-11-30T23:59:59Z' });
  assert.equal(before.enforced, false);
  assert.equal(before.problems.length, 1);
  // Newest first: the oldest entry is when the module reached the base.
  const after = await check({ body: describe(''), createdAt: '2026-12-01T00:00:00Z', history: ['2027-01-05T00:00:00Z', INTRODUCED] });
  assert.equal(after.enforced, true);
});

test('a PR past the listed-commit limit fails rather than guess', async () => {
  const commits = Array.from({ length: 250 }, (_, index) => ({ sha: index.toString(16).padStart(40, '0') }));
  const result = await check({ body: describe(ran(HEAD)), commits });
  assert.deepEqual(result.problems, [
    'The PR has 250 or more commits, past what GitHub lists, so the evidence cannot be matched to them.',
  ]);
});

test('an API error propagates instead of passing', async () => {
  await assert.rejects(check({ body: describe(ran(HEAD)), fail: new Error('HTTP 502') }), /HTTP 502/);
});

test('the reported message lists each problem and how the check re-runs', () => {
  assert.equal(
    describeProblems(['one', 'two']),
    "The PR description's Evidence section is incomplete:\n- one\n- two\n" +
      'Every claim needs the commit it ran at, the command, and its output (AGENTS.md, Evidence). ' +
      'Editing the description re-runs this check.'
  );
});

// --- One rule for "shipped code" ----------------------------------------------

test('shipped code means what the no-Rust fast-track in pr.yml says it means', () => {
  const workflow = readFileSync(fileURLToPath(new URL('../workflows/pr.yml', import.meta.url)), 'utf8');
  const match = /^\s*const rustAffecting = \/(.*)\/;\s*$/m.exec(workflow);
  assert.ok(match, 'pr.yml no longer declares `const rustAffecting = /.../;`');
  assert.equal(
    RUST_AFFECTING.source,
    match[1],
    'RUST_AFFECTING in pr_evidence.mjs differs from rustAffecting in pr.yml; copy the pattern across'
  );
});

await runAll();
