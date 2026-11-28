// Copyright 2024-2026 The Spice.ai OSS Authors
//
// Unit tests for pr_evidence.mjs, the evidence rule Attestation (pr.yml) applies to
// a pull request's description, and for bot_pr_evidence.sh, which writes the
// section for PRs a workflow opens. Run: node .github/scripts/test_pr_evidence.mjs

import assert from 'node:assert/strict';
import { execFileSync, spawnSync } from 'node:child_process';
import { chmodSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';

import {
  EVIDENCE_GATE,
  RUST_AFFECTING,
  checkEvidence,
  describeProblems,
  evidenceEntries,
  evidenceSection,
  evidenceSections,
  notRunWithoutReason,
} from './pr_evidence.mjs';
import { testRunner } from './fake_github.mjs';

const { test, runAll } = testRunner();

const FIRST = '1a'.repeat(20);
const SECOND = '2b'.repeat(20);
const MERGE = '3c'.repeat(20);
const HEAD = '4d'.repeat(20);
const BASE = 'f'.repeat(40);
const ON_BASE = 'e'.repeat(40);
const ELSEWHERE = '9'.repeat(40);
// Finished runs: 42 at HEAD, 7 at FIRST, 9 at the base. Run 5 is still queued at HEAD.
const RUN = 'https://github.com/o/r/actions/runs/42';
const done = (headSha) => ({ head_sha: headSha, status: 'completed', conclusion: 'success' });
const RUN_AT = { 42: done(HEAD), 7: done(FIRST), 9: done(BASE), 5: { head_sha: HEAD, status: 'queued', conclusion: null } };

const TEMPLATE_COMMENT = '<!--\nList every check in scope you did not run as `Not run — <reason>`.\n## Not a heading\n-->';

/** A description with the given Evidence section text between Summary and Docs. */
const describe = (evidence, docs = 'None.') =>
  `## 📝 Summary\n\nFixes the thing.\n\n## 🧪 Evidence\n\n${TEMPLATE_COMMENT}\n\n${evidence}\n\n## 📚 Docs\n\n${docs}\n`;

const ran = (sha, command = 'make test') => `Ran at ${sha.slice(0, 12)}:\n\n\`\`\`\n$ ${command}\n12 passed\n\`\`\``;

const files = (...names) => names.map((filename) => ({ filename }));

/**
 * A stub of the Octokit calls checkEvidence makes. `commits` are listed oldest
 * first as { sha, parents? }; `changes` maps a commit SHA to the files it changes
 * against its first parent (or null for a response without a file list);
 * `compares` maps "base...head" to { status, files }; `runs` maps a run id to the
 * commit it ran at.
 */
function stubGithub({ body, commits = [{ sha: HEAD }], changes = {}, compares = {}, runs = RUN_AT, fail }) {
  const reads = [];
  const github = {
    paginate: async (method, params) => (await method(params)).data,
    rest: {
      pulls: {
        get: async () => {
          if (fail) throw fail;
          return { data: { body, head: { sha: commits.at(-1).sha }, base: { sha: BASE } } };
        },
        listCommits: async () => ({
          data: commits.map(({ sha, parents = [BASE] }) => ({ sha, parents: parents.map((parent) => ({ sha: parent })) })),
        }),
      },
      repos: {
        getCommit: async ({ ref, per_page: perPage, page }) => {
          reads.push(`commit ${ref} page ${page}`);
          if (changes[ref] === null) return { data: {} };
          const all = changes[ref] ?? [];
          return { data: { files: all.slice((page - 1) * perPage, page * perPage) } };
        },
        compareCommitsWithBasehead: async ({ basehead }) => {
          reads.push(`compare ${basehead}`);
          const found = compares[basehead];
          assert.ok(found, `the test did not define the comparison ${basehead}`);
          return { data: found };
        },
      },
      actions: {
        getWorkflowRun: async ({ owner, repo, run_id: runId }) => {
          assert.equal(`${owner}/${repo}`, 'o/r', 'only this repository is asked about runs');
          reads.push(`run ${runId}`);
          return { data: runs[runId] };
        },
      },
    },
  };
  return { github, reads };
}

const check = (state) => checkEvidence({ github: stubGithub(state).github, owner: 'o', repo: 'r', pullNumber: 7 });

const stale = (line, at, rest) =>
  `"${line}" ran at ${at.slice(0, 12)}, but later commits change shipped code or test inputs: ${rest}. ` +
  'Re-run the same command at the head and add its output, or remove the entry.';

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

test('heading-shaped lines in pasted output do not end the section', () => {
  const output = '```\n# run the suite\n## Results\n12 passed\n```';
  assert.equal(evidenceSection(describe(`${output}\nRan at abcdef123456.`)), `${output}\nRan at abcdef123456.`);
});

test('a heading that is just "Evidence" wins over one that mentions the word', () => {
  const body = `## Summary\n\n### Why evidence is out of scope\n\nfoo\n\n## 🧪 Evidence\n\n${ran(HEAD)}\n\n## Docs`;
  assert.equal(evidenceSection(body), ran(HEAD));
});

test('every exact Evidence heading counts', () => {
  assert.equal(evidenceSections(`## Evidence\n\nstub\n\n## 🧪 Evidence\n\nreal\n\n## Docs`).count, 2);
});

// --- Entries and not-run marks -------------------------------------------------

test('entries record fenced output, this repository\'s run links, and commands from pasted output', () => {
  const resolve = (token) => [FIRST, SECOND, HEAD].find((sha) => sha.startsWith(token)) ?? null;
  const entries = evidenceEntries(
    `Before, at ${FIRST.slice(0, 9)}:\n\n\`\`\`\n$ make test\nFAIL\n\`\`\`\n\n` +
      `At ${SECOND.slice(0, 9)}, re-ran \`make lint\`: https://github.com/other/repo/actions/runs/5 https://github.com/o/r/pull/7/checks\n\n` +
      `At ${HEAD.slice(0, 9)}: ${RUN}/job/99\n\n\`\`\`\ncommit ${SECOND} printed by git log\n\`\`\`\n\n` +
      `Empty block at ${FIRST.slice(0, 9)}:\n\n\`\`\`\n\n\`\`\``,
    resolve,
    'o/r'
  );
  assert.deepEqual(
    entries.map(({ shas, fenced, runs, commands }) => [shas, fenced, runs, [...commands]]),
    [
      [[FIRST], true, [], ['make test']],
      [[SECOND], false, [], []],
      [[HEAD], true, ['42'], []],
      [[FIRST], false, [], []],
    ],
    'only this repository\'s runs count, code spans are not commands, a SHA inside output starts no entry, and an empty block is no output'
  );
});

test('output before the first entry belongs to it', () => {
  const entries = evidenceEntries(`\`\`\`\n$ make test\n12 passed\n\`\`\`\n\nAll of the above ran at ${HEAD.slice(0, 12)}.`, (token) =>
    HEAD.startsWith(token) ? HEAD : null, 'o/r'
  );
  assert.deepEqual(entries.map(({ fenced, commands }) => [fenced, [...commands]]), [[true, ['make test']]]);
});

test('not-run marks need a real reason; prose and pasted output are not marks', () => {
  assert.deepEqual(
    notRunWithoutReason(
      'Not run — needs an S3 bucket\nnot run: no GPU here\nThe TPC-DS bench was not run because it needs SF100\n' +
        'Queries that do not run on DuckDB are skipped.\n| Bench | not run | needs SF100 |\n' +
        'Integration tests: not run\n- [ ] Not run —\nFuzzing: **not run**\n| S3 integration | not run |\n' +
        '- S3: Not run — <reason>\n- Docs build: not run: TODO\n```\nintegration    not run\n```'
    ),
    [
      'Integration tests: not run',
      '- [ ] Not run —',
      'Fuzzing: **not run**',
      '| S3 integration | not run |',
      '- S3: Not run — <reason>',
      '- Docs build: not run: TODO',
    ]
  );
});

// --- Verdicts ----------------------------------------------------------------

test('complete evidence at the head passes', async () => {
  assert.deepEqual(await check({ body: describe(ran(HEAD)) }), { problems: [], cited: HEAD });
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

test('a second Evidence section fails, so a stub cannot stand in for the real one', async () => {
  const body = `## Evidence\n\nGreen at ${HEAD.slice(0, 12)}: ${RUN}\n\n## 🧪 Evidence\n\nIntegration tests: not run\n`;
  const result = await check({ body });
  assert.deepEqual(result.problems, [
    'A check is marked not run without a reason: "Integration tests: not run". Write `Not run — <reason>`.',
    'The description has 2 Evidence sections. Keep all the evidence in one.',
  ]);
});

test('a check marked not run anywhere in the description needs a reason', async () => {
  const result = await check({ body: describe(ran(HEAD), 'Docs build: not run') });
  assert.deepEqual(result.problems, [
    'A check is marked not run without a reason: "Docs build: not run". Write `Not run — <reason>`.',
  ]);
});

test('a bare SHA with an unrelated link is not evidence', async () => {
  const line = `Tests pass at ${HEAD.slice(0, 12)}. Docs: https://spiceai.org/docs`;
  const result = await check({ body: describe(line) });
  assert.deepEqual(result.problems, [
    `"${line}" names ${HEAD.slice(0, 12)} but carries no output: put the output in a fenced block under it, ` +
      `or link a finished run of this repository's workflows that ran at ${HEAD.slice(0, 12)}.`,
  ]);
  assert.equal(result.cited, null);
});

test('a run of this repository that ran at the entry\'s commit counts as output', async () => {
  const { github, reads } = stubGithub({ body: describe(`Green at ${HEAD.slice(0, 12)}: ${RUN}`) });
  const result = await checkEvidence({ github, owner: 'o', repo: 'r', pullNumber: 7 });
  assert.deepEqual(result.problems, []);
  assert.deepEqual(reads, ['run 42']);
});

test('a run at another commit, a queued run, another repository\'s run, a checks page, a gist, or an upload is not output', async () => {
  for (const link of [
    'https://github.com/o/r/actions/runs/9',
    'https://github.com/o/r/actions/runs/5',
    'https://github.com/other/repo/actions/runs/42',
    'https://github.com/o/r/pull/7/checks',
    'https://gist.github.com/someone/abc123',
    'https://github.com/user-attachments/files/1/log.txt',
  ]) {
    // eslint-disable-next-line no-await-in-loop
    const result = await check({ body: describe(`Green at ${HEAD.slice(0, 12)}: ${link}`) });
    assert.equal(result.problems.length, 1, link);
    assert.match(result.problems[0], /carries no output/, link);
  }
});

test('a check marked not run needs a reason', async () => {
  const result = await check({ body: describe(`${ran(HEAD)}\n\nIntegration tests: not run\nBenchmarks: Not run — no change to a shipped binary`) });
  assert.deepEqual(result.problems, [
    'A check is marked not run without a reason: "Integration tests: not run". Write `Not run — <reason>`.',
  ]);
});

test('citing only a commit outside the PR fails, naming the head', async () => {
  const result = await check({ body: describe(ran(BASE)) });
  assert.deepEqual(result.problems, [
    `The Evidence section names no commit from this PR outside its output. Name the commit each result ran at (the head is ${HEAD.slice(0, 12)}).`,
  ]);
});

const withLaterChange = { commits: [{ sha: FIRST }, { sha: HEAD }], changes: { [HEAD]: files('crates/runtime/src/lib.rs') } };
const afterHead = (line) => stale(line, FIRST, `${HEAD.slice(0, 12)} (crates/runtime/src/lib.rs)`);

test('naming the head without output does not refresh an older run', async () => {
  const result = await check({ ...withLaterChange, body: describe(`${ran(FIRST)}\n\nThe head is ${HEAD.slice(0, 12)}.`) });
  assert.deepEqual(result.problems, [
    `"The head is ${HEAD.slice(0, 12)}." names ${HEAD.slice(0, 12)} but carries no output: put the output in a fenced block under it, ` +
      `or link a finished run of this repository's workflows that ran at ${HEAD.slice(0, 12)}.`,
    afterHead(`Ran at ${FIRST.slice(0, 12)}:`),
  ]);
  assert.equal(result.cited, FIRST);
});

test('a later commit that changes shipped code makes the evidence stale', async () => {
  const result = await check({
    body: describe(ran(FIRST)),
    commits: [{ sha: FIRST }, { sha: SECOND }, { sha: HEAD }],
    changes: { [SECOND]: files('docs/dev/testing.md'), [HEAD]: files('crates/cayenne/src/lib.rs', 'Cargo.lock') },
  });
  assert.deepEqual(result.problems, [stale(`Ran at ${FIRST.slice(0, 12)}:`, FIRST, `${HEAD.slice(0, 12)} (crates/cayenne/src/lib.rs, Cargo.lock)`)]);
});

test('a later change to test inputs makes the evidence stale too', async () => {
  const result = await check({
    body: describe(ran(FIRST)),
    commits: [{ sha: FIRST }, { sha: HEAD }],
    changes: { [HEAD]: files('test/spicescript/feed-chaos.yaml') },
  });
  assert.deepEqual(result.problems, [stale(`Ran at ${FIRST.slice(0, 12)}:`, FIRST, `${HEAD.slice(0, 12)} (test/spicescript/feed-chaos.yaml)`)]);
});

test('a later change to the evidence check itself makes the evidence stale', async () => {
  for (const file of ['.github/scripts/pr_evidence.mjs', '.github/workflows/pr.yml', '.github/evals/evidence-rule/score_eval.py', 'AGENTS.md']) {
    // eslint-disable-next-line no-await-in-loop
    const result = await check({ body: describe(ran(FIRST)), commits: [{ sha: FIRST }, { sha: HEAD }], changes: { [HEAD]: files(file) } });
    assert.deepEqual(result.problems, [stale(`Ran at ${FIRST.slice(0, 12)}:`, FIRST, `${HEAD.slice(0, 12)} (${file})`)], file);
  }
});

test('a fenced command with no output is not output, and is no re-run of it', async () => {
  const line = `At ${HEAD.slice(0, 12)}:`;
  const result = await check({ ...withLaterChange, body: describe(`${ran(FIRST)}\n\n${line}\n\n\`\`\`\n$ make test\n\`\`\``) });
  assert.deepEqual(result.problems, [
    `"${line}" names ${HEAD.slice(0, 12)} but carries no output: put the output in a fenced block under it, ` +
      `or link a finished run of this repository's workflows that ran at ${HEAD.slice(0, 12)}.`,
    afterHead(`Ran at ${FIRST.slice(0, 12)}:`),
  ]);
});

test('a fence closes only on its own character: a literal ~~~ inside a backtick block hides nothing', async () => {
  const body = `## Evidence\n\nRan at ${HEAD.slice(0, 12)}:\n\n\`\`\`\n~~~\n12 passed\n\`\`\`\n\n## 🧪 Evidence\n\nS3: Not run\n`;
  const result = await check({ body });
  assert.deepEqual(result.problems, [
    'A check is marked not run without a reason: "S3: Not run". Write `Not run — <reason>`.',
    'The description has 2 Evidence sections. Keep all the evidence in one.',
  ]);
});

test('a rename out of shipped code counts as a change to it', async () => {
  const result = await check({
    body: describe(ran(FIRST)),
    commits: [{ sha: FIRST }, { sha: HEAD }],
    changes: { [HEAD]: [{ filename: 'notes.txt', previous_filename: 'crates/runtime/src/old.rs' }] },
  });
  assert.equal(result.problems.length, 1);
  assert.match(result.problems[0], /later commits change shipped code or test inputs: 4d4d4d4d4d4d \(notes\.txt\)/);
});

test('every page of a large commit is read', async () => {
  const many = Array.from({ length: 150 }, (_, index) => ({ filename: `docs/page-${index}.md` }));
  many.push({ filename: 'crates/runtime/src/lib.rs' });
  const { github, reads } = stubGithub({ body: describe(ran(FIRST)), commits: [{ sha: FIRST }, { sha: HEAD }], changes: { [HEAD]: many } });
  const result = await checkEvidence({ github, owner: 'o', repo: 'r', pullNumber: 7 });
  assert.match(result.problems[0], /\(crates\/runtime\/src\/lib\.rs\)/);
  assert.deepEqual(reads, [`commit ${HEAD} page 1`, `commit ${HEAD} page 2`]);
});

test('a commit past the listed-file limit, or listed without files, cannot be shown current', async () => {
  const many = Array.from({ length: 3000 }, (_, index) => ({ filename: `docs/page-${index}.md` }));
  for (const listing of [many, null]) {
    // eslint-disable-next-line no-await-in-loop
    const result = await check({ body: describe(ran(FIRST)), commits: [{ sha: FIRST }, { sha: HEAD }], changes: { [HEAD]: listing } });
    assert.match(result.problems[0], /4d4d4d4d4d4d \(more files than GitHub lists, so it cannot be checked\)/);
  }
});

test('a clean merge of the base keeps the evidence current', async () => {
  const { github, reads } = stubGithub({
    body: describe(ran(FIRST)),
    commits: [{ sha: FIRST }, { sha: MERGE, parents: [FIRST, ON_BASE] }],
    compares: {
      [`${FIRST}...${MERGE}`]: { status: 'ahead', files: files('crates/runtime/src/from_base.rs') },
      [`${ON_BASE}...${BASE}`]: { status: 'ahead', files: [] },
      [`${ON_BASE}...${MERGE}`]: { status: 'ahead', files: files('docs/dev/testing.md') },
    },
  });
  const result = await checkEvidence({ github, owner: 'o', repo: 'r', pullNumber: 7 });
  assert.deepEqual(result.problems, []);
  assert.deepEqual(reads, [`compare ${FIRST}...${MERGE}`, `compare ${ON_BASE}...${BASE}`, `compare ${ON_BASE}...${MERGE}`]);
});

test('a base merge that changes a shipped file against both parents makes the evidence stale', async () => {
  const result = await check({
    body: describe(ran(FIRST)),
    commits: [{ sha: FIRST }, { sha: MERGE, parents: [FIRST, ON_BASE] }],
    compares: {
      [`${FIRST}...${MERGE}`]: { status: 'ahead', files: files('crates/runtime/src/conflict.rs', 'crates/runtime/src/from_base.rs') },
      [`${ON_BASE}...${BASE}`]: { status: 'identical', files: [] },
      [`${ON_BASE}...${MERGE}`]: { status: 'ahead', files: files('crates/runtime/src/conflict.rs') },
    },
  });
  assert.equal(result.problems.length, 1);
  assert.match(result.problems[0], /3c3c3c3c3c3c \(crates\/runtime\/src\/conflict\.rs\)/);
});

test('a merge of a branch other than the base counts everything it brings in', async () => {
  const result = await check({
    body: describe(ran(FIRST)),
    commits: [{ sha: FIRST }, { sha: MERGE, parents: [FIRST, ELSEWHERE] }],
    compares: {
      [`${FIRST}...${MERGE}`]: { status: 'ahead', files: files('crates/runtime/src/feature.rs') },
      [`${ELSEWHERE}...${BASE}`]: { status: 'diverged', files: [] },
    },
  });
  assert.match(result.problems[0], /3c3c3c3c3c3c \(crates\/runtime\/src\/feature\.rs\)/);
});

test('a newer run of the same command keeps an older entry current: before and after', async () => {
  const result = await check({
    body: describe(`Before the fix, at ${FIRST.slice(0, 12)}:\n\n\`\`\`\n$ make test\n1 failed\n\`\`\`\n\n${ran(SECOND)}`),
    commits: [{ sha: FIRST }, { sha: SECOND }],
    changes: { [SECOND]: files('crates/runtime/src/lib.rs') },
  });
  assert.deepEqual(result.problems, []);
  assert.equal(result.cited, SECOND);
});

test('a command in a code span on the entry\'s line does not count as re-run', async () => {
  const line = `Re-ran \`make test\` at ${HEAD.slice(0, 12)}: ${RUN}`;
  const result = await check({ ...withLaterChange, body: describe(`${ran(FIRST)}\n\n${line}`) });
  assert.deepEqual(result.problems, [afterHead(`Ran at ${FIRST.slice(0, 12)}:`)]);
});

test('an older entry needs a newer run of every command it ran', async () => {
  const result = await check({
    ...withLaterChange,
    body: describe(`At ${FIRST.slice(0, 12)}:\n\n\`\`\`\n$ make test\nok\n$ make lint\nok\n\`\`\`\n\n${ran(HEAD, 'make lint')}`),
  });
  assert.deepEqual(result.problems, [afterHead(`At ${FIRST.slice(0, 12)}:`)]);
});

test('commands compare as written: whitespace inside an argument matters', async () => {
  const result = await check({ ...withLaterChange, body: describe(`${ran(FIRST, 'grep "a  b" x')}\n\n${ran(HEAD, 'grep "a b" x')}`) });
  assert.deepEqual(result.problems, [afterHead(`Ran at ${FIRST.slice(0, 12)}:`)]);
});

test('an older entry that names no command cannot be shown re-run', async () => {
  const line = `Green at ${FIRST.slice(0, 12)}: https://github.com/o/r/actions/runs/7`;
  const result = await check({ ...withLaterChange, body: describe(`${line}\n\n${ran(HEAD)}`) });
  assert.deepEqual(result.problems, [afterHead(line)]);
});

test('an entry naming two commits from the PR is ambiguous', async () => {
  const line = `Ran at ${FIRST.slice(0, 12)} (the head is now ${HEAD.slice(0, 12)}):`;
  const result = await check({ ...withLaterChange, body: describe(`${line}\n\n\`\`\`\n$ make test\nok\n\`\`\``) });
  assert.deepEqual(result.problems, [
    `"${line}" names more than one commit from this PR (${FIRST.slice(0, 12)}, ${HEAD.slice(0, 12)}): ` +
      'give each result its own entry, naming only the commit it ran at.',
  ]);
});

test('a newer run of a different check does not refresh an older entry', async () => {
  const result = await check({
    ...withLaterChange,
    body: describe(`${ran(FIRST, 'cargo test -p runtime')}\n\n${ran(HEAD, 'cargo fmt --check')}`),
  });
  assert.deepEqual(result.problems, [afterHead(`Ran at ${FIRST.slice(0, 12)}:`)]);
  assert.equal(result.cited, HEAD);
});

test('past the listed-commit limit, only evidence naming just the head passes', async () => {
  const commits = Array.from({ length: 250 }, (_, index) => ({ sha: index.toString(16).padStart(40, '0') }));
  commits[249] = { sha: HEAD };
  assert.deepEqual((await check({ body: describe(ran(HEAD)), commits })).problems, []);
  const older = await check({ body: describe(ran(commits[10].sha)), commits });
  assert.deepEqual(older.problems, [
    `"Ran at ${commits[10].sha.slice(0, 12)}:" ran at ${commits[10].sha.slice(0, 12)}, but the PR has 250 or more commits, ` +
      `past what GitHub lists, so only evidence at the head (${HEAD.slice(0, 12)}) can be shown current.`,
  ]);
  // A commit GitHub left out of the list is kept, not dropped, so it cannot make an older run read as the head's.
  const omitted = 'c0ffee'.padEnd(40, '0');
  const line = `Ran at ${omitted.slice(0, 12)} (head ${HEAD.slice(0, 12)}):`;
  const mixed = await check({ body: describe(`${line}\n\n\`\`\`\n$ make test\nok\n\`\`\``), commits });
  assert.deepEqual(mixed.problems, [
    `"${line}" names ${omitted.slice(0, 12)}, which GitHub does not list for this PR (it lists 250 commits at most): ` +
      `name only the head, ${HEAD.slice(0, 12)}.`,
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

test('changes to the check are judged by the same list pr.yml uses to keep the check on', () => {
  const workflow = readFileSync(fileURLToPath(new URL('../workflows/pr.yml', import.meta.url)), 'utf8');
  const match = /^\s*const evidenceGate = \/(.*)\/;\s*$/m.exec(workflow);
  assert.ok(match, 'pr.yml no longer declares `const evidenceGate = /.../;`');
  assert.equal(EVIDENCE_GATE.source, match[1], 'EVIDENCE_GATE in pr_evidence.mjs differs from evidenceGate in pr.yml; copy the pattern across');
});

// --- The section bots write ----------------------------------------------------

const HELPER = fileURLToPath(new URL('./bot_pr_evidence.sh', import.meta.url));
const repoHead = execFileSync('git', ['rev-parse', 'HEAD'], { encoding: 'utf8' }).trim();
const actionsEnv = {
  ...process.env,
  GITHUB_SERVER_URL: 'https://github.com',
  GITHUB_REPOSITORY: 'o/r',
  GITHUB_RUN_ID: '123',
};
const botEntry = (sha, command) =>
  `Generated at ${sha.slice(0, 12)} by https://github.com/o/r/actions/runs/123:\n\n\`\`\`\n$ ${command}\ncommitted ${sha.slice(0, 12)}\n\`\`\`\n`;

test('the section bot_pr_evidence.sh writes passes the check, on its fenced output', async () => {
  const section = execFileSync('bash', [HELPER, 'section', 'testoperator run bench tpch sf1 federated/x.yaml'], {
    encoding: 'utf8',
    env: actionsEnv,
  });
  assert.equal(section, `## 🧪 Evidence\n\n${botEntry(repoHead, 'testoperator run bench tpch sf1 federated/x.yaml')}`);
  // The run ran at the commit it was dispatched on, not the generated one, so the fence is the output.
  const result = await check({ body: `Automated snapshot update.\n\n${section}`, commits: [{ sha: repoHead }], runs: { 123: done(BASE) } });
  assert.deepEqual(result.problems, []);
});

/**
 * Run `bot_pr_evidence.sh update` against a fake `gh` that keeps the description in
 * a file. `overwrites` edits are undone right after they land, as a concurrent
 * writer working from an older read would. Returns { body, status, stderr }.
 */
function runUpdate(body, command, { sha = repoHead, overwrites = 0, head = sha } = {}) {
  const dir = mkdtempSync(join(tmpdir(), 'bot-evidence-'));
  try {
    writeFileSync(join(dir, 'body.md'), body);
    writeFileSync(join(dir, 'original.md'), body);
    writeFileSync(join(dir, 'overwrites'), String(overwrites));
    writeFileSync(
      join(dir, 'gh'),
      `#!/usr/bin/env bash\nd="${dir}"\n` +
        `if [ "$1 $2" = "pr view" ]; then case "$*" in *headRefOid*) echo ${head};; *) cat "$d/body.md";; esac; fi\n` +
        'if [ "$1 $2" = "pr edit" ]; then\n' +
        '  while [ $# -gt 0 ]; do [ "$1" = --body-file ] && cp "$2" "$d/body.md"; shift; done\n' +
        '  n=$(cat "$d/overwrites"); if [ "$n" -gt 0 ]; then cp "$d/original.md" "$d/body.md"; echo $((n - 1)) > "$d/overwrites"; fi\n' +
        'fi\n'
    );
    writeFileSync(join(dir, 'git'), `#!/usr/bin/env bash\necho ${sha}\n`);
    chmodSync(join(dir, 'gh'), 0o755);
    chmodSync(join(dir, 'git'), 0o755);
    const result = spawnSync('bash', [HELPER, 'update', '7', command], {
      encoding: 'utf8',
      env: { ...actionsEnv, PATH: `${dir}:${process.env.PATH}`, EVIDENCE_RECHECK_DELAY: '0' },
    });
    return { body: readFileSync(join(dir, 'body.md'), 'utf8'), status: result.status, stderr: result.stderr };
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
}

test('bot_pr_evidence.sh update replaces the section and keeps every other part of the description', async () => {
  const command = 'testoperator run schema tpch/sf1/x.yaml';
  const body =
    `Automated update.\n\n## 🧪 Evidence\n\n${botEntry(FIRST, command)}\n` +
    '```\n## printed by an earlier run, not a heading\n```\n\n# Release notes\n\nKeep me.';
  const { body: edited, status, stderr } = runUpdate(body, command, { sha: SECOND });
  assert.equal(status, 0, stderr);
  assert.equal(edited, `Automated update.\n\n## 🧪 Evidence\n\n${botEntry(SECOND, command)}\n# Release notes\n\nKeep me.\n`);
  const result = await check({
    body: edited,
    commits: [{ sha: FIRST }, { sha: SECOND }],
    changes: { [SECOND]: files('crates/runtime/tests/snapshots/plan.snap') },
    runs: { 123: done(BASE) },
  });
  assert.deepEqual(result.problems, []);
});

test('bot_pr_evidence.sh update closes a fence only on its own character', () => {
  const command = 'testoperator run schema tpch/sf1/x.yaml';
  const body = `## 🧪 Evidence\n\nOld notes:\n\n\`\`\`\n~~~\n## printed by a run, inside the block\n\`\`\`\n\n## Notes\n\nKeep me.`;
  const { body: edited, status, stderr } = runUpdate(body, command);
  assert.equal(status, 0, stderr);
  assert.equal(edited, `## 🧪 Evidence\n\n${botEntry(repoHead, command)}\n## Notes\n\nKeep me.\n`);
});

test('bot_pr_evidence.sh update adds the section to a description without one', () => {
  const { body, status, stderr } = runUpdate('Automated update.', 'testoperator run bench tpch sf1 a.yaml');
  assert.equal(status, 0, stderr);
  assert.equal(body, `Automated update.\n\n## 🧪 Evidence\n\n${botEntry(repoHead, 'testoperator run bench tpch sf1 a.yaml')}`);
});

test('bot_pr_evidence.sh update writes again after a concurrent update replaced its entry', () => {
  const { body, status, stderr } = runUpdate('Automated update.', 'testoperator run bench tpch sf1 a.yaml', { overwrites: 2 });
  assert.equal(status, 0, stderr);
  assert.match(stderr, /missing from 7 after a concurrent update; writing it again\./);
  assert.match(body, /^Generated at [0-9a-f]{12} by https:\/\/github\.com\/o\/r\/actions\/runs\/123:$/m);
});

test('bot_pr_evidence.sh update leaves a newer run\'s evidence alone', () => {
  const command = 'testoperator run bench tpch sf1 a.yaml';
  const body = `Automated update.\n\n## 🧪 Evidence\n\n${botEntry(SECOND, command)}`;
  const { body: after, status, stderr } = runUpdate(body, command, { sha: FIRST, head: SECOND });
  assert.equal(status, 0, stderr);
  assert.match(stderr, /head is no longer 1a1a1a1a1a1a; the run that pushed the head writes its evidence\./);
  assert.equal(after, body);
});

test('bot_pr_evidence.sh update fails the run when the entry cannot be kept', () => {
  const { body, status, stderr } = runUpdate('Automated update.', 'testoperator run bench tpch sf1 a.yaml', { overwrites: 99 });
  assert.equal(status, 1);
  assert.match(stderr, /could not be kept in 7's description; add it by hand: Generated at/);
  assert.equal(body, 'Automated update.');
});

// One PR per workload is what lets the helper replace the section: a branch shared
// by workloads would mix their evidence, and one workload's commit would make
// another's entry stale. The snapshot workflows key the branch and name the
// evidence on the same workload string.
test('snapshot workflows key each branch, and its evidence, on one workload', () => {
  const read = (name) => readFileSync(fileURLToPath(new URL(`../workflows/${name}`, import.meta.url)), 'utf8');
  const bench = read('testoperator_run_bench.yml');
  assert.match(bench, /^\s*BASE_BRANCH="testoperator-snapshot-update\/\$\{BRANCH_ID\}-\$\{WORKLOAD_KEY\}"$/m);
  assert.match(bench, /WORKLOAD_KEY="\$\(printf '%s' "\$\{WORKLOAD\}" \| git hash-object --stdin/);
  assert.equal((bench.match(/bot_pr_evidence\.sh (?:update "\$\{PR_URL\}"|section) "testoperator run bench \$\{WORKLOAD\}"/g) ?? []).length, 2);
  const cluster = read('testoperator_run_cluster_bench.yml');
  assert.match(cluster, /^\s*BRANCH_NAME="testoperator-cluster-snapshot-update\/\$\{BRANCH_ID\}-\$\{slug\}-\$\{WORKLOAD_KEY\}"$/m);
  assert.match(cluster, /WORKLOAD="\$\{WORKLOAD\} queries=\$\{CB_SCENARIO_QUERY_FILE\}"/, 'scenario queries are part of the workload');
  assert.equal((cluster.match(/bot_pr_evidence\.sh (?:update "\$\{PR_URL\}"|section) "testoperator cluster bench \$\{WORKLOAD\}"/g) ?? []).length, 2);
  const schema = read('testoperator_run_schema.yml');
  assert.match(schema, /^\s*BRANCH_NAME="testoperator-snapshot-update\/\$\{SLUG\}"$/m);
  assert.equal((schema.match(/bot_pr_evidence\.sh (?:update "\$\{PR_URL\}"|section) "testoperator run schema \$\{\{ github\.event\.inputs\.spicepod_path \}\}"/g) ?? []).length, 2);
});

await runAll();
