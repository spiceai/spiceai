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
  ENFORCED_FROM,
  RUST_AFFECTING,
  checkEvidence,
  describeProblems,
  evidenceEntries,
  evidenceSection,
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
const AFTER_CUTOFF = '2026-12-08T00:00:00Z';
const RUN = 'https://github.com/o/r/actions/runs/42';

const TEMPLATE_COMMENT = '<!--\nList every check in scope you did not run as `Not run — <reason>`.\n## Not a heading\n-->';

/** A description with the given Evidence section text between Summary and Docs. */
const describe = (evidence) =>
  `## 📝 Summary\n\nFixes the thing.\n\n## 🧪 Evidence\n\n${TEMPLATE_COMMENT}\n\n${evidence}\n\n## 📚 Docs\n\nNone.\n`;

const ran = (sha) => `Ran at ${sha.slice(0, 12)}:\n\n\`\`\`\n$ make test\n12 passed\n\`\`\``;

const files = (...names) => names.map((filename) => ({ filename }));

/**
 * A stub of the Octokit calls checkEvidence makes. `commits` are listed oldest
 * first as { sha, parents? }; `changes` maps a commit SHA to the files it changes
 * against its first parent; `compares` maps "base...head" to { status, files }.
 */
function stubGithub({ body, commits = [{ sha: HEAD }], changes = {}, compares = {}, createdAt = '2026-11-01T00:00:00Z', fail }) {
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
          data: commits.map(({ sha, parents = [BASE] }) => ({ sha, parents: parents.map((parent) => ({ sha: parent })) })),
        }),
      },
      repos: {
        getCommit: async ({ ref, per_page: perPage, page }) => {
          reads.push(`commit ${ref} page ${page}`);
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
    },
  };
  return { github, reads };
}

const check = (state) =>
  checkEvidence({ github: stubGithub(state).github, owner: 'o', repo: 'r', pullNumber: 7, now: new Date(state.now ?? AFTER_CUTOFF) });

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

// --- Entries and not-run marks -------------------------------------------------

test('an entry needs its own output: a non-empty fenced block or a link to a run', () => {
  const resolve = (token) => [FIRST, SECOND, HEAD].find((sha) => sha.startsWith(token)) ?? null;
  const entries = evidenceEntries(
    `Before, at ${FIRST.slice(0, 9)}:\n\n\`\`\`\nFAIL\n\`\`\`\n\n` +
      `At ${SECOND.slice(0, 9)}: see https://spiceai.org/docs\n\n` +
      `At ${HEAD.slice(0, 9)}: ${RUN}\n\n\`\`\`\ncommit ${SECOND} printed by git log\n\`\`\`\n\n` +
      `Empty block at ${FIRST.slice(0, 9)}:\n\n\`\`\`\n\n\`\`\``,
    resolve
  );
  assert.deepEqual(
    entries.map(({ shas, hasOutput }) => [shas, hasOutput]),
    [[[FIRST], true], [[SECOND], false], [[HEAD], true], [[FIRST], false]],
    'a SHA inside output starts no entry, and an empty block is no output'
  );
});

test('output before the first entry belongs to it', () => {
  const entries = evidenceEntries(`\`\`\`\n12 passed\n\`\`\`\n\nAll of the above ran at ${HEAD.slice(0, 12)}.`, (token) =>
    HEAD.startsWith(token) ? HEAD : null
  );
  assert.deepEqual(entries.map(({ hasOutput }) => hasOutput), [true]);
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

test('a bare SHA with an unrelated link is not evidence', async () => {
  const result = await check({ body: describe(`Tests pass at ${HEAD.slice(0, 12)}. Docs: https://spiceai.org/docs`) });
  assert.deepEqual(result.problems, [
    `"Tests pass at ${HEAD.slice(0, 12)}. Docs: https://spiceai.org/docs" names ${HEAD.slice(0, 12)} but carries no output: ` +
      'put the output in a fenced block under it, or link the run.',
  ]);
  assert.equal(result.cited, null);
});

test('a link to the run counts as output', async () => {
  const result = await check({ body: describe(`Green at ${HEAD.slice(0, 12)}: ${RUN}`) });
  assert.deepEqual(result.problems, []);
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

test('naming the head without output does not refresh an older run', async () => {
  const result = await check({
    body: describe(`${ran(FIRST)}\n\nThe head is ${HEAD.slice(0, 12)}.`),
    commits: [{ sha: FIRST }, { sha: HEAD }],
    changes: { [HEAD]: files('crates/runtime/src/lib.rs') },
  });
  assert.deepEqual(result.problems, [
    `"The head is ${HEAD.slice(0, 12)}." names ${HEAD.slice(0, 12)} but carries no output: put the output in a fenced block under it, or link the run.`,
    `"Ran at ${FIRST.slice(0, 12)}:" ran at ${FIRST.slice(0, 12)}, but later commits change shipped code: ` +
      `${HEAD.slice(0, 12)} (crates/runtime/src/lib.rs). Re-run the same command at the head and add its output, or remove the entry.`,
  ]);
  assert.equal(result.cited, FIRST);
});

test('a later commit that changes shipped code makes the evidence stale', async () => {
  const result = await check({
    body: describe(ran(FIRST)),
    commits: [{ sha: FIRST }, { sha: SECOND }, { sha: HEAD }],
    changes: { [SECOND]: files('docs/dev/testing.md'), [HEAD]: files('crates/cayenne/src/lib.rs', 'Cargo.lock') },
  });
  assert.deepEqual(result.problems, [
    `"Ran at ${FIRST.slice(0, 12)}:" ran at ${FIRST.slice(0, 12)}, but later commits change shipped code: ` +
      `${HEAD.slice(0, 12)} (crates/cayenne/src/lib.rs, Cargo.lock). Re-run the same command at the head and add its output, or remove the entry.`,
  ]);
});

test('a rename out of shipped code counts as a change to it', async () => {
  const result = await check({
    body: describe(ran(FIRST)),
    commits: [{ sha: FIRST }, { sha: HEAD }],
    changes: { [HEAD]: [{ filename: 'notes.txt', previous_filename: 'crates/runtime/src/old.rs' }] },
  });
  assert.equal(result.problems.length, 1);
  assert.match(result.problems[0], /later commits change shipped code: 4d4d4d4d4d4d \(notes\.txt\)/);
});

test('every page of a large commit is read', async () => {
  const many = Array.from({ length: 150 }, (_, index) => ({ filename: `docs/page-${index}.md` }));
  many.push({ filename: 'crates/runtime/src/lib.rs' });
  const { github, reads } = stubGithub({ body: describe(ran(FIRST)), commits: [{ sha: FIRST }, { sha: HEAD }], changes: { [HEAD]: many } });
  const result = await checkEvidence({ github, owner: 'o', repo: 'r', pullNumber: 7 });
  assert.match(result.problems[0], /\(crates\/runtime\/src\/lib\.rs\)/);
  assert.deepEqual(reads, [`commit ${HEAD} page 1`, `commit ${HEAD} page 2`]);
});

test('a commit past the listed-file limit cannot be shown current', async () => {
  const many = Array.from({ length: 3000 }, (_, index) => ({ filename: `docs/page-${index}.md` }));
  const result = await check({ body: describe(ran(FIRST)), commits: [{ sha: FIRST }, { sha: HEAD }], changes: { [HEAD]: many } });
  assert.match(result.problems[0], /4d4d4d4d4d4d \(more files than GitHub lists, so it cannot be checked\)/);
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
  assert.match(result.problems[0], /later commits change shipped code: 3c3c3c3c3c3c \(crates\/runtime\/src\/conflict\.rs\)/);
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
    body: describe(`Before the fix, at ${FIRST.slice(0, 12)}:\n\n\`\`\`\n$ make test\n1 failed\n\`\`\`\n\nRe-ran \`make test\` at ${SECOND.slice(0, 12)}: ${RUN}`),
    commits: [{ sha: FIRST }, { sha: SECOND }],
    changes: { [SECOND]: files('crates/runtime/src/lib.rs') },
  });
  assert.deepEqual(result.problems, []);
  assert.equal(result.cited, SECOND);
});

const staleAfterHead = (line) =>
  `"${line}" ran at ${FIRST.slice(0, 12)}, but later commits change shipped code: ` +
  `${HEAD.slice(0, 12)} (crates/runtime/src/lib.rs). Re-run the same command at the head and add its output, or remove the entry.`;
const withLaterChange = { commits: [{ sha: FIRST }, { sha: HEAD }], changes: { [HEAD]: files('crates/runtime/src/lib.rs') } };

test('an older entry needs a newer run of every command it ran', async () => {
  const result = await check({
    ...withLaterChange,
    body: describe(`At ${FIRST.slice(0, 12)}:\n\n\`\`\`\n$ make test\nok\n$ make lint\nok\n\`\`\`\n\nAt ${HEAD.slice(0, 12)}:\n\n\`\`\`\n$ make lint\nok\n\`\`\``),
  });
  assert.deepEqual(result.problems, [staleAfterHead(`At ${FIRST.slice(0, 12)}:`)]);
});

test('commands compare as written: whitespace inside an argument matters', async () => {
  const result = await check({
    ...withLaterChange,
    body: describe(`At ${FIRST.slice(0, 12)}:\n\n\`\`\`\n$ grep "a  b" x\nok\n\`\`\`\n\nAt ${HEAD.slice(0, 12)}:\n\n\`\`\`\n$ grep "a b" x\nok\n\`\`\``),
  });
  assert.deepEqual(result.problems, [staleAfterHead(`At ${FIRST.slice(0, 12)}:`)]);
});

test('an entry naming two commits from the PR is ambiguous', async () => {
  const line = `Ran at ${FIRST.slice(0, 12)} (the head is now ${HEAD.slice(0, 12)}):`;
  const result = await check({ ...withLaterChange, body: describe(`${line}\n\n\`\`\`\n$ make test\nok\n\`\`\``) });
  assert.deepEqual(result.problems, [
    `"${line}" names more than one commit from this PR (${FIRST.slice(0, 12)}, ${HEAD.slice(0, 12)}): ` +
      'give each result its own entry, naming only the commit it ran at.',
  ]);
});

test('an older entry that names no command cannot be shown re-run', async () => {
  const line = `Green at ${FIRST.slice(0, 12)}: ${RUN}`;
  const result = await check({ ...withLaterChange, body: describe(`${line}\n\n${ran(HEAD)}`) });
  assert.deepEqual(result.problems, [staleAfterHead(line)]);
});

test('a newer run of a different check does not refresh an older entry', async () => {
  const result = await check({
    body: describe(
      `Engine regression at ${FIRST.slice(0, 12)}:\n\n\`\`\`\n$ cargo test -p runtime\n1 passed\n\`\`\`\n\n` +
        `Formatting at ${HEAD.slice(0, 12)}:\n\n\`\`\`\n$ cargo fmt --check\nok\n\`\`\``
    ),
    commits: [{ sha: FIRST }, { sha: HEAD }],
    changes: { [HEAD]: files('crates/runtime/src/lib.rs') },
  });
  assert.deepEqual(result.problems, [
    `"Engine regression at ${FIRST.slice(0, 12)}:" ran at ${FIRST.slice(0, 12)}, but later commits change shipped code: ` +
      `${HEAD.slice(0, 12)} (crates/runtime/src/lib.rs). Re-run the same command at the head and add its output, or remove the entry.`,
  ]);
  assert.equal(result.cited, HEAD);
});

test('the check warns until ENFORCED_FROM and then fails every PR, however old', async () => {
  assert.equal(ENFORCED_FROM, '2026-12-07T00:00:00Z');
  assert.equal((await check({ body: describe(''), now: '2026-12-06T23:59:59Z' })).enforced, false);
  // A PR opened long before the cutoff is enforced once the cutoff has passed.
  assert.equal((await check({ body: describe(''), createdAt: '2026-01-01T00:00:00Z', now: ENFORCED_FROM })).enforced, true);
});

test('past the listed-commit limit, evidence at the head still passes', async () => {
  const commits = Array.from({ length: 250 }, (_, index) => ({ sha: index.toString(16).padStart(40, '0') }));
  commits[249] = { sha: HEAD };
  assert.deepEqual((await check({ body: describe(ran(HEAD)), commits })).problems, []);
  const older = await check({ body: describe(ran(commits[10].sha)), commits });
  assert.deepEqual(older.problems, [
    `"Ran at ${commits[10].sha.slice(0, 12)}:" ran at ${commits[10].sha.slice(0, 12)}, but the PR has 250 or more commits, ` +
      `past what GitHub lists, so only evidence at the head (${HEAD.slice(0, 12)}) can be shown current.`,
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

// --- The section bots write ----------------------------------------------------

const HELPER = fileURLToPath(new URL('./bot_pr_evidence.sh', import.meta.url));
const repoHead = execFileSync('git', ['rev-parse', 'HEAD'], { encoding: 'utf8' }).trim();
const actionsEnv = {
  ...process.env,
  GITHUB_SERVER_URL: 'https://github.com',
  GITHUB_REPOSITORY: 'spiceai/spiceai',
  GITHUB_RUN_ID: '123',
};

test('the section bot_pr_evidence.sh writes passes the check', async () => {
  const section = execFileSync('bash', [HELPER, 'section', 'the snapshot update in this workflow run'], {
    encoding: 'utf8',
    env: actionsEnv,
  });
  assert.equal(
    section,
    `## 🧪 Evidence\n\nGenerated at ${repoHead.slice(0, 12)} by \`the snapshot update in this workflow run\`: ` +
      'https://github.com/spiceai/spiceai/actions/runs/123\n'
  );
  const result = await check({ body: `Automated snapshot update.\n\n${section}`, commits: [{ sha: repoHead }] });
  assert.deepEqual(result.problems, []);
});

/**
 * Run `bot_pr_evidence.sh update` against a fake `gh` that keeps the description in
 * a file. `overwrites` edits are undone right after they land, as a concurrent
 * writer working from an older read would. Returns { body, status, stderr }.
 */
function runUpdate(body, what, { sha = repoHead, overwrites = 0 } = {}) {
  const dir = mkdtempSync(join(tmpdir(), 'bot-evidence-'));
  try {
    writeFileSync(join(dir, 'body.md'), body);
    writeFileSync(join(dir, 'original.md'), body);
    writeFileSync(join(dir, 'overwrites'), String(overwrites));
    writeFileSync(
      join(dir, 'gh'),
      `#!/usr/bin/env bash\nd="${dir}"\n` +
        'if [ "$1 $2" = "pr view" ]; then cat "$d/body.md"; fi\n' +
        'if [ "$1 $2" = "pr edit" ]; then\n' +
        '  while [ $# -gt 0 ]; do [ "$1" = --body-file ] && cp "$2" "$d/body.md"; shift; done\n' +
        '  n=$(cat "$d/overwrites"); if [ "$n" -gt 0 ]; then cp "$d/original.md" "$d/body.md"; echo $((n - 1)) > "$d/overwrites"; fi\n' +
        'fi\n'
    );
    writeFileSync(join(dir, 'git'), `#!/usr/bin/env bash\necho ${sha}\n`);
    chmodSync(join(dir, 'gh'), 0o755);
    chmodSync(join(dir, 'git'), 0o755);
    const result = spawnSync('bash', [HELPER, 'update', '7', what], {
      encoding: 'utf8',
      env: { ...actionsEnv, PATH: `${dir}:${process.env.PATH}`, EVIDENCE_RECHECK_DELAY: '0' },
    });
    return { body: readFileSync(join(dir, 'body.md'), 'utf8'), status: result.status, stderr: result.stderr };
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
}
const updateBody = (body, what, sha) => {
  const { body: edited, status, stderr } = runUpdate(body, what, { sha });
  assert.equal(status, 0, stderr);
  return edited;
};

test('bot_pr_evidence.sh update puts back an entry a concurrent update dropped', () => {
  const { body, status, stderr } = runUpdate('Automated update.', 'the benchmark run for tpch on a.yaml', { overwrites: 2 });
  assert.equal(status, 0, stderr);
  assert.match(stderr, /missing from 7 after a concurrent update; adding it again\./);
  assert.match(body, /Generated at [0-9a-f]{12} by `the benchmark run for tpch on a\.yaml`/);
});

test('bot_pr_evidence.sh update fails the run when the entry cannot be kept', () => {
  const { body, status, stderr } = runUpdate('Automated update.', 'the benchmark run for tpch on a.yaml', { overwrites: 99 });
  assert.equal(status, 1);
  assert.match(stderr, /could not be kept in 7's description; add it by hand: Generated at/);
  assert.equal(body, 'Automated update.');
});

test('bot_pr_evidence.sh update adds an entry and keeps every earlier entry and section', async () => {
  const run1 = 'https://github.com/spiceai/spiceai/actions/runs/1';
  const body =
    `Automated update.\n\n## 🧪 Evidence\n\nGenerated at ${FIRST.slice(0, 12)} by \`the schema snapshot run\`: ${run1}\n\n` +
    '```\n## printed by the run, not a heading\n```\n\n# Release notes\n\nKeep me.';
  const edited = updateBody(body, 'the schema snapshot run', SECOND);
  assert.equal(
    edited,
    `Automated update.\n\n## 🧪 Evidence\n\nGenerated at ${FIRST.slice(0, 12)} by \`the schema snapshot run\`: ${run1}\n\n` +
      '```\n## printed by the run, not a heading\n```\n\n' +
      `Generated at ${SECOND.slice(0, 12)} by \`the schema snapshot run\`: https://github.com/spiceai/spiceai/actions/runs/123\n\n` +
      '# Release notes\n\nKeep me.\n'
  );
  // The older entry ran the same generation, so the newer one keeps it current.
  const result = await check({
    body: edited,
    commits: [{ sha: FIRST }, { sha: SECOND }],
    changes: { [SECOND]: files('crates/runtime/tests/snapshots/plan.snap') },
  });
  assert.deepEqual(result.problems, []);
});

test('bot_pr_evidence.sh update adds the section to a description without one', () => {
  assert.equal(
    updateBody('Automated update.', 'the benchmark run'),
    `Automated update.\n\n## 🧪 Evidence\n\nGenerated at ${repoHead.slice(0, 12)} by \`the benchmark run\`: ` +
      'https://github.com/spiceai/spiceai/actions/runs/123\n'
  );
});

await runAll();
