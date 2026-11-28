// Copyright 2024-2026 The Spice.ai OSS Authors
//
// Unit tests for the sign-off logic the Attestation job runs in
// `.github/workflows/pr.yml`.
//
// Attestation is the required check that lets a PR into the merge queue, and its
// logic lives in `actions/github-script` blocks inside the workflow rather than
// in a checked-in module: the job deliberately does not check out the PR before
// running them, because a `pull_request` checkout would put fork-authored code in
// the job that decides whether that fork's PR is signed off. The modules it uses,
// the pure-revert rule and the evidence rule, come from a checkout of the base
// commit and are tested in test_pure_revert.mjs and test_pr_evidence.mjs; the
// evidence step's wiring is tested here.
//
// So these tests read the workflow, lift each step's `script:` block out of it,
// and run the real text against a stub `github`/`core`/`context`. Nothing is
// copied here — a change to the workflow changes what is under test.

import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { copyFileSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';

const WORKFLOW = fileURLToPath(new URL('../workflows/pr.yml', import.meta.url));
const REJECT_STEP = 'Reject a failed sign-off on the head commit';
const INSPECT_STEP = 'Inspect developer sign-off';
const QUEUE_PASSTHROUGH_STEP = 'Merge queue passthrough';
const DISPATCH_REJECT_STEP = 'Reject an attestation asserted by dispatch';
const EVIDENCE_STEP = "Check the pull request's evidence";
const EVIDENCE_CHECKOUT_STEP = "Check out the default branch's evidence rule";
const EVIDENCE_MODULE = fileURLToPath(new URL('./pr_evidence.mjs', import.meta.url));

const HEAD = 'a'.repeat(40);
const BASE = 'b'.repeat(40);
const OLDER_BASE = 'c'.repeat(40);
const SIGNED = 'd'.repeat(40);
const MIDDLE = 'e'.repeat(40);
const TREE = 'f'.repeat(40);

/** The `attestation:` job's lines, so a step name elsewhere in the file can't match. */
function attestationJob(workflow) {
  const lines = workflow.split('\n');
  const start = lines.indexOf('  attestation:');
  assert.notEqual(start, -1, 'pr.yml no longer declares an `attestation:` job');

  let end = lines.length;
  for (let index = start + 1; index < lines.length; index += 1) {
    // The next job key at job indentation ends this one. Comments start with
    // `#` and so are not matched.
    if (/^ {2}[A-Za-z_][A-Za-z0-9_-]*:/.test(lines[index])) {
      end = index;
      break;
    }
  }
  return lines.slice(start, end);
}

const stepMarker = (stepName) => `      - name: ${stepName}`;

/** The index of one named step within the job, asserting it is declared exactly once. */
function stepIndex(jobLines, stepName) {
  const marker = stepMarker(stepName);
  const matches = jobLines.filter((line) => line.trimEnd() === marker).length;
  assert.equal(
    matches,
    1,
    matches === 0
      ? `the attestation job has no step named "${stepName}"`
      : `"${stepName}" is declared more than once, so these tests cannot tell them apart`
  );
  return jobLines.findIndex((line) => line.trimEnd() === marker);
}

/** The lines of one named step, up to the next step. */
function stepLines(jobLines, stepName) {
  const start = stepIndex(jobLines, stepName);
  let end = jobLines.length;
  for (let index = start + 1; index < jobLines.length; index += 1) {
    if (/^ {6}- name: /.test(jobLines[index])) {
      end = index;
      break;
    }
  }
  return jobLines.slice(start, end);
}

/** Every step name in the job, in declaration order. */
function jobStepNames(jobLines) {
  return jobLines
    .filter((line) => /^ {6}- name: /.test(line))
    .map((line) => line.replace(/^ {6}- name: /, '').trimEnd());
}

/** The trigger names in the workflow's `on:` block. */
function declaredTriggers(workflow) {
  const lines = workflow.split('\n');
  const start = lines.indexOf('on:');
  assert.notEqual(start, -1, 'pr.yml no longer declares an `on:` block');

  const triggers = [];
  for (const line of lines.slice(start + 1)) {
    // A key back at column 0 ends the block.
    if (/^[A-Za-z_]/.test(line)) break;
    const match = /^ {2}([a-z_]+):/.exec(line);
    if (match) triggers.push(match[1]);
  }
  assert.ok(triggers.length > 0, 'pr.yml declares no triggers');
  return triggers;
}

/**
 * A step's `if:` expression, with the `${{ }}` wrapper and surrounding space removed.
 *
 * The gating is as load-bearing as the script bodies below: a step that runs on the
 * wrong trigger decides Attestation without inspecting anything (#12679).
 */
function stepIf(jobLines, stepName) {
  const lines = stepLines(jobLines, stepName);
  const condition = lines.find((line) => /^ *if: /.test(line));
  assert.notEqual(condition, undefined, `step "${stepName}" has no \`if:\``);
  return condition
    .replace(/^ *if: /, '')
    .trim()
    .replace(/^\$\{\{\s*/, '')
    .replace(/\s*\}\}$/, '');
}

/** The dedented body of a step's `script: |` block. */
function stepScript(jobLines, stepName) {
  const lines = stepLines(jobLines, stepName);
  const scriptIndex = lines.findIndex((line) => /^ *script: \|-?\s*$/.test(line));
  assert.notEqual(scriptIndex, -1, `step "${stepName}" has no \`script: |\` block`);

  const body = [];
  let blockIndent = null;
  for (const line of lines.slice(scriptIndex + 1)) {
    if (line.trim() === '') {
      body.push('');
      continue;
    }
    const indent = line.length - line.trimStart().length;
    if (blockIndent === null) {
      blockIndent = indent;
    } else if (indent < blockIndent) {
      break;
    }
    body.push(line.slice(blockIndent));
  }
  assert.notEqual(blockIndent, null, `step "${stepName}" has an empty \`script:\` block`);
  return body.join('\n');
}

/** Records what the script did, in place of the Actions toolkit. */
function stubCore() {
  const calls = { failed: [], notices: [], infos: [], outputs: {} };
  return {
    calls,
    // The four `core` methods the attestation job calls.
    core: {
      setFailed: (message) => calls.failed.push(message),
      notice: (message) => calls.notices.push(message),
      info: (message) => calls.infos.push(message),
      setOutput: (name, value) => {
        calls.outputs[name] = value;
      },
    },
  };
}

/**
 * Run a step's script.
 *
 * `statuses` maps a commit SHA to the `signoff` state it carries (omit for a
 * commit with no status), and `commits` maps a SHA to its parent SHAs.
 */
async function runScript({
  script, statuses = {}, commits = {}, files = [], headSha = HEAD, baseSha = BASE,
}) {
  const { calls, core } = stubCore();
  const reads = [];
  const github = {
    paginate: async () => files,
    rest: {
      pulls: { listFiles: () => {} },
      repos: {
        getCombinedStatusForRef: async ({ ref }) => {
          reads.push(ref);
          const state = statuses[ref];
          return {
            data: {
              statuses: [
                // A real response carries every context on the commit; the
                // script has to pick `signoff` out of them.
                { context: 'enforce-pull-with-spice', state: 'success' },
                ...(state === undefined
                  ? []
                  : [
                      {
                        context: 'signoff',
                        state,
                        description: `sign-off reported ${state}`,
                        creator: { login: 'claudespice' },
                      },
                    ]),
              ],
            },
          };
        },
        getCommit: async ({ ref }) => {
          const parents = commits[ref];
          assert.ok(parents, `the test did not define commit ${ref}`);
          return {
            data: { parents: parents.map((sha) => ({ sha })), commit: { tree: { sha: TREE } } },
          };
        },
      },
    },
  };
  const context = {
    repo: { owner: 'spiceai', repo: 'spiceai' },
    payload: {
      pull_request: {
        number: 1,
        head: { sha: headSha, repo: { full_name: 'spiceai/spiceai' } },
        base: { sha: baseSha, repo: { full_name: 'spiceai/spiceai' } },
      },
    },
  };

  const AsyncFunction = Object.getPrototypeOf(async () => {}).constructor;
  await new AsyncFunction('github', 'context', 'core', script)(github, context, core);
  return { ...calls, reads };
}

const workflow = readFileSync(WORKFLOW, 'utf8');
const job = attestationJob(workflow);
const rejectScript = stepScript(job, REJECT_STEP);
const inspectScript = stepScript(job, INSPECT_STEP);
const noRustScript = stepScript(job, 'Fast-track branches with no Rust-affecting files');

const tests = [];
const test = (name, body) => tests.push({ name, body });

// --- The verdict on HEAD (#12357) ------------------------------------------

test('a failed sign-off on HEAD fails Attestation', async () => {
  const result = await runScript({ script: rejectScript, statuses: { [HEAD]: 'failure' } });
  assert.equal(result.failed.length, 1);
  assert.match(result.failed[0], new RegExp(HEAD));
  assert.match(result.failed[0], /reported failure/);
  assert.match(result.failed[0], /sign-off reported failure/);
  assert.match(result.failed[0], /make signoff/);
});

test('an errored sign-off on HEAD fails Attestation', async () => {
  const result = await runScript({ script: rejectScript, statuses: { [HEAD]: 'error' } });
  assert.equal(result.failed.length, 1);
  assert.match(result.failed[0], /reported error/);
});

test('a successful sign-off on HEAD is left to the inspection step', async () => {
  const result = await runScript({ script: rejectScript, statuses: { [HEAD]: 'success' } });
  assert.deepEqual(result.failed, []);
});

// A sign-off in flight is not a verdict. Rejecting it would red every branch
// that is mid-sign-off but already has a valid sign-off to inherit.
test('a pending sign-off on HEAD does not fail Attestation', async () => {
  const result = await runScript({ script: rejectScript, statuses: { [HEAD]: 'pending' } });
  assert.deepEqual(result.failed, []);
});

test('a HEAD with no sign-off status does not fail Attestation', async () => {
  const result = await runScript({ script: rejectScript });
  assert.deepEqual(result.failed, []);
});

test('only HEAD is read; the chain is the inspection step\'s job', async () => {
  const result = await runScript({ script: rejectScript, statuses: { [HEAD]: 'failure' } });
  assert.deepEqual(result.reads, [HEAD]);
});

// The fast-track paths mean "this head needs no sign-off", not "a failed
// sign-off may be ignored", so the rejection must not be gated on them.
test('the rejection runs before, and independently of, every fast-track', () => {
  const reject = stepIndex(job, REJECT_STEP);
  for (const later of [
    'Fast-track Dependabot dependency bumps',
    'Fast-track branches with no Rust-affecting files',
    'Fast-track pure reverts',
    INSPECT_STEP,
  ]) {
    assert.ok(reject < stepIndex(job, later), `"${REJECT_STEP}" must precede "${later}"`);
  }

  const declaration = stepLines(job, REJECT_STEP).join('\n');
  assert.doesNotMatch(declaration, /fast_track/, 'the rejection must not be gated on a fast-track');
  assert.doesNotMatch(
    declaration,
    /continue-on-error/,
    'the rejection is the gate; it cannot be best-effort'
  );
});

// --- Which triggers can decide the job (#12679) -----------------------------

// The job has no job-level `if:`, so it runs for every trigger in the `on:` block and
// every step decides for itself. A step gated on the *absence* of a trigger therefore
// picks up triggers nobody weighed: `!= 'pull_request'` was written for the merge queue
// and silently covered `workflow_dispatch` too, so dispatching pr.yml posted a green
// Attestation — the only gate a PR has — having read no sign-off at all.
test('every step gates on a named event, never on the absence of one', () => {
  for (const stepName of jobStepNames(job)) {
    const condition = stepIf(job, stepName);
    assert.doesNotMatch(
      condition,
      /github\.event_name\s*!=/,
      `"${stepName}" gates on the absence of a trigger, so a trigger added to \`on:\` ` +
        'later would take this path without anyone deciding it should'
    );
    assert.match(
      condition,
      /github\.event_name\s*==\s*'[a-z_]+'/,
      `"${stepName}" does not gate on a named event`
    );
  }
});

// The passthrough asserts the sign-off was already validated. That is true of the merge
// queue, because entry into it is itself gated on a green Attestation, and it is true of
// nothing else.
test('the queue passthrough is reachable only from the merge queue', () => {
  assert.equal(stepIf(job, QUEUE_PASSTHROUGH_STEP), "github.event_name == 'merge_group'");
});

test('a dispatch is rejected rather than passed through', () => {
  assert.equal(stepIf(job, DISPATCH_REJECT_STEP), "github.event_name == 'workflow_dispatch'");

  const declaration = stepLines(job, DISPATCH_REJECT_STEP).join('\n');
  assert.match(
    declaration,
    /exit 1/,
    `"${DISPATCH_REJECT_STEP}" must fail the job; a message alone still leaves it green`
  );
  assert.doesNotMatch(
    declaration,
    /continue-on-error/,
    'a rejection that cannot fail the job is not a rejection'
  );
});

// The bug was not that `workflow_dispatch` was handled wrongly — it was that nothing
// handled it, so it inherited a path meant for something else. Any trigger added to
// `on:` without its own verdict lands in exactly that position again.
test('every declared trigger has a step that decides the job', () => {
  const conditions = jobStepNames(job).map((stepName) => stepIf(job, stepName));
  for (const trigger of declaredTriggers(workflow)) {
    assert.ok(
      conditions.some((condition) => condition.includes(`github.event_name == '${trigger}'`)),
      `pr.yml runs on "${trigger}" but no attestation step gates on it, so the job ` +
        'reports success for that trigger without reaching a verdict'
    );
  }
});

// --- Inheritance still works (regression guards) ----------------------------

test('a sign-off on HEAD needs no inheritance', async () => {
  const result = await runScript({ script: inspectScript, statuses: { [HEAD]: 'success' } });
  assert.deepEqual(result.failed, []);
  assert.deepEqual(result.outputs, {});
  assert.equal(result.notices.length, 1);
  assert.match(result.notices[0], /Signed off by claudespice/);
});

test('a clean base merge inherits the sign-off behind it', async () => {
  const result = await runScript({
    script: inspectScript,
    statuses: { [SIGNED]: 'success' },
    commits: { [HEAD]: [SIGNED, BASE] },
  });
  assert.deepEqual(result.failed, []);
  assert.equal(result.outputs.inheritance_candidate, 'true');
  assert.equal(result.outputs.signed_sha, SIGNED);
  assert.deepEqual(JSON.parse(result.outputs.merge_chain), [
    { head: HEAD, previous: SIGNED, base: BASE, tree: TREE },
  ]);
});

// An intermediate commit's status says nothing about the tree under review, so
// the walk passes it by. Only HEAD's own verdict disqualifies (see above).
test('a failed sign-off on an intermediate merge does not block inheritance', async () => {
  const result = await runScript({
    script: inspectScript,
    statuses: { [MIDDLE]: 'failure', [SIGNED]: 'success' },
    commits: { [HEAD]: [MIDDLE, BASE], [MIDDLE]: [SIGNED, OLDER_BASE] },
  });
  assert.deepEqual(result.failed, []);
  assert.equal(result.outputs.inheritance_candidate, 'true');
  assert.equal(result.outputs.signed_sha, SIGNED);
  assert.equal(JSON.parse(result.outputs.merge_chain).length, 2);
});

test('a HEAD that is not a merge of the current base cannot inherit', async () => {
  const result = await runScript({
    script: inspectScript,
    statuses: { [SIGNED]: 'success' },
    commits: { [HEAD]: [SIGNED, OLDER_BASE] },
  });
  assert.equal(result.failed.length, 1);
  assert.match(result.failed[0], /does not merge the current base commit/);
  assert.deepEqual(result.outputs, {});
});

test('an unsigned non-merge commit cannot inherit', async () => {
  const result = await runScript({
    script: inspectScript,
    statuses: { [SIGNED]: 'success' },
    commits: { [HEAD]: [SIGNED] },
  });
  assert.equal(result.failed.length, 1);
  assert.match(result.failed[0], /not a two-parent merge/);
});

for (const filename of [
  'crates/runtime/tests/fixtures/result.snap',
  'crates/runtime/tests/fixtures/input.csv',
  'bin/spice/tests/fixtures/config.yaml',
  'tools/testoperator/fixtures/results.json',
  'vendor/library/schema.proto',
  'fixtures/result.snap',
  'version.txt',
  'test/tls/spiced_cert.pem',
  'test/spicepods/tpch/sf1/accelerated/postgres[catalog][changes].yaml',
]) {
  test(`test/build input ${filename} requires sign-off`, async () => {
    const result = await runScript({ script: noRustScript, files: [{ filename }] });
    assert.deepEqual(result.outputs, {});
    assert.match(result.infos[0], /can affect Rust lint\/build\/tests/);
  });
}

test('renaming a test input out of a crate still requires sign-off', async () => {
  const result = await runScript({
    script: noRustScript,
    files: [{ filename: 'docs/fixture.csv', previous_filename: 'crates/probe/input.csv' }],
  });
  assert.deepEqual(result.outputs, {});
});

test('documentation outside source trees can still fast-track', async () => {
  const result = await runScript({
    script: noRustScript,
    files: [{ filename: 'docs/dev/example.md' }, { filename: 'README.md' }],
  });
  assert.equal(result.outputs.fast_track, 'true');
});

test('a test input no Rust source names can still fast-track', async () => {
  const result = await runScript({
    script: noRustScript,
    files: [{ filename: 'test/spicepods/chbench/accelerated/mysql-cayenne[file]-adaptive.yaml' }],
  });
  assert.equal(result.outputs.fast_track, 'true');
});

// --- The evidence check -------------------------------------------------------

// The evidence steps run for exactly the PRs that need a sign-off, plus any PR the
// no-Rust fast-track covers that changes the evidence check itself, and they run
// even when the sign-off steps failed, so one run reports both problems.
const EVIDENCE_TIP_STEP = "Resolve the default branch's tip";
const EVIDENCE_GATE =
  "!cancelled() && github.event_name == 'pull_request' && steps.dependabot.outputs.fast_track != 'true' && " +
  "(steps.no_rust.outputs.fast_track != 'true' || steps.no_rust.outputs.evidence_gate == 'true') && " +
  "steps.revert.outputs.fast_track != 'true'";

test('the evidence steps run for every PR that needs a sign-off or changes the evidence check', () => {
  for (const step of [EVIDENCE_TIP_STEP, EVIDENCE_CHECKOUT_STEP, EVIDENCE_STEP]) assert.equal(stepIf(job, step), EVIDENCE_GATE, step);
  assert.doesNotMatch(
    stepLines(job, EVIDENCE_STEP).join('\n'),
    /continue-on-error/,
    'an evidence check that cannot fail the job is not a check'
  );
});

test('a no-Rust PR that changes the evidence check is still judged by it', async () => {
  for (const filename of ['.github/scripts/pr_evidence.mjs', '.github/workflows/pr.yml', '.github/workflows/attestation_script_test.yml', '.github/workflows/agent_evals_selftest.yml', 'AGENTS.md', '.github/evals/evidence-rule/score_eval.py']) {
    // eslint-disable-next-line no-await-in-loop
    const result = await runScript({ script: noRustScript, files: [{ filename: 'docs/dev/testing.md' }, { filename }] });
    assert.equal(result.outputs.fast_track, 'true', filename);
    assert.equal(result.outputs.evidence_gate, 'true', filename);
  }
  const docsOnly = await runScript({ script: noRustScript, files: [{ filename: 'docs/dev/testing.md' }] });
  assert.equal(docsOnly.outputs.evidence_gate, undefined);
});

// `Check out pull request history` checks the PR out over the workspace root, and
// the checkout action empties a destination that is not a repository. A checker
// checked out before it would be deleted, and the PR could put its own at that path.
test('the evidence rule is checked out at the resolved tip after the PR, right before it runs', () => {
  const names = jobStepNames(job);
  const at = names.indexOf(EVIDENCE_TIP_STEP);
  assert.ok(names.indexOf('Check out pull request history') < at, 'the checker must be checked out after the PR checkout');
  assert.deepEqual(names.slice(at, at + 3), [EVIDENCE_TIP_STEP, EVIDENCE_CHECKOUT_STEP, EVIDENCE_STEP], 'nothing may run between them');
  const declaration = stepLines(job, EVIDENCE_CHECKOUT_STEP).join('\n');
  assert.match(declaration, /ref: \$\{\{ steps\.evidence_tip\.outputs\.sha \}\}/);
  assert.match(declaration, /path: evidence-rule\n/);
  assert.match(declaration, /persist-credentials: false/);
  assert.match(stepLines(job, EVIDENCE_STEP).join('\n'), /EVIDENCE_TIP: \$\{\{ steps\.evidence_tip\.outputs\.sha \}\}/);
});

test('the tip step resolves the default branch from the API', async () => {
  const outputs = {};
  const github = { rest: { repos: { getBranch: async ({ branch }) => ({ data: { commit: { sha: `${branch}-tip` } } }) } } };
  const core = { setOutput: (name, value) => { outputs[name] = value; } };
  const AsyncFunction = Object.getPrototypeOf(async () => {}).constructor;
  await new AsyncFunction('github', 'context', 'core', stepScript(job, EVIDENCE_TIP_STEP))(
    github, { repo: { owner: 'spiceai', repo: 'spiceai' }, payload: { repository: { default_branch: 'trunk' } } }, core
  );
  assert.deepEqual(outputs, { sha: 'trunk-tip' });
});

test('editing the description re-runs Attestation', () => {
  assert.match(workflow, /^ {2}pull_request:\n(?: {4}#.*\n)* {4}types: \[opened, synchronize, reopened, edited\]$/m);
});

const git = (cwd, ...args) => {
  const result = spawnSync('git', ['-C', cwd, ...args], { encoding: 'utf8' });
  assert.equal(result.status, 0, `git ${args.join(' ')} failed: ${result.stderr}`);
  return result.stdout.trim();
};

/**
 * Run the evidence step's real script against a real repository standing in for
 * the default branch's checkout. `module`: whether that commit carries
 * pr_evidence.mjs; `checkout`: whether the checkout happened; `tip`: what the tip
 * step resolved ('head' for the checkout's own commit); `gitFails`: make the tree
 * read fail; `prFiles`: the PR's changed files (or an Error to throw).
 */
async function runEvidence({ body, module = true, checkout = true, tip = 'head', gitFails = false, prFiles = [], prNumber = 1 }) {
  const workspace = mkdtempSync(join(tmpdir(), 'evidence-'));
  let resolved = tip;
  if (checkout) {
    const repo = join(workspace, 'evidence-rule');
    mkdirSync(join(repo, '.github/scripts'), { recursive: true });
    git(repo, 'init', '-q');
    if (module) copyFileSync(EVIDENCE_MODULE, join(repo, '.github/scripts/pr_evidence.mjs'));
    writeFileSync(join(repo, '.github/scripts/README'), 'default branch scripts\n');
    git(repo, 'add', '-A');
    git(repo, '-c', 'user.name=t', '-c', 'user.email=t@example.com', 'commit', '-q', '-m', 'trunk');
    if (tip === 'head') resolved = git(repo, 'rev-parse', 'HEAD');
  }
  if (resolved === 'head') resolved = 'c'.repeat(40);
  const calls = { failed: [], notices: [], infos: [] };
  const core = {
    setFailed: (message) => calls.failed.push(message),
    notice: (message) => calls.notices.push(message),
    info: (message) => calls.infos.push(message),
  };
  const exec = {
    getExecOutput: async (command, args) => {
      if (gitFails && args.includes('ls-tree')) return { exitCode: 128, stdout: '', stderr: 'fatal: unable to read tree' };
      const result = spawnSync(command, args, { encoding: 'utf8' });
      return { exitCode: result.status, stdout: result.stdout, stderr: result.stderr };
    },
  };
  const github = {
    paginate: async (method, params) => (await method(params)).data,
    rest: {
      pulls: {
        get: async () => ({ data: { body, head: { sha: HEAD }, base: { sha: BASE } } }),
        listCommits: async () => ({ data: [{ sha: HEAD, parents: [{ sha: BASE }] }] }),
        listFiles: async () => {
          if (prFiles instanceof Error) throw prFiles;
          return { data: prFiles };
        },
      },
    },
  };
  const context = {
    repo: { owner: 'spiceai', repo: 'spiceai' },
    payload: { pull_request: { number: prNumber, base: { sha: BASE } }, repository: { default_branch: 'trunk' } },
  };
  const previous = { workspace: process.env.GITHUB_WORKSPACE, tip: process.env.EVIDENCE_TIP };
  process.env.GITHUB_WORKSPACE = workspace;
  if (resolved === undefined) delete process.env.EVIDENCE_TIP;
  else process.env.EVIDENCE_TIP = resolved;
  try {
    const AsyncFunction = Object.getPrototypeOf(async () => {}).constructor;
    await new AsyncFunction('github', 'context', 'core', 'exec', stepScript(job, EVIDENCE_STEP))(github, context, core, exec);
  } finally {
    process.env.GITHUB_WORKSPACE = previous.workspace;
    if (previous.tip === undefined) delete process.env.EVIDENCE_TIP;
    else process.env.EVIDENCE_TIP = previous.tip;
    rmSync(workspace, { recursive: true, force: true });
  }
  return calls;
}

const EVIDENCE = `## Evidence\n\nRan at ${HEAD.slice(0, 12)}:\n\n\`\`\`\n$ make test\n12 passed\n\`\`\`\n`;
const unverified = (why) => `The evidence rule could not be verified as trunk's (${why}), so the evidence check could not run. Re-run the job.`;

test('complete evidence passes the step', async () => {
  const calls = await runEvidence({ body: EVIDENCE });
  assert.deepEqual(calls.failed, []);
  assert.deepEqual(calls.infos, [`The Evidence section is complete and current as of ${HEAD.slice(0, 12)}.`]);
});

test('missing evidence fails the step, with no warning period', async () => {
  const calls = await runEvidence({ body: '## Summary\n\nFixes the thing.' });
  assert.equal(calls.failed.length, 1);
  assert.match(calls.failed[0], /^The PR description's Evidence section is incomplete:\n- The description has no Evidence section\./);
});

test('an unresolved tip fails the step', async () => {
  // An output the tip step never set reaches the step as an empty string.
  const calls = await runEvidence({ body: EVIDENCE, tip: '' });
  assert.deepEqual(calls.failed, [unverified('its tip was not resolved')]);
});

test('a checkout that is not the resolved tip is never imported', async () => {
  const calls = await runEvidence({ body: '## Summary', tip: 'd'.repeat(40) });
  assert.deepEqual(calls.failed, [unverified(`the checkout is not trunk's tip ${'d'.repeat(12)}`)]);
  assert.deepEqual(calls.notices, []);
});

test('a missing checkout fails the step', async () => {
  const calls = await runEvidence({ body: EVIDENCE, checkout: false });
  assert.deepEqual(calls.failed, [unverified(`the checkout is not trunk's tip ${'c'.repeat(12)}`)]);
});

test('a git error reading the tree fails the step instead of skipping it', async () => {
  const calls = await runEvidence({ body: '## Summary', gitFails: true });
  assert.deepEqual(calls.notices, []);
  assert.deepEqual(calls.failed, [unverified('git could not read the tree: fatal: unable to read tree')]);
});

test('a tip without the module skips the check only for the PR that introduced it', async () => {
  const addsModule = [{ filename: '.github/scripts/pr_evidence.mjs', status: 'added' }];
  const bootstrap = await runEvidence({ body: '## Summary', module: false, prFiles: addsModule, prNumber: 16914 });
  assert.deepEqual(bootstrap.failed, []);
  assert.deepEqual(bootstrap.notices, [
    'This PR adds the evidence check (.github/scripts/pr_evidence.mjs), which trunk does not have yet, so it was skipped.',
  ]);
  // A later PR adding a module back gets no skip: the exemption cannot be reused.
  const readds = await runEvidence({ body: '## Summary', module: false, prFiles: addsModule, prNumber: 20000 });
  assert.deepEqual(readds.notices, []);
  assert.deepEqual(readds.failed, ['trunk has no evidence rule (.github/scripts/pr_evidence.mjs), so the evidence check could not run. Restore it on trunk.']);
  const other = await runEvidence({ body: '## Summary', module: false, prFiles: [{ filename: 'crates/a.rs', status: 'modified' }] });
  assert.deepEqual(other.notices, []);
  assert.deepEqual(other.failed, ['trunk has no evidence rule (.github/scripts/pr_evidence.mjs), so the evidence check could not run. Restore it on trunk.']);
  const unreadable = await runEvidence({ body: '## Summary', module: false, prFiles: new Error('HTTP 502') });
  assert.deepEqual(unreadable.failed, [unverified("listing the PR's files failed: HTTP 502")]);
});

let failures = 0;
for (const { name, body } of tests) {
  try {
    // eslint-disable-next-line no-await-in-loop
    await body();
    console.log(`ok   ${name}`);
  } catch (error) {
    failures += 1;
    console.error(`FAIL ${name}\n     ${error.message}`);
  }
}

console.log(`\n${tests.length - failures}/${tests.length} passed`);
process.exit(failures === 0 ? 0 : 1);
