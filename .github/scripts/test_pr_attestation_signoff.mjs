// Copyright 2024-2026 The Spice.ai OSS Authors
//
// Unit tests for the sign-off logic the Attestation job runs in
// `.github/workflows/pr.yml`.
//
// Attestation is the required check that lets a PR into the merge queue, and its
// logic lives in `actions/github-script` blocks inside the workflow rather than
// in a checked-in module: the job deliberately does not check the repository out
// before running them, because a `pull_request` checkout would put fork-authored
// code in the job that decides whether that fork's PR is signed off.
//
// So these tests read the workflow, lift each step's `script:` block out of it,
// and run the real text against a stub `github`/`core`/`context`. Nothing is
// copied here — a change to the workflow changes what is under test.

import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';

const WORKFLOW = fileURLToPath(new URL('../workflows/pr.yml', import.meta.url));
const REJECT_STEP = 'Reject a failed sign-off on the head commit';
const INSPECT_STEP = 'Inspect developer sign-off';
const QUEUE_PASSTHROUGH_STEP = 'Merge queue passthrough';
const DISPATCH_REJECT_STEP = 'Reject an attestation asserted by dispatch';

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
async function runScript({ script, statuses = {}, commits = {}, headSha = HEAD, baseSha = BASE }) {
  const { calls, core } = stubCore();
  const reads = [];
  const github = {
    rest: {
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
    payload: { pull_request: { head: { sha: headSha }, base: { sha: baseSha } } },
  };

  const AsyncFunction = Object.getPrototypeOf(async () => {}).constructor;
  await new AsyncFunction('github', 'context', 'core', script)(github, context, core);
  return { ...calls, reads };
}

const workflow = readFileSync(WORKFLOW, 'utf8');
const job = attestationJob(workflow);
const rejectScript = stepScript(job, REJECT_STEP);
const inspectScript = stepScript(job, INSPECT_STEP);

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
