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

// --- Which triggers can reach a green Attestation (#12679) ------------------
//
// Attestation is the only required quality gate on a PR, so a trigger that
// reaches the end of this job without inspecting a sign-off mints that gate for
// free. The steps above are gated on `pull_request`; the checks below assert
// that every *other* trigger declared in `on:` is either the merge queue — whose
// entry was itself gated on a green Attestation — or refused outright.

const PASSTHROUGH_STEP = 'Merge queue passthrough';

/** The event names declared in the workflow's `on:` block. */
function triggers(workflow) {
  const lines = workflow.split('\n');
  const start = lines.findIndex((line) => /^on:\s*$/.test(line));
  assert.notEqual(start, -1, 'pr.yml no longer declares an `on:` block');

  const names = [];
  for (const line of lines.slice(start + 1)) {
    // A key at column 0 ends the block.
    if (/^[A-Za-z_]/.test(line)) {
      break;
    }
    const match = /^ {2}([A-Za-z_][A-Za-z0-9_]*):/.exec(line);
    if (match) {
      names.push(match[1]);
    }
  }
  assert.ok(names.length > 0, 'could not read any trigger out of the `on:` block');
  return names;
}

/** A step's `if:` expression, or null when it is unconditional. */
function stepCondition(jobLines, stepName) {
  const line = stepLines(jobLines, stepName).find((candidate) => /^ {8}if: /.test(candidate));
  return line === undefined ? null : line.replace(/^ {8}if: /, '').trim();
}

/** Every step name declared in the job, in order. */
function stepNames(jobLines) {
  return jobLines
    .filter((line) => /^ {6}- name: /.test(line))
    .map((line) => line.replace(/^ {6}- name: /, '').trim());
}

/**
 * Whether a step's condition can hold for `eventName`.
 *
 * Only the `github.event_name` comparisons are modelled; every other term (a
 * `steps.*.outputs.*` fast-track flag) is treated as possibly true, which is
 * what makes this an over-approximation of what runs — the safe direction for
 * asking "could this trigger reach a step that reports success?".
 *
 * The conditions in this job are pure conjunctions. A `||` would make that
 * reading wrong, so it is rejected rather than guessed at.
 */
function admitsEvent(condition, eventName) {
  if (condition === null) {
    return true;
  }
  assert.doesNotMatch(
    condition,
    /\|\|/,
    `this check cannot model the disjunction in \`${condition}\`; teach it the new shape`
  );

  for (const [, operator, value] of condition.matchAll(
    /github\.event_name\s*(==|!=)\s*'([^']*)'/g
  )) {
    if (operator === '==' && value !== eventName) {
      return false;
    }
    if (operator === '!=' && value === eventName) {
      return false;
    }
  }
  return true;
}

/** The steps that could run for `eventName`. */
const admittedSteps = (eventName) =>
  stepNames(job).filter((name) => admitsEvent(stepCondition(job, name), eventName));

/** Whether a step fails the job rather than reporting success. */
const stepFails = (stepName) => /(^|\n)\s*exit 1\s*(\n|$)/.test(stepLines(job, stepName).join('\n'));

/**
 * Whether a step admitted for `eventName` is certain to run, rather than merely
 * able to.
 *
 * `admitsEvent` treats a `steps.*.outputs.*` term as possibly true, so an
 * admitted step is only *maybe* reached. That over-approximation is the wrong
 * direction for asserting that a trigger hits a step which fails the job: a
 * refusal gated on some earlier step's output would satisfy the assertion while
 * a real run skipped it and reported success. Requiring the condition to rest on
 * `github.event_name` alone closes that gap.
 */
function certainlyRuns(stepName, eventName) {
  const condition = stepCondition(job, stepName);
  if (!admitsEvent(condition, eventName)) {
    return false;
  }
  if (condition === null) {
    return true;
  }
  const residue = condition.replace(/github\.event_name\s*(?:==|!=)\s*'[^']*'/g, '');
  return !/steps\.|needs\.|inputs\.|github\./.test(residue);
}

test('the merge queue passthrough names merge_group instead of negating pull_request', () => {
  const condition = stepCondition(job, PASSTHROUGH_STEP);
  assert.match(
    condition,
    /github\.event_name\s*==\s*'merge_group'/,
    `"${PASSTHROUGH_STEP}" must name merge_group explicitly; its premise is that queue ` +
      `entry already validated the sign-off, which no other trigger provides`
  );
  assert.doesNotMatch(
    condition,
    /github\.event_name\s*!=\s*'pull_request'/,
    `"${PASSTHROUGH_STEP}" must not admit a trigger by negating pull_request`
  );
});

test('a pull request still reaches the sign-off inspection', () => {
  const admitted = admittedSteps('pull_request');
  for (const step of [REJECT_STEP, INSPECT_STEP]) {
    assert.ok(admitted.includes(step), `a pull_request must still reach "${step}"`);
  }
  assert.ok(
    !admitted.includes(PASSTHROUGH_STEP),
    `a pull_request must not reach "${PASSTHROUGH_STEP}"`
  );
});

test('the merge queue passes through without inspecting a sign-off', () => {
  const admitted = admittedSteps('merge_group');
  assert.deepEqual(
    admitted,
    [PASSTHROUGH_STEP],
    'a merge_group run must reach the passthrough and nothing else'
  );
});

// The regression guard. On a workflow_dispatch every inspecting step is gated
// out, so if the only step left standing reports success, the dispatch posts a
// green required Attestation having verified nothing.
test('no other trigger can reach a green Attestation', () => {
  const others = triggers(workflow).filter(
    (event) => event !== 'pull_request' && event !== 'merge_group'
  );
  assert.ok(others.length > 0, 'expected pr.yml to declare a trigger beyond pull_request/merge_group');

  for (const event of others) {
    const admitted = admittedSteps(event);
    assert.ok(
      admitted.length > 0,
      `a ${event} run reaches no step at all, so Attestation reports success for free`
    );
    assert.ok(
      admitted.some((step) => stepFails(step) && certainlyRuns(step, event)),
      `a ${event} run reaches only [${admitted.join(', ')}], none of which is certain to fail ` +
        `the job, so it posts a green required Attestation without inspecting any sign-off`
    );
  }
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
