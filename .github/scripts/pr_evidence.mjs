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

// The evidence a pull request's description must carry before Attestation
// passes it (AGENTS.md, *Evidence*). Each entry names one commit from the PR and
// carries its output: a fenced block, or a link to a run of this repository's
// workflows that ran at that commit. Every check marked not run, anywhere in the
// description, gives a reason. And every entry is current: no later commit
// changes shipped code or test inputs (a clean merge of the base aside), unless
// newer entries re-run every command it ran.
//
// Attestation (`.github/workflows/pr.yml`) imports this from a checkout of the
// default branch, never the PR's, so a PR cannot change the rule that judges it.
// It checks that evidence is present, current, and complete in form. It cannot
// check that the evidence is true; review does that.

// GitHub lists at most this many commits for a PR.
const MAX_LISTED_COMMITS = 250;
// A commit's files come 100 to a page, and GitHub returns at most 3000 of them.
const FILES_PER_PAGE = 100;
const MAX_FILE_PAGES = 30;
// A comparison returns at most this many files.
const MAX_COMPARED_FILES = 300;

// The files that need a developer sign-off: the same pattern Attestation's no-Rust
// fast-track uses in pr.yml. test_pr_evidence.mjs fails if the two copies differ.
export const RUST_AFFECTING = /(^(crates|bin|tools|vendor)\/|\.(rs|snap|proto)$|(^|\/)Cargo\.(toml|lock)$|(^|\/)rust-toolchain(\.toml)?$|(^|\/)\.cargo\/|(^|\/)\.?(clippy|rustfmt)\.toml$|(^|\/)nextest\.toml$|(^|\/)layers\.toml$|^scripts\/(test_)?check_(crate_layers|table_layers|rust_gate_paths|module_reachability|fork_patches|bench_mysql_load_nulls|cayenne_data_path_fs)\.py$|^scripts\/cayenne_data_path_fs_allowlist\.json$|^scripts\/(test_)?rust_guard_common\.py$|^scripts\/(test_)?nextest_build_targets\.py$|^docs\/dev\/fork_patches\.md$|^(Makefile|version\.txt)$|^test\/tpc-bench\/(Makefile|mysql-null-spec\.awk)$|^test\/tls\/|^test\/spicepods\/tpch\/sf1\/accelerated\/postgres\[catalog\]\[changes\]\.yaml$|^test\/source_versions\.json$|^\.schema\/spicepod\.schema\.json$)/;
// The evidence check's own files and the rules it enforces. A PR changing them is
// judged even when the no-Rust fast-track covers it (`evidenceGate` in pr.yml, a
// copy test_pr_evidence.mjs keeps equal), and a later change to them makes earlier
// evidence stale, so the check cannot be weakened under evidence from before.
export const EVIDENCE_GATE = /^(\.github\/scripts\/(pr_evidence\.mjs|bot_pr_evidence\.sh|test_pr_evidence\.mjs|test_pr_attestation_signoff\.mjs)|\.github\/workflows\/(pr|attestation_script_test|agent_evals_selftest)\.yml|\.github\/PULL_REQUEST_TEMPLATE\.md|AGENTS\.md|\.github\/evals\/.+)$/;
// What makes earlier evidence stale: shipped code, every test input (spicescript
// scenarios, Spicepods, expected rows), which can change a check's outcome without
// changing code, and the evidence check itself. Documentation does not.
const STALENESS_INPUT = (path) => RUST_AFFECTING.test(path) || /^test\//.test(path) || EVIDENCE_GATE.test(path);

const HEADING = /^(#{1,6})\s+(.*)$/;
const EXACT_EVIDENCE = /^[^a-z0-9]*evidence[^a-z0-9]*$/i;
const COMMENT = /<!--[\s\S]*?-->/g;
// GitHub-flavoured fences: an opening run of three or more backticks or tildes
// (with an info string after it), closed only by a run of the same character at
// least as long, with nothing after it.
const FENCE_OPEN = /^ {0,3}(`{3,}|~{3,})(.*)$/;
const FENCE_CLOSE = /^ {0,3}(`{3,}|~{3,})\s*$/;
const SHA_TOKEN = /\b[0-9a-f]{7,40}\b/gi;
// A check marked not run with nothing after the mark ("Tests: not run", "Not run:",
// "| S3 | not run |"), or with only a placeholder for a reason ("Not run — <reason>",
// "not run: TODO"). A real reason passes, and so does prose that merely contains
// the words ("queries that do not run on DuckDB").
const NOT_RUN_WITHOUT_REASON = /\bnot run\b[\s—–:.-]*$/i;
const NOT_RUN_PLACEHOLDER = /\bnot run\b\s*(?:[—–:-]+|because\b)\s*(?:<[^>]*>|todo|tbd|n\/a)?[\s.]*$/i;
const DECORATION = /[*_`]+/g;
const TABLE_EDGE = /\s*\|\s*$/;
// A command in pasted output ("$ make test"): what identifies a re-run of the same
// check. Compared as written, since whitespace can be part of an argument.
const PROMPT = /^\s*\$\s+(.+?)\s*$/;

/** A link to a run (or a job in a run) of this repository's workflows, by run id. */
const runLinks = (repoSlug) =>
  new RegExp(`https://github\\.com/${repoSlug.replace(/[.]/g, '\\.')}/actions/runs/(\\d+)(?:/job/\\d+)?`, 'gi');

/** Each line of a body with HTML comments removed, marked as inside a fenced block or not. */
function markedLines(text) {
  let open = null;
  return text
    .replace(/\r\n/g, '\n')
    .replace(COMMENT, '')
    .split('\n')
    .map((line) => {
      if (open === null) {
        const opening = FENCE_OPEN.exec(line);
        if (opening && !(opening[1][0] === '`' && opening[2].includes('`'))) {
          open = opening[1];
          return { line, fence: true, fenced: false };
        }
        return { line, fence: false, fenced: false };
      }
      const closing = FENCE_CLOSE.exec(line);
      if (closing && closing[1][0] === open[0] && closing[1].length >= open.length) {
        open = null;
        return { line, fence: true, fenced: false };
      }
      return { line, fence: false, fenced: true };
    });
}

/**
 * The description's Evidence sections: { sections: [text], count }. Headings count
 * only outside fenced blocks and HTML comments. Headings that are just "Evidence"
 * (as in the template's "🧪 Evidence") are used; only when there is none does a
 * heading that merely contains the word stand in. A section runs to the next
 * heading at the same or a higher level.
 */
export function evidenceSections(body) {
  const lines = markedLines(body ?? '');
  const headings = [];
  lines.forEach(({ line, fence, fenced }, index) => {
    const match = !fence && !fenced ? HEADING.exec(line) : null;
    if (match) headings.push({ index, level: match[1].length, text: match[2] });
  });
  let chosen = headings.filter((heading) => EXACT_EVIDENCE.test(heading.text));
  if (chosen.length === 0) chosen = headings.filter((heading) => /\bevidence\b/i.test(heading.text)).slice(0, 1);
  const sections = chosen.map((heading) => {
    const next = headings.find((other) => other.index > heading.index && other.level <= heading.level);
    return lines
      .slice(heading.index + 1, next ? next.index : lines.length)
      .map(({ line }) => line)
      .join('\n')
      .trim();
  });
  return { sections, count: chosen.length };
}

/** The first Evidence section's text, or null when there is none. */
export function evidenceSection(body) {
  return evidenceSections(body).sections[0] ?? null;
}

/**
 * The section's evidence entries. An entry starts at a line, outside a fenced
 * block, naming a commit `resolve` recognizes, and runs to the next such line. It
 * records whether it holds a fenced block with output (a line that is not a `$ `
 * command), the run ids of this repository's run links on its lines, and its
 * commands: each `$ ` line followed by output of its own before the next `$ ` line
 * or the end of the block. A command with no output is no run of it. Output before
 * the first entry belongs to the first entry.
 */
export function evidenceEntries(section, resolve, repoSlug) {
  const linkPattern = runLinks(repoSlug);
  const entries = [];
  let current = null;
  const pending = { fenced: false, runs: [], commands: new Set() };
  let block = null;
  const target = () => current ?? pending;
  const closeBlock = () => {
    if (block.output) target().fenced = true;
    for (const command of block.ran) target().commands.add(command);
    block = null;
  };
  for (const { line, fence, fenced } of markedLines(section)) {
    if (fence) {
      if (block) closeBlock();
      else block = { output: false, command: null, ran: [] };
      continue;
    }
    if (fenced) {
      const prompt = PROMPT.exec(line);
      if (prompt) {
        block.command = prompt[1].trim();
      } else if (line.trim()) {
        block.output = true;
        if (block.command !== null) block.ran.push(block.command);
        block.command = null;
      }
      continue;
    }
    const runs = [...line.matchAll(linkPattern)].map((match) => match[1]);
    const shas = [...new Set((line.replace(linkPattern, '').match(SHA_TOKEN) ?? []).map((token) => resolve(token.toLowerCase())).filter(Boolean))];
    if (shas.length > 0) {
      current = { shas, line: line.trim(), fenced: pending.fenced, runs: [...pending.runs, ...runs], commands: new Set(pending.commands) };
      pending.fenced = false;
      pending.runs = [];
      pending.commands.clear();
      entries.push(current);
    } else {
      target().runs.push(...runs);
    }
  }
  if (block) closeBlock();
  return entries;
}

/** Lines outside fenced blocks that mark a check not run without a real reason. */
export function notRunWithoutReason(text) {
  return markedLines(text)
    .filter(({ fence, fenced }) => !fence && !fenced)
    .map(({ line }) => line.trim())
    .filter((line) => {
      const plain = line.replace(DECORATION, '').replace(TABLE_EDGE, '');
      return NOT_RUN_WITHOUT_REASON.test(plain) || NOT_RUN_PLACEHOLDER.test(plain);
    });
}

const short = (sha) => sha.slice(0, 12);
const touchesStalenessInput = (file) =>
  STALENESS_INPUT(file.filename) || (file.previous_filename !== undefined && STALENESS_INPUT(file.previous_filename));

/** Every file a commit changes against its first parent, or null when GitHub cannot list them all. */
async function commitFiles(github, owner, repo, ref) {
  const files = [];
  for (let page = 1; page <= MAX_FILE_PAGES; page += 1) {
    // eslint-disable-next-line no-await-in-loop
    const { data } = await github.rest.repos.getCommit({ owner, repo, ref, per_page: FILES_PER_PAGE, page });
    if (!Array.isArray(data.files)) return null;
    files.push(...data.files);
    if (data.files.length < FILES_PER_PAGE) return files;
  }
  return null;
}

/** The files two commits differ in, or null when GitHub cannot list them all. */
async function comparedFiles(github, owner, repo, base, head) {
  const { data } = await github.rest.repos.compareCommitsWithBasehead({ owner, repo, basehead: `${base}...${head}` });
  const files = Array.isArray(data.files) && data.files.length < MAX_COMPARED_FILES ? data.files : null;
  return { status: data.status, files };
}

/**
 * The staleness inputs a later commit changes, or a reason it cannot be told. A
 * merge whose second parent is on the base branch counts only for files it changes
 * against both parents: files both sides touched, or edited by hand while merging.
 * Any other merge counts for everything it brings in.
 */
async function laterChanges(github, owner, repo, commit, baseSha) {
  const parents = (commit.parents ?? []).map((parent) => parent.sha);
  if (parents.length > 2) return { unknown: 'an octopus merge' };
  if (parents.length < 2) {
    const files = await commitFiles(github, owner, repo, commit.sha);
    if (files === null) return { unknown: 'more files than GitHub lists' };
    return { paths: files.filter(touchesStalenessInput).map((file) => file.filename) };
  }
  const [prSide, otherSide] = parents;
  const fromPr = await comparedFiles(github, owner, repo, prSide, commit.sha);
  if (fromPr.files === null) return { unknown: 'more files than GitHub compares' };
  const onBase = await comparedFiles(github, owner, repo, otherSide, baseSha);
  if (onBase.status !== 'ahead' && onBase.status !== 'identical') {
    return { paths: fromPr.files.filter(touchesStalenessInput).map((file) => file.filename) };
  }
  const fromBase = await comparedFiles(github, owner, repo, otherSide, commit.sha);
  if (fromBase.files === null) return { unknown: 'more files than GitHub compares' };
  const changedAgainstBase = new Set(fromBase.files.filter(touchesStalenessInput).map((file) => file.filename));
  return { paths: fromPr.files.filter(touchesStalenessInput).map((file) => file.filename).filter((name) => changedAgainstBase.has(name)) };
}

/**
 * Check a pull request's Evidence section. Reads the PR, its commits, the runs its
 * entries link, and the changes after each entry's commit.
 *
 * Returns { problems: [string], cited: sha | null }. API errors propagate: a check
 * that cannot read the PR has reached no verdict, and the caller must not pass it.
 */
export async function checkEvidence({ github, owner, repo, pullNumber }) {
  const { data: pull } = await github.rest.pulls.get({ owner, repo, pull_number: pullNumber });
  const head = pull.head.sha.toLowerCase();

  const problems = [];
  for (const line of notRunWithoutReason(pull.body ?? '')) {
    problems.push(`A check is marked not run without a reason: "${line}". Write \`Not run — <reason>\`.`);
  }
  const { sections, count } = evidenceSections(pull.body);
  if (count === 0) {
    problems.push('The description has no Evidence section. Add the `🧪 Evidence` section from .github/PULL_REQUEST_TEMPLATE.md.');
    return { problems, cited: null };
  }
  if (count > 1) {
    problems.push(`The description has ${count} Evidence sections. Keep all the evidence in one.`);
    return { problems, cited: null };
  }
  const section = sections[0];
  if (section === '') {
    problems.push('The Evidence section is empty (its template comment does not count).');
    return { problems, cited: null };
  }

  const commits = await github.paginate(github.rest.pulls.listCommits, {
    owner, repo, pull_number: pullNumber, per_page: 100,
  });
  const listed = commits.map((commit) => commit.sha.toLowerCase());
  const truncated = commits.length >= MAX_LISTED_COMMITS;
  // Past GitHub's list, a SHA-shaped token that is neither the head nor listed may
  // be a PR commit the list left out, so it is kept as an unlisted commit rather
  // than dropped: dropping it could turn "ran at <older commit>, head is <head>"
  // into evidence at the head.
  const UNLISTED = 'unlisted:';
  const resolve = (token) =>
    head.startsWith(token) ? head : listed.find((sha) => sha.startsWith(token)) ?? (truncated ? `${UNLISTED}${token}` : null);

  const entries = evidenceEntries(section, resolve, `${owner}/${repo}`);
  if (entries.length === 0) {
    problems.push(
      `The Evidence section names no commit from this PR outside its output. Name the commit each result ran at (the head is ${short(head)}).`
    );
    return { problems, cited: null };
  }

  const unlisted = (entry) => entry.shas.some((sha) => sha.startsWith(UNLISTED));
  for (const entry of entries.filter(unlisted)) {
    problems.push(
      `"${entry.line}" names ${entry.shas.filter((sha) => sha.startsWith(UNLISTED)).map((sha) => sha.slice(UNLISTED.length)).join(', ')}, ` +
        `which GitHub does not list for this PR (it lists ${MAX_LISTED_COMMITS} commits at most): name only the head, ${short(head)}.`
    );
  }
  for (const entry of entries.filter((candidate) => candidate.shas.length > 1 && !unlisted(candidate))) {
    problems.push(
      `"${entry.line}" names more than one commit from this PR (${entry.shas.map(short).join(', ')}): ` +
        'give each result its own entry, naming only the commit it ran at.'
    );
  }

  // A run link is output only for the commit the run ran at, and only for a run
  // that executed: it finished with a verdict (success or failure; a failing run is
  // evidence too) and at least one of its jobs ran steps. A run GitHub holds for
  // approval reads completed/action_required with no job, and a held run closed
  // later can read failure with no job; neither ran anything.
  const single = entries.filter((entry) => entry.shas.length === 1 && !unlisted(entry));
  const runAt = new Map();
  for (const id of new Set(single.flatMap((entry) => entry.runs))) {
    // eslint-disable-next-line no-await-in-loop
    const { data } = await github.rest.actions.getWorkflowRun({ owner, repo, run_id: Number(id) });
    let executed = data.status === 'completed' && (data.conclusion === 'success' || data.conclusion === 'failure');
    if (executed) {
      // Every page, until a job that ran steps turns up: the jobs that ran need not come first.
      executed = false;
      for (let page = 1; !executed; page += 1) {
        // eslint-disable-next-line no-await-in-loop
        const { data: listing } = await github.rest.actions.listJobsForWorkflowRun({ owner, repo, run_id: Number(id), per_page: 100, page });
        const jobs = listing.jobs ?? [];
        executed = jobs.some((job) => (job.steps ?? []).some((step) => step.conclusion === 'success' || step.conclusion === 'failure'));
        if (jobs.length < 100) break;
      }
    }
    runAt.set(id, executed ? String(data.head_sha ?? '').toLowerCase() : null);
  }
  for (const entry of single) {
    entry.hasOutput = entry.fenced || entry.runs.some((id) => runAt.get(id) === entry.shas[0]);
    if (!entry.hasOutput) {
      problems.push(
        `"${entry.line}" names ${short(entry.shas[0])} but carries no output: put the output in a fenced block under it, ` +
          `or link a run of this repository's workflows that ran at ${short(entry.shas[0])} and executed.`
      );
    }
  }

  // Every entry with output must describe the current code: nothing after its
  // commit changes a staleness input, or newer entries re-run every command it ran
  // (the "after" of a before-and-after pair). A newer run of a different check, or
  // of some of its commands, proves nothing about the rest.
  const position = (sha) => (sha === head ? Number.MAX_SAFE_INTEGER : listed.indexOf(sha));
  const withOutput = single.filter((entry) => entry.hasOutput);
  const cited = withOutput.map((entry) => entry.shas[0]).reduce((newest, sha) => (newest === null || position(sha) > position(newest) ? sha : newest), null);

  const changes = new Map();
  const changedAfter = async (sha) => {
    const described = [];
    for (const commit of commits.slice(position(sha) + 1)) {
      if (!changes.has(commit.sha)) {
        // eslint-disable-next-line no-await-in-loop
        const change = await laterChanges(github, owner, repo, commit, pull.base.sha);
        changes.set(
          commit.sha,
          change.unknown
            ? `${short(commit.sha)} (${change.unknown}, so it cannot be checked)`
            : change.paths.length > 0
              ? `${short(commit.sha)} (${change.paths.slice(0, 3).join(', ')}${change.paths.length > 3 ? ', …' : ''})`
              : null
        );
      }
      if (changes.get(commit.sha)) described.push(changes.get(commit.sha));
    }
    return described;
  };

  for (const entry of withOutput) {
    const at = entry.shas[0];
    if (at === head) continue;
    const newer = withOutput.filter((other) => position(other.shas[0]) > position(at));
    const rerun = entry.commands.size > 0 && [...entry.commands].every((command) => newer.some((other) => other.commands.has(command)));
    if (rerun) continue;
    if (truncated) {
      problems.push(
        `"${entry.line}" ran at ${short(at)}, but the PR has ${commits.length} or more commits, past what GitHub lists, ` +
          `so only evidence at the head (${short(head)}) can be shown current.`
      );
      continue;
    }
    // eslint-disable-next-line no-await-in-loop
    const stale = await changedAfter(at);
    if (stale.length > 0) {
      problems.push(
        `"${entry.line}" ran at ${short(at)}, but later commits change shipped code or test inputs: ${stale.join('; ')}. ` +
          'Re-run the same command at the head and add its output, or remove the entry.'
      );
    }
  }
  return { problems, cited };
}

/** The message Attestation reports for a set of problems. */
export function describeProblems(problems) {
  return (
    `The PR description's Evidence section is incomplete:\n- ${problems.join('\n- ')}\n` +
    'Every claim needs the commit it ran at, the command, and its output (AGENTS.md, Evidence). ' +
    'Editing the description re-runs this check.'
  );
}
