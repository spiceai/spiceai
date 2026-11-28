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
// passes it (AGENTS.md, *Evidence*). Each entry names a commit from the PR and
// carries its output, as a fenced block or a link to the run; every check marked
// not run gives a reason; and every entry is current: no later commit changes
// shipped code (a clean merge of the base aside), unless newer entries re-run
// every command it ran.
//
// Attestation (`.github/workflows/pr.yml`) imports this from a checkout of the
// *base* commit, never the PR's, so a PR cannot change the rule that judges it.
// It checks that evidence is present, current, and complete in form. It cannot
// check that the evidence is true; review does that.

// Until this time the check reports its findings as a warning, so open PRs and the
// bots that open PRs have time to adopt the Evidence section. From then on it
// fails every PR it finds a problem with, whenever the PR was opened.
export const ENFORCED_FROM = '2026-12-07T00:00:00Z';

// GitHub lists at most this many commits for a PR.
const MAX_LISTED_COMMITS = 250;
// A commit's files come 100 to a page, and GitHub returns at most 3000 of them.
const FILES_PER_PAGE = 100;
const MAX_FILE_PAGES = 30;
// A comparison returns at most this many files.
const MAX_COMPARED_FILES = 300;

// The files that need a developer sign-off: the same pattern Attestation's no-Rust
// fast-track uses in pr.yml. A commit touching none of them changes no shipped
// code, so it does not make earlier evidence stale. test_pr_evidence.mjs fails if
// the two copies differ.
export const RUST_AFFECTING = /(^(crates|bin|tools|vendor)\/|\.(rs|snap|proto)$|(^|\/)Cargo\.(toml|lock)$|(^|\/)rust-toolchain(\.toml)?$|(^|\/)\.cargo\/|(^|\/)\.?(clippy|rustfmt)\.toml$|(^|\/)nextest\.toml$|(^|\/)layers\.toml$|^scripts\/(test_)?check_(crate_layers|table_layers|rust_gate_paths|module_reachability|fork_patches|bench_mysql_load_nulls|cayenne_data_path_fs)\.py$|^scripts\/cayenne_data_path_fs_allowlist\.json$|^scripts\/(test_)?rust_guard_common\.py$|^scripts\/(test_)?nextest_build_targets\.py$|^docs\/dev\/fork_patches\.md$|^(Makefile|version\.txt)$|^test\/tpc-bench\/(Makefile|mysql-null-spec\.awk)$|^test\/tls\/|^test\/spicepods\/tpch\/sf1\/accelerated\/postgres\[catalog\]\[changes\]\.yaml$|^test\/source_versions\.json$|^\.schema\/spicepod\.schema\.json$)/;

const HEADING = /^(#{1,6})\s+(.*)$/;
const COMMENT = /<!--[\s\S]*?-->/g;
const FENCE = /^\s*(```|~~~)/;
const SHA_TOKEN = /\b[0-9a-f]{7,40}\b/gi;
// Output by link: a workflow run or job, a PR's checks, or an uploaded file. A
// link to anything else (docs, an issue) is not output.
const RUN_LINK =
  /\bhttps:\/\/(?:github\.com\/[\w.-]+\/[\w.-]+\/(?:actions\/runs\/\d+|runs\/\d+|pull\/\d+\/checks)|github\.com\/user-attachments\/|(?:private-)?user-images\.githubusercontent\.com\/|gist\.github\.com\/)/i;
// A check marked not run with nothing after the mark ("Tests: not run", "Not run:",
// "| S3 | not run |"), or with only a placeholder for a reason ("Not run — <reason>",
// "not run: TODO"). A real reason passes, and so does prose that merely contains
// the words ("queries that do not run on DuckDB").
const NOT_RUN_WITHOUT_REASON = /\bnot run\b[\s—–:.-]*$/i;
const NOT_RUN_PLACEHOLDER = /\bnot run\b\s*(?:[—–:-]+|because\b)\s*(?:<[^>]*>|todo|tbd|n\/a)?[\s.]*$/i;
const DECORATION = /[*_`]+/g;
// A command in pasted output ("$ make test") or a code span on the entry's line
// that is not a SHA: what identifies a re-run of the same check.
const PROMPT = /^\s*\$\s+(.+?)\s*$/;
const CODE_SPAN = /`([^`]+)`/g;
// Compared as written: whitespace inside a command can be part of an argument.
const normalize = (command) => command.trim();
const TABLE_EDGE = /\s*\|\s*$/;

/** Each line of a body with HTML comments removed, marked as inside a fenced block or not. */
function markedLines(text) {
  let fenced = false;
  return text
    .replace(/\r\n/g, '\n')
    .replace(COMMENT, '')
    .split('\n')
    .map((line) => {
      if (FENCE.test(line)) {
        fenced = !fenced;
        return { line, fence: true, fenced: false };
      }
      return { line, fence: false, fenced };
    });
}

/**
 * The text of the description's Evidence section, or null when it has none.
 * Headings count only outside fenced blocks and HTML comments. A heading that is
 * just "Evidence" (as in the template's "🧪 Evidence") wins over one that merely
 * contains the word. The section runs to the next heading at the same or a
 * higher level.
 */
export function evidenceSection(body) {
  const lines = markedLines(body ?? '');
  const headings = [];
  lines.forEach(({ line, fence, fenced }, index) => {
    const match = !fence && !fenced ? HEADING.exec(line) : null;
    if (match) headings.push({ index, level: match[1].length, text: match[2] });
  });
  const chosen =
    headings.find((heading) => /^[^a-z0-9]*evidence[^a-z0-9]*$/i.test(heading.text)) ??
    headings.find((heading) => /\bevidence\b/i.test(heading.text));
  if (!chosen) return null;
  const next = headings.find((heading) => heading.index > chosen.index && heading.level <= chosen.level);
  return lines
    .slice(chosen.index + 1, next ? next.index : lines.length)
    .map(({ line }) => line)
    .join('\n')
    .trim();
}

/**
 * The section's evidence entries. An entry starts at a line, outside a fenced
 * block, naming a commit `resolve` recognizes, and runs to the next such line.
 * It has output when it holds a fenced block with content or a run link. Its
 * commands are the `$ ` lines in its output and the code spans on its line that
 * are not SHAs. Output before the first entry belongs to the first entry.
 */
export function evidenceEntries(section, resolve) {
  const entries = [];
  let current = null;
  let pendingOutput = false;
  const pendingCommands = new Set();
  let blockHasContent = false;
  const credit = () => {
    if (current) current.hasOutput = true;
    else pendingOutput = true;
  };
  for (const { line, fence, fenced } of markedLines(section)) {
    if (fence) {
      // A closing fence ends a block; credit it if it held anything.
      if (blockHasContent) credit();
      blockHasContent = false;
      continue;
    }
    if (fenced) {
      if (line.trim()) blockHasContent = true;
      const prompt = PROMPT.exec(line);
      if (prompt) (current ? current.commands : pendingCommands).add(normalize(prompt[1]));
      continue;
    }
    const shas = [...new Set((line.match(SHA_TOKEN) ?? []).map((token) => resolve(token.toLowerCase())).filter(Boolean))];
    if (shas.length > 0) {
      const spans = [...line.matchAll(CODE_SPAN)].map((match) => normalize(match[1])).filter((span) => !/^[0-9a-f]{7,40}$/i.test(span));
      current = {
        shas,
        line: line.trim(),
        hasOutput: pendingOutput || RUN_LINK.test(line),
        commands: new Set([...pendingCommands, ...spans]),
      };
      pendingOutput = false;
      pendingCommands.clear();
      entries.push(current);
    } else if (RUN_LINK.test(line)) {
      credit();
    }
  }
  if (blockHasContent) credit();
  return entries;
}

/** Lines outside fenced blocks that mark a check not run without a real reason. */
export function notRunWithoutReason(section) {
  return markedLines(section)
    .filter(({ fence, fenced }) => !fence && !fenced)
    .map(({ line }) => line.trim())
    .filter((line) => {
      const plain = line.replace(DECORATION, '').replace(TABLE_EDGE, '');
      return NOT_RUN_WITHOUT_REASON.test(plain) || NOT_RUN_PLACEHOLDER.test(plain);
    });
}

const short = (sha) => sha.slice(0, 12);
const touchesShippedCode = (file) =>
  RUST_AFFECTING.test(file.filename) || (file.previous_filename !== undefined && RUST_AFFECTING.test(file.previous_filename));

/** Every file a commit changes against its first parent, or null past GitHub's limit. */
async function commitFiles(github, owner, repo, ref) {
  const files = [];
  for (let page = 1; page <= MAX_FILE_PAGES; page += 1) {
    // eslint-disable-next-line no-await-in-loop
    const { data } = await github.rest.repos.getCommit({ owner, repo, ref, per_page: FILES_PER_PAGE, page });
    const listed = data.files ?? [];
    files.push(...listed);
    if (listed.length < FILES_PER_PAGE) return files;
  }
  return null;
}

/** The files two commits differ in, or null past GitHub's limit. */
async function comparedFiles(github, owner, repo, base, head) {
  const { data } = await github.rest.repos.compareCommitsWithBasehead({ owner, repo, basehead: `${base}...${head}` });
  const files = data.files ?? [];
  return { status: data.status, files: files.length >= MAX_COMPARED_FILES ? null : files };
}

/**
 * The shipped-code paths a later commit changes, or a reason it cannot be told.
 * A merge whose second parent is on the base branch counts only for files it
 * changes against both parents: files both sides touched, or edited by hand while
 * merging. Any other merge counts for everything it brings in.
 */
async function laterChanges(github, owner, repo, commit, baseSha) {
  const parents = (commit.parents ?? []).map((parent) => parent.sha);
  if (parents.length > 2) return { unknown: 'an octopus merge' };
  if (parents.length < 2) {
    const files = await commitFiles(github, owner, repo, commit.sha);
    if (files === null) return { unknown: 'more files than GitHub lists' };
    return { paths: files.filter(touchesShippedCode).map((file) => file.filename) };
  }
  const [prSide, otherSide] = parents;
  const fromPr = await comparedFiles(github, owner, repo, prSide, commit.sha);
  if (fromPr.files === null) return { unknown: 'more files than GitHub compares' };
  const onBase = await comparedFiles(github, owner, repo, otherSide, baseSha);
  if (onBase.status !== 'ahead' && onBase.status !== 'identical') {
    return { paths: fromPr.files.filter(touchesShippedCode).map((file) => file.filename) };
  }
  const fromBase = await comparedFiles(github, owner, repo, otherSide, commit.sha);
  if (fromBase.files === null) return { unknown: 'more files than GitHub compares' };
  const changedAgainstBase = new Set(fromBase.files.filter(touchesShippedCode).map((file) => file.filename));
  return { paths: fromPr.files.filter(touchesShippedCode).map((file) => file.filename).filter((name) => changedAgainstBase.has(name)) };
}

/**
 * Check a pull request's Evidence section. Reads the PR, its commits, and the
 * changes after the newest commit the evidence cites.
 *
 * Returns { problems: [string], enforced: bool, cited: sha | null }. `enforced`
 * is false before ENFORCED_FROM, when problems are reported as a warning. API
 * errors propagate: a check that cannot read the PR has reached no verdict, and
 * the caller must not pass it.
 */
export async function checkEvidence({ github, owner, repo, pullNumber, now = new Date() }) {
  const { data: pull } = await github.rest.pulls.get({ owner, repo, pull_number: pullNumber });
  const enforced = now >= new Date(ENFORCED_FROM);
  const head = pull.head.sha.toLowerCase();

  const problems = [];
  const section = evidenceSection(pull.body);
  if (section === null) {
    problems.push('The description has no Evidence section. Add the `🧪 Evidence` section from .github/PULL_REQUEST_TEMPLATE.md.');
    return { problems, enforced, cited: null };
  }
  if (section === '') {
    problems.push('The Evidence section is empty (its template comment does not count).');
    return { problems, enforced, cited: null };
  }
  for (const line of notRunWithoutReason(section)) {
    problems.push(`A check is marked not run without a reason: "${line}". Write \`Not run — <reason>\`.`);
  }

  const commits = await github.paginate(github.rest.pulls.listCommits, {
    owner, repo, pull_number: pullNumber, per_page: 100,
  });
  const listed = commits.map((commit) => commit.sha.toLowerCase());
  const truncated = commits.length >= MAX_LISTED_COMMITS;
  const resolve = (token) => (head.startsWith(token) ? head : listed.find((sha) => sha.startsWith(token)) ?? null);

  const entries = evidenceEntries(section, resolve);
  if (entries.length === 0) {
    problems.push(
      `The Evidence section names no commit from this PR outside its output. Name the commit each result ran at (the head is ${short(head)}).`
    );
    return { problems, enforced, cited: null };
  }
  for (const entry of entries.filter((candidate) => candidate.shas.length > 1)) {
    problems.push(
      `"${entry.line}" names more than one commit from this PR (${entry.shas.map(short).join(', ')}): ` +
        'give each result its own entry, naming only the commit it ran at.'
    );
  }
  for (const entry of entries.filter((candidate) => !candidate.hasOutput)) {
    problems.push(
      `"${entry.line}" names ${entry.shas.map(short).join(', ')} but carries no output: put the output in a fenced block under it, or link the run.`
    );
  }

  // Every entry with output must describe the current code: nothing after its
  // commit changes shipped code, or newer entries re-run every command it ran
  // (the "after" of a before-and-after pair). A newer run of a different check,
  // or of some of its commands, proves nothing about the rest.
  const position = (sha) => (sha === head ? Number.MAX_SAFE_INTEGER : listed.indexOf(sha));
  const newestOf = (entry) => entry.shas.reduce((newest, sha) => (position(sha) > position(newest) ? sha : newest));
  const withOutput = entries.filter((entry) => entry.hasOutput);
  const cited = withOutput.map(newestOf).reduce((newest, sha) => (newest === null || position(sha) > position(newest) ? sha : newest), null);

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
    const at = newestOf(entry);
    if (at === head) continue;
    const newer = withOutput.filter((other) => position(newestOf(other)) > position(at));
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
        `"${entry.line}" ran at ${short(at)}, but later commits change shipped code: ${stale.join('; ')}. ` +
          'Re-run the same command at the head and add its output, or remove the entry.'
      );
    }
  }
  return { problems, enforced, cited };
}

/** The message Attestation reports for a set of problems. */
export function describeProblems(problems) {
  return (
    `The PR description's Evidence section is incomplete:\n- ${problems.join('\n- ')}\n` +
    'Every claim needs the commit it ran at, the command, and its output (AGENTS.md, Evidence). ' +
    'Editing the description re-runs this check.'
  );
}
