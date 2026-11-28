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
// passes it (AGENTS.md, *Evidence*): a commit from the PR that the evidence ran
// at, the output or a link to the run, a reason on every check that was not run,
// and no later commit that changes shipped code without the evidence being
// re-run.
//
// Attestation (`.github/workflows/pr.yml`) imports this from a checkout of the
// *base* commit, never the PR's, so a PR cannot change the rule that judges it.
// It checks that the evidence is present, current, and complete in form. It
// cannot check that the evidence is true; review does that.

// GitHub's list-commits endpoint for a pull request returns at most this many.
const MAX_LISTED_COMMITS = 250;

// The module's own path, whose first commit on the base branch is when the check
// took effect: a PR opened before then gets a warning, not a failure.
export const MODULE_PATH = '.github/scripts/pr_evidence.mjs';

// The files that need a developer sign-off: the same pattern Attestation's no-Rust
// fast-track uses in pr.yml. A commit touching none of them changes no shipped
// code, so it does not make earlier evidence stale. test_pr_evidence.mjs fails if
// the two copies differ.
export const RUST_AFFECTING = /(^(crates|bin|tools|vendor)\/|\.(rs|snap|proto)$|(^|\/)Cargo\.(toml|lock)$|(^|\/)rust-toolchain(\.toml)?$|(^|\/)\.cargo\/|(^|\/)\.?(clippy|rustfmt)\.toml$|(^|\/)nextest\.toml$|(^|\/)layers\.toml$|^scripts\/(test_)?check_(crate_layers|table_layers|rust_gate_paths|module_reachability|fork_patches|bench_mysql_load_nulls|cayenne_data_path_fs)\.py$|^scripts\/cayenne_data_path_fs_allowlist\.json$|^scripts\/(test_)?rust_guard_common\.py$|^scripts\/(test_)?nextest_build_targets\.py$|^docs\/dev\/fork_patches\.md$|^(Makefile|version\.txt)$|^test\/tpc-bench\/(Makefile|mysql-null-spec\.awk)$|^test\/tls\/|^test\/spicepods\/tpch\/sf1\/accelerated\/postgres\[catalog\]\[changes\]\.yaml$|^test\/source_versions\.json$|^\.schema\/spicepod\.schema\.json$)/;

const HEADING = /^(#{1,6})\s+(.*)$/;
const COMMENT = /<!--[\s\S]*?-->/g;
const SHA_TOKEN = /\b[0-9a-f]{7,40}\b/gi;
const URL = /\bhttps?:\/\/\S+/;
const FENCE = /^\s*(```|~~~)/m;
const NOT_RUN = /\bnot run\b/i;
// "Not run — reason", "not run: reason", "not run - reason", "not run because ...".
const NOT_RUN_WITH_REASON = /\bnot run\b\s*(?:[—–:-]+|because\b)\s*\S/i;

/**
 * The text of the description's Evidence section with HTML comments removed,
 * or null when it has none. The section runs from a heading containing the word
 * "Evidence" to the next heading at the same or a higher level.
 */
export function evidenceSection(body) {
  const lines = (body ?? '').replace(/\r\n/g, '\n').split('\n');
  let start = -1;
  let level = 0;
  for (let index = 0; index < lines.length; index += 1) {
    const match = HEADING.exec(lines[index]);
    if (!match) continue;
    if (start === -1) {
      if (/\bevidence\b/i.test(match[2])) {
        start = index + 1;
        level = match[1].length;
      }
    } else if (match[1].length <= level) {
      return lines.slice(start, index).join('\n').replace(COMMENT, '').trim();
    }
  }
  return start === -1 ? null : lines.slice(start).join('\n').replace(COMMENT, '').trim();
}

/** What the section offers, before any of it is matched against the PR. */
export function parseEvidence(section) {
  const tokens = [...new Set((section.match(SHA_TOKEN) ?? []).map((token) => token.toLowerCase()))];
  const notRunWithoutReason = section
    .split('\n')
    .filter((line) => NOT_RUN.test(line) && !NOT_RUN_WITH_REASON.test(line))
    .map((line) => line.trim());
  return {
    tokens,
    hasOutput: FENCE.test(section) || URL.test(section),
    notRunWithoutReason,
  };
}

const short = (sha) => sha.slice(0, 12);

/**
 * Check a pull request's Evidence section. Reads the PR, its commits, and the
 * files of any commit after the newest one the evidence cites.
 *
 * Returns { problems: [string], enforced: bool, cited: sha | null }. `enforced`
 * is false for a PR opened before this check reached its base branch, whose
 * problems are reported as a warning. API errors propagate: a check that cannot
 * read the PR has reached no verdict, and the caller must not pass it.
 */
export async function checkEvidence({ github, owner, repo, pullNumber }) {
  const { data: pull } = await github.rest.pulls.get({ owner, repo, pull_number: pullNumber });

  const history = await github.paginate(github.rest.repos.listCommits, {
    owner, repo, sha: pull.base.sha, path: MODULE_PATH, per_page: 100,
  });
  // Newest first, so the last entry is the commit that introduced the check.
  const introduced = history.at(-1)?.commit?.committer?.date;
  const enforced = !introduced || new Date(pull.created_at) >= new Date(introduced);

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

  const evidence = parseEvidence(section);
  if (!evidence.hasOutput) {
    problems.push('The Evidence section has no output: paste it in a fenced code block, or link the CI run.');
  }
  for (const line of evidence.notRunWithoutReason) {
    problems.push(`A check is marked not run without a reason: "${line}". Write \`Not run — <reason>\`.`);
  }

  const commits = await github.paginate(github.rest.pulls.listCommits, {
    owner, repo, pull_number: pullNumber, per_page: 100,
  });
  if (commits.length >= MAX_LISTED_COMMITS) {
    problems.push(`The PR has ${commits.length} or more commits, past what GitHub lists, so the evidence cannot be matched to them.`);
    return { problems, enforced, cited: null };
  }

  // Commits are listed oldest first; the newest one the evidence cites decides
  // what counts as a later change.
  let citedIndex = -1;
  commits.forEach((commit, index) => {
    if (evidence.tokens.some((token) => commit.sha.toLowerCase().startsWith(token))) citedIndex = index;
  });
  if (citedIndex === -1) {
    problems.push(
      `The Evidence section names no commit from this PR. Name the commit each result ran at (the head is ${short(pull.head.sha)}).`
    );
    return { problems, enforced, cited: null };
  }
  const cited = commits[citedIndex].sha;

  // A merge brings in the base branch, which Attestation's own sign-off
  // inheritance treats as no change to the work under review; any other later
  // commit that touches shipped code means the evidence describes older code.
  const stale = [];
  for (const commit of commits.slice(citedIndex + 1)) {
    if ((commit.parents?.length ?? 1) > 1) continue;
    // eslint-disable-next-line no-await-in-loop
    const { data } = await github.rest.repos.getCommit({ owner, repo, ref: commit.sha });
    const touched = (data.files ?? []).map((file) => file.filename).filter((name) => RUST_AFFECTING.test(name));
    if (touched.length > 0) stale.push(`${short(commit.sha)} (${touched.slice(0, 3).join(', ')}${touched.length > 3 ? ', …' : ''})`);
  }
  if (stale.length > 0) {
    problems.push(
      `The evidence ran at ${short(cited)}, but later commits change shipped code: ${stale.join('; ')}. ` +
        'Re-run it at the new head and update the SHA.'
    );
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
