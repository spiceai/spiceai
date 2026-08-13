---
name: writeReleaseNotes
description: Write or update Spice release notes for a version from git history, in Simplified Technical English. Use this whenever the user asks to write, draft, cut, update, or refresh release notes, release candidate notes, or a changelog entry under docs/release_notes/ — including "notes for vX.Y.Z", "what shipped since <tag>", or "add the new PRs to the release notes".
argument-hint: The new version tag and the previous version tag to diff against, or the literal word "update" to refresh an existing in-progress release notes file with new commits since it was last edited (e.g. "v2.0.0-rc.5 since v2.0.0-rc.4", or "update").
---

Write or update release notes for the specified version based on all changes since the specified previous release.

## Modes

- **Create**: No release notes file exists yet for the version. Build the file from scratch following all steps below.
- **Update**: Argument is `update` (or the release notes file already exists). Treat the existing file as the source of truth for tone, ordering and editorial decisions, and only ADD entries for commits landed on `origin/trunk` since the file was last edited. Do not rewrite or reorder existing sections unless the user asks explicitly.

To detect update mode and find the relevant commit range:

- Check whether `docs/release_notes/<version>.md` exists.
- Find when it was last edited: `git log -1 --format='%H' docs/release_notes/<version>.md`
- New commits to consider: `git fetch origin && git log <last-edit-sha>..origin/trunk --no-merges --format='%h | %an | %s'`

## Writing style: Simplified Technical English

Write every sentence you author in Simplified Technical English (ASD-STE100 writing rules). Release notes are read fast, by people deciding whether to upgrade, often in a second language or through a machine translator, and re-read months later by someone bisecting a behaviour change. A 50-word sentence with three subordinate clauses fails all of those readers. Short declarative sentences do not make the content less technical — they make each technical claim separately checkable.

Read `.github/prompts/references/simplified_technical_english.md` before writing prose. It carries the full rules, the rewrite patterns, the terminology table, and worked before/after examples from real Spice releases. The short version:

- **One idea per sentence, 25 words or fewer.** Split where the subject changes. The total gets longer; that is the trade.
- **Active voice, named actor.** "The runtime derives memory budgets from its cgroup limit", not "Memory budgets are derived". Passive is acceptable only when the actor is genuinely unknown.
- **No participle clauses** — no `using`, `enabling`, `including`, `allowing`, `by improving`. Use a finite verb in its own sentence. Established technical nouns that end in `-ing` (caching, sharding, CPU sizing) are fine.
- **Simple tenses.** Present for current behaviour, past for what a release changed.
- **One word, one meaning.** Hold each term for the whole document — `setting`, not a rotation of knob/option/param.
- **Keep `that`, articles, and prepositions.** Dropping them is the ambiguity STE exists to prevent.
- **At most three stacked nouns.** Break longer stacks with a preposition or hyphen.
- **Give the number, not the intensifier.** No `significantly`, `various`, `seamless`, `powerful`, `unlocks`, `under the hood`, `headlined by`. If no measurement exists, describe the mechanism.
- **No semicolons**, and at most one em-dash aside per sentence.
- **Bug fixes in a fixed order**: symptom, then cause, then current behaviour.

This governs the summary paragraph, the Highlights bullets, every `## What's New` subsection, breaking-change and migration text, and bug-fix descriptions. It does not govern `### Changelog` bullets (they quote PR titles verbatim so they stay greppable against git history), `## Contributors`, `## Upgrading`, `## Cookbook Updates`, the dependency table, code blocks, or any inline identifier.

STE constrains sentence construction, never technical depth. Keep every setting name, metric name, error variant, version number, and YAML sample exact. Never trade a specific mechanism for a vague summary to shorten a sentence.

## Steps

1. **Study previous release notes** in `docs/release_notes/` for **structure and format only** — the sections, their order, the header and date convention, how features are grouped, and the changelog line format. Do **not** copy their sentence construction: the published files predate Simplified Technical English, so their long multi-clause sentences are the pattern to break, not to match. Pay attention to:
   - Header format and date conventions (e.g. `# Spice vX.Y.Z (Month D, YYYY)`)
   - Opening summary paragraph that names the release-defining themes
   - Bulleted "Highlights in this release candidate include:" list right under the summary
   - How features are grouped and described (subsections under `## What's New`)
   - Level of technical detail per feature; preferred prose vs. bullets; example YAML snippets
   - Enterprise feature callout style (`> [Spice.ai Enterprise]...` blockquote at the top of the subsection)
   - Breaking Changes section format with migration before/after YAML
   - Contributors section format (GitHub profile links, alphabetised)
   - Upgrading instructions format
   - Changelog format with PR links and author attribution

2. **Gather changes** using `git log` between the two version tags:
   - All non-merge commits: `git log <prev-tag>..<new-tag> --oneline --no-merges` (or `..HEAD` if the new tag does not exist yet; or `<last-edit-sha>..origin/trunk` in update mode).
   - For each non-trivial PR, look up the PR title, body and author on GitHub. Commit subjects often lack the user-facing framing the release notes need.
     - PR metadata: `gh pr view <num> --json title,body,author,labels`
     - Author handle (use this when the commit author name is ambiguous, bot-mangled, or differs from GitHub login): `gh pr view <num> --json author -q '.author.login'`
   - Identify contributors: `git log <range> --format='%an <%ae>' --no-merges | sort -u`, then map each to a GitHub username using `gh pr view` on one of their PRs and by cross-referencing previous release notes.

3. **Filter noise**. Exclude entirely from both the narrative and the changelog:
   - `dependabot[bot]` and `github-actions[bot]` commits unless they update a user-visible dependency (e.g. DuckDB, Iceberg, Turso) — those go in the dependency table.
   - Test/snapshot updates (`fix(tests): ...`, `chore(benchmarks): ...`, `Update snapshots`, `Disable failing ... test in CI`).
   - Internal refactors with no user-visible behaviour change (e.g. lint deny attributes, internal trait reshuffles).
   - Reverts of changes that never shipped in a prior release.
   - `chore: Clean up Cargo.lock`-style housekeeping.
   - Significant internal changes (e.g. CI infrastructure rewrites) MAY be included in the detailed changelog at the bottom but never in the highlights or `## What's New`.

4. **Categorize changes** into:
   - Major new features (deserve their own `### Subsection` with description, key points, and YAML examples when configuration changes).
   - Dependency upgrades (presented in a table at the end of `## What's New`).
   - Smaller improvements (bullet list under broader subsections such as `### SQL, Query, and Developer Experience` or `### Caching & Search`).
   - Breaking changes (with before/after migration guidance).
   - Bug fixes (grouped by area, e.g. `### Connector Bug Fixes`).

5. **Write the release notes** in the established structure, with every authored sentence in [Simplified Technical English](#writing-style-simplified-technical-english):
   - `# Spice v<version> (<Month D, YYYY>)` header followed by a one-paragraph summary naming the headline themes.
   - `Highlights in this release candidate include:` bullet list.
   - `## What's New in v<version>` with subsections for each major feature.
   - `## Contributors` with GitHub profile links, alphabetised case-insensitively.
   - `## Breaking Changes` (omit if none).
   - `## Cookbook Updates` (state "No new cookbook recipes." if none).
   - `## Upgrading` with CLI, Homebrew, Docker, Helm, and AWS Marketplace instructions.
   - `## What's Changed` → `### Changelog` with one bullet per included PR in the form
     `- <title> by [@handle](https://github.com/<handle>) in [#<num>](https://github.com/spiceai/spiceai/pull/<num>)`
   - `**Full Changelog**: <https://github.com/spiceai/spiceai/compare/<prev-tag>...<new-tag>>`

6. **Check the prose** before you report the file as finished:

   ```bash
   python3 .github/prompts/scripts/check_ste.py docs/release_notes/v<version>.md
   ```

   The script reads only the authored prose and reports STE violations with line numbers. **Fix every error** — sentence length, semicolons, participle clauses, vague wording. **Read every warning and decide** — passive voice, noun stacks, paragraph length, and inconsistent terminology all have defensible exceptions, so judge each one rather than rewriting on reflex. If a fix would cost a technical fact, keep the fact and leave the warning; never delete information to satisfy the checker.

   What counts as done depends on the mode:

   - **Create**: re-run until the whole file reports zero errors.
   - **Update**: the checker reads the whole file, but only the sections you added are yours to fix. Re-run until **those sections** report zero errors. The file itself can still report errors, because an existing file may predate these rules. Match each finding's line number against the lines you added to tell the two apart. Leave every error in prose you did not touch — existing prose is the user's editorial decision — and report it instead of rewriting it.

## Ordering

Within both the Highlights bullets and the `## What's New` subsections, **keep the two lists in the same relative order** so the reader can move between them without surprise.

Default thematic order for highlights/subsections, top to bottom:

1. **Spice Cayenne** — always first when there is meaningful Cayenne news.
2. Security & TLS (mTLS, auth)
3. CDC sources (MongoDB Change Streams, Kafka offsets, Debezium fixes)
4. DML / write-back (PostgreSQL, Snowflake, Arrow upserts, DuckLake Beta)
5. SQL & UDFs (User-Defined Functions, Spatial SQL UDFs)
6. Runtime features (On-Demand Dataset Loading, SMB client, Unified Cancellation)
7. HTTP / connector improvements (Dynamic HTTP Connector, HTTP rate-control persistence)
8. Acceleration (`refresh_mode: snapshot`, new accelerator features)
9. AI / LLM (Prompt caching, Responses API)
10. Cross-cutting trailing sections inside `## What's New`: Distributed Cluster Improvements → Caching & Search → Security Improvements → SQL/Developer Experience → Connector Bug Fixes → Dependency Updates.

## Project-specific conventions

- The product surface name is **Spice Cayenne** in narrative prose (highlights, opening paragraph). Inside subsections about Cayenne internals, plain "Cayenne" is fine after the first mention.
- [@claudespice](https://github.com/claudespice) is a bot and **must not** appear in the `## Contributors` section. It may appear in the `### Changelog` author attribution because that follows the PR's actual author.
- Use `## What's Changed` then `### Changelog` (not `## Changelog`) to match the GitHub auto-generated layout that prior releases mirror.
- Verify each PR is referenced at most once in the changelog and at most once in the narrative `## What's New`. Use `grep -c '#<num>' <file>` to spot-check.
- When updating an existing file, also update the **Contributors** list if the new commits introduce a new author. Skip bots and `claudespice`.
- The terminology table in `.github/prompts/references/simplified_technical_english.md` fixes one term per concept (dataset, accelerator, the runtime, setting, flag, release). Hold each term for the whole document.

## Documentation links

Link feature names in subsections to the appropriate documentation host. Pick the host based on where the feature is documented, not by audience:

- **OSS / runtime features** → `https://spiceai.org/docs` (e.g. `https://spiceai.org/docs/components/data-connectors/postgres`).
- **Spice.ai Cloud** → `https://docs.spice.ai/docs` (e.g. `https://docs.spice.ai/docs/api/sql`).
- **Spice.ai Enterprise** → `https://docs.spice.ai/docs/enterprise` (e.g. `https://docs.spice.ai/docs/enterprise/features/distributed-accelerations`). Enterprise subsections also get the `> [Spice.ai Enterprise](https://docs.spice.ai/docs/enterprise) feature. See [...](<deep-link>).` blockquote at the top.

Verify any new doc link you introduce actually resolves. If a deep link cannot be confirmed, link to the section root instead.

## Output

Save the release notes as `docs/release_notes/v<version>.md`. In update mode, edit the existing file in place and commit with a `docs(release): update v<version> notes with latest trunk PRs` style message.

Report the checker result with the file. State the error count for the prose you authored (zero, once fixed) and name any warning you chose to keep, with the reason. In update mode, list the pre-existing errors you left in place as a separate count, so a reader does not read your zero as a whole-file zero. A silent pass tells the reviewer nothing about which judgements you made.
