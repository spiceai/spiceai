---
name: write-release-notes
description: Write or update Spice release notes for a version from git history, in Simplified Technical English. Use this whenever the user asks to write, draft, cut, update, or refresh release notes, release candidate notes, patch release notes, or a changelog entry under docs/release_notes/ — including "notes for vX.Y.Z", "what shipped since v2.3.0", "add the new PRs to the release notes", or "the intro/highlights of the release notes are bad, fix them".
---

Write or update release notes for the specified version based on all changes since the specified previous release.

## Input

The new version tag and the previous version tag to diff against (e.g. `v2.0.0-rc.5 since v2.0.0-rc.4`), or the literal word `update` to refresh an existing in-progress release notes file with new commits since it was last edited.

## Modes

- **Create**: No release notes file exists yet for the version. Build the file from scratch following all steps below.
- **Update**: Argument is `update` (or the release notes file already exists). Treat the existing file as the source of truth for tone, ordering and editorial decisions, and only ADD entries for commits landed since the file was last edited. Do not rewrite or reorder existing sections unless the user asks explicitly.

To detect update mode and find the relevant commit range:

- Check whether `docs/release_notes/<version>.md` exists.
- Find when it was last edited: `git log -1 --format='%H' docs/release_notes/<version>.md`
- New commits to consider: `git fetch origin && git log <last-edit-sha>..<release-ref> --no-merges --format='%h | %an | %s'`, where `<release-ref>` is chosen as in [Find the commit range](#find-the-commit-range).

Update mode is additive, and that includes the opening. You may add a sentence or a highlight for a new change that belongs there. Do not reword, reorder, or remove the author's existing opening sentences and highlights, even when [the ranking](#lead-with-what-matters-to-the-reader) says a new change should now lead. Say so in your report and propose the new opening instead, so the author makes that call. Likewise, a fix to a feature that first ships in this same release is not a bug fix from a reader's point of view: add it to the changelog only, leave the feature's subsection untouched, and mention in your report that the subsection may need a sentence.

## Find the commit range

Release candidates after rc.1 and patch releases ship from a release branch, `release/X.Y`, by cherry-pick, while trunk keeps moving. A range that ends at trunk or `HEAD` silently pulls in work that is not in the release, and the notes then describe changes users will not get.

- Run `git fetch origin --tags` first.
- If the new tag exists, use `<prev-tag>..<new-tag>`.
- Otherwise, if `origin/release/X.Y` exists for the version, use `<prev-tag>..origin/release/X.Y`.
- Use `origin/trunk` only for a release cut directly from trunk. Never use `HEAD`: the current checkout can be any branch. If you cannot tell which ref the release is cut from, ask.
- Cherry-picked commits can lack the PR number or carry a suffix such as `(release/2.4)`. Find the original PR from the subject: `git log origin/trunk --fixed-strings --grep='<subject>' --format='%s'`, or `gh pr list --state merged --search '<subject>'`. One PR can arrive as several commits (the fix plus its test or doc follow-ups); list the PR once.
- Release-branch housekeeping (version bumps, regenerated schema files) is noise.
- For the first RC of a minor release, fixes that were cherry-picked into the previous line's patch releases already shipped. Check the previous patch notes before you describe them as new.

## Shape the notes as an inverted pyramid

Release notes loosely follow the [inverted pyramid](https://en.wikipedia.org/wiki/Inverted_pyramid_(journalism)) of news writing: the most important information first, then details in descending order of importance, then background. A reader can stop after any paragraph and still have the most important facts, and the notes can be cut from the bottom without losing the news. Keep the established section headings and conventions. The pyramid decides the order inside them and what each part says first.

- **The document.** The opening paragraph is the lede. The highlights and the `## What's New` subsections follow in [ranked](#rank-every-change) order. Then come the sections a reader acts on (`## Breaking Changes`, `## Upgrading`), then background (`## Cookbook Updates`, `## Contributors`), and the changelog last.
- **The opening.** It answers the five Ws of the release: what kind of release it is and what changed, who is affected and where (which accelerator, connector, or API), why the release exists, and how to act. The header carries the date.
- **Each subsection.** The heading and the first sentence are the subsection's own lede: what changed for the user, and who it affects, in the user's terms. Write "Faster first query after startup", not "Plan cache warmed by the bootstrap task". Internal names (runtimes, caches, schedulers) come later, if at all. Then how to use it (the setting, the YAML sample), then limits and edge cases, then the mechanism and the history ("Before this release, …"), then links. Do not open a subsection with background. A reader who stops after its first sentence must still know what changed.
- **Each bug-fix entry.** The first sentence names the symptom users saw and says that it no longer happens: "Queries no longer fail with `<error>` after `<trigger>`." The cause and the details follow. A reader who stops there knows whether the fix is theirs.
- **Each list.** The most important item comes first: the highlights, the areas inside `## What's New`, and the bullets inside each area.

## Lead with what matters to the reader

The opening paragraph is the lede, and most readers read only it and the highlights. They are deciding three things: what kind of release this is, whether it affects them, and what to do. Every sentence in the opening must serve one of those decisions.

The easy mistake is to lead with whatever the PRs make most prominent: the `feat:` PR, the longest PR body, the cleverest mechanism. That produces an opening about an edge case that few users hit, while a failure that every user of a common feature could hit sits in the fixes list. Rank first, then write.

### Rank every change

For each change that survives filtering, take these from the PR body, not from the commit subject:

- **Symptom**: what a user saw before. An error message, missing or wrong rows, a failed refresh, a crash, high latency. For a new capability, what users can now do that they could not before.
- **Reach**: which deployments hit it, on this scale:
  - **Everyone**: default settings, every query, every accelerated dataset, startup, or upgrade.
  - **A common setup**: a widely used accelerator, connector, or refresh mode, with its default settings.
  - **A specific setup**: one connector together with one mode or one non-default setting.
  - **A rare combination**: several conditions at once, a particular data pattern (NULLs in a composite key, timestamps before 1970), a race that only a test with an injected fault reproduced, or a configuration that few deployments use.

  Every "when …" and "and …" in the PR's description of the trigger narrows the reach. A bug seen in a user deployment reaches further than one found only by a test.
- **Consequence class**, worst first:
  1. Silent wrong results or data loss (missing rows, wrong counts, corruption).
  2. Security fixes.
  3. Failures users see: query errors, crashes, refreshes or startups that fail.
  4. Changes users must act on: breaking changes, changed defaults, deprecations.
  5. Measured performance gains on common paths.
  6. New capabilities, ranked by how many users want or benefit from them.
  7. Narrow items: edge cases, rare configurations, diagnostics, log wording.
- **Provenance**, when the PR says: a regression in this release line, a bug that existed only in an earlier RC of this release, or a long-standing bug.

Reach decides what goes into the opening, and the consequence class orders items of similar reach. The opening serves the whole readership, so its top items are the ones that reach the most deployments: a query error that any user of the default accelerator can hit outranks a wrong-result fix that needs three conditions at once. A rare-combination item does not lead, whatever its class, unless it can cause lasting data loss or a security exposure. It still gets named: by its symptom in the opening's grouping sentence, so the users it affected can recognize it, and first in its own section, with its conditions stated.

These are not signals: PR size, the length of the PR body, a `feat:` versus `fix:` prefix, how much engineering the change took, and how novel it is. A big PR with a long body about a rare case is still a rare case.

### Keep every claim as narrow as the PR

Release notes for a database must be as correct as its query results. A claim that is broader than its PR is wrong, and the pressure to write short sentences and confident openings produces exactly that error. Check every claim in the opening, the highlights, and the subsections against the PR body:

- **Keep the conditions.** If a change applies only to one accelerator, to datasets with or without a primary key, to one refresh mode, or only to tables partitioned by date, the sentence keeps that condition. If the sentence gets too long, split it. Do not drop the condition.
- **Keep the bounds.** A timeout raised from 30 to 300 seconds is not "no longer times out". A larger default cache is not "no more cache misses". Give the bound: "waits up to 300 seconds, then reports an error that names the dataset".
- **Describe a mechanism as a mechanism.** If the PR changes how something is sized or scheduled and measures the effect in a test, write the change and the measured result ("splits large writes into batches; in a test, peak memory stayed under 2 GiB"). Do not turn it into a guarantee ("never runs out of memory").
- **Say who is not affected only when the PR establishes it.** Telling readers they were not affected by a wrong-results bug, when they were, is the most expensive error a release note can make. If the PR does not scope the bug, do not scope it for the PR.
- **Say what the users left out should do.** When a change leaves a gap (a platform that keeps the old behavior, a configuration the change does not cover), name those users and the action, in the opening if the action matters.
- **Highlight titles are claims too.** "Queries never time out" is an absolute claim. "The default query timeout rises to 300 seconds" is not.

If a fix stopped rows from going missing, say that queries returned incomplete results and for which datasets. If the PR does not show that, do not imply it. Say how likely the problem was, without inflating or minimizing it.

### Write the opening paragraph

1. **Sentence one: the version, the kind of release, and why it exists.** "For early testing and feedback" says nothing about this release. Say what the release is for: its theme, or the fact that it is a stabilization release that contains only fixes since the previous RC, or the main fix that justifies a patch.
2. **The one to three top-ranked items**, each as the symptom and who it affects. Use the user's words: the error they saw, the rows they missed. Do not use the internal mechanism.
3. **One sentence that counts and groups the rest**, e.g. "It also fixes N other issues: <symptom in area>, <symptom in area>, and <area>." Name a rare-combination wrong-result fix here by its symptom, so its users can find it. Do not explain any of them here. When an item's conditions do not fit in this sentence, mark it as conditional ("in some setups", "in specific configurations") and let its section give the conditions. Never state it as if it applied everywhere.
4. **Who should upgrade and what to do first.** Name the audience, and say who does not need to act when the PRs establish it. If there are breaking changes, required steps, or a setting that some users should now change, state it here or point to it. When the lead is a wrong-result or security fix, put the upgrade verdict right after it, as Rust 1.52.1 does.

Keep the opening to about 60–120 words. The details belong in the sections below it, which the opening links to through the highlights. Keeping conditions and provenance makes an opening grow. When it runs long, cut from its bottom, never from a claim's conditions: shorten the grouping part to areas with their "in specific setups" markers, and drop items that the reader will meet in the highlights or in their own section anyway.

For a release candidate, the notes cover changes since the previous RC, so say that, and tell testers what to retest by name, not "these areas". Mark the fixes for regressions that an earlier RC of the same release introduced: the testers of that RC are the readers who need them most. Add one sentence that sends readers who are new to the release to the earlier RC notes for its headline features. For a feature release, state the theme in one sentence, then lead with the single most important change and summarize the next few. Any performance claim in the opening carries its number and the baseline from the PR. If the PR gives no number, say what got faster and for whom, or keep the claim out of the opening.

### Highlights

The highlights repeat the ranking, top item first, and each one names the outcome for the user, not the component. A feature release has three to six. A stabilization RC or a patch release has zero to three: only changes that reach a common setup and that a reader would act on. If nothing qualifies, leave the list out rather than promote small fixes to headlines. Keep the highlights and the `## What's New` subsections in the same order.

### Check the opening before the prose

Read the opening as a user of the most common setup, with 15 seconds to spare:

- Can they tell what kind of release this is?
- Is the first change named one that reaches most deployments? If it needs a rare combination of conditions, move it to the grouping sentence.
- Does each named fix read as the symptom they would have seen?
- Do they know whether to upgrade, and what to do first?
- Does every claim keep the conditions and bounds that its PR gives it?

If any answer is no, rewrite the opening before you run the prose checker.

[references/exemplars.md](references/exemplars.md) collects verbatim openings from PostgreSQL, Rust, SQLite, Redis, TimescaleDB, TypeScript, Python, Spark, Go, Kotlin, Deno, and Docker Compose, published 2018–2022, with what each one teaches. Read the section for the kind of release you are writing before you draft the opening. Copy the patterns, not their sentence construction: Simplified Technical English still governs every sentence you write.

## Writing style: Simplified Technical English

Write every sentence you author in Simplified Technical English (ASD-STE100 writing rules). Release notes are read fast, by people deciding whether to upgrade, often in a second language or through a machine translator, and re-read months later by someone bisecting a behaviour change. A 50-word sentence with three subordinate clauses fails all of those readers. Short declarative sentences do not make the content less technical — they make each technical claim separately checkable.

Read [references/simplified_technical_english.md](references/simplified_technical_english.md) before writing prose. It carries the full rules, the rewrite patterns, the terminology table, and worked before/after examples from real Spice releases. The short version:

- **One idea per sentence, 25 words or fewer.** Split where the subject changes. The total gets longer; that is the trade.
- **Active voice, named actor.** "The runtime derives memory budgets from its cgroup limit", not "Memory budgets are derived". Passive is acceptable only when the actor is genuinely unknown.
- **No participle clauses** — no `using`, `enabling`, `including`, `allowing`, `by improving`. Use a finite verb in its own sentence. Established technical nouns that end in `-ing` (caching, sharding, CPU sizing) are fine.
- **Simple tenses.** Present for current behaviour, past for what a release changed.
- **One word, one meaning.** Hold each term for the whole document — `setting`, not a rotation of knob/option/param.
- **Keep `that`, articles, and prepositions.** Dropping them is the ambiguity STE exists to prevent.
- **At most three stacked nouns.** Break longer stacks with a preposition or hyphen.
- **Give the number, not the intensifier.** No `significantly`, `various`, `seamless`, `powerful`, `unlocks`, `under the hood`, `headlined by`. If no measurement exists, describe the mechanism.
- **No semicolons**, and at most one em-dash aside per sentence.
- **Bug fixes lead with the fixed symptom**: one sentence that names what users saw and says it no longer happens, then the cause, then any detail of the current behaviour.

This governs the summary paragraph, the Highlights bullets, every `## What's New` subsection, breaking-change and migration text, and bug-fix descriptions. It does not govern `### Changelog` bullets (they quote PR titles verbatim so they stay greppable against git history), `## Contributors`, `## Upgrading`, `## Cookbook Updates`, the dependency table, code blocks, or any inline identifier.

STE constrains sentence construction, never technical depth. Keep every setting name, metric name, error variant, version number, and YAML sample exact. Never trade a specific mechanism for a vague summary to shorten a sentence.

## Steps

1. **Study previous release notes** in `docs/release_notes/` for **structure and format only** — the sections, their order, the header and date convention, how features are grouped, and the changelog line format. Do **not** copy their sentence construction or their openings: the published files predate Simplified Technical English and the ranking above, so their long multi-clause sentences and their lists of equal-weight highlights are the pattern to break, not to match. Pay attention to:
   - Header format and date conventions (e.g. `# Spice vX.Y.Z (Month D, YYYY)`)
   - How features are grouped and described (subsections under `## What's New`)
   - Level of technical detail per feature; preferred prose vs. bullets; example YAML snippets
   - Enterprise feature callout style (`> [Spice.ai Enterprise]...` blockquote at the top of the subsection)
   - Breaking Changes section format with migration before/after YAML
   - Contributors section format (GitHub profile links, alphabetised)
   - Upgrading instructions format
   - Changelog format with PR links and author attribution

2. **Gather changes** over the range from [Find the commit range](#find-the-commit-range):
   - All non-merge commits: `git log <range> --oneline --no-merges`.
   - For each non-trivial PR, look up the PR title, body and author on GitHub. Commit subjects often lack the user-facing framing the release notes need. Read the body for the symptom, who is affected, any measured numbers with their baseline, and whether the bug was a regression.
     - PR metadata: `gh pr view <num> --json title,body,author,labels`
     - Author handle (use this when the commit author name is ambiguous, bot-mangled, or differs from GitHub login): `gh pr view <num> --json author -q '.author.login'`
   - Identify contributors: `git log <range> --format='%an <%ae>' --no-merges | sort -u`, then map each to a GitHub username using `gh pr view` on one of their PRs and by cross-referencing previous release notes.

3. **Filter noise**. Exclude entirely from both the narrative and the changelog:
   - `dependabot[bot]` and `github-actions[bot]` commits unless they update a user-visible dependency (e.g. DuckDB, Iceberg, Turso) — those go in the dependency table.
   - Test/snapshot updates (`fix(tests): ...`, `chore(benchmarks): ...`, `Update snapshots`, `Disable failing ... test in CI`).
   - Internal refactors with no user-visible behaviour change (e.g. lint deny attributes, internal trait reshuffles).
   - Reverts of changes that never shipped in a prior release.
   - `chore: Clean up Cargo.lock`-style housekeeping, and release-branch version bumps.
   - Significant internal changes (e.g. CI infrastructure rewrites) MAY be included in the detailed changelog at the bottom but never in the highlights or `## What's New`.

4. **Categorize changes** into:
   - Major new features (deserve their own `### Subsection` with description, key points, and YAML examples when configuration changes).
   - Dependency upgrades (presented in a table at the end of `## What's New`).
   - Smaller improvements (bullet list under broader subsections such as `### SQL, Query, and Developer Experience` or `### Caching & Search`).
   - Breaking changes (with before/after migration guidance).
   - Bug fixes (grouped by area, e.g. `### Connector Bug Fixes`), each led by the fixed symptom, as in [the inverted pyramid](#shape-the-notes-as-an-inverted-pyramid).

5. **Rank the changes** as described in [Lead with what matters to the reader](#lead-with-what-matters-to-the-reader). Write the ranked list down, with the symptom, reach, and consequence class for each item, before you draft any prose. The opening and the highlights come from the top of that list.

6. **Write the release notes** in the established structure, with every authored sentence in [Simplified Technical English](#writing-style-simplified-technical-english):
   - `# Spice v<version> (<Month D, YYYY>)` header followed by the opening paragraph from [Write the opening paragraph](#write-the-opening-paragraph).
   - `Highlights in this release candidate include:` (or `in v<version>`) bullet list, sized as in [Highlights](#highlights).
   - `## What's New in v<version>` with subsections for each major feature, in ranked order, each opening with its own lede sentence.
   - `## Breaking Changes` (omit if none).
   - `## Upgrading` with CLI, Homebrew, Docker, Helm, and AWS Marketplace instructions.
   - `## Cookbook Updates` (state "No new cookbook recipes." if none).
   - `## Contributors` with GitHub profile links, alphabetised case-insensitively.
   - `## What's Changed` → `### Changelog` with one bullet per included PR in the form
     `- <title> by [@handle](https://github.com/<handle>) in [#<num>](https://github.com/spiceai/spiceai/pull/<num>)`
   - `**Full Changelog**: <https://github.com/spiceai/spiceai/compare/<prev-tag>...<new-tag>>`

7. **Check the opening** with the four questions in [Check the opening before the prose](#check-the-opening-before-the-prose). Rewrite it until every answer is yes.

8. **Check the prose** before you report the file as finished:

   ```bash
   python3 .agents/skills/write-release-notes/scripts/check_ste.py docs/release_notes/v<version>.md
   ```

   The script reads only the authored prose and reports STE violations with line numbers. **Fix every error** — sentence length, semicolons, participle clauses, vague wording. **Read every warning and decide** — passive voice, noun stacks, paragraph length, and inconsistent terminology all have defensible exceptions, so judge each one rather than rewriting on reflex. If a fix would cost a technical fact, keep the fact and leave the warning; never delete information to satisfy the checker.

   What counts as done depends on the mode:

   - **Create**: re-run until the whole file reports zero errors.
   - **Update**: the checker reads the whole file, but only the sections you added are yours to fix. Re-run until **those sections** report zero errors. The file itself can still report errors, because an existing file may predate these rules. Match each finding's line number against the lines you added to tell the two apart. Leave every error in prose you did not touch — existing prose is the user's editorial decision — and report it instead of rewriting it.

## Ordering

The ranking decides the opening, the highlights, and the order of the `## What's New` subsections. Keep the Highlights bullets and the subsections in the same relative order so the reader can move between them without surprise.

Use the thematic order below to group the remaining subsections and to break ties between changes of similar rank. It never lifts a narrow change above a broader or more severe one.

1. **Spice Cayenne** — first among changes of similar rank when there is meaningful Cayenne news.
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
- The terminology table in [references/simplified_technical_english.md](references/simplified_technical_english.md) fixes one term per concept (dataset, accelerator, the runtime, setting, flag, release). Hold each term for the whole document.

## Documentation links

Link feature names in subsections to the appropriate documentation host. Pick the host based on where the feature is documented, not by audience:

- **OSS / runtime features** → `https://spiceai.org/docs` (e.g. `https://spiceai.org/docs/components/data-connectors/postgres`).
- **Spice.ai Cloud** → `https://docs.spice.ai/docs` (e.g. `https://docs.spice.ai/docs/api/sql`).
- **Spice.ai Enterprise** → `https://docs.spice.ai/docs/enterprise` (e.g. `https://docs.spice.ai/docs/enterprise/features/distributed-accelerations`). Enterprise subsections also get the `> [Spice.ai Enterprise](https://docs.spice.ai/docs/enterprise) feature. See [...](<deep-link>).` blockquote at the top.

Verify any new doc link you introduce actually resolves. If a deep link cannot be confirmed, link to the section root instead.

## Output

Save the release notes as `docs/release_notes/v<version>.md`. In update mode, edit the existing file in place and commit with a `docs(release): update v<version> notes with latest trunk PRs` style message.

Report the commit range you used and why, the ranked list behind the opening, and the checker result with the file. State the error count for the prose you authored (zero, once fixed) and name any warning you chose to keep, with the reason. In update mode, list the pre-existing errors you left in place as a separate count, so a reader does not read your zero as a whole-file zero, and propose a new opening if a new change outranks the current lead. A silent pass tells the reviewer nothing about which judgements you made.
