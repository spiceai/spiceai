# Simplified Technical English for Spice release notes

Release notes are the most widely read document the project publishes, and they
are read under the worst conditions: quickly, by someone deciding whether to
upgrade, often by a reader whose first language is not English, often through a
machine translator, and often months later by an engineer who is bisecting a
behaviour change. Simplified Technical English (ASD-STE100) exists for exactly
that audience. It is a set of writing rules that make a sentence carry one
unambiguous meaning.

This guide adapts those rules to Spice release notes. It applies the STE
*writing rules*. It does not apply the STE approved-word list, because the
vocabulary of this product — accelerator, federation, replication slot, Vortex —
is technical naming that the standard explicitly permits and that a reader needs.

## Contents

- [What the rules govern](#what-the-rules-govern)
- [The rules](#the-rules)
- [Rewrite patterns](#rewrite-patterns)
- [Terminology](#terminology)
- [What STE does not mean](#what-ste-does-not-mean)
- [Worked example](#worked-example)
- [The checker](#the-checker)

## What the rules govern

STE governs prose that an author writes:

- the summary paragraph under the title
- the `Highlights in vX.Y.Z include:` bullets
- every `## What's New` subsection, including the sentences around YAML samples
- `## Breaking Changes` descriptions and migration text
- bug-fix descriptions

STE does not govern text the release process owns, because rewriting it destroys
its purpose:

- `### Changelog` bullets quote PR titles verbatim, which is what makes them
  greppable against git history
- `## Contributors`, `## Upgrading`, `## Cookbook Updates` boilerplate
- code blocks, YAML samples, JSON, commands, and inline identifiers
- the dependency table

## The rules

### 1. One idea per sentence, 25 words or fewer

A long sentence forces the reader to hold several facts open at once. Split at
the point where the subject changes.

> **Before** (55 words) — A federated query combining a `LEFT JOIN` with a
> `WHERE` filter on the left table previously returned all rows instead of the
> filtered rows: when the query was pushed down to the data source or
> accelerator, the filter was folded into the `JOIN ON` clause, where it no
> longer filters the left side (`RIGHT JOIN` was affected symmetrically).

> **After** — A federated query with a `LEFT JOIN` and a `WHERE` filter on the
> left table returned all rows instead of the filtered rows. The planner pushed
> the query down to the data source or accelerator and folded the filter into
> the `JOIN ON` clause. A filter in that clause does not filter the left side.
> `RIGHT JOIN` had the same fault on the right side.

The rewrite is longer in total. That is expected and correct: STE trades total
length for the number of ideas the reader holds per sentence.

### 2. Use the active voice and name the actor

Passive voice hides who does the work. In release notes the actor is almost
always known — the runtime, the planner, the accelerator, a specific component —
and naming it is what turns a vague claim into a testable one.

> **Before** — Memory budgets are now derived from the process's own cgroup limit.
>
> **After** — The runtime now derives memory budgets from its own cgroup limit.

Passive voice is acceptable where the actor is genuinely unknown or irrelevant
(`The setting is deprecated.`). The checker reports passive voice as a warning,
not an error, so you can make that judgement.

### 3. No participle clauses

`-ing` clauses (`using`, `enabling`, `including`, `allowing`, `by improving`)
attach a second idea to a sentence without saying who performs it or when. Use a
finite verb in its own sentence.

> **Before** — Memory pool refusals now return `ResourcesExhausted` and HTTP
> `503`, distinguishing them from query errors.
>
> **After** — Memory pool refusals now return `ResourcesExhausted` and HTTP
> `503`. A caller can tell them apart from query errors.

`-ing` words that are established technical nouns are fine: *caching*,
*sharding*, *logging*, *CPU sizing*, *prompt caching*. The rule targets the
clause, not the letters.

### 4. Use simple tenses

Use the simple present for current behaviour, and the simple past for the
behaviour a release changes. Avoid `would have`, `will have been`, and
progressive forms (`is running`, `was failing`).

> **Before** — Accelerating an Iceberg dataset with a `timestamptz` column using
> the Cayenne engine previously failed during the refresh write.
>
> **After** — Cayenne acceleration of an Iceberg dataset with a `timestamptz`
> column failed during the refresh write.

### 5. One word, one meaning

Use the same word for the same thing every time, even when repetition feels
dull. A reader who sees `setting` in one paragraph and `knob`, `option`, and
`param` in the next has to decide whether the writer means four different
things. See [Terminology](#terminology).

### 6. Do not drop `that`, articles, or prepositions

Compression that removes function words creates the exact ambiguity STE exists
to prevent, and it defeats machine translation.

> **Before** — Fixes bug setting `runtime.task_history.enabled: false` also
> disabled query metrics.
>
> **After** — This release fixes a bug where the setting
> `runtime.task_history.enabled: false` also disabled the query metrics.

### 7. Stack at most three nouns

Four nouns in a row make the reader guess which one is the head and which are
modifiers. Break the stack with a preposition or a hyphen.

> **Before** — per-batch directory barrier coalescing latency
>
> **After** — the latency of directory-barrier coalescing per batch

### 8. Give the number, not the intensifier

`significantly`, `dramatically`, `a number of`, `various` all mean "the author
did not measure it". If a measurement exists, publish it. If none exists,
describe the mechanism instead.

> **Before** — Significantly improves CDC throughput.
>
> **After** — Cuts replication lag on high-volume CDC workloads from 40s to
> under 5s at SF-1000.

Do not write marketing metaphor: `seamless`, `powerful`, `blazing fast`,
`unlocks`, `under the hood`, `out of the box`, `headlined by`. Each one replaces
a fact with a mood.

### 9. Put the condition before the action

The reader must know whether a sentence applies to them before they read what to
do about it.

> **Before** — Set `pg_replication_slot` to the same name on each dataset if you
> run more than one `changes`-mode dataset on one connection.
>
> **After** — If you run more than one `changes`-mode dataset on one connection,
> set `pg_replication_slot` to the same name on each dataset.

### 10. Keep paragraphs to six sentences, and use lists for parallel items

Three or more items of the same kind belong in a bulleted list, not in a
sentence joined by commas and `and`.

### 11. No semicolons, and no chains of em dashes

A semicolon joins two independent clauses, which is rule 1 broken with different
punctuation. A single em-dash aside is acceptable. Two in one sentence is a sign
the sentence holds three ideas.

### 12. Describe a bug fix in a fixed order

Symptom, then cause, then the current behaviour. This order lets a reader stop
as soon as they know the bug is not theirs.

> A federated query with a `LEFT JOIN` returned all rows instead of the filtered
> rows. *(symptom)* The planner folded the `WHERE` filter into the `JOIN ON`
> clause, where it does not filter the left side. *(cause)* Filters now stay on
> the side of the join they came from. *(current behaviour)*

## Rewrite patterns

| Instead of | Write |
| --- | --- |
| X, enabling Y | X. Y is now possible. / X. This lets you Y. |
| X, including A and B | X. This covers A and B. |
| by leveraging X | with X |
| A is derived from B | B determines A / The runtime derives A from B |
| in order to | to |
| is able to / has the ability to | can |
| provides support for | supports |
| makes use of | uses |
| a number of / various | three (give the count) |
| due to the fact that | because |
| prior to | before |
| utilize | use |
| headlined by X | The main change is X. |
| X was affected symmetrically | X had the same fault. |
| This release brings X | This release adds X. |

## Terminology

Pick one term per concept and hold it for the whole document.

| Concept | Use | Do not use |
| --- | --- | --- |
| The Cayenne accelerator | **Spice Cayenne** on first mention, **Cayenne** after | Cayenne engine, the Cayenne |
| A Spicepod dataset | dataset | table, source table |
| The acceleration engine | accelerator | engine (except as the YAML key `engine:`) |
| `spiced` | the runtime | the server, the daemon, the binary |
| A Spicepod or `params` value | setting | knob, option, param, config |
| A command-line option | flag | switch, argument |
| A published version | release | drop, ship |
| Change data capture | CDC after first expansion | change-data-capture, change data capture (mixed) |

Write a product name the way the product writes it: `Spice.ai Enterprise`,
`DataFusion`, `PostgreSQL`, `DuckDB`, `Iceberg`, `Vortex`.

## What STE does not mean

STE constrains sentence construction. It does not constrain technical depth, and
using it as a reason to publish less information is a misuse of it.

- **Keep every identifier exact.** Setting names, metric names, error variants,
  SQLSTATE codes, and version numbers are the reason someone greps the notes.
- **Keep the YAML and the code samples.** They are worth more than the prose
  around them.
- **Keep the mechanism.** "The runtime sizes itself for every core on the node
  instead of its allocated share" is a specific, checkable claim. Do not reduce
  it to "improves CPU handling".
- **Do not write for a beginner.** The reader knows what a join and a replication
  slot are. Short sentences are for speed, not for simplification of the subject.
- **Headings stay as they are.** Subsection titles are labels and follow the
  existing style.

## Worked example

**Before** — the v2.1.0 opening paragraph, as published:

> Spice v2.1.0 is the next minor release of Spice, headlined by
> **high-throughput Cayenne CDC**, scaling and resilience improvements to
> **PostgreSQL logical replication**, expanded **distributed query** with
> Iceberg catalog scans and broadcast joins, and the upgrade to **DataFusion
> v54** (including v53), Arrow v58.3, and Vortex v0.74.

One sentence, 54 words, five ideas, one metaphor (`headlined by`), one participle
(`including`).

**After**:

> Spice v2.1.0 is a minor release. Spice Cayenne now sustains higher CDC write
> throughput. PostgreSQL logical replication scales to more datasets and
> recovers from more failures. Distributed query adds Iceberg catalog scans and
> broadcast joins. This release also upgrades DataFusion to v54, which folds in
> v53, and it upgrades Arrow to v58.3 and Vortex to v0.74.

Five sentences, each one idea, longest 25 words, no metaphor, no participle. The
facts and version numbers are unchanged.

## The checker

```bash
python3 .github/prompts/scripts/check_ste.py docs/release_notes/v<version>.md
```

The script reads only the prose sections listed in
[What the rules govern](#what-the-rules-govern). It reports:

- **errors** — sentence length, semicolons, participle clauses, vague or
  figurative wording. Fix all of these.
- **warnings** — passive voice, noun stacks, paragraph length, inconsistent
  terminology. Read each one and decide. Passive voice with a genuinely unknown
  actor is fine.

`--json` prints the metrics, which is how the evals in
`.github/prompts/evals/` score a run.
