# Release-note openings worth copying

Human-written release notes from projects whose notes are widely respected, published 2018–2022. Each quote was copied from the fetched page on 2026-10-21 and is trimmed with `…`. They are here for their **patterns**: what they put first, how they describe a fix, how they tell the reader what to do. Do not copy their sentence construction. Simplified Technical English still governs every sentence you write.

Read the section for the kind of release you are writing.

- [Fix-driven and patch releases](#fix-driven-and-patch-releases)
- [Release candidates](#release-candidates)
- [Feature releases](#feature-releases)
- [Breaking changes and required actions](#breaking-changes-and-required-actions)
- [Anti-patterns](#anti-patterns), including two from published Spice notes

## Fix-driven and patch releases

### PostgreSQL 14.4 (2022-06-16) — one severe fix leads, the rest is a count

> The PostgreSQL Global Development Group has released PostgreSQL 14.4 to fix an issue that could cause silent data corruption when using the CREATE INDEX CONCURRENTLY or REINDEX CONCURRENTLY commands. Please see the following section for information on how to detect and correct silent data corruption in your indexes.
>
> This release also fixes over 15 bugs since PostgreSQL 14.3 was released in May.

<https://www.postgresql.org/about/news/postgresql-144-released-2470/>

- Sentence one is a purpose clause: the release exists *to fix* one thing, and it names the consequence ("silent data corruption") and the trigger.
- The body states the symptom in user terms: "causing SELECT queries that used the index to not find certain rows."
- It is honest about likelihood: "This issue may not have corrupted your indexes, but if you are unsure, we advise you to reindex."
- It gives a detect command and a repair command, and puts that action before the changelog.
- Everything else is one count and a link.

### PostgreSQL 14.2 (2022-02-10) — a rare but severe fix leads the list, not the opening

> The PostgreSQL Global Development Group has released an update to all supported versions of PostgreSQL, including 14.2, 13.6, 12.10, 11.15, and 10.20. This release fixes over 55 bugs reported over the last three months.

<https://www.postgresql.org/about/news/postgresql-142-136-1210-1115-and-1020-released-2402/>

- Nothing in the release reached most users, so the opening is a count and a time window. It does not promote any single fix.
- The list below it starts with the most severe item and says how unlikely it is: "Fix for a low probability scenario of index corruption when a HOT (heap-only tuple) chain changes state during VACUUM. Encountering this issue is unlikely, but if you are concerned, please consider reindexing."
- Compare 14.4 above: there, the corruption came from commands that many users run, so it led the opening.

### Rust 1.52.1 (2021-05-10) — the recommendation is in the first paragraph

> The Rust team has prepared a new release, 1.52.1, working around a bug in incremental compilation which was made into a compiler error in 1.52.0. We recommend all Rust users, including those currently using stable versions prior to 1.52.0, upgrade to 1.52.1 or disable incremental compilation.

<https://blog.rust-lang.org/2021/05/10/Rust-1.52.1/>

- One sentence says what the release does, what triggered it, and which release exposed the problem. The next gives the action, scoped to an audience, with a workaround.
- It bounds the blast radius: "Incremental compilation is off by default for release builds, so few production builds should be affected (only for users who have opted in)."
- It quotes the exact error text ("found unstable fingerprints"), so users can match it against their logs and find the post by search.

### SQLite 3.37.2 and 3.35.1 — consequence class first, and who need not upgrade

> Version 3.37.2 fixes a database corruption bug. You are encouraged to upgrade, especially if you are using SAVEPOINT. The problem first appeared in version 3.35.0 (2021-03-12) and affects all subsequent releases through 3.37.1.

> A user discovered an issue with the new DROP COLUMN capability in version 3.35.0, and so version 3.35.1 was created to fix it. No need to upgrade if you are not using DROP COLUMN.

<https://www.sqlite.org/news.html>

- The first words name the class of consequence: corruption, incorrect answers, a crash.
- The affected range ("first appeared in … through …") tells readers whether staying put protects them.
- Telling some readers they do *not* need to upgrade is as useful as telling others they do.
- Likelihood in plain words: "with low but non-zero probability", "incorrect answers for some obscure queries".

### Redis 6.0.4 (2020-05-28) — urgency label, then who is safe

> Upgrade urgency CRITICAL: this release fixes a severe replication bug.

> People starting with Redis 6.0.4 are fine. People with Redis 5 are fine. People upgrading from Redis 5 to Redis 6.0.4 are fine. TLDR: The problem is with users of 6.0.0, 6.0.1, 6.0.2, 6.0.3.

<https://raw.githubusercontent.com/redis/redis/6.0/00-RELEASENOTES>

- The verdict comes before anything else, and the affected audience is listed exactly.
- Redis 6.2.0 makes the verdict depend on the reader: "Upgrade urgency: SECURITY if you use 32bit build of redis …, MODERATE if you used earlier versions of Redis 6.2, LOW otherwise."
- Redis groups fixes by where the bug came from: "Bug fixes for regressions in previous releases of Redis 6.2", and "Bug fixes that are only applicable to previous releases of Redis 6.2" (bugs that existed only in pre-releases).

### TimescaleDB 2.1.1 (2021-03-29) — a mostly-fix release, grouped by area

> This maintenance release contains bugfixes since the 2.1.0 release. We deem it high priority for upgrading.
>
> The bug fixes in this release address issues with CREATE INDEX and UPSERT for hypertables, custom jobs, and gapfill queries.

<https://raw.githubusercontent.com/timescale/timescaledb/main/CHANGELOG.md>

- Release type and priority in two sentences, then one sentence that groups the fixes by the user-facing area they affect. That gives the reader a theme without listing tickets.

### Django 4.0.1 (2022-01-04) — counts and severity, then provenance on every fix

> Django 4.0.1 fixes one security issue with severity "medium", two security issues with severity "low", and several bugs in 4.0.

<https://docs.djangoproject.com/en/4.0/releases/4.0.1/>

- Every bug-fix line says where the bug came from: "Fixed a regression in Django 4.0 that caused a crash of …", "Fixed a bug in Django 4.0 that …", "Fixed a long standing bug in …". Readers learn whether staying on the old version would have protected them.
- Each fix names the API and the condition, so readers can decide for themselves whether they were affected.

## Release candidates

### TypeScript 4.5 RC (2021-11-02) — stability promise, then the biggest change since the last preview

> Today we're excited to announce our Release Candidate (RC) of TypeScript 4.5! Between now and the stable release of TypeScript 4.5, we expect no further changes apart from critical bug fixes.

> The biggest change we've made since the beta is that ECMAScript module support for Node.js 12 has been deferred to a future release …

<https://devblogs.microsoft.com/typescript/announcing-typescript-4-5-rc/>

- It routes two kinds of reader: those who read the previous preview go to "What's New Since the Beta?", and everyone else gets the release's highlights.
- The delta is ranked, and its top item is the one that changes expectations (a deferral), not the most novel one.
- A regression fix is written for the people it blocked: "If this regression blocked you from trying TypeScript 4.4, you should see comparable or better speed in --build mode compared to past versions."

### Python 3.9.0rc1 (2020-08-11) — position in the sequence and the change-control rule

> This release, 3.9.0rc1, is the penultimate release preview. Entering the release candidate phase, only reviewed code changes which are clear bug fixes are allowed between this release candidate and the final release.

<https://www.python.org/downloads/release/python-390rc1/>

- It says where the release sits ("penultimate release preview") and what can still change.
- The call to action names its audience: "We strongly encourage maintainers of third-party Python projects to prepare their projects for 3.9 compatibility during this phase."
- It warns: "Please keep in mind that this is a preview release and its use is not recommended for production environments."
- Weakness: the rc2 page repeats the feature list but never says what changed since rc1, so rc1 testers cannot tell whether their bug is fixed.

### PostgreSQL 14 RC 1 (2021-09-23) — what an RC promises, and the GA date

> As a release candidate, PostgreSQL 14 RC 1 will be mostly identical to the initial release of PostgreSQL 14, though some more fixes may be applied prior to the general availability of PostgreSQL 14.
>
> The planned date for the general availability of PostgreSQL 14 is September 30, 2021.

<https://www.postgresql.org/about/news/postgresql-14-rc-1-released-2309/>

- Its "Changes Since 14 Beta 3" section leads with a revert, the change that resets expectations, before ordinary fixes.

### Redis 7.0 RC1 (2022-01-31) — what an RC is for, and what to watch for

> Upgrade urgency LOW: This is the first Release Candidate of Redis 7.0.
>
> Redis Release Candidate (RC) versions are early versions that are made available for early adopters in the community to test them. We do not consider them suitable for production environments.

<https://raw.githubusercontent.com/redis/redis/7.0/00-RELEASENOTES>

- Changes that can break an upgrade come before features: "In particular, users should be aware of the following changes:".
- Later RCs list only what changed "compared to the previous release candidate".

### A skeleton for a stabilization RC

Assembled from the examples above. Fill each slot from the PRs, and leave a slot out when the PRs do not support it.

1. Identity, position, and purpose: "Spice vX.Y.0-rc.N is the Nth release candidate for vX.Y.0. Since rc.N−1 it contains only fixes." (PostgreSQL 14.4, Python 3.9.0rc1, TypeScript 4.5 RC)
2. The one to three top-ranked fixes, each as what users saw and who it affected. A wrong-result fix leads when a common setup could hit it. When it needs a rare combination of conditions, name it by its symptom in the grouping sentence instead, and put it first in its section. (PostgreSQL 14.4 and 14.2, SQLite, Django)
3. Provenance where the PR states it: a regression in rc.K, or a long-standing bug. (Redis, Django)
4. One sentence that counts and groups the rest by area. (PostgreSQL 14.4, TimescaleDB 2.1.1)
5. Who should upgrade or retest, and who need not. Point to required actions if there are any. (Redis, SQLite 3.35.1, Rust 1.52.1)
6. One sentence that sends readers who are new to the release to the earlier RC notes for its headline features. (TypeScript 4.5 RC)

## Feature releases

### Apache Spark 3.2.0 (2021-10-13) — one headline, then a single list

> In this release, Spark supports the Pandas API layer on Spark. Pandas users can scale out their applications on Spark with one line code change. Other major updates include RocksDB StateStore support, session window support, push-based shuffle support, ANSI SQL INTERVAL types, enabling Adaptive Query Execution (AQE) by default, and ANSI SQL mode GA.

<https://spark.apache.org/releases/spark-release-3-2-0.html>

- One headline, chosen for the largest new audience and stated as a benefit with its cost ("one line code change"). Everything else is one list.

### Go 1.18 (2022-03-15) — ranked headlines with evidence of demand

> Go 1.18 is a massive release that includes new features, performance improvements, and our biggest change ever to the language.

> Apple M1, ARM64, and PowerPC64 users rejoice! Go 1.18 includes CPU performance improvements of up to 20% … Just to underscore how big this release is, a 20% performance improvement is the fourth most important headline!

<https://go.dev/blog/go1.18>

- The headlines are ordered by impact and justified by demand ("Go's most often requested feature"; the most common challenge in the user survey).
- It names the users who benefit, and it is honest about scope: generics cover what "the majority of users need today".

### Python 3.11 What's New (2022-10-24) — the number, the baseline, and how it was measured

> Python 3.11 is between 10-60% faster than Python 3.10. On average, we measured a 1.25x speedup on the standard benchmark suite.

<https://docs.python.org/3/whatsnew/3.11.html>

- A change that every user gets with no code change comes before every new syntax feature.

### Kotlin 1.4 (2020-08-17) — make stability the theme when it is the story

Title: "Kotlin 1.4 Released with a Focus on Quality and Performance".

> We've reviewed and fixed more than 60 performance issues, including many that were causing IDE freezes or memory leaks.

<https://blog.jetbrains.com/kotlin/2020/08/kotlin-1-4-released-with-a-focus-on-quality-and-performance/>

- The fixes are counted and described by the symptoms users felt, and the outcome is measured ("highlighting is now 1.5-4 times faster").

### Deno 1.9 (2021-04-13) — "name: benefit", with a number on every performance claim

> Native HTTP/2 web server: a fast, correct, fully featured HTTP server in Deno. Faster calls into Rust with serde_v8: 98% improvement to our baseline op overhead.

<https://deno.com/blog/v1.9>

### ClickHouse 22.1 (2022-01-26) — show the before and after

The schema-inference section shows a query with a 63-line hand-written schema, then says "In the new ClickHouse release 22.1 it becomes much easier:" and shows the one-line query. <https://clickhouse.com/blog/whats-new-in-clickhouse-22-1>

## Breaking changes and required actions

- **Docker Compose V2 GA (2022-04-26)** uses a three-column table, "What's Changed / Potential Impact / Mitigation", and gives the default answer for most readers: "In most cases, you won't need to do anything." <https://www.docker.com/blog/announcing-compose-v2-general-availability/>
- **Kubernetes 1.24 (2022-05-03)** leads its major themes with a removal, the change most likely to break an upgrade, written as "From v1.24 onwards, you will need to either … or …". <https://kubernetes.io/blog/2022/05/03/kubernetes-1-24-release-announcement/>
- **Terraform 1.0 (2021-06-08)** scopes an end-of-life by audience: "Users of the Terraform CLI and Terraform Cloud are not affected by this and do not need to take any action."
- **Django 4.0 (2021-12-07)** pairs every deprecation with its removal version and a transition setting: "Support for pytz is now deprecated and will be removed in Django 5.0." <https://docs.djangoproject.com/en/4.0/releases/4.0/>

## Anti-patterns

- **The most interesting change leads instead of the most widely felt one.** Kafka 3.0 gives its first two paragraphs to KRaft, which "is not yet recommended for production". The producer default change that reaches every user comes third.
- **A correctness fix worded as an enhancement.** "<Feature> now handles …" hides that queries previously returned wrong or incomplete results. The exemplars name the wrong result ("to not find certain rows", "incorrect answers") and who saw it.
- **Adjectives instead of numbers.** CockroachDB v21.1.0: "we've made a variety of flexibility, performance, and compatibility improvements". Spice v2.3.0 opens with "brings performance improvements, broader query federation, and expanded data connector capabilities", which names three categories and no change.
- **A highlight list sized for a feature month, used for a fix release.** Nine equal "key highlights" suit a monthly editor release. In a patch or stabilization release they promote small fixes to headlines and hide the one or two that matter. Spice v2.3.2 has nine highlights, including three narrow fixes. Spice v2.3.0's first highlight is "SQL Federation Improvements — bug fixes for function translation, NULL handling, and timezone declarations".
- **Components instead of effects.** Go point releases say "includes fixes to the compiler, linker, runtime, …". That is a fine index, but readers cannot tell whether their bug is fixed. A list of subsystems ("fixes X commits, Y storage faults, and Z models") has the same problem.
- **The most severe fix buried.** Apache Arrow 6.0.1 calls itself "mostly a bugfix release" and puts "Fixed a potential data loss in S3 multipart upload" as the third bullet under "C++ notes".
- **Ceremony before content.** Vote dates, codenames, community statistics, and event plugs before the first change push the substance below the fold (Spark 3.0.0, Kubernetes enhancement counts, Grafana 7.0's cloud sign-up paragraph).
- **An RC that never says what changed since the previous RC.** Python 3.11.0rc2 repeats the cumulative feature list, so rc1 testers cannot tell whether their bug is fixed.
