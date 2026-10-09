# Fork patches and their guards

Spice builds against forks of 30 upstream crates. Some of those forks carry Spice
patches; the rest are pinned for a version or dependency reason and carry no
behaviour of ours at all.

A patch on a fork exists only as a commit on a fork branch, and every fork branch is
re-cut when its upstream releases a new major (`spiceai-52` → `-53` → `-54`, …). A
patch that is not deliberately carried across a re-cut is **lost silently**: nothing
fails, the crate reverts to upstream behaviour, and the defect the patch fixed
returns in the next Spice release. The fork's own tests are no protection — they are
on the branch that was replaced, so they leave with the patch.

This has already happened. Vortex `Map` support shipped on `spiceai-51`, `-52` and
`-53` and was absent from `-54`; half the patch survived, so it surfaced not as a
build failure but as `Array encoding not implemented for Arrow data type Map(...)`
on every write in a released build ([#13524](https://github.com/spiceai/spiceai/issues/13524)).
A reentrant-waker use-after-free in `vortex-io` was fixed three separate times on
branches that never reached a shipping one, and shipped as a `SIGSEGV` under
ordinary task cancellation.

So the protection has to live **here**, in `spiceai/spiceai`, where it survives the
re-cut. This file is the ledger of what that protection covers.

## How to use this

**Pinning at a pull request's branch.** A change that spans this repository and a
fork is reviewed with the pin on the fork's pull request branch. That is fine to
open and must not land: the branch is deleted when the fork's pull request
merges, so trunk would name a revision no branch reaches. Mark such a row
`(TEMPORARY: <the pull request that has to merge first>)` in its branch cell;
`scripts/check_fork_patches.py` fails while the marker is present, so the pin
cannot land by being forgotten. Remove it when you repoint at the merge commit.

**At every fork pin bump**, for the fork you moved:

1. Diff the new revision against its upstream merge base and enumerate the Spice
   commits, or read the fork's own `SPICE_PATCHES.md` / `SPICE_FORK_CHANGES.md`
   where it has one. Cargo's clone of the fork holds no upstream remote and no
   tags, so give it one in a scratch repo that borrows its objects rather than
   re-downloading them:

   ```sh
   db=~/.cargo/git/db/<fork>-<hash>          # the clone cargo already has
   git init --bare /tmp/<fork>.git
   echo "$db/objects" > /tmp/<fork>.git/objects/info/alternates
   git --git-dir /tmp/<fork>.git fetch --no-tags "$db" '+refs/*:refs/fork/*'
   git --git-dir /tmp/<fork>.git remote add upstream https://github.com/<owner>/<fork>.git
   git --git-dir /tmp/<fork>.git fetch --no-tags upstream '+refs/heads/*:refs/remotes/upstream/*'

   base=$(git --git-dir /tmp/<fork>.git merge-base <new-rev> refs/remotes/upstream/<default-branch>)
   git --git-dir /tmp/<fork>.git log --no-merges --format='%h %an%x09%s' "$base..<new-rev>"
   git --git-dir /tmp/<fork>.git cherry -v refs/remotes/upstream/<default-branch> <new-rev> "$base"
   ```

   The `fetch` from `$db` first is what makes the upstream fetch cheap — with no
   refs to negotiate against, git asks for the entire history. `cherry` marks each
   commit `+` (ours) or `-` (an upstream commit cherry-picked ahead of a release),
   which is what separates a Spice patch from a backport on a release branch.
2. For every patch in that fork's table below, confirm it is still present, or
   record that it landed upstream and drop the row.
3. Run the guards named in the **Guard** column. They must pass on the new pin.
4. Update the fork's revision in the pin table. `scripts/check_fork_patches.py`
   (run by `make lint-rust`) fails the build until you do, which is the point: the
   ledger is only true of the revision it names.

**When you add a patch to a fork**, add its row here in the same change, with a
guard. A patch with no guard is a patch the next re-cut can drop for free.

## What counts as a guard

A guard is a test **in this repo** that fails when the patch is missing. Not a
comment, not a test in the fork.

**And one the gate runs.** A test nothing runs makes this table claim coverage
that does not exist, which is worse than a **GAP** — a gap is at least on the
list below. `kind(=lib)` sweeps up every unit test, so the exposure is the
integration-test targets, which `NEXTEST_FILTER` has to name one at a time;
`--all --tests` compiles them either way, so an unnamed binary is built and then
skipped. Guards were found running nowhere for that reason, in integration-test
targets now named there: `adbc_cancellation` for the two `arrow-adbc`
cancellation rows (fork PRs #4 and #65, which share one test), and `cpu_budget`
for vortex's `set_available_parallelism`. (`json_semantics` is named there too; it
guarded the `datafusion-functions-json` fork, which is now taken from crates.io.)
Nothing yet checks this automatically — adding a guard here means checking by hand that `NEXTEST_FILTER` selects the
target it lives in, or that a workflow of its own runs it.

The **Loss** column says how a missing patch would surface:

- **silent** — it compiles and runs, and returns different results, hangs, crashes
  or degrades. These are the rows that need a behaviour test.
- **build** — the patch adds or changes an API this workspace calls, so losing it
  fails `cargo check`. The compiler is the guard. Worth recording anyway, because a
  re-cut that keeps the signature and drops the behaviour turns a **build** row into
  a **silent** one, and only a reader of this table would notice.

A row whose guard is **GAP** has no repo-side coverage today. Those are listed
together in [Open gaps](#open-gaps).

## Pinned revisions

Machine-checked against `Cargo.lock` by `scripts/check_fork_patches.py`. One row per
fork; the revision must be the one cargo resolves. What each fork carries is in its
own section below — a count here would be one more thing to keep true by hand.

| Fork | Pinned revision | Branch |
|---|---|---|
| [arrow-adbc](#arrow-adbc) | `6e4119ac2007c6647702a1817505d6849b07e0e0` | `spiceai-24` |
| [arrow-rs](#arrow-rs) | `2e2cc330c64ac8a9e44d2a5f2da171b391f775b8` | `spiceai-59` |
| [async-openai](#async-openai) | `6bda5533dd118afcf80aa6f5ef59ad35277627a7` | `spiceai` |
| [candle](#candle-and-its-kernel-crates) | `efbb9a72e92789eafed0806c3e16f14640c504f6` | `lukim/spiceai-0.11.0` |
| [candle-cublaslt](#candle-and-its-kernel-crates) | `c41bf9c6e87195749c2262d16ca320af2bbebbfe` | `main` |
| [candle-index-select-cu](#candle-and-its-kernel-crates) | `75fc0b689b33a327907d36dd479f7d242640ca71` | `master` |
| [candle-layer-norm](#candle-and-its-kernel-crates) | `dfdbfbb953ceeb0366e5e3b69f2933204309d3dd` | `main` |
| [candle-rotary](#candle-and-its-kernel-crates) | `e12f91a6c8beec5373ccec91a5ccad80619cf065` | `main` |
| [clickhouse-rs](#clickhouse-rs) | `20153c9c8eea6f1939dd03fc95e50198854f7fbf` | `14921-clickhouse-types` (TEMPORARY: spiceai/clickhouse-rs#2) |
| [datafusion](#datafusion) | `eea120e236447a70d7c8802401b3ed3ee24f0980` | `spiceai-55` |
| [datafusion-ballista](#datafusion-ballista) | `a7c4c58502a16e2181a26fdb8e937ee005807e5e` | `spiceai-55` |
| [datafusion-federation](#datafusion-federation-and-datafusion-table-providers) | `750561d79e88fd48afd06b6a136e3bc1dc2b8a12` | `spiceai-55` |
| [datafusion-table-providers](#datafusion-federation-and-datafusion-table-providers) | `b4a86350d571921829e5ad141d94dc2470f98b17` | `spiceai-55` |
| [delta-kernel-rs](#delta-kernel-rs) | `16ac28606464d742b6837de4a51f41011c3f6dc0` | `spiceai-0.27` |
| [docx-rs](#docx-rs) | `2a85dce57d0128e2cd7c369545516c347cb8c529` | `spiceai` |
| [duckdb-rs](#duckdb-rs) | `504debbf8269a76aef74daa2247b6296c2fe2290` | `spiceai-1.4.4` (datafusion-table-providers pins this exact revision; the two move together) |
| [graph-rs-sdk](#graph-rs-sdk) | `25bc483efc3200df7a4f5426c176cddb18a84ad9` | `spiceai` |
| [iceberg-rust](#iceberg-rust) | `7735caa2d9a839ad81aa6a1316932c99ea3019c5` | `spiceai-0.11.0-df-55` |
| [mistral.rs](#mistralrs-and-text-embeddings-inference) | `2d15d171236803481d582a9fbf8a80869bf74d8c` | `spiceai` |
| [model2vec-rs](#model2vec-rs) | `55fef28a3556895b20204634b788f7c836b610bc` | `spiceai` |
| [reqwest-eventsource](#dependency-only-forks) | `eb11e695128ce264bf05e4220ce2311c25992c73` | `spiceai` |
| [rusqlite](#rusqlite-and-tokio-rusqlite) | `e39c9c46dea1f0983cd8d87dabb69b41c9efe1fd` | `master` |
| [sea-query](#sea-query) | `ae75baef819513fb8d19af014972dcfa324e201a` | `spiceai` |
| [snowflake-rs](#snowflake-rs) | `f5557381f79f1535014e0dc0555c53ac8de1cafc` | `spiceai-59` |
| [spark-connect-rs](#spark-connect-rs) | `18ae9bd3ac5c447612f292e41bf348a5c6ed9b50` | `spiceai-59-2` |
| [text-embeddings-inference](#mistralrs-and-text-embeddings-inference) | `ac4e457936bc11c9b4fee453f2be33133d3146d8` | `spiceai` |
| [text-splitter](#text-splitter) | `58f9c21006e01e5e968c5de80a0398b3f5ec439a` | `spiceai` |
| [tiberius](#dependency-only-forks) | `9ae93c65222b51b0579945ffce5cba053cb23cca` | `spiceai` |
| [tokio-rusqlite](#rusqlite-and-tokio-rusqlite) | `b10df82e3bbc4f4700562a14a3a00714cbc2f0c7` | `spiceai` |
| [vortex](#vortex) | `806e48da24401eab673ef88ea93863cf8b1035e7` | `spiceai-55` |

`spiceai/spice-rs` and `spiceai/spicebench` are also pinned as git dependencies but
are not forks — they are Spice repositories with no upstream, so nothing can drop a
patch from them. They are excluded from the guard.

`datafusion-functions-json` is no longer a fork. Its branch carried no Spice patch —
only three upstream correctness fixes (upstream PRs #121, #124, #125) that had not
been released — and all three ship in upstream `v0.55.6`, which the workspace now
takes from crates.io. Their tests stay where they were, in
`crates/runtime-udfs-api/tests/json_semantics.rs`, and now guard the release rather
than a pin.

---

## vortex

Upstream [vortex-data/vortex](https://github.com/vortex-data/vortex). The fork
carries its own ledger, `SPICE_PATCHES.md`, which tracks the patch set and the
*fork-side* verification for each. The rows below are the *repo-side* half: what
fails **here** when a patch goes missing.

The Vortex `vortex-datafusion` crate is vendored into this repo as `crates/vortex`,
so patches to it are not fork state and are not listed. Only patches to
`vortex-array`, `vortex-arrow`, `vortex-io`, `vortex-file`, `vortex-layout` and
`vortex-utils` can be lost by a re-cut.

The move to `spiceai-55` (upstream `0.86.1` merged into the previous line) brought
one new Spice commit, a review fixup. The merge is where a patch could have been
overwritten, so every row below was re-audited against it; the rows for fork PRs
#87 and #95 were carried at the previous pin too but had no row until now.

Fork PR #33, which made the sink honour `target_file_size_mb`, is one of those
vendored patches — it touches `vortex-datafusion` and nothing else — so it has no
row here. Its behaviour is covered where the code lives, by
`crates/vortex/src/persistent/sink.rs::test_file_splitting_62mb_into_4_files`,
`…::test_file_splitting_compressible_data`,
`…::test_write_large_batch_target_file_size_disabled` and
`…::test_target_file_size_uses_single_sink_input_partition`.

| Patch | What breaks if it is lost | Loss | Guard |
|---|---|---|---|
| Arrow `Map` support (`vortex-arrow`). Upstreamed: `0.86.1` has a native `DType::Map` (vortex-data/vortex#9107, #9111), which replaces the fork's alias and map-entry recursion; the guards stay because they pin the behaviour, not the patch | Every write of a `Map` column fails with `Array encoding not implemented for Arrow data type Map(...)`. The table is created happily first, so it surfaces only on flush | silent | `crates/vortex/src/persistent/mod.rs::map_column_roundtrips_through_a_vortex_file`, and `crates/cayenne/src/schema.rs::vortex_encodes_exactly_the_types_not_listed_as_unsupported` for the whole type list |
| Tokio one-shot for the spawned-task result channel (`vortex-io/src/runtime/handle.rs`) | Reentrant waker drop on the cancellation path → `SIGSEGV` under ordinary query cancellation | silent (crash) | `crates/cayenne/tests/vortex_task_cancellation.rs` |
| Tokio one-shot in `vortex-io/src/runtime/single.rs` | The same hazard on the single-runtime path | silent (crash) | as above |
| Tokio one-shot for the segment-read result channel (`vortex-file/src/segments/source.rs`) | The same hazard on `ReadFuture`, polled and then dropped on cancellation | silent (crash) | as above |
| Fixed-offset timezone resolution in timestamp extension types (`vortex-array/src/extension/datetime/timezone.rs`) | Panic `failed to find time zone '+00:00'` reading any `timestamptz` column whose offset is numeric rather than named | silent (panic) | `crates/cayenne/tests/fixed_offset_timezone_test.rs::fixed_offset_timezone_column_survives_a_vortex_file_write` |
| `set_available_parallelism` (`vortex-utils`) | Vortex sizes encode fan-out and scan lookahead from the machine's core count instead of the process's CPU entitlement, so a limited pod over-subscribes ([#12328](https://github.com/spiceai/spiceai/issues/12328)) | silent | `bin/spiced/tests/cpu_budget.rs::spicepod_cores_size_the_runtime_pools` |
| `DECIMAL` → floating-point cast applies the scale (fork PR #51). Upstreamed: `0.86.1`'s decimal cast (`cast_to_f64`, vortex-data/vortex#8649) applies the scale for a `f64` target; whether an `f32` target does is unconfirmed, and the guard is what would show it | Decimal columns read back off by a factor of 10^scale | silent (wrong data) | `crates/vortex/src/persistent/mod.rs::test_decimal_to_float_cast_applies_scale` |
| `UncompressedSizeInBytes` statistic handling | `ColumnStatistics.byte_size` is wrong, so the optimizer mis-sizes joins built over Vortex scans | silent | vendored: the patch touched only `vortex-datafusion/src/persistent/format.rs`, which now lives in `crates/vortex/src/persistent/format.rs`, guarded by `propagates_per_column_byte_size` |
| `vortex.date` → `vortex.timestamp` **array** cast (fork PR #28) | Upstream refuses the cast, so a pushed-down `CAST(date_col AS TIMESTAMP)` fails the scan on the rows it reads | silent | `crates/vortex/src/persistent/mod.rs::test_date_to_timestamp_extension_cast` |
| `vortex.date` → `vortex.timestamp` **scalar** cast (fork PR #93) | The row above converts a chunk's rows. A scan also casts the file's `max` statistic — a scalar — to decide whether to read the file at all, and without this `Scalar::cast` re-labels it through the target's storage type instead of converting it. `date[days]` fails the scan; `date[ms]` shares `i64` with `timestamp[ns]`, so it succeeds with an instant 10^6 too small and the file is pruned as unable to match ([#13624](https://github.com/spiceai/spiceai/issues/13624)) | silent (wrong data) | `crates/vortex/src/persistent/mod.rs::a_pushed_down_date_to_timestamp_cast_returns_the_matching_rows` for the failure, `…::a_pushed_down_date64_to_timestamp_cast_does_not_prune_the_matching_file` for the wrongly pruned file |
| Timestamp validation uses `storage_range`, and rendering never aborts (fork PR #93) | The row above converts a date into a count of the target unit; this is the range that count has to land inside, and the same fork PR carries both. A Jiff span's limits are not a timestamp's: they stop one short of `i64::MIN` nanoseconds — 1677-09-21, which a `timestamp[ns]` column holds as an ordinary value read from Arrow — so a scalar built from such a column's `min`/`max` statistic was refused although the array carried it, and the scan failed on data it could read. They also run past the last instant, and the unchecked constructors abort outside them, so rendering a count past the span range took the process down rather than reporting it. (No `vortex.date` reaches `i64::MIN` nanoseconds — neither of its units divides it — so this is the range being wrong, not the conversion.) | silent (wrong data), and abort | `crates/vortex/src/persistent/mod.rs::a_nanosecond_timestamp_scalar_spans_the_whole_i64_range`, which builds that scalar and renders it, and `…::a_timestamp_count_that_is_not_an_instant_renders_instead_of_aborting` for the other three units — they keep a span, so what the patch changes for them is that it is built and added through the checked forms, and only a count outside the range exercises that. The two cast guards above pass on either side of this row, so a re-cut that carried only the cast would not be caught without these |
| Balanced `list_contains` OR tree for large `IN` lists (fork PR #37) | A large `IN (...)` filter builds a right-leaning OR tree; deep enough and the plan blows the stack during pushdown conversion | silent (crash) | `crates/vortex/src/persistent/mod.rs::test_large_in_list_filter_pushdown_stays_evaluable` — its decimal arm is the one that reaches the tree; a primitive list that long is answered by a set probe instead, so keep an arm on a type the probe declines |
| Constant `IN` lists answered by a set probe and falsified by interval, and a null-bearing list sent back to the OR-of-equalities form before anything is keyed — including a nullable extension list against a non-nullable column, which panicked in `Scalar::new_unchecked` (fork PR #95) | A null is not a key on any probe path, so a probe that keyed a null-bearing list would be a key short and answer `false` for rows matching a later element — rows dropped with no error; and the extension-list case takes the task down. Losing only the probe itself is a perf loss | silent (wrong data, and panic) | `crates/vortex/src/persistent/mod.rs::an_in_list_holding_a_null_still_answers_its_other_elements`, whose `BIGINT` and `TIMESTAMP` arms put the null in each position past the probe's threshold. Whether the `TIMESTAMP` arm reaches the extension-kernel panic specifically is unconfirmed; the fork's `vortex-array/tests/in_list_differential.rs` does |
| `ScanBuilder::with_absolute_concurrency` (`SplitConcurrency`), and no concurrency default derived from host parallelism (fork PR #87) | The scan's split concurrency can no longer be set to the process's own figure, and the default reverts to the machine's core count | build | compile-guarded by `crates/vortex/src/persistent/opener.rs`, which calls `with_absolute_concurrency`; the CPU-entitlement half rests on the `set_available_parallelism` row's guard |
| Avoid session lock re-entry in writer init (fork PR #29). The patched code is gone at this pin: upstream rewrote writer initialisation to read the session once, and vortex-data/vortex#8919 replaced the lock-backed session registry with a lock-free one. By reading the code, the re-entry this avoided no longer exists; no run confirms it | Deadlock in `vortex-file` writer initialisation — the write never completes and the refresh hangs | silent (hang) | **GAP** — the deadlock needs a writer waiting on the session lock *between* the two reads this patch collapses into one, so a test either wins the race and passes on unpatched code or hangs the suite. That is a timing test, not a guard; what would close it is making the re-entry unrepresentable rather than avoided by convention |
| Unsupported pushdown node bubbles `TRUE` rather than erroring; empty `IN` list handled (fork PR #8) | A predicate Vortex cannot convert fails the scan instead of degrading to "keep the row" | silent | vendored: the pushdown conversion now lives in `crates/vortex/src/convert/exprs.rs`, guarded by `test_empty_in_list_conversion_produces_boolean_literal` and the `can_be_pushed_down` unsupported-operand cases |
| Intra-file decode parallelism — sub-split large chunk spans (fork PR #62) | Scan throughput on large chunk spans drops to single-stream decode | silent (perf) | **GAP** — a perf-only row; see [Open gaps](#open-gaps) for why it is deliberately unguarded |

## datafusion

Upstream [apache/datafusion](https://github.com/apache/datafusion), branch
`spiceai-55` (upstream `55.2.0-rc1` merged into the previous line in
spiceai/datafusion#248) and spiceai/datafusion#249, which carries three
`spiceai-54` patches onto 55. On top of #249 come #240, the metadata-column listing
pruning that #229 carried on `spiceai-54`, and #251, which gives a file that holds no
rows no partition-column bounds; the pin is #251's merge commit. Neither changes a
manifest. #240 threads its metadata filters through the listing calls in
`catalog-listing`'s `table.rs` and `helpers.rs`, next to the metadata-column rows'
code, and removes no line of it. The branch also carries
upstream's own `[branch-52]` … `[branch-55]` backports; those are upstream commits
and are not Spice patches. The `55.2.0-rc1` merge changed no Spice patch: outside
`Cargo.lock`, its diff from the previous pin is upstream's `55.1.0...55.2.0-rc1`
diff hunk for hunk, except `datafusion-spark`'s `quote.rs`, which the fork's backport
had already made identical to `55.2.0-rc1`'s, so that backport's row is dropped
(upstream carries the fix as apache/datafusion#25277). #249 adds the four
rows after the `Date32` one. Fixes cherry-picked from upstream `main` ahead of any release are listed
below like Spice patches: a re-cut onto a release that already has one drops its row,
and a re-cut onto one that does not has to carry it.

The unparser rows are the highest-consequence set in this file: every one of them
changes the SQL sent to a federated engine, and every failure mode is *more or fewer
rows than the plan asked for*, with no error.

Every guard naming `crates/data_components/src/federation.rs` runs in a plain scoped
test of the crate, with no feature flag:

```sh
cargo test -p data_components --lib federation::
```

The module is compiled unconditionally — the crate enables
`datafusion-table-providers/federation` itself rather than behind an opt-in feature —
so no scoped run can leave the file out and report green with nothing checked, which
is the same shape as the loss these guards exist to catch ([#13625](https://github.com/spiceai/spiceai/issues/13625)).

| Patch | What breaks if it is lost | Loss | Guard |
|---|---|---|---|
| Unparser: a `fetch` pushed into a join input keeps its own scope (fork PR #197) | The remote engine is asked for the whole table and the join is evaluated over it — more rows than the plan ([#12406](https://github.com/spiceai/spiceai/issues/12406)) | silent (wrong data) | `crates/data_components/src/federation.rs::a_fetch_pushed_into_a_join_input_survives_unparsing` |
| Unparser: a `Filter` above a `Limit` keeps the limit scoped (fork PR #198) | SQL evaluates `WHERE` before `LIMIT`, so the limit selects from filtered rows instead of bounding them ([#12591](https://github.com/spiceai/spiceai/issues/12591)) | silent (wrong data) | `…::a_filter_above_a_limit_keeps_the_limit_scoped` |
| Unparser: `ORDER BY` kept out of a derived table when the sort key is computed (fork PR #191) | The ordering is emitted inside a derived table, which SQL does not require the outer query to preserve — rows come back in any order | silent (wrong order) | `…::a_computed_sort_key_keeps_order_by_at_the_top_level` |
| Unparser: a stacked aggregate is unparsed as a derived table (fork PR #192) | An aggregate over an aggregate is flattened into one query, changing the grouping | silent (wrong data) | `…::a_stacked_aggregate_keeps_its_inner_group_by`, `…::a_grouped_stacked_aggregate_binds_its_outer_clauses_through_the_derived_scope` |
| Unparser: a bounded `EXISTS` build side is scoped, so its limit selects rows (fork PR #201) | The limit binds to the correlated subquery rather than the build side, so the `EXISTS` matches rows it should not | silent (wrong data) | `…::a_bounded_exists_build_side_is_scoped_outside_the_correlation`, `…::an_offset_only_exists_build_side_is_scoped_outside_the_correlation`, `…::an_unbounded_exists_build_side_is_left_unscoped` |
| Unparser: refuse an `EXISTS` bound that cannot be scoped, rather than emit wrong rows (fork PR #205) | The unparser silently emits SQL that returns wrong rows for the bounded shapes it cannot scope ([#13277](https://github.com/spiceai/spiceai/issues/13277)) | silent (wrong data) | `…::a_bounded_exists_refuses_a_correlation_naming_two_build_inputs` |
| Unparser: name a derived table's unnamed outputs (fork PR #206) | A derived table with an unnamed output column produces SQL the remote engine rejects, or binds the wrong column ([#12751](https://github.com/spiceai/spiceai/issues/12751)) | silent (wrong data / query failure) | `…::a_derived_tables_unnamed_outputs_are_named` |
| Unparser: refuse an `EXISTS` correlation the emitted `FROM` would capture, at any bound (fork PR #207) | A relation the emitted body introduces answers to the correlated reference, so the reference binds inside the subquery instead of to the query it was written against and the remote engine returns wrong rows. The capture is decided by SQL name scoping rather than by a row bound, so the bounded and unbounded shapes are both affected, and an unqualified reference reaches it without either side naming the other ([#12840](https://github.com/spiceai/spiceai/issues/12840)) | silent (wrong data) | `…::an_unbounded_exists_refuses_a_correlation_shadowed_by_its_build_relation`, `…::an_exists_refuses_a_correlation_the_probe_qualifier_captures_at_any_bound`, `…::an_exists_refuses_an_unqualified_correlation_the_body_exposes`, `…::an_exists_keeps_an_unqualified_correlation_the_body_lacks`, `…::an_exists_keeps_a_correlation_no_relation_in_the_body_answers_to` |
| Unparser: a filter on a projection output that cannot be repeated — a volatile expression or a subquery — is applied from a scope above the projection, an aliased output is inlined, and a dialect whose derived tables do not fix a volatile value (`SqliteDialect`, `MySqlDialect`) refuses the shape (fork PR #227, refs [#12751](https://github.com/spiceai/spiceai/issues/12751) and [#13445](https://github.com/spiceai/spiceai/issues/13445)) | `SELECT * FROM (SELECT a, random() AS r FROM t) WHERE r > 0.5` is folded into one `SELECT` whose `WHERE` cannot see the alias: `PostgreSQL` and MySQL reject the statement, and an engine that inlines the call draws a second value and returns rows the predicate excluded (517 of 990 measured on `SQLite`). A filter on an aliased repeatable output is emitted as `WHERE (s > 1)`, which `PostgreSQL` rejects. A wrapper dialect inheriting the trait default (`true`) re-enables the scope on an engine that flattens it | silent (wrong data / query failure) | `crates/data_components/src/federation.rs::a_filter_on_a_volatile_projection_output_is_applied_above_the_projection`, `…::a_filter_on_an_aliased_projection_output_is_inlined`, `…::sqlite_refuses_every_route_to_a_volatile_output_scope`; `TursoDialect` forwards `SqliteDialect`'s `false`, `SpiceBigQueryDialect` forwards `BigQueryDialect`'s answer (its `missing_trait_methods` deny fails the build on a missing forward), and Spice's `MsSqlDialect` opts out until SQL Server is measured — `crates/data_components/src/turso.rs::test_turso_dialect_reports_that_a_derived_table_does_not_fix_a_volatile_value` and `crates/data_components/src/mssql/dialect.rs::test_derived_table_is_not_trusted_to_fix_a_volatile_value` pin those answers |
| Unparser: a `RightMark` join swaps its inputs like `RightSemi` and `RightAnti`, so the outer query reads the relation the join returns (fork PR #230) | The outer `FROM` names the build side, the mark column is absent and there is no `EXISTS` at all — `SELECT t1.c, t1.d FROM t1` for a join that returns `t2`'s rows — SQL that binds and answers from the wrong relation ([#13022](https://github.com/spiceai/spiceai/issues/13022)) | silent (wrong data) | `…::a_right_mark_join_reads_the_relation_it_returns` |
| Unparser: a `FULL JOIN` input that is itself a join keeps each filtered scan's filter in that scan's own derived table (fork PR #231) | The filter is lifted to the enclosing query's `WHERE`, which SQL evaluates after the `FULL JOIN`, so the null-extended rows the join preserves are discarded — 1 row where the plan returns 2 ([#12593](https://github.com/spiceai/spiceai/issues/12593)); a predicate on such an input that no scan applies is refused rather than emitted where it would discard rows | silent (wrong data) | `…::a_full_join_input_that_is_a_join_keeps_its_scan_filters_scoped` |
| Unparser: an `EXISTS`-style join refuses a build-side key that only the build projection binds (fork PR #232) | `SELECT 1` replaces the build projection, so a key the projection renamed onto the probe's qualifier binds to the probe instead — `WHERE (p.c = p.c)`, always true: a semi join returns every row and an anti join none ([#13493](https://github.com/spiceai/spiceai/issues/13493)) | silent (wrong data) | `…::an_exists_refuses_a_build_key_only_the_build_projection_binds` |
| Unparser: empty `Projection` emits `SELECT 1` | A projection with no expressions unparses to `SELECT FROM …`, which is not valid SQL, so the federated query fails outright | silent (query failure) | `…::an_empty_projection_does_not_unparse_to_an_empty_select_list` |
| Unparser: `AT TIME ZONE` faithfully unparsed (fork PR #160), and suppressed for fixed-offset timezones on DuckDB (fork PR #195) | The timezone is dropped from the SQL, so the remote engine evaluates the expression in its own session timezone | silent (wrong data) | `…::a_timezone_survives_unparsing_except_where_the_engine_cannot_resolve_it` |
| BigQuery dialect: `FLOAT64` not `DOUBLE`, timestamp literal format, `date_field_extract_style` / `interval_style` overrides, `date_trunc` support, no column alias inside a table alias (fork PRs #144, #146, #147, #148, #169). The #144 override is no longer on the branch — the upstream `Dialect` default now renders an accepted format, which the guard pins either way | Federated BigQuery queries are rejected by BigQuery, or silently coerce types. Losing the `date_trunc` week mapping is quieter still: DataFusion truncates a week to Monday and BigQuery's bare `WEEK` is Sunday-based, so the week starts on the wrong day — one day out for a Monday-to-Saturday timestamp, six for a Sunday one — and the query returns wrong rows with no error | silent (wrong data / query failure) | `…::bigquery_names_the_float_type_the_way_bigquery_does`, `…::bigquery_attaches_a_timestamp_offset_to_the_time`, `…::bigquery_extracts_date_fields_and_spells_intervals_the_standard_way`, `…::bigquery_inlines_a_derived_tables_column_aliases`, `…::bigquery_truncates_a_timestamp_the_way_bigquery_does` |
| BigQuery dialect: emit the required `DISTINCT` quantifier for distinct unions | A federated distinct union reaches BigQuery as bare `UNION`, which BigQuery rejects before executing either branch | silent (query failure) | `crates/runtime-datafusion/src/dialect/bigquery.rs::the_wrapper_emits_valid_bigquery_set_and_window_syntax`; real-engine guard: `test/scripts/bigquery-pushdown.sh` |
| BigQuery dialect: omit frames from numbering functions while retaining aggregate frames | BigQuery rejects `ROW_NUMBER`, `RANK`, and the other numbering functions when DataFusion's normalized frame is emitted; dropping aggregate frames instead changes which rows contribute | silent (query failure / wrong data) | `crates/runtime-datafusion/src/dialect/bigquery.rs::the_wrapper_emits_valid_bigquery_set_and_window_syntax`; real-engine guard: `test/scripts/bigquery-pushdown.sh` |
| Unparser: `Dialect::range_window_default_nulls_first`, and the leading `IS NULL` key a dialect that reports one needs | A federated query with an aggregate window function fails outright: BigQuery accepts no NULL placement but its own inside a `RANGE` clause, and an `ORDER BY` with no explicit frame implies `RANGE` for an aggregate, so the plain `SUM(x) OVER (ORDER BY x)` a plan normalizes to `ASC NULLS LAST` is refused. Dropping the clause instead is worse than the failure: BigQuery defaults to NULLs *first* ascending where DataFusion defaults to last, so the NULL rows move to the other end of the ordering and every frame covers different rows — measured on real BigQuery as `(NULL,42) (1,10) (2,30) (3,35)` becoming `(NULL,7) (1,17) (2,37) (3,42)` | build (flag), then silent (query failure; wrong data if "fixed" by dropping the clause) | `crates/runtime-datafusion/src/dialect/bigquery.rs::the_wrapper_forwards_every_bigquery_specific_rendering` (the RANGE-window arm), and in the fork `plan_to_sql.rs::test_range_window_nulls_placement_becomes_a_leading_key` with `::test_range_window_nulls_placement_left_alone_where_it_binds` as its controls; real-engine guard: `test/scripts/bigquery_pushdown.py::aggregate-window-range-frame` |
| Unparser: a `DISTINCT ON` output alias does not capture its own key (fork PR #209) | Naming a `DISTINCT ON`'s computed output changes which key it groups by where the name the output takes is also spelled as a bare column by `on_expr` or `sort_expr`: PostgreSQL resolves a bare name in both clauses against the output list first, so the new alias captures those references and the statement groups by the projected expression instead of the input column. Valid SQL, unchanged reported schema name, wrong rows | silent (wrong data) | `crates/data_components/src/federation.rs::a_distinct_on_output_is_not_named_over_its_own_key`, with `…::a_distinct_on_output_is_named_over_a_qualified_key` as the control for the qualified key an output alias cannot capture. The fork's own `plan_to_sql.rs` `DISTINCT ON` tests cover the same shapes; these are what survive the next re-cut. Note the `distinct-on` arms of `…::a_derived_projection_names_the_output_its_scope_references` do not catch this — they assert the output *is* named, which still holds when the patch is lost |
| Unparser: `Dialect::group_by_matches_select_subexpressions`, and the aggregate scope a dialect that answers `false` needs | A `Projection` over an `Aggregate` is flattened into one `SELECT`, leaving the grouping expression bare in `GROUP BY` and wrapped inside a select item. BigQuery matches a whole select item and a column reference and nothing in between, so it refuses the statement outright. The two cheaper renderings are worse than the failure: `GROUP BY <output alias>` and `GROUP BY <ordinal>` group by the value the projection computes, so a projection that is not injective over the grouping expression collapses distinct groups and sums their aggregates, with no error | build (flag), then silent (query failure) | `crates/data_components/src/federation.rs::a_projection_wrapping_a_grouping_expression_keeps_the_aggregate_scoped`, which reads the flag, so losing the patch fails `cargo check` before it can fail the assertion; real-engine guard: `test/scripts/bigquery_pushdown.py::group-by-expr-nested-in-select` |
| Unparser: a subquery in a join predicate is routed out of `ON` — to `WHERE` on an inner join, and into the non-preserved input's own scope on an outer one (replaces the `supports_subquery_in_join_predicate` flag of fork PR #151) | A subquery is emitted inside a `JOIN … ON`, which several engines reject outright. No dialect opts in: both destinations select the same rows as `ON` does, so the routing needs no flag — which is what kept the flag disappearing, twice, on an upstream merge | silent (query failure) | in the fork, `plan_to_sql.rs::test_a_subquery_in_an_inner_join_predicate_moves_to_where` and `::test_a_pushed_down_subquery_filter_stays_in_the_null_extended_input`; both fail on the previous rendering |
| Unparser: a call's return type is read through `return_field_from_args` | A function that reads a literal argument — a `date_trunc` granularity — answers only through that entry point and its `return_type` reports an internal error, so the call's type is unreadable and every rendering that needs it declines. A comparison against such a call is then pushed down uncoerced, and a dialect that spells an instant and a civil timestamp as different types refuses the pair | silent (query failure) | `crates/runtime-datafusion/src/dialect/bigquery.rs::the_wrapper_forwards_every_bigquery_specific_rendering` (the truncated-date-comparison arm); in the fork, `plan_to_sql.rs::test_bigquery_agrees_a_schemaless_comparison_against_a_truncated_date` |
| Unparser: `Dialect::string_to_date_to_sql`, and the `BigQuery` rendering that parses text before narrowing it to a date (spiceai/datafusion#219) | `BigQuery`'s `DATE` cast takes a bare `YYYY-MM-DD` and nothing else, so an ISO instant held as text — nine fractional digits and a `Z` — is refused outright (`Invalid date: '2025-01-01T09:00:00.240314144Z'`, measured on a customer statement). The hook parses to an instant first and then narrows, `CAST(TIMESTAMP(REGEXP_REPLACE(…)) AS DATE)`. Losing it puts the plain cast back and every such column stops being readable | silent (query failure) | `crates/runtime-datafusion/src/dialect/bigquery.rs::the_wrapper_forwards_every_bigquery_specific_rendering` (the text-parsed-into-a-date arm) |
| Unparser: a text operand compared with a temporal one is brought to one type (spiceai/datafusion#219) | `BigQuery` has no implicit parse from `STRING` to a temporal type and refuses the pair outright with "No matching signature for operator <". Without the coercion the comparison is pushed down as written and the statement fails; this is the same `provable_data_type` chain the truncated-date row above rests on, extended to a text operand | silent (query failure) | `crates/runtime-datafusion/src/dialect/bigquery.rs::the_wrapper_forwards_every_bigquery_specific_rendering` (the text-compared-with-a-timestamp arm) |
| Unparser: a recursive CTE met below the statement root is hoisted to the top of the statement (spiceai/datafusion#219) | `BigQuery` accepts `WITH RECURSIVE` only at the top level, and a generator is rarely at the top of a *plan* — joined to a table it is a join input, which unparses as a derived table. Attaching the CTE to the nearest enclosing query put it inside those parentheses and BigQuery refused the statement: "WITH RECURSIVE is only allowed at the top level of the SELECT, CREATE TABLE ...". Measured on **six** corpus statements of that shape, all of which had been passing until the recursive CTE started federating. Two subtleties keep it working: the pending list is shared across the `Unparser` that `with_schema` builds mid-walk (a fresh one there is dropped, leaving a dangling reference), and a depth counter distinguishes "a statement" from "the top level", because a derived table is rendered by unparsing its plan as a whole statement | silent (query failure) | `crates/runtime-datafusion/src/dialect/bigquery.rs::a_recursive_cte_renders_through_the_wrapper_only_where_it_is_supported` and `::a_recursive_cte_behind_a_derived_table_opens_the_statement` assert the emitted SQL *opens* with `WITH RECURSIVE` and contains no `JOIN (WITH`, for a directly joined generator and for one behind a derived table. **Partial GAP**: the arrangement that actually failed is produced by the *federation analyzer*, which re-plans before unparsing — not by the optimizer and not by hand. Four attempts to build it (three assembled plans, one optimized `SessionContext` plan) each routed through a different renderer and passed with the fix reverted, so none of these unit guards would have caught it. What caught it, and what covers it, is the real-engine corpus run: six statements failed there and pass now. In the fork, `plan_to_sql.rs::test_bigquery_hoists_a_recursive_cte_behind_a_derived_table` and `::test_bigquery_hoisting_handles_repeats_and_nesting` |
| Unparser: `Dialect::supports_recursive_cte`, and the `WITH RECURSIVE` rendering behind it (spiceai/datafusion#219) | A `RecursiveQuery` has no unparsing at all by default, so a plan carrying one cannot be federated. The flag is the gate rather than the feature: emitting `WITH RECURSIVE` to an engine that cannot run one turns a query that used to evaluate locally into a failure, because a federated statement has no local-execution fallback. `BigQuery` opts in; the default dialect does not | silent (query failure, in either direction) | `crates/runtime-datafusion/src/dialect/bigquery.rs::a_recursive_cte_renders_through_the_wrapper_only_where_it_is_supported`, which asserts both the rendering **and** that a dialect which has not opted in still refuses |
| Unparser: `Dialect::string_to_timestamp_to_sql`, and the `BigQuery` rendering that **parses** text into a timestamp rather than casting it (fork PRs #216, #218) | A `CAST(<text> AS TIMESTAMP)` reaches BigQuery as a cast, and BigQuery's `DATETIME` cast refuses every zone marker its `TIMESTAMP` cast accepts, and neither accepts more than six sub-second digits — so a stored timestamp string with an offset or nanosecond precision fails the statement outright (`Invalid date: '2025-01-01T09:00:00.240314144Z'`, measured). The hook renders `DATETIME(TIMESTAMP(REGEXP_REPLACE(…)))` instead, which parses the text and truncates the sub-second digits to the six BigQuery holds. Losing it puts the plain cast back and those rows stop being readable at all | silent (query failure) | `crates/runtime-datafusion/src/dialect/bigquery.rs::the_wrapper_forwards_every_bigquery_specific_rendering` (the text-parsed-into-a-timestamp arm, which asserts the `DATETIME(TIMESTAMP(REGEXP_REPLACE(` form is emitted and the bare `CAST(… AS DATETIME)` is not) |
| BigQuery dialect: a `FILTER (WHERE …)` on an aggregate is rewritten, and only for aggregates the rewriting is exact for (fork PRs #216, #218) | BigQuery has no `FILTER` clause, so an unrewritten one reaches it as SQL it cannot parse. Rewriting it too widely is worse: `COUNTIF` over `COUNT(NULL)`'s rows answers 2 where `COUNT(NULL)` answers 0, and `CASE WHEN p THEN arg END` hands `array_agg` a null element per *rejected* row, which BigQuery refuses to build. The rewriting is therefore gated on an allowlist of aggregates that skip nulls and ignore input order — `count`, `sum`, `min`, `max`, `avg`, `bit_and`, `bit_or`, `bit_xor` — and declines the rest. A decline must be matched by a federation refusal — the `FunctionSupport::with_aggregate_call_support` row in the datafusion-table-providers section below — or the decline is itself a failed query | silent (query failure; **wrong data** if the allowlist widens) | `crates/runtime-datafusion/src/dialect/bigquery.rs::the_wrapper_forwards_every_bigquery_specific_rendering` (the filtered row-count, filtered-sum, `COUNT(NULL)`, filtered-`bit_and`, declined-`array_agg` and ordered-sum arms) |
| BigQuery dialect: a frame is dropped from `LEAD`/`LAG` while aggregate frames are kept (fork PR #216) | BigQuery refuses a window frame on the navigation functions, so DataFusion's normalized frame is a syntax error; dropping frames from aggregates instead would change which rows contribute, so the two cases must stay separate | silent (query failure; wrong data if aggregate frames are dropped too) | `crates/runtime-datafusion/src/dialect/bigquery.rs::the_wrapper_forwards_every_bigquery_specific_rendering` (the `LEAD`-frame arm, with a `FIRST_VALUE` arm as its control) |
| Unparser: a comparison's operands are typed from the operands themselves, so a dialect can bring a pair it has no supertype for to one type (fork PR #216) | BigQuery has no common supertype for an instant (`TIMESTAMP`) and a civil timestamp (`DATETIME`) or a `DATE`, and refuses the comparison outright. Reading the operand types is what lets the dialect insert the cast; without it the pair is pushed down uncoerced | silent (query failure) | `crates/runtime-datafusion/src/dialect/bigquery.rs::the_wrapper_forwards_every_bigquery_specific_rendering` (the instant-vs-date and truncated-date-comparison arms) |
| Unparser: `Dialect::decimal_type_to_sql`, and the unparameterized `NUMERIC`/`BIGNUMERIC` BigQuery selects through it — by **both** halves of the width, accepting the rounding on a scale it cannot hold (fork PRs #216, #218) | A cast target carries no precision or scale — `CAST(x AS NUMERIC(38, 9))` is refused as a parameterized type — so the width has to come from the type, and the width arrives from arithmetic so no declared column type reveals it. Measured limits: `NUMERIC` holds 29 integer digits and 9 fractional, `BIGNUMERIC` 39 and 38. The two halves fail differently, which is why the rule needs both. An integer overflow is refused outright ("Invalid NUMERIC value"), so the widest type is always worth emitting. A **scale** past the type's is *rounded away silently*, and that rounding is **accepted** rather than declined: a federated statement has no local-execution fallback, so declining fails the query outright, and reaching a scale past 38 needs a `BIGNUMERIC` column and a division (`Decimal256(76, 42)`), where the measured loss is 4.4e-39 absolute — the thirty-ninth decimal place. Losing the **integer-width** half is the silent one: it renders `NUMERIC` and BigQuery refuses every value past 29 integer digits | silent (query failure) — **silent (query failure at BigQuery)** if the integer-width rule is lost, and a wider rounding than the measured 4.4e-39 if the scale rule is ever applied to a narrower type | `crates/runtime-datafusion/src/dialect/bigquery.rs::the_wrapper_forwards_every_bigquery_specific_rendering` (the decimal, integer-width, wide-precision `Decimal256`, negative-scale and rounded-away-scale arms). The negative-scale arm guards a *caller* rather than the dialect: the unparser folds a negative Arrow scale into the precision (`Decimal128(28, -2)` becomes `(30, 0)`) before the hook is consulted, so the hook never meets one — lose that and a width of thirty integer digits renders `NUMERIC`, which BigQuery refuses; in the fork, `plan_to_sql.rs::test_bigquery_spells_a_wide_decimal_bignumeric`, `::test_bigquery_widens_a_decimal_whose_integer_part_overflows_numeric`, `::test_bigquery_accepts_the_rounding_for_a_scale_past_bignumeric` |
| Unparser: a sort above an aggregate given a scope of its own names that scope's output | The sort key is unprojected into the grouping expression, which names the relation the scope encloses, so the remote binder reports the qualifier as unknown. One predicate now owns the scope decision for both the projection acting on it and the sort reading it | silent (query failure) | in the fork, `plan_to_sql.rs::test_a_sort_over_a_scoped_aggregate_names_the_scope_not_its_grouping_expr` |
| Metadata columns (`_location`, `_last_modified`, `_size`) on `ListingOptions`/`FileScanConfig`, and their projection, pushdown and statistics handling | Datasets that select file metadata columns lose them, or project the wrong column | build | `crates/data-connector-api/src/listing/connector.rs` (metadata-column tests) |
| Object-version pinning on `ListingOptions` (`with_object_versioning_type`), forwarded through `DFParquetMetadata` and `ParquetFileReader` (DataFusion 55's single Parquet reader, which replaced `CachedParquetFileReader`; the pin was ported onto it) on the **scan** path; `HEAD` when the listing has no version id, kept only when HEAD's ETag matches the listed ETag. Schema/statistics inference (`ParquetFormat::{infer_schema,infer_stats,infer_stats_and_ordering}`) does not forward the pin | A scan stops pinning the object version, so a file replaced mid-scan is read half-old and half-new. Losing only the metadata-path forward is enough: the scan footer is unpinned while the pages stay pinned. Losing the `HEAD` leaves versioned buckets pinning by ETag, so a replace 412s instead of reading the listed generation | build (API) + silent (behaviour) | `crates/data-connector-api/src/listing/connector.rs::a_versioned_parquet_read_pins_every_request_to_one_object_version`, `…::a_versioned_parquet_read_pins_by_etag_when_the_listing_has_no_version_id`, `crates/runtime/tests/s3_parquet_overwrite/mod.rs::listing_table_scan_does_not_decode_a_replaced_object` (listing/overwrite **scan** race). Planning-time schema/statistics footer reads are a remaining unpinned gap, unreproduced as a product failure |
| Bloom-filter replacement readers reuse the version discovered on the listing-table scan (fork PR #213) | A predicate scan whose bloom-filter reader is built separately still sends the listed ETag as `If-Match`. A replaced object therefore 412s instead of mixing generations; the query retries or fails | silent (query failure / extra retry) | `crates/data-connector-api/src/listing/connector.rs::a_second_reader_for_the_same_file_keeps_the_version_the_first_one_pinned` for the factory, which is where the patch lives, and `crates/runtime/tests/s3_parquet_overwrite/mod.rs::a_predicate_scan_of_bloom_filtered_parquet_pins_one_generation` for the scan that builds that second reader — the wire assertion is guarded by the plan's bloom-filter metric so it cannot pass by never taking the branch |
| Placeholder type inference (`Expr::infer_placeholder_types`, incl. `CASE`, `LIMIT`/`OFFSET` `Int64`, name/metadata preservation) (fork PRs #87, #88, #89, #167, and commit `d37a426e`) | A parameterised query fails to plan, or infers the wrong type for `$1` | silent (query failure) | `crates/runtime/src/datafusion/query.rs::every_shape_the_fork_patches_cover_infers_its_parameter_type`, `…::a_limit_and_an_offset_placeholder_are_both_int64`, `…::a_comparison_of_two_placeholders_still_plans`, `…::a_placeholder_inferred_from_a_column_keeps_the_columns_metadata` |
| BigQuery dialect: temporal typing and naming — a tz-naive timestamp cast is `DATETIME` not `TIMESTAMP`, a timestamp literal's cast target follows the offset it renders with, sub-second digits are truncated to six, a comparison BigQuery has no supertype for is brought to one, `date - date` is `DATE_DIFF`, `CAST(date AS INT64)` is `UNIX_DATE`, `btrim`/`now`/`to_unixtime`/`unix_seconds`/`to_timestamp` are renamed or type-directed, `median`/`approx_percentile_cont` are rendered by ordering the group, a constant `GROUP BY` key is cast to its own type, and `array_element` subscripts with `SAFE_ORDINAL` (fork PR #212) | BigQuery puts no timezone qualifier on a timestamp type, so a tz-naive value typed `TIMESTAMP` becomes an instant with no supertype against a `DATETIME` column and the statement is refused; the name and cast rows are refused outright too. Two are quieter: `array_element` is 1-based where a bare BigQuery subscript is 0-based, so the neighbouring element is read with no error, and dropping a constant grouping key turns a grouped aggregate into a global one, returning one row of zeros where the grouped form returns none | silent (query failure; wrong data for the subscript and the dropped grouping key) | `crates/runtime-datafusion/src/dialect/bigquery.rs::the_wrapper_forwards_every_bigquery_specific_rendering` (the four `#212` arms: `DATE_DIFF`, `UNIX_DATE`, `DATETIME`, cast `GROUP BY`) and `::array_element_federates_only_for_a_non_negative_integer_index`; in the fork, the per-rendering tests in `plan_to_sql.rs` and, restored by fork PR #214 after #212 deleted them, `rewrite.rs`'s own; real-engine guard: `test/scripts/bigquery-pushdown.sh` |
| Spark concat coerces an untyped NULL argument to a string type (fork PR #217). Upstream adopted the behaviour in DataFusion 55 (`ConcatFunc` coercion, apache/datafusion#22244), so the merge took upstream's `coerce_types`; the fork keeps its tests | A string array concatenated with an untyped NULL reaches an unsupported kernel branch | silent (panic) | `crates/runtime/src/datafusion/builder.rs::tests::the_built_session_concatenates_an_untyped_null` |
| Substrait VarChar literals decode as UTF-8 strings (fork PR #215) | Plans containing VarChar literals fail to decode | silent (query failure) | `crates/runtime/src/flight/flightsql/statement_substrait_plan.rs::tests::decode_plan_executes_a_varchar_literal`; `tools/substrait-compliance/src/mode_a.rs::varchar_literal_lowers_to_utf8` (Isthmus TPC-H plans emit `VarChar` literals such as `EUROPE`, length 25; the Mode A harness `spice-substrait-compliance` exercises them on q02/q03/q05/q11/q12/q16/q17/q19–q22) |
| Substrait `extract` enum arguments lower to `date_part` cast to the plan's declared output type (fork PR #220) | Isthmus TPC-H q07/q08/q09 emit `extract:req_date` with `FunctionArgument { enum: "YEAR" }`; without the patch `from_substrait_plan` errors (`Function argument non-Value type not supported`) and Mode A reports ERROR for all three | silent (query failure) | `tools/substrait-compliance/src/mode_a.rs::enum_function_argument_lowers_to_date_part`, `::registered_extract_udf_takes_precedence_over_date_part` (a UDF registered as `extract` wins over the mapping), `::extract_indexing_option_is_an_offset_from_date_part` (`MONTH ZERO` and `SUNDAY_DAY_OF_WEEK ONE` on 1998-09-01 give 8 and 3), `::unmapped_extract_component_is_rejected_by_name` (`MILLISECOND` fails as an unsupported component, not as an unsupported argument); Mode A harness (`spice-substrait-compliance`) on q07/q08/q09 |
| Unparser names a subquery alias's columns when the scan pushdown renames them (fork PR #221, refs spiceai/spiceai#13140) | A `SubqueryAlias` over a projection the alias pushdown requalifies exposes outputs named after the requalified expression while the enclosing scope refers to them by the name the alias's schema reports, so the reference binds to nothing and the remote engine rejects the statement | silent (query failure) | `crates/data_components/src/federation.rs::a_projected_scan_under_an_alias_names_the_output_its_scope_references` (a pushed-down scan projection under alias `s` unparses so the `FROM` clause exposes the enclosing identifier; without the patch the derived table cannot report that name). Also the fork's own `plan_to_sql.rs` tests (`test_subquery_alias_over_pushed_down_scan_is_named_by_the_alias`, `…_keeps_a_named_output_unaliased`, `…_on_dialect_without_column_list`, `test_subquery_alias_column_list_escapes_a_quote_in_an_output_name`) |
| Substrait subquery scans of a table the enclosing scope also reads get their own qualifier; a scan's own `ReadRel.filter` binds to the Substrait base schema and sits above the scan whenever it holds an outer reference, aliased or not; joins, intersects and excepts requalified inside a subquery keep clear of the enclosing scope's `left`/`right` (fork PR #226) | SQL names such a scan (`lineitem l2`); Substrait cannot, so both scans were `LINEITEM` and decorrelation resolved `LINEITEM.L_ORDERKEY = outer_ref(LINEITEM.L_ORDERKEY)` to the inner scan alone: the semi/anti join lost its condition and `L_SUPPKEY != L_SUPPKEY` stayed behind, so TPC-H q21 returned no rows where the SQL returns one. Without the follow-ups, a correlated predicate carried as `ReadRel.filter` was consumed against the provider's schema (field 0 bound to a provider's extra leading column: `Cannot cast string 'x' to value of Int64 type`), and a self-join inside a subquery took the fixed `left`/`right` that an enclosing self-join already used, collapsing the correlation to no rows | silent (wrong data) | `tools/substrait-compliance/src/mode_a.rs::correlated_subquery_over_the_same_table_keeps_its_rows` (same-table correlated EXISTS over three rows returns two; empty without the patch), `::correlated_read_filter_binds_to_the_substrait_schema` (the predicate as `ReadRel.filter` against a provider with an extra leading column; the fork's own test failed with the cast error before the follow-up), `::requalified_join_inside_a_subquery_keeps_its_correlation` (self-joins in both scopes return six rows; empty on the pin before the follow-up), `::intersect_inside_a_subquery_keeps_its_correlation` (a self-intersect in the subquery; empty on the pin before the follow-up), `::correlated_read_filter_on_another_table_keeps_its_rows` (the predicate as `ReadRel.filter` on a table no enclosing scope reads; unexecutable on the pin before the follow-up), `::subquery_scan_alias_skips_a_taken_name` (the enclosing scope reads `t` and a table named `t_1`, so the inner scan of `t` becomes `t_2`; six rows); Mode A harness (`spice-substrait-compliance`) on q21 |
| BigQuery renders integer-typed division with `DIV` (fork PR #222) | Fractional division followed by an integer cast rounds cohort cutoff hours instead of preserving the logical plan's integer quotient | silent (wrong data) | `crates/runtime-datafusion/src/dialect/bigquery.rs::the_wrapper_forwards_every_bigquery_specific_rendering` (the integer cohort hours arm) |
| Unparser isolates standalone expression state and qualifies filtered recursive join inputs (fork PR #219) | A refused recursive expression poisons a reused unparser, or a filtered recursive self-join renders ambiguous columns | silent (query failure) | `crates/runtime-datafusion/src/dialect/bigquery.rs::filtered_recursive_join_inputs_keep_their_qualified_columns` and `::a_recursive_cte_renders_through_the_wrapper_only_where_it_is_supported`; real-engine control: `test/scripts/bigquery_pushdown.py::filtered-recursive-self-join` |
| Unparser preserves a recursive CTE column-list projection through a join alias (fork PR #225) | A recursive hour generator with an explicit column list fails SQL generation when joined to a remote table | silent (query failure) | `crates/runtime-datafusion/src/dialect/bigquery.rs::recursive_column_list_survives_a_join_alias`; real-engine guard: `test/scripts/bigquery_pushdown.py::recursive-cte-joined-to-a-table` |
| Eager-aggregation physical optimizer rule (`datafusion/physical-optimizer/src/eager_aggregation.rs`, ~3000 lines, Spice-only) | Aggregations stop being pushed below joins — a large planned regression, not a correctness one | silent (perf) | `crates/runtime/src/datafusion/builder.rs::eager_aggregation_pushes_an_aggregate_below_a_join` plans `SUM … GROUP BY` over a join in a session built by `DataFusionBuilder` and asserts a pre-aggregation lands below the `HashJoinExec` and the rows are right; the same session with the rule disabled is the control that shows the plan check tells the two apart. In the fork, the 24 `eager_aggregation` tests |
| Pluggable `CollectLeftAccumulator` seam on `HashJoinExec` (re-applied in the DataFusion 55 merge around upstream's `MinMaxLeftAccumulator`) | Cayenne's custom left-side accumulator cannot be installed | build | compile-guarded by `crates/runtime-datafusion/src/join_accumulator/mod.rs`, which implements it, and `crates/cayenne`, which installs it |
| Eager aggregation reads its statistics through DataFusion 55's `StatisticsContext` (`plan_statistics` in `eager_aggregation.rs`, made in the DataFusion 55 merge) | 55 deprecates `partition_statistics`, and `FilterExec` no longer derives its statistics through it, so a rule still calling it sees no row counts, its cost gate never passes, and the rule silently stops firing. Same rows, lost pushdown — and spiced enables the rule by default. This is how the port first went wrong: the fork's own `eager_aggregation` tests caught the rule not firing | silent (perf) | `crates/runtime/src/datafusion/builder.rs::eager_aggregation_pushes_an_aggregate_below_a_join` — its push side reaches the join through a `FilterExec`, so it fails with `plan_statistics` reverted to `partition_statistics` (measured: the rule declines and no aggregate is planned below the join). In the fork, the 24 `eager_aggregation` tests |
| Inner and outer join statistics drop each input's `sum_value` (`estimate_join_cardinality` in `datafusion/physical-plan/src/joins/utils.rs`, commit `006f3d21`; upstream `55.1.0` and `main` both carry the input sums through) | DataFusion 55's `AggregateStatistics` answers a bare `SUM(col)` from exact statistics, so `SUM` over an inner or outer join returns the whole input table's sum: `SUM(t.value)` over `t JOIN d … WHERE d.region = 'NA'` answered 209612800, the sum of all of `t`, instead of 806400 | silent (wrong data) | `crates/cayenne/tests/result_correctness_vs_duckdb_test.rs::micro_bench_shapes_full_result_parity_vs_duckdb` (its `micro_join_filter` shape), and the SQLite and TPC-DS/ClickBench parity tests beside it; in the fork, `joins::utils::tests::test_inner_and_outer_joins_drop_input_sums` |
| Unparser: a `Date32` literal's cast is spelled with the dialect's date type (spiceai/datafusion#237 on `spiceai-54`, carried onto 55 as `fc84f2d1`, spiceai/datafusion#242) | `SQLite` reads `CAST('1994-01-01' AS DATE)` as the number `1994`, so a date range pushed to `SQLite` compares text against a number and matches no row | silent (wrong data) | `crates/data_components/src/federation.rs::tests::a_date_range_unparsed_for_sqlite_keeps_the_rows_it_selects` (with `--features sqlite` it runs the SQL on `SQLite` and counts the rows) |
| Unparser: a join that is another join's right input stays a parenthesised joined table on that join's right, and a LEFT JOIN folds a filter from inside it into its own `ON` (spiceai/datafusion#233 on `spiceai-54`, carried onto 55 as `2288d1a46`, spiceai/datafusion#249) | `a ⋈ (b ⋈ c)` is linearised as `FROM a INNER JOIN c ON b.id = c.id INNER JOIN b ON a.id = b.id`, naming `b` before it is in scope: `PostgreSQL`, `DuckDB` and `SQLite` refuse the statement, and an engine that binds lazily runs a different join tree ([#14373](https://github.com/spiceai/spiceai/issues/14373)) | silent (query failure, or wrong data) | `crates/data_components/src/federation.rs::tests::a_join_that_is_another_joins_right_input_stays_on_its_right` (with `--features sqlite` it runs the SQL on `SQLite` and compares the rows with `DataFusion`'s for the same plan) |
| Unparser: a `Limit` that is a join input is derived under its scan's own name, whatever the enclosing `SELECT` carries (spiceai/datafusion#234 on `spiceai-54`, carried onto 55 as `917b07021`, spiceai/datafusion#249) | Without a `WHERE` or a projection above the join, the input's `LIMIT` lands on the enclosing query and bounds the join's output instead of that input: `b FULL JOIN (c LIMIT 1)` over `b = {1, 2}`, `c = {1}` returns 1 row where the plan returns 2 ([#14375](https://github.com/spiceai/spiceai/issues/14375)) | silent (wrong data) | `…::a_limit_on_a_join_input_bounds_that_input_rather_than_the_join` (with `--features sqlite`, as above) |
| The file metadata cache drops an evicted entry's hit counter with it (spiceai/datafusion#245 on `spiceai-54`, for [#12952](https://github.com/spiceai/spiceai/issues/12952)). Upstreamed: 55's single generic `DefaultCache` (apache/datafusion#22613) already prunes its hit map in `evict_entries`, so 55 carries no patch — only #245's regression test, ported as `b4c5b52b4` in spiceai/datafusion#249. The guard stays because it pins the behaviour, not the patch | The cache Cayenne's Vortex footers are read through keeps a `(Path, usize)` counter for every file it ever cached, outside its own memory accounting. Cayenne writes each refresh and compaction under fresh paths, so the map grows for the life of the process — 7.6 MiB to 27.0 MiB of unaccounted heap over 10,000 refreshes at a constant 1,600 live entries, as #245 measured on 54 | silent (memory) | `crates/cayenne/tests/footer_cache_hit_counter_test.rs` |
| Unparser: the join arms' bookkeeping lives in helper methods, outside `select_to_sql_recursively_inner`'s frame (`cbda233a6`, spiceai/datafusion#249; made on the 55 line to carry #233 and #234, no `spiceai-54` counterpart) | Each level of the unparser's plan walk takes a bigger frame in an unoptimised build — 135,136 B instead of 117,696 B, against 120,576 B before #233 and #234 — so the fork's own `roundtrip_statement` needs 2,112 KiB of stack instead of 1,984 KiB and overflows a 2 MiB test thread, with or without `recursive_protection`. Release builds were not measured | silent (stack overflow, unoptimised builds) | **GAP** — see [Open gaps](#open-gaps): this repo runs tests on 8 MiB threads, so no test here sees the difference deterministically |
| Listing prunes files by metadata-column predicates (`_last_modified`, `_size`, `_location`) before opening them and reports those filters `Exact` (`pruned_partition_list_with_metadata`, `filter_by_metadata`; spiceai/datafusion#229 on `spiceai-54`; ported to `spiceai-55` as spiceai/datafusion#240) | A `_last_modified`/`_size`/`_location` bound falls back to a row-level `FilterExec`, so every file under the prefix is opened each scan even when it cannot match. An accelerated `refresh_mode: append` keyed on `_last_modified` then re-reads (and for `jsonl.gz` re-decompresses) the whole source every check interval, and a file below the watermark that cannot be read fails the refresh instead of being pruned unopened ([#14264](https://github.com/spiceai/spiceai/issues/14264)). The listing connector's `_location` fast path calls `filter_by_metadata`, so a re-cut that drops the entry point fails the build | build (the entry point is removed on a re-cut) + silent (perf; refresh failure on an unreadable pruned-away file) | `crates/data-connector-api/src/listing/connector.rs::listing_table_prunes_files_by_a_metadata_column_predicate`, which builds the `ListingTable` without the connector, and `crates/data-connector-api/tests/metadata_pruning_guard.rs::metadata_predicate_prunes_the_listing_before_opening_files` for the listing (`pruned_partition_list_with_metadata`) path; `crates/data-connector-api/src/listing/connector.rs::tests::metadata_prune_matrix::case2_location_and_last_modified_prunes_before_opening` for the `head()`-based fast path (`filter_by_metadata`) |
| A file that holds no rows gets no `min`/`max` for its partition columns (spiceai/datafusion#251; upstream `main` still gives it the partition value as an exact bound) | `MIN`/`MAX` of a partition column is answered from the listing's statistics with the value of a partition whose files hold no rows: `max(p)` is `'99'` for a `p=99` directory holding one empty file, where the rows give `'3'`. Upstream `datafusion-python` 54.1.0 with `collect_statistics` answers the same | silent (wrong data) | `crates/data-connector-api/src/listing/connector.rs::max_of_a_partition_column_skips_a_partition_holding_only_an_empty_file` |
| `JoinSelection` keeps the order of a hash join that already has a dynamic filter (spiceai/datafusion#252; `HashJoinExec::swap_inputs` rejects such a join) | A plan that is optimized twice, such as the sub-plan `vector_search` returns from `scan`, reaches `JoinSelection` with a join that `FilterPushdown` already gave a dynamic filter, and the query fails with `Cannot swap HashJoinExec inputs after dynamic filters have been constructed` | silent (query failure) | `crates/runtime-datafusion/src/fork_backport_guards.rs::join_selection_keeps_the_order_of_a_join_with_a_dynamic_filter` |
| Unparser: `rescope_projection_over_projection` — a projection over an unaliased derived projection is rescoped rather than left naming qualifiers the derived table hides (made in the DataFusion 55 merge; upstream `55.1.0`, `branch-55` and `main` all lack it) | The unparser emits an unaliased `FROM (SELECT … FROM products AS p LEFT JOIN …)` whose outer `SELECT` still references `"p"."…"`, which the remote engine refuses — DuckDB with "Referenced table p not found" — so the federated query fails ([apache/datafusion#22961](https://github.com/apache/datafusion/issues/22961)'s query). Upstream's own `optimized_duckdb_unparse_preserves_derived_table_scope` passes on the broken output | silent (query failure) | `crates/data_components/src/federation.rs::a_projection_over_a_derived_projection_reads_only_relations_in_scope` unparses the shape in every federation dialect and asserts the outer `SELECT` qualifies no column by a relation the derived table hides — for unique outputs, read by name, and for the same-named pair, merged — that the volatile merge is refused, and, when built with `duckdb`, that `DuckDB` binds the statement. In the fork, `plan_to_sql.rs::test_projection_over_projection_merges_same_named_columns` (the #22961 regression test), `::test_projection_over_projection_reads_unique_columns_by_name` and `::test_projection_over_projection_same_named_columns_over_volatile_is_refused` |
| Backport of apache/datafusion#24817: an aggregate builds a dynamic filter only when every aggregate in it can contribute one | The filter is built from the plain-column `MIN`/`MAX` aggregates alone and prunes the rows that decide an expression aggregate beside them, so `MIN(c + 1)` is computed from a subset of the rows | silent (wrong data) | `crates/cayenne/tests/datafusion_dynamic_filter_backports_test.rs::an_aggregate_dynamic_filter_keeps_rows_an_expression_aggregate_needs` |
| Backport of apache/datafusion#25259, applied over #24428, which it is written against: a filter pushed through an operator has its columns mapped by position, not by name | A join or `TopK` dynamic filter pushed below an operator whose output holds two columns of one name (nested joins, a filter or projection over a join, a `GROUP BY`) lands on the wrong one: the join loses its matches, or `ORDER BY … LIMIT` returns the wrong row | silent (wrong data) | `…::a_join_dynamic_filter_maps_same_named_columns_by_position`, `…::a_topk_dynamic_filter_maps_same_named_columns_by_position` |
| Backport of apache/datafusion#24248: bitwise `XOR` simplification keeps NULL | `(a # b) # a` is folded to `b` and `a # a` to `0`, so a NULL `a` answers a number | silent (wrong data) | `…::xor_simplification_keeps_null` |
| Backport of apache/datafusion#24380: `col ~ '.*'` simplification keeps NULL | The match-everything pattern is folded to `true`, so a NULL `col` answers `true` | silent (wrong data) | `…::regex_match_all_simplification_keeps_null` |
| Backport of apache/datafusion#24686: merging nested projections keeps every layer | Nested projections that each redefine a column are merged onto the wrong layer, so six `i + 1` layers add three | silent (wrong data) | `…::nested_projections_keep_every_layer` |
| Backport of apache/datafusion#24958: a sort pushed below `GlobalLimitExec` keeps its `skip` | `ORDER BY` over a `LIMIT … OFFSET` subquery returns too few rows | silent (wrong data) | `…::a_sort_over_limit_offset_keeps_every_row` |
| Backport of apache/datafusion#24997: `COUNT` with `ORDER BY` counts every argument | `COUNT(a, c ORDER BY b)` counts only `a`'s non-NULLs, and a grouped `COUNT(a ORDER BY b)` panics | silent (wrong data / query failure) | `…::count_with_order_by_counts_every_argument` |
| Backport of apache/datafusion#25348: a constant `NOT IN (subquery)` sees the subquery's NULLs | `3 NOT IN (subquery)` returns rows although the subquery holds a NULL | silent (wrong data) | `…::constant_not_in_sees_the_subquerys_null` |
| A correlated `NOT IN (subquery)` in a `WHERE` clause is planned as the `NOT EXISTS` it equals there: a plain anti join on `x IS NULL OR y IS NULL OR x = y` and the correlation (spiceai/datafusion#235). Upstream evaluates these shapes in the null-aware join executor instead (apache/datafusion#25339, #25560), which builds on the null-aware mark joins of #21585; drop this patch on a re-cut onto a release that has them | A NULL correlation key drops rows although the subquery holds no NULL, a subquery NULL that the correlation excludes still removes every outer row, and a column `NOT IN` with an equality correlation, or a constant one with a non-equality correlation, fails to plan | silent (wrong data / query failure) | `…::correlated_not_in_is_answered_as_not_exists` |
| Backport of apache/datafusion#25227: cast statistics propagate only through safe conversions | A `MIN`/`MAX` over a cast column is answered from the uncast Parquet statistics, so `MAX(CAST(a AS INT))` over the strings `'1'`, `'100'`, `'2'` answers `2` | silent (wrong data) | `…::a_cast_aggregate_is_not_answered_from_uncast_statistics` |
| Backport of apache/datafusion#24516: a scalar subquery that can return no rows is nullable | `(SELECT 1 WHERE FALSE) IS NULL` answers `false`, and `WHERE (SELECT z FROM t) IS NULL` over an empty `t` whose `z` is `NOT NULL` keeps no row | silent (wrong data) | `…::an_empty_scalar_subquery_is_null` |

## arrow-rs

Upstream [apache/arrow-rs](https://github.com/apache/arrow-rs), branch
`spiceai-59-patches` (upstream `59.3.0` merged into the previous line). Every row
below was re-confirmed present at the pinned revision. Upstream deprecates
`ParquetObjectReader` in 59 (apache/arrow-rs#10354); the Spice patches to it
merged cleanly and are kept. The pin lives in the object-store Parquet reader
(`parquet/src/arrow/async_reader/store.rs`), in the push-decoder short-read
path (`parquet/src/util/push_buffers.rs` and its callers), in
`arrow-buffer/src/buffer/immutable.rs`, in the cast kernel
(`arrow-cast/src/cast/mod.rs`), and in the Flight SQL client
(`arrow-flight/src/sql/client.rs`).

| Patch | What breaks if it is lost | Loss | Guard |
|---|---|---|---|
| `ParquetObjectReader::new_with_meta` — take `ObjectMeta` so the file size is known up front | The reader falls back to suffix range requests, which Azure Blob Storage does not support: Parquet reads over ABFS fail or take an extra round trip per file | build (constructor) | `crates/runtime/tests/abfs/mod.rs::test_azure_parquet_reading_with_object_meta` (needs Azurite) |
| `with_object_versioning_type` — attach `if_match`/`version` to every metadata, byte-range and suffix fetch; a `Version` pin with no version id falls back to `If-Match` on the listed ETag; `set_object_version` applies a `HEAD` version id to later page reads | The reader stops pinning the object version. A file replaced between the metadata read and the data reads is read as a mixture of both — the footer of one file, the pages of another. Losing the ETag fallback is quieter still: unversioned buckets never carry a version id, so the pin becomes a no-op | build (API) + silent (behaviour) | `crates/data-connector-api/src/listing/connector.rs::a_versioned_parquet_read_pins_every_request_to_one_object_version`, `…::a_versioned_parquet_read_pins_by_etag_when_the_listing_has_no_version_id` |
| `get_byte_ranges` override — coalesce ranges through `get_opts` rather than `ObjectStore::get_ranges` | Version pinning is dropped for the data reads specifically (the metadata read keeps it), and range coalescing is lost, so a scan issues one request per column chunk | silent | as above |
| `Buffer::has_custom_allocation` — expose whether a buffer's memory is freed by its own owner rather than by the buffer ([spiceai/arrow-rs#25](https://github.com/spiceai/arrow-rs/pull/25)) | The results cache can no longer tell that a batch rests on memory it does not own, so it shares the producer's arrays instead of copying them. `capacity` reports the size the producer declared, so such an entry looks compact and is billed as if it were: a DuckDB- or ADBC-imported result pins the driver's chunk, a Flight-decoded one pins the whole IPC message body, and `max_size` bounds none of it. Measured at ~4.5 KB per entry unbilled on a one-row DuckDB result, flat as the result widened to 10 rows | build (the predicate) + silent (the accounting, if the call is dropped rather than the function) | `crates/arrow_tools/src/record_batch.rs::a_batch_resting_on_foreign_memory_is_copied_even_with_nothing_to_reclaim` |
| `PushBuffers::push_range` returns `ParquetError` on a short read instead of asserting (apache/arrow-rs#10564). Upstream adopted it in `60.0.0`, not in any 59 release, so it is still carried on this line; drop the row at the move to 60 | A footer prefetch that races an in-place shrink panics the reader thread (`Range length must match buffer length`) instead of a retriable decode error | silent (panic) | `crates/data-connector-api/src/listing/connector.rs::a_short_range_body_is_a_parquet_error_and_not_a_panic`, driven through `ParquetMetaDataPushDecoder`, which is the public surface `push_range` sits behind. The listing/overwrite harness cannot reach it: it 412s a pinned `If-Match` before a short *successful* range body is ever decoded |
| `Decimal` → floating-point cast rounds from the exact decimal digits instead of widening the coefficient to `f64` and dividing by `10^scale` ([spiceai/arrow-rs#26](https://github.com/spiceai/arrow-rs/pull/26)) | A coefficient past 2^53 loses precision before the divide, so a decimal read back as a float is off in the low digits. Measured: a pushed-down Postgres `avg` read as `Decimal128(38, 20)` returned `47.50000000000001` instead of `47.5` ([#13978](https://github.com/spiceai/spiceai/issues/13978)) | silent (wrong data) | `crates/arrow_tools/src/record_batch.rs::test::decimal_to_float_cast_is_correctly_rounded` |
| `FlightSqlServiceClient::do_get_flight_data` — a `DoGet` that returns the raw `FlightData` stream with the client's headers and bearer token applied, as `do_get` applies them | The Flight SQL connector can no longer replace a schema message before arrow decodes it. A server that declares a `MAP`'s `entries` field nullable (which arrow 59 refuses at decode) fails every read of that table with `The nullable should be set to false for the map entries field`, naming no column. Going through `inner_mut()` instead would drop the `traceparent` and bearer token, which are private to the client | build (the method) | `crates/data_components/src/flightsql.rs::tests::query_to_stream_corrects_a_servers_nullable_map_entries_declaration` |

## datafusion-ballista

Upstream [apache/datafusion-ballista](https://github.com/apache/datafusion-ballista),
branch `spiceai-55` (upstream's `55.0.0-rc1` tag, merged into the previous line through
spiceai/datafusion-ballista#73 and #68). The fork carries its own inventory, `SPICE_FORK_CHANGES.md`.
Upstream adopted several of the patches below in that merge; those rows are kept,
marked upstreamed, because the guard still pins the behaviour and a later upstream
change could move it.
This fork is heavily Spice-modified — distributed execution is largely our code —
so the rows below name the contracts, not every commit.

| Patch | What breaks if it is lost | Loss | Guard |
|---|---|---|---|
| Cluster RPC TLS and API-key auth (fork PR #3). TLS configuration is now upstream's (apache/datafusion-ballista#1400, `use_tls`, with the fork's `client_use_tls` kept as an alias); the API-key interceptors are still ours | Scheduler/executor traffic falls back to plaintext and unauthenticated | silent (security) | `crates/runtime/tests/tls/mod.rs` (two guards) |
| Object-store shuffle storage (S3/Azure), `PrefixStore` wrapping, single-stream IPC per partition (fork PRs #9, #18, #40–#43) | Shuffles fall back to local disk, or S3 shuffle paths resolve to the wrong key | silent | `crates/runtime/tests/cluster/distributed_acceleration.rs` and the rest of `crates/runtime/tests/cluster/` |
| In-memory shuffle storage with remote-fetch fallback (fork PRs #7, #8) | Every shuffle round-trips through storage | silent (perf) | `crates/runtime/tests/cluster/in_memory_shuffle.rs` |
| Shuffle-fetch resilience: a buried `FetchFailed` surfaced so the scheduler can recover, retry on a fresh connection, h2 receive-window sizing, bounded read inactivity, unordered stream consumption (fork PRs #36, #61, #62, #63). The `FetchFailed` drill-through is carried as `find_fetch_failed_in_df` (upstream now has its own `find_fetch_failed`) and the inactivity bound as `InactivityTimeoutStream`; the fresh-connection retry is now upstream's pool `discard()` with `with_retry`, the window sizing upstream's `ballista.client.initial_*_window_size` keys, and unordered consumption upstream's `fetch_partition_buffered` | A transient fetch failure fails the whole query; large shuffles stall. The surfacing is the quiet half: a `FetchFailed` reaches the scheduler wrapped as `Shared(Arc(ArrowError(ExternalError(…))))`, and an unwrapping that stops at `ArrowError` leaves it buried, so `FailedTask::from` falls through to `FailedReason::ExecutionError` — non-retryable — and the `FetchPartitionError` recovery that reruns the offending map stage never runs | silent | **GAP** |
| Scheduler lock hygiene across persists and awaits (fork PR #60). The persist timeout (`JOB_PERSIST_TIMEOUT`) is carried; the lock-free stage-plan encoding no longer applies, because upstream dropped the stage-plan cache and encodes each task on its own | Cluster wedge / runtime freeze under load | silent (hang) | **GAP** |
| Don't swap null-aware anti joins in `JoinSelection` (fork PR #58). Upstream adopted it: `JoinSelection` honours `null_aware` on the upstream side of the merge, and the fork adds only a test | A distributed anti-join returns wrong rows | silent (wrong data) | `crates/runtime/src/cluster/datafusion/mod.rs::a_null_among_the_values_leaves_no_row_selected`, `…::a_null_among_the_values_leaves_no_row_selected_where_no_swap_is_profitable`, `…::values_without_a_null_select_every_probe_absent_from_them`, `…::the_rule_neither_swaps_the_sides_nor_drops_the_flag` — these drive the scheduler's own rule rather than a live cluster. A join that is already `Partitioned` is left alone by the rule, as upstream's is, and lowered to a single-task `CollectLeft` join by Ballista's distributed planner (apache/datafusion-ballista#2188): `…::a_partitioned_null_aware_join_is_corrected_to_collect_left` plans such a join into stages and asserts the lowered join |
| Vortex columnar shuffle format (fork PR #7) | Shuffles fall back to Arrow IPC | build | compile-guarded |
| Stale `TaskStatus` rejection for reset partitions (fork PR #53; upstreamed) | A status update already in flight when its executor was lost arrives for a partition whose task info the reset cleared. Upstream unwraps that `None`, and the panic lands on the scheduler event-loop worker: the event channel closes and every later job submission and executor heartbeat fails with `Fail to send event due to channel closed` — one late packet wedges the cluster | silent (panic, then cluster wedge) | `crates/runtime/src/cluster/datafusion/mod.rs::stale_status_for_a_reset_partition` — upstream adopted the refusal (`RunningStage::update_task_info`, now keyed by task id, refuses a status whose task info is absent or killed); the guard binds two tasks into the append-only `task_infos` the way the binder does, resets the lost executor's with `reset_tasks`, and asserts both that the late status is refused and leaves the reset intact and that the live task's status is accepted |
| An executor's poll loop accepts a vcore semaphore with no permits yet (commit `862f765b`, on the DataFusion 55 merge) — upstream's shared-semaphore change (apache/datafusion-ballista#1892) asserts at least one permit | The Spice executor registers with an empty semaphore and opens its vcores only once object stores are bound (`crates/runtime/src/cluster/mod.rs`). With the assert the poll loop panics on startup, the executor never registers, and every distributed query fails to find an executor | loud (no executor registers) | every cluster integration test that starts an executor, e.g. `crates/runtime/tests/cluster/simple.rs::test_simple_cluster_mode` — all of them timed out with `Timed out waiting for 1 executors; found 0` against the unpatched merge |
| Distributed `EXPLAIN` (fork PR #34; upstreamed — `scheduler/src/state/distributed_explain.rs` is upstream's, unmodified) | The scheduler substitutes a distributed-aware explain for the plan it was sent. Without it a cluster cannot explain its own plans, which is the only way to see how a statement was distributed | silent (no diagnostic) | `crates/runtime/tests/cluster/distributed_cayenne_catalog.rs` and `…/distributed_iceberg.rs` run `EXPLAIN` through the cluster harness (`harness.explain` issues `EXPLAIN <sql>`) and assert on the plan it emits |
| Distributed `EXPLAIN ANALYZE` and `EXPLAIN FORMAT TREE` (fork PR #34). `ANALYZE` is upstream's now (`DistributedExplainAnalyzeExec`); `FORMAT TREE` is not carried — `SPICE_FORK_CHANGES.md` records it lost, and it was already absent from the previous pin | The two other explain formats. `ANALYZE` is what reports the rows and time each distributed operator actually saw, so without it a cluster's plan can be read but not measured | silent (no diagnostic) | **GAP** — nothing here issues either form *through the cluster*. `crates/runtime/tests/cluster/distributed_task_history.rs` looks like it does and does not: it submits the plain query and uses the `EXPLAIN ANALYZE` text only as the expected `input` label of a captured `plan` row, and asserts that no `EXPLAIN ANALYZE` query ran. Closing this needs the cluster harness to issue the statement itself |
| `executor_id` persisted on `TaskInfo`, and `ExecutionGraph` exposed to embedded callers (fork PR #38) | The scheduler is embedded here rather than run as its own binary, so both are API this workspace calls; `executor_id` is also what lets a reset identify the tasks a lost executor was running | build | compile-guarded — `crates/runtime/src/cluster/datafusion/mod.rs`'s fork PR #53 guard constructs a `TaskInfo` with it, and `crates/runtime/src/cluster/shared_job_state.rs` uses the exposed graph |
| `get_job_execution_graph` re-exposed as `pub` (fork PR #49) | An embedded scheduler cannot read the graph of a job it is running | build | compile-guarded by `crates/runtime/src/datafusion/query/handle.rs` |
| Execution graphs serialized for cross-scheduler recovery (fork PR #56) | A job cannot be resumed by a scheduler other than the one that planned it, so a scheduler restart loses every in-flight distributed query | build | compile-guarded by `crates/runtime/src/cluster/shared_job_state.rs`, which calls `execution_graph_to_bytes`/`execution_graph_from_bytes` and holds an `ExecutionGraphBox` |
| `TaskInfo` proto fields 11 and 12 — `global_input_partition_ids` and `vcores_consumed` — encoded and decoded with the execution graph (merge of upstream's DataFusion 55 `main`) | Upstream's tasks now cover several input partitions, but upstream does not persist task info, so its proto has neither field; the fork's graph serialization (fork PR #56) needs both. Without them a scheduler that recovers a multi-partition task keeps only its first partition — the decoder falls back to `[partition_id]` — so the rest are never rescheduled after an executor loss, and the vcore refund comes out as 1, so the executor's budget drifts. Removing the fields breaks the build only if the encode/decode goes with them; a merge that takes upstream's proto and keeps the fallback decode still builds | silent (lost partitions on recovery) | `crates/runtime/src/cluster/shared_job_state.rs::a_persisted_multi_partition_task_keeps_its_partitions_and_vcores` loads, through `SharedJobState::load_graph`, a persisted graph whose successful stage holds one task covering partitions `0..=2` with two vcores, asserts the decoded task keeps both, then saves it with `put_graph` and asserts the bytes carry both fields. The graph is a hand-built protobuf rather than one driven through a scheduler: `crates/runtime/tests/cluster/scheduler_failover.rs` recovers a job that never bound a task, and a running stage is persisted without its task infos |
| A missing partition file is read as an empty partition (fork PR #54) | A map task that produced no rows for a given reducer partition writes no file for it, and the executor's flight service answers the fetch `NotFound`. Read as a failed fetch that is a failed query; read as the data-level signal it is, the partition is simply empty. Fork PR #57 keeps the pooled client on a `NotFound` for the same reason — it is not a broken connection | silent (query failure) | **GAP** — needs a cluster and a shuffle with an empty partition |
| Shuffle-fetch clients pooled per peer instead of dialled per fetch, with HTTP/2 keepalive on the pooled connections (fork PR #57) | `fetch_partition_remote` opened a fresh `BallistaClient` — a new gRPC connection and TLS handshake — for every partition fetch, and a distributed shuffle has every reducer partition fetch from every map peer, so one query issues thousands of concurrent connection attempts. Under CPU load those handshakes run slow enough that clients abort mid-handshake and peers report `connection reset by peer`, failing the fetch and the query. One client per `(host, port, use_tls)` is cloned per fetch instead, collapsing the storm to one connection per peer, and is evicted on failure so the next fetch reconnects | silent (query failure under load) | **GAP** — the failure needs a cluster under enough CPU load to slow a handshake; measured by the fork on a distributed TPC-H run |
| A task's file scan is restricted to its own partition (fork PR #57, porting apache/datafusion-ballista#1907; upstreamed — the fork's `restrict_scan_to_partition` is replaced by upstream's `scheduler/src/state/task_builder.rs::restrict_plan_to_partitions`) | Each executor task runs one partition on its own plan instance, in its own process. Restricting the scan narrows the leaf `DataSourceExec`'s `FileScanConfig` to that task's own file group before execution; without it, the task's lone stream drains the scan's shared file work-queue by itself and reads every file in the table, so every aggregate over the scan is inflated by however many tasks read it, on a query that reports success | silent (wrong data) | `crates/runtime/tests/cluster/ballista_partition_scoped_scan.rs::distributed_scan_reads_each_task_its_own_file_group` (its doc comment still names the fork's replaced `restrict_scans` as the revert point; the behaviour it asserts is unchanged) — a three-file, non-accelerated dataset distributed across two executors (`target_partitions = 3`, so the scan plans as three file groups — each task gets its own freshly-deserialized plan instance regardless of which executor runs it, which is what the loss needs); `COUNT(*)`/`SUM(id)` must return the exact total. Reverting the patch locally (`restrict_scans` returning its input plan unchanged) reproduces the loss directly: the same query returns `COUNT(*) = 48` — exactly 3× the true 16 rows, one full over-read per task — with the patch restored it returns 16 |
| Physical uncorrelated scalar subqueries disabled under distributed stage splitting (fork PR #57, porting apache/datafusion-ballista#1909; upstreamed — `enable_physical_uncorrelated_scalar_subquery` in `core/src/extension.rs`) | An uncorrelated scalar subquery plans as a physical `ScalarSubqueryExec` the executor cannot decode once stage splitting separates it from its parent, so TPC-H q11/q15/q22 fail outright unless the restricted configuration disables them into joins instead | query failure | **GAP** — needs a distributed plan whose stage splitting isolates an uncorrelated scalar subquery from its parent; the fork observed the failure via TPC-H, not a minimal repro built here |
| Reconciliation sweep for pull-based stage revival and lost job completion (fork PR #57) | Pull-based scheduling resolves downstream stages only on the event-driven path, so one lost or raced revival wedges the job forever: it stays `Running` with no available tasks, executors poll and get nothing, and the scheduler reports itself healthy. The same sweep re-emits `JobFinished` for a graph that is fully successful but still in the active cache, which is the other way a finished query never finishes | silent (hang) | **GAP** — needs a cluster and a lost revival; the fork observed it on a SF10 distributed TPC-H query whose correlated-subquery DAG completed its branch stages and never resolved the dependants |
| Job graph persisted off the event loop and outside the execution-graph write lock, and awaited so job status advances monotonically (fork PR #57) | Persisting inside the lock and on the event loop drops task updates that arrive while the write is in flight; not awaiting it lets a status poll read a graph older than one it has already been shown, so job status goes backwards | silent (dropped task updates, status regression) | **GAP** — a race between a persist and a poll, so any test of it is a timing test |
| Task statuses re-delivered after a failed `poll_work` (fork PR #57) | An executor that fails to deliver a batch of task statuses drops them, so the scheduler never learns those tasks finished and the stage waits on work that is already done | silent (hang) | **GAP** — needs a cluster and an induced `poll_work` failure |
| Terminal job status persisted before the job leaves the active cache (fork PR #59; upstreamed as `persist_terminal_and_evict`, apache/datafusion-ballista#2037) | `succeed_job` removed the job from the active execution-graph cache before the `save_job` write completed, so a concurrent `get_job_status` fell through to the not-yet-updated shared state and answered a stale `Running`. The distributed query client polls on a 2s budget, so it reported a timeout for a query that had in fact succeeded | silent (a successful query reported as a timeout) | **GAP** — a race between a status poll and a save, so any test of it is a timing test; measured by the fork against the client's poll budget |
| Stuck-query detection (fork PR #39) | A distributed query that stops making progress is not reported, so it has to be diagnosed by rerunning it | not carried | none — not carried. There is no watchdog or progress sampler at this pin or the previous one, and `SPICE_FORK_CHANGES.md` records the patch lost; restoring it would need a row with a guard |

## datafusion-federation and datafusion-table-providers

Upstream `datafusion-contrib/*`. Both are **Spice-maintained in practice** — upstream
has not moved in a long time and essentially the entire content of these forks is
ours (`datafusion-federation`: 148 commits ahead; `datafusion-table-providers`: 251).

Their exposure is different in kind, not absent. Both are still re-cut per DataFusion
major, but the base of the new branch is *our own* previous branch rather than a moved
upstream, so a patch dropped in a conflict resolution stays visible in our own `git`
history instead of being replaced by upstream code. That makes the loss recoverable
and attributable, which is what the per-patch tables above exist to provide, so no
table is kept for these two.

What does cover them is the federation and SQL-connector integration suites
(`crates/runtime/tests/{postgres,mysql,sqlite,duckdb,clickhouse,…}` and
`crates/data_components/src/federation.rs`), which exercise these forks on every run
rather than patch by patch.

Two things would change that and mean giving each a table here: either fork being
rebased onto a moved upstream, or upstream resuming releases we track.

`datafusion-table-providers` takes `datafusion-federation` from **git, on the same
URL this workspace uses** (its own workspace `Cargo.toml`). Nothing here can
redirect that: `[patch.crates-io]` does not apply to a git dependency, and Cargo
refuses a git-source patch naming the same source — "patches must point to
different sources". So the two pins move together, and a bump to one that leaves
the other behind puts two copies of the crate in the graph: `data_components`
stops compiling with "the trait bound `SqlTable<T, P>: SQLExecutor` is not
satisfied … there are multiple different versions of crate `datafusion_federation`
in the dependency graph", and `scripts/check_fork_patches.py` reports the same
thing as two pinned revisions at once.

The patches below have rows, because losing one of them is a wrong answer or a
silently disabled optimization rather than a build failure. Every row was
re-confirmed present at both pinned revisions; each new pin descends linearly from
the previous one, and the federation analyzer (`analyzer/mod.rs`) is unchanged
between them:

| Patch | What breaks if it is lost | Loss | Guard |
|---|---|---|---|
| Federation pushes filters into scans it does not own, so a scan must decline what it cannot evaluate (repo-side guard against `datafusion-federation` `contains_federated_table`, which arms the analyzer's whole-plan filter pushdown as soon as *any* table in the statement is federated) | The analyzer runs before decorrelation, so a subquery accepted by a non-federated scan is written into its filters and then fails physical planning with "Physical plan does not support logical expression". Verified: with one federated table present, `EXISTS`/`IN`/`ANY` are offered to an unrelated non-federated scan | silent (query failure) | `crates/runtime/tests/acceleration/on_zero_results_subqueries.rs`: `duckdb_on_zero_results_subqueries` and `sqlite_on_zero_results_subqueries` execute correlated scalar, `IN`, and `EXISTS` queries with populated and empty acceleration, explicit `return_empty` controls, and mixed federation. `cayenne_on_zero_results_subqueries` is the local-accelerator control. The guard reads storage directly before querying through the runtime. Requires runtime features `duckdb,sqlite`. This is a repo-side guard rather than a patch carried on the fork: it is listed here so a pin move re-checks whether the fork still needs it |
| DuckDB timestamp literal rendered through microseconds, not a `DOUBLE`, with DuckDB's two infinity sentinels declined (fork PR #57) | `TO_TIMESTAMP` takes a `DOUBLE`, so a timestamp count past 2^53 µs (≈ year 2255, and symmetrically before ≈ 1684) could not be represented exactly: a pushed-down filter names a neighbouring microsecond and keeps or drops the wrong row. Separately, a `TimestampMicrosecond` holding ±`i64::MAX` rendered fine, was reported pushdown-capable, and then failed the statement built from it | silent (wrong rows) | `crates/search/src/index/duckdb/sql.rs::a_timezone_aware_timestamp_filter_is_rendered_without_the_millisecond_truncation` and `::a_microsecond_count_past_the_double_bound_is_rendered_exactly`, `query_exec.rs::scan_renders_filters_against_the_whole_table_schema`, and `sql.rs::a_timestamp_duckdb_cannot_hold_is_declined_by_both_the_probe_and_the_statement`. The first and third pin the rendered microsecond count, so a revert to the `DOUBLE` form fails them; the second names a count above 2^53, which is the precision the patch exists for; the fourth names the two sentinel counts, so a revert that renders them instead of declining them fails there rather than inside a generated statement |
| Cancel an abandoned ADBC query and release its pooled connection (fork PR #65) | Dropping the record-batch stream only detaches the `spawn_blocking` task; it stays inside `Statement::execute` until the remote query finishes on its own, and owns the pooled connection for that whole time. Repeated cancellations then exhaust the pool ([#13781](https://github.com/spiceai/spiceai/issues/13781)) | silent | `crates/data-connectors/connector-adbc/tests/adbc_cancellation.rs::dropping_the_stream_cancels_the_query_and_frees_the_pool_connection`, which fails if either this patch or the `arrow-adbc` one is missing |
| Date literal rendered in the unit and width the type calls for (fork PR #60) | `Date32` literals overflow `i32` past 2038 and `Date64` literals render as if milliseconds were days, so a federated filter or join on a date column matches the wrong rows ([#13476](https://github.com/spiceai/spiceai/issues/13476)) | silent | `crates/search/src/index/duckdb/sql.rs::a_date32_literal_outside_the_i32_second_range_names_the_day_it_holds` and `::a_date64_literal_is_read_as_milliseconds_not_as_days`, with `::a_date32_literal_inside_the_i32_second_range_renders_unchanged` as the control against a renderer that declines or shifts every date. The scaling the patch fixes is shared by every engine arm — only the call it is formatted into differs — so a revert fails those three whichever engine renders; `::the_sqlite_arm_renders_a_date_from_the_same_count` pins the sibling arm directly, because this repository renders through that function for DuckDB alone |
| `DuckDBTable` carries its index list onto the `DuckSqlExec` it builds (fork PR #69) | `DuckDBIntermediateIndexMaterialization` reads that list off the exec node and returns the plan untouched when it is empty, so a DuckDB accelerator declaring `indexes` stops materializing the indexed filters into a CTE and every such query goes back to scanning the whole table. The field has twice survived a refactor that defaulted it to empty, which is why it has a row | silent (perf) | `crates/datafusion-optimizer-rules/src/physical_plan/duckdb/intermediate_index_cte.rs::tests::a_tables_indexes_reach_the_rule_through_the_exec_node`, which plans a filtered scan off a `DuckDBTable` built with an index and asserts the rule rewrites it. Needs `--features duckdb`. The neighbouring `test_rewrite_statement` passes either way — it hands `rewrite_statement` its indexes directly and never crosses the table/exec boundary the patch restores |
| `FunctionSupport` per-call check (fork PR #61) | A function a backend carves out of the deny-list because its dialect rewrites it federates in *every* call shape, including the ones the dialect cannot render. The unparser then emits the function verbatim into the remote SQL — the unknown-function failure of [#10703](https://github.com/spiceai/spiceai/issues/10703) | build, then silent | `crates/data-connectors/connector-adbc/src/lib.rs::function_support_tests::bigquery_refuses_the_json_call_shapes_its_dialect_cannot_translate` and `::an_untranslatable_predicate_is_left_above_the_federated_scan`. Losing the API fails `cargo check`; a re-cut that keeps `with_scalar_call_support` and drops its use in `contains_unsupported_functions` fails these instead |
| `FunctionSupport::with_aggregate_call_support` and `with_window_call_support`, and the aggregate/window arms of the walk that consult them (table-providers PR #70; re-confirmed at the pinned revision) | The name-based `FunctionRestriction` cannot refuse a *shape*, and the aggregate and window slots were unused entirely — so every aggregate and window call federated unconditionally. The BigQuery dialect declines the filtered-aggregate shapes it cannot rewrite exactly, and a declined rendering is a **failed query** unless federation refuses the same shape, because a federated statement has no local-execution fallback. Losing this patch therefore does not lose a pushdown, it breaks the query: `array_agg(v) FILTER (…)` reaches BigQuery as `FILTER` it cannot parse, and `COUNT(x) FILTER (…) OVER (…)` does the same through the window arm (measured: `Syntax error: Expected ")" but got "("`) | silent (query failure) | `crates/data-connectors/connector-adbc/src/lib.rs::bigquery_refuses_only_the_filtered_aggregate_shapes_it_cannot_rewrite`, which tests the allowlist *boundary* rather than a fixed set because the two sides live in different repositories, and `::bigquery_refuses_a_filtered_window_call` |
| `FunctionSupport` expression policy gates logical federation, scan filter admission, and convertible physical-filter pushdown ([table-providers PR #75](https://github.com/spiceai/datafusion-table-providers/pull/75)) | BigQuery has no `ILIKE` operator. Losing any gate either emits invalid GoogleSQL or lets a remote `LIMIT` run before the residual local predicate, which can return the wrong row or no row | silent (query failure / wrong rows) | `crates/runtime-datafusion/src/function_support.rs::tests::bigquery_refuses_only_case_insensitive_like_expressions`; `crates/data-connectors/connector-adbc/src/lib.rs::function_support_tests::{bigquery_refuses_case_insensitive_like_but_keeps_like,bigquery_catalog_refuses_case_insensitive_like_but_keeps_like,bigquery_ilike_stays_local_before_limit_on_every_registration_path,bigquery_ilike_residuals_preserve_boolean_and_projection_boundaries,bigquery_supported_like_and_comparison_still_push_down}`; the fork's `supported_functions`, `federation`, scan-filter, and physical-filter policy tests |
| Analyzer: recursive work tables are neutral, with dialect renderability checked before selecting a remote plan (federation PR #84) | Recursive joins split at the work table, or an unsupported dialect receives a plan it cannot execute | silent (query failure / perf) | `crates/data-connectors/connector-adbc/src/lib.rs::function_support_tests::bigquery_federates_a_recursive_cte_and_its_remote_join`; real-engine guard: `test/scripts/bigquery_pushdown.py::recursive-cte-joined-to-a-table`. The fork also guards unsupported-dialect fallback |
| Analyzer: consider the complete recursive CTE before splitting its terms (federation PR #85) | A remote scalar-subquery bound makes an incomplete recursive term look unfederatable, so the enclosing CTE executes locally | silent (extra remote jobs) | `crates/data-connectors/connector-adbc/src/lib.rs::function_support_tests::bigquery_federates_a_recursive_cte_and_its_remote_join` (the scalar-bound case) |
| Analyzer: table rewrites resolve existing output names and preserve aliases and field metadata, including UNNEST projections (federation PR #84) | Computed output references stop resolving or explicit user aliases and metadata change | silent (query failure / output schema) | Real-engine grouped-expression cards in `test/scripts/bigquery_pushdown.py`; in the fork, `test_rewrite_table_scans_moves_a_pinned_name_with_its_table`, quoted user-alias controls, and `test_rewrite_unnest_preserves_alias_metadata` |
| Schema cast is strict, so a value that will not fit its declared type errors instead of becoming NULL (federation PR #67) | `SchemaCastScanExec` brings every batch a remote returns to the schema the plan declared. Arrow's default cast is the *safe* one: a value the target type cannot hold becomes NULL rather than an error. So a federated column read one width too narrow — a remote `BIGINT` the plan typed `INT`, which is what a schema inferred from one source and used against another gives — answers with NULLs where the remote sent numbers, on a query that reports success | silent (wrong data) | `crates/data_components/src/federation.rs::a_federated_value_too_wide_for_its_declared_type_is_an_error_not_a_null`, which casts one past `i32::MAX` and requires the error, with an in-range value as the control so a cast that refused everything could not pass |
| DML plans are returned unwrapped rather than federated (federation PR #73) | The analyzer wraps the largest federable sub-tree in a `FederatedPlanNode`, and a `Dml` node over a federated table qualifies. Wrapped, it cannot be rendered — the unparser has no `dml_to_sql` — and `DataFusion`'s physical planner dispatches `delete_from`/`update` by matching `LogicalPlan::Dml`, which it cannot do through a `LogicalPlan::Extension`, so `INSERT`/`UPDATE`/`DELETE` against a federated table stops working. The fork gives a third reason, that a wrapped `Dml` is invisible to a write-permission validator that walks for it; that is the fork's rationale rather than a claim reproduced here, because `validate_sql_query_operations` runs on the plan `create_logical_plan` returns and analyzer rules have not run at that point | silent (query failure) | `crates/data_components/src/federation.rs::nothing_under_a_dml_plan_is_federated_by_the_analyzer`, which asserts on the `Dml`'s *input* rather than its root: losing the patch federates what sits under the node rather than replacing it, so the root is a `Dml` either way. Its control establishes that the input is a shape the analyzer really does federate, without which the assertion would hold for the wrong reason — and a `Limit` is used rather than a filter because a filter is pushed into the scan before federation runs, collapsing the plan to a bare `TableScan` that the adaptor serves itself and the analyzer leaves alone. The fork's own `dml_plan_is_returned_unchanged` builds its DML over a plain table, where the early return is not what makes it pass. The same fork PR also has `FederatedTableProviderAdaptor` forward `delete_from` and `update` to the provider it wraps — an independent behaviour, since `TableProvider` defaults both to reporting the operation unsupported, so a re-cut can keep the early return and drop either method and still compile; `…::the_adaptor_forwards_dml_to_the_provider_it_wraps` covers that half |
| `EXISTS`/`NOT EXISTS` subqueries are seen and federated by the analyzer (federation PR #74) | `Expr::Exists` fell through the expression walk, so the tables inside an `EXISTS` subquery were invisible to the provider verdict and the subquery was never federated: it executes locally, one scan per table reference, while the statement around it federates — the shape the scanless-correlation row above was measured at, 24 statements where one was correct. The patch also wraps a federated subquery in a no-op `Projection`, because `DecorrelatePredicateSubquery` will not take a `LogicalPlan::Extension` as a subquery and leaves the correlation undecorrelated otherwise | silent (perf, badly) | `crates/data_components/src/federation.rs::an_exists_subquery_over_a_federated_table_is_federated`, whose outer table is deliberately *not* federated so the statement cannot federate as one unit — it asserts nothing outside the subquery is wrapped, and then that the subquery is, so the second assertion cannot be met by the wrong node, and `…::a_not_exists_subquery_is_federated_and_stays_negated` for the negated shape — the patch rebuilds the expression rather than wrapping it, carrying `negated` across by hand, and a rebuild that reset the flag federates exactly as well while returning the complement of the rows asked for. In the fork, seven `sql/mod.rs` snapshots across same-provider, cross-provider and mixed-provider shapes, which leave with the patch |
| ADBC schema fetch leaves a query's own `WITH` at the top level (table-providers PR #71) | A driver using the query-based schema fallback nests `WITH RECURSIVE` inside the schema-probe CTE, which BigQuery rejects | silent (query failure) | `test/scripts/bigquery_pushdown.py::recursive-cte-joined-to-a-table`, which executes through the real driver; EXPLAIN alone does not exercise schema discovery |
| Analyzer (federation PR #83): federate a statement whose only tables are inside a subquery — `contains_federated_table` descends into subquery expressions, and a correlated reference to a relation that scans nothing is neutral rather than ambiguous | A query whose outer `FROM` is a constant relation and whose federated tables are all inside a scalar/`IN`/`EXISTS` subquery is not federated *at all, in any part*: the analyzer returns before doing anything, or the unresolved correlation reads as a second engine and that verdict propagates through every enclosing node. The statement reaches the engine as one scan per table reference — each re-executed for every place the plan mentions it — with every join and aggregate evaluated locally. A dashboard card of this shape was measured at 24 statements where one was correct | silent (perf, badly) | `datafusion-federation/src/sql/mod.rs::tests::a_correlation_against_a_scanless_relation_federates_as_one_statement` (in the fork, with `::a_correlation_against_a_scanning_relation_still_federates_as_one_statement` as the control); real-engine guard: `test/scripts/bigquery_pushdown.py::correlated-subquery-over-constant-relation`. The neutral verdict is deliberately narrow — it needs a unique relation of that name that scans nothing — because binding a correlation to the wrong relation of the same name would return wrong rows rather than fail |
| `SchemaCastScanExec` forwards its input's statistics through DataFusion 55's `StatisticsContext` (`child_stats_requests` / `statistics_from_inputs`; federation commit `b150542`) | The cast node would report unknown row counts and min/max over an input that has them, so join sizing and statistics-answered aggregates degrade through a federated scan. A performance loss, not wrong rows — and, by inspection only, not observable on today's paths: the fork builds the node over `VirtualExecutionPlan`, whose statistics come from `SQLExecutor::statistics`, which no executor here overrides | silent (perf) | `crates/data_components/src/federation.rs::a_schema_cast_scan_reports_its_inputs_statistics` builds the node over a `MemorySourceConfig` scan with an exact row count and asserts `StatisticsContext` reports that count through it. In the fork, `schema_cast/mod.rs::tests::schema_cast_forwards_input_statistics` |
| MongoDB filters are pushed down only where MongoDB selects the rows SQL keeps from the converted documents — negations that exclude null and missing fields, conditions guarded by the BSON types a column accepts, arrays and `ObjectId`s and symbols rendered as the conversion renders them, NaN and signed zeros, the collection's collation, and casts that keep values — and the scan claims no sort order; a projection names no path beside one of its prefixes; a date too distant for a microsecond or nanosecond column reads as NULL ([table-providers PR #77](https://github.com/spiceai/datafusion-table-providers/pull/77)) | The operator-for-operator translation comes back: `$ne`/`$nin` keep rows SQL evaluates to NULL, a type-bracketed comparison drops a value rendered into a string column, a case-insensitive default collation narrows `<>` and ranges, `TRY_CAST` to a finer unit is read as the stored value, and MongoDB's sort order (nulls first, types before values) is reported to `DataFusion` as the SQL one. A filter that matches nothing, such as `x = 2.5` on an integer column, matches every document of a view that projects `_id` away. With unnesting, `SELECT *` over a field that is a document in some documents and a scalar in others fails with `Path collision`, and a year-2262 date in a column declared `timestamp` wraps to 1677 | silent (wrong rows); query failure | `crates/runtime/tests/mongo/pushdown_roundtrip.rs::mongodb_pushdown_round_trips`, which runs each query against the federated dataset and an Arrow acceleration of the same collection and requires the same rows, and that each case expected to push down did. Its `noid` view projects `_id` away, its `nested` collection fails to load with the path collision, and its `distant` collection asserts the NULLs directly, since both sides share the conversion. Needs Docker and the `mongodb` runtime feature. The fork's `mongodb::utils::expression` tests pin the translated documents |
| `VirtualExecutionPlan` asks the executor for the `SELECT 1` placeholder of an empty projection and reduces it to the row count (federation PR #91) | A federated scan whose plan needs no column — one that only filters, or only feeds `count(*)` — is unparsed as `SELECT 1` for `DuckDB` and `SQLite`, which have no empty select list, while the executor is told to expect no column: `DuckDB` fails with "Unexpected number of columns. Expected: 0, Found: 1" and the `SQLite` row decoder panics, surfacing as `ConnectionClosed`. TPC-DS q9 failed this way on both accelerators while decimal `AVG` was kept out of the pushed-down plan (#14670) | silent (query failure) | `crates/runtime/tests/acceleration/empty_projection_scan.rs`: `duckdb_filter_only_scan_keeps_its_rows` and `sqlite_filter_only_scan_keeps_its_rows`, in memory and file modes. The statement only filters the accelerated table and selects a scalar subquery over an Arrow acceleration, the TPC-DS q9 shape; the test asserts the exact rows and that the filtered table is pushed down as `SELECT 1 FROM …`, so a plan that stops producing the empty projection fails it rather than passing vacuously (a cross join of the same tables reads the filter column and would) Requires runtime features `duckdb,sqlite`. In the fork, `sql/mod.rs::tests::empty_projection_*` |

## arrow-adbc

Upstream [apache/arrow-adbc](https://github.com/apache/arrow-adbc), branch
`spiceai-24-patches`: upstream tag `apache-arrow-adbc-24` — the commit the
`adbc_core` / `adbc_driver_manager` / `adbc_ffi` 0.24.0 crates were published from —
merged into the previous line, so the only delta a build picks up is the patches
below. Both were re-confirmed at the pinned revision. All three crates are replaced together through `[patch.crates-io]`: the patch changes an
`adbc_ffi` signature that `adbc_driver_manager` calls.

| Patch | What breaks if it is lost | Loss | Guard |
|---|---|---|---|
| `AdbcStatementCancel` issued without the statement lock, and statement release moved from the clonable handle to the shared inner (fork PR #4) | `cancel` is serialized behind the `execute` it exists to interrupt, so it returns only once the query has finished on its own and cancels nothing. A Flight client that goes away then leaves the remote query running — billing, on BigQuery — and holds its pooled connection for the rest of that query's life, which exhausts a small pool ([#13781](https://github.com/spiceai/spiceai/issues/13781)) | silent | `crates/data-connectors/connector-adbc/tests/adbc_cancellation.rs::dropping_the_stream_cancels_the_query_and_frees_the_pool_connection` |
| Arrow requirement narrowed to the one major the workspace builds against, now `>=59.0.0, <60` (fork PR #4 set it to 58; moved to 59 in the merge of ADBC 24, where upstream's own range is `>=58, <60`) | A range spanning two majors lets a workspace that also carries an older arrow subtree — a geospatial stack on an older `DataFusion`, say — resolve the ADBC crates onto it while the rest of the workspace runs on 59. Two copies of `arrow-schema` then exist and a `Schema` does not match across the ADBC boundary; the enterprise runtime does not compile | build | `cargo check -p connector-adbc` in the enterprise runtime, which fails with `expected arrow_schema::Schema, found arrow_schema::schema::Schema` when the requirement is widened |

## duckdb-rs

Upstream [duckdb/duckdb-rs](https://github.com/duckdb/duckdb-rs), branch
`spiceai-1.4.4`: the previous line plus the arrow 59 bump (fork PR #49), the chrono
write backport (fork PR #50) and the Rust 1.98.1 toolchain (fork PR #51), all below
or build-only — every other row was re-confirmed present, unchanged, at the pinned
revision. Against the previous pin, `8ee4307` (the head of `spiceai-1.4.4-patches-2`,
which #49 was squash-merged from), the only other source change is an equivalent
`RecordBatch::try_from_iter(columns)` in `vtab/arrow.rs`.

| Patch | What breaks if it is lost | Loss | Guard |
|---|---|---|---|
| `duckdb_arrow_scan` support — `register_arrow_scan_view` for Arrow-stream ingestion (fork PR #18) | DuckLake writes and Arrow-stream ingestion have no path in | build | `crates/data_components/src/ducklake/writer.rs` calls it |
| ICU extension statically linked into bundled DuckDB (fork PR #23) | Any query using a named timezone (`AT TIME ZONE 'America/New_York'`) fails at runtime, and DuckDB tries to download the extension from the network | silent (query failure) | `crates/accelerators/accelerator-duckdb/src/lib.rs::bundled_duckdb_resolves_a_named_time_zone_without_installing_icu` |
| VSS (HNSW) extension statically linked (fork PR #37) | Vector search over a DuckDB accelerator fails, or silently falls back to a full scan | silent (query failure) | `crates/accelerators/accelerator-duckdb/src/lib.rs::bundled_duckdb_builds_an_hnsw_index_without_installing_vss` |
| Bundled DuckDB version pinned to the release (fork PR #38) | Extension downloads resolve against a mismatched DuckDB version and fail | silent | covered by the two extension guards above |
| Thrift `TEnumIterator::operator==` backport for macOS 27 / libc++ (fork PR #47; upstream [duckdb/duckdb@fccde6aa](https://github.com/duckdb/duckdb/commit/fccde6aa1932f48dfa6282a916ea2477b57aa44d)) | Bundled DuckDB with Parquet fails to compile against the macOS 27 SDK: newer libc++ constructs Thrift enum maps with `iterator == end`, and the vendored Thrift header only defined `operator!=` | build (macOS 27) | `scripts/check_fork_patches.py::duckdb_thrift_iterator_equality` — reads `operator==(const TEnumIterator` out of the pinned revision's `duckdb.tar.gz`. The fork's own `crates/libduckdb-sys/tests/test_bundled_thrift.py` covers the same property inside the fork and does not survive a re-cut of this pin |
| Arrow bumped to 59 (fork PR #49, squash-merged into `spiceai-1.4.4`), with the fork's `test_fixed_array_roundtrip` rebuilt from a whole number of lists — arrow 58 silently dropped a trailing value that arrow 59 rejects | Upstream duckdb-rs is on arrow 58 on every line. Without the bump the workspace resolves two arrow majors, and every Arrow value that crosses into or out of DuckDB — the accelerator's appender and its query results, `register_arrow_scan_view` — is a type from the other copy, so the crates that call it stop compiling. The test fix is fork-internal and changes no behaviour here | build | compile-guarded by `crates/accelerators/accelerator-duckdb` and `crates/data_components/src/ducklake/writer.rs`, which pass arrow types across that boundary |
| `ToSql for DateTime<Tz>` writes the UTC instant instead of the local wall-clock fields with a `+00:00` suffix (fork PR #50, backport of upstream duckdb-rs `2bf67df`) | A non-UTC `chrono::DateTime` bound as a parameter is stored shifted by its UTC offset | none in this workspace | Not reachable here: the impl is behind duckdb-rs's `chrono` feature, which no crate in this workspace enables (`cargo metadata` resolves `duckdb` without it), so nothing binds a `chrono::DateTime` through `ToSql`. A change that enables `chrono` turns this into a silent wrong-data row and needs a guard with it |

## iceberg-rust

Upstream [apache/iceberg-rust](https://github.com/apache/iceberg-rust), branch
`spiceai-0.11.0-df-55`: the 0.10.1 / DataFusion 55 line with upstream's `0.11.x`
release branch (0.11.0 RC) merged in (spiceai/iceberg-rust#55). Each row below was
re-confirmed present at the pinned revision; the SigV4 row was re-implemented on
upstream's new REST auth API.

| Patch | What breaks if it is lost | Loss | Guard |
|---|---|---|---|
| `RowDeltaAction` for row-level deletes via delete files (fork PR #28) | `DELETE` against an Iceberg table has no commit path | build | `crates/data_components/src/iceberg/delete.rs` calls `tx.row_delta()` |
| SigV4 signing for REST catalogs on AWS Glue (commits `c9f1c85`, `f67e44c`; re-implemented as an `AuthManager` in `crates/catalog/rest/src/auth/sigv4.rs` by fork PR #55) | Glue-backed Iceberg catalogs fail to authenticate | build (module) + silent (signing) | `crates/runtime/src/catalogconnector/iceberg.rs` wires `rest.sigv4-enabled`; the signing itself is guarded by `crates/data_components/src/iceberg/catalog/rest/catalog.rs::a_sigv4_catalog_signs_every_request_it_sends`, with `…::a_catalog_without_sigv4_sends_no_signature` as its control |
| Limit push-down for `IcebergTableProvider` (fork PR #19) | `SELECT … LIMIT n` scans the whole table | silent (perf) | `crates/data_components/src/iceberg/provider.rs::a_scan_given_a_limit_reads_no_more_rows_than_it_asked_for` for the single-node scan, counted at the provider because a `GlobalLimitExec` above it returns the right rows either way; the distributed path is covered by `crates/runtime/src/cluster/datafusion/codec/spice_physical_codec.rs`, which refuses to serialise a scan whose limit it cannot carry |
| Pinned snapshot reads in `IcebergTableProvider` (fork PR #45) | A scan reads the current snapshot instead of the pinned one — time-travel and repeatable reads silently return live data | silent (wrong data) | `crates/data_components/src/iceberg/provider.rs::a_scan_pinned_to_a_snapshot_reads_that_snapshot_not_the_current_one` |
| Parallel file scanning with eager task bucketing (fork PR #43) | Iceberg scans lose file-level parallelism | silent (perf) | **GAP** |
| `IcebergTableProvider::try_new` made public | No construction path from Spice | build | compile-guarded by `crates/data_components/src/iceberg/provider.rs`, which calls it |
| `IcebergTableProvider::catalog` and `::table_ident` exposed (fork PR #49, on `spiceai-0.11.0-df-55`) | The Iceberg REST `loadTable` cannot load the table a provider reads, so no table can be served to an Iceberg client as itself | build | compile-guarded by `crates/runtime/src/http/v1/iceberg/passthrough.rs`, which calls both; what is served is guarded by `…::tests::an_iceberg_table_read_unchanged_is_served_as_itself` |
| Extended file metadata (`FileIO::lister`, `FileMetadata::mode`) — **carries no code** | Nothing. Recorded so the next audit does not go looking: upstream moved opendal out of the core crate, and re-adding a `Lister` and an `EntryMode` there would put the dependency back and break every `Storage` impl. Spice reaches neither — its Hadoop catalog uses its own opendal `Operator::lister()` — so the commit on the branch is a README whitespace change kept for provenance | none | not applicable |

## async-openai

Upstream [64bit/async-openai](https://github.com/64bit/async-openai).

| Patch | What breaks if it is lost | Loss | Guard |
|---|---|---|---|
| `reasoning_content` on `ChatCompletionResponseMessage` | Reasoning models' output is dropped from responses | build | every provider in `crates/llms` constructs the field |
| Azure Entra token auth in `config.rs` | Azure OpenAI with Entra credentials cannot authenticate | build | compile-guarded |
| `post`/`post_stream` and the GET operation made public | Non-OpenAI providers built on the same client lose their entry point | build | compile-guarded |
| Don't serialize nulls; hide `usage` when null (fork PR #32) | Requests carry explicit `null`s that some OpenAI-compatible servers reject | silent (request failure) | `crates/runtime/src/model/wrapper/mod.rs::a_streamed_request_carries_no_null_stream_option`, with `…::unset_stream_options_serialize_to_an_empty_object` pinning the same property at the type |
| `Eq`/`Hash` on `EmbeddingInput` and `CreateEmbeddingRequest` | Embedding request caching cannot key on the request | build | compile-guarded |
| `Authorization` is sent only when there is a key to send | Spice builds every OpenAI client through `new_openai_client_with_chat_backend`, which starts from `with_api_key("")` on purpose — so the library cannot pick a key up from the environment — and overrides it only when one was configured. Upstream inserts the header unconditionally, so without this a model with no `api_key` sends `Authorization: Bearer ` with an empty value on every request, and an OpenAI-compatible endpoint that needs no key refuses the malformed credential instead of serving it | silent (request failure) | `crates/llms/src/openai/mod.rs::authorization_header_tests::a_client_with_no_api_key_sends_no_authorization_header`, which drives a health check through that constructor against a one-shot local endpoint and reads the request it received, with `::a_client_with_an_api_key_sends_it_as_a_bearer_token` as the control — otherwise the first would pass on a client that sent no credential at all |
| `EasyInputMessage::type` is `#[serde(default)]` | The Responses API does not send `type` on every message, and upstream requires the field, so a reply that omits it fails to deserialize and the request errors — on the path `responses_adapter` builds and reads (`InputItem::EasyMessage`) | silent (request failure) | `crates/llms/src/openai/responses_adapter.rs::tests::a_responses_message_deserializes_with_or_without_its_type_field`, which deserializes the field both absent and present, so a default that swallowed the field would not pass |
| A 404 ends the retry loop instead of being retried | The retry path treats a 404 as permanent, because it means the base URL is wrong rather than that the service is busy. Without it a mistyped `endpoint` is retried through the whole backoff budget before reporting, so a configuration error looks like a slow provider | silent (a configuration error reported late) | **GAP** — the difference is *when* the error arrives rather than what it is, and asserting that is a timing test. `binary(=list_models_errors)` covers the error mapping for the lister, not the retry decision |
| Aggregated rate-limit retry logging; `retry-after` honoured from the response header (fork PRs #37, #38) | One `WARN` per retried request instead of one per burst; retries ignore the server's back-off hint | silent (log noise, throughput) | **GAP** |

## clickhouse-rs

Upstream [gengteng/clickhouse-rs](https://github.com/gengteng/clickhouse-rs), pinned
by revision on branch `async-await`.

| Patch | What breaks if it is lost | Loss | Guard |
|---|---|---|---|
| `Date32` support — `DateConverter for i32`, `Value`/`ValueRef::Date32`, `FromSql for NaiveDate` (fork commit `7e98394f`, which is the pinned revision itself) | ClickHouse `Date32` columns (dates outside 1970–2149) fail to decode | build (variant) + silent (range) | `crates/data-connectors/connector-clickhouse/src/block_to_arrow.rs::a_date32_value_decodes_the_dates_a_date_column_cannot_hold` for the decode `block_to_arrow` calls and for `Date32` still reporting `SqlType::Date`, which is what selects that arm. The wire half is only reachable against a server — `column::factory`'s `"Date32"` arm is fed from the `pub(crate)` `Block::load`, and `Block::add_column` over `NaiveDate` builds the 16-bit column — so the `Date32` column in `test/scripts/setup-data-clickhouse.sql` guards it end-to-end in the ClickHouse quickstart job |
| `ConnectionError::NoPacketReceived` | A dropped connection surfaces as a less specific error | build | compile-guarded |
| `LowCardinality(Nullable(T))` decoding — the dictionary is read as plain `T`, with key 0 as NULL | Reading the column misreads the stream: the query fails with a garbage compression method or the process aborts on a huge allocation | silent (crash) | The `lc_nullable_string_column` column in `test/scripts/setup-data-clickhouse.sql`, read end-to-end by the ClickHouse quickstart job; only a server produces this wire layout |
| `Tuple` columns — `SqlType`/`Value`/`ValueRef::Tuple` and `TupleColumnData` | ClickHouse `Tuple` columns fail to decode (`Unsupported column type`) | build (variant) | compile-guarded by `crates/data-connectors/connector-clickhouse/src/block_to_arrow.rs`, and end-to-end by `tuple_column` in `test/scripts/setup-data-clickhouse.sql` |
| `ValueRef::Map` holds its entries as a `Vec` in server order | Map entries lose their order and duplicate keys, and a key type the driver cannot hash (`Date`, `UUID`, `Enum`) panics while decoding | build (type) | compile-guarded: `block_to_arrow.rs` iterates the entries as pairs |

## rusqlite and tokio-rusqlite

Upstream [rusqlite/rusqlite](https://github.com/rusqlite/rusqlite) and
[programatik29/tokio-rusqlite](https://github.com/programatik29/tokio-rusqlite).

| Patch | What breaks if it is lost | Loss | Guard |
|---|---|---|---|
| `bundled-decimal` — SQLite's `decimal` extension compiled into `libsqlite3-sys` and exposed as `sqlite3_decimal_init` | The SQLite accelerator cannot register the decimal extension, so decimal columns compare and sort as text | build (symbol) + silent (comparison) | `crates/accelerators/accelerator-sqlite/src/lib.rs::test_sqlite_decimal_round_trip` |
| `tokio-rusqlite`: relaxed `rusqlite` version bound | Version resolution fails | build | compile-guarded |

## sea-query

Upstream [SeaQL/sea-query](https://github.com/SeaQL/sea-query).

| Patch | What breaks if it is lost | Loss | Guard |
|---|---|---|---|
| SQLite backend emits a decimal declared type rather than panicking above 16 digits | `CREATE TABLE` for a `Decimal256(40, 4)` column panics; below that the declared type changes, and the SQLite reader keys value decoding off the declared type | silent (panic / wrong decode) | `crates/accelerators/accelerator-sqlite/src/lib.rs::test_sqlite_decimal_round_trip` |
| Chrono fractional seconds in SQL literals | Timestamp writeback loses microseconds for timezone-aware values rendered through `InsertBuilder` | silent (wrong data) | `crates/runtime/tests/postgres/write_back_delivery.rs::timestamp_microseconds_survive_write_back_and_echo` |

## snowflake-rs

Upstream [andrusha/snowflake-rs](https://github.com/andrusha/snowflake-rs), branch
`spiceai-59-patches`: the previous line plus an arrow 59 version bump, which touches
only the manifests, so every row below is carried unchanged.

Every row below is **GAP**, and none is reachable from this repo as it stands:
`snowflake-api` builds its URL as `https://{account}.snowflakecomputing.com` with
no host override, so no local server can stand in for Snowflake, and `responses`
is a private module, so the response types cannot be deserialized directly
either. Closing any of these needs a live Snowflake account, or an upstream
change that lets the base URL be set — the cheaper of the two, and it would make
all five testable at once.

| Patch | What breaks if it is lost | Loss | Guard |
|---|---|---|---|
| Streaming Arrow batches instead of collecting the whole result | Large Snowflake queries materialise fully in memory — OOM risk | silent (memory) | **GAP** |
| Async query response support | Long-running Snowflake queries time out | silent | **GAP** |
| Chunked JSON responses | Large JSON-format results are truncated | silent (wrong data) | **GAP** |
| Record-batch ordering fix | Result batches come back in the wrong order | silent (wrong order) | **GAP** |
| Invalid warehouse/account errors surfaced correctly | A misconfigured warehouse produces an opaque error instead of an actionable one | silent (message) | **GAP** |

## graph-rs-sdk

Upstream [sreeise/graph-rs-sdk](https://github.com/sreeise/graph-rs-sdk).

| Patch | What breaks if it is lost | Loss | Guard |
|---|---|---|---|
| Default drive for `Group` | SharePoint group-scoped datasets cannot resolve their drive | build | compile-guarded by `crates/data-connectors/connector-sharepoint` |
| Tower service setup moved to `RequestHandler` (upstream PR #494) | Middleware (retry, tracing) is not applied to Graph requests | silent | **GAP** — nothing here configures Graph middleware, so there is no behaviour of *ours* to assert on, and the only seam that reaches the client is the opt-in `sharepoint-mock-host` feature, which the gate does not build. Closing it means configuring the retry middleware this patch exists to enable, which is a change to the connector rather than a test |

## docx-rs

Upstream [bokuweb/docx-rs](https://github.com/bokuweb/docx-rs).

| Patch | What breaks if it is lost | Loss | Guard |
|---|---|---|---|
| `Render` trait for `Document`/`DocumentChild`, including paragraph newlines and table rendering | `.docx` documents cannot be turned into text — the document parser has no extraction path | build (trait) | `crates/document_parse/src/docx.rs` imports `docx_rs::Render` |
| Paragraph and table newline placement (fork commits `3bf3c89e` for paragraphs, `ccea2029` for tables) | Extracted text runs together, changing chunk boundaries and therefore embeddings | silent (wrong text) | `crates/document_parse/src/docx.rs::a_docx_separates_paragraphs_and_not_the_runs_inside_one` and `…::a_docx_table_separates_its_rows_and_cells`, which build a `.docx` in memory and assert the placement in both directions — the fork got this wrong once internally before fixing it, by separating paragraph children instead of document children |

## model2vec-rs

Upstream [MinishLab/model2vec-rs](https://github.com/MinishLab/model2vec-rs).

| Patch | What breaks if it is lost | Loss | Guard |
|---|---|---|---|
| IDs-only fast WordPiece tokenizer for the potion models | Static embedding throughput drops sharply | silent (perf) | **GAP** |
| `config.json` made optional, and the embedding tensor read from `embedding.weight` or `0` as well as `embeddings`, for sentence-transformers compatibility (fork commit `f1190f1f`) | Loading a sentence-transformers static model fails. Two independent halves of the same use case: such an export ships no `config.json` **and** names its tensor `embedding.weight`, so losing either one leaves the model failing to load | silent (load failure) | `crates/llms/src/model2vec.rs::a_local_model_loads_without_a_config_json` and `…::a_local_model_loads_with_the_sentence_transformers_tensor_names`, against a model directory the test writes — a tokenizer and a hand-built `safetensors` tensor, no `config.json`, and the tensor name as a parameter so each test is the other's control |
| HF cache directory read from the environment (fork commit `1259c0d3`) | Models are re-downloaded instead of reusing the shared cache | silent | `crates/llms/tests/model2vec_hf_cache.rs::a_cached_model_is_read_from_the_directory_hf_hub_cache_names`. Its own test binary because it sets a process-wide environment variable, so it is selected by name in the Makefile's `NEXTEST_FILTER` alongside the other credential-free `llms` binaries |

## text-splitter

Upstream [benbrandt/text-splitter](https://github.com/benbrandt/text-splitter).

| Patch | What breaks if it is lost | Loss | Guard |
|---|---|---|---|
| Tokenizer sizing accounts for special characters (`src/chunk_size/huggingface.rs`, fork commit `b33e4748`) | Chunks are sized without the tokenizer's special tokens, so a chunk can exceed the model's context window at embed time | silent (embedding failure / truncation) | `crates/chunking/src/lib.rs::a_tokenizer_sized_chunk_counts_the_special_tokens_the_model_will_add` for the sizing and `…::a_tokenizer_sized_chunk_fits_the_budget_the_model_will_measure_it_against` for the consequence, both against a `WordPiece` fixture built in the test rather than a downloaded model |

## mistral.rs and text-embeddings-inference

Upstream [EricLBuehler/mistral.rs](https://github.com/EricLBuehler/mistral.rs) and
[huggingface/text-embeddings-inference](https://github.com/huggingface/text-embeddings-inference).

The `mistral.rs` fork's base is `master@2d4ba4f16`, not a release tag, and it
carries 71 commits. Most are Spice-side integration (dependency re-pointing onto
`spiceai/candle`, CUDA and Windows build fixes).

Two rows this table used to carry — assistant messages with `tool_calls` in the
chat template, and `tracing_subscriber.init()` removed from the loaders — are gone
because neither is fork state any longer. Both behaviours are present in
`master@2d4ba4f16` itself, the commit this fork line was cut from. It already
defines `MessageContent` as `Either<String, Vec<IndexMap<String, Value>>>`, which
is the widening the `tool_calls` patch existed to make; and no loader in either
tree installs a global subscriber — the only `tracing_subscriber` call anywhere in
`mistralrs-core` is a `try_init()` behind a `OnceLock` in `utils/debug.rs`, byte
identical to upstream's, and `try_init` cannot displace a subscriber `spiced` has
already installed. The Spice commits that once made those two changes are not
ancestors of that base, so upstream reached the same state by its own route
rather than by taking them. A re-cut cannot lose what the fork does not carry.

| Patch | What breaks if it is lost | Loss | Guard |
|---|---|---|---|
| `mistral.rs`: i-quant MoE `index_select` + in-place row dequant | ~34% slower local MoE inference | silent (perf) | **GAP** |
| `mistral.rs`: candle dependency re-pointed at `spiceai/candle` | Two candle versions in the graph | build | compile-guarded |
| `text-embeddings-inference`: Spice integration + candle re-pointing | Local embedding models fail to load | build | compile-guarded |
| `text-embeddings-inference`: pooling/model-loading fixes | Embeddings differ from the reference implementation | silent (wrong vectors) | **GAP** |

## candle and its kernel crates

Upstream [huggingface/candle](https://github.com/huggingface/candle) plus the
`candle-cublaslt`, `candle-layer-norm`, `candle-rotary` and `candle-index-select-cu`
kernel crates.

The kernel-crate forks are build-only: Windows/MSVC build fixes, `-fPIC`, and
`cudarc`/`candle` version bounds. They carry no Spice behaviour, so a re-cut cannot
lose one silently — it fails to build.

| Patch | What breaks if it is lost | Loss | Guard |
|---|---|---|---|
| `candle`: i-quant MoE kernels and `slice_assign`/`set_dtype` extensions (carried with the mistral.rs squash) | Local MoE inference regresses in speed, or fails to run quantised MoE models | silent (perf) / build | `crates/llms` model tests cover loading; the kernel behaviour is a **GAP** |
| `candle`: A10G CUDA header fix | CUDA builds fail on A10G | build | compile-guarded |
| `candle-index-select-cu`: fallback-only shim | GPU index-select falls back to the slow path silently | silent (perf) | **GAP** |

## spark-connect-rs

Upstream [sjrusso8/spark-connect-rs](https://github.com/sjrusso8/spark-connect-rs),
branch `spiceai-59-patches`: the previous line plus an arrow 59 version bump, which
touches only the workspace manifest, so every row below is carried unchanged.

| Patch | What breaks if it is lost | Loss | Guard |
|---|---|---|---|
| Default to the `http` scheme when `use_ssl` is false (fork PR #3) | A non-TLS Spark Connect endpoint is dialled over TLS and the connection fails | silent (connection failure) | `crates/data_components/src/spark_connect.rs::a_non_tls_spark_endpoint_is_dialled_in_plaintext`, which asserts the HTTP/2 preface arrives at a local plaintext listener through the real `SparkSessionBuilder::remote(…).build()` path, and `…::a_non_tls_connection_string_resolves_to_an_http_endpoint` for the mechanism |
| Edmondo fork changes merged in | Spark Connect features Spice depends on go missing | build | compile-guarded |
| A TLS endpoint is dialled with a TLS configuration — `ClientTlsConfig::new().with_native_roots()` — and `use_ssl` is exposed to ask (fork PR #7) | `Endpoint::connect` attaches no TLS configuration of its own, so without this a `use_ssl=true` endpoint is either dialled in the clear, which the server rejects, or refused by `tonic` for having no TLS configuration. Every dataset on a Databricks endpoint fails to load either way. The counterpart to the `http`-scheme row above: that one decides the scheme, this one supplies the TLS | build (the `use_ssl` accessor) + silent (connection failure) | `crates/data_components/src/spark_connect.rs::a_tls_spark_endpoint_is_dialled_with_a_tls_handshake`, which dials a listener that speaks no TLS and requires the first bytes to be a TLS handshake record. It pins that TLS is configured at all, not *which* root store was chosen — the roots a client trusts are not observable from its `ClientHello` — so `with_native_roots` itself rests on the accessor, which `::a_non_tls_connection_string_resolves_to_an_http_endpoint` calls and the compiler requires |
| `SparkSession::set_token`, so a session's bearer token can be replaced without rebuilding it (fork PR #8) | A rotated Databricks token cannot be applied to a live session, so every session has to be torn down and rebuilt when a token refreshes | build | compile-guarded by `crates/data_components/src/spark_connect.rs`, which calls `session.set_token(Some(token))` on refresh |
| `user_agent` read from the connection string, and used to replace the default rather than extend it (fork PRs #9, #10) | A Spark Connect client identifies itself to the server by user agent, and Databricks meters and attributes traffic by it. Without these the value in a connection string is ignored, or appended to `spark-connect-rs`'s own, so the traffic is attributed to the library. This is live on every production Databricks connection: `DatabricksSparkConnect::new_with_rate_controller` formats `user_agent=` into the connection string, `SparkSessionFactory::from_connection` keeps it in `base_options` (only `token` and `session_id` are dropped), and `render_connection` puts it back for `SparkSessionBuilder::remote` | silent (attribution) | `crates/data_components/src/spark_connect.rs::a_connection_string_user_agent_replaces_the_client_default`, which reads the value back off the `ChannelBuilder` and requires the library default to be *gone* — extending it would fail. Its control asserts that default is what appears when the option is absent, so the replacement assertion cannot be met by a builder carrying no user agent. Read through `Debug` because the value's only other appearance is the `client_type` field of an outgoing request, which needs a gRPC server to observe |

## delta-kernel-rs

Upstream [delta-io/delta-kernel-rs](https://github.com/delta-io/delta-kernel-rs),
branch `spiceai-0.27`.

**No Spice patches.** Both patches that existed on the earlier 0.18.x fork line —
timestamp-column file skipping, and `ParquetObjectReader` Azure suffix-range handling
— landed upstream and are still present in v0.27.1 (the Azure handling moved, with the
default engine, into the `delta_kernel_default_engine` crate). The pin is upstream
v0.27.1 plus the fork's own `SPICE_PATCHES.md`, a best-effort Codecov upload in its CI
workflow, and two lint-only upstream commits cherry-picked ahead of v0.28.0
(delta-io/delta-kernel-rs#3164, #3218), none of which changes behaviour.

Re-confirm this at the next bump rather than assuming it: if a Spice patch becomes
necessary again, it needs a row here and a guard.

## Dependency-only forks

These forks exist to move a dependency version, not to change behaviour. A lost
patch is a build failure, so no behaviour guard applies.

| Fork | Why it is forked |
|---|---|
| `reqwest-eventsource` | `reqwest` 0.13 bound |
| `tiberius` | `rustls` 0.23 upgrade, and feature-gating so the TLS modules compile with TLS off |
| `tokio-rusqlite` | `rusqlite` 0.40 bound |
| `candle-cublaslt`, `candle-layer-norm`, `candle-rotary` | Windows/MSVC CUDA build fixes and `cudarc`/`candle` version bounds |

---

## Open gaps

**27 rows above are marked GAP** — they have no repo-side guard. Every one of them
is accounted for below; `scripts/check_fork_patches.py` fails if that count and this
sentence disagree, so the list cannot quietly fall behind the tables.

They are not equal in consequence; this is the order to close them in.

**Wrong data or wrong text, silently.** These change what a user gets back:

1. `datafusion-ballista` physical uncorrelated scalar subqueries disabled under
   distributed stage splitting (fork PR #57, porting
   apache/datafusion-ballista#1909) — its sibling half, the per-task file-scan
   restriction from the same fork PR, is now guarded
   (`crates/runtime/tests/cluster/ballista_partition_scoped_scan.rs`). This half
   needs a distributed plan whose stage splitting isolates an uncorrelated
   scalar subquery from its parent; the fork observed the failure via TPC-H, not
   a minimal repro built here.
2. `text-embeddings-inference` pooling and model-loading fixes — embeddings
   differ from the reference implementation.

**Hangs, crashes and failures.** These take a query or the process down:

3. `datafusion-ballista` scheduler lock hygiene (fork PR #60) and shuffle-fetch
   resilience (fork PRs #36, #61–#63) — #36 is the quiet half, a `FetchFailed`
   that reaches the scheduler buried in `Shared(Arc(ArrowError(ExternalError(…))))`
   and is read as a non-retryable execution error, so the recovery that reruns
   the offending map stage never runs.
4. `datafusion-ballista` cluster reliability, six rows across fork PRs #54, #57
   and #59: a missing partition file read as an empty partition; shuffle-fetch
   clients pooled per peer; the reconciliation sweep that revives a lost stage or
   finishes a job whose graph already succeeded; job-graph persistence moved off
   the event loop and awaited; task statuses re-delivered after a failed
   `poll_work`; and the terminal job status persisted before the job leaves the
   active cache. Every one needs a running cluster to exhibit, and three are
   races, so each row records what the fork measured rather than what a test
   here could assert.

**Blocked, not merely undone.** These have been looked at and cannot be closed by
writing a test; each says what would unblock it:

5. `snowflake-rs` (five rows) — no host override, private response types. Needs a
   live account, or an upstream change letting the base URL be set.
6. `vortex` session lock re-entry in writer init (fork PR #29) — the deadlock is a
   race, so any test of it is a timing test. Upstream has since removed the code
   path and the lock (vortex-data/vortex#8919); if a run confirms that, drop the row
   rather than guard it.
7. `graph-rs-sdk` tower middleware — nothing here configures Graph middleware, so
   there is no behaviour of ours to assert on.

**Reported late rather than wrongly.** The right answer, after an avoidable wait:

8. `async-openai` a 404 ends the retry loop instead of being retried — a mistyped
   `endpoint` is retried through the whole backoff budget before reporting, so a
   configuration error looks like a slow provider. What changes is *when* the
   error arrives, and asserting that is a timing test.

**Diagnostics only.** The query is unaffected; what is lost is the ability to see
how it ran:

9. `datafusion-ballista` distributed `EXPLAIN ANALYZE` (fork PR #34, now
   upstream's; `EXPLAIN FORMAT TREE` is not carried) — plain `EXPLAIN` is guarded
   through the cluster harness, and `ANALYZE` is not issued through it by anything. Closing this needs the
   harness to submit the statement rather than build its text as a label.

**Performance only.** A lost patch here costs throughput, not correctness. These are
deliberately left to the benchmark suites (`testoperator`, the CH-benCH lab runs and
the scheduled TPC-H/TPC-DS jobs), which already trend these numbers over time and
will show the regression as a step change. A unit test cannot assert a speedup
without becoming a flaky timing test:

10. `vortex` intra-file decode parallelism; `iceberg-rust` parallel file scanning;
    `mistral.rs`/`candle` i-quant MoE kernels; `candle-index-select-cu` fallback shim; `model2vec-rs` fast WordPiece;
    `snowflake-rs` streaming batches (memory, not latency — but see the
    `snowflake-rs` note above: it is blocked with the rest of that fork);
    `async-openai` retry-after handling.

**Unoptimised builds only.** No query answers anything differently; what is lost
is stack headroom in a debug build (release builds were not measured):

11. `datafusion` join-arm helper refactor (`cbda233a6`, spiceai/datafusion#249) —
    it keeps the unparser's opt-level-0 frame from growing with #233 and #234, which
    without it push the fork's `roundtrip_statement` past the 2 MiB default test
    thread (it needs 2,112 KiB without the refactor, 1,984 KiB with it). This repo's
    tests run on 8 MiB threads (`RUST_MIN_STACK` in `.cargo/config.toml`), so a test
    here would not reach that limit, and one sized to the fork's 2 MiB thread would fail
    on any unrelated frame growth instead. At a re-cut, re-run the fork's
    `cargo test -p datafusion-sql --test sql_integration -- roundtrip_statement` at
    the default stack: an overflow there means the refactor was dropped or the frame
    grew again.
