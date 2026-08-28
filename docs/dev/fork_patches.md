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
| [arrow-rs](#arrow-rs) | `18a40370d014a0f8eed12ee0d5a914e8cb2070d8` | `lukim/spiceai-58.3.0` |
| [async-openai](#async-openai) | `6bda5533dd118afcf80aa6f5ef59ad35277627a7` | `spiceai` |
| [candle](#candle-and-its-kernel-crates) | `efbb9a72e92789eafed0806c3e16f14640c504f6` | `lukim/spiceai-0.11.0` |
| [candle-cublaslt](#candle-and-its-kernel-crates) | `c41bf9c6e87195749c2262d16ca320af2bbebbfe` | `main` |
| [candle-index-select-cu](#candle-and-its-kernel-crates) | `75fc0b689b33a327907d36dd479f7d242640ca71` | `master` |
| [candle-layer-norm](#candle-and-its-kernel-crates) | `dfdbfbb953ceeb0366e5e3b69f2933204309d3dd` | `main` |
| [candle-rotary](#candle-and-its-kernel-crates) | `e12f91a6c8beec5373ccec91a5ccad80619cf065` | `main` |
| [clickhouse-rs](#clickhouse-rs) | `7e98394f44cfa33919ebc5a92c06d5bddba708bf` | tag `0.2.2` |
| [datafusion](#datafusion) | `859621d612511efb93a7f3e020f8baae8e33e3b4` | `spiceai-54` |
| [datafusion-ballista](#datafusion-ballista) | `f3b8c4b49d251cb5f1326b69fe4846dc09d36ac0` | `spiceai-54` |
| [datafusion-federation](#datafusion-federation-and-datafusion-table-providers) | `0cb3781608b89f40c6585618ec3071f83345671a` | `spiceai-54` |
| [datafusion-functions-json](#datafusion-functions-json) | `ca9d4c6e5a0de3bfa9fe20a683a9f7d58e36e2cc` | `spiceai-54` |
| [datafusion-table-providers](#datafusion-federation-and-datafusion-table-providers) | `b9ea24c3101a24e8b3186a6a552362cf0a91bc03` | `spiceai-54-expression-aware-function-support` |
| [delta-kernel-rs](#delta-kernel-rs) | `714d64fd5369efc4835109be0fd718db5a3be0aa` | `spiceai-0.23.0` |
| [docx-rs](#docx-rs) | `2a85dce57d0128e2cd7c369545516c347cb8c529` | `spiceai` |
| [duckdb-rs](#duckdb-rs) | `7229b20daf24765c84d294c52cf4b4165ca79073` | `spiceai-1.5.5` |
| [graph-rs-sdk](#graph-rs-sdk) | `af383410a9c86915263fbd1145b8becfc1e317b5` | `spiceai` |
| [iceberg-rust](#iceberg-rust) | `351d1bc7b6ac9a835397e248e9c687f305e947d1` | `spiceai-0.10.1-df-54` |
| [mistral.rs](#mistralrs-and-text-embeddings-inference) | `2d15d171236803481d582a9fbf8a80869bf74d8c` | `spiceai` |
| [model2vec-rs](#model2vec-rs) | `55fef28a3556895b20204634b788f7c836b610bc` | `spiceai` |
| [reqwest-eventsource](#dependency-only-forks) | `eb11e695128ce264bf05e4220ce2311c25992c73` | `spiceai` |
| [rusqlite](#rusqlite-and-tokio-rusqlite) | `e39c9c46dea1f0983cd8d87dabb69b41c9efe1fd` | `master` |
| [sea-query](#sea-query) | `213b6b876068f58159ebdd5852604a021afaebf9` | `spiceai` |
| [snowflake-rs](#snowflake-rs) | `744ffd77fe82171a805562ce001a341a94d52541` | `spiceai-58` |
| [spark-connect-rs](#spark-connect-rs) | `5f7c2452d4202d7496abac0a6f2eaa4bef46a5ad` | `spiceai` |
| [text-embeddings-inference](#mistralrs-and-text-embeddings-inference) | `ac4e457936bc11c9b4fee453f2be33133d3146d8` | `spiceai` |
| [text-splitter](#text-splitter) | `58f9c21006e01e5e968c5de80a0398b3f5ec439a` | `spiceai` |
| [tiberius](#dependency-only-forks) | `9ae93c65222b51b0579945ffce5cba053cb23cca` | `spiceai` |
| [tokio-rusqlite](#rusqlite-and-tokio-rusqlite) | `b10df82e3bbc4f4700562a14a3a00714cbc2f0c7` | `spiceai` |
| [vortex](#vortex) | `ba043de0ab6e214e825932210cc336b7ce5e8309` | `spiceai-54` |

`spiceai/spice-rs` and `spiceai/spicebench` are also pinned as git dependencies but
are not forks — they are Spice repositories with no upstream, so nothing can drop a
patch from them. They are excluded from the guard.

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

| Patch | What breaks if it is lost | Loss | Guard |
|---|---|---|---|
| Arrow `Map` alias (`vortex-arrow`), both halves: the `DType` alias and the map-entry recursion in the session importer | Every write of a `Map` column fails with `Array encoding not implemented for Arrow data type Map(...)`. The table is created happily first, so it surfaces only on flush | silent | `crates/vortex/src/persistent/mod.rs::map_column_roundtrips_through_a_vortex_file`, and `crates/cayenne/src/schema.rs::vortex_encodes_exactly_the_types_not_listed_as_unsupported` for the whole type list |
| Tokio one-shot for the spawned-task result channel (`vortex-io/src/runtime/handle.rs`) | Reentrant waker drop on the cancellation path → `SIGSEGV` under ordinary query cancellation | silent (crash) | `crates/cayenne/tests/vortex_task_cancellation.rs` |
| Tokio one-shot in `vortex-io/src/runtime/single.rs` | The same hazard on the single-runtime path | silent (crash) | as above |
| Tokio one-shot for the segment-read result channel (`vortex-file/src/segments/source.rs`) | The same hazard on `ReadFuture`, polled and then dropped on cancellation | silent (crash) | as above |
| Fixed-offset timezone resolution in timestamp extension types (`vortex-array/src/extension/datetime/timezone.rs`) | Panic `failed to find time zone '+00:00'` reading any `timestamptz` column whose offset is numeric rather than named | silent (panic) | `crates/cayenne/tests/fixed_offset_timezone_test.rs::fixed_offset_timezone_column_survives_a_vortex_file_write` |
| `set_available_parallelism` (`vortex-utils`) | Vortex sizes encode fan-out and scan lookahead from the machine's core count instead of the process's CPU entitlement, so a limited pod over-subscribes ([#12328](https://github.com/spiceai/spiceai/issues/12328)) | silent | `bin/spiced/tests/cpu_budget.rs::spicepod_cores_size_the_runtime_pools` |
| `DECIMAL` → floating-point cast applies the scale (fork PR #51) | Decimal columns read back off by a factor of 10^scale | silent (wrong data) | `crates/vortex/src/persistent/mod.rs::test_decimal_to_float_cast_applies_scale` |
| `UncompressedSizeInBytes` statistic handling | `ColumnStatistics.byte_size` is wrong, so the optimizer mis-sizes joins built over Vortex scans | silent | `crates/vortex/src/persistent/format.rs::propagates_per_column_byte_size` |
| Target file size respected in the sink (fork PR #33) | The writer ignores `target_file_size_mb` and emits one file per flush regardless of size | silent | `crates/vortex/src/persistent/format.rs::format_plumbs_target_file_size_mb` guards the plumbing only; the sink's own honouring of it is a **GAP** |
| `vortex.date` → `vortex.timestamp` extension cast (fork PR #28) | Upstream refuses the cast, so a pushed-down `CAST(date_col AS TIMESTAMP)` fails the scan | silent | `crates/vortex/src/persistent/mod.rs::test_date_to_timestamp_extension_cast` guards the kernel. The patch covers `ExtensionArray` only, and the scan reaches a second cast path that it does not patch — see [Open gaps](#open-gaps) |
| Balanced `list_contains` OR tree for large `IN` lists (fork PR #37) | A large `IN (...)` filter builds a right-leaning OR tree; deep enough and the plan blows the stack during pushdown conversion | silent (crash) | `crates/vortex/src/persistent/mod.rs::test_large_in_list_filter_pushdown_stays_evaluable` |
| Avoid session lock re-entry in writer init (fork PR #29) | Deadlock in `vortex-file` writer initialisation — the write never completes and the refresh hangs | silent (hang) | **GAP** |
| Unsupported pushdown node bubbles `TRUE` rather than erroring; empty `IN` list handled (fork PR #8) | A predicate Vortex cannot convert fails the scan instead of degrading to "keep the row" | silent | vendored: the pushdown conversion now lives in `crates/vortex/src/convert/exprs.rs`, guarded by `test_empty_in_list_conversion_produces_boolean_literal` and the `can_be_pushed_down` unsupported-operand cases |
| Intra-file decode parallelism — sub-split large chunk spans (fork PR #62) | Scan throughput on large chunk spans drops to single-stream decode | silent (perf) | **GAP** — a perf-only row; see [Open gaps](#open-gaps) for why it is deliberately unguarded |

## datafusion

Upstream [apache/datafusion](https://github.com/apache/datafusion), branch
`spiceai-54`. The branch also carries upstream's own `[branch-52]`/`[branch-53]`/
`[branch-54]` backports; those are upstream commits and are not Spice patches.

The unparser rows are the highest-consequence set in this file: every one of them
changes the SQL sent to a federated engine, and every failure mode is *more or fewer
rows than the plan asked for*, with no error.

Every guard naming `crates/data_components/src/federation.rs` needs the crate's
`federation` feature, which is **not** in its defaults:

```sh
cargo test -p data_components --features federation --lib federation::
```

Without it the module is not compiled and the whole file is skipped — the run is
green and reports nothing, which is the same shape as the loss these guards exist to
catch.

| Patch | What breaks if it is lost | Loss | Guard |
|---|---|---|---|
| Unparser: a `fetch` pushed into a join input keeps its own scope (fork PR #197) | The remote engine is asked for the whole table and the join is evaluated over it — more rows than the plan ([#12406](https://github.com/spiceai/spiceai/issues/12406)) | silent (wrong data) | `crates/data_components/src/federation.rs::a_fetch_pushed_into_a_join_input_survives_unparsing` |
| Unparser: a `Filter` above a `Limit` keeps the limit scoped (fork PR #198) | SQL evaluates `WHERE` before `LIMIT`, so the limit selects from filtered rows instead of bounding them ([#12591](https://github.com/spiceai/spiceai/issues/12591)) | silent (wrong data) | `…::a_filter_above_a_limit_keeps_the_limit_scoped` |
| Unparser: `ORDER BY` kept out of a derived table when the sort key is computed (fork PR #191) | The ordering is emitted inside a derived table, which SQL does not require the outer query to preserve — rows come back in any order | silent (wrong order) | `…::a_computed_sort_key_keeps_order_by_at_the_top_level` |
| Unparser: a stacked aggregate is unparsed as a derived table (fork PR #192) | An aggregate over an aggregate is flattened into one query, changing the grouping | silent (wrong data) | `…::a_stacked_aggregate_keeps_its_inner_group_by`, `…::a_grouped_stacked_aggregate_binds_its_outer_clauses_through_the_derived_scope` |
| Unparser: a bounded `EXISTS` build side is scoped, so its limit selects rows (fork PR #201) | The limit binds to the correlated subquery rather than the build side, so the `EXISTS` matches rows it should not | silent (wrong data) | `…::a_bounded_exists_build_side_is_scoped_outside_the_correlation`, `…::an_offset_only_exists_build_side_is_scoped_outside_the_correlation`, `…::an_unbounded_exists_build_side_is_left_unscoped` |
| Unparser: refuse an `EXISTS` bound that cannot be scoped, rather than emit wrong rows (fork PR #205) | The unparser silently emits SQL that returns wrong rows for the shapes it cannot scope ([#13277](https://github.com/spiceai/spiceai/issues/13277)) | silent (wrong data) | `…::a_bounded_exists_refuses_a_correlation_naming_two_build_inputs`, `…::a_bounded_exists_refuses_a_correlation_qualified_by_the_probe` |
| Unparser: name a derived table's unnamed outputs (fork PR #206) | A derived table with an unnamed output column produces SQL the remote engine rejects, or binds the wrong column ([#12751](https://github.com/spiceai/spiceai/issues/12751)) | silent (wrong data / query failure) | `…::a_derived_tables_unnamed_outputs_are_named` |
| Unparser: empty `Projection` emits `SELECT 1` | A projection with no expressions unparses to `SELECT FROM …`, which is not valid SQL, so the federated query fails outright | silent (query failure) | `…::an_empty_projection_does_not_unparse_to_an_empty_select_list` |
| Unparser: `AT TIME ZONE` faithfully unparsed (fork PR #160), and suppressed for fixed-offset timezones on DuckDB (fork PR #195) | The timezone is dropped from the SQL, so the remote engine evaluates the expression in its own session timezone | silent (wrong data) | `…::a_timezone_survives_unparsing_except_where_the_engine_cannot_resolve_it` |
| BigQuery dialect: `FLOAT64` not `DOUBLE`, timestamp literal format, `date_field_extract_style` / `interval_style` overrides, `date_trunc` support, no column alias inside a table alias (fork PRs #144, #146, #147, #148, #169) | Federated BigQuery queries are rejected by BigQuery, or silently coerce types | silent (query failure) | `…::bigquery_names_the_float_type_the_way_bigquery_does` covers the type spelling; the remaining four are a **GAP** |
| `supports_subquery_in_join_predicate` dialect flag (fork PR #151) | A subquery is emitted inside a `JOIN … ON`, which several engines reject | build (flag) + silent (behaviour) | **GAP** |
| Metadata columns (`_location`, `_last_modified`, `_size`) on `ListingOptions`/`FileScanConfig`, and their projection, pushdown and statistics handling | Datasets that select file metadata columns lose them, or project the wrong column | build | `crates/data-connector-api/src/listing/connector.rs` (metadata-column tests) |
| Object-version pinning on `ListingOptions` (`with_object_versioning_type`) | A scan stops pinning the object version, so a file replaced mid-scan is read half-old and half-new | build (API) + silent (behaviour) | `crates/data-connector-api/src/listing/connector.rs::a_versioned_parquet_read_pins_every_request_to_one_object_version` |
| Placeholder type inference (`Expr::infer_placeholder_types`, incl. `CASE`, `LIMIT`/`OFFSET` `Int64`, name/metadata preservation) (fork PRs #87, #88, #89) | A parameterised query fails to plan, or infers the wrong type for `$1` | silent (query failure) | **GAP** |
| Eager-aggregation physical optimizer rule (`datafusion/physical-optimizer/src/eager_aggregation.rs`, ~3000 lines, Spice-only) | Aggregations stop being pushed below joins — a large planned regression, not a correctness one | silent (perf) | **GAP** |
| Pluggable `CollectLeftAccumulator` seam on `HashJoinExec` | Cayenne's custom left-side accumulator cannot be installed | build | compile-guarded by `crates/cayenne` |

## arrow-rs

Upstream [apache/arrow-rs](https://github.com/apache/arrow-rs), branch
`lukim/spiceai-58.3.0`. The whole patch is one file,
`parquet/src/arrow/async_reader/store.rs`.

| Patch | What breaks if it is lost | Loss | Guard |
|---|---|---|---|
| `ParquetObjectReader::new_with_meta` — take `ObjectMeta` so the file size is known up front | The reader falls back to suffix range requests, which Azure Blob Storage does not support: Parquet reads over ABFS fail or take an extra round trip per file | build (constructor) | `crates/runtime/tests/abfs/mod.rs::test_azure_parquet_reading_with_object_meta` (needs Azurite) |
| `with_object_versioning_type` — attach `if_match`/`version` to every metadata, byte-range and suffix fetch | The reader stops pinning the object version. A file replaced between the metadata read and the data reads is read as a mixture of both — the footer of one file, the pages of another | build (API) + silent (behaviour) | `crates/data-connector-api/src/listing/connector.rs::a_versioned_parquet_read_pins_every_request_to_one_object_version` |
| `get_byte_ranges` override — coalesce ranges through `get_opts` rather than `ObjectStore::get_ranges` | Version pinning is dropped for the data reads specifically (the metadata read keeps it), and range coalescing is lost, so a scan issues one request per column chunk | silent | as above |

## datafusion-ballista

Upstream [apache/datafusion-ballista](https://github.com/apache/datafusion-ballista),
branch `spiceai-54`. The fork carries its own inventory, `SPICE_FORK_CHANGES.md`.
This fork is heavily Spice-modified — distributed execution is largely our code —
so the rows below name the contracts, not every commit.

| Patch | What breaks if it is lost | Loss | Guard |
|---|---|---|---|
| Cluster RPC TLS and API-key auth (fork PR #3) | Scheduler/executor traffic falls back to plaintext and unauthenticated | silent (security) | `crates/runtime/tests/tls/mod.rs` (two guards) |
| Object-store shuffle storage (S3/Azure), `PrefixStore` wrapping, single-stream IPC per partition (fork PRs #9, #18, #40–#43) | Shuffles fall back to local disk, or S3 shuffle paths resolve to the wrong key | silent | `crates/runtime/tests/cluster/distributed_acceleration.rs` and the rest of `crates/runtime/tests/cluster/` |
| In-memory shuffle storage with remote-fetch fallback (fork PRs #7, #8) | Every shuffle round-trips through storage | silent (perf) | `crates/runtime/tests/cluster/in_memory_shuffle.rs` |
| Shuffle-fetch resilience: retry on a fresh connection, h2 receive-window sizing, bounded read inactivity, unordered stream consumption (fork PRs #61, #62, #63) | A transient fetch failure fails the whole query; large shuffles stall | silent | **GAP** |
| Scheduler lock hygiene across persists and awaits (fork PR #60) | Cluster wedge / runtime freeze under load | silent (hang) | **GAP** |
| Don't swap null-aware anti joins in `JoinSelection` (fork PR #58) | A distributed anti-join returns wrong rows | silent (wrong data) | **GAP** |
| Vortex columnar shuffle format (fork PR #7) | Shuffles fall back to Arrow IPC | build | compile-guarded |
| Stuck-query detection and stale `TaskStatus` rejection (fork PRs #39, #53) | A reset partition's stale status is accepted, corrupting the execution graph | silent | **GAP** |

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

`datafusion-table-providers` is pinned to a change branch rather than `spiceai-54`
because the branch head carries a second patch this workspace has not audited. Fold
the pin back onto `spiceai-54` at the next bump, which is where that patch gets its
own audit.

One patch here does have a repo-side guard, because losing it is a wrong answer
rather than a build failure:

| Patch | What breaks if it is lost | Loss | Guard |
|---|---|---|---|
| `FunctionSupport` per-call check (fork PR #61) | A function a backend carves out of the deny-list because its dialect rewrites it federates in *every* call shape, including the ones the dialect cannot render. The unparser then emits the function verbatim into the remote SQL — the unknown-function failure of [#10703](https://github.com/spiceai/spiceai/issues/10703) | build, then silent | `crates/data-connectors/connector-adbc/src/lib.rs::function_support_tests::bigquery_refuses_the_json_call_shapes_its_dialect_cannot_translate` and `::an_untranslatable_predicate_is_left_above_the_federated_scan`. Losing the API fails `cargo check`; a re-cut that keeps `with_scalar_call_support` and drops its use in `contains_unsupported_functions` fails these instead |

## datafusion-functions-json

Upstream
[datafusion-contrib/datafusion-functions-json](https://github.com/datafusion-contrib/datafusion-functions-json),
branch `spiceai-54`.

**No Spice patches.** The branch is upstream `main` unmodified. It is pinned rather
than taken from crates.io because three correctness fixes landed upstream after
`v0.54.2` and have not been published; the pin exists only to carry them, and should
be dropped for a plain version requirement as soon as a release contains them.

Every one of the three returns a wrong answer rather than an error, so the loss mode
if the pin is dropped early is silent. All three guards live in one file, and the
`json_get_int`/`json_get_float` rows cover the sign and type matrix rather than only
the case named:

| Patch | What breaks if it is lost | Loss | Guard |
|---|---|---|---|
| `json_get_int` / `json_get_float` read negative numbers (upstream PR #125) | Every negative JSON number reads as NULL — `json_get_int('{"a": -1}', 'a')` is NULL, not `-1`. No error, no warning | silent (wrong data) | `crates/runtime-udfs-api/tests/json_semantics.rs::json_get_int_reads_negative_numbers`, `::json_get_float_reads_negative_numbers` |
| Integers outside jiter's `i64` fast path (upstream PR #124) | `json_get` panics on an in-range integer jiter hands back as a big integer, taking the query down; `json_get_int` reads the same value as NULL | silent (panic, and wrong data) | `crates/runtime-udfs-api/tests/json_semantics.rs::json_get_reads_an_integer_outside_the_fast_path_without_panicking`, `::json_get_int_spans_the_whole_i64_range` |
| Nested `json_as_text` is not flattened (upstream PR #121) | `json_as_text(json_as_text(x, 'a'), 'b')` folds into one two-element path, which reads the wrong value whenever the inner result is itself a JSON document | silent (wrong data) | `crates/runtime-udfs-api/tests/json_semantics.rs::a_json_string_holding_json_is_read_one_level_at_a_time` |

## duckdb-rs

Upstream [duckdb/duckdb-rs](https://github.com/duckdb/duckdb-rs), branch
`spiceai-1.5.5`.

| Patch | What breaks if it is lost | Loss | Guard |
|---|---|---|---|
| `duckdb_arrow_scan` support — `register_arrow_scan_view` for Arrow-stream ingestion (fork PR #18) | DuckLake writes and Arrow-stream ingestion have no path in | build | `crates/data_components/src/ducklake/writer.rs` calls it |
| ICU extension statically linked into bundled DuckDB (fork PR #23) | Any query using a named timezone (`AT TIME ZONE 'America/New_York'`) fails at runtime, and DuckDB tries to download the extension from the network | silent (query failure) | `crates/accelerators/accelerator-duckdb/src/lib.rs::bundled_duckdb_resolves_a_named_time_zone_without_installing_icu` |
| VSS (HNSW) extension statically linked (fork PR #37) | Vector search over a DuckDB accelerator fails, or silently falls back to a full scan | silent (query failure) | `crates/accelerators/accelerator-duckdb/src/lib.rs::bundled_duckdb_builds_an_hnsw_index_without_installing_vss` |
| Bundled DuckDB version pinned to the release (fork PR #38) | Extension downloads resolve against a mismatched DuckDB version and fail | silent | covered by the two extension guards above |

## iceberg-rust

Upstream [apache/iceberg-rust](https://github.com/apache/iceberg-rust), branch
`spiceai-0.10.1-df-54`.

| Patch | What breaks if it is lost | Loss | Guard |
|---|---|---|---|
| `RowDeltaAction` for row-level deletes via delete files (fork PR #28) | `DELETE` against an Iceberg table has no commit path | build | `crates/data_components/src/iceberg/delete.rs` calls `tx.row_delta()` |
| SigV4 signing middleware for REST catalogs on AWS Glue | Glue-backed Iceberg catalogs fail to authenticate | build (module) + silent (signing) | `crates/runtime/src/catalogconnector/iceberg.rs` wires `rest.sigv4-enabled`; end-to-end signing is a **GAP** |
| Limit push-down for `IcebergTableProvider` (fork PR #19) | `SELECT … LIMIT n` scans the whole table | silent (perf) | partial: `crates/runtime/src/cluster/datafusion/codec/spice_physical_codec.rs` refuses to serialise a scan whose limit it cannot carry, so the distributed path cannot silently drop it. That the single-node scan *applies* the limit is a **GAP** |
| Pinned snapshot reads in `IcebergTableProvider` (fork PR #45) | A scan reads the current snapshot instead of the pinned one — time-travel and repeatable reads silently return live data | silent (wrong data) | **GAP** |
| Parallel file scanning with eager task bucketing (fork PR #43) | Iceberg scans lose file-level parallelism | silent (perf) | **GAP** |
| `IcebergTableProvider::try_new` made public; extended file metadata | No construction path from Spice | build | compile-guarded |

## async-openai

Upstream [64bit/async-openai](https://github.com/64bit/async-openai).

| Patch | What breaks if it is lost | Loss | Guard |
|---|---|---|---|
| `reasoning_content` on `ChatCompletionResponseMessage` | Reasoning models' output is dropped from responses | build | every provider in `crates/llms` constructs the field |
| Azure Entra token auth in `config.rs` | Azure OpenAI with Entra credentials cannot authenticate | build | compile-guarded |
| `post`/`post_stream` and the GET operation made public | Non-OpenAI providers built on the same client lose their entry point | build | compile-guarded |
| Don't serialize nulls; hide `usage` when null | Requests carry explicit `null`s that some OpenAI-compatible servers reject | silent (request failure) | **GAP** |
| `Eq`/`Hash` on `EmbeddingInput` and `CreateEmbeddingRequest` | Embedding request caching cannot key on the request | build | compile-guarded |
| Aggregated rate-limit retry logging; `retry-after` honoured from the response header (fork PRs #37, #38) | One `WARN` per retried request instead of one per burst; retries ignore the server's back-off hint | silent (log noise, throughput) | **GAP** |

## clickhouse-rs

Upstream [gengteng/clickhouse-rs](https://github.com/gengteng/clickhouse-rs), pinned
by tag `0.2.2`.

| Patch | What breaks if it is lost | Loss | Guard |
|---|---|---|---|
| `Date32` support — `DateConverter for i32`, `Value`/`ValueRef::Date32`, `FromSql for NaiveDate` | ClickHouse `Date32` columns (dates outside 1970–2149) fail to decode | build (variant) + silent (range) | **GAP** — `crates/data-connectors/connector-clickhouse/src/block_to_arrow.rs` covers `Date` only |
| `ConnectionError::NoPacketReceived` | A dropped connection surfaces as a less specific error | build | compile-guarded |

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

## snowflake-rs

Upstream [andrusha/snowflake-rs](https://github.com/andrusha/snowflake-rs), branch
`spiceai-58`.

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
| Tower service setup moved to `RequestHandler` (upstream PR #494) | Middleware (retry, tracing) is not applied to Graph requests | silent | **GAP** |

## docx-rs

Upstream [bokuweb/docx-rs](https://github.com/bokuweb/docx-rs).

| Patch | What breaks if it is lost | Loss | Guard |
|---|---|---|---|
| `Render` trait for `Document`/`DocumentChild`, including paragraph newlines and table rendering | `.docx` documents cannot be turned into text — the document parser has no extraction path | build (trait) | `crates/document_parse/src/docx.rs` imports `docx_rs::Render` |
| Paragraph and table newline placement | Extracted text runs together, changing chunk boundaries and therefore embeddings | silent (wrong text) | **GAP** |

## model2vec-rs

Upstream [MinishLab/model2vec-rs](https://github.com/MinishLab/model2vec-rs).

| Patch | What breaks if it is lost | Loss | Guard |
|---|---|---|---|
| IDs-only fast WordPiece tokenizer for the potion models | Static embedding throughput drops sharply | silent (perf) | **GAP** |
| `config.json` made optional for sentence-transformers compatibility | Loading a sentence-transformers static model fails | silent (load failure) | **GAP** |
| HF cache directory read from the environment | Models are re-downloaded instead of reusing the shared cache | silent | **GAP** |

## text-splitter

Upstream [benbrandt/text-splitter](https://github.com/benbrandt/text-splitter).

| Patch | What breaks if it is lost | Loss | Guard |
|---|---|---|---|
| Tokenizer sizing accounts for special characters (`src/chunk_size/huggingface.rs`) | Chunks are sized without the tokenizer's special tokens, so a chunk can exceed the model's context window at embed time | silent (embedding failure / truncation) | **GAP** — `crates/chunking` tests cover the splitter, not the sizing |

## mistral.rs and text-embeddings-inference

Upstream [EricLBuehler/mistral.rs](https://github.com/EricLBuehler/mistral.rs) and
[huggingface/text-embeddings-inference](https://github.com/huggingface/text-embeddings-inference).

The `mistral.rs` fork's base is `master@2d4ba4f16`, not a release tag, and it carries
71 commits. Most are Spice-side integration (dependency re-pointing onto
`spiceai/candle`, logging removal so the loader does not install a global
subscriber).

| Patch | What breaks if it is lost | Loss | Guard |
|---|---|---|---|
| `mistral.rs`: i-quant MoE `index_select` + in-place row dequant | ~34% slower local MoE inference | silent (perf) | **GAP** |
| `mistral.rs`: assistant messages with `tool_calls` handled in the chat template | Tool-calling conversations render wrong prompts, so the model loses tool context | silent (wrong output) | **GAP** |
| `mistral.rs`: `tracing_subscriber.init()` removed from the loaders | The loader installs a global subscriber and hijacks `spiced`'s logging | silent (logging) | **GAP** |
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

Upstream [sjrusso8/spark-connect-rs](https://github.com/sjrusso8/spark-connect-rs).

| Patch | What breaks if it is lost | Loss | Guard |
|---|---|---|---|
| Default to the `http` scheme when `use_ssl` is false | A non-TLS Spark Connect endpoint is dialled over TLS and the connection fails | silent (connection failure) | **GAP** |
| Edmondo fork changes merged in | Spark Connect features Spice depends on go missing | build | compile-guarded |

## delta-kernel-rs

Upstream [delta-io/delta-kernel-rs](https://github.com/delta-io/delta-kernel-rs),
branch `spiceai-0.23.0`.

**No Spice patches.** Both patches that existed on the earlier 0.18.x fork line —
timestamp-column file skipping, and `ParquetObjectReader` Azure suffix-range handling
— landed upstream and are present unmodified in v0.23.0. The pin is byte-identical to
upstream v0.23.0 plus the fork's own `SPICE_PATCHES.md`.

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

**36 rows above are marked GAP** — they have no repo-side guard. Every one of them
is accounted for below; `scripts/check_fork_patches.py` fails if that count and this
sentence disagree, so the list cannot quietly fall behind the tables.

They are not equal in consequence; this is the order to close them in.

**Wrong data or wrong text, silently.** These change what a user gets back:

1. `datafusion` BigQuery dialect: timestamp literal format, `date_field_extract_style`
   / `interval_style`, `date_trunc`, table-alias column aliases (fork PRs #144, #146,
   #148, #169).
2. `datafusion` `supports_subquery_in_join_predicate` (fork PR #151).
3. `datafusion` placeholder type inference (fork PRs #87, #88, #89).
4. `iceberg-rust` pinned snapshot reads (fork PR #45) — time travel silently reads live data.
5. `datafusion-ballista` null-aware anti-join swap (fork PR #58).
6. `datafusion-ballista` stuck-query detection and stale `TaskStatus` rejection (fork
   PRs #39, #53) — a reset partition's stale status corrupts the execution graph.
7. `snowflake-rs` chunked JSON responses and record-batch ordering.
8. `clickhouse-rs` `Date32` range.
9. `text-splitter` special-character sizing, and `docx-rs` newline placement — both
   change the text that gets embedded.
10. `mistral.rs` `tool_calls` chat-template handling.
11. `text-embeddings-inference` pooling and model-loading fixes — embeddings differ
    from the reference implementation.

**Hangs, crashes and failures.** These take a query or the process down:

12. `vortex` session lock re-entry in writer init (fork PR #29).
13. `datafusion-ballista` scheduler lock hygiene (fork PR #60) and shuffle-fetch
    resilience (fork PRs #61–#63).
14. `async-openai` null-suppression in requests.
15. `spark-connect-rs` `http` scheme when `use_ssl` is false.
16. `model2vec-rs` optional `config.json`.
17. `snowflake-rs` async query response support — long-running queries time out.

**Wrong shape, but bounded.** Neither wrong rows nor an outage; a knob that stops
being honoured:

18. `vortex` target file size in the sink (fork PR #33) — the plumbing is guarded,
    the sink's own honouring of `target_file_size_mb` is not, so the writer can emit
    one file per flush regardless of size.
19. `iceberg-rust` single-node limit application (fork PR #19) — the distributed path
    cannot silently drop the limit, the single-node scan can.
20. `snowflake-rs` invalid warehouse/account errors surfaced correctly — a
    misconfigured warehouse produces an opaque error instead of an actionable one.
21. `model2vec-rs` HF cache directory read from the environment — models are
    re-downloaded instead of reusing the shared cache.
22. `mistral.rs` `tracing_subscriber.init()` removed from the loaders — the loader
    installs a global subscriber and hijacks `spiced`'s logging.

**Security posture.** No correctness effect, but a silent downgrade:

23. `iceberg-rust` end-to-end SigV4 signing against a Glue REST catalog.
24. `graph-rs-sdk` tower middleware application.

**Performance only.** A lost patch here costs throughput, not correctness. These are
deliberately left to the benchmark suites (`testoperator`, the CH-benCH lab runs and
the scheduled TPC-H/TPC-DS jobs), which already trend these numbers over time and
will show the regression as a step change. A unit test cannot assert a speedup
without becoming a flaky timing test:

25. `vortex` intra-file decode parallelism; `iceberg-rust` parallel file scanning;
    `datafusion` eager aggregation; `mistral.rs`/`candle` i-quant MoE kernels;
    `candle-index-select-cu` fallback shim; `model2vec-rs` fast WordPiece;
    `snowflake-rs` streaming batches (memory, not latency — worth a guard if a
    cheap one exists); `async-openai` retry-after handling.

## A patch that is present but incomplete

Found while writing the guard for it, so it is a defect rather than a coverage gap,
and it is not one of the 36 above.

`vortex.date` → `vortex.timestamp` (fork PR #28) registers its cast kernel on
`ExtensionArray`. A scan also evaluates a pushed-down predicate against *constant*
arrays built from chunk statistics, and that path resolves a different kernel, which
the fork does not patch. A `SELECT … WHERE CAST(date_col AS TIMESTAMP) > TIMESTAMP '…'`
over a Vortex file therefore fails the scan outright with:

```
No CastReduce to cast constant array from vortex.date[days](i32?) to vortex.timestamp[ns](i64?)
```

Reproduced on the pin above against a four-row table. The kernel guard passes, so
this is not caught by the row it belongs to and needs a fix on the fork, not a test
here.
