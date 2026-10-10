/*
Copyright 2026 The Spice.ai OSS Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

//! Correctness checks for secondary indexes, run on the REAL scan path: two
//! identical Cayenne tables over identical data, one declaring `indexes` and one
//! without, so the same process compares an indexed scan against an ordinary
//! one.
//!
//! The probe counters are the proof that the indexed arm really used file/row
//! selection: an index that silently fell back would return identical rows and
//! prove nothing.

#![allow(clippy::expect_used, clippy::unwrap_used)]

mod common;

use common::lookup_index::{
    SplitMix64, TableSpec, counters, explain_total, insert, open_table, overwrite, poll_until,
    query, rendered,
};

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{Array, Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use cayenne::metadata::{CreateTableOptions, DeletionMode, VortexConfig};
use cayenne::provider::CayenneContext;
use cayenne::{CayenneTableProvider, CayenneTableProviderBuilder, MetadataCatalog};

use datafusion::datasource::TableProvider;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::prelude::{SessionConfig, SessionContext};

/// The indexed table; every probe goes through the index.
const INDEXED: &str = "svc_indexed";
/// The identical control table, built without index keys, so it always scans
/// normally.
const PLAIN: &str = "svc_plain";
/// The plan-evidence test uses its own pair of tables.
const INDEXED_EVIDENCE: &str = "svc_indexed_evidence";
const PLAIN_EVIDENCE: &str = "svc_plain_evidence";
const INDEXED_COUNT: &str = "svc_indexed_count";
/// The write-time build test owns its own table.
const INDEXED_WRITE_TIME: &str = "svc_indexed_write_time";
/// The `IN`-list test uses its own pair of tables.
const INDEXED_IN: &str = "svc_indexed_in";
const PLAIN_IN: &str = "svc_plain_in";
/// The collision test's indexed table and its control.
const INDEXED_COLLIDING: &str = "svc_indexed_colliding";
const PLAIN_COLLIDING: &str = "svc_plain_colliding";

/// The indexed tables' `indexes`, one column set per entry.
const INDEX_KEYS: [&[&str]; 3] = [
    &["TenantId", "ServiceId"],
    &["TenantId", "PoolId"],
    &["AutoId"],
];

const ROWS: usize = 40_000;
/// Accounts and pools are low-cardinality, so `(TenantId, PoolId)` is
/// genuinely non-unique — the index must keep every candidate position.
const ACCOUNTS: i64 = 500;
const POOLS: i64 = 300;

/// A key planted with three rows, the first two inactive, so `LIMIT 1` with
/// `\"Active\" = 1` cannot be satisfied by taking the first indexed candidate.
const DUP_ACCOUNT: &str = "ACdddddddddddddddddddddddddddddddd";
const DUP_APPLICATION: &str = "MGdddddddddddddddddddddddddddddddd";
/// A key whose only rows are inactive: a valid non-empty index posting list
/// whose residual predicate removes everything.
const INACTIVE_ACCOUNT: &str = "ACiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii";
const INACTIVE_APPLICATION: &str = "MGiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii";
/// A key present in no row at all.
const MISSING_ACCOUNT: &str = "ACzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz";
const MISSING_APPLICATION: &str = "MGzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz";

fn service_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("AutoId", DataType::Int64, false),
        Field::new("TenantId", DataType::Utf8, false),
        Field::new("ServiceId", DataType::Utf8, false),
        Field::new("PoolId", DataType::Utf8, true),
        Field::new("Active", DataType::Int32, false),
        Field::new("Payload", DataType::Utf8, false),
    ]))
}

/// Deterministic fixture. `offset` shifts `AutoId` and the derived sids so a
/// second call produces rows the first batch never contained.
fn service_rows(offset: i64, rows: usize) -> RecordBatch {
    let mut rng = SplitMix64(0x5EED_100C_2026);
    let mut auto_id = Vec::with_capacity(rows);
    let mut account = Vec::with_capacity(rows);
    let mut application = Vec::with_capacity(rows);
    let mut pool: Vec<Option<String>> = Vec::with_capacity(rows);
    let mut active = Vec::with_capacity(rows);
    let mut payload = Vec::with_capacity(rows);

    for i in 0..rows {
        let id = offset + i64::try_from(i).expect("fits i64");
        auto_id.push(id);
        account.push(format!("AC{:032x}", id % ACCOUNTS));
        application.push(format!("MG{id:032x}"));
        // A NULL key column can never satisfy an equality predicate, so the
        // index must simply not hold those rows.
        pool.push(if i % 10 < 7 {
            Some(format!("NP{:032x}", id % POOLS))
        } else {
            None
        });
        active.push(i32::from(i % 10 != 3));
        payload.push(format!("{:016x}-{:016x}", rng.next_u64(), rng.next_u64()));
    }

    // Three rows on one key; only the LAST is active.
    for (index, is_active) in [0, 0, 1].into_iter().enumerate() {
        auto_id.push(offset + 900_000 + i64::try_from(index).expect("fits i64"));
        account.push(DUP_ACCOUNT.to_string());
        application.push(DUP_APPLICATION.to_string());
        pool.push(Some(format!("NP{:032x}", 777)));
        active.push(is_active);
        payload.push(format!("duplicate-candidate-{index}"));
    }

    // Two rows on one key, neither active.
    for index in 0..2i32 {
        auto_id.push(offset + 950_000 + i64::from(index));
        account.push(INACTIVE_ACCOUNT.to_string());
        application.push(INACTIVE_APPLICATION.to_string());
        pool.push(None);
        active.push(0);
        payload.push(format!("inactive-only-{index}"));
    }

    RecordBatch::try_new(
        service_schema(),
        vec![
            Arc::new(Int64Array::from(auto_id)),
            Arc::new(StringArray::from(account)),
            Arc::new(StringArray::from(application)),
            Arc::new(StringArray::from(pool)),
            Arc::new(Int32Array::from(active)),
            Arc::new(StringArray::from(payload)),
        ],
    )
    .expect("fixture batch")
}

/// A table with no primary key, as a serving view has, in file mode (see
/// `common::lookup_index::file_mode_config`): its small target file size
/// spreads the table over several Vortex files, so the index has candidate
/// FILES to prune, not just rows within one file. A table under test and its
/// control differ only in `index_keys`, which is the whole comparison.
async fn build_table(
    fixture: &common::TestFixture,
    table_name: &str,
    index_keys: &[&[&str]],
    runtime_env: Arc<RuntimeEnv>,
) -> Arc<CayenneTableProvider> {
    open_table(
        fixture,
        runtime_env,
        TableSpec::new(table_name, service_schema(), index_keys),
    )
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn indexed_table_preserves_metadata_only_count() {
    let fixture = common::TestFixture::new(common::BackendType::Sqlite)
        .await
        .expect("fixture");
    let runtime_env = Arc::new(RuntimeEnv::default());
    let indexed = build_table(
        &fixture,
        INDEXED_COUNT,
        &INDEX_KEYS,
        Arc::clone(&runtime_env),
    )
    .await;
    insert(&indexed, INDEXED_COUNT, service_rows(0, ROWS)).await;
    wait_for_index(&indexed, INDEXED_COUNT).await;

    let analyzed = query(
        &indexed,
        INDEXED_COUNT,
        &format!("EXPLAIN ANALYZE SELECT COUNT(*) FROM {INDEXED_COUNT}"),
    )
    .await;
    let plan = arrow::util::pretty::pretty_format_batches(&analyzed)
        .expect("format count plan")
        .to_string();
    assert!(
        plan.contains("PlaceholderRowExec") && !plan.contains("DataSourceExec"),
        "indexed count did not use exact metadata statistics:\n{plan}"
    );
    assert_eq!(
        rendered(
            &query(
                &indexed,
                INDEXED_COUNT,
                &format!("SELECT COUNT(*) FROM {INDEXED_COUNT}"),
            )
            .await
        ),
        vec![(ROWS + 5).to_string()]
    );
}

/// Issues a probe-shaped query until the background build publishes an index.
async fn wait_for_index(provider: &Arc<CayenneTableProvider>, table: &str) {
    let sql = format!(
        "SELECT * FROM {table} WHERE \"TenantId\" = '{DUP_ACCOUNT}' \
         AND \"ServiceId\" = '{DUP_APPLICATION}' AND \"Active\" = 1 LIMIT 1"
    );
    poll_until(
        Duration::from_mins(2),
        Duration::from_millis(250),
        async || {
            let _ = query(provider, table, &sql).await;
            let now = counters(provider);
            if now.access_plans_attached > 0 {
                Ok(())
            } else {
                Err(now)
            }
        },
        |now| format!("point-lookup index was never published: {now:?}"),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lookup_index_matches_the_ordinary_scan() {
    let fixture = common::TestFixture::new(common::BackendType::Sqlite)
        .await
        .expect("fixture");
    let runtime_env = Arc::new(RuntimeEnv::default());

    let indexed = build_table(&fixture, INDEXED, &INDEX_KEYS, Arc::clone(&runtime_env)).await;
    let plain = build_table(&fixture, PLAIN, &[], Arc::clone(&runtime_env)).await;

    let batch = service_rows(0, ROWS);
    insert(&indexed, INDEXED, batch.clone()).await;
    insert(&plain, PLAIN, batch).await;

    wait_for_index(&indexed, INDEXED).await;
    let before = counters(&indexed);

    // --- 20 keys of each shape, compared row-for-row without LIMIT so the
    //     comparison does not depend on which of several matches is returned.
    let mut checked = 0;
    for i in 0..20i64 {
        let id = i * 97;
        let account = format!("AC{:032x}", id % ACCOUNTS);
        let application = format!("MG{id:032x}");
        let sql = format!(
            "SELECT * FROM {{table}} WHERE \"TenantId\" = '{account}' \
             AND \"ServiceId\" = '{application}' AND \"Active\" = 1 ORDER BY \"AutoId\""
        );
        let indexed_rows =
            rendered(&query(&indexed, INDEXED, &sql.replace("{table}", INDEXED)).await);
        let plain_rows = rendered(&query(&plain, PLAIN, &sql.replace("{table}", PLAIN)).await);
        assert_eq!(
            indexed_rows, plain_rows,
            "ServiceId lookup diverged for {account}/{application}"
        );
        checked += 1;
    }

    for i in 0..20i64 {
        let id = i * 131;
        let account = format!("AC{:032x}", id % ACCOUNTS);
        let pool = format!("NP{:032x}", id % POOLS);
        let sql = format!(
            "SELECT * FROM {{table}} WHERE \"TenantId\" = '{account}' \
             AND \"PoolId\" = '{pool}' AND \"Active\" = 1 ORDER BY \"AutoId\""
        );
        let indexed_rows =
            rendered(&query(&indexed, INDEXED, &sql.replace("{table}", INDEXED)).await);
        let plain_rows = rendered(&query(&plain, PLAIN, &sql.replace("{table}", PLAIN)).await);
        assert_eq!(
            indexed_rows, plain_rows,
            "PoolId lookup diverged for {account}/{pool}"
        );
        checked += 1;
    }
    assert_eq!(checked, 40);

    // Both lookup shapes must have gone through file/row selection, not a
    // silent fallback to the ordinary scan.
    let after_sample = counters(&indexed);
    let probes = (after_sample.full + after_sample.partial) - (before.full + before.partial);
    assert_eq!(
        probes, 40,
        "every sampled lookup must reach the index: {before:?} -> {after_sample:?}"
    );
    // A NULL key column is never indexed, so the pool-shape keys the fixture
    // nulls out are fully covered probes that find no row.
    let full = after_sample.full - before.full;
    assert!(
        full >= 30,
        "too few lookups were fully covered: {before:?} -> {after_sample:?}"
    );
    assert!(
        after_sample.access_plans_attached - before.access_plans_attached >= full,
        "row selections were not attached to the Vortex scan: {after_sample:?}"
    );
    // One candidate file per probe on a unique key is the point of the index;
    // allow slack for the non-unique pool shape but not a whole-table fan-out.
    assert!(
        after_sample.candidate_files < after_sample.full * 4,
        "index pruned too few files: {after_sample:?}"
    );
    assert!(
        after_sample.candidate_rows >= after_sample.full,
        "the probes must carry candidate rows: {after_sample:?}"
    );

    // --- An inactive first candidate must not hide the later active match.
    let dup_sql = format!(
        "SELECT \"Payload\" FROM {{table}} WHERE \"TenantId\" = '{DUP_ACCOUNT}' \
         AND \"ServiceId\" = '{DUP_APPLICATION}' AND \"Active\" = 1 LIMIT 1"
    );
    let dup_indexed =
        rendered(&query(&indexed, INDEXED, &dup_sql.replace("{table}", INDEXED)).await);
    let dup_plain = rendered(&query(&plain, PLAIN, &dup_sql.replace("{table}", PLAIN)).await);
    assert_eq!(dup_indexed, vec!["duplicate-candidate-2".to_string()]);
    assert_eq!(dup_indexed, dup_plain);

    // Every duplicate candidate must be retained, active or not.
    let dup_all_sql = format!(
        "SELECT \"Payload\" FROM {{table}} WHERE \"TenantId\" = '{DUP_ACCOUNT}' \
         AND \"ServiceId\" = '{DUP_APPLICATION}' ORDER BY \"AutoId\""
    );
    let dup_all_indexed =
        rendered(&query(&indexed, INDEXED, &dup_all_sql.replace("{table}", INDEXED)).await);
    assert_eq!(dup_all_indexed.len(), 3, "a duplicate posting was dropped");
    assert_eq!(
        dup_all_indexed,
        rendered(&query(&plain, PLAIN, &dup_all_sql.replace("{table}", PLAIN)).await)
    );

    // --- A key whose candidates are all inactive returns nothing on both arms.
    let inactive_sql = format!(
        "SELECT * FROM {{table}} WHERE \"TenantId\" = '{INACTIVE_ACCOUNT}' \
         AND \"ServiceId\" = '{INACTIVE_APPLICATION}' AND \"Active\" = 1 LIMIT 1"
    );
    assert!(
        rendered(&query(&indexed, INDEXED, &inactive_sql.replace("{table}", INDEXED)).await)
            .is_empty()
    );
    assert!(
        rendered(&query(&plain, PLAIN, &inactive_sql.replace("{table}", PLAIN)).await).is_empty()
    );

    // --- A key present in no row.
    let miss_sql = format!(
        "SELECT * FROM {{table}} WHERE \"TenantId\" = '{MISSING_ACCOUNT}' \
         AND \"ServiceId\" = '{MISSING_APPLICATION}' AND \"Active\" = 1 LIMIT 1"
    );
    assert!(
        rendered(&query(&indexed, INDEXED, &miss_sql.replace("{table}", INDEXED)).await).is_empty()
    );
    assert!(rendered(&query(&plain, PLAIN, &miss_sql.replace("{table}", PLAIN)).await).is_empty());
    assert!(
        counters(&indexed).full > 0,
        "a complete index miss should be a fully covered probe"
    );

    // --- A NULL key column is never indexed, and the predicate never matches
    //     it either, so the two arms must still agree.
    let null_key_sql = format!(
        "SELECT * FROM {{table}} WHERE \"TenantId\" = '{INACTIVE_ACCOUNT}' \
         AND \"PoolId\" = 'NP{:032x}' ORDER BY \"AutoId\"",
        0
    );
    assert_eq!(
        rendered(&query(&indexed, INDEXED, &null_key_sql.replace("{table}", INDEXED)).await),
        rendered(&query(&plain, PLAIN, &null_key_sql.replace("{table}", PLAIN)).await)
    );

    // --- Rows written after the index was built stay reachable, and the very
    //     next lookup is still answered by the index. (A write this small is
    //     inlined into the metastore rather than written as a file; the
    //     lifecycle suite covers appends that write files.)
    let appended_before = counters(&indexed);
    let new_batch = service_rows(2_000_000, 256);
    insert(&indexed, INDEXED, new_batch.clone()).await;
    insert(&plain, PLAIN, new_batch).await;

    let new_id = 2_000_000i64 + 7;
    let new_account = format!("AC{:032x}", new_id % ACCOUNTS);
    let new_application = format!("MG{new_id:032x}");
    let new_sql = format!(
        "SELECT * FROM {{table}} WHERE \"TenantId\" = '{new_account}' \
         AND \"ServiceId\" = '{new_application}' ORDER BY \"AutoId\""
    );
    let new_indexed =
        rendered(&query(&indexed, INDEXED, &new_sql.replace("{table}", INDEXED)).await);
    let new_plain = rendered(&query(&plain, PLAIN, &new_sql.replace("{table}", PLAIN)).await);
    assert_eq!(
        new_indexed.len(),
        1,
        "a row written after the index build was lost: {:?}",
        counters(&indexed)
    );
    assert_eq!(new_indexed, new_plain);
    let appended_after = counters(&indexed);
    assert_eq!(
        (
            (appended_after.full + appended_after.partial)
                - (appended_before.full + appended_before.partial),
            appended_after.none - appended_before.none,
        ),
        (1, 0),
        "the lookup right after an append did not use the index: {appended_before:?} -> {appended_after:?}"
    );

    // Every row of the moved snapshot must still be reachable on both arms.
    let total_indexed = rendered(
        &query(
            &indexed,
            INDEXED,
            &format!("SELECT COUNT(*) FROM {INDEXED}"),
        )
        .await,
    );
    let total_plain =
        rendered(&query(&plain, PLAIN, &format!("SELECT COUNT(*) FROM {PLAIN}")).await);
    assert_eq!(total_indexed, total_plain);

    println!("lookup-index counters: {:?}", counters(&indexed));
}

/// Prints the executed plan for one lookup on each arm. The scan-level metrics
/// are the evidence that a selection changed what the engine read, which the
/// probe counters alone cannot show.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lookup_index_plan_evidence() {
    let fixture = common::TestFixture::new(common::BackendType::Sqlite)
        .await
        .expect("fixture");
    let runtime_env = Arc::new(RuntimeEnv::default());
    let indexed = build_table(
        &fixture,
        INDEXED_EVIDENCE,
        &INDEX_KEYS,
        Arc::clone(&runtime_env),
    )
    .await;
    let plain = build_table(&fixture, PLAIN_EVIDENCE, &[], Arc::clone(&runtime_env)).await;

    let batch = service_rows(0, ROWS);
    insert(&indexed, INDEXED_EVIDENCE, batch.clone()).await;
    insert(&plain, PLAIN_EVIDENCE, batch).await;
    wait_for_index(&indexed, INDEXED_EVIDENCE).await;

    let id = 12_345i64;
    let account = format!("AC{:032x}", id % ACCOUNTS);
    let application = format!("MG{id:032x}");
    let sql = format!(
        "EXPLAIN ANALYZE SELECT * FROM {{table}} WHERE \"TenantId\" = '{account}' \
         AND \"ServiceId\" = '{application}' AND \"Active\" = 1 LIMIT 1"
    );

    let analyzed = query(
        &indexed,
        INDEXED_EVIDENCE,
        &sql.replace("{table}", INDEXED_EVIDENCE),
    )
    .await;
    let indexed_text = arrow::util::pretty::pretty_format_batches(&analyzed)
        .expect("format indexed plan")
        .to_string();
    assert!(
        indexed_text.contains("lookup_index=(TenantId, ServiceId)")
            && indexed_text.contains("uncovered_files=0")
            && indexed_text.contains("candidate_files=")
            && indexed_text.contains("candidate_rows="),
        "indexed plan did not expose its lookup decision:\n{indexed_text}"
    );

    let analyzed = query(
        &plain,
        PLAIN_EVIDENCE,
        &sql.replace("{table}", PLAIN_EVIDENCE),
    )
    .await;
    let plain_text = arrow::util::pretty::pretty_format_batches(&analyzed)
        .expect("format plain plan")
        .to_string();
    assert!(
        !plain_text.contains("lookup_index="),
        "an unindexed table claimed an index decision:\n{plain_text}"
    );

    let fallback =
        format!("EXPLAIN SELECT * FROM {INDEXED_EVIDENCE} WHERE \"TenantId\" = '{account}'");
    let fallback = query(&indexed, INDEXED_EVIDENCE, &fallback).await;
    let fallback_text = arrow::util::pretty::pretty_format_batches(&fallback)
        .expect("format fallback plan")
        .to_string();
    assert!(
        fallback_text.contains("lookup_index=none")
            && !fallback_text.contains("lookup_index_outcome")
            && fallback_text.contains("lookup_index_reason=no_key_pinned"),
        "fallback plan did not explain why the index was skipped:\n{fallback_text}"
    );

    println!("=== {INDEXED_EVIDENCE} ===\n{indexed_text}");
    println!("=== {PLAIN_EVIDENCE} ===\n{plain_text}");
    println!("=== fallback ===\n{fallback_text}");
    println!("lookup-index counters: {:?}", counters(&indexed));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn oversized_dynamic_key_sets_fall_back_before_probing() {
    const INDEXED: &str = "bounded_indexed";
    const PLAIN: &str = "bounded_plain";
    let fixture = common::TestFixture::new(common::BackendType::Sqlite)
        .await
        .expect("fixture");
    let runtime_env = Arc::new(RuntimeEnv::default());
    let indexed = build_table(&fixture, INDEXED, &INDEX_KEYS, Arc::clone(&runtime_env)).await;
    let plain = build_table(&fixture, PLAIN, &[], runtime_env).await;
    let batch = service_rows(0, ROWS);
    insert(&indexed, INDEXED, batch.clone()).await;
    insert(&plain, PLAIN, batch).await;
    wait_for_index(&indexed, INDEXED).await;

    let mut config = SessionConfig::new().with_target_partitions(4);
    config
        .options_mut()
        .optimizer
        .hash_join_inlist_pushdown_max_distinct_values = 8_192;
    let ctx = SessionContext::new_with_config(config);
    ctx.register_table(INDEXED, Arc::clone(&indexed) as Arc<dyn TableProvider>)
        .expect("register indexed table");
    ctx.register_table(PLAIN, plain as Arc<dyn TableProvider>)
        .expect("register plain table");
    let schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Int64, true)]));
    let mut values = (0..4_096).map(Some).collect::<Vec<_>>();
    values.extend([Some(7), Some(7), None]);
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(values))],
    )
    .expect("key batch");
    let keys =
        datafusion::datasource::MemTable::try_new(schema, vec![vec![batch]]).expect("key table");
    ctx.register_table("keys", Arc::new(keys))
        .expect("register keys");
    let sql = |table| {
        format!(
            "SELECT s.\"AutoId\" FROM keys k INNER JOIN {table} s \
             ON k.key = s.\"AutoId\" ORDER BY s.\"AutoId\""
        )
    };
    let expected = ctx
        .sql(&sql(PLAIN))
        .await
        .expect("plain join plan")
        .collect()
        .await
        .expect("plain join");
    let before = counters(&indexed);
    let actual = ctx
        .sql(&sql(INDEXED))
        .await
        .expect("indexed join plan")
        .collect()
        .await
        .expect("indexed join");
    let after = counters(&indexed);
    assert_eq!(rendered(&actual), rendered(&expected));
    assert_eq!(rendered(&actual).len(), 4_098);
    assert_eq!(after.full, before.full);
    assert_eq!(after.access_plans_attached, before.access_plans_attached);
    assert_eq!(
        after.runtime_fallback,
        before.runtime_fallback + 1,
        "extraction declines the oversized key set once per filter, before any index probe"
    );
    let explain = ctx
        .sql(&format!("EXPLAIN ANALYZE {}", sql(INDEXED)))
        .await
        .expect("explain plan")
        .collect()
        .await
        .expect("explain execution");
    let analyzed = arrow::util::pretty::pretty_format_batches(&explain)
        .expect("format plan")
        .to_string();
    assert!(analyzed.contains("mode=CollectLeft"));
    assert!(analyzed.contains("DynamicFilter") && analyzed.contains(" IN (SET)"));
    println!("oversized exact runtime filter: 4098 matching rows, {before:?} -> {after:?}");
}

/// A hash join's exact runtime key set is batch-probed against the secondary
/// index after physical planning. The scan-level `lookup_index` remains
/// `none` because no literal existed at `TableProvider::scan` time;
/// the counter delta proves the later dynamic probe selected row positions.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dynamic_filter_batch_probes_the_lookup_index() {
    let fixture = common::TestFixture::new(common::BackendType::Sqlite)
        .await
        .expect("fixture");
    let runtime_env = Arc::new(RuntimeEnv::default());
    let indexed = build_table(
        &fixture,
        INDEXED_EVIDENCE,
        &INDEX_KEYS,
        Arc::clone(&runtime_env),
    )
    .await;
    insert(&indexed, INDEXED_EVIDENCE, service_rows(0, ROWS)).await;
    wait_for_index(&indexed, INDEXED_EVIDENCE).await;

    let key_schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Int64, false)]));
    let key_batch = RecordBatch::try_new(
        Arc::clone(&key_schema),
        vec![Arc::new(Int64Array::from(vec![7, 7, 12_345, 39_999]))],
    )
    .expect("key batch");
    let keys = datafusion::datasource::MemTable::try_new(key_schema, vec![vec![key_batch]])
        .expect("key table");

    let ctx = SessionContext::new();
    ctx.register_table(
        INDEXED_EVIDENCE,
        Arc::clone(&indexed) as Arc<dyn TableProvider>,
    )
    .expect("register indexed table");
    ctx.register_table("keys", Arc::new(keys))
        .expect("register keys");

    let sql = format!(
        "SELECT s.\"AutoId\" FROM keys k INNER JOIN {INDEXED_EVIDENCE} s \
         ON k.key = s.\"AutoId\" ORDER BY s.\"AutoId\""
    );
    let before = counters(&indexed);
    let rows = ctx
        .sql(&sql)
        .await
        .expect("join plan")
        .collect()
        .await
        .expect("join execution");
    assert_eq!(rendered(&rows), vec!["12345", "39999", "7", "7"]);
    let after = counters(&indexed);
    assert_eq!(
        after.full - before.full,
        1,
        "four build rows should be one batched index probe: {before:?} -> {after:?}"
    );
    assert_eq!(
        after.candidate_rows - before.candidate_rows,
        3,
        "duplicate build keys should resolve only three candidate row positions: \
         {before:?} -> {after:?}"
    );
    assert!(
        after.access_plans_attached > before.access_plans_attached,
        "the runtime probe did not reach the Vortex access plan: {before:?} -> {after:?}"
    );

    let explain = ctx
        .sql(&format!("EXPLAIN ANALYZE {sql}"))
        .await
        .expect("explain plan")
        .collect()
        .await
        .expect("explain execution");
    let plan = arrow::util::pretty::pretty_format_batches(&explain)
        .expect("format plan")
        .to_string();
    assert!(
        plan.contains("HashJoinExec") && plan.contains("DynamicFilter"),
        "the evidence query did not use a dynamically filtered hash join:\n{plan}"
    );
    assert!(
        plan.contains("lookup_index=none"),
        "the index was unexpectedly chosen from static scan filters:\n{plan}"
    );

    let composite_schema = Arc::new(Schema::new(vec![
        Field::new("account", DataType::Utf8, false),
        Field::new("service", DataType::Utf8, false),
    ]));
    let composite_batch = RecordBatch::try_new(
        Arc::clone(&composite_schema),
        vec![
            Arc::new(StringArray::from(vec![
                format!("AC{:032x}", 7 % ACCOUNTS),
                format!("AC{:032x}", 12_345 % ACCOUNTS),
                format!("AC{:032x}", 39_999 % ACCOUNTS),
            ])),
            Arc::new(StringArray::from(vec![
                format!("MG{:032x}", 7),
                format!("MG{:032x}", 12_345),
                format!("MG{:032x}", 39_999),
            ])),
        ],
    )
    .expect("composite key batch");
    let composite_keys =
        datafusion::datasource::MemTable::try_new(composite_schema, vec![vec![composite_batch]])
            .expect("composite key table");
    ctx.register_table("composite_keys", Arc::new(composite_keys))
        .expect("register composite keys");
    let composite_sql = format!(
        "SELECT s.\"AutoId\" FROM composite_keys k INNER JOIN {INDEXED_EVIDENCE} s \
         ON k.account = s.\"TenantId\" AND k.service = s.\"ServiceId\" \
         ORDER BY s.\"AutoId\""
    );
    let composite_before = counters(&indexed);
    let composite_rows = ctx
        .sql(&composite_sql)
        .await
        .expect("composite join plan")
        .collect()
        .await
        .expect("composite join execution");
    assert_eq!(rendered(&composite_rows), vec!["12345", "39999", "7"]);
    let composite_after = counters(&indexed);
    assert_eq!(
        composite_after.full - composite_before.full,
        1,
        "the correlated dynamic tuples should be one batched composite index probe: \
         {composite_before:?} -> {composite_after:?}"
    );
    assert_eq!(
        composite_after.candidate_rows - composite_before.candidate_rows,
        3,
        "the composite batch should resolve exactly three row positions: \
         {composite_before:?} -> {composite_after:?}"
    );
    let composite_explain = ctx
        .sql(&format!("EXPLAIN ANALYZE {composite_sql}"))
        .await
        .expect("composite explain plan")
        .collect()
        .await
        .expect("composite explain execution");
    let composite_plan = arrow::util::pretty::pretty_format_batches(&composite_explain)
        .expect("format composite plan")
        .to_string();
    assert!(
        composite_plan.contains("HashJoinExec") && composite_plan.contains("DynamicFilter"),
        "the composite evidence query did not use a dynamically filtered hash join:\n\
         {composite_plan}"
    );

    // Partitioned hash joins publish one CASE branch per hash partition. No
    // branch alone is a complete key set, so the lookup index must decline the
    // runtime filter rather than select only one branch's rows.
    let mut partitioned_config = SessionConfig::new().with_target_partitions(4);
    partitioned_config
        .options_mut()
        .optimizer
        .hash_join_single_partition_threshold = 0;
    partitioned_config
        .options_mut()
        .optimizer
        .hash_join_single_partition_threshold_rows = 0;
    let partitioned_ctx = SessionContext::new_with_config(partitioned_config);
    partitioned_ctx
        .register_table(
            INDEXED_EVIDENCE,
            Arc::clone(&indexed) as Arc<dyn TableProvider>,
        )
        .expect("register indexed table for partitioned join");
    let partitioned_schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Int64, false)]));
    let partitioned_batch = RecordBatch::try_new(
        Arc::clone(&partitioned_schema),
        vec![Arc::new(Int64Array::from_iter_values(0..128))],
    )
    .expect("partitioned key batch");
    let partitioned_keys = datafusion::datasource::MemTable::try_new(
        partitioned_schema,
        vec![vec![partitioned_batch]],
    )
    .expect("partitioned key table");
    partitioned_ctx
        .register_table("partitioned_keys", Arc::new(partitioned_keys))
        .expect("register partitioned keys");
    let partitioned_sql = format!(
        "SELECT s.\"AutoId\" FROM partitioned_keys k INNER JOIN {INDEXED_EVIDENCE} s \
         ON k.key = s.\"AutoId\" ORDER BY s.\"AutoId\""
    );
    let partitioned_explain = partitioned_ctx
        .sql(&format!("EXPLAIN {partitioned_sql}"))
        .await
        .expect("partitioned explain plan")
        .collect()
        .await
        .expect("partitioned explain execution");
    let partitioned_plan = arrow::util::pretty::pretty_format_batches(&partitioned_explain)
        .expect("format partitioned plan")
        .to_string();
    assert!(
        partitioned_plan.contains("HashJoinExec")
            && partitioned_plan.contains("mode=Partitioned")
            && partitioned_plan.contains("DynamicFilter"),
        "the fallback query did not use a partitioned dynamically filtered hash join:\n\
         {partitioned_plan}"
    );
    let partitioned_before = counters(&indexed);
    let partitioned_rows = partitioned_ctx
        .sql(&partitioned_sql)
        .await
        .expect("partitioned join plan")
        .collect()
        .await
        .expect("partitioned join execution");
    let mut expected = (0..128).map(|value| value.to_string()).collect::<Vec<_>>();
    expected.sort();
    assert_eq!(rendered(&partitioned_rows), expected);
    let partitioned_after = counters(&indexed);
    assert_eq!(
        partitioned_after.full, partitioned_before.full,
        "a CASE-partitioned key set must fall back without a partial selection: \
         {partitioned_before:?} -> {partitioned_after:?}"
    );
    assert_eq!(
        partitioned_after.candidate_rows, partitioned_before.candidate_rows,
        "a declined partitioned filter must not record candidate rows"
    );

    // A fully covered index selection sizes the build by candidate rows,
    // rather than by all rows in the files that contain those candidates.
    let mut selective_config = SessionConfig::new().with_target_partitions(4);
    selective_config
        .options_mut()
        .optimizer
        .hash_join_single_partition_threshold = 1024;
    selective_config
        .options_mut()
        .optimizer
        .hash_join_single_partition_threshold_rows = 128;
    let selective_ctx = SessionContext::new_with_config(selective_config);
    selective_ctx
        .register_table(
            INDEXED_EVIDENCE,
            Arc::clone(&indexed) as Arc<dyn TableProvider>,
        )
        .expect("register indexed table for selective join");
    let selective_sql = format!(
        "SELECT s.\"AutoId\" FROM {INDEXED_EVIDENCE} s \
         INNER JOIN {INDEXED_EVIDENCE} b ON s.\"AutoId\" = b.\"AutoId\" \
         WHERE b.\"TenantId\" = 'AC{:032x}' AND b.\"ServiceId\" = 'MG{:032x}'",
        7 % ACCOUNTS,
        7
    );
    let selective_plan = selective_ctx
        .sql(&selective_sql)
        .await
        .expect("selective join dataframe")
        .create_physical_plan()
        .await
        .expect("selective join physical plan");
    let selective_display = datafusion::physical_plan::displayable(selective_plan.as_ref())
        .indent(true)
        .to_string();
    assert!(
        selective_display.contains("HashJoinExec: mode=CollectLeft"),
        "the single-candidate build should use a collected hash join:\n{selective_display}"
    );
    let selective_rows =
        datafusion::physical_plan::collect(Arc::clone(&selective_plan), selective_ctx.task_ctx())
            .await
            .expect("selective join execution");
    assert_eq!(rendered(&selective_rows), vec!["7"]);
    let repeated_rows = selective_ctx
        .sql(&selective_sql)
        .await
        .expect("repeated selective join plan")
        .collect()
        .await
        .expect("repeated selective join execution");
    assert_eq!(rendered(&repeated_rows), vec!["7"]);

    println!("=== dynamic indexed join ===\n{plan}");
    println!("=== dynamic composite indexed join ===\n{composite_plan}");
    println!("=== partitioned dynamic fallback ===\n{partitioned_plan}");
    println!("=== selective indexed build ===\n{selective_display}");
    println!("lookup-index counters: {before:?} -> {after:?}");
    println!("composite lookup-index counters: {composite_before:?} -> {composite_after:?}");
}

/// A fully indexed miss plans an empty scan and sizes an absent-key join at
/// zero rows even though no per-file access-plan provider is needed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn indexed_miss_has_zero_scan_and_join_statistics() {
    use datafusion::physical_plan::{StatisticsArgs, StatisticsContext, collect, displayable};
    use datafusion::prelude::{col, lit};
    use datafusion_common::stats::Precision;

    const TABLE: &str = "svc_indexed_miss_stats";
    let fixture = common::TestFixture::new(common::BackendType::Sqlite)
        .await
        .expect("fixture");
    let runtime_env = Arc::new(RuntimeEnv::default());
    let indexed = build_table(&fixture, TABLE, &INDEX_KEYS, runtime_env).await;
    insert(&indexed, TABLE, service_rows(0, ROWS)).await;
    wait_for_index(&indexed, TABLE).await;
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(4));
    ctx.register_table(TABLE, Arc::clone(&indexed) as Arc<dyn TableProvider>)
        .expect("register indexed table");
    let filters = vec![
        col("\"TenantId\"").eq(lit(MISSING_ACCOUNT)),
        col("\"ServiceId\"").eq(lit(MISSING_APPLICATION)),
    ];
    let before = counters(&indexed);
    let scan = indexed
        .scan(&ctx.state(), None, &filters, None)
        .await
        .expect("absent-key scan");
    let after = counters(&indexed);
    assert_eq!(after.full - before.full, 1, "must probe full coverage");
    assert_eq!(after.candidate_rows - before.candidate_rows, 0);
    let scan_stats = StatisticsContext::new()
        .compute(scan.as_ref(), &StatisticsArgs::new())
        .expect("empty scan statistics");
    assert_eq!(scan_stats.num_rows, Precision::Exact(0));
    let scan_display = displayable(scan.as_ref()).indent(true).to_string();
    assert!(scan_display.contains("EmptyExec"), "{scan_display}");
    assert!(
        collect(scan, ctx.task_ctx())
            .await
            .expect("empty scan")
            .is_empty()
    );

    let sql = format!(
        "SELECT s.\"AutoId\" FROM {TABLE} s INNER JOIN {TABLE} b \
         ON s.\"AutoId\" = b.\"AutoId\" \
         WHERE b.\"TenantId\" = '{MISSING_ACCOUNT}' \
         AND b.\"ServiceId\" = '{MISSING_APPLICATION}'"
    );
    let join = ctx
        .sql(&sql)
        .await
        .expect("absent-key join")
        .create_physical_plan()
        .await
        .expect("absent-key join plan");
    let join_stats = StatisticsContext::new()
        .compute(join.as_ref(), &StatisticsArgs::new())
        .expect("empty join statistics");
    assert_eq!(join_stats.num_rows.get_value(), Some(&0));
    let join_display = displayable(join.as_ref()).indent(true).to_string();
    assert!(
        !join_display.contains("mode=Partitioned"),
        "an absent indexed build must not require a partitioned join:\n{join_display}"
    );
    assert!(
        collect(join, ctx.task_ctx())
            .await
            .expect("empty join")
            .is_empty()
    );
    println!(
        "fully covered absent key: scan rows={:?}, join rows={:?}; \
         counters={before:?} -> {after:?}\n{scan_display}\n{join_display}",
        scan_stats.num_rows, join_stats.num_rows
    );
}

/// Runtime file restriction skips covered non-candidates while preserving
/// every uncovered file and the rows it holds.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dynamic_filter_restricts_files_and_retains_uncovered_files() {
    use datafusion::physical_plan::{ExecutionPlan, collect, displayable};
    use datafusion_datasource::file_scan_config::FileScanConfig;
    use datafusion_datasource::source::DataSourceExec;

    fn planned_files(plan: &dyn ExecutionPlan) -> usize {
        if let Some(config) = plan
            .downcast_ref::<DataSourceExec>()
            .and_then(|scan| scan.data_source().downcast_ref::<FileScanConfig>())
        {
            return config
                .file_groups
                .iter()
                .map(|group| group.iter().count())
                .sum();
        }
        plan.children()
            .iter()
            .map(|child| planned_files(child.as_ref()))
            .sum()
    }

    fn scan_opened_files(plan: &dyn ExecutionPlan) -> Vec<usize> {
        if plan
            .downcast_ref::<DataSourceExec>()
            .and_then(|scan| scan.data_source().downcast_ref::<FileScanConfig>())
            .is_some()
        {
            return vec![
                plan.metrics()
                    .expect("file scan metrics")
                    .sum_by_name("files_opened")
                    .expect("opened file metric")
                    .as_usize(),
            ];
        }
        plan.children()
            .iter()
            .flat_map(|child| scan_opened_files(child.as_ref()))
            .collect()
    }

    fn restricted_opened_files(plan: &dyn ExecutionPlan) -> Vec<usize> {
        if plan.name() == "RuntimeRestrictedScanExec" {
            // Unary operators within the wrapper need not forward file metrics.
            return scan_opened_files(plan);
        }
        plan.children()
            .iter()
            .flat_map(|child| restricted_opened_files(child.as_ref()))
            .collect()
    }

    const TABLE: &str = "svc_runtime_files";
    let fixture = common::TestFixture::new(common::BackendType::Sqlite)
        .await
        .expect("fixture");
    let runtime_env = Arc::new(RuntimeEnv::default());
    let indexed = build_table(&fixture, TABLE, &INDEX_KEYS, Arc::clone(&runtime_env)).await;
    insert(&indexed, TABLE, service_rows(0, ROWS)).await;
    wait_for_index(&indexed, TABLE).await;
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(4));
    ctx.register_table(TABLE, Arc::clone(&indexed) as Arc<dyn TableProvider>)
        .expect("register indexed table");
    let sql = |keys: &str| {
        format!(
            "SELECT s.\"AutoId\" FROM (VALUES {keys}) k(key) \
             INNER JOIN {TABLE} s ON k.key = s.\"AutoId\" ORDER BY s.\"AutoId\""
        )
    };
    let full_plan = ctx
        .sql(&sql("(7)"))
        .await
        .expect("fully covered join")
        .create_physical_plan()
        .await
        .expect("fully covered physical plan");
    let covered_files = planned_files(full_plan.as_ref());
    assert!(
        covered_files > 1,
        "fixture must contain non-candidate files"
    );
    let rows = collect(Arc::clone(&full_plan), ctx.task_ctx())
        .await
        .expect("fully covered execution");
    assert_eq!(rendered(&rows), vec!["7"]);
    assert_eq!(restricted_opened_files(full_plan.as_ref()), vec![1]);
    println!(
        "full coverage: {covered_files} planned files, 1 opened; rows=7\n{}",
        displayable(full_plan.as_ref()).indent(true)
    );

    // A writer without indexes publishes new data files without index runs.
    // The reader retains its covered files and pins the mixed-coverage view;
    // a rebuild requested during execution cannot alter that pinned view.
    let writer = build_table(&fixture, TABLE, &[], runtime_env).await;
    insert(&writer, TABLE, service_rows(80_000, ROWS)).await;
    indexed
        .refresh(&writer)
        .await
        .expect("refresh appended files");
    let partial_plan = ctx
        .sql(&sql("(7), (80007)"))
        .await
        .expect("partially covered join")
        .create_physical_plan()
        .await
        .expect("partially covered physical plan");
    let all_files = planned_files(partial_plan.as_ref());
    let uncovered_files = all_files - covered_files;
    assert!(uncovered_files > 0, "append must produce uncovered files");
    let before = counters(&indexed);
    let rows = collect(Arc::clone(&partial_plan), ctx.task_ctx())
        .await
        .expect("partially covered execution");
    let after = counters(&indexed);
    assert_eq!(rendered(&rows), vec!["7", "80007"]);
    assert_eq!(
        after.partial - before.partial,
        1,
        "must probe mixed coverage"
    );
    assert_eq!(
        restricted_opened_files(partial_plan.as_ref()),
        vec![1 + uncovered_files],
        "open the covered candidate and every uncovered file"
    );
    assert!(
        1 + uncovered_files < all_files,
        "skip covered non-candidates"
    );
    println!(
        "partial coverage: {covered_files} covered + {uncovered_files} uncovered, {} opened; \
         rows=7,80007; counters={before:?} -> {after:?}\n{}",
        1 + uncovered_files,
        displayable(partial_plan.as_ref()).indent(true)
    );
}

/// The write-time index must be indistinguishable from one built by reading the
/// finished files, and it must be in place the moment the snapshot is visible.
///
/// This is the check that the writer's positions are real: the index is built
/// from what the sink reports during the overwrite, then diffed key-for-key and
/// address-for-address against a build that actually scans the written files.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn write_time_index_matches_a_read_back_build() {
    let fixture = common::TestFixture::new(common::BackendType::Sqlite)
        .await
        .expect("fixture");
    let runtime_env = Arc::new(RuntimeEnv::default());
    let table = build_table(
        &fixture,
        INDEXED_WRITE_TIME,
        &INDEX_KEYS,
        Arc::clone(&runtime_env),
    )
    .await;

    // A full-refresh overwrite is the write the index is built during.
    overwrite(&table, vec![service_rows(0, ROWS)]).await;

    // No query has run yet, so nothing could have triggered a read-back build:
    // an index published at this point can only have come from the write.
    let after_write = counters(&table);
    assert_eq!(
        after_write.none, 0,
        "a probe fell back before any query ran: {after_write:?}"
    );

    let report = table
        .verify_lookup_index_against_read_back()
        .await
        .expect("verification ran");
    assert!(
        report.agrees(),
        "write-time index disagrees with the read-back build: {:?}",
        report.mismatches
    );
    assert!(
        report.keys_compared > 0,
        "verification compared nothing: {report:?}"
    );
    // Positions restart at every file roll, so a single-file snapshot would
    // leave the counter-reset path untested.
    assert!(
        report.files > 1,
        "fixture produced one file; the file-roll path was not exercised: {report:?}"
    );
    for (shape, write_time, read_back) in &report.keys_per_shape {
        assert_eq!(write_time, read_back, "{shape}: key counts differ");
    }
    for (shape, write_time, read_back) in &report.postings_per_shape {
        assert_eq!(write_time, read_back, "{shape}: posting counts differ");
    }
    println!(
        "write-time vs read-back: files={} keys_compared={} keys_per_shape={:?} postings_per_shape={:?}",
        report.files, report.keys_compared, report.keys_per_shape, report.postings_per_shape
    );

    // The index must be usable on the FIRST query after the overwrite — that is
    // the whole point of building it before the snapshot goes visible.
    let before = counters(&table);
    let sql = format!(
        "SELECT * FROM {INDEXED_WRITE_TIME} WHERE \"TenantId\" = '{DUP_ACCOUNT}' \
         AND \"ServiceId\" = '{DUP_APPLICATION}' AND \"Active\" = 1 LIMIT 1"
    );
    let rows = rendered(&query(&table, INDEXED_WRITE_TIME, &sql).await);
    assert_eq!(
        rows.len(),
        1,
        "first post-overwrite lookup returned nothing"
    );
    let after = counters(&table);
    assert_eq!(
        after.full,
        before.full + 1,
        "the first query after the overwrite did not use the index: {before:?} -> {after:?}"
    );
    assert_eq!(
        after.none, before.none,
        "the first query after the overwrite saw an index that covers nothing: {after:?}"
    );

    // A SECOND overwrite must swap in a fresh index just as seamlessly, with the
    // new rows addressable straight away.
    overwrite(&table, vec![service_rows(3_000_000, 4_096)]).await;
    let report = table
        .verify_lookup_index_against_read_back()
        .await
        .expect("verification ran after the second overwrite");
    assert!(
        report.agrees(),
        "second overwrite diverged: {:?}",
        report.mismatches
    );

    let new_id = 3_000_000i64 + 11;
    let new_account = format!("AC{:032x}", new_id % ACCOUNTS);
    let new_application = format!("MG{new_id:032x}");
    let before = counters(&table);
    let sql = format!(
        "SELECT \"AutoId\" FROM {INDEXED_WRITE_TIME} WHERE \"TenantId\" = '{new_account}' \
         AND \"ServiceId\" = '{new_application}' ORDER BY \"AutoId\""
    );
    let rows = rendered(&query(&table, INDEXED_WRITE_TIME, &sql).await);
    assert_eq!(rows, vec![new_id.to_string()]);
    let after = counters(&table);
    assert_eq!(
        after.full,
        before.full + 1,
        "the first query after the second overwrite did not use the index"
    );
}

/// A write that replaces files swaps them in already covered, however large:
/// a full refresh (an overwrite) of more rows than an append would finish in
/// the background is finished before its commit, so the very first lookup
/// after it uses the index and no read-back build runs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_large_overwrite_is_covered_when_it_becomes_visible() {
    const LARGE: usize = 1_200_000;
    let fixture = common::TestFixture::new(common::BackendType::Sqlite)
        .await
        .expect("fixture");
    let runtime_env = Arc::new(RuntimeEnv::default());
    // The rewrite layout pinned to one sort column, so the lookup this test
    // runs cannot switch the table's compactions onto curve clustering.
    let vortex_config = VortexConfig {
        sort_columns: vec!["AutoId".to_string()],
        ..VortexConfig::default()
    };
    let table = open_table(
        &fixture,
        Arc::clone(&runtime_env),
        TableSpec::new(INDEXED_WRITE_TIME, service_schema(), &INDEX_KEYS).config(vortex_config),
    )
    .await;
    overwrite(&table, vec![service_rows(0, LARGE)]).await;

    let id = 1_000_003_i64;
    let sql = format!(
        "SELECT \"AutoId\" FROM {INDEXED_WRITE_TIME} WHERE \"TenantId\" = 'AC{:032x}' \
         AND \"ServiceId\" = 'MG{id:032x}'",
        id % ACCOUNTS
    );
    let before = counters(&table);
    assert_eq!(
        rendered(&query(&table, INDEXED_WRITE_TIME, &sql).await),
        vec![id.to_string()]
    );
    let after = counters(&table);
    assert_eq!(
        after.full,
        before.full + 1,
        "the first lookup after a large overwrite did not use the index: {after:?}"
    );
    assert_eq!(after.builds_started, 0, "a read-back build ran: {after:?}");

    let report = table
        .verify_lookup_index_against_read_back()
        .await
        .expect("verification ran");
    assert!(
        report.agrees(),
        "the index disagrees with a read-back build: {:?}",
        report.mismatches
    );
    assert!(
        report.keys_compared > 0,
        "verification compared nothing: {report:?}"
    );
}

/// A primary-key upsert table named `name` whose protected snapshots stay
/// unfolded, so an upsert's rows stay in the protected snapshot that holds
/// them; created, or reopened when the catalog already holds it.
async fn upsert_table(
    fixture: &common::TestFixture,
    runtime_env: Arc<RuntimeEnv>,
    name: &str,
) -> Arc<CayenneTableProvider> {
    open_table(fixture, runtime_env, upsert_spec(name)).await
}

/// The table [`upsert_table`] opens.
fn upsert_spec(name: &str) -> TableSpec<'_> {
    // Protected snapshots stay unfolded for the length of the test, and the
    // rewrite layout is pinned, so the only thing under test is the index.
    let vortex_config = VortexConfig {
        target_vortex_file_size_mb: 1,
        deletion_mode: DeletionMode::Key,
        sort_columns: vec!["AutoId".to_string()],
        compaction_trigger_protected_snapshots: 1_000,
        compaction_background_interval_ms: 0,
        ..VortexConfig::default()
    };
    TableSpec::new(name, service_schema(), &INDEX_KEYS)
        .config(vortex_config)
        .upsert_key("AutoId")
}

/// On a primary-key upsert table, an upsert's rows land in a protected
/// snapshot until compaction folds them in. The index covers those files too:
/// a lookup of an upserted key returns exactly its new row, and reads the one
/// protected snapshot that holds it rather than every protected snapshot.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn upserted_rows_are_found_through_the_index() {
    const TABLE: &str = "svc_upsert";
    const BATCHES: i64 = 4;
    // Large enough to be written to files rather than inlined.
    const PER_BATCH: usize = 10_000;
    let fixture = common::TestFixture::new(common::BackendType::Sqlite)
        .await
        .expect("fixture");
    let runtime_env = Arc::new(RuntimeEnv::default());
    let table = upsert_table(&fixture, runtime_env, TABLE).await;
    insert(&table, TABLE, service_rows(0, ROWS)).await;
    // Each batch rewrites 10,000 existing keys with a new payload.
    for batch in 0..BATCHES {
        let rows = service_rows(batch * 10_000, PER_BATCH);
        let ids = rows
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("AutoId");
        let payload: StringArray = ids
            .iter()
            .map(|id| id.map(|id| format!("updated-{id}")))
            .collect();
        let mut columns = rows.columns().to_vec();
        columns[5] = Arc::new(payload);
        let updated = RecordBatch::try_new(rows.schema(), columns).expect("updated batch");
        insert(&table, TABLE, updated).await;
    }

    // More writes, each followed by a scan that reconciles the index against
    // a new file set, as a serving table sees: an index that only knew the
    // current snapshot's files would retire the upserts' runs after a few.
    for write in 0..6_i64 {
        insert(
            &table,
            TABLE,
            service_rows(100_000 + write * 10_000, PER_BATCH),
        )
        .await;
        let _ = query(&table, TABLE, &format!("SELECT count(*) FROM {TABLE}")).await;
    }

    let id = 11_i64;
    let lookup = format!(
        "SELECT \"Payload\" FROM {TABLE} WHERE \"TenantId\" = 'AC{:032x}' \
         AND \"ServiceId\" = 'MG{id:032x}'",
        id % ACCOUNTS
    );
    assert_eq!(
        rendered(&query(&table, TABLE, &lookup).await),
        vec!["updated-11".to_string()],
        "the lookup must return exactly the upserted row"
    );
    let plan = arrow::util::pretty::pretty_format_batches(
        &query(&table, TABLE, &format!("EXPLAIN ANALYZE {lookup}")).await,
    )
    .expect("format plan")
    .to_string();
    let scanned: usize = plan
        .split("files_scanned=")
        .nth(1)
        .and_then(|rest| rest.split(|c: char| !c.is_ascii_digit()).next())
        .and_then(|digits| digits.parse().ok())
        .expect("the plan reports files_scanned");
    let snapshots: usize = plan
        .split("snapshots_scanned=")
        .nth(1)
        .and_then(|rest| rest.split(|c: char| !c.is_ascii_digit()).next())
        .and_then(|digits| digits.parse().ok())
        .expect("the plan reports snapshots_scanned");
    assert!(
        !plan.contains("lookup_index=none") && explain_total(&plan, "uncovered_files") == 0,
        "a lookup answered from a protected snapshot must report the selection\n{plan}"
    );
    assert!(
        scanned <= 2,
        "the lookup read {scanned} files across {snapshots} snapshots instead of the candidates only\n{plan}"
    );
}

/// After a restart with nothing indexed, the background build indexes every
/// file a lookup reads, the protected snapshots' as well as the current
/// snapshot's: the lookups that follow read no file in full, and an upserted
/// key is still found.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_background_build_indexes_the_files_of_protected_snapshots() {
    const TABLE: &str = "svc_upsert_reopened";
    const PER_BATCH: usize = 10_000;
    let fixture = common::TestFixture::new(common::BackendType::Sqlite)
        .await
        .expect("fixture");
    let runtime_env = Arc::new(RuntimeEnv::default());
    let table = upsert_table(&fixture, Arc::clone(&runtime_env), TABLE).await;
    insert(&table, TABLE, service_rows(0, ROWS)).await;
    // Each batch rewrites existing keys with a new payload, into a protected
    // snapshot.
    for batch in 0..2_i64 {
        let rows = service_rows(batch * 10_000, PER_BATCH);
        let ids = rows
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("AutoId");
        let payload: StringArray = ids
            .iter()
            .map(|id| id.map(|id| format!("updated-{id}")))
            .collect();
        let mut columns = rows.columns().to_vec();
        columns[5] = Arc::new(payload);
        let updated = RecordBatch::try_new(rows.schema(), columns).expect("updated batch");
        insert(&table, TABLE, updated).await;
    }
    drop(table);
    let table = upsert_table(&fixture, runtime_env, TABLE).await;

    let id = 11_i64;
    let lookup = format!(
        "SELECT \"Payload\" FROM {TABLE} WHERE \"TenantId\" = 'AC{:032x}' \
         AND \"ServiceId\" = 'MG{id:032x}'",
        id % ACCOUNTS
    );
    poll_until(
        Duration::from_secs(30),
        Duration::from_millis(100),
        async || {
            assert_eq!(
                rendered(&query(&table, TABLE, &lookup).await),
                vec!["updated-11".to_string()],
                "the lookup must return exactly the upserted row"
            );
            let plan = arrow::util::pretty::pretty_format_batches(
                &query(&table, TABLE, &format!("EXPLAIN ANALYZE {lookup}")).await,
            )
            .expect("format plan")
            .to_string();
            if !plan.contains("lookup_index=none") && explain_total(&plan, "uncovered_files") == 0 {
                Ok(())
            } else {
                Err(plan)
            }
        },
        |plan| format!("the background build never indexed every file the lookup reads\n{plan}"),
    )
    .await;
}

/// A reopened table loads the persisted runs over its protected snapshots'
/// files as well as the current snapshot's: the first lookup after the restart
/// is fully covered by the loaded index, with no build, and an upserted key is
/// found.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_reopened_table_loads_the_persisted_runs_of_its_protected_snapshots() {
    use cayenne::lookup_index::IndexPersistence;
    const TABLE: &str = "svc_upsert_persisted";
    let fixture = common::TestFixture::new(common::BackendType::Sqlite)
        .await
        .expect("fixture");
    let runtime_env = Arc::new(RuntimeEnv::default());
    let spec = || upsert_spec(TABLE).persistence(IndexPersistence::Enabled);
    let table = open_table(&fixture, Arc::clone(&runtime_env), spec()).await;
    insert(&table, TABLE, service_rows(0, ROWS)).await;
    for batch in 0..2_i64 {
        insert(&table, TABLE, upserted(batch * 10_000, 10_000)).await;
    }
    let id = 11_i64;
    let lookup = format!(
        "SELECT \"Payload\" FROM {TABLE} WHERE \"TenantId\" = 'AC{:032x}' \
         AND \"ServiceId\" = 'MG{id:032x}'",
        id % ACCOUNTS
    );
    // Every file the lookup reads is indexed, the protected snapshots' too.
    let deadline = Instant::now() + Duration::from_secs(30);
    while {
        let plan = explained(&table, TABLE, &lookup).await;
        plan.contains("lookup_index=none") || explain_total(&plan, "uncovered_files") > 0
    } {
        assert!(
            Instant::now() < deadline,
            "the table was never fully indexed"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    // The persisted runs settle once the background sync has caught up.
    let table_id = fixture
        .catalog
        .get_table(TABLE)
        .await
        .expect("table")
        .table_id;
    let mut last = Vec::new();
    let mut stable = 0;
    while stable < 5 {
        let mut runs: Vec<String> = fixture
            .catalog
            .list_index_runs(&table_id)
            .await
            .expect("list persisted runs")
            .into_iter()
            .map(|record| record.run_name)
            .collect();
        runs.sort();
        stable = if !runs.is_empty() && runs == last {
            stable + 1
        } else {
            0
        };
        last = runs;
        assert!(
            Instant::now() < deadline,
            "the persisted runs never settled"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    drop(table);

    let reopened = open_table(&fixture, runtime_env, spec()).await;
    let plan = explained(&reopened, TABLE, &lookup).await;
    assert!(
        !plan.contains("lookup_index=none") && explain_total(&plan, "uncovered_files") == 0,
        "the first lookup after reopening must be covered by the loaded runs\n{plan}"
    );
    assert_eq!(counters(&reopened).builds_started, 0, "no build was needed");
    assert_eq!(
        rendered(&query(&reopened, TABLE, &lookup).await),
        vec!["updated-11".to_string()]
    );
}

/// `rows` service rows from `first`, with every payload rewritten, as an
/// upsert of existing keys.
fn upserted(first: i64, rows: usize) -> RecordBatch {
    let batch = service_rows(first, rows);
    let ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("AutoId");
    let payload: StringArray = ids
        .iter()
        .map(|id| id.map(|id| format!("updated-{id}")))
        .collect();
    let mut columns = batch.columns().to_vec();
    columns[5] = Arc::new(payload);
    RecordBatch::try_new(batch.schema(), columns).expect("updated batch")
}

/// The `EXPLAIN` of `sql` against the table registered as `name`.
async fn explained(provider: &Arc<CayenneTableProvider>, name: &str, sql: &str) -> String {
    arrow::util::pretty::pretty_format_batches(
        &query(provider, name, &format!("EXPLAIN {sql}")).await,
    )
    .expect("format plan")
    .to_string()
}

/// An `IN` list on an indexed key is answered from the index, as one batched
/// probe of every listed key: single-column lists, lists on every column of a
/// composite key (their cartesian product), `NULL` members, and duplicates all
/// return exactly what the ordinary scan returns. A negated list never touches
/// the index.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn in_lists_are_answered_from_the_index() {
    let fixture = common::TestFixture::new(common::BackendType::Sqlite)
        .await
        .expect("fixture");
    let runtime_env = Arc::new(RuntimeEnv::default());
    let indexed = build_table(&fixture, INDEXED_IN, &INDEX_KEYS, Arc::clone(&runtime_env)).await;
    let plain = build_table(&fixture, PLAIN_IN, &[], Arc::clone(&runtime_env)).await;
    let batch = service_rows(0, ROWS);
    overwrite(&indexed, vec![batch.clone()]).await;
    overwrite(&plain, vec![batch]).await;

    let account = |id: i64| format!("'AC{:032x}'", id % ACCOUNTS);
    let application = |id: i64| format!("'MG{id:032x}'");
    let answered = [
        "SELECT * FROM {t} WHERE \"AutoId\" IN (3, 17, 17, 39999, 123456789) ORDER BY \"AutoId\""
            .to_string(),
        "SELECT * FROM {t} WHERE \"AutoId\" IN (NULL, 5) ORDER BY \"AutoId\"".to_string(),
        format!(
            "SELECT * FROM {{t}} WHERE \"TenantId\" IN ({}, {}) AND \"ServiceId\" IN ({}, {}, {}) ORDER BY \"AutoId\"",
            account(7),
            account(8),
            application(7),
            application(8),
            application(507),
        ),
        format!(
            "SELECT * FROM {{t}} WHERE \"TenantId\" = {} AND \"ServiceId\" IN ({}, {}) AND \"Active\" = 1 ORDER BY \"AutoId\"",
            account(11),
            application(11),
            application(12),
        ),
        format!(
            "SELECT * FROM {{t}} WHERE \"TenantId\" IN ('{DUP_ACCOUNT}', '{MISSING_ACCOUNT}') AND \"ServiceId\" IN ('{DUP_APPLICATION}', '{MISSING_APPLICATION}') ORDER BY \"AutoId\""
        ),
    ];
    for query_sql in &answered {
        let before = counters(&indexed);
        let found =
            rendered(&query(&indexed, INDEXED_IN, &query_sql.replace("{t}", INDEXED_IN)).await);
        let expected =
            rendered(&query(&plain, PLAIN_IN, &query_sql.replace("{t}", PLAIN_IN)).await);
        assert_eq!(found, expected, "{query_sql}");
        let after = counters(&indexed);
        assert_eq!(
            (after.full + after.partial) - (before.full + before.partial),
            1,
            "{query_sql} was not answered from the index: {before:?} -> {after:?}"
        );
        assert_eq!(after.none, before.none, "{query_sql}: {after:?}");
    }

    // 60 accounts by 40 services is 2,400 tuples, past the 2,048 a lookup
    // probes: the lookup scans, says why, and still matches.
    let accounts: Vec<String> = (0..60).map(account).collect();
    let applications: Vec<String> = (0..40).map(application).collect();
    let too_many = format!(
        "SELECT * FROM {{t}} WHERE \"TenantId\" IN ({}) AND \"ServiceId\" IN ({}) ORDER BY \"AutoId\"",
        accounts.join(", "),
        applications.join(", ")
    );
    assert_eq!(
        rendered(&query(&indexed, INDEXED_IN, &too_many.replace("{t}", INDEXED_IN)).await),
        rendered(&query(&plain, PLAIN_IN, &too_many.replace("{t}", PLAIN_IN)).await),
    );
    let bounded = query(
        &indexed,
        INDEXED_IN,
        &format!("EXPLAIN {}", too_many.replace("{t}", INDEXED_IN)),
    )
    .await;
    let bounded = arrow::util::pretty::pretty_format_batches(&bounded)
        .expect("format plan")
        .to_string();
    assert!(
        bounded.contains("lookup_index=none")
            && bounded.contains("lookup_index_reason=too_many_keys"),
        "a lookup past the key bound did not say why it scanned:\n{bounded}"
    );

    let negated = "SELECT COUNT(*) FROM {t} WHERE \"AutoId\" NOT IN (3, 17)";
    let before = counters(&indexed);
    assert_eq!(
        rendered(&query(&indexed, INDEXED_IN, &negated.replace("{t}", INDEXED_IN)).await),
        rendered(&query(&plain, PLAIN_IN, &negated.replace("{t}", PLAIN_IN)).await),
    );
    let after = counters(&indexed);
    assert_eq!(
        (after.full, after.none),
        (before.full, before.none),
        "a negated list must not be probed: {after:?}"
    );
}

/// An index answers candidate rows, and a key's word may be shared with
/// other keys (a 64-bit hash of a string key): every query must still return
/// exactly its own rows. With 4-bit words, about 40,000 keys share 16 words,
/// so every lookup's candidates are mostly other keys' rows, and the results
/// must still match a table with no index row for row.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn keys_sharing_a_word_return_exactly_their_own_rows() {
    let fixture = common::TestFixture::new(common::BackendType::Sqlite)
        .await
        .expect("fixture");
    let runtime_env = Arc::new(RuntimeEnv::default());
    let vortex_config = VortexConfig {
        target_vortex_file_size_mb: 1,
        ..VortexConfig::default()
    };
    let context = CayenneContext::new(&vortex_config, Arc::clone(&runtime_env), INDEXED_COLLIDING);
    let catalog: Arc<dyn MetadataCatalog> =
        Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
    let indexed = Arc::new(
        CayenneTableProviderBuilder::new(catalog, Arc::clone(&runtime_env))
            .with_context(context)
            .with_index_word_bits(4)
            .with_secondary_indexes(
                INDEX_KEYS
                    .iter()
                    .map(|columns| columns.iter().map(|c| (*c).to_string()).collect())
                    .collect(),
            )
            .create(CreateTableOptions {
                table_name: INDEXED_COLLIDING.to_string(),
                schema: service_schema(),
                primary_key: vec![],
                on_conflict: None,
                base_path: fixture.data_path.to_string_lossy().to_string(),
                partition_column: None,
                vortex_config,
            })
            .await
            .expect("create table"),
    );
    let plain = build_table(&fixture, PLAIN_COLLIDING, &[], Arc::clone(&runtime_env)).await;
    let batch = service_rows(0, ROWS);
    insert(&indexed, INDEXED_COLLIDING, batch.clone()).await;
    insert(&plain, PLAIN_COLLIDING, batch).await;
    wait_for_index(&indexed, INDEXED_COLLIDING).await;

    let before = counters(&indexed);
    let mut result_rows = 0_u64;
    let mut lookups = 0_u64;
    for i in 0..15i64 {
        let id = i * 97;
        let account = format!("AC{:032x}", id % ACCOUNTS);
        let application = format!("MG{id:032x}");
        for sql in [
            format!(
                "SELECT * FROM {{table}} WHERE \"TenantId\" = '{account}' \
                 AND \"ServiceId\" = '{application}' ORDER BY \"AutoId\""
            ),
            format!("SELECT * FROM {{table}} WHERE \"AutoId\" = {id} ORDER BY \"AutoId\""),
            // A key no row holds: every candidate belongs to another key.
            format!(
                "SELECT * FROM {{table}} WHERE \"TenantId\" = '{account}' \
                 AND \"ServiceId\" = 'MGabsent{id}' ORDER BY \"AutoId\""
            ),
        ] {
            let indexed_rows = rendered(
                &query(
                    &indexed,
                    INDEXED_COLLIDING,
                    &sql.replace("{table}", INDEXED_COLLIDING),
                )
                .await,
            );
            let plain_rows = rendered(
                &query(
                    &plain,
                    PLAIN_COLLIDING,
                    &sql.replace("{table}", PLAIN_COLLIDING),
                )
                .await,
            );
            assert_eq!(indexed_rows, plain_rows, "{sql}");
            result_rows += u64::try_from(plain_rows.len()).expect("fits u64");
            lookups += 1;
        }
    }
    let after = counters(&indexed);
    let full = after.full - before.full;
    let candidates = after.candidate_rows - before.candidate_rows;
    println!(
        "{lookups} lookups: {full} answered from the index, {candidates} candidate rows, {result_rows} result rows"
    );
    assert!(
        full >= lookups,
        "every lookup must use the index: {full} of {lookups}"
    );
    assert!(
        candidates > 100 * result_rows,
        "4-bit words must make most candidates other keys' rows: {candidates} candidates for {result_rows} rows"
    );
}
