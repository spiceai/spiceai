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

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{Array, Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use cayenne::metadata::{CreateTableOptions, VortexConfig};
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

struct SplitMix64(u64);

impl SplitMix64 {
    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
}

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

async fn build_table(
    fixture: &common::TestFixture,
    table_name: &str,
    index_keys: &[&[&str]],
    runtime_env: Arc<RuntimeEnv>,
) -> Arc<CayenneTableProvider> {
    // A small target file size so the table spans several Vortex files and the
    // index has candidate FILES to prune, not just rows within one file. A table
    // under test and its control differ only in `index_keys`, which is the whole
    // comparison.
    let vortex_config = VortexConfig {
        target_vortex_file_size_mb: 1,
        ..VortexConfig::default()
    };
    let context = CayenneContext::new(&vortex_config, Arc::clone(&runtime_env), table_name);
    let options = CreateTableOptions {
        table_name: table_name.to_string(),
        // A serving view has no primary key; match that shape.
        schema: service_schema(),
        primary_key: vec![],
        on_conflict: None,
        base_path: fixture.data_path.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config,
    };
    let catalog = Arc::clone(&fixture.catalog);
    let catalog: Arc<dyn MetadataCatalog> = catalog;
    Arc::new(
        CayenneTableProviderBuilder::new(catalog, runtime_env)
            .with_context(context)
            .with_secondary_indexes(
                index_keys
                    .iter()
                    .map(|columns| columns.iter().map(|c| (*c).to_string()).collect())
                    .collect(),
            )
            .create(options)
            .await
            .expect("create table"),
    )
}

async fn insert(provider: &Arc<CayenneTableProvider>, table_name: &str, batch: RecordBatch) {
    let ctx = SessionContext::new();
    ctx.register_table(table_name, Arc::clone(provider) as Arc<dyn TableProvider>)
        .expect("register target");
    let mem = datafusion::datasource::MemTable::try_new(service_schema(), vec![vec![batch]])
        .expect("memtable");
    ctx.register_table("src", Arc::new(mem))
        .expect("register src");
    ctx.sql(&format!("INSERT INTO {table_name} SELECT * FROM src"))
        .await
        .expect("insert plan")
        .collect()
        .await
        .expect("insert");
}

async fn overwrite(provider: &Arc<CayenneTableProvider>, batch: RecordBatch) {
    let ctx = SessionContext::new();
    let exec = datafusion::datasource::memory::MemorySourceConfig::try_new_exec(
        &[vec![batch]],
        service_schema(),
        None,
    )
    .expect("overwrite source");
    let plan = provider
        .insert_into(
            &ctx.state(),
            exec,
            datafusion_expr::dml::InsertOp::Overwrite,
        )
        .await
        .expect("overwrite plan");
    datafusion_physical_plan::collect(plan, ctx.task_ctx())
        .await
        .expect("overwrite");
}

async fn query(
    provider: &Arc<CayenneTableProvider>,
    table_name: &str,
    sql: &str,
) -> Vec<RecordBatch> {
    let ctx = SessionContext::new();
    ctx.register_table(table_name, Arc::clone(provider) as Arc<dyn TableProvider>)
        .expect("register");
    ctx.sql(sql)
        .await
        .expect("plan")
        .collect()
        .await
        .expect("execute")
}

/// Rows rendered as text so the two arms can be compared exactly, independent
/// of batch boundaries.
fn rendered(batches: &[RecordBatch]) -> Vec<String> {
    let mut rows = Vec::new();
    for batch in batches {
        for row in 0..batch.num_rows() {
            let mut cells = Vec::with_capacity(batch.num_columns());
            for column in 0..batch.num_columns() {
                let array = batch.column(column);
                cells.push(if array.is_null(row) {
                    "NULL".to_string()
                } else {
                    arrow::util::display::array_value_to_string(array, row).expect("render cell")
                });
            }
            rows.push(cells.join("|"));
        }
    }
    rows.sort();
    rows
}

fn counters_of(provider: &Arc<CayenneTableProvider>) -> cayenne::lookup_index::LookupIndexCounters {
    provider
        .lookup_index_counters()
        .expect("indexed table has index state")
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
    let deadline = Instant::now() + Duration::from_mins(2);
    loop {
        let _ = query(provider, table, &sql).await;
        if counters_of(provider).access_plans_attached > 0 {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "point-lookup index was never published: {:?}",
            counters_of(provider)
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
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
    let before = counters_of(&indexed);
    assert_eq!(
        before.snapshot_mismatch, 0,
        "the index must bind to the snapshot it was built from"
    );

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
    let after_sample = counters_of(&indexed);
    let probes = (after_sample.selected + after_sample.empty) - (before.selected + before.empty);
    assert_eq!(
        probes, 40,
        "every sampled lookup must reach the index: {before:?} -> {after_sample:?}"
    );
    // A NULL key column is never indexed, so the pool-shape keys the fixture
    // nulls out are legitimate empty probes rather than selections.
    let selected = after_sample.selected - before.selected;
    assert!(
        selected >= 30,
        "too few lookups attached a selection: {before:?} -> {after_sample:?}"
    );
    assert!(
        after_sample.access_plans_attached - before.access_plans_attached >= selected,
        "row selections were not attached to the Vortex scan: {after_sample:?}"
    );
    assert_eq!(
        after_sample.snapshot_mismatch, 0,
        "unexpected snapshot mismatch: {after_sample:?}"
    );
    // One candidate file per probe on a unique key is the point of the index;
    // allow slack for the non-unique pool shape but not a whole-table fan-out.
    assert!(
        after_sample.candidate_files < after_sample.selected * 4,
        "index pruned too few files: {after_sample:?}"
    );
    assert!(
        after_sample.candidate_rows >= after_sample.selected,
        "a selected probe must carry at least one candidate row: {after_sample:?}"
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
        counters_of(&indexed).empty > 0,
        "a complete index miss should be recorded as an empty probe"
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

    // --- Rows written after the index was built move the snapshot. The index
    //     must refuse itself rather than answer from stale row addresses.
    let mismatch_before = counters_of(&indexed).snapshot_mismatch;
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
        counters_of(&indexed)
    );
    assert_eq!(new_indexed, new_plain);
    assert!(
        counters_of(&indexed).snapshot_mismatch > mismatch_before
            || counters_of(&indexed).unbuilt > before.unbuilt
            || counters_of(&indexed).access_plans_attached > after_sample.access_plans_attached,
        "the moved snapshot was neither refused nor re-indexed: {:?}",
        counters_of(&indexed)
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

    println!("lookup-index counters: {:?}", counters_of(&indexed));
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
            && indexed_text.contains("lookup_index_outcome=selected")
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
            && fallback_text.contains("lookup_index_outcome=not_applicable"),
        "fallback plan did not explain why the index was skipped:\n{fallback_text}"
    );

    println!("=== {INDEXED_EVIDENCE} ===\n{indexed_text}");
    println!("=== {PLAIN_EVIDENCE} ===\n{plain_text}");
    println!("=== fallback ===\n{fallback_text}");
    println!("lookup-index counters: {:?}", counters_of(&indexed));
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
    let before = counters_of(&indexed);
    let actual = ctx
        .sql(&sql(INDEXED))
        .await
        .expect("indexed join plan")
        .collect()
        .await
        .expect("indexed join");
    let after = counters_of(&indexed);
    assert_eq!(rendered(&actual), rendered(&expected));
    assert_eq!(rendered(&actual).len(), 4_098);
    assert_eq!(after.selected, before.selected);
    assert_eq!(after.access_plans_attached, before.access_plans_attached);
    assert_eq!(
        after.runtime_fallback, before.runtime_fallback,
        "extraction must decline the oversized key set before reaching an index probe"
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
/// index after physical planning. The scan-level `lookup_index_outcome` remains
/// `not_applicable` because no literal existed at `TableProvider::scan` time;
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
    let before = counters_of(&indexed);
    let rows = ctx
        .sql(&sql)
        .await
        .expect("join plan")
        .collect()
        .await
        .expect("join execution");
    assert_eq!(rendered(&rows), vec!["12345", "39999", "7", "7"]);
    let after = counters_of(&indexed);
    assert_eq!(
        after.selected - before.selected,
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
        plan.contains("lookup_index_outcome=not_applicable"),
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
    let composite_before = counters_of(&indexed);
    let composite_rows = ctx
        .sql(&composite_sql)
        .await
        .expect("composite join plan")
        .collect()
        .await
        .expect("composite join execution");
    assert_eq!(rendered(&composite_rows), vec!["12345", "39999", "7"]);
    let composite_after = counters_of(&indexed);
    assert_eq!(
        composite_after.selected - composite_before.selected,
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
    let partitioned_before = counters_of(&indexed);
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
    let partitioned_after = counters_of(&indexed);
    assert_eq!(
        partitioned_after.selected, partitioned_before.selected,
        "a CASE-partitioned key set must fall back without a partial selection: \
         {partitioned_before:?} -> {partitioned_after:?}"
    );
    assert_eq!(
        partitioned_after.candidate_rows, partitioned_before.candidate_rows,
        "a declined partitioned filter must not record candidate rows"
    );

    println!("=== dynamic indexed join ===\n{plan}");
    println!("=== dynamic composite indexed join ===\n{composite_plan}");
    println!("=== partitioned dynamic fallback ===\n{partitioned_plan}");
    println!("lookup-index counters: {before:?} -> {after:?}");
    println!("composite lookup-index counters: {composite_before:?} -> {composite_after:?}");
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
    overwrite(&table, service_rows(0, ROWS)).await;

    // No query has run yet, so nothing could have triggered a read-back build:
    // an index published at this point can only have come from the write.
    let after_write = counters_of(&table);
    assert_eq!(
        after_write.unbuilt, 0,
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
    let before = counters_of(&table);
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
    let after = counters_of(&table);
    assert_eq!(
        after.selected,
        before.selected + 1,
        "the first query after the overwrite did not use the index: {before:?} -> {after:?}"
    );
    assert_eq!(
        after.unbuilt, before.unbuilt,
        "the first query after the overwrite saw an unbuilt index: {after:?}"
    );
    assert_eq!(
        after.snapshot_mismatch, 0,
        "unexpected snapshot mismatch: {after:?}"
    );

    // A SECOND overwrite must swap in a fresh index just as seamlessly, with the
    // new rows addressable straight away.
    overwrite(&table, service_rows(3_000_000, 4_096)).await;
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
    let before = counters_of(&table);
    let sql = format!(
        "SELECT \"AutoId\" FROM {INDEXED_WRITE_TIME} WHERE \"TenantId\" = '{new_account}' \
         AND \"ServiceId\" = '{new_application}' ORDER BY \"AutoId\""
    );
    let rows = rendered(&query(&table, INDEXED_WRITE_TIME, &sql).await);
    assert_eq!(rows, vec![new_id.to_string()]);
    let after = counters_of(&table);
    assert_eq!(
        after.selected,
        before.selected + 1,
        "the first query after the second overwrite did not use the index"
    );
}
