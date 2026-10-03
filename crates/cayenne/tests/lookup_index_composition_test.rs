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

//! How the point-lookup index composes with the rest of the scan: predicates
//! that only look like an indexed equality, and position-delete vectors.
//!
//! Every check compares an indexed table against an identical unindexed one in
//! the same process, so a divergence is the index changing a result.

#![allow(clippy::expect_used, clippy::unwrap_used)]

mod common;

use common::lookup_index::{
    TableSpec, counters, file_mode_config, memory_mode_config, open_table, overwrite, poll_until,
    query, rendered,
};

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use cayenne::metadata::{CreateTableOptions, VortexConfig};
use cayenne::provider::CayenneContext;
use cayenne::{CayenneTableProvider, CayenneTableProviderBuilder, MetadataCatalog};

use datafusion::datasource::TableProvider;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::prelude::SessionContext;

/// Above the inline caps, so the overwrite writes Vortex files the index can
/// address.
const ROWS: usize = 20_000;

async fn build_table(
    fixture: &common::TestFixture,
    runtime_env: Arc<RuntimeEnv>,
    name: &str,
    schema: Arc<Schema>,
    index_keys: Option<&[&str]>,
) -> Arc<CayenneTableProvider> {
    let indexes: Vec<&[&str]> = index_keys.into_iter().collect();
    open_table(fixture, runtime_env, TableSpec::new(name, schema, &indexes)).await
}

fn scored_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("AutoId", DataType::Int64, false),
        Field::new("TenantId", DataType::Utf8, false),
        Field::new("Score", DataType::Int64, false),
    ]))
}

/// A floating-point column can be an index column at every width: the table is
/// created with an index on it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn floating_point_index_columns_are_accepted_at_table_creation() {
    let fixture = common::TestFixture::new(common::BackendType::Sqlite)
        .await
        .expect("fixture");
    let runtime_env = Arc::new(RuntimeEnv::default());

    for (suffix, data_type) in [
        ("f16", DataType::Float16),
        ("f32", DataType::Float32),
        ("f64", DataType::Float64),
    ] {
        let name = format!("float_index_{suffix}");
        let schema = Arc::new(Schema::new(vec![Field::new(
            "Score",
            data_type.clone(),
            false,
        )]));
        let vortex_config = VortexConfig::default();
        let context = CayenneContext::new(&vortex_config, Arc::clone(&runtime_env), &name);
        let options = CreateTableOptions {
            table_name: name.clone(),
            schema,
            primary_key: vec![],
            on_conflict: None,
            base_path: fixture.data_path.to_string_lossy().to_string(),
            partition_column: None,
            vortex_config,
        };
        let catalog = Arc::clone(&fixture.catalog);
        let catalog: Arc<dyn MetadataCatalog> = catalog;
        let table = CayenneTableProviderBuilder::new(catalog, Arc::clone(&runtime_env))
            .with_context(context)
            .with_secondary_indexes(vec![vec!["Score".to_string()]])
            .create(options)
            .await
            .unwrap_or_else(|error| panic!("{data_type} lookup index was refused: {error}"));
        assert!(
            table.lookup_index_counters().is_some(),
            "{data_type}: the table has no index"
        );
    }
}

/// A cast on the COLUMN side can map many stored values onto the literal:
/// adjacent integers above 2^53 become the same `DOUBLE`. The index is keyed on
/// the stored integer, so answering the cast from it would silently drop one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_column_side_cast_is_never_answered_from_the_index() {
    const INDEXED: &str = "cast_indexed";
    const PLAIN: &str = "cast_plain";
    let fixture = common::TestFixture::new(common::BackendType::Sqlite)
        .await
        .expect("fixture");
    let runtime_env = Arc::new(RuntimeEnv::default());
    let indexed = build_table(
        &fixture,
        Arc::clone(&runtime_env),
        INDEXED,
        scored_schema(),
        Some(&["TenantId", "Score"]),
    )
    .await;
    let plain = build_table(&fixture, runtime_env, PLAIN, scored_schema(), None).await;

    let mut auto_id = Vec::with_capacity(ROWS + 3);
    let mut tenant = Vec::with_capacity(ROWS + 3);
    let mut score = Vec::with_capacity(ROWS + 3);
    for i in 0..ROWS {
        auto_id.push(i64::try_from(i).expect("fits i64"));
        tenant.push(format!("T{:04}", i % 50));
        score.push(i64::try_from(i).expect("fits i64"));
    }
    for (offset, value) in [9_007_199_254_740_992_i64, 9_007_199_254_740_993]
        .into_iter()
        .enumerate()
    {
        auto_id.push(1_000_000 + i64::try_from(offset).expect("fits i64"));
        tenant.push("PLANTED".to_string());
        score.push(value);
    }
    let batch = RecordBatch::try_new(
        scored_schema(),
        vec![
            Arc::new(Int64Array::from(auto_id)),
            Arc::new(StringArray::from(tenant)),
            Arc::new(Int64Array::from(score)),
        ],
    )
    .expect("batch");
    overwrite(&indexed, vec![batch.clone()]).await;
    overwrite(&plain, vec![batch]).await;

    // The index is in place: a bare equality on both key columns uses it.
    let before = counters(&indexed);
    let bare = "SELECT \"AutoId\" FROM {t} WHERE \"TenantId\" = 'PLANTED' \
                AND \"Score\" = 9007199254740992";
    assert_eq!(
        rendered(&query(&indexed, INDEXED, &bare.replace("{t}", INDEXED)).await),
        rendered(&query(&plain, PLAIN, &bare.replace("{t}", PLAIN)).await)
    );
    assert_eq!(
        counters(&indexed).full,
        before.full + 1,
        "a bare equality on both key columns should use the index"
    );

    let casted = "SELECT \"AutoId\" FROM {t} WHERE \"TenantId\" = 'PLANTED' \
                  AND CAST(\"Score\" AS DOUBLE) = CAST(9007199254740992 AS DOUBLE) \
                  ORDER BY \"AutoId\"";
    let expected = rendered(&query(&plain, PLAIN, &casted.replace("{t}", PLAIN)).await);
    assert_eq!(
        expected.len(),
        2,
        "control table: both adjacent integers round to the same double"
    );
    let actual = rendered(&query(&indexed, INDEXED, &casted.replace("{t}", INDEXED)).await);
    assert_eq!(
        actual, expected,
        "a column-side cast was answered from the index and dropped rows"
    );
}

fn service_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("AutoId", DataType::Int64, false),
        Field::new("TenantId", DataType::Utf8, false),
        Field::new("ServiceId", DataType::Utf8, false),
        Field::new("Active", DataType::Int32, false),
    ]))
}

fn service_rows(rows: usize) -> RecordBatch {
    let mut auto_id = Vec::with_capacity(rows);
    let mut tenant = Vec::with_capacity(rows);
    let mut service = Vec::with_capacity(rows);
    let mut active = Vec::with_capacity(rows);
    for i in 0..rows {
        let id = i64::try_from(i).expect("fits i64");
        // Every tenth key is shared by two rows, so a deleted candidate can sit
        // next to a live one under the same key.
        let key = id - i64::from(i % 10 == 1);
        auto_id.push(id);
        tenant.push(format!("AC{:032x}", key % 97));
        service.push(format!("MG{key:032x}"));
        active.push(1i32);
    }
    RecordBatch::try_new(
        service_schema(),
        vec![
            Arc::new(Int64Array::from(auto_id)),
            Arc::new(StringArray::from(tenant)),
            Arc::new(StringArray::from(service)),
            Arc::new(Int32Array::from(active)),
        ],
    )
    .expect("batch")
}

/// Position-delete vectors and a lookup row selection both restrict what a file
/// scan reads. Deleted candidates must stay hidden, and the index should keep
/// serving the lookup rather than falling back to a scan.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn position_deletes_compose_with_the_index() {
    const INDEXED: &str = "deletes_indexed";
    const PLAIN: &str = "deletes_plain";
    let fixture = common::TestFixture::new(common::BackendType::Sqlite)
        .await
        .expect("fixture");
    let runtime_env = Arc::new(RuntimeEnv::default());
    let indexed = build_table(
        &fixture,
        Arc::clone(&runtime_env),
        INDEXED,
        service_schema(),
        Some(&["TenantId", "ServiceId"]),
    )
    .await;
    let plain = build_table(&fixture, runtime_env, PLAIN, service_schema(), None).await;
    let batch = service_rows(ROWS);
    overwrite(&indexed, vec![batch.clone()]).await;
    overwrite(&plain, vec![batch]).await;

    // Keys whose rows were deleted, keys with a deleted and a live row, keys that
    // were never touched, and keys no row holds.
    let keys: Vec<i64> = (0..200).map(|i| i * 7 + i % 3).collect();
    let mut deleted = Vec::new();
    for (i, &id) in keys.iter().enumerate() {
        if i % 2 == 0 {
            deleted.push(id);
        }
        if i % 4 == 0 && id % 10 == 0 {
            deleted.push(id + 1);
        }
    }
    let deleted = deleted
        .iter()
        .map(i64::to_string)
        .collect::<Vec<_>>()
        .join(", ");

    // Delete through the ordinary DML path on both tables.
    for (provider, name) in [(&indexed, INDEXED), (&plain, PLAIN)] {
        query(
            provider,
            name,
            &format!("DELETE FROM {name} WHERE \"AutoId\" IN ({deleted})"),
        )
        .await;
    }
    assert_eq!(
        rendered(
            &query(
                &indexed,
                INDEXED,
                &format!("SELECT COUNT(*) FROM {INDEXED}")
            )
            .await
        ),
        rendered(&query(&plain, PLAIN, &format!("SELECT COUNT(*) FROM {PLAIN}")).await)
    );
    assert_eq!(
        rendered(&query(&plain, PLAIN, &format!("SELECT COUNT(*) FROM {PLAIN}")).await),
        vec![(ROWS - deleted.split(", ").count()).to_string()],
        "the control table did not apply the delete"
    );
    let query_for = |id: i64| {
        format!(
            "SELECT \"AutoId\" FROM {{t}} WHERE \"TenantId\" = 'AC{:032x}' \
             AND \"ServiceId\" = 'MG{:032x}' ORDER BY \"AutoId\"",
            id % 97,
            id
        )
    };

    // The index may be rebuilt for the post-delete snapshot in the background;
    // keep issuing lookups until one is served from it.
    poll_until(
        Duration::from_mins(2),
        Duration::from_millis(250),
        async || {
            let before = counters(&indexed).full;
            let _ = query(
                &indexed,
                INDEXED,
                &query_for(keys[1]).replace("{t}", INDEXED),
            )
            .await;
            let after = counters(&indexed);
            if after.full > before { Ok(()) } else { Err(after) }
        },
        |after| {
            format!(
                "lookups on a table with position deletes were never served from the index: {after:?}"
            )
        },
    )
    .await;

    let before = counters(&indexed);
    for &id in &keys {
        let lookup = query_for(id);
        assert_eq!(
            rendered(&query(&indexed, INDEXED, &lookup.replace("{t}", INDEXED)).await),
            rendered(&query(&plain, PLAIN, &lookup.replace("{t}", PLAIN)).await),
            "lookup {id} diverged from the control table after deletes"
        );
    }
    let after = counters(&indexed);
    // Every lookup is answered by the index, whether or not a row holds its
    // key. None falls back to a scan.
    let answered = (after.full - before.full) + (after.partial - before.partial);
    assert!(
        answered == u64::try_from(keys.len()).expect("fits")
            && after.full > before.full
            && after.none == before.none,
        "lookups on a table with position deletes were not served from the index: {before:?} -> {after:?}"
    );

    let dynamic_keys = &keys[..64];
    let key_schema = Arc::new(Schema::new(vec![
        Field::new("tenant", DataType::Utf8, false),
        Field::new("service", DataType::Utf8, false),
    ]));
    let key_batch = RecordBatch::try_new(
        Arc::clone(&key_schema),
        vec![
            Arc::new(StringArray::from(
                dynamic_keys
                    .iter()
                    .map(|id| format!("AC{:032x}", id % 97))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                dynamic_keys
                    .iter()
                    .map(|id| format!("MG{id:032x}"))
                    .collect::<Vec<_>>(),
            )),
        ],
    )
    .expect("dynamic keys");
    let run_join = |provider: Arc<CayenneTableProvider>, table: &'static str| {
        let key_schema = Arc::clone(&key_schema);
        let key_batch = key_batch.clone();
        async move {
            let ctx = SessionContext::new();
            ctx.register_table(table, provider as Arc<dyn TableProvider>)
                .expect("register target");
            let key_table =
                datafusion::datasource::MemTable::try_new(key_schema, vec![vec![key_batch]])
                    .expect("key table");
            ctx.register_table("dynamic_keys", Arc::new(key_table))
                .expect("register keys");
            ctx.sql(&format!(
                "SELECT s.\"AutoId\" FROM dynamic_keys k INNER JOIN {table} s \
                 ON k.tenant = s.\"TenantId\" AND k.service = s.\"ServiceId\" \
                 ORDER BY s.\"AutoId\""
            ))
            .await
            .expect("join plan")
            .collect()
            .await
            .expect("join execution")
        }
    };
    let expected = rendered(&run_join(Arc::clone(&plain), PLAIN).await);
    let full_before = counters(&indexed).full;
    let actual = rendered(&run_join(Arc::clone(&indexed), INDEXED).await);
    assert_eq!(
        actual, expected,
        "dynamic indexed join exposed position-deleted rows"
    );
    assert_eq!(
        counters(&indexed).full,
        full_before + 1,
        "the dynamic join did not compose its index selection with position deletes"
    );
}

/// At every float width, in file and memory mode, a lookup on an indexed float
/// column is answered from the index and returns exactly the rows an unindexed
/// table returns: for both zeros, NaN, infinities, the width's extremes,
/// values held by many rows, and values no row holds.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn float_key_lookups_return_the_rows_of_an_unindexed_table() {
    use arrow::array::{ArrayRef, Float32Array, Float64Array, PrimitiveArray};
    use arrow::datatypes::Float16Type;
    type F16 = <Float16Type as arrow::datatypes::ArrowPrimitiveType>::Native;
    let fixture = common::TestFixture::new(common::BackendType::Sqlite)
        .await
        .expect("fixture");
    let runtime_env = Arc::new(RuntimeEnv::default());
    // Enough rows to be written to files, repeating each value many times.
    let values: Vec<Option<f64>> = [
        Some(0.0),
        Some(-0.0),
        Some(f64::NAN),
        Some(f64::INFINITY),
        Some(f64::NEG_INFINITY),
        Some(1.5),
        Some(-2.5),
        Some(0.1),
        Some(65_504.0),
        Some(-65_504.0),
        Some(f64::from(f32::MAX)),
        Some(6.0e-8),
        None,
    ]
    .into_iter()
    .cycle()
    .take(ROWS)
    .collect();
    for ((suffix, data_type), (mode, config)) in [
        ("f16", DataType::Float16),
        ("f32", DataType::Float32),
        ("f64", DataType::Float64),
    ]
    .into_iter()
    .flat_map(|width| {
        [
            ("file", file_mode_config()),
            ("memory", memory_mode_config()),
        ]
        .map(|mode| (width.clone(), mode))
    }) {
        let keys: ArrayRef = match data_type {
            DataType::Float16 => Arc::new(
                values
                    .iter()
                    .map(|v| v.map(F16::from_f64))
                    .collect::<PrimitiveArray<Float16Type>>(),
            ),
            DataType::Float64 => Arc::new(values.iter().copied().collect::<Float64Array>()),
            #[expect(clippy::cast_possible_truncation, reason = "narrowed on purpose")]
            _ => Arc::new(
                values
                    .iter()
                    .map(|v| v.map(|v| v as f32))
                    .collect::<Float32Array>(),
            ),
        };
        let schema = Arc::new(Schema::new(vec![
            Field::new("AutoId", DataType::Int64, false),
            Field::new("K", data_type.clone(), true),
        ]));
        let ids = Int64Array::from_iter_values(0..i64::try_from(ROWS).expect("fits"));
        let batch =
            RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(ids), keys]).expect("batch");
        let (indexed_name, plain_name) = (
            format!("float_{suffix}_{mode}"),
            format!("plain_{suffix}_{mode}"),
        );
        let indexed = open_table(
            &fixture,
            Arc::clone(&runtime_env),
            TableSpec::new(&indexed_name, Arc::clone(&schema), &[&["K"]]).config(config.clone()),
        )
        .await;
        let plain = open_table(
            &fixture,
            Arc::clone(&runtime_env),
            TableSpec::new(&plain_name, schema, &[]).config(config),
        )
        .await;
        overwrite(&indexed, vec![batch.clone()]).await;
        overwrite(&plain, vec![batch]).await;

        let mut probes: Vec<String> = [
            "0.0", "-0.0", "1.5", "-2.5", "0.1", "65504.0", "-65504.0", "6.0e-8", "2.5", "7.0",
        ]
        .iter()
        .map(|v| format!("arrow_cast({v}, '{data_type}')"))
        .collect();
        probes.extend(
            ["NaN", "Infinity", "-Infinity"]
                .iter()
                .map(|v| format!("arrow_cast('{v}', '{data_type}')")),
        );
        let before = counters(&indexed);
        for probe in &probes {
            let lookup = format!("SELECT \"AutoId\" FROM {{t}} WHERE \"K\" = {probe}");
            assert_eq!(
                rendered(
                    &query(
                        &indexed,
                        &indexed_name,
                        &lookup.replace("{t}", &indexed_name)
                    )
                    .await
                ),
                rendered(&query(&plain, &plain_name, &lookup.replace("{t}", &plain_name)).await),
                "{data_type}, {mode} mode: {lookup}"
            );
        }
        let after = counters(&indexed);
        let answered = (after.full - before.full) + (after.partial - before.partial);
        assert_eq!(
            answered,
            probes.len() as u64,
            "{data_type}, {mode} mode: every lookup must be answered from the index: {before:?} -> {after:?}"
        );
    }
}
