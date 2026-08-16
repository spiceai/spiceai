/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

#![allow(clippy::expect_used)]

//! A Cayenne table stores the timestamp unit its source reports.
//!
//! Vortex represents second, millisecond, microsecond and nanosecond timestamps, so
//! a table created for a `Timestamp(ns, "UTC")` column — what every `PostgreSQL`
//! `timestamptz` infers as — stores nanoseconds, and a value carrying a
//! sub-microsecond remainder survives a write and read back unchanged. Units may
//! also be mixed within one table.
//!
//! Regression test for <https://github.com/spiceai/spiceai/issues/13018>.

mod common;

use std::path::Path;
use std::sync::Arc;

use arrow::array::{
    Int64Array, RecordBatch, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use cayenne::metadata::{CreateTableOptions, VortexConfig};
use cayenne::{CayenneTableProvider, MetadataCatalog};
use common::{BackendType, TestFixture, insert_batch};
use datafusion::datasource::TableProvider;
use datafusion::physical_plan::collect;
use datafusion::prelude::SessionContext;

const TZ: &str = "UTC";

/// 2023-11-14T22:13:20.123456789Z. The `789` tail is the part a microsecond
/// column cannot hold, so it is what makes the round-trip evidence rather than
/// decoration.
const BASE_NANOS: i64 = 1_700_000_000_123_456_789;

/// Roughly a second, plus a tail that is not a whole microsecond, so no two rows
/// share a sub-microsecond remainder and truncation cannot go unnoticed.
const STEP_NANOS: i64 = 1_000_000_137;

/// Small on purpose: `inline_max_rows: 0` is what forces the Vortex file write, so
/// the row count does not have to clear an inlining threshold.
const ROW_COUNT: usize = 64;

const NANOS_PER_MICRO: i64 = 1_000;
const NANOS_PER_MILLI: i64 = 1_000_000;
const NANOS_PER_SECOND: i64 = 1_000_000_000;

/// Both nanosecond spellings, and a microsecond column beside them so the test
/// also covers a table holding more than one timestamp unit at once.
fn test_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(
            "ts_ns",
            DataType::Timestamp(TimeUnit::Nanosecond, Some(TZ.into())),
            false,
        ),
        Field::new(
            "ts_ns_naive",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new(
            "ts_us",
            DataType::Timestamp(TimeUnit::Microsecond, Some(TZ.into())),
            false,
        ),
        Field::new(
            "ts_ms",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("ts_s", DataType::Timestamp(TimeUnit::Second, None), false),
    ]))
}

fn expected_nanos(rows: usize) -> Vec<i64> {
    let row_count = i64::try_from(rows).expect("row count fits i64");
    (0..row_count)
        .map(|i| BASE_NANOS + i * STEP_NANOS)
        .collect()
}

/// The coarser columns hold the same instants at their own precision, so each one
/// round-trips against a value the column can actually represent.
fn scaled(nanos: &[i64], divisor: i64) -> Vec<i64> {
    nanos.iter().map(|n| n / divisor).collect()
}

fn timestamp_batch(rows: usize) -> RecordBatch {
    let row_count = i64::try_from(rows).expect("row count fits i64");
    let ids: Vec<i64> = (0..row_count).collect();
    let nanos = expected_nanos(rows);
    RecordBatch::try_new(
        test_schema(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(TimestampNanosecondArray::from(nanos.clone()).with_timezone(TZ)),
            Arc::new(TimestampNanosecondArray::from(nanos.clone())),
            Arc::new(
                TimestampMicrosecondArray::from(scaled(&nanos, NANOS_PER_MICRO))
                    .with_timezone(TZ),
            ),
            Arc::new(TimestampMillisecondArray::from(scaled(
                &nanos,
                NANOS_PER_MILLI,
            ))),
            Arc::new(TimestampSecondArray::from(scaled(&nanos, NANOS_PER_SECOND))),
        ],
    )
    .expect("build timestamp batch")
}

async fn create_table(
    fixture: &TestFixture,
    table_name: &str,
    vortex_config: VortexConfig,
) -> Arc<CayenneTableProvider> {
    let options = CreateTableOptions {
        table_name: table_name.to_string(),
        schema: test_schema(),
        primary_key: vec![],
        on_conflict: None,
        base_path: fixture.data_path.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config,
    };
    let catalog: Arc<dyn MetadataCatalog> =
        Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
    let ctx = SessionContext::new();
    Arc::new(
        CayenneTableProvider::create_table(catalog, options, ctx.runtime_env())
            .await
            .expect("create table"),
    )
}

/// Sum the sizes of all `.vortex` data files under a table's data directory.
fn vortex_bytes_under(dir: &Path) -> u64 {
    let mut total = 0;
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            total += vortex_bytes_under(&path);
        } else if path.extension().is_some_and(|ext| ext == "vortex") {
            total += entry.metadata().map_or(0, |m| m.len());
        }
    }
    total
}

async fn query(table: &Arc<CayenneTableProvider>, name: &str, sql: &str) -> Vec<RecordBatch> {
    let ctx = SessionContext::new();
    ctx.register_table(name, Arc::clone(table) as Arc<dyn TableProvider>)
        .expect("register table");
    let df = ctx.sql(sql).await.expect("plan query");
    let plan = df.create_physical_plan().await.expect("physical plan");
    collect(plan, ctx.task_ctx()).await.expect("collect rows")
}

/// The i64 values of a timestamp column, whatever its unit, so one helper can
/// check every column against the instants written.
fn timestamp_values(batch: &RecordBatch, name: &str) -> Vec<i64> {
    let index = batch
        .schema()
        .index_of(name)
        .unwrap_or_else(|_| panic!("column {name} present"));
    let column = batch.column(index);
    let unit = match column.data_type() {
        DataType::Timestamp(unit, _) => *unit,
        other => panic!("column {name} is {other:?}, not a timestamp"),
    };
    macro_rules! values {
        ($array:ty) => {
            column
                .as_any()
                .downcast_ref::<$array>()
                .unwrap_or_else(|| panic!("column {name} is a {} array", stringify!($array)))
                .values()
                .to_vec()
        };
    }
    match unit {
        TimeUnit::Nanosecond => values!(TimestampNanosecondArray),
        TimeUnit::Microsecond => values!(TimestampMicrosecondArray),
        TimeUnit::Millisecond => values!(TimestampMillisecondArray),
        TimeUnit::Second => values!(TimestampSecondArray),
    }
}

/// Every timestamp column must carry the exact type it was created with, unit and
/// timezone included. Checked field by field rather than against the whole schema,
/// because a `CayenneTableProvider` advertises a view-typed read schema and may
/// carry metadata the test never set.
fn assert_timestamp_fields_match(actual: &Schema, what: &str) {
    for expected in test_schema().fields() {
        let DataType::Timestamp(..) = expected.data_type() else {
            continue;
        };
        let field = actual
            .field_with_name(expected.name())
            .unwrap_or_else(|_| panic!("{} has a {} column", what, expected.name()));
        assert_eq!(
            field.data_type(),
            expected.data_type(),
            "{what} must keep {}'s unit and timezone",
            expected.name()
        );
    }
}

/// Write one batch, read it back, and check that both the schema and the values
/// survived at the precision they were written with.
async fn assert_timestamp_units_round_trip(vortex_config: VortexConfig, require_vortex_file: bool) {
    let fixture = TestFixture::new(BackendType::Sqlite)
        .await
        .expect("fixture");
    let table_name = "timestamp_units";
    let table = create_table(&fixture, table_name, vortex_config).await;

    // The table's own schema, before any data moves: creation must carry the
    // source's unit through on its own, independently of the write path.
    assert_timestamp_fields_match(&table.schema(), "the created table");

    let written = insert_batch(&table, timestamp_batch(ROW_COUNT))
        .await
        .expect("insert timestamp batch");
    assert_eq!(
        written,
        u64::try_from(ROW_COUNT).expect("row count fits u64"),
        "insert row count"
    );

    // Validity gate: with `inline_max_rows: 0` the rows must have reached the
    // Vortex file writer, so the nanosecond values went through a real encode and
    // decode rather than sitting in the inline memtable. The inline case makes no
    // claim in the other direction — a background checkpoint may drain the
    // memtable to a file at any point, and either way the values must survive.
    if require_vortex_file {
        assert!(
            vortex_bytes_under(&fixture.data_path) > 0,
            "expected at least one .vortex file, so the file-write path actually ran"
        );
    }

    let rows = query(
        &table,
        table_name,
        &format!(
            "SELECT id, ts_ns, ts_ns_naive, ts_us, ts_ms, ts_s FROM {table_name} ORDER BY id"
        ),
    )
    .await;
    let total: usize = rows.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total, ROW_COUNT, "scan row count");

    assert_timestamp_fields_match(&rows[0].schema(), "the scanned data");

    let nanos = expected_nanos(ROW_COUNT);
    // A microsecond column could hold these values unchanged if they happened to
    // land on microsecond boundaries, which would make the nanosecond assertions
    // below pass for the wrong reason.
    assert!(
        nanos.iter().all(|n| n % NANOS_PER_MICRO != 0),
        "every written instant must carry a sub-microsecond remainder"
    );

    for (column, expected) in [
        ("ts_ns", nanos.clone()),
        ("ts_ns_naive", nanos.clone()),
        ("ts_us", scaled(&nanos, NANOS_PER_MICRO)),
        ("ts_ms", scaled(&nanos, NANOS_PER_MILLI)),
        ("ts_s", scaled(&nanos, NANOS_PER_SECOND)),
    ] {
        let actual: Vec<i64> = rows
            .iter()
            .flat_map(|batch| timestamp_values(batch, column))
            .collect();
        assert_eq!(
            actual, expected,
            "{column} must round-trip exactly at its own precision"
        );
    }
}

/// The Vortex file path: the nanosecond values are encoded to disk and read back.
#[tokio::test]
async fn timestamp_units_round_trip_through_a_vortex_file() {
    assert_timestamp_units_round_trip(
        VortexConfig {
            inline_max_rows: 0,
            ..VortexConfig::default()
        },
        true,
    )
    .await;
}

/// The default path, which inlines a write this small into the metastore.
#[tokio::test]
async fn timestamp_units_round_trip_through_the_inline_memtable() {
    assert_timestamp_units_round_trip(VortexConfig::default(), false).await;
}
