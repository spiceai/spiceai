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

//! Writing a timestamp column whose Arrow timezone is a fixed UTC offset.
//!
//! Arrow permits the timezone of a timestamp to be either an IANA zone name or
//! a fixed offset such as `+00:00`, and Iceberg maps every `timestamptz` column
//! to the offset form. Resolving a timezone by IANA name only rejects the
//! offset form, and the zone-map min/max scalars built during a Vortex file
//! write are constructed on a path that cannot report that failure — so an
//! unresolvable timezone takes down the writer instead of surfacing an error.
//!
//! Regression test for <https://github.com/spiceai/spiceai/issues/12447>.

mod common;

use std::path::Path;
use std::sync::Arc;

use arrow::array::{Array, Int64Array, RecordBatch, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use cayenne::metadata::{CreateTableOptions, VortexConfig};
use cayenne::{CayenneTableProvider, MetadataCatalog};
use common::{BackendType, TestFixture, insert_batch};
use datafusion::datasource::TableProvider;
use datafusion::physical_plan::collect;
use datafusion::prelude::SessionContext;

/// The timezone Iceberg gives every `timestamptz` column: a fixed UTC offset,
/// not an IANA zone name.
const FIXED_OFFSET_TZ: &str = "+00:00";

/// A non-zero fixed offset, so the offset form is covered beyond the
/// zero-offset case that a UTC special case alone would satisfy.
const NONZERO_OFFSET_TZ: &str = "+05:30";

/// A negative fixed offset, covering the sign the positive cases cannot.
const NEGATIVE_OFFSET_TZ: &str = "-05:30";

/// An IANA zone name, as a control — resolving the offset form must not come at
/// the cost of resolving named zones.
const IANA_TZ: &str = "UTC";

/// Rows per write. Small on purpose: `inline_max_rows: 0` is what forces the
/// file write, so the row count does not have to clear an inlining threshold.
const ROW_COUNT: usize = 256;

/// Every Nth row of the nullable column is NULL, so the write covers a nullable
/// fixed-offset column and the round-trip has nulls to preserve.
const NULL_EVERY: usize = 4;

/// 2023-11-14T22:13:20Z, an arbitrary fixed instant.
const BASE_MICROS: i64 = 1_700_000_000_000_000;

/// One timestamp per second, so min and max are distinct and the zone map has a
/// real range to record.
const STEP_MICROS: i64 = 1_000_000;

fn test_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(
            "event_time",
            DataType::Timestamp(TimeUnit::Microsecond, Some(FIXED_OFFSET_TZ.into())),
            false,
        ),
        Field::new(
            "event_time_offset",
            DataType::Timestamp(TimeUnit::Microsecond, Some(NONZERO_OFFSET_TZ.into())),
            false,
        ),
        Field::new(
            "event_time_negative_offset",
            DataType::Timestamp(TimeUnit::Microsecond, Some(NEGATIVE_OFFSET_TZ.into())),
            false,
        ),
        Field::new(
            "event_time_named",
            DataType::Timestamp(TimeUnit::Microsecond, Some(IANA_TZ.into())),
            false,
        ),
        Field::new(
            "event_time_nullable",
            DataType::Timestamp(TimeUnit::Microsecond, Some(FIXED_OFFSET_TZ.into())),
            true,
        ),
    ]))
}

fn expected_micros(rows: usize) -> Vec<i64> {
    let row_count = i64::try_from(rows).expect("row count fits i64");
    (0..row_count)
        .map(|i| BASE_MICROS + i * STEP_MICROS)
        .collect()
}

/// The nullable column: the same instants, with every `NULL_EVERY`th row NULL.
fn expected_nullable_micros(rows: usize) -> Vec<Option<i64>> {
    expected_micros(rows)
        .into_iter()
        .enumerate()
        .map(|(i, micros)| (i % NULL_EVERY != 0).then_some(micros))
        .collect()
}

fn timestamp_batch(rows: usize) -> RecordBatch {
    let row_count = i64::try_from(rows).expect("row count fits i64");
    let ids: Vec<i64> = (0..row_count).collect();
    let micros = expected_micros(rows);
    RecordBatch::try_new(
        test_schema(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(
                TimestampMicrosecondArray::from(micros.clone()).with_timezone(FIXED_OFFSET_TZ),
            ),
            Arc::new(
                TimestampMicrosecondArray::from(micros.clone()).with_timezone(NONZERO_OFFSET_TZ),
            ),
            Arc::new(
                TimestampMicrosecondArray::from(micros.clone()).with_timezone(NEGATIVE_OFFSET_TZ),
            ),
            Arc::new(TimestampMicrosecondArray::from(micros).with_timezone(IANA_TZ)),
            Arc::new(
                TimestampMicrosecondArray::from(expected_nullable_micros(rows))
                    .with_timezone(FIXED_OFFSET_TZ),
            ),
        ],
    )
    .expect("build timestamp batch")
}

async fn create_table(fixture: &TestFixture, table_name: &str) -> Arc<CayenneTableProvider> {
    let options = CreateTableOptions {
        table_name: table_name.to_string(),
        schema: test_schema(),
        primary_key: vec![],
        on_conflict: None,
        base_path: fixture.data_path.to_string_lossy().to_string(),
        partition_column: None,
        // `inline_max_rows: 0` sends every write to a Vortex file instead of
        // inlining it into the metastore. The zone-map min/max scalars are only
        // built on the file path, so without this the test could pass purely on
        // the inline memtable and prove nothing.
        vortex_config: VortexConfig {
            inline_max_rows: 0,
            ..VortexConfig::default()
        },
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

fn timestamp_array<'a>(batch: &'a RecordBatch, name: &str) -> &'a TimestampMicrosecondArray {
    let index = batch
        .schema()
        .index_of(name)
        .unwrap_or_else(|_| panic!("column {name} present"));
    batch
        .column(index)
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .unwrap_or_else(|| panic!("column {name} is TimestampMicrosecondArray"))
}

/// Values of a non-nullable timestamp column.
fn timestamp_values(batch: &RecordBatch, name: &str) -> Vec<i64> {
    timestamp_array(batch, name).values().to_vec()
}

/// Values of a timestamp column, preserving nulls — `values()` alone would read
/// the backing buffer and silently report a null slot as whatever it holds.
fn nullable_timestamp_values(batch: &RecordBatch, name: &str) -> Vec<Option<i64>> {
    let array = timestamp_array(batch, name);
    (0..array.len())
        .map(|i| (!array.is_null(i)).then(|| array.value(i)))
        .collect()
}

fn scanned_type(batch: &RecordBatch, name: &str, tz: &str) -> (DataType, DataType) {
    let actual = batch
        .schema()
        .field_with_name(name)
        .unwrap_or_else(|_| panic!("{name} in scanned schema"))
        .data_type()
        .clone();
    (
        actual,
        DataType::Timestamp(TimeUnit::Microsecond, Some(tz.into())),
    )
}

#[tokio::test]
async fn fixed_offset_timezone_column_survives_a_vortex_file_write() {
    let fixture = TestFixture::new(BackendType::Sqlite)
        .await
        .expect("fixture");
    let table = create_table(&fixture, "fixed_offset_tz").await;

    // Before the timezone was resolvable in offset form this write aborted the
    // Vortex writer while building the zone map, rather than returning an error.
    let written = insert_batch(&table, timestamp_batch(ROW_COUNT))
        .await
        .expect("insert timestamptz batch");
    assert_eq!(
        written,
        u64::try_from(ROW_COUNT).expect("row count fits u64"),
        "insert row count"
    );

    // Validity gate: the rows must have reached the Vortex file writer. The
    // zone-map scalars are only built on a file write, so without a `.vortex`
    // file on disk this test would prove nothing.
    assert!(
        vortex_bytes_under(&fixture.data_path) > 0,
        "expected at least one .vortex file, so the file-write path actually ran"
    );

    // Correctness: every row round-trips, in every timezone spelling.
    let rows = query(
        &table,
        "fixed_offset_tz",
        "SELECT id, event_time, event_time_offset, event_time_negative_offset, \
         event_time_named, event_time_nullable \
         FROM fixed_offset_tz ORDER BY id",
    )
    .await;
    let total: usize = rows.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total, ROW_COUNT, "scan row count");

    let expected = expected_micros(ROW_COUNT);
    for column in [
        "event_time",
        "event_time_offset",
        "event_time_negative_offset",
        "event_time_named",
    ] {
        let actual: Vec<i64> = rows
            .iter()
            .flat_map(|batch| timestamp_values(batch, column))
            .collect();
        assert_eq!(
            actual, expected,
            "{column} timestamps must round-trip unchanged"
        );
    }

    // Nulls in a fixed-offset column must survive as nulls, not as whatever the
    // backing buffer happens to hold.
    let actual_nullable: Vec<Option<i64>> = rows
        .iter()
        .flat_map(|batch| nullable_timestamp_values(batch, "event_time_nullable"))
        .collect();
    assert_eq!(
        actual_nullable,
        expected_nullable_micros(ROW_COUNT),
        "nullable fixed-offset timestamps must round-trip with nulls intact"
    );

    // The scanned schema must still carry a timezone rather than a naive
    // timestamp, since a naive column would resolve trivially and sidestep the
    // path under test. This reads the schema the scan reports, so it pins the
    // fixture's types rather than independently re-reading the file footer.
    for (column, tz) in [
        ("event_time", FIXED_OFFSET_TZ),
        ("event_time_offset", NONZERO_OFFSET_TZ),
        ("event_time_negative_offset", NEGATIVE_OFFSET_TZ),
        ("event_time_named", IANA_TZ),
        ("event_time_nullable", FIXED_OFFSET_TZ),
    ] {
        let (actual, expected_type) = scanned_type(&rows[0], column, tz);
        assert_eq!(
            actual, expected_type,
            "{column} must still carry its timezone, not a naive timestamp"
        );
    }

    // Aggregating over a fixed-offset column must return the true range, and
    // must ignore nulls in the nullable one. This is a value-correctness check
    // on the scanned data, not an assertion about zone-map pruning.
    let aggregated = query(
        &table,
        "fixed_offset_tz",
        "SELECT min(event_time) AS lo, max(event_time) AS hi, \
         min(event_time_nullable) AS null_lo, max(event_time_nullable) AS null_hi \
         FROM fixed_offset_tz",
    )
    .await;
    let expected_nullable = expected_nullable_micros(ROW_COUNT);
    let present: Vec<i64> = expected_nullable.iter().flatten().copied().collect();
    for (column, want) in [
        ("lo", expected.first().copied()),
        ("hi", expected.last().copied()),
        ("null_lo", present.first().copied()),
        ("null_hi", present.last().copied()),
    ] {
        assert_eq!(
            timestamp_values(&aggregated[0], column).first().copied(),
            want,
            "{column} over a fixed-offset timestamp column"
        );
    }
}
