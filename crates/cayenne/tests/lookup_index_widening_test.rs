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

//! A secondary index over a key column that a schema change widens from
//! `Int32` to `Float64`, as `on_schema_change: sync_all_columns` does when the
//! source column's type changes. Every lookup here runs against the indexed
//! table and against an unindexed twin given the same writes, and the two must
//! return the same rows: before the change, after it, after floats the integers
//! never held (`-0.0`, NaN, `1.5`, values past `i32`) are written, after the
//! index is rebuilt over every file, and after the table is reopened.

#![allow(clippy::expect_used, clippy::unwrap_used)]

mod common;

use common::lookup_index::{
    SplitMix64, TableSpec, counters, file_mode_config, int64_column, memory_mode_config,
    open_table, query, until_covered,
};

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Float64Array, Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow_tools::schema_evolution::{EvolutionContext, SchemaEvolution, classify};

use cayenne::CayenneTableProvider;

use datafusion::datasource::TableProvider;
use datafusion::execution::runtime_env::RuntimeEnv;

const INDEXES: [&[&str]; 2] = [&["K"], &["Tag", "K"]];

fn schema(key: DataType) -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("AutoId", DataType::Int64, false),
        Field::new("K", key, true),
        Field::new("Tag", DataType::Utf8, false),
        Field::new("Payload", DataType::Utf8, false),
    ]))
}

#[derive(Clone, Copy, Debug)]
enum Mode {
    File,
    Memory,
}

/// Creates the table, or reopens it when the catalog already holds one of
/// this name.
async fn open(
    fixture: &common::TestFixture,
    env: &Arc<RuntimeEnv>,
    mode: Mode,
    name: &str,
    key: DataType,
    indexed: bool,
) -> Arc<CayenneTableProvider> {
    let config = match mode {
        Mode::File => file_mode_config(),
        Mode::Memory => memory_mode_config(),
    };
    let keys: &[&[&str]] = if indexed { &INDEXES } else { &[] };
    open_table(
        fixture,
        Arc::clone(env),
        TableSpec::new(name, schema(key), keys).config(config),
    )
    .await
}

/// The integer keys a user's data could hold: the extremes, zero and its
/// neighbours, values repeated across many rows, scattered values, and NULLs.
fn integer_keys() -> Vec<Option<i32>> {
    let mut keys = vec![
        Some(i32::MIN),
        Some(i32::MIN + 1),
        Some(-1_000_000),
        Some(-1),
        Some(0),
        Some(1),
        Some(7),
        Some(42),
        Some(16_777_217),
        Some(1_000_000),
        Some(i32::MAX - 1),
        Some(i32::MAX),
        None,
    ];
    let mut rng = SplitMix64(7);
    for i in 0..6_000 {
        keys.push(match i % 4 {
            // Repeated small values, NULLs among them.
            0 | 1 => {
                let small = i32::try_from(rng.next_u64() % 101).expect("small") - 50;
                (small != 13).then_some(small)
            }
            #[expect(clippy::cast_possible_truncation, reason = "a random i32")]
            _ => Some(rng.next_u64() as i32),
        });
    }
    keys
}

/// Floats an integer column never held, once it is widened, and ones equal to
/// integers it did.
fn float_keys() -> Vec<Option<f64>> {
    vec![
        Some(-0.0),
        Some(0.0),
        Some(f64::NAN),
        Some(-f64::NAN),
        Some(f64::from_bits(0x7FF0_0000_0000_0001)),
        Some(1.5),
        Some(-2.5),
        Some(0.1),
        Some(3.0e9),
        Some(-3.0e9),
        Some(1.0e300),
        Some(f64::INFINITY),
        Some(f64::NEG_INFINITY),
        Some(7.0),
        Some(f64::from(i32::MAX)),
        Some(f64::from(i32::MIN)),
        Some(16_777_217.0),
        None,
    ]
}

fn batch(first_id: i64, keys: ArrayRef) -> RecordBatch {
    let rows = keys.len();
    let ids: Vec<i64> = (0..i64::try_from(rows).expect("fits"))
        .map(|i| first_id + i)
        .collect();
    RecordBatch::try_new(
        schema(keys.data_type().clone()),
        vec![
            Arc::new(Int64Array::from(ids.clone())),
            keys,
            Arc::new(StringArray::from(
                ids.iter()
                    .map(|id| format!("t{}", id % 3))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                ids.iter().map(|id| format!("p{id}")).collect::<Vec<_>>(),
            )),
        ],
    )
    .expect("batch")
}

async fn write(tables: &[&Arc<CayenneTableProvider>], batch: &RecordBatch) {
    for table in tables {
        common::insert_batches(table, vec![batch.clone()])
            .await
            .expect("write");
    }
}

/// The ids `sql` returns from `table`, registered as `t`, sorted.
async fn ids<T: TableProvider + 'static>(table: &Arc<T>, sql: &str) -> Vec<i64> {
    let mut ids = int64_column(&query(table, "t", sql).await);
    ids.sort_unstable();
    ids
}

/// A float as a SQL literal of type `DOUBLE`.
fn double(value: f64) -> String {
    if value.is_nan() {
        "CAST('NaN' AS DOUBLE)".to_string()
    } else if value.is_infinite() {
        format!(
            "CAST('{}Infinity' AS DOUBLE)",
            if value < 0.0 { "-" } else { "" }
        )
    } else {
        format!("CAST({value:?} AS DOUBLE)")
    }
}

/// The lookups to compare: each probe value alone, under the compound key, and
/// in an `IN` list with values on either side of it.
fn lookups(probes: &[String]) -> Vec<String> {
    let mut sql = Vec::new();
    for (i, probe) in probes.iter().enumerate() {
        sql.push(format!("SELECT \"AutoId\" FROM t WHERE \"K\" = {probe}"));
        sql.push(format!(
            "SELECT \"AutoId\" FROM t WHERE \"Tag\" = 't{}' AND \"K\" = {probe}",
            i % 3
        ));
        let next = &probes[(i + 1) % probes.len()];
        sql.push(format!(
            "SELECT \"AutoId\" FROM t WHERE \"K\" IN ({probe}, {next})"
        ));
    }
    sql
}

/// Every row written so far, with its key in `key`'s type, as an in-memory
/// table: the engine's own answer to a lookup, independent of Cayenne.
fn oracle(written: &[RecordBatch], key: &DataType) -> Arc<datafusion::datasource::MemTable> {
    let target = schema(key.clone());
    let batches: Vec<RecordBatch> = written
        .iter()
        .map(|batch| {
            let mut columns = batch.columns().to_vec();
            columns[1] = arrow::compute::cast(&columns[1], key).expect("cast key");
            RecordBatch::try_new(Arc::clone(&target), columns).expect("oracle batch")
        })
        .collect();
    Arc::new(datafusion::datasource::MemTable::try_new(target, vec![batches]).expect("oracle"))
}

/// Runs every lookup on the indexed table, the unindexed one and the in-memory
/// `oracle`, and fails on the first that disagrees. The indexed table must
/// match the unindexed one on every lookup, so the index never changes an
/// answer, and the engine's answer on every lookup except an equality on NaN,
/// which an unindexed Cayenne table also answers wrongly once the rows reach a
/// file (its statistics leave NaN out, so the scan is pruned away). Returns how
/// many lookups the index answered, so a caller can check that the comparison
/// exercised the index rather than two scans.
async fn compare(
    stage: &str,
    indexed: &Arc<CayenneTableProvider>,
    plain: &Arc<CayenneTableProvider>,
    oracle: &Arc<datafusion::datasource::MemTable>,
    probes: &[String],
) -> u64 {
    let before = counters(indexed);
    let mut rows = 0;
    for sql in lookups(probes) {
        let expected = ids(plain, &sql).await;
        let got = ids(indexed, &sql).await;
        assert_eq!(got, expected, "{stage}: {sql}");
        if !sql.contains("'NaN'") {
            assert_eq!(
                got,
                ids(oracle, &sql).await,
                "{stage}, against the engine: {sql}"
            );
        }
        rows += expected.len();
    }
    let after = counters(indexed);
    let answered = (after.full - before.full) + (after.partial - before.partial);
    println!(
        "{stage}: {} lookups returned {rows} rows, {answered} answered by the index",
        lookups(probes).len()
    );
    answered
}

/// Every integer probe: each distinct key the data holds, as an integer
/// literal and as a `DOUBLE`, and integers it does not hold.
fn integer_probes(keys: &[Option<i32>]) -> Vec<String> {
    let mut distinct: Vec<i32> = keys.iter().flatten().copied().collect();
    distinct.sort_unstable();
    distinct.dedup();
    // Every extreme and small value, and a sample of the scattered ones.
    let mut probes: Vec<i32> = distinct
        .iter()
        .copied()
        .filter(|v| v.unsigned_abs() <= 1_000_000 || v.unsigned_abs() >= (i32::MAX as u32) - 1)
        .collect();
    probes.extend(distinct.iter().step_by(97).copied());
    probes.extend([2, -7_777_777, 13, 16_777_216]);
    probes.sort_unstable();
    probes.dedup();
    probes
        .iter()
        .flat_map(|&v| [v.to_string(), double(f64::from(v))])
        .collect()
}

fn float_probes() -> Vec<String> {
    let mut probes: Vec<String> = float_keys().into_iter().flatten().map(double).collect();
    probes.extend(["2.5", "-0.0", "0", "1.0e-300"].map(str::to_string));
    probes
}

fn widening_plan(table: &Arc<CayenneTableProvider>) -> arrow_tools::schema_evolution::WideningPlan {
    match classify(
        table.schema().as_ref(),
        &schema(DataType::Float64),
        &EvolutionContext {
            constraint_columns: &[],
        },
    ) {
        SchemaEvolution::Widening(plan) => plan,
        other => panic!("Int32 -> Float64 must be a widening: {other:?}"),
    }
}

async fn widened_keys_return_the_rows_of_an_unindexed_table(mode: Mode) {
    let fixture = common::TestFixture::new(common::BackendType::Sqlite)
        .await
        .expect("fixture");
    let env = Arc::new(RuntimeEnv::default());
    let indexed = open(&fixture, &env, mode, "indexed", DataType::Int32, true).await;
    let plain = open(&fixture, &env, mode, "plain", DataType::Int32, false).await;

    let keys = integer_keys();
    let half = keys.len() / 2;
    let mut written = vec![
        batch(0, Arc::new(Int32Array::from(keys[..half].to_vec()))),
        batch(
            i64::try_from(half).expect("fits"),
            Arc::new(Int32Array::from(keys[half..].to_vec())),
        ),
    ];
    for batch in &written {
        write(&[&indexed, &plain], batch).await;
    }
    let int_probes = integer_probes(&keys);
    let truth = oracle(&written, &DataType::Int32);
    let answered = compare("Int32", &indexed, &plain, &truth, &int_probes).await;
    assert!(answered > 0, "the Int32 lookups never used the index");

    for table in [&indexed, &plain] {
        // The first write after a live widening can panic while statistics of
        // the writes before it are still pending (#14718); persist them first,
        // so this test exercises the index rather than that.
        table
            .flush_pending_maintenance()
            .await
            .expect("flush pending statistics");
        table
            .evolve_schema_live(&widening_plan(table))
            .await
            .expect("widen K to Float64");
    }
    let truth = oracle(&written, &DataType::Float64);
    compare("widened", &indexed, &plain, &truth, &int_probes).await;

    let floats = float_keys();
    let first = i64::try_from(keys.len()).expect("fits");
    written.push(batch(first, Arc::new(Float64Array::from(floats.clone()))));
    write(&[&indexed, &plain], written.last().expect("written")).await;
    let truth = oracle(&written, &DataType::Float64);
    let mut probes = int_probes.clone();
    probes.extend(float_probes());
    compare("floats written", &indexed, &plain, &truth, &probes).await;
    // An integer range over the widened column holds the floats between its
    // bounds, which no enumeration of the integers in it would probe.
    for sql in [
        "SELECT \"AutoId\" FROM t WHERE \"K\" BETWEEN 1 AND 3",
        "SELECT \"AutoId\" FROM t WHERE \"K\" BETWEEN -2 AND 2",
        "SELECT \"AutoId\" FROM t WHERE \"K\" IN (1, 2, 3)",
    ] {
        assert_eq!(
            ids(&indexed, sql).await,
            ids(&plain, sql).await,
            "floats written: {sql}"
        );
    }

    if matches!(mode, Mode::File) {
        until_covered(&indexed, async || {
            ids(&indexed, "SELECT \"AutoId\" FROM t WHERE \"K\" = 7").await;
        })
        .await;
    }
    let answered = compare("index rebuilt", &indexed, &plain, &truth, &probes).await;
    assert!(answered > 0, "the Float64 lookups never used the index");

    if matches!(mode, Mode::File) {
        drop((indexed, plain));
        let indexed = open(&fixture, &env, mode, "indexed", DataType::Float64, true).await;
        let plain = open(&fixture, &env, mode, "plain", DataType::Float64, false).await;
        compare("reopened", &indexed, &plain, &truth, &probes).await;
        until_covered(&indexed, async || {
            ids(&indexed, "SELECT \"AutoId\" FROM t WHERE \"K\" = 7").await;
        })
        .await;
        let answered = compare("reopened and rebuilt", &indexed, &plain, &truth, &probes).await;
        assert!(answered > 0, "the reopened lookups never used the index");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn file_mode_widened_keys_return_the_rows_of_an_unindexed_table() {
    widened_keys_return_the_rows_of_an_unindexed_table(Mode::File).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn memory_mode_widened_keys_return_the_rows_of_an_unindexed_table() {
    widened_keys_return_the_rows_of_an_unindexed_table(Mode::Memory).await;
}
