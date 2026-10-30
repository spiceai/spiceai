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

//! A seq-prefix bake re-encodes only the input files its deletion filter removes
//! a row from, and links the rest into its output. These tests run the same
//! history through a table that bakes that way and one that rewrites every file,
//! and hold both to a model of what the table must contain.

use super::*;
use arrow::array::StringArray;
use std::collections::BTreeMap;

const PK: [&str; 2] = ["ts", "h"];

/// Rows per checkpoint: about 4 MiB, four 1 MiB files routed on `ts`.
const ROWS_PER_CHECKPOINT: i64 = 20_000;

/// Keys of the previous checkpoint each checkpoint writes again, the newest
/// ones: a replay of recent points, as the metrics workload sends.
const REPLAYED_PER_CHECKPOINT: i64 = 500;

type Model = BTreeMap<(i64, i64), i64>;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("ts", DataType::Int64, false),
        Field::new("h", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
        Field::new("pad", DataType::Utf8, false),
    ]))
}

fn hash_of(ts: i64) -> i64 {
    (ts.wrapping_mul(7_919)).rem_euclid(1_000)
}

/// Incompressible hex, so files stay large enough to split.
fn pad(ts: i64, v: i64) -> String {
    use std::fmt::Write as _;
    let mut x = (ts.unsigned_abs() << 8) ^ v.unsigned_abs() ^ 0x9E37_79B9_7F4A_7C15;
    let mut hex = String::with_capacity(4 * 32);
    for _ in 0..4 {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        write!(hex, "{x:016x}{:016x}", x.rotate_left(29)).expect("write to a String");
    }
    hex
}

fn config() -> VortexConfig {
    VortexConfig {
        cdc_durability: crate::metadata::CdcDurability::Memory,
        deletion_mode: crate::metadata::DeletionMode::Key,
        compaction_background_interval_ms: 0,
        write_concurrency: Some(4),
        target_vortex_file_size_mb: 1,
        ..VortexConfig::default()
    }
}

fn on_conflict() -> OnConflict {
    OnConflict::Upsert(
        datafusion_table_providers::util::column_reference::ColumnReference::new(
            PK.iter().map(|c| (*c).to_string()).collect(),
        ),
    )
}

struct Table {
    provider: CayenneTableProvider,
    catalog: Arc<dyn MetadataCatalog>,
    name: &'static str,
    tmp: TempDir,
}

async fn table(name: &'static str, rewrite_all: bool) -> Table {
    let ctx = SessionContext::new();
    let (provider, catalog, tmp) = create_cdc_table_with_schema(
        name,
        ctx.runtime_env(),
        schema(),
        PK.iter().map(|c| (*c).to_string()).collect(),
        config(),
        on_conflict(),
    )
    .await;
    provider.install_slot_advancer(Arc::new(NoopSlotAdvancer));
    provider.test_bake_files.lock().rewrite_all = rewrite_all;
    Table {
        provider,
        catalog,
        name,
        tmp,
    }
}

/// Open the table again over its catalog and files, as a restart does.
async fn reopen(table: &Table) -> CayenneTableProvider {
    let ctx = SessionContext::new();
    let base_path = format!(
        "{}/data",
        table.tmp.path().to_str().expect("utf-8 temp dir")
    );
    let provider = CayenneTableProviderBuilder::new(Arc::clone(&table.catalog), ctx.runtime_env())
        .create(CreateTableOptions {
            table_name: table.name.to_string(),
            schema: schema(),
            primary_key: PK.iter().map(|c| (*c).to_string()).collect(),
            on_conflict: Some(on_conflict()),
            base_path,
            partition_column: None,
            vortex_config: config(),
        })
        .await
        .expect("table reopened");
    provider.install_slot_advancer(Arc::new(NoopSlotAdvancer));
    provider
}

/// Upsert `(ts, v)` rows into `provider` and checkpoint them.
async fn write_checkpoint(provider: &CayenneTableProvider, rows: &[(i64, i64)]) {
    let batch = RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|(ts, _)| *ts))),
            Arc::new(Int64Array::from_iter_values(
                rows.iter().map(|(ts, _)| hash_of(*ts)),
            )),
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|(_, v)| *v))),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|(ts, v)| pad(*ts, *v)),
            )),
        ],
    )
    .expect("batch");
    let ctx = SessionContext::new();
    provider
        .write_cdc_append_stream(single_batch_stream(batch), &ctx.task_ctx())
        .await
        .expect("CDC upsert")
        .finish()
        .await
        .expect("CDC upsert published");
    provider.checkpoint_mem_tier().await.expect("checkpoint");
}

/// Checkpoint `c`: its own fresh keys at version `c`, and the newest
/// `REPLAYED_PER_CHECKPOINT` keys of checkpoint `c - 1` again at version `c`.
fn checkpoint_rows(c: i64) -> Vec<(i64, i64)> {
    let fresh = c * ROWS_PER_CHECKPOINT;
    let mut rows: Vec<(i64, i64)> = (fresh..fresh + ROWS_PER_CHECKPOINT).map(|ts| (ts, c)).collect();
    if c > 0 {
        rows.extend((fresh - REPLAYED_PER_CHECKPOINT..fresh).map(|ts| (ts, c)));
    }
    rows
}

async fn write_checkpoints(tables: &[&CayenneTableProvider], model: &mut Model, range: std::ops::Range<i64>) {
    for c in range {
        let rows = checkpoint_rows(c);
        for (ts, v) in &rows {
            model.insert((*ts, hash_of(*ts)), *v);
        }
        for provider in tables {
            write_checkpoint(provider, &rows).await;
        }
    }
}

/// Every `(ts, h, v)` the table returns, in key order.
async fn rows_of(provider: &CayenneTableProvider, name: &str) -> Vec<(i64, i64, i64)> {
    let ctx = SessionContext::new();
    ctx.register_table(name, Arc::new(provider.clone_for_write()))
        .expect("table registered");
    let batches = ctx
        .sql(&format!("SELECT ts, h, v FROM {name} ORDER BY ts, h, v"))
        .await
        .expect("query planned")
        .collect()
        .await
        .expect("query ran");
    let mut rows = Vec::new();
    for batch in &batches {
        let column = |i: usize| {
            batch
                .column(i)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("Int64 column")
                .clone()
        };
        let (ts, h, v) = (column(0), column(1), column(2));
        for row in 0..batch.num_rows() {
            rows.push((ts.value(row), h.value(row), v.value(row)));
        }
    }
    rows
}

fn model_rows(model: &Model) -> Vec<(i64, i64, i64)> {
    model.iter().map(|((ts, h), v)| (*ts, *h, *v)).collect()
}

/// Assert the table returns exactly the model's rows, naming the first
/// difference rather than dumping 100k rows.
fn assert_rows(actual: &[(i64, i64, i64)], model: &Model, what: &str) {
    let expected = model_rows(model);
    if let Some(index) = actual
        .iter()
        .zip(&expected)
        .position(|(actual, expected)| actual != expected)
    {
        panic!(
            "{what}: row {index} is {:?}, the model has {:?} ({} rows returned, {} expected)",
            actual[index],
            expected[index],
            actual.len(),
            expected.len()
        );
    }
    assert_eq!(
        actual.len(),
        expected.len(),
        "{what}: {} rows returned, the model has {}",
        actual.len(),
        expected.len()
    );
}

/// The history below, with recent keys replayed and two bakes, gives the same
/// rows from a table that keeps files a bake removes no row from as from one
/// that rewrites every file, and both match the model — including after a
/// restart. The partial table must actually keep files, and still rewrite the
/// ones holding replayed keys, or the comparison proves nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_partial_bake_returns_the_rows_a_full_bake_does() {
    let partial = table("bake_partial_kept", false).await;
    let full = table("bake_partial_full", true).await;
    let mut model = Model::new();

    write_checkpoints(&[&partial.provider, &full.provider], &mut model, 0..6).await;
    assert!(
        partial.provider.pk_deletion_snapshot().delete_len() > 0,
        "precondition: the replays left tombstones"
    );

    for round in 1..=2 {
        for (t, label) in [(&partial, "partial"), (&full, "full")] {
            assert!(
                t.provider
                    .bake_seq_prefix_protected_snapshots()
                    .await
                    .expect("bake"),
                "round {round}: the {label} table's bake commits"
            );
        }
        let (rewritten, kept) = partial
            .provider
            .test_bake_files
            .lock()
            .last
            .expect("the partial table's bake split its inputs");
        assert!(
            rewritten > 0 && kept > 0,
            "round {round}: the bake rewrites the files holding replayed keys and keeps \
             the rest (rewritten {rewritten}, kept {kept})"
        );
        assert_eq!(
            full.provider.test_bake_files.lock().last,
            None,
            "round {round}: the full table's bake rewrote every file"
        );
        assert_eq!(
            partial.provider.pk_deletion_snapshot().delete_len(),
            full.provider.pk_deletion_snapshot().delete_len(),
            "round {round}: both bakes prune the same tombstones"
        );

        let partial_rows = rows_of(&partial.provider, partial.name).await;
        let full_rows = rows_of(&full.provider, full.name).await;
        assert_rows(&full_rows, &model, &format!("round {round}: full bake"));
        assert_rows(&partial_rows, &model, &format!("round {round}: partial bake"));

        write_checkpoints(
            &[&partial.provider, &full.provider],
            &mut model,
            (6 + 4 * (round - 1))..(6 + 4 * round),
        )
        .await;
    }

    assert_rows(
        &rows_of(&partial.provider, partial.name).await,
        &model,
        "after the last writes",
    );
    let reopened = reopen(&partial).await;
    assert_rows(
        &rows_of(&reopened, partial.name).await,
        &model,
        "after a restart",
    );
}

/// The kept files' names are checked before any is linked: a name repeated
/// among them, or already written by the bake, is a collision.
#[test]
fn a_bake_refuses_to_link_two_files_of_one_name() {
    let written: HashSet<String> = ["w2_p000_00000.vortex".to_string()].into();
    assert_eq!(
        first_bake_link_collision(["a.vortex", "b.vortex"], &written),
        None
    );
    assert_eq!(
        first_bake_link_collision(["a.vortex", "b.vortex", "a.vortex"], &written),
        Some("a.vortex")
    );
    assert_eq!(
        first_bake_link_collision(["a.vortex", "w2_p000_00000.vortex"], &written),
        Some("w2_p000_00000.vortex")
    );
}
