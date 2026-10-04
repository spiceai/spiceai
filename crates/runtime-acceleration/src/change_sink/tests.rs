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

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use arrow::array::{
    ArrayRef, Float32Array, Float64Array, Int64Array, ListArray, RecordBatch, StringArray,
    StructArray,
};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow_tools::schema_evolution::WideningPlan;
use async_trait::async_trait;
use data_components::cdc::{ChangeBatch as SourceBatch, changes_schema};
use datafusion::common::{Constraint, Constraints};
use datafusion::datasource::{MemTable, TableProvider};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{Expr, col, lit};
use datafusion::sql::TableReference;
use tokio::runtime::Handle;
use tokio::sync::{Notify, Semaphore, oneshot};

use super::provider::{ProviderChangeSinkBackend, refusal::is_before_mutation};
use super::{
    BackendWrite, ChangeBatch, ChangeCapabilities, ChangeSink, ChangeSinkBackend,
    ChangeSinkContext, DurabilityObserver, ReplacementSupport, SchemaEvolutionSupport, SetKey,
    StorageDurability, WriteOptions,
};

const WAIT: Duration = Duration::from_secs(5);

fn scope_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("tenant", DataType::Utf8, false),
        Field::new("region", DataType::Utf8, false),
        Field::new("id", DataType::Int64, false),
    ]))
}

fn scope_row(schema: &SchemaRef, tenant: &str, region: &str, id: i64) -> RecordBatch {
    RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            Arc::new(StringArray::from(vec![tenant])),
            Arc::new(StringArray::from(vec![region])),
            Arc::new(Int64Array::from(vec![id])),
        ],
    )
    .expect("scope row must match its schema")
}

async fn validate_scopes(columns: Vec<usize>, inputs: Vec<(Expr, RecordBatch)>) -> Result<()> {
    let schema = scope_schema();
    let table: Arc<dyn TableProvider> = Arc::new(
        MemTable::try_new(Arc::clone(&schema), vec![vec![]])?.with_constraints(
            Constraints::new_unverified(vec![Constraint::PrimaryKey(columns)]),
        ),
    );
    let context = ChangeSinkContext::new(TableReference::bare("scoped"), table);
    let mut merged: Option<ChangeBatch> = None;
    for (filter, batch) in inputs {
        let scope = SetKey::from_filters(Arc::clone(&schema), &[filter])?;
        let input = ChangeBatch::append_scoped(scope, Arc::clone(&schema), vec![batch])?;
        if let Some(merged) = &mut merged {
            merged
                .merge_append(input)
                .expect("scoped appends must merge");
        } else {
            merged = Some(input);
        }
    }
    ProviderChangeSinkBackend::new(context)
        .apply(
            merged.expect("test needs at least one scope"),
            WriteOptions::default(),
            &SessionContext::new(),
        )
        .await
        .map(|_| ())
}

#[tokio::test]
async fn overlapping_scope_shapes_reject_equal_full_keys() {
    let schema = scope_schema();
    let result = validate_scopes(
        vec![0, 1, 2],
        vec![
            (col("tenant").eq(lit("A")), scope_row(&schema, "A", "B", 7)),
            (col("region").eq(lit("B")), scope_row(&schema, "A", "B", 7)),
        ],
    )
    .await;
    let error = result.expect_err("different overlapping scopes must not share a physical key");
    assert!(is_before_mutation(&error));
}

#[tokio::test]
async fn overlapping_scope_shapes_reject_equal_partial_keys() {
    let schema = scope_schema();
    let result = validate_scopes(
        vec![2],
        vec![
            (col("tenant").eq(lit("A")), scope_row(&schema, "A", "B", 7)),
            (col("region").eq(lit("B")), scope_row(&schema, "A", "B", 7)),
        ],
    )
    .await;
    let error = result.expect_err("membership in both scopes must not authorize a key collision");
    assert!(is_before_mutation(&error));
}

#[tokio::test]
async fn disjoint_scope_shapes_preserve_independent_keys() {
    let schema = scope_schema();
    validate_scopes(
        vec![0, 1, 2],
        vec![
            (
                col("tenant").eq(lit("A")),
                scope_row(&schema, "A", "west", 7),
            ),
            (
                col("tenant").eq(lit("B")),
                scope_row(&schema, "B", "west", 7),
            ),
        ],
    )
    .await
    .expect("distinct tenant keys must remain independent");
}

#[tokio::test]
async fn same_scope_preserves_duplicate_inputs() {
    let schema = scope_schema();
    validate_scopes(
        vec![2],
        vec![
            (
                col("tenant").eq(lit("A")),
                scope_row(&schema, "A", "west", 7),
            ),
            (
                col("tenant").eq(lit("A")),
                scope_row(&schema, "A", "west", 7),
            ),
        ],
    )
    .await
    .expect("duplicates inside the same scope belong to the provider conflict policy");
}

#[tokio::test]
async fn mixed_primary_key_lists_preserve_deletes() {
    use super::provider::cdc::{ChangeOperationType, group_into_sub_batches};

    let cases = [
        ([Some("id"), None, None], vec![vec![0], vec![1], vec![2]]),
        (
            [None, Some("id"), Some("id")],
            vec![vec![0], vec![1], vec![2]],
        ),
        (
            [Some("id"), Some("payload"), Some("payload")],
            vec![vec![0], vec![1], vec![2]],
        ),
        ([Some("id"); 3], vec![vec![0, 2]]),
        ([None; 3], vec![vec![0, 1, 2]]),
    ];
    for (key_names, expected_groups) in cases {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("payload", DataType::Utf8, false),
        ]));
        let initial = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![9, 9, 42])),
                Arc::new(StringArray::from(vec!["old-a", "old-b", "unrelated"])),
            ],
        )
        .expect("initial rows");
        let table = Arc::new(
            data_components::arrow::write::MemTable::try_new(
                Arc::clone(&schema),
                vec![vec![initial]],
            )
            .expect("Arrow accelerator"),
        );
        let mut offsets = vec![0_i32];
        let mut names = Vec::new();
        for name in key_names {
            names.extend(name);
            offsets.push(i32::try_from(names.len()).expect("three keys fit i32"));
        }
        let keys = ListArray::try_new(
            Arc::new(Field::new("item", DataType::Utf8, false)),
            OffsetBuffer::new(offsets.into()),
            Arc::new(StringArray::from(names)),
            None,
        )
        .expect("row-specific keys");
        let data: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(vec![99, 9, 9])),
            Arc::new(StringArray::from(vec!["absent", "old-a", "old-b"])),
        ];
        let changes = SourceBatch::try_new(
            RecordBatch::try_new(
                Arc::new(changes_schema(&schema)),
                vec![
                    Arc::new(StringArray::from(vec!["d", "d", "d"])),
                    Arc::new(keys),
                    Arc::new(StructArray::new(schema.fields().clone(), data, None)),
                ],
            )
            .expect("CDC rows"),
        )
        .expect("valid CDC batch");
        let groups = group_into_sub_batches(&changes);
        assert!(
            groups
                .iter()
                .all(|(op, _)| *op == ChangeOperationType::Delete)
        );
        assert_eq!(
            groups.into_iter().map(|(_, rows)| rows).collect::<Vec<_>>(),
            expected_groups,
            "key definitions: {key_names:?}",
        );
        let context = SessionContext::new();
        context
            .register_table("target", Arc::clone(&table) as Arc<dyn TableProvider>)
            .expect("register Arrow accelerator");
        let backend = ProviderChangeSinkBackend::new(ChangeSinkContext::new(
            TableReference::bare("target"),
            table,
        ));
        backend
            .apply(ChangeBatch::cdc(changes), WriteOptions::default(), &context)
            .await
            .expect("mixed-key deletes must execute");
        let rows = context
            .sql("SELECT id, payload FROM target")
            .await
            .expect("plan read")
            .collect()
            .await
            .expect("read Arrow accelerator");
        assert_eq!(
            rows.iter().map(RecordBatch::num_rows).sum::<usize>(),
            1,
            "key definitions: {key_names:?}; actual rows: {rows:?}"
        );
        let row = rows
            .iter()
            .find(|batch| batch.num_rows() > 0)
            .expect("one remaining row");
        assert_eq!(
            row.column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("id")
                .value(0),
            42
        );
        assert_eq!(
            row.column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("payload")
                .value(0),
            "unrelated"
        );
    }
}

fn zero_changes(values: ArrayRef) -> (SchemaRef, SourceBatch) {
    let field = Arc::new(Field::new("id", values.data_type().clone(), false));
    let schema = Arc::new(Schema::new(vec![Arc::clone(&field)]));
    let keys = ListArray::try_new(
        Arc::new(Field::new("item", DataType::Utf8, false)),
        OffsetBuffer::new(vec![0_i32, 1, 2].into()),
        Arc::new(StringArray::from(vec!["id", "id"])),
        None,
    )
    .expect("two single-column primary keys");
    let record = RecordBatch::try_new(
        Arc::new(changes_schema(&schema)),
        vec![
            Arc::new(StringArray::from(vec!["d", "u"])),
            Arc::new(keys),
            Arc::new(StructArray::from(vec![(field, values)])),
        ],
    )
    .expect("CDC record must match its transport schema");
    (
        schema,
        SourceBatch::try_new(record).expect("valid CDC batch"),
    )
}

async fn assert_float_primary_key_delete_rejected(values: ArrayRef) {
    let expected_type = values.data_type().to_string();
    let (schema, changes) = zero_changes(values);
    let table: Arc<dyn TableProvider> =
        Arc::new(MemTable::try_new(schema, vec![vec![]]).expect("empty target table"));
    let backend = ProviderChangeSinkBackend::new(ChangeSinkContext::new(
        TableReference::bare("signed_zero"),
        table,
    ));
    let result = backend
        .apply(
            ChangeBatch::cdc(changes),
            WriteOptions::default(),
            &SessionContext::new(),
        )
        .await;
    let Err(DataFusionError::External(error)) = result else {
        panic!("unsupported primary key must return a typed error");
    };
    let Some(data_components::pk_filter_expr::Error::PrimaryKeyTypeNotYetSupported { data_type }) =
        error.downcast_ref::<data_components::pk_filter_expr::Error>()
    else {
        panic!("unexpected primary key error: {error}");
    };
    assert_eq!(data_type, &expected_type);
}

#[tokio::test]
async fn float32_primary_key_delete_is_rejected() {
    for values in [[0.0_f32, -0.0], [-0.0, 0.0]] {
        assert_float_primary_key_delete_rejected(Arc::new(Float32Array::from(values.to_vec())))
            .await;
    }
}

#[tokio::test]
async fn float64_primary_key_delete_is_rejected() {
    for values in [[0.0_f64, -0.0], [-0.0, 0.0]] {
        assert_float_primary_key_delete_rejected(Arc::new(Float64Array::from(values.to_vec())))
            .await;
    }
}

struct GatedBackend {
    started: Arc<Notify>,
    release: Arc<Semaphore>,
    finalized: Arc<AtomicUsize>,
    flushed: AtomicUsize,
}

#[async_trait]
impl ChangeSinkBackend for GatedBackend {
    fn schema(&self) -> SchemaRef {
        scope_schema()
    }

    fn capabilities(&self) -> ChangeCapabilities {
        ChangeCapabilities {
            replacement: ReplacementSupport::Ordered,
            deferred_durability: false,
            deferred_deletes: false,
            schema_evolution: SchemaEvolutionSupport::Restart,
        }
    }

    async fn apply(
        &self,
        _batch: ChangeBatch,
        _options: WriteOptions,
        _context: &SessionContext,
    ) -> Result<BackendWrite> {
        let started = Arc::clone(&self.started);
        let release = Arc::clone(&self.release);
        let finalized = Arc::clone(&self.finalized);
        Ok(BackendWrite {
            changed: true,
            durability: StorageDurability::Durable,
            finalizer: Some(Box::pin(async move {
                started.notify_one();
                release
                    .acquire()
                    .await
                    .map_err(|error| DataFusionError::Execution(error.to_string()))?
                    .forget();
                finalized.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })),
        })
    }

    fn set_durability_observer(&self, _observer: Arc<dyn DurabilityObserver>) {}

    async fn flush(&self) -> Result<()> {
        self.flushed.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn evolve_schema(&self, _plan: &WideningPlan) -> Result<()> {
        Ok(())
    }
}

fn gated_sink() -> (Arc<GatedBackend>, ChangeSink, ChangeBatch) {
    let backend = Arc::new(GatedBackend {
        started: Arc::new(Notify::new()),
        release: Arc::new(Semaphore::new(0)),
        finalized: Arc::new(AtomicUsize::new(0)),
        flushed: AtomicUsize::new(0),
    });
    let selected: Arc<dyn ChangeSinkBackend> = Arc::<GatedBackend>::clone(&backend);
    let sink = ChangeSink::new(selected, SessionContext::new(), &Handle::current(), 2);
    let schema = scope_schema();
    let batch = ChangeBatch::append(Arc::clone(&schema), vec![scope_row(&schema, "A", "B", 7)])
        .expect("valid append");
    (backend, sink, batch)
}

#[tokio::test]
async fn dropped_submission_keeps_finalizer_owned_until_close() {
    let (backend, sink, batch) = gated_sink();
    let submission = sink
        .reserve()
        .await
        .expect("reserve")
        .submit(batch, WriteOptions::default())
        .expect("submit");
    drop(submission);
    tokio::time::timeout(WAIT, backend.started.notified())
        .await
        .expect("finalizer must start");
    let close = sink.begin_close();
    assert!(!close.is_ready());
    assert_eq!(backend.finalized.load(Ordering::SeqCst), 0);
    backend.release.add_permits(1);
    tokio::time::timeout(WAIT, close.wait())
        .await
        .expect("close must finish")
        .expect("close must succeed");
    assert_eq!(backend.finalized.load(Ordering::SeqCst), 1);
    assert_eq!(backend.flushed.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn callback_survives_cancelled_close_waiter() {
    let (backend, sink, batch) = gated_sink();
    let (complete, completion) = oneshot::channel();
    sink.enqueue(
        batch,
        WriteOptions::default(),
        Box::new(move |result| {
            let _ = complete.send(result);
        }),
    )
    .await
    .expect("enqueue");
    tokio::time::timeout(WAIT, backend.started.notified())
        .await
        .expect("finalizer must start");
    {
        let close = sink.close(WAIT);
        tokio::pin!(close);
        assert!(futures::poll!(close).is_pending());
    }
    let drain = sink.begin_close();
    assert!(!drain.is_ready());
    backend.release.add_permits(1);
    tokio::time::timeout(WAIT, completion)
        .await
        .expect("callback must run")
        .expect("callback must not be dropped")
        .expect("publication must succeed");
    tokio::time::timeout(WAIT, drain.wait())
        .await
        .expect("drain must finish")
        .expect("drain must succeed");
    assert_eq!(backend.finalized.load(Ordering::SeqCst), 1);
    assert_eq!(backend.flushed.load(Ordering::SeqCst), 1);
}
