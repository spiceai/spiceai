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

use std::ops::ControlFlow;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use arrow::array::{
    ArrayRef, AsArray, Float32Array, Float64Array, Int64Array, ListArray, RecordBatch, StringArray,
    StructArray,
};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow_tools::schema_evolution::WideningPlan;
use async_trait::async_trait;
use data_components::cdc::{ChangeBatch as SourceBatch, changes_schema};
use datafusion::common::TableReference;
use datafusion::common::{Constraint, Constraints};
use datafusion::datasource::{MemTable, TableProvider};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{Expr, col, lit};
use tokio::runtime::Handle;
use tokio::sync::{Notify, Semaphore, oneshot};

use super::batching::{
    AppendBurst, AppendIngress, ApplyingGuard, CdcBurst, CdcIngress, CoalescingBurst,
    CoalescingLimits,
};
use super::provider::{ProviderChangeSinkBackend, refusal::is_before_mutation};
use super::source_policy::{CdcPolicy, SchemaDecision};
use super::{
    BackendWrite, ChangeBatch, ChangeCapabilities, ChangeSink, ChangeSinkBackend,
    ChangeSinkContext, DurabilityObserver, Recovery, ReplacementSupport, SchemaEvolutionSupport,
    SetKey, StorageDurability, WriteOptions,
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
            assert!(
                merged.merge_append(input).is_continue(),
                "scoped appends must merge"
            );
        } else {
            merged = Some(input);
        }
    }
    ProviderChangeSinkBackend::new(context)
        .apply(
            merged.expect("test needs at least one scope"),
            WriteOptions::default(),
            &SessionContext::new(),
            Vec::new(),
        )
        .await
        .map(|_| ())
}

#[test]
fn append_merge_preserves_unconsumed_replacement() {
    let schema = scope_schema();
    let mut first = ChangeBatch::append(
        Arc::clone(&schema),
        vec![scope_row(&schema, "A", "west", 1)],
    )
    .expect("first append");
    let row = scope_row(&schema, "B", "east", 2);
    let scope = SetKey::from_filters(Arc::clone(&schema), &[col("tenant").eq(lit("B"))])
        .expect("replacement scope");
    let replacement =
        ChangeBatch::replace_set(scope.clone(), schema, vec![row.clone()]).expect("replacement");
    let ControlFlow::Break(returned) = first.merge_append(replacement) else {
        panic!("replacement must remain separate from the append");
    };
    assert_eq!(first.num_rows(), 1);
    assert_eq!(
        returned
            .replaced_scopes()
            .next()
            .expect("scope retained")
            .filters(),
        scope.filters()
    );
    let super::ChangePayload::Rows { batches, .. } = returned.payload() else {
        panic!("replacement rows retained");
    };
    assert_eq!(batches, &[row]);
}

fn two_input_limits() -> CoalescingLimits {
    CoalescingLimits {
        max_inputs: 2,
        max_bytes: usize::MAX,
        max_age: Duration::ZERO,
    }
}

#[test]
fn append_burst_returns_unconsumed_input_at_capacity() {
    let schema = scope_schema();
    let ingress = Arc::new(AppendIngress::new(
        &TableReference::bare("append"),
        two_input_limits(),
    ));
    let input = |id| {
        ChangeBatch::append(
            Arc::clone(&schema),
            vec![scope_row(&schema, "A", "west", id)],
        )
        .expect("append")
        .with_append_ingress(Arc::clone(&ingress))
        .expect("append lane")
    };
    let mut burst = AppendBurst::new(input(1), WriteOptions::default()).expect("burst");
    assert!(burst.push(input(2), WriteOptions::default()).is_continue());
    let ControlFlow::Break(returned) = burst.push(input(3), WriteOptions::default()) else {
        panic!("third input must remain outside the full burst");
    };
    assert_eq!(burst.len(), 2);
    assert!(Arc::ptr_eq(
        returned.append_ingress().expect("lane retained"),
        &ingress
    ));
    let super::ChangePayload::Rows { batches, .. } = returned.payload() else {
        panic!("append rows retained");
    };
    assert_eq!(batches, &[scope_row(&schema, "A", "west", 3)]);
    let (accepted, _) = burst.finish();
    assert_eq!(accepted.num_rows(), 2);
}

struct UnchangedSchema;

impl CdcPolicy for UnchangedSchema {
    fn classify(
        &self,
        _incoming: &SchemaRef,
        _target: &SchemaRef,
        _capabilities: ChangeCapabilities,
    ) -> Result<SchemaDecision> {
        Ok(SchemaDecision::Proceed)
    }

    fn applied(&self, _plan: &WideningPlan) {}

    fn split_on_schema_change(&self) -> bool {
        true
    }
}

#[test]
fn applying_guard_clears_the_flag_when_dropped() {
    let ingress = Arc::new(CdcIngress::new(
        &TableReference::bare("cdc"),
        Arc::new(UnchangedSchema),
        two_input_limits(),
    ));
    {
        let _guard = ApplyingGuard::enter(&ingress);
        assert!(
            ingress.is_applying(),
            "enter must mark the apply as in flight"
        );
    }
    assert!(
        !ingress.is_applying(),
        "drop must clear the flag so a cancelled apply cannot leave the producer building ahead"
    );
}

#[test]
fn cdc_burst_returns_unconsumed_input_at_capacity() {
    let ingress = Arc::new(CdcIngress::new(
        &TableReference::bare("cdc"),
        Arc::new(UnchangedSchema),
        two_input_limits(),
    ));
    let input = |values| {
        let (_, rows) = zero_changes(Arc::new(Int64Array::from(values)));
        ChangeBatch::cdc_rows(
            data_components::cdc::LazyChangeBatch::ready(rows),
            Arc::clone(&ingress),
        )
    };
    let mut burst = CdcBurst::new(input(vec![1, 2]), WriteOptions::default()).expect("CDC burst");
    assert!(
        burst
            .push(input(vec![3, 4]), WriteOptions::default())
            .is_continue()
    );
    let next = input(vec![5, 6]);
    let record = next.cdc_batch().expect("materialized CDC").record.clone();
    let ControlFlow::Break(returned) = burst.push(next, WriteOptions::default()) else {
        panic!("third input must remain outside the full burst");
    };
    assert_eq!(burst.len(), 2);
    assert_eq!(returned.cdc_batch().expect("CDC retained").record, record);
    let super::ChangePayload::Cdc(rows) = returned.payload() else {
        panic!("CDC payload retained");
    };
    assert!(Arc::ptr_eq(
        rows.ingress().expect("lane retained"),
        &ingress
    ));
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
    assert_eq!(
        error.to_string(),
        "External error: Error during planning: Unique key conflicts between scoped appends for dataset 'scoped'"
    );
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
    assert_eq!(
        error.to_string(),
        "External error: Error during planning: Unique key conflicts between scoped appends for dataset 'scoped'"
    );
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
            .apply(
                ChangeBatch::cdc(changes),
                WriteOptions::default(),
                &context,
                Vec::new(),
            )
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
            Vec::new(),
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
        _positions: Vec<super::PositionWrite>,
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

fn scoped_lane(max_age: Duration) -> Arc<AppendIngress> {
    Arc::new(AppendIngress::new(
        &TableReference::bare("scoped"),
        CoalescingLimits {
            max_inputs: 64,
            max_bytes: usize::MAX,
            max_age,
        },
    ))
}

fn scoped_write(
    schema: &SchemaRef,
    ingress: &Arc<AppendIngress>,
    (tenant, region): (&str, &str),
    ids: &[i64],
    replaces: bool,
) -> ChangeBatch {
    let scope = SetKey::from_filters(
        Arc::clone(schema),
        &[col("tenant").eq(lit(tenant)), col("region").eq(lit(region))],
    )
    .expect("scope");
    let rows = ids
        .iter()
        .map(|&id| scope_row(schema, tenant, region, id))
        .collect();
    if replaces {
        ChangeBatch::replace_set(scope, Arc::clone(schema), rows)
    } else {
        ChangeBatch::append_scoped(scope, Arc::clone(schema), rows)
    }
    .expect("scoped write")
    .with_append_ingress(Arc::clone(ingress))
    .expect("append lane")
}

async fn id_keyed_table(rows: Vec<RecordBatch>) -> Arc<dyn TableProvider> {
    Arc::new(
        data_components::arrow::write::MemTable::try_new(scope_schema(), vec![rows])
            .expect("Arrow accelerator")
            .try_with_constraints(Constraints::new_unverified(vec![Constraint::PrimaryKey(
                vec![2],
            )]))
            .await
            .expect("primary key on id"),
    )
}

async fn stored_rows(table: &Arc<dyn TableProvider>) -> Vec<(String, String, i64)> {
    let batches = SessionContext::new()
        .read_table(Arc::clone(table))
        .expect("read table")
        .collect()
        .await
        .expect("collect table");
    let mut rows = Vec::new();
    for batch in &batches {
        let tenant = batch.column(0).as_string::<i32>();
        let region = batch.column(1).as_string::<i32>();
        let id = batch
            .column(2)
            .as_primitive::<arrow::datatypes::Int64Type>();
        for row in 0..batch.num_rows() {
            rows.push((
                tenant.value(row).to_string(),
                region.value(row).to_string(),
                id.value(row),
            ));
        }
    }
    rows.sort();
    rows
}

#[test]
fn append_burst_admits_only_scopes_no_replacement_can_overlap() {
    let schema = scope_schema();
    let ingress = scoped_lane(Duration::ZERO);
    let options = WriteOptions::default();
    let write = |scope, replaces| scoped_write(&schema, &ingress, scope, &[1], replaces);
    let mut burst = AppendBurst::new(write(("A", "west"), true), options).expect("burst");
    assert!(
        burst
            .push(write(("B", "west"), false), options)
            .is_continue(),
        "an append to a disjoint scope joins"
    );
    assert!(
        burst
            .push(write(("C", "west"), true), options)
            .is_continue(),
        "a disjoint replacement joins"
    );
    for (scope, replaces, why) in [
        (
            ("A", "west"),
            true,
            "a second replacement of a replaced scope",
        ),
        (("C", "west"), false, "an append into a replaced scope"),
        (("B", "west"), true, "a replacement of an appended scope"),
    ] {
        assert!(
            burst.push(write(scope, replaces), options).is_break(),
            "{why} must start the next burst"
        );
    }
    let tenant_only =
        SetKey::from_filters(Arc::clone(&schema), &[col("tenant").eq(lit("D"))]).expect("scope");
    let wider = ChangeBatch::replace_set(
        tenant_only,
        Arc::clone(&schema),
        vec![scope_row(&schema, "D", "east", 1)],
    )
    .expect("replacement")
    .with_append_ingress(Arc::clone(&ingress))
    .expect("append lane");
    assert!(
        burst.push(wider, options).is_break(),
        "a replacement over other grouping columns may overlap the burst"
    );
    let (batch, _) = burst.finish();
    let members = batch
        .scoped_inputs()
        .iter()
        .map(|input| (input.batch_range(), input.replaces()))
        .collect::<Vec<_>>();
    assert_eq!(members, vec![(0..1, true), (1..2, false), (2..3, true)]);
}

/// Forwards to a provider backend and records how writes were grouped.
struct CountingBackend {
    inner: ProviderChangeSinkBackend,
    applies: AtomicUsize,
    most_replaced: AtomicUsize,
}

#[async_trait]
impl ChangeSinkBackend for CountingBackend {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn capabilities(&self) -> ChangeCapabilities {
        self.inner.capabilities()
    }

    async fn apply(
        &self,
        batch: ChangeBatch,
        options: WriteOptions,
        context: &SessionContext,
        positions: Vec<super::PositionWrite>,
    ) -> Result<BackendWrite> {
        self.applies.fetch_add(1, Ordering::SeqCst);
        self.most_replaced
            .fetch_max(batch.replaced_scopes().count(), Ordering::SeqCst);
        self.inner.apply(batch, options, context, positions).await
    }

    fn set_durability_observer(&self, observer: Arc<dyn DurabilityObserver>) {
        self.inner.set_durability_observer(observer);
    }

    async fn flush(&self) -> Result<()> {
        self.inner.flush().await
    }

    async fn evolve_schema(&self, plan: &WideningPlan) -> Result<()> {
        self.inner.evolve_schema(plan).await
    }
}

/// Seeded xorshift, so a failing history can be replayed from its seed.
struct Xorshift(u64);

impl Xorshift {
    fn below(&mut self, bound: usize) -> usize {
        let mut state = self.0;
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        self.0 = state;
        usize::try_from(state % u64::try_from(bound).expect("bound fits u64"))
            .expect("remainder fits usize")
    }
}

/// Differential: random histories of scoped appends and replacements go
/// through a real owner, which coalesces them, and both each write's outcome and
/// the stored rows must equal the same history applied one write at a time to
/// an in-memory model. Some writes reuse a key another scope holds, which the
/// model refuses exactly when that scope still holds it at the write's turn.
#[tokio::test]
async fn coalesced_scoped_writes_match_writes_applied_in_order() {
    const TENANTS: [&str; 8] = ["A", "B", "C", "D", "E", "F", "G", "H"];
    const REGIONS: [&str; 8] = ["n", "s", "e", "w", "ne", "nw", "se", "sw"];
    const WRITES: usize = 300;
    let mut refused_total = 0;
    for seed in 1..=16_u64 {
        let schema = scope_schema();
        let table = id_keyed_table(Vec::new()).await;
        let backend = Arc::new(CountingBackend {
            inner: ProviderChangeSinkBackend::new(ChangeSinkContext::new(
                TableReference::bare("model"),
                Arc::clone(&table),
            ))
            .with_ordered_replacement(),
            applies: AtomicUsize::new(0),
            most_replaced: AtomicUsize::new(0),
        });
        let selected: Arc<dyn ChangeSinkBackend> = Arc::<CountingBackend>::clone(&backend);
        let sink = ChangeSink::new(selected, SessionContext::new(), &Handle::current(), 1024);
        let ingress = scoped_lane(Duration::from_millis(50));
        let mut random = Xorshift(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1);
        let mut model: std::collections::BTreeMap<(usize, usize), Vec<i64>> =
            std::collections::BTreeMap::new();
        let mut expected_outcomes = Vec::with_capacity(WRITES);
        let mut next_id = 0_i64;
        let (done, mut completions) = tokio::sync::mpsc::unbounded_channel();
        for index in 0..WRITES {
            let scope = (random.below(TENANTS.len()), random.below(REGIONS.len()));
            let held = model.get(&scope).cloned().unwrap_or_default();
            // A scope is appended only while it holds nothing, as a proven-fresh fill is.
            let replaces = !held.is_empty() || random.below(3) == 0;
            let mut ids = held
                .iter()
                .copied()
                .filter(|_| replaces && random.below(2) == 0)
                .collect::<Vec<_>>();
            for _ in 0..random.below(3) + usize::from(!replaces) {
                next_id += 1;
                ids.push(next_id);
            }
            // Sometimes reuse a key held by another scope.
            if random.below(6) == 0
                && let Some(foreign) = model
                    .iter()
                    .filter(|(other, _)| **other != scope)
                    .flat_map(|(_, held)| held.iter().copied())
                    .nth(random.below(4))
            {
                ids.push(foreign);
            }
            let conflicts = model
                .iter()
                .any(|(other, held)| *other != scope && held.iter().any(|id| ids.contains(id)));
            expected_outcomes.push(!conflicts);
            if !conflicts {
                if ids.is_empty() {
                    model.remove(&scope);
                } else {
                    model.insert(scope, ids.clone());
                }
            }
            let write = scoped_write(
                &schema,
                &ingress,
                (TENANTS[scope.0], REGIONS[scope.1]),
                &ids,
                replaces,
            );
            let done = done.clone();
            sink.enqueue(
                write,
                WriteOptions {
                    recovery: Recovery::Rebuildable,
                    ..WriteOptions::default()
                },
                Box::new(move |result| {
                    let _ = done.send((index, result.map_err(|error| error.to_string())));
                }),
            )
            .await
            .expect("admitted");
        }
        sink.flush().await.expect("flush");
        let mut outcomes = vec![None; WRITES];
        for _ in 0..WRITES {
            let (index, result) = tokio::time::timeout(WAIT, completions.recv())
                .await
                .expect("every write completes")
                .expect("completion delivered");
            if let Err(error) = &result {
                assert!(
                    error.contains("conflicts with a row outside the declared scope")
                        || error.contains("Unique key conflicts between scoped appends"),
                    "seed {seed}: write {index} failed for another reason: {error}"
                );
            }
            outcomes[index] = Some(result.is_ok());
        }
        let outcomes = outcomes
            .into_iter()
            .map(|outcome| outcome.expect("each write completes once"))
            .collect::<Vec<_>>();
        assert_eq!(
            outcomes, expected_outcomes,
            "seed {seed}: per-write outcomes"
        );
        refused_total += expected_outcomes
            .iter()
            .filter(|applied| !**applied)
            .count();
        let mut expected = model
            .iter()
            .flat_map(|(&(tenant, region), ids)| {
                ids.iter()
                    .map(move |&id| (TENANTS[tenant].to_string(), REGIONS[region].to_string(), id))
            })
            .collect::<Vec<_>>();
        expected.sort();
        assert_eq!(stored_rows(&table).await, expected, "seed {seed}");
        let applies = backend.applies.load(Ordering::SeqCst);
        assert!(
            applies <= WRITES / 2,
            "seed {seed}: {applies} applies for {WRITES} writes; writes did not coalesce"
        );
        assert!(
            backend.most_replaced.load(Ordering::SeqCst) >= 2,
            "seed {seed}: no apply coalesced two replacements"
        );
    }
    assert!(refused_total > 0, "no history exercised a refused write");
}

#[tokio::test]
async fn coalesced_probe_exempts_only_keys_in_replaced_scopes() {
    let schema = scope_schema();
    let table = id_keyed_table(vec![scope_row(&schema, "A", "west", 1)]).await;
    let backend = ProviderChangeSinkBackend::new(ChangeSinkContext::new(
        TableReference::bare("probe"),
        Arc::clone(&table),
    ))
    .with_ordered_replacement();
    let ingress = scoped_lane(Duration::ZERO);
    let options = WriteOptions::default();

    let Err(error) = backend
        .apply(
            scoped_write(&schema, &ingress, ("B", "west"), &[1], false),
            options,
            &SessionContext::new(),
            Vec::new(),
        )
        .await
    else {
        panic!("a key held by an untouched scope conflicts");
    };
    assert!(is_before_mutation(&error), "{error}");
    assert!(
        error
            .to_string()
            .contains("incoming unique key (id) conflicts with a row outside the declared scope"),
        "{error}"
    );
    assert_eq!(
        stored_rows(&table).await,
        vec![("A".to_string(), "west".to_string(), 1)]
    );

    let mut burst = AppendBurst::new(
        scoped_write(&schema, &ingress, ("A", "west"), &[], true),
        options,
    )
    .expect("burst");
    assert!(
        burst
            .push(
                scoped_write(&schema, &ingress, ("B", "west"), &[1], false),
                options
            )
            .is_continue()
    );
    let (batch, options) = burst.finish();
    backend
        .apply(batch, options, &SessionContext::new(), Vec::new())
        .await
        .expect("the held row is deleted before the append");
    assert_eq!(
        stored_rows(&table).await,
        vec![("B".to_string(), "west".to_string(), 1)]
    );
}

/// One ordering of two coalesced writes and how it must end.
struct ArrivalOrderCase {
    initial: Vec<(&'static str, i64)>,
    /// Each write's tenant, ids and whether it replaces its scope.
    writes: Vec<(&'static str, Vec<i64>, bool)>,
    outcomes: Vec<bool>,
    rows: Vec<(&'static str, i64)>,
}

/// Two writes the owner coalesces must end as they would applied in arrival
/// order: a key held by a scope replaced only later still conflicts, and that
/// conflict refuses only its own write.
#[tokio::test]
async fn coalesced_burst_keeps_arrival_order_outcomes() {
    let cases = [
        ArrivalOrderCase {
            initial: vec![("A", 1), ("B", 2)],
            writes: vec![("B", vec![1], true), ("A", vec![], true)],
            outcomes: vec![false, true],
            rows: vec![("B", 2)],
        },
        ArrivalOrderCase {
            initial: vec![("A", 1), ("C", 2)],
            writes: vec![("A", vec![], true), ("B", vec![2], true)],
            outcomes: vec![true, false],
            rows: vec![("C", 2)],
        },
    ];
    for ArrivalOrderCase {
        initial,
        writes,
        outcomes: expected_outcomes,
        rows: expected_rows,
    } in cases
    {
        let schema = scope_schema();
        let table = id_keyed_table(
            initial
                .iter()
                .map(|&(tenant, id)| scope_row(&schema, tenant, "west", id))
                .collect(),
        )
        .await;
        let backend = Arc::new(CountingBackend {
            inner: ProviderChangeSinkBackend::new(ChangeSinkContext::new(
                TableReference::bare("order"),
                Arc::clone(&table),
            ))
            .with_ordered_replacement(),
            applies: AtomicUsize::new(0),
            most_replaced: AtomicUsize::new(0),
        });
        let selected: Arc<dyn ChangeSinkBackend> = Arc::<CountingBackend>::clone(&backend);
        let sink = ChangeSink::new(selected, SessionContext::new(), &Handle::current(), 16);
        let ingress = scoped_lane(Duration::from_millis(200));
        let mut completions = Vec::new();
        for (tenant, ids, replaces) in &writes {
            let (done, completion) = oneshot::channel();
            sink.enqueue(
                scoped_write(&schema, &ingress, (tenant, "west"), ids, *replaces),
                WriteOptions {
                    recovery: Recovery::Rebuildable,
                    ..WriteOptions::default()
                },
                Box::new(move |result| {
                    let _ = done.send(result.is_ok());
                }),
            )
            .await
            .expect("admitted");
            completions.push(completion);
        }
        sink.flush().await.expect("flush");
        let mut outcomes = Vec::new();
        for completion in completions {
            outcomes.push(
                tokio::time::timeout(WAIT, completion)
                    .await
                    .expect("completes")
                    .expect("delivered"),
            );
        }
        assert_eq!(
            backend.most_replaced.load(Ordering::SeqCst),
            2,
            "both replacements must reach the backend as one write"
        );
        assert_eq!(outcomes, expected_outcomes, "writes {writes:?}");
        let expected = expected_rows
            .iter()
            .map(|&(tenant, id)| (tenant.to_string(), "west".to_string(), id))
            .collect::<Vec<_>>();
        assert_eq!(stored_rows(&table).await, expected, "writes {writes:?}");
    }
}
