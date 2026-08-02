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

use std::{
    any::Any,
    sync::{Arc, Mutex},
};

use arrow::array::{Int32Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, FieldRef, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::{
    catalog::Session,
    datasource::{DefaultTableSource, MemTable, TableProvider, TableType},
    error::{DataFusionError, Result as DataFusionResult},
    logical_expr::{Expr, LogicalPlan, LogicalPlanBuilder, TableProviderFilterPushDown},
    physical_plan::ExecutionPlan,
    prelude::SessionContext,
};
use runtime_datafusion_index::Index;

use super::{CompoundReadMode, CompoundSearchIndex, CompoundVectorIndex, Error};
use crate::index::{SearchIndex, VectorIndex};

/// A [`SearchIndex`]/[`VectorIndex`] test double. Query and list plans scan a `MemTable`
/// built from the configured batches; writes and lifecycle callbacks are recorded into a
/// shared event log tagged with the mock's label.
#[derive(Debug)]
// One flag per behavior this double can be asked to exhibit; a state machine would obscure
// rather than clarify what each test is configuring.
#[expect(clippy::struct_excessive_bools)]
struct MockIndex {
    label: &'static str,
    search_column: String,
    primary_fields: Vec<Field>,
    /// `Some(dimension)` makes `as_vector_index` return `Some`.
    dimension: Option<i32>,
    query_batches: Vec<RecordBatch>,
    list_batches: Vec<RecordBatch>,
    /// Column name appended (as an `Int64` of zeros) to every write output.
    write_output_column: Option<&'static str>,
    /// Number of rows returned from `write` (`None` = same as input).
    write_output_rows: Option<usize>,
    fail_write: bool,
    fail_on_write_start: bool,
    /// What this mock reports from `Index::write_complete_failure_is_fatal`.
    write_complete_fatal: bool,
    /// What this mock reports from `Index::deletes_by_partial_key`.
    deletes_partial_key: bool,
    events: Arc<Mutex<Vec<String>>>,
}

impl MockIndex {
    fn new(label: &'static str, events: &Arc<Mutex<Vec<String>>>) -> Self {
        Self {
            label,
            search_column: "content".to_string(),
            primary_fields: vec![Field::new("id", DataType::Int64, false)],
            dimension: None,
            query_batches: vec![],
            list_batches: vec![],
            write_output_column: None,
            write_output_rows: None,
            fail_write: false,
            fail_on_write_start: false,
            write_complete_fatal: false,
            deletes_partial_key: false,
            events: Arc::clone(events),
        }
    }

    fn record(&self, event: &str) {
        self.events
            .lock()
            .expect("event log mutex")
            .push(format!("{}:{event}", self.label));
    }

    fn plan_over(batches: &[RecordBatch]) -> Result<LogicalPlan, DataFusionError> {
        let schema = batches
            .first()
            .map(RecordBatch::schema)
            .expect("mock plans need at least one (possibly empty) batch");
        let table = PushdownMemTable(MemTable::try_new(schema, vec![batches.to_vec()])?);
        LogicalPlanBuilder::scan(
            "mock",
            Arc::new(DefaultTableSource::new(
                Arc::new(table) as Arc<dyn TableProvider>
            )),
            None,
        )?
        .build()
    }
}

/// A [`MemTable`] that reports [`TableProviderFilterPushDown::Inexact`] support for every
/// filter, so the fallback provider (which delegates its pushdown decision to the primary's
/// source) receives pushed-down filters in tests. `Inexact` means DataFusion re-applies the
/// filters above the scan, so ignoring them in [`TableProvider::scan`] stays correct.
#[derive(Debug)]
struct PushdownMemTable(MemTable);

#[async_trait]
impl TableProvider for PushdownMemTable {
    fn schema(&self) -> SchemaRef {
        self.0.schema()
    }

    fn table_type(&self) -> TableType {
        self.0.table_type()
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        self.0.scan(state, projection, &[], limit).await
    }
}

#[async_trait]
impl Index for MockIndex {
    fn name(&self) -> &'static str {
        "MockIndex"
    }

    fn required_columns(&self) -> Vec<String> {
        vec![self.search_column.clone(), self.label.to_string()]
    }

    async fn on_write_start(&self) -> Result<(), DataFusionError> {
        self.record("on_write_start");
        if self.fail_on_write_start {
            return Err(DataFusionError::Execution(format!(
                "{} refuses to start a write",
                self.label
            )));
        }
        Ok(())
    }

    async fn on_write_failed(&self) -> Result<(), DataFusionError> {
        self.record("on_write_failed");
        Ok(())
    }

    async fn on_write_complete(&self) -> Result<(), DataFusionError> {
        self.record("on_write_complete");
        Ok(())
    }

    async fn delete_by_keys(&self, keys: RecordBatch) -> DataFusionResult<()> {
        self.record(&format!("delete_by_keys:{}", keys.num_rows()));
        Ok(())
    }

    fn deletes_by_partial_key(&self) -> bool {
        self.deletes_partial_key
    }

    fn write_complete_failure_is_fatal(&self) -> bool {
        self.write_complete_fatal
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[async_trait]
impl SearchIndex for MockIndex {
    fn search_column(&self) -> String {
        self.search_column.clone()
    }

    fn primary_fields(&self) -> Vec<Field> {
        self.primary_fields.clone()
    }

    async fn write(
        &self,
        record: RecordBatch,
    ) -> Result<RecordBatch, Box<dyn std::error::Error + Send + Sync>> {
        self.record("write");
        if self.fail_write {
            return Err(Box::from(format!("{} write failed", self.label)));
        }
        let rows = self
            .write_output_rows
            .unwrap_or(record.num_rows())
            .min(record.num_rows());
        let record = record.slice(0, rows);
        let Some(extra) = self.write_output_column else {
            return Ok(record);
        };
        let (schema, mut arrays, _) = record.into_parts();
        let mut fields: Vec<FieldRef> = schema.fields().iter().cloned().collect();
        fields.push(Arc::new(Field::new(extra, DataType::Int64, false)));
        arrays.push(Arc::new(Int64Array::from(vec![0_i64; rows])));
        Ok(RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)?)
    }

    fn query_table_provider(&self, _query: &str) -> Result<Arc<LogicalPlan>, DataFusionError> {
        Ok(Arc::new(Self::plan_over(&self.query_batches)?))
    }

    fn as_vector_index(self: Arc<Self>) -> Option<Arc<dyn VectorIndex>> {
        if self.dimension.is_some() {
            Some(self as Arc<dyn VectorIndex>)
        } else {
            None
        }
    }
}

impl VectorIndex for MockIndex {
    fn list_table_provider(&self) -> Result<LogicalPlan, DataFusionError> {
        Self::plan_over(&self.list_batches)
    }

    fn dimension(&self) -> i32 {
        self.dimension.expect("mock configured as a vector index")
    }
}

fn result_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("source", DataType::Utf8, false),
    ]))
}

fn result_batch(ids: &[i64], source: &str) -> RecordBatch {
    RecordBatch::try_new(
        result_schema(),
        vec![
            Arc::new(Int64Array::from(ids.to_vec())),
            Arc::new(StringArray::from(vec![source; ids.len()])),
        ],
    )
    .expect("valid result batch")
}

fn input_batch(rows: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("content", DataType::Utf8, false),
    ]));
    #[expect(clippy::cast_possible_wrap, reason = "small test row counts")]
    let ids: Vec<i64> = (0..rows as i64).collect();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(vec!["text"; rows])),
        ],
    )
    .expect("valid input batch")
}

async fn collect_sources_and_ids(plan: LogicalPlan) -> (Vec<String>, Vec<i64>) {
    let ctx = SessionContext::new();
    let batches = ctx
        .execute_logical_plan(plan)
        .await
        .expect("plan executes")
        .collect()
        .await
        .expect("plan collects");
    let mut sources = vec![];
    let mut ids = vec![];
    for batch in &batches {
        let id_col = batch
            .column_by_name("id")
            .expect("id column")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int64 ids");
        let source_col = batch
            .column_by_name("source")
            .expect("source column")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("utf8 sources");
        for i in 0..batch.num_rows() {
            ids.push(id_col.value(i));
            sources.push(source_col.value(i).to_string());
        }
    }
    ids.sort_unstable();
    sources.sort();
    sources.dedup();
    (sources, ids)
}

fn compound(
    primary: MockIndex,
    secondary: MockIndex,
    read_mode: CompoundReadMode,
) -> CompoundSearchIndex {
    CompoundSearchIndex::try_new(
        Arc::new(primary) as Arc<dyn SearchIndex>,
        Arc::new(secondary) as Arc<dyn SearchIndex>,
        read_mode,
    )
    .expect("compatible indexes")
}

#[test]
fn try_new_rejects_search_column_mismatch() {
    let events = Arc::new(Mutex::new(vec![]));
    let primary = MockIndex::new("primary", &events);
    let mut secondary = MockIndex::new("secondary", &events);
    secondary.search_column = "other".to_string();

    let err = CompoundSearchIndex::try_new(
        Arc::new(primary),
        Arc::new(secondary),
        CompoundReadMode::PrimaryOnly,
    )
    .expect_err("mismatched search columns must be rejected");
    assert!(matches!(err, Error::SearchColumnMismatch { .. }), "{err}");
}

#[test]
fn try_new_rejects_primary_fields_mismatch() {
    let events = Arc::new(Mutex::new(vec![]));
    let primary = MockIndex::new("primary", &events);
    let mut secondary = MockIndex::new("secondary", &events);
    secondary.primary_fields = vec![Field::new("id", DataType::Int32, false)];

    let err = CompoundSearchIndex::try_new(
        Arc::new(primary),
        Arc::new(secondary),
        CompoundReadMode::PrimaryOnly,
    )
    .expect_err("mismatched primary-key types must be rejected");
    assert!(matches!(err, Error::PrimaryFieldsMismatch { .. }), "{err}");
}

#[test]
fn try_new_accepts_primary_fields_in_different_order() {
    let events = Arc::new(Mutex::new(vec![]));
    let pk_a = vec![
        Field::new("id", DataType::Int64, false),
        Field::new("tenant", DataType::Utf8, false),
    ];
    let pk_b = vec![
        Field::new("tenant", DataType::Utf8, false),
        Field::new("id", DataType::Int64, false),
    ];
    let mut primary = MockIndex::new("primary", &events);
    primary.primary_fields = pk_a;
    let mut secondary = MockIndex::new("secondary", &events);
    secondary.primary_fields = pk_b;

    CompoundSearchIndex::try_new(
        Arc::new(primary),
        Arc::new(secondary),
        CompoundReadMode::PrimaryOnly,
    )
    .expect("field order must not matter for key compatibility");
}

#[test]
fn try_new_rejects_mixed_index_variants() {
    let events = Arc::new(Mutex::new(vec![]));
    let mut primary = MockIndex::new("primary", &events);
    primary.dimension = Some(4);
    let secondary = MockIndex::new("secondary", &events);

    let err = CompoundSearchIndex::try_new(
        Arc::new(primary),
        Arc::new(secondary),
        CompoundReadMode::PrimaryOnly,
    )
    .expect_err("a vector index cannot be compounded with a non-vector index");
    assert!(matches!(err, Error::IndexVariantMismatch { .. }), "{err}");
}

#[test]
fn try_new_rejects_dimension_mismatch() {
    let events = Arc::new(Mutex::new(vec![]));
    let mut primary = MockIndex::new("primary", &events);
    primary.dimension = Some(4);
    let mut secondary = MockIndex::new("secondary", &events);
    secondary.dimension = Some(8);

    let err = CompoundVectorIndex::try_new(
        Arc::new(primary) as Arc<dyn VectorIndex>,
        Arc::new(secondary) as Arc<dyn VectorIndex>,
        CompoundReadMode::PrimaryOnly,
    )
    .expect_err("mismatched embedding dimensions must be rejected");
    assert!(matches!(err, Error::DimensionMismatch { .. }), "{err}");
}

#[test]
fn as_vector_index_yields_compound_vector_index() {
    let events = Arc::new(Mutex::new(vec![]));
    let mut primary = MockIndex::new("primary", &events);
    primary.dimension = Some(4);
    let mut secondary = MockIndex::new("secondary", &events);
    secondary.dimension = Some(4);

    let idx = Arc::new(compound(primary, secondary, CompoundReadMode::PrimaryOnly));
    let vector = (idx as Arc<dyn SearchIndex>)
        .as_vector_index()
        .expect("both sides are vector indexes");
    assert_eq!(vector.dimension(), 4);
}

#[tokio::test]
async fn write_goes_to_both_and_merges_output_columns() {
    let events = Arc::new(Mutex::new(vec![]));
    let mut primary = MockIndex::new("primary", &events);
    primary.write_output_column = Some("primary_derived");
    let mut secondary = MockIndex::new("secondary", &events);
    secondary.write_output_column = Some("secondary_derived");

    let idx = compound(primary, secondary, CompoundReadMode::PrimaryOnly);
    let out = idx.write(input_batch(3)).await.expect("write succeeds");

    assert_eq!(out.num_rows(), 3);
    assert!(out.column_by_name("primary_derived").is_some());
    assert!(
        out.column_by_name("secondary_derived").is_some(),
        "secondary-only output columns must be merged into the result"
    );
    let events = events.lock().expect("event log mutex").clone();
    assert!(events.contains(&"primary:write".to_string()));
    assert!(events.contains(&"secondary:write".to_string()));
}

#[tokio::test]
async fn write_error_names_the_failing_index() {
    let events = Arc::new(Mutex::new(vec![]));
    let primary = MockIndex::new("primary", &events);
    let mut secondary = MockIndex::new("secondary", &events);
    secondary.fail_write = true;

    let idx = compound(primary, secondary, CompoundReadMode::PrimaryOnly);
    let err = idx
        .write(input_batch(2))
        .await
        .expect_err("secondary write failure must propagate");
    assert!(
        err.to_string().contains("secondary index"),
        "error must identify the failing side: {err}"
    );
    // The primary write must still have been driven to completion.
    let events = events.lock().expect("event log mutex").clone();
    assert!(events.contains(&"primary:write".to_string()));
}

#[tokio::test]
async fn write_rejects_row_count_mismatch() {
    let events = Arc::new(Mutex::new(vec![]));
    let primary = MockIndex::new("primary", &events);
    let mut secondary = MockIndex::new("secondary", &events);
    secondary.write_output_rows = Some(1);

    let idx = compound(primary, secondary, CompoundReadMode::PrimaryOnly);
    let err = idx
        .write(input_batch(3))
        .await
        .expect_err("diverging row counts must be rejected");
    assert!(err.to_string().contains("rows"), "{err}");
}

#[tokio::test]
async fn lifecycle_hooks_forward_to_both_indexes() {
    let events = Arc::new(Mutex::new(vec![]));
    let primary = MockIndex::new("primary", &events);
    let secondary = MockIndex::new("secondary", &events);
    let idx = compound(primary, secondary, CompoundReadMode::PrimaryOnly);

    idx.on_write_start().await.expect("start");
    idx.on_write_complete().await.expect("complete");
    idx.on_write_failed().await.expect("failed");

    let events = events.lock().expect("event log mutex").clone();
    for side in ["primary", "secondary"] {
        for event in ["on_write_start", "on_write_complete", "on_write_failed"] {
            assert!(
                events.contains(&format!("{side}:{event}")),
                "missing {side}:{event} in {events:?}"
            );
        }
    }
}

/// A compound index is stale if *either* half fails to finalize, so the fatality flag
/// must be the union of both halves rather than the trait default (#12038).
#[test]
fn write_complete_fatality_is_the_union_of_both_search_halves() {
    let events = Arc::new(Mutex::new(vec![]));

    for (primary_fatal, secondary_fatal, expected) in [
        (false, false, false),
        (true, false, true),
        (false, true, true),
        (true, true, true),
    ] {
        let mut primary = MockIndex::new("primary", &events);
        primary.write_complete_fatal = primary_fatal;
        let mut secondary = MockIndex::new("secondary", &events);
        secondary.write_complete_fatal = secondary_fatal;

        let idx = compound(primary, secondary, CompoundReadMode::PrimaryOnly);
        assert_eq!(
            idx.write_complete_failure_is_fatal(),
            expected,
            "primary_fatal={primary_fatal}, secondary_fatal={secondary_fatal}"
        );
    }
}

#[test]
fn write_complete_fatality_is_the_union_of_both_vector_halves() {
    let events = Arc::new(Mutex::new(vec![]));

    for (primary_fatal, secondary_fatal, expected) in [
        (false, false, false),
        (true, false, true),
        (false, true, true),
        (true, true, true),
    ] {
        let mut primary = MockIndex::new("primary", &events);
        primary.dimension = Some(4);
        primary.write_complete_fatal = primary_fatal;
        let mut secondary = MockIndex::new("secondary", &events);
        secondary.dimension = Some(4);
        secondary.write_complete_fatal = secondary_fatal;

        let idx = CompoundVectorIndex::try_new(
            Arc::new(primary) as Arc<dyn VectorIndex>,
            Arc::new(secondary) as Arc<dyn VectorIndex>,
            CompoundReadMode::PrimaryOnly,
        )
        .expect("compatible vector indexes");

        assert_eq!(
            idx.write_complete_failure_is_fatal(),
            expected,
            "primary_fatal={primary_fatal}, secondary_fatal={secondary_fatal}"
        );
    }
}

/// `delete_by_keys` fans out to both halves, so a partial key only clears the whole compound
/// index when *both* halves delete on one — the intersection, not the trait default.
#[test]
fn partial_key_deletion_requires_both_halves() {
    let events = Arc::new(Mutex::new(vec![]));

    for (primary_partial, secondary_partial, expected) in [
        (false, false, false),
        (true, false, false),
        (false, true, false),
        (true, true, true),
    ] {
        let mock = |label: &'static str, partial: bool| {
            let mut idx = MockIndex::new(label, &events);
            idx.dimension = Some(4);
            idx.deletes_partial_key = partial;
            idx
        };

        let search = compound(
            mock("primary", primary_partial),
            mock("secondary", secondary_partial),
            CompoundReadMode::PrimaryOnly,
        );
        assert_eq!(
            search.deletes_by_partial_key(),
            expected,
            "search: primary={primary_partial}, secondary={secondary_partial}"
        );

        let (primary, secondary) = (
            mock("primary", primary_partial),
            mock("secondary", secondary_partial),
        );
        let vector = CompoundVectorIndex::try_new(
            Arc::new(primary) as Arc<dyn VectorIndex>,
            Arc::new(secondary) as Arc<dyn VectorIndex>,
            CompoundReadMode::PrimaryOnly,
        )
        .expect("compatible vector indexes");
        assert_eq!(
            vector.deletes_by_partial_key(),
            expected,
            "vector: primary={primary_partial}, secondary={secondary_partial}"
        );
    }
}

#[tokio::test]
async fn on_write_start_rolls_back_primary_when_secondary_fails() {
    let events = Arc::new(Mutex::new(vec![]));
    let primary = MockIndex::new("primary", &events);
    let mut secondary = MockIndex::new("secondary", &events);
    secondary.fail_on_write_start = true;

    let idx = compound(primary, secondary, CompoundReadMode::PrimaryOnly);
    idx.on_write_start()
        .await
        .expect_err("secondary start failure must propagate");

    let events = events.lock().expect("event log mutex").clone();
    assert!(
        events.contains(&"primary:on_write_failed".to_string()),
        "primary write window must be rolled back: {events:?}"
    );
}

#[test]
fn required_columns_are_the_union_of_both_indexes() {
    let events = Arc::new(Mutex::new(vec![]));
    let primary = MockIndex::new("primary", &events);
    let secondary = MockIndex::new("secondary", &events);
    let idx = compound(primary, secondary, CompoundReadMode::PrimaryOnly);

    assert_eq!(
        idx.required_columns(),
        vec![
            "content".to_string(),
            "primary".to_string(),
            "secondary".to_string()
        ]
    );
}

#[tokio::test]
async fn query_primary_only_never_reads_secondary() {
    let events = Arc::new(Mutex::new(vec![]));
    let mut primary = MockIndex::new("primary", &events);
    primary.query_batches = vec![RecordBatch::new_empty(result_schema())];
    let mut secondary = MockIndex::new("secondary", &events);
    secondary.query_batches = vec![result_batch(&[1, 2], "secondary")];

    let idx = compound(primary, secondary, CompoundReadMode::PrimaryOnly);
    let plan = idx.query_table_provider("q").expect("plan builds");
    let (sources, ids) = collect_sources_and_ids(Arc::unwrap_or_clone(plan)).await;
    assert!(
        sources.is_empty() && ids.is_empty(),
        "writethrough-without-fallback must return the primary's (empty) results"
    );
}

#[tokio::test]
async fn query_fallback_prefers_primary_when_it_has_results() {
    let events = Arc::new(Mutex::new(vec![]));
    let mut primary = MockIndex::new("primary", &events);
    primary.query_batches = vec![result_batch(&[10, 11], "primary")];
    let mut secondary = MockIndex::new("secondary", &events);
    secondary.query_batches = vec![result_batch(&[1, 2], "secondary")];

    let idx = compound(primary, secondary, CompoundReadMode::FallbackToSecondary);
    let plan = idx.query_table_provider("q").expect("plan builds");
    let (sources, ids) = collect_sources_and_ids(Arc::unwrap_or_clone(plan)).await;
    assert_eq!(sources, vec!["primary".to_string()]);
    assert_eq!(ids, vec![10, 11]);
}

#[tokio::test]
async fn query_fallback_uses_secondary_when_primary_is_empty() {
    let events = Arc::new(Mutex::new(vec![]));
    let mut primary = MockIndex::new("primary", &events);
    primary.query_batches = vec![RecordBatch::new_empty(result_schema())];
    let mut secondary = MockIndex::new("secondary", &events);
    secondary.query_batches = vec![result_batch(&[1, 2], "secondary")];

    let idx = compound(primary, secondary, CompoundReadMode::FallbackToSecondary);
    let plan = idx.query_table_provider("q").expect("plan builds");
    let (sources, ids) = collect_sources_and_ids(Arc::unwrap_or_clone(plan)).await;
    assert_eq!(sources, vec!["secondary".to_string()]);
    assert_eq!(ids, vec![1, 2]);
}

/// A primary that emits only zero-row batches counts as empty — the fallback must drain
/// past them rather than treating a zero-row batch as "results".
#[tokio::test]
async fn query_fallback_ignores_zero_row_primary_batches() {
    let events = Arc::new(Mutex::new(vec![]));
    let mut primary = MockIndex::new("primary", &events);
    primary.query_batches = vec![
        RecordBatch::new_empty(result_schema()),
        RecordBatch::new_empty(result_schema()),
    ];
    let mut secondary = MockIndex::new("secondary", &events);
    secondary.query_batches = vec![result_batch(&[7], "secondary")];

    let idx = compound(primary, secondary, CompoundReadMode::FallbackToSecondary);
    let plan = idx.query_table_provider("q").expect("plan builds");
    let (sources, ids) = collect_sources_and_ids(Arc::unwrap_or_clone(plan)).await;
    assert_eq!(sources, vec!["secondary".to_string()]);
    assert_eq!(ids, vec![7]);
}

/// Secondary result columns with a different (but castable) type are cast to the
/// primary's schema so both sides of the fallback are type-identical.
#[tokio::test]
async fn query_fallback_casts_secondary_columns_to_primary_types() {
    let events = Arc::new(Mutex::new(vec![]));
    let mut primary = MockIndex::new("primary", &events);
    primary.query_batches = vec![RecordBatch::new_empty(result_schema())];

    let secondary_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("source", DataType::Utf8, false),
    ]));
    let secondary_batch = RecordBatch::try_new(
        secondary_schema,
        vec![
            Arc::new(Int32Array::from(vec![5_i32])),
            Arc::new(StringArray::from(vec!["secondary"])),
        ],
    )
    .expect("valid batch");
    let mut secondary = MockIndex::new("secondary", &events);
    secondary.query_batches = vec![secondary_batch];

    let idx = compound(primary, secondary, CompoundReadMode::FallbackToSecondary);
    let plan = idx.query_table_provider("q").expect("plan builds");
    let (sources, ids) = collect_sources_and_ids(Arc::unwrap_or_clone(plan)).await;
    assert_eq!(sources, vec!["secondary".to_string()]);
    assert_eq!(ids, vec![5], "Int32 id must be cast to the primary's Int64");
}

#[test]
fn query_fallback_rejects_secondary_missing_a_primary_column() {
    let events = Arc::new(Mutex::new(vec![]));
    let mut primary = MockIndex::new("primary", &events);
    primary.query_batches = vec![result_batch(&[1], "primary")];

    let secondary_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let mut secondary = MockIndex::new("secondary", &events);
    secondary.query_batches = vec![RecordBatch::new_empty(secondary_schema)];

    let idx = compound(primary, secondary, CompoundReadMode::FallbackToSecondary);
    let err = idx
        .query_table_provider("q")
        .expect_err("secondary missing a primary column must fail at plan time");
    assert!(err.to_string().contains("source"), "{err}");
}

/// Filters pushed into the fallback provider's scan apply to both plans *before* the
/// emptiness decision: a primary that has rows but none matching the filter must fall back
/// to the secondary, and the secondary's rows are filtered too.
#[tokio::test]
async fn query_fallback_applies_pushed_filters_before_deciding() {
    use datafusion::prelude::{col, lit};

    let events = Arc::new(Mutex::new(vec![]));
    let mut primary = MockIndex::new("primary", &events);
    primary.query_batches = vec![result_batch(&[1, 2], "primary")];
    let mut secondary = MockIndex::new("secondary", &events);
    secondary.query_batches = vec![result_batch(&[3, 5, 6], "secondary")];

    let idx = compound(primary, secondary, CompoundReadMode::FallbackToSecondary);
    let plan = idx.query_table_provider("q").expect("plan builds");

    let ctx = SessionContext::new();
    let batches = ctx
        .execute_logical_plan(Arc::unwrap_or_clone(plan))
        .await
        .expect("plan executes")
        .filter(col("id").gt(lit(4_i64)))
        .expect("filters")
        .collect()
        .await
        .expect("collects");

    let mut ids = vec![];
    let mut sources = vec![];
    for batch in &batches {
        let id_col = batch
            .column_by_name("id")
            .expect("id column")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int64 ids");
        let source_col = batch
            .column_by_name("source")
            .expect("source column")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("utf8 sources");
        for i in 0..batch.num_rows() {
            ids.push(id_col.value(i));
            sources.push(source_col.value(i).to_string());
        }
    }
    ids.sort_unstable();
    sources.sort();
    sources.dedup();

    assert_eq!(
        sources,
        vec!["secondary".to_string()],
        "primary has no rows matching the filter, so the (filtered) secondary must serve the query"
    );
    assert_eq!(ids, vec![5, 6], "the secondary's rows must be filtered too");
}

/// Projection and limit pushed into the fallback provider's scan must apply to whichever
/// side ends up serving the query.
#[tokio::test]
async fn query_fallback_supports_projection_and_limit() {
    let events = Arc::new(Mutex::new(vec![]));
    let mut primary = MockIndex::new("primary", &events);
    primary.query_batches = vec![RecordBatch::new_empty(result_schema())];
    let mut secondary = MockIndex::new("secondary", &events);
    secondary.query_batches = vec![result_batch(&[1, 2, 3], "secondary")];

    let idx = compound(primary, secondary, CompoundReadMode::FallbackToSecondary);
    let plan = idx.query_table_provider("q").expect("plan builds");

    let ctx = SessionContext::new();
    let batches = ctx
        .execute_logical_plan(Arc::unwrap_or_clone(plan))
        .await
        .expect("plan executes")
        .select_columns(&["id"])
        .expect("projects")
        .limit(0, Some(2))
        .expect("limits")
        .collect()
        .await
        .expect("collects");

    let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(rows, 2);
    for batch in &batches {
        assert_eq!(batch.num_columns(), 1, "only 'id' was projected");
    }
}

#[tokio::test]
async fn list_fallback_uses_secondary_when_primary_is_empty() {
    let events = Arc::new(Mutex::new(vec![]));
    let mut primary = MockIndex::new("primary", &events);
    primary.dimension = Some(4);
    primary.list_batches = vec![RecordBatch::new_empty(result_schema())];
    let mut secondary = MockIndex::new("secondary", &events);
    secondary.dimension = Some(4);
    secondary.list_batches = vec![result_batch(&[3, 4], "secondary")];

    let idx = CompoundVectorIndex::try_new(
        Arc::new(primary) as Arc<dyn VectorIndex>,
        Arc::new(secondary) as Arc<dyn VectorIndex>,
        CompoundReadMode::FallbackToSecondary,
    )
    .expect("compatible vector indexes");

    let plan = idx.list_table_provider().expect("list plan builds");
    let (sources, ids) = collect_sources_and_ids(plan).await;
    assert_eq!(sources, vec!["secondary".to_string()]);
    assert_eq!(ids, vec![3, 4]);
}

#[tokio::test]
async fn list_primary_only_never_reads_secondary() {
    let events = Arc::new(Mutex::new(vec![]));
    let mut primary = MockIndex::new("primary", &events);
    primary.dimension = Some(4);
    primary.list_batches = vec![result_batch(&[9], "primary")];
    let mut secondary = MockIndex::new("secondary", &events);
    secondary.dimension = Some(4);
    secondary.list_batches = vec![result_batch(&[3, 4], "secondary")];

    let idx = CompoundVectorIndex::try_new(
        Arc::new(primary) as Arc<dyn VectorIndex>,
        Arc::new(secondary) as Arc<dyn VectorIndex>,
        CompoundReadMode::PrimaryOnly,
    )
    .expect("compatible vector indexes");

    let plan = idx.list_table_provider().expect("list plan builds");
    let (sources, ids) = collect_sources_and_ids(plan).await;
    assert_eq!(sources, vec!["primary".to_string()]);
    assert_eq!(ids, vec![9]);
}

fn delete_keys_batch(rows: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    #[expect(clippy::cast_possible_wrap, reason = "small test row counts")]
    let ids: Vec<i64> = (0..rows as i64).collect();
    RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(ids))]).expect("valid keys batch")
}

#[tokio::test]
async fn delete_by_keys_hits_both_primary_and_secondary() {
    let events = Arc::new(Mutex::new(vec![]));
    let primary = MockIndex::new("primary", &events);
    let secondary = MockIndex::new("secondary", &events);
    let idx = compound(primary, secondary, CompoundReadMode::PrimaryOnly);

    idx.delete_by_keys(delete_keys_batch(2))
        .await
        .expect("delete_by_keys succeeds");

    let events = events.lock().expect("event log mutex").clone();
    assert!(events.contains(&"primary:delete_by_keys:2".to_string()));
    assert!(events.contains(&"secondary:delete_by_keys:2".to_string()));
}

#[tokio::test]
async fn vector_delete_by_keys_hits_both_primary_and_secondary() {
    let events = Arc::new(Mutex::new(vec![]));
    let mut primary = MockIndex::new("primary", &events);
    primary.dimension = Some(4);
    let mut secondary = MockIndex::new("secondary", &events);
    secondary.dimension = Some(4);

    let idx = CompoundVectorIndex::try_new(
        Arc::new(primary) as Arc<dyn VectorIndex>,
        Arc::new(secondary) as Arc<dyn VectorIndex>,
        CompoundReadMode::PrimaryOnly,
    )
    .expect("compatible vector indexes");

    idx.delete_by_keys(delete_keys_batch(1))
        .await
        .expect("delete_by_keys succeeds");

    let events = events.lock().expect("event log mutex").clone();
    assert!(events.contains(&"primary:delete_by_keys:1".to_string()));
    assert!(events.contains(&"secondary:delete_by_keys:1".to_string()));
}

/// Exercises the exact composition the runtime builds for `.vectors` datasets and views:
/// a [`MemoryVectorIndex`] warm primary in front of a vector-engine index (stood in for by
/// [`MockIndex`]), in [`CompoundReadMode::FallbackToSecondary`]. Writes go to both indexes;
/// searches and lists are served from memory once it has been written to, and by the
/// engine index while the in-memory index is still empty (e.g. after a restart).
#[cfg(feature = "llms")]
mod warm_memory {
    use super::*;
    use crate::SEARCH_SCORE_COLUMN_NAME;
    use crate::index::memory::{MemoryDistanceMetric, MemoryVectorIndex};
    use crate::metadata::MetadataColumns;
    use arrow::array::{
        FixedSizeListArray, Float32Array, Float32Builder, Float64Array, ListBuilder,
    };
    use datafusion::logical_expr::ColumnarValue;
    use datafusion::scalar::ScalarValue;
    use datafusion_expr::{Volatility, create_udf};
    use llms::embeddings::{Embed, EmbeddingInput};

    const DIM: i32 = 3;

    /// Deterministic, model-free embedder: maps a string to a fixed vector derived from
    /// its byte content.
    #[derive(Debug)]
    struct ByteEmbed;

    fn byte_vector(text: &str) -> Vec<f32> {
        let dim = usize::try_from(DIM).expect("DIM is positive");
        let mut vector = vec![0.0_f32; dim];
        for (i, b) in text.bytes().enumerate() {
            vector[i % dim] += f32::from(b) / 255.0;
        }
        vector
    }

    #[async_trait]
    impl Embed for ByteEmbed {
        async fn embed(&self, input: EmbeddingInput) -> llms::embeddings::Result<Vec<Vec<f32>>> {
            match input {
                EmbeddingInput::String(s) => Ok(vec![byte_vector(&s)]),
                EmbeddingInput::StringArray(v) => Ok(v.iter().map(|s| byte_vector(s)).collect()),
                _ => Ok(vec![]),
            }
        }
        fn size(&self) -> i32 {
            DIM
        }
    }

    /// A DataFusion UDF matching [`ByteEmbed`]/[`byte_vector`], for use as the query-time
    /// `embed(text, model_name)` expression in [`crate::index::memory::MemoryVectorIndex`]'s
    /// score plan. Unlike the write-time [`Embed`] trait, this is actually invoked whenever a
    /// query is executed against a non-empty memory index, so it must produce real vectors
    /// rather than a stub.
    fn embed_udf() -> Arc<datafusion_expr::ScalarUDF> {
        Arc::new(create_udf(
            "embed",
            vec![DataType::Utf8, DataType::Utf8],
            DataType::List(Arc::new(Field::new_list_field(DataType::Float32, true))),
            Volatility::Volatile,
            Arc::new(|args: &[ColumnarValue]| {
                let ColumnarValue::Scalar(ScalarValue::Utf8(Some(text))) = &args[0] else {
                    return Err(DataFusionError::Execution(
                        "test embed UDF expects a literal text argument".to_string(),
                    ));
                };
                let mut builder = ListBuilder::new(Float32Builder::new());
                builder.values().append_slice(&byte_vector(text));
                builder.append(true);
                Ok(ColumnarValue::Scalar(ScalarValue::List(Arc::new(
                    builder.finish(),
                ))))
            }),
        ))
    }

    fn memory_index() -> MemoryVectorIndex {
        MemoryVectorIndex::try_new(
            "content".to_string(),
            vec![Field::new("id", DataType::Int64, false)],
            MetadataColumns::none(),
            Arc::new(ByteEmbed),
            embed_udf(),
            "model_name".to_string(),
            MemoryDistanceMetric::Cosine,
        )
        .expect("valid memory index")
    }

    fn embedding_field() -> Field {
        Field::new(
            "content_embedding",
            DataType::FixedSizeList(
                Arc::new(Field::new_list_field(DataType::Float32, false)),
                DIM,
            ),
            true,
        )
    }

    fn embedding_array(rows: usize) -> Arc<FixedSizeListArray> {
        let values = Float32Array::from(
            (0..rows)
                .flat_map(|_| [1.0_f32, 0.0, 0.0])
                .collect::<Vec<_>>(),
        );
        Arc::new(
            FixedSizeListArray::try_new(
                Arc::new(Field::new_list_field(DataType::Float32, false)),
                DIM,
                Arc::new(values),
                None,
            )
            .expect("valid fixed size list"),
        )
    }

    /// A query-result batch shaped like the memory index's query plan: primary key +
    /// embedding + score.
    fn engine_query_batch(ids: &[i64]) -> RecordBatch {
        let rows = ids.len();
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                embedding_field(),
                Field::new(SEARCH_SCORE_COLUMN_NAME, DataType::Float64, true),
            ])),
            vec![
                Arc::new(Int64Array::from(ids.to_vec())),
                embedding_array(rows),
                Arc::new(Float64Array::from(vec![0.5; rows])),
            ],
        )
        .expect("valid engine query batch")
    }

    /// A list-result batch shaped like the memory index's list plan (its stored schema):
    /// embedding + primary key.
    fn engine_list_batch(ids: &[i64]) -> RecordBatch {
        let rows = ids.len();
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                embedding_field(),
                Field::new("id", DataType::Int64, false),
            ])),
            vec![
                embedding_array(rows),
                Arc::new(Int64Array::from(ids.to_vec())),
            ],
        )
        .expect("valid engine list batch")
    }

    async fn collect_ids(plan: LogicalPlan) -> Vec<i64> {
        let ctx = SessionContext::new();
        let batches = ctx
            .execute_logical_plan(plan)
            .await
            .expect("plan executes")
            .collect()
            .await
            .expect("plan collects");
        let mut ids = vec![];
        for batch in &batches {
            let id_col = batch
                .column_by_name("id")
                .expect("id column")
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("int64 ids");
            for i in 0..batch.num_rows() {
                ids.push(id_col.value(i));
            }
        }
        ids.sort_unstable();
        ids
    }

    fn warm_compound(secondary: MockIndex) -> CompoundVectorIndex {
        CompoundVectorIndex::try_new(
            Arc::new(memory_index()) as Arc<dyn VectorIndex>,
            Arc::new(secondary) as Arc<dyn VectorIndex>,
            CompoundReadMode::FallbackToSecondary,
        )
        .expect("memory index is compatible with the engine index")
    }

    #[tokio::test]
    async fn query_served_by_engine_until_memory_is_written() {
        let events = Arc::new(Mutex::new(vec![]));
        let mut secondary = MockIndex::new("engine", &events);
        secondary.dimension = Some(DIM);
        secondary.query_batches = vec![engine_query_batch(&[99])];

        let idx = warm_compound(secondary);

        // Cold in-memory index: the engine serves the query.
        let plan = idx.query_table_provider("q").expect("query plan builds");
        assert_eq!(collect_ids(Arc::unwrap_or_clone(plan)).await, vec![99]);

        // A write through the compound populates the in-memory index...
        idx.write(input_batch(2)).await.expect("write succeeds");

        // ...which then serves the query, without touching the engine.
        let plan = idx.query_table_provider("q").expect("query plan builds");
        assert_eq!(collect_ids(Arc::unwrap_or_clone(plan)).await, vec![0, 1]);
    }

    #[tokio::test]
    async fn list_served_by_engine_until_memory_is_written() {
        let events = Arc::new(Mutex::new(vec![]));
        let mut secondary = MockIndex::new("engine", &events);
        secondary.dimension = Some(DIM);
        secondary.list_batches = vec![engine_list_batch(&[99])];

        let idx = warm_compound(secondary);

        let plan = idx.list_table_provider().expect("list plan builds");
        assert_eq!(collect_ids(plan).await, vec![99]);

        idx.write(input_batch(2)).await.expect("write succeeds");

        let plan = idx.list_table_provider().expect("list plan builds");
        assert_eq!(collect_ids(plan).await, vec![0, 1]);
    }
}
