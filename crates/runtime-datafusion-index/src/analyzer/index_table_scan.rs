/*
Copyright 2025 The Spice.ai OSS Authors

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
    cmp::Ordering,
    collections::HashSet,
    fmt,
    hash::{Hash, Hasher},
    sync::Arc,
};

use async_trait::async_trait;
use datafusion::{
    arrow::datatypes::Schema,
    common::{
        DFSchemaRef,
        tree_node::{Transformed, TreeNode, TreeNodeRecursion},
    },
    datasource::DefaultTableSource,
    error::{DataFusionError, Result},
    execution::{SendableRecordBatchStream, SessionState, TaskContext},
    logical_expr::{Extension, LogicalPlan, UserDefinedLogicalNode, UserDefinedLogicalNodeCore},
    optimizer::{OptimizerConfig, OptimizerRule},
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, execution_plan::CardinalityEffect,
        stream::RecordBatchStreamAdapter,
    },
    physical_planner::{ExtensionPlanner, PhysicalPlanner},
    prelude::Expr,
};
use futures::{StreamExt, TryStreamExt};
use itertools::Itertools;

use crate::{Index, IndexedTableProvider};

/// [`OptimizerRule`] that looks for [`IndexedTableProvider`] nodes and adds an [`IndexTableScanNode`].
#[derive(Debug, Default)]
pub struct IndexTableScanOptimizerRule {}

impl IndexTableScanOptimizerRule {
    #[must_use]
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for IndexTableScanOptimizerRule {
    fn name(&self) -> &'static str {
        "IndexTableScanOptimizerRule"
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        plan.transform_down(|plan| match plan {
            LogicalPlan::Extension(extension) => {
                if extension
                    .node
                    .as_any()
                    .downcast_ref::<IndexTableScanNode>()
                    .is_some()
                {
                    Ok(Transformed::new(
                        LogicalPlan::Extension(extension),
                        false,
                        TreeNodeRecursion::Jump, // Don't process any further children of this sub-tree.
                    ))
                } else {
                    Ok(Transformed::no(LogicalPlan::Extension(extension)))
                }
            }
            LogicalPlan::TableScan(table_scan) => {
                let Some(default_source) = table_scan
                    .source
                    .as_any()
                    .downcast_ref::<DefaultTableSource>()
                else {
                    return Ok(Transformed::no(LogicalPlan::TableScan(table_scan)));
                };
                let underlying = Arc::clone(&default_source.table_provider);
                let Some(indexed_table_provider) =
                    underlying.as_any().downcast_ref::<IndexedTableProvider>()
                else {
                    return Ok(Transformed::no(LogicalPlan::TableScan(table_scan)));
                };
                let projected_schema = Arc::clone(&table_scan.projected_schema);

                // Filter to just the indexes that can be served by the projected schema
                let available_indexes: Vec<_> = indexed_table_provider
                    .indexes
                    .iter()
                    .filter(|index| {
                        // Check if all required columns for this index are in the projected schema
                        index
                            .required_columns()
                            .iter()
                            .all(|col| projected_schema.has_column_with_unqualified_name(col))
                    })
                    .cloned()
                    .collect();

                if available_indexes.is_empty() {
                    // No indexes can be served by the projected schema
                    let required_columns = indexed_table_provider.indexes.iter().flat_map(|i| i.required_columns()).collect::<HashSet<_>>().into_iter().join(",");
                    let projected_schema_columns = projected_schema.fields().iter().map(|c| c.name()).join(",");
                    tracing::warn!(
                        "Could not index table {}, did not find expected columns [{required_columns}] in the projected schema [{projected_schema_columns}]",
                        table_scan.table_name.table(),
                    );
                    return Ok(Transformed::no(LogicalPlan::TableScan(table_scan)));
                }

                // Create new node with just the available indexes
                let new_node =
                    IndexTableScanNode::new(LogicalPlan::TableScan(table_scan), available_indexes);

                let plan = LogicalPlan::Extension(Extension {
                    node: Arc::new(new_node),
                });

                // We don't need to process the TableScan we just processed, so we jump to the next node.
                Ok(Transformed::new(plan, true, TreeNodeRecursion::Jump))
            }
            _ => Ok(Transformed::no(plan)),
        })
    }
}

#[derive(Debug)]
pub struct IndexTableScanNode {
    input: LogicalPlan,
    indexes: Vec<Arc<dyn Index + Send + Sync>>,
}

impl IndexTableScanNode {
    #[must_use]
    pub fn new(input: LogicalPlan, indexes: Vec<Arc<dyn Index + Send + Sync>>) -> Self {
        Self { input, indexes }
    }

    #[must_use]
    pub fn indexes(&self) -> &[Arc<dyn Index + Send + Sync>] {
        &self.indexes
    }
    #[must_use]
    pub fn input(&self) -> &LogicalPlan {
        &self.input
    }
}

impl Hash for IndexTableScanNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.input.hash(state);
        for index in &self.indexes {
            index.name().hash(state);
        }
    }
}

impl PartialEq for IndexTableScanNode {
    fn eq(&self, other: &Self) -> bool {
        self.input == other.input
            && self.indexes.iter().all(|index| {
                other
                    .indexes
                    .iter()
                    .any(|other_index| index.name() == other_index.name())
            })
    }
}

impl Eq for IndexTableScanNode {}

impl PartialOrd for IndexTableScanNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.input.partial_cmp(&other.input)
    }
}

impl UserDefinedLogicalNodeCore for IndexTableScanNode {
    fn name(&self) -> &'static str {
        "IndexTableScanNode"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        self.input.schema()
    }

    fn expressions(&self) -> Vec<Expr> {
        Vec::new()
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "IndexTableScanNode")?;
        for index in &self.indexes {
            write!(f, " index:{}", index.name())?;
        }
        Ok(())
    }

    /// Returns the necessary input columns for this node required to compute
    /// the columns in the output schema
    ///
    /// This is used for projection push-down when `DataFusion` has determined that
    /// only a subset of the output columns of this node are needed by its parents.
    /// This API is used to tell `DataFusion` which, if any, of the input columns are no longer
    /// needed.
    ///
    /// Return `None`, the default, if this information can not be determined.
    /// Returns `Some(_)` with the column indices for each child of this node that are
    /// needed to compute `output_columns`
    fn necessary_children_exprs(&self, output_columns: &[usize]) -> Option<Vec<Vec<usize>>> {
        // Since the input & output schema is the same, output columns require their corresponding index in the input columns.
        Some(vec![output_columns.to_vec()])
    }

    /// A list of output columns (e.g. the names of columns in
    /// `self.schema()`) for which predicates can not be pushed below
    /// this node without changing the output.
    ///
    /// By default, this returns all columns and thus prevents any
    /// predicates from being pushed below this node.
    fn prevent_predicate_push_down_columns(&self) -> HashSet<String> {
        // Allow filters for all columns to be pushed down
        HashSet::new()
    }

    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        if inputs.len() != 1 {
            return Err(DataFusionError::External(
                crate::Error::MultipleInputs {
                    input_len: inputs.len(),
                }
                .into(),
            ));
        }

        if !exprs.is_empty() {
            return Err(DataFusionError::External(
                crate::Error::NoExpressions {
                    expr_len: exprs.len(),
                }
                .into(),
            ));
        }

        let Some(input) = inputs.into_iter().next() else {
            unreachable!("should have one input");
        };

        Ok(Self {
            input,
            indexes: self.indexes.clone(),
        })
    }
}

#[derive(Debug, Default)]
pub struct IndexTableScanExtensionPlanner {}

impl IndexTableScanExtensionPlanner {
    #[must_use]
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl ExtensionPlanner for IndexTableScanExtensionPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let Some(index_table_scan_node) = node.as_any().downcast_ref::<IndexTableScanNode>() else {
            return Ok(None);
        };

        if logical_inputs.len() != 1 {
            return Err(datafusion::error::DataFusionError::Internal(format!(
                "IndexTableScanNode should have 1 logical input, got {}",
                logical_inputs.len()
            )));
        }

        if physical_inputs.len() != 1 {
            return Err(datafusion::error::DataFusionError::Internal(format!(
                "IndexTableScanNode should have 1 physical input, got {}",
                physical_inputs.len()
            )));
        }

        let physical_input = &physical_inputs[0];
        let exec_plan = Arc::new(IndexerExec::new(
            Arc::clone(physical_input),
            index_table_scan_node.indexes.clone(),
        ));
        Ok(Some(exec_plan))
    }
}

#[derive(Debug)]
pub(crate) struct IndexerExec {
    input_exec: Arc<dyn ExecutionPlan>,
    indexes: Vec<Arc<dyn Index + Send + Sync>>,
}

impl IndexerExec {
    pub(crate) fn new(
        input_exec: Arc<dyn ExecutionPlan>,
        indexes: Vec<Arc<dyn Index + Send + Sync>>,
    ) -> Self {
        Self {
            input_exec,
            indexes,
        }
    }
}

impl DisplayAs for IndexerExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "IndexerExec")?;
        for index in &self.indexes {
            if matches!(t, DisplayFormatType::TreeRender) {
                writeln!(f, "index:{}", index.name())?;
            } else {
                write!(f, " index:{}", index.name())?;
            }
        }
        write!(f, " input:")?;
        self.input_exec.fmt_as(t, f)?;
        Ok(())
    }
}

impl ExecutionPlan for IndexerExec {
    fn name(&self) -> &'static str {
        "IndexerExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &datafusion::physical_plan::PlanProperties {
        self.input_exec.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input_exec]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true; self.children().len()]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(datafusion::error::DataFusionError::Internal(
                "IndexerExec requires exactly one input".to_string(),
            ));
        }
        let input = children.into_iter().next().ok_or_else(|| {
            datafusion::error::DataFusionError::Internal(
                "IndexerExec requires exactly one input".to_string(),
            )
        })?;
        Ok(Arc::new(Self {
            input_exec: input,
            indexes: self.indexes.clone(),
        }))
    }

    fn supports_limit_pushdown(&self) -> bool {
        true
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        CardinalityEffect::Equal
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion::error::Result<SendableRecordBatchStream> {
        let schema = self.input_exec.schema();
        let expected_schema = Arc::clone(&schema);
        let indexes = self.indexes.clone();
        let stream = self
            .input_exec
            .execute(partition, Arc::clone(&context))?
            .and_then(move |batch| {
                let indexes = indexes.clone();
                let expected_schema = Arc::clone(&expected_schema);
                async move {
                    let mut b = batch;

                    // Each index consumes the record batch and produces a new record batch with
                    // the same schema. The indexes are executed in order, with the output of the
                    // first index becoming the input of the second, etc.
                    for idx in &indexes {
                        let mut out = idx.compute_index(vec![b]).await?;

                        match out.len() {
                            1 => {
                                b = out
                                    .pop()
                                    .unwrap_or_else(|| unreachable!("length is checked"));
                                if b.schema().as_ref() != expected_schema.as_ref() {
                                    let exp = schema_signature(expected_schema.as_ref());
                                    let got = schema_signature(b.schema().as_ref());
                                    return Err(DataFusionError::Execution(format!(
                                        "Index {} changed schema.\
                                        Expected fields ({}): {}\
                                        Got fields ({}): {}",
                                        idx.name(),
                                        expected_schema.fields().len(),
                                        exp,
                                        b.schema().fields().len(),
                                        got,
                                    )));
                                }
                            }
                            0 => {
                                return Err(DataFusionError::Execution(format!(
                                    "Index {} produced no record batch",
                                    idx.name()
                                )));
                            }
                            _ => {
                                return Err(DataFusionError::Execution(format!(
                                    "Index {} produced {} record batches; expected 1",
                                    idx.name(),
                                    out.len()
                                )));
                            }
                        }
                    }

                    Ok(b)
                }
            })
            .boxed();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

/// Helper for better diagnostics when schema is mismatched.
fn schema_signature(s: &Schema) -> String {
    use std::fmt::Write;
    let mut buf = String::new();
    for (i, f) in s.fields().iter().enumerate() {
        if i > 0 {
            buf.push_str(", ");
        }
        let _ = write!(
            &mut buf,
            "{}: {:?}{}",
            f.name(),
            f.data_type(),
            if f.is_nullable() { "?" } else { "" }
        );
    }
    buf
}

#[cfg(test)]
mod test {
    use std::{
        any::Any,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
    };

    use async_trait::async_trait;
    use datafusion::{
        arrow::{
            self,
            array::{ArrayRef, Int64Array, RecordBatch, StringArray},
            datatypes::{DataType, Field, Schema},
        },
        catalog::{MemTable, TableProvider},
        error::DataFusionError,
        execution::{SessionState, SessionStateBuilder, context::QueryPlanner},
        logical_expr::LogicalPlan,
        physical_plan::ExecutionPlan,
        physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner},
        prelude::SessionContext,
    };

    use crate::{
        Index, IndexedTableProvider,
        analyzer::{IndexTableScanExtensionPlanner, IndexTableScanOptimizerRule},
    };

    #[derive(Debug, Default)]
    pub struct TestQueryPlanner {}

    impl TestQueryPlanner {
        #[must_use]
        pub fn new() -> Self {
            Self {}
        }
    }

    #[async_trait]
    impl QueryPlanner for TestQueryPlanner {
        async fn create_physical_plan(
            &self,
            logical_plan: &LogicalPlan,
            session_state: &SessionState,
        ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
            let physical_planner = DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(
                IndexTableScanExtensionPlanner::new(),
            )]);
            physical_planner
                .create_physical_plan(logical_plan, session_state)
                .await
        }
    }

    pub struct TestIndex {
        required_cols: Vec<String>,
        #[allow(clippy::type_complexity)]
        compute_index_cb: Option<fn(Vec<RecordBatch>) -> Result<Vec<RecordBatch>, DataFusionError>>,
        calls: AtomicUsize,
    }

    impl TestIndex {
        #[allow(clippy::type_complexity)]
        fn new(
            required_cols: Vec<String>,
            compute_index_cb: Option<
                fn(Vec<RecordBatch>) -> Result<Vec<RecordBatch>, DataFusionError>,
            >,
        ) -> Self {
            Self {
                required_cols,
                compute_index_cb,
                calls: AtomicUsize::default(),
            }
        }

        fn compute_index_calls(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }
    }

    impl std::fmt::Debug for TestIndex {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("TestIndex")
                .field("required_cols", &self.required_cols)
                .finish_non_exhaustive()
        }
    }

    #[async_trait]
    impl Index for TestIndex {
        fn name(&self) -> &'static str {
            "s3_vector_index"
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn required_columns(&self) -> Vec<String> {
            self.required_cols.clone()
        }

        async fn compute_index(
            &self,
            batches: Vec<RecordBatch>,
        ) -> Result<Vec<RecordBatch>, DataFusionError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if let Some(compute_index_cb) = self.compute_index_cb {
                return compute_index_cb(batches);
            }
            Ok(batches)
        }
    }

    fn get_ctx() -> SessionContext {
        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_query_planner(Arc::new(TestQueryPlanner::new()))
            .with_optimizer_rule(Arc::new(IndexTableScanOptimizerRule::new()))
            .build();

        SessionContext::new_with_state(state)
    }

    fn test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("region", DataType::Utf8, false),
            Field::new("value", DataType::Int64, false),
        ]))
    }

    #[allow(clippy::expect_used)]
    fn test_empty_batch() -> RecordBatch {
        let empty_columns: Vec<ArrayRef> = test_schema()
            .fields()
            .iter()
            .map(|f| arrow::array::new_empty_array(f.data_type()))
            .collect();

        RecordBatch::try_new(test_schema(), empty_columns).expect("valid batch")
    }

    #[allow(clippy::expect_used)]
    fn mem_table() -> Arc<dyn TableProvider> {
        let empty_batch = test_empty_batch();
        let mem_table = Arc::new(
            MemTable::try_new(empty_batch.schema(), vec![vec![empty_batch]]).expect("valid table"),
        );
        mem_table as Arc<dyn TableProvider>
    }

    fn index_table(
        index: Arc<dyn Index + Send + Sync>,
        table: Arc<dyn TableProvider>,
    ) -> Arc<dyn TableProvider> {
        Arc::new(IndexedTableProvider::new(table).add_index(index)) as Arc<dyn TableProvider>
    }

    #[allow(clippy::expect_used)]
    fn test_one_row_batch() -> RecordBatch {
        use datafusion::arrow::array::{Int64Array, StringArray};
        let schema = test_schema();
        let id: ArrayRef = Arc::new(Int64Array::from(vec![1]));
        let region: ArrayRef = Arc::new(StringArray::from(vec!["A"]));
        let value: ArrayRef = Arc::new(Int64Array::from(vec![10]));
        RecordBatch::try_new(schema, vec![id, region, value]).expect("valid batch")
    }

    #[allow(clippy::expect_used)]
    fn mem_table_from_batches(batches: Vec<RecordBatch>) -> Arc<dyn TableProvider> {
        let schema = batches[0].schema();
        Arc::new(MemTable::try_new(schema, vec![batches]).expect("valid table"))
            as Arc<dyn TableProvider>
    }

    #[allow(clippy::expect_used)]
    fn one_row_batch() -> RecordBatch {
        let schema = test_schema();
        let id: ArrayRef = Arc::new(Int64Array::from(vec![1]));
        let region: ArrayRef = Arc::new(StringArray::from(vec!["A"]));
        let value: ArrayRef = Arc::new(Int64Array::from(vec![10]));
        RecordBatch::try_new(schema, vec![id, region, value]).expect("valid batch")
    }

    #[tokio::test]
    async fn optimizer_rule_happy_path() {
        let ctx = get_ctx();
        let index = Arc::new(TestIndex::new(vec!["id".to_string()], None));
        let index_table = index_table(
            Arc::clone(&index) as Arc<dyn Index + Send + Sync>,
            mem_table(),
        );

        ctx.register_table("test_idx_table", index_table)
            .expect("valid table");

        let df = ctx.table("test_idx_table").await.expect("valid");

        let _results = df.collect().await.expect("should complete");
        assert_eq!(1, index.compute_index_calls());
    }

    #[tokio::test]
    async fn optimizer_rule_indexer_error() {
        let ctx = get_ctx();
        let index = Arc::new(TestIndex::new(
            vec!["id".to_string()],
            Some(|_| {
                Err(DataFusionError::Execution(
                    "Some error while indexing".to_string(),
                ))
            }),
        ));
        let index_table = index_table(
            Arc::clone(&index) as Arc<dyn Index + Send + Sync>,
            mem_table(),
        );

        ctx.register_table("test_idx_table", index_table)
            .expect("valid table");

        let df = ctx.table("test_idx_table").await.expect("valid");

        let err = df
            .collect()
            .await
            .expect_err("should return an error on indexing");
        assert!(matches!(err, DataFusionError::Execution(_)));
        assert_eq!(
            err.to_string(),
            "Execution error: Some error while indexing".to_string()
        );
        assert_eq!(1, index.compute_index_calls());
    }

    #[tokio::test]
    async fn pipelines_multiple_indexes_in_order_and_passes_batch() {
        let ctx = get_ctx();

        // index #1: set value column to 100
        let idx1 = Arc::new(TestIndex::new(
            vec!["id".to_string()],
            Some(|batches| {
                let b = batches.into_iter().next().expect("one batch in");
                let id = Arc::clone(b.column(0));
                let region = Arc::clone(b.column(1));
                let new_value: ArrayRef =
                    Arc::new(datafusion::arrow::array::Int64Array::from(vec![100]));
                let out = RecordBatch::try_new(b.schema(), vec![id, region, new_value])
                    .map_err(|e| DataFusionError::Execution(e.to_string()))?;
                Ok(vec![out])
            }),
        ));

        // index #2: assert it sees 100, then set to 200
        let idx2 = Arc::new(TestIndex::new(
            vec!["id".to_string()],
            Some(|batches| {
                let b = batches.into_iter().next().expect("one batch in");
                let v = b
                    .column(2)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("valid array")
                    .value(0);
                if v != 100 {
                    return Err(DataFusionError::Execution(format!("expected 100, got {v}")));
                }
                let id = Arc::clone(b.column(0));
                let region = Arc::clone(b.column(1));
                let new_value: ArrayRef =
                    Arc::new(datafusion::arrow::array::Int64Array::from(vec![200]));
                let out = RecordBatch::try_new(b.schema(), vec![id, region, new_value])
                    .map_err(|e| DataFusionError::Execution(e.to_string()))?;
                Ok(vec![out])
            }),
        ));

        // table with a single non-empty batch so we can assert data changes
        let table = mem_table_from_batches(vec![test_one_row_batch()]);

        // build an IndexedTableProvider with *two* indexes, in order
        let provider = IndexedTableProvider::new(table)
            .add_index(Arc::clone(&idx1) as Arc<dyn Index + Send + Sync>)
            .add_index(Arc::clone(&idx2) as Arc<dyn Index + Send + Sync>);

        ctx.register_table(
            "pipeline_idx_table",
            Arc::new(provider) as Arc<dyn TableProvider>,
        )
        .expect("valid table");

        let df = ctx.table("pipeline_idx_table").await.expect("valid");
        let results = df.collect().await.expect("should complete");
        assert_eq!(results.len(), 1);
        let out = &results[0];

        // final value should be 200 (idx2 saw 100 from idx1 and set to 200)
        let out_val = out
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("valid array")
            .value(0);
        assert_eq!(out_val, 200);

        // each index called exactly once
        assert_eq!(1, idx1.compute_index_calls());
        assert_eq!(1, idx2.compute_index_calls());
    }

    #[tokio::test]
    async fn pipeline_errors_when_index_returns_zero_batches() {
        let ctx = get_ctx();

        // index returns zero batches (violates contract)
        let bad_idx = Arc::new(TestIndex::new(vec!["id".to_string()], Some(|_| Ok(vec![]))));

        let table = mem_table(); // empty batch is fine; the error is from the index
        let provider = IndexedTableProvider::new(table)
            .add_index(Arc::clone(&bad_idx) as Arc<dyn Index + Send + Sync>);

        ctx.register_table(
            "zero_batches_idx_table",
            Arc::new(provider) as Arc<dyn TableProvider>,
        )
        .expect("valid table");

        let df = ctx.table("zero_batches_idx_table").await.expect("valid");
        let err = df
            .collect()
            .await
            .expect_err("should error due to zero batches");
        assert_eq!(
            err.to_string(),
            "Execution error: Index s3_vector_index produced no record batch"
        );
        assert_eq!(1, bad_idx.compute_index_calls());
    }

    #[tokio::test]
    async fn pipeline_errors_when_index_returns_multiple_batches() {
        let ctx = get_ctx();

        // index returns two batches (violates contract)
        let bad_idx = Arc::new(TestIndex::new(
            vec!["id".to_string()],
            Some(|batches| {
                let b = batches.into_iter().next().expect("one batch in");
                Ok(vec![b.clone(), b])
            }),
        ));

        let table = mem_table(); // any input works
        let provider = IndexedTableProvider::new(table)
            .add_index(Arc::clone(&bad_idx) as Arc<dyn Index + Send + Sync>);

        ctx.register_table(
            "multi_batches_idx_table",
            Arc::new(provider) as Arc<dyn TableProvider>,
        )
        .expect("valid table");

        let df = ctx.table("multi_batches_idx_table").await.expect("valid");
        let err = df
            .collect()
            .await
            .expect_err("should error due to multiple batches");
        assert_eq!(
            err.to_string(),
            "Execution error: Index s3_vector_index produced 2 record batches; expected 1"
        );
        assert_eq!(1, bad_idx.compute_index_calls());
    }

    #[tokio::test]
    async fn pipeline_stops_when_later_index_errors() {
        let ctx = get_ctx();

        let pass_through = Arc::new(TestIndex::new(vec!["id".to_string()], None));
        let failing = Arc::new(TestIndex::new(
            vec!["id".to_string()],
            Some(|_| Err(DataFusionError::Execution("boom".to_string()))),
        ));

        let table = mem_table_from_batches(vec![test_one_row_batch()]);
        let provider = IndexedTableProvider::new(table)
            .add_index(Arc::clone(&pass_through) as Arc<dyn Index + Send + Sync>)
            .add_index(Arc::clone(&failing) as Arc<dyn Index + Send + Sync>);

        ctx.register_table(
            "late_fail_idx_table",
            Arc::new(provider) as Arc<dyn TableProvider>,
        )
        .expect("valid table");

        let df = ctx.table("late_fail_idx_table").await.expect("valid");
        let err = df
            .collect()
            .await
            .expect_err("should error from second index");
        assert_eq!("Execution error: boom", err.to_string());

        // first ran once, second ran once
        assert_eq!(1, pass_through.compute_index_calls());
        assert_eq!(1, failing.compute_index_calls());
    }

    #[tokio::test]
    async fn pipeline_errors_when_index_changes_datatype() {
        let ctx = get_ctx();

        // Index that changes the type of "value" from Int64 -> Utf8
        let idx = Arc::new(TestIndex::new(
            vec!["id".to_string()],
            Some(|batches| {
                let b = batches.into_iter().next().expect("one batch");
                // reuse id & region; replace value with Utf8
                let id = Arc::clone(b.column(0));
                let region = Arc::clone(b.column(1));
                let new_value: ArrayRef = Arc::new(StringArray::from(vec!["10"]));
                let new_schema = Arc::new(Schema::new(vec![
                    Field::new("id", DataType::Int64, false),
                    Field::new("region", DataType::Utf8, false),
                    Field::new("value", DataType::Utf8, false),
                ]));
                let out = RecordBatch::try_new(new_schema, vec![id, region, new_value])
                    .map_err(|e| DataFusionError::Execution(e.to_string()))?;
                Ok(vec![out])
            }),
        ));

        let table = mem_table_from_batches(vec![one_row_batch()]);
        let provider = IndexedTableProvider::new(table)
            .add_index(Arc::clone(&idx) as Arc<dyn Index + Send + Sync>);
        ctx.register_table(
            "schema_change_type",
            Arc::new(provider) as Arc<dyn TableProvider>,
        )
        .expect("valid");

        let df = ctx.table("schema_change_type").await.expect("valid");
        let err = df
            .collect()
            .await
            .expect_err("should error due to schema change");
        let msg = err.to_string();

        assert!(msg.contains("changed schema"));
        assert!(msg.contains("Expected fields"));
        assert!(msg.contains("Got fields"));
        assert!(msg.contains("value: Int64")); // from expected
        assert!(msg.contains("value: Utf8")); // from got

        assert_eq!(1, idx.compute_index_calls());
    }

    #[tokio::test]
    async fn pipeline_errors_when_index_adds_or_drops_columns() {
        let ctx = get_ctx();

        // Index that adds a new column "extra"
        let idx_add = Arc::new(TestIndex::new(
            vec!["id".to_string()],
            Some(|batches| {
                let b = batches.into_iter().next().expect("one batch");
                let id = Arc::clone(b.column(0));
                let region = Arc::clone(b.column(1));
                let value = Arc::clone(b.column(2));
                let extra: ArrayRef = Arc::new(Int64Array::from(vec![999]));
                let new_schema = Arc::new(Schema::new(vec![
                    Field::new("id", DataType::Int64, false),
                    Field::new("region", DataType::Utf8, false),
                    Field::new("value", DataType::Int64, false),
                    Field::new("extra", DataType::Int64, false),
                ]));
                let out = RecordBatch::try_new(new_schema, vec![id, region, value, extra])
                    .map_err(|e| DataFusionError::Execution(e.to_string()))?;
                Ok(vec![out])
            }),
        ));

        let table = mem_table_from_batches(vec![one_row_batch()]);
        let provider = IndexedTableProvider::new(table)
            .add_index(Arc::clone(&idx_add) as Arc<dyn Index + Send + Sync>);

        ctx.register_table(
            "schema_change_add",
            Arc::new(provider) as Arc<dyn TableProvider>,
        )
        .expect("valid");

        let df = ctx.table("schema_change_add").await.expect("valid");
        let err = df
            .collect()
            .await
            .expect_err("should error due to schema change");
        let msg = err.to_string();

        assert!(msg.contains("changed schema"));
        assert!(msg.contains("Expected fields (3)"));
        assert!(msg.contains("Got fields (4)"));
        assert!(msg.contains("extra: Int64"));

        assert_eq!(1, idx_add.compute_index_calls());
    }
}
