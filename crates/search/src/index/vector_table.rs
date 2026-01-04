/*
Copyright 2024-2025 The Spice.ai OSS Authors

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
    collections::{HashMap, HashSet},
    sync::Arc,
};

use arrow::datatypes::SchemaRef;
use arrow_schema::{FieldRef, Fields, Schema};
use async_trait::async_trait;

use datafusion::{
    catalog::Session,
    common::{Column, Constraints, JoinType},
    datasource::{DefaultTableSource, TableProvider, TableType},
    error::{DataFusionError, Result as DataFusionResult},
    execution::{SessionState, SessionStateBuilder},
    logical_expr::{Expr, LogicalPlan},
    physical_plan::ExecutionPlan,
    sql::TableReference,
};
use datafusion_expr::{LogicalPlanBuilder, TableProviderFilterPushDown, ident};

use datafusion_optimizer_rules::physical_plan::EmptyHashJoinExecPhysicalOptimization;
use itertools::Itertools;

use crate::index::VectorIndex;

/// A [`TableProvider`] that adds an embedding column to an underlying [`TableProvider`].
#[derive(Debug, Clone)]
pub struct VectorScanTableProvider {
    pub table_provider: Arc<dyn TableProvider>,
    pub vector_index_list: Arc<LogicalPlan>,
    pub primary_key: Vec<String>,
}

impl VectorScanTableProvider {
    pub fn try_new(
        table_provider: Arc<dyn TableProvider>,
        index: &Arc<dyn VectorIndex>,
    ) -> Result<Self, DataFusionError> {
        Ok(Self {
            table_provider,
            primary_key: index
                .primary_fields()
                .iter()
                .map(|f| f.name().clone())
                .collect(),
            vector_index_list: index.list_table_provider()?.into(),
        })
    }

    fn schema_is_sufficient(
        schema: &Fields,
        projection: &HashSet<String>,
        filters: &[Expr],
    ) -> bool {
        if !projection.is_subset(
            &schema
                .iter()
                .map(|f| f.name().clone())
                .collect::<HashSet<String>>(),
        ) {
            // schema does not have all columns.
            return false;
        }
        // Ensure filters do not reference column not in the schema
        columns_missing_from(filters, schema).is_empty()
    }

    fn apply_proj_and_filter(
        input: LogicalPlanBuilder,
        projection: &HashSet<String>,
        filters: &[Expr],
    ) -> Result<LogicalPlanBuilder, DataFusionError> {
        let filtered = if let Some(filter) = filters.iter().cloned().reduce(Expr::and) {
            input.filter(filter)?
        } else {
            input
        };

        filtered.project(projection.iter().sorted_unstable().cloned().map(ident))
    }

    fn columns_projected(
        &self,
        projection: Option<&Vec<usize>>,
    ) -> Result<HashSet<String>, DataFusionError> {
        let source_schema = match projection {
            None => self.schema(),
            Some(indices) => {
                let projected = self
                    .schema()
                    .project(indices)
                    .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
                Arc::new(projected)
            }
        };
        let columns_requested: HashSet<String> = source_schema
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();

        Ok(columns_requested)
    }

    /// Return all columns that appear in the [`Self::vector_index_list`] that are not in [`Self::table_provider`] as well as all primary keys.
    fn columns_needed_from_index(&self) -> Vec<Expr> {
        let table_schema = self.table_provider.schema();
        self.vector_index_list
            .schema()
            .columns()
            .into_iter()
            .filter(|c| {
                table_schema.column_with_name(&c.name).is_none()
                    || self.primary_key.contains(&c.name)
            })
            .map(Expr::Column)
            .collect()
    }
}

// Return the unqualified names of columns missing from those referenced by in `expr`.
fn columns_missing_from(expr: &[Expr], schema: &Fields) -> Vec<String> {
    let schema_cols = schema
        .iter()
        .map(|f| f.name().clone())
        .collect::<HashSet<_>>();

    expr.iter()
        .flat_map(|e| {
            let filter_cols = e
                .column_refs()
                .iter()
                .map(|c| c.name().to_string())
                .collect::<HashSet<_>>();
            filter_cols
                .difference(&schema_cols)
                .cloned()
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>()
}

#[async_trait]
impl TableProvider for VectorScanTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        let mut fields_map = self
            .table_provider
            .schema()
            .fields()
            .iter()
            .map(|f| (f.name().clone(), Arc::clone(f)))
            .collect::<HashMap<String, FieldRef>>();

        // Only add if key not in base table (we chose base table over index columns in `scan` afterall).
        for f in self.vector_index_list.schema().fields() {
            if !fields_map.contains_key(f.name()) {
                // Any field only present in vector index must be nullable since row may be in `self.table_provider` before `self.vector_index_list`.
                fields_map.insert(
                    f.name().clone(),
                    Arc::new(Arc::unwrap_or_clone(Arc::clone(f)).with_nullable(true)),
                );
            }
        }

        let mut fields = fields_map.values().cloned().collect::<Vec<_>>();
        fields.sort_unstable();
        Arc::new(Schema::new(fields))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>, DataFusionError> {
        self.table_provider.supports_filters_pushdown(filters)
    }

    fn constraints(&self) -> Option<&Constraints> {
        self.table_provider.constraints()
    }

    fn table_type(&self) -> TableType {
        self.table_provider.table_type()
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let columns_requested = self.columns_projected(projection)?;

        if Self::schema_is_sufficient(
            self.table_provider.schema().fields(),
            &columns_requested,
            filters,
        ) {
            let lp = Self::apply_proj_and_filter(
                LogicalPlanBuilder::scan(
                    "base_table",
                    Arc::new(DefaultTableSource::new(Arc::clone(&self.table_provider))),
                    None,
                )?,
                &columns_requested,
                filters,
            )?
            .build()?;

            return state.create_physical_plan(&lp).await;
        }

        // Reenable once we can distinguish between query and indexing `.scan()`.
        // See `<https://github.com/spiceai/spiceai/issues/7404>`
        // if Self::schema_is_sufficient(
        //     self.vector_index_list.schema().fields(),
        //     &columns_requested,
        //     filters,
        // ) {
        //     let lp = Self::apply_proj_and_filter(
        //         LogicalPlanBuilder::new_from_arc(Arc::clone(&self.vector_index_list)),
        //         &columns_requested,
        //         filters,
        //     )?
        //     .build()?;

        //     return state.create_physical_plan(&lp).await;
        // }

        // Join on primary keys, prefer to use columns from base table, push down filters where we can.
        let mut join = LogicalPlanBuilder::scan(
            "base_table",
            Arc::new(DefaultTableSource::new(Arc::clone(&self.table_provider))),
            None,
        )?
        .join(
            LogicalPlanBuilder::new_from_arc(Arc::clone(&self.vector_index_list))
                .project(self.columns_needed_from_index())?
                .alias("vector_index")?
                .build()?,
            JoinType::Left,
            self.primary_key
                .iter()
                .map(|pk| (Column::from_name(pk.clone()), Column::from_name(pk.clone())))
                .collect(),
            // If the filter affects any primary key column, we must apply after we have removed the duplicate primary key columns.
            filters
                .iter()
                .filter(|f| {
                    f.column_refs()
                        .iter()
                        .any(|col| !self.primary_key.contains(&col.name))
                })
                .cloned()
                .reduce(Expr::and),
        )?;

        let join_schema = Arc::clone(join.schema());
        join = join.project(
            // DataFusion will not deduplicate the `Join::on` keys. For simplicity with non-join
            // case, we will remove duplicate primary key columns from the right table.
            join_schema
                .iter()
                .filter(|(tbl, f)| {
                    !(self.primary_key.contains(f.name())
                        && tbl.is_some_and(|t| *t == TableReference::parse_str("vector_index")))
                })
                .map(|(tbl, field_ref)| match tbl {
                    Some(table_ref) => Column::new(Some(table_ref.clone()), field_ref.name()),
                    None => Column::new(None::<TableReference>, field_ref.name()),
                }),
        )?;

        if let Some(filter) = filters.iter().cloned().reduce(Expr::and) {
            join = join.filter(filter)?;
        }

        join = join
            .project(columns_requested.into_iter().sorted_unstable().map(ident))?
            .limit(0, limit)?;

        match with_join_optimization(state) {
            Some(state) => state.create_physical_plan(&join.build()?).await,
            None => state.create_physical_plan(&join.build()?).await,
        }
    }
}

/// Attempts to add [`EmptyHashJoinExecPhysicalOptimization`] to a given [`Session`].
/// Datafusion does not propagate [`SessionState::physical_optimizers`] into [`TableProvider::scan`].
fn with_join_optimization(state: &dyn Session) -> Option<Arc<dyn Session>> {
    Some(Arc::new(
        SessionStateBuilder::new_from_existing(
            state.as_any().downcast_ref::<SessionState>()?.clone(),
        )
        .with_physical_optimizer_rule(Arc::new(EmptyHashJoinExecPhysicalOptimization {}))
        .build(),
    ) as Arc<dyn Session>)
}

#[cfg(test)]
mod tests {

    use std::{collections::HashMap, sync::Arc};

    use arrow_schema::{DataType, Field, Schema};
    use datafusion::{
        catalog::{MemTable, TableProvider},
        sql::TableReference,
    };
    use std::any::Any;

    use arrow::{
        array::{
            ArrayData, ArrayRef, BooleanArray, FixedSizeListArray, Float32Array, Float64Array,
            Int8Array, Int16Array, Int32Array, Int64Array, RecordBatch, StringArray, UInt8Array,
            UInt16Array, UInt32Array, UInt64Array, new_null_array,
        },
        buffer::Buffer,
        util::pretty,
    };
    use arrow_schema::SchemaRef;
    use async_trait::async_trait;
    use datafusion::{
        catalog::Session,
        datasource::{DefaultTableSource, TableType},
        error::DataFusionError,
        logical_expr::TableProviderFilterPushDown,
        physical_plan::{DisplayAs, ExecutionPlan},
        prelude::{Expr, SessionConfig, SessionContext},
    };
    use datafusion_expr::{LogicalPlan, TableScan};
    use runtime_datafusion_index::Index;

    use crate::{
        generation::util::append_fields,
        index::{SearchIndex, VectorIndex, VectorScanTableProvider},
    };
    use snafu::ResultExt;

    /// This is just a [`MemTable`] that pretends it can support all filter pushdowns.
    /// This is useful for testing explain plans.
    #[derive(Debug)]
    pub struct ExplainMemTable(pub MemTable, pub &'static str);
    impl ExplainMemTable {
        #[must_use]
        pub fn new(table: MemTable, name: &'static str) -> Self {
            Self(table, name)
        }
    }
    /// Wraps a [`ExecutionPlan`] with a new [`DisplayAs`] to show what filters have been pushed down.
    /// This is useful for testing explain plans.
    #[derive(Debug)]
    pub struct ExplainExecutionPlan(
        Arc<dyn ExecutionPlan>,
        Vec<Expr>,
        Option<usize>,
        Option<Vec<usize>>,
        &'static str,
    );

    impl ExecutionPlan for ExplainExecutionPlan {
        fn name(&self) -> &'static str {
            self.4
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn properties(&self) -> &datafusion::physical_plan::PlanProperties {
            self.0.properties()
        }

        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            self.0.children()
        }

        fn with_new_children(
            self: Arc<Self>,
            children: Vec<Arc<dyn ExecutionPlan>>,
        ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
            Ok(Arc::new(ExplainExecutionPlan(
                Arc::clone(&self.0).with_new_children(children)?,
                self.1.clone(),
                self.2,
                self.3.clone(),
                self.4,
            )) as Arc<dyn ExecutionPlan>)
        }

        fn execute(
            &self,
            partition: usize,
            context: Arc<datafusion::execution::TaskContext>,
        ) -> datafusion::error::Result<datafusion::execution::SendableRecordBatchStream> {
            self.0.execute(partition, context)
        }
    }

    impl DisplayAs for ExplainExecutionPlan {
        fn fmt_as(
            &self,
            _t: datafusion::physical_plan::DisplayFormatType,
            f: &mut std::fmt::Formatter,
        ) -> std::fmt::Result {
            let columns: Vec<String> = self
                .schema()
                .fields()
                .iter()
                .map(|f| f.name())
                .cloned()
                .collect();

            write!(
                f,
                "{}: projection={columns:?} filter={:?} limit={:?}",
                self.4, self.1, self.2,
            )?;
            Ok(())
        }
    }

    #[async_trait]
    impl TableProvider for ExplainMemTable {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn schema(&self) -> SchemaRef {
            self.0.schema()
        }

        fn table_type(&self) -> TableType {
            self.0.table_type()
        }

        async fn scan(
            &self,
            state: &dyn Session,
            projection: Option<&Vec<usize>>,
            filters: &[Expr],
            limit: Option<usize>,
        ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
            Ok(Arc::new(ExplainExecutionPlan(
                self.0.scan(state, projection, filters, limit).await?,
                filters.to_vec(),
                limit,
                projection.cloned(),
                self.1,
            )) as Arc<dyn ExecutionPlan>)
        }

        fn supports_filters_pushdown(
            &self,
            filters: &[&Expr],
        ) -> Result<Vec<TableProviderFilterPushDown>, DataFusionError> {
            Ok(vec![TableProviderFilterPushDown::Exact; filters.len()])
        }
    }

    /// An implementation of [`SearchIndex`] that has one row. Useful for testing explain plans.
    #[derive(Debug)]
    pub struct PretendVectorIndex {
        embedded_column: String,
        primary_columns: Vec<Field>,
        schema: Schema,
    }
    impl PretendVectorIndex {
        #[must_use]
        pub fn new(embedded_column: String, primary_columns: Vec<Field>, schema: Schema) -> Self {
            Self {
                embedded_column,
                primary_columns,
                schema,
            }
        }
    }

    #[async_trait]
    impl VectorIndex for PretendVectorIndex {
        fn dimension(&self) -> i32 {
            self.schema
                .column_with_name(self.search_column().as_str())
                .map(|(_, f)| {
                    match f.data_type() {
                        DataType::FixedSizeList(_, dim) => *dim,
                        _ => 0, // Should not be reachable
                    }
                })
                .unwrap_or_default()
        }
        fn list_table_provider(&self) -> Result<LogicalPlan, DataFusionError> {
            let mem_table = MemTable::try_new(
                Arc::new(self.schema.clone()),
                vec![vec![one_row_default_record_batch_for_schema(&Arc::new(
                    self.schema.clone(),
                ))]],
            )?;

            Ok(LogicalPlan::TableScan(TableScan::try_new(
                "tbl",
                Arc::new(DefaultTableSource::new(Arc::new(ExplainMemTable::new(
                    mem_table,
                    "PretendVectorIndex",
                ))
                    as Arc<dyn TableProvider>)),
                None,
                vec![],
                None,
            )?))
        }
    }

    #[async_trait]
    impl Index for PretendVectorIndex {
        fn name(&self) -> &'static str {
            "PretendVectorIndex"
        }

        fn required_columns(&self) -> Vec<String> {
            self.schema
                .fields
                .iter()
                .filter(|c| *c.name() != format!("{}_embedding", self.search_column()))
                .map(|f| f.name().clone())
                .collect()
        }

        fn as_any(&self) -> &dyn Any {
            self
        }
    }

    #[async_trait]
    impl SearchIndex for PretendVectorIndex {
        fn search_column(&self) -> String {
            self.embedded_column.clone()
        }

        fn primary_fields(&self) -> Vec<Field> {
            self.primary_columns.clone()
        }

        async fn write(
            &self,
            record: RecordBatch,
        ) -> Result<RecordBatch, Box<dyn std::error::Error + Send + Sync>> {
            Ok(record)
        }

        fn query_table_provider(&self, _query: &str) -> Result<Arc<LogicalPlan>, DataFusionError> {
            let schema = append_fields(
                &Arc::new(self.schema.clone()),
                vec![Arc::new(Field::new("score", DataType::Float64, false))],
            );
            Ok(LogicalPlan::TableScan(TableScan::try_new(
                "explain",
                Arc::new(DefaultTableSource::new(Arc::new(ExplainMemTable::new(
                    MemTable::try_new(
                        Arc::clone(&schema),
                        vec![vec![one_row_default_record_batch_for_schema(&schema)]],
                    )
                    .boxed()?,
                    "PretendVectorIndex",
                ))
                    as Arc<dyn TableProvider>)),
                None,
                vec![],
                None,
            )?)
            .into())
        }
    }

    pub async fn test_explain(
        provider: Arc<dyn TableProvider>,
        tbl: TableReference,
        sql: &str,
        snapshot_name: &str,
    ) -> Result<(), String> {
        let session =
            SessionContext::new_with_config(SessionConfig::new().with_target_partitions(3));
        session
            .register_table(tbl, provider)
            .map_err(|e| e.to_string())?;

        let df = session
            .sql(format!("EXPLAIN {sql}").as_str())
            .await
            .map_err(|e| e.to_string())?;

        let col = df.collect().await.map_err(|e| e.to_string())?;
        insta::assert_snapshot!(
            snapshot_name,
            format!(
                "{}",
                pretty::pretty_format_batches(&col).map_err(|e| e.to_string())?
            )
        );
        Ok(())
    }

    #[expect(
        clippy::cast_sign_loss,
        clippy::cast_precision_loss,
        clippy::missing_panics_doc
    )]
    #[must_use]
    pub fn default_value_array(dt: &DataType) -> ArrayRef {
        match dt {
            DataType::Int8 => Arc::new(Int8Array::from(vec![0])) as ArrayRef,
            DataType::Int16 => Arc::new(Int16Array::from(vec![0])) as ArrayRef,
            DataType::Int32 => Arc::new(Int32Array::from(vec![0])) as ArrayRef,
            DataType::Int64 => Arc::new(Int64Array::from(vec![0])) as ArrayRef,
            DataType::UInt8 => Arc::new(UInt8Array::from(vec![0])) as ArrayRef,
            DataType::UInt16 => Arc::new(UInt16Array::from(vec![0])) as ArrayRef,
            DataType::UInt32 => Arc::new(UInt32Array::from(vec![0])) as ArrayRef,
            DataType::UInt64 => Arc::new(UInt64Array::from(vec![0])) as ArrayRef,
            DataType::Float32 => Arc::new(Float32Array::from(vec![0.0])) as ArrayRef,
            DataType::Float64 => Arc::new(Float64Array::from(vec![0.0])) as ArrayRef,
            DataType::Boolean => Arc::new(BooleanArray::from(vec![false])) as ArrayRef,
            DataType::Utf8 => Arc::new(StringArray::from(vec![""])) as ArrayRef,
            DataType::FixedSizeList(_, length) => {
                let list_data_type = DataType::FixedSizeList(
                    Arc::new(Field::new_list_field(DataType::Float32, false)),
                    *length,
                );
                Arc::new(FixedSizeListArray::from(
                    ArrayData::builder(list_data_type)
                        .len(1)
                        .add_child_data(
                            ArrayData::builder(DataType::Float32)
                                .len(*length as usize)
                                .add_buffer(Buffer::from_slice_ref(
                                    (0..(*length as usize))
                                        .map(|s| s as f32)
                                        .collect::<Vec<_>>(),
                                ))
                                .build()
                                .expect("unable to build FixedSizeListArray's ArrayData"),
                        )
                        .build()
                        .expect("unable to build FixedSizeListArray"),
                ))
            }
            _ => new_null_array(dt, 1),
        }
    }

    /// Creates a [`RecordBatch`] with a single row that has default value of types, as per the [`Schema`].
    #[expect(clippy::missing_panics_doc)]
    #[must_use]
    pub fn one_row_default_record_batch_for_schema(schema: &Arc<Schema>) -> RecordBatch {
        let arrays: Vec<ArrayRef> = schema
            .fields()
            .iter()
            .map(|field| default_value_array(field.data_type()))
            .collect();

        RecordBatch::try_new(Arc::clone(schema), arrays)
            .expect("could not build RecordBatch with one row")
    }

    #[tokio::test]
    pub async fn test_vector_scan_basic() -> Result<(), String> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("pk", DataType::Int64, false),
            Field::new("body", DataType::Utf8, false),
            Field::new("another_column", DataType::Utf8, false),
        ]));

        let p = VectorScanTableProvider::try_new(
            Arc::new(ExplainMemTable::new(
                MemTable::try_new(
                    Arc::clone(&schema),
                    vec![vec![one_row_default_record_batch_for_schema(&schema)]],
                )
                .expect("could not make MemTable"),
                "BaseTable",
            )),
            &(Arc::new(PretendVectorIndex::new(
                "body".to_string(),
                vec![Field::new("pk", DataType::Int64, false)],
                Schema::new(vec![
                    Field::new("pk", DataType::Int64, false),
                    Field::new(
                        "body_embedding",
                        DataType::new_fixed_size_list(DataType::Float32, 10, false),
                        false,
                    ),
                ]),
            )) as Arc<dyn VectorIndex>),
        )
        .expect("could not make 'VectorScanTableProvider'");

        let provider: Arc<dyn TableProvider> = Arc::new(p);

        test_explain(
            Arc::clone(&provider),
            TableReference::parse_str("my_vectored_table"),
            "SELECT pk, body_embedding from my_vectored_table ORDER BY pk desc LIMIT 5",
            "scan_table_basic",
        )
        .await?;

        test_explain(
            Arc::clone(&provider),
            TableReference::parse_str("my_vectored_table"),
            "SELECT pk, another_column, body_embedding from my_vectored_table ORDER BY pk desc LIMIT 5",
            "scan_table_join_for_projection",
        )
        .await?;

        test_explain(
            Arc::clone(&provider),
            TableReference::parse_str("my_vectored_table"),
            "SELECT pk, body_embedding from my_vectored_table WHERE another_column != 'something' ORDER BY pk desc LIMIT 5",
            "scan_table_join_for_filter",
        )
        .await?;

        Ok(())
    }

    // [`VectorScanTableProvider`] cannot use metadata column to get data from vector index.
    #[tokio::test]
    pub async fn test_vector_scan_index_metadata() -> Result<(), String> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("pk", DataType::Int64, false),
            Field::new("body", DataType::Utf8, false),
            Field::new("another_column", DataType::Utf8, false),
            Field::new("a_number", DataType::Int64, false),
            Field::new("not_where", DataType::Utf8, false),
        ]));
        let p = VectorScanTableProvider::try_new(
            Arc::new(ExplainMemTable(
                MemTable::try_new(
                    Arc::clone(&schema),
                    vec![vec![one_row_default_record_batch_for_schema(&schema)]],
                )
                .expect("could not make MemTable"),
                "BaseTable",
            )),
            &(Arc::new(PretendVectorIndex::new(
                "body".to_string(),
                vec![Field::new("pk", DataType::Int64, false)],
                Schema::new(vec![
                    Field::new("pk", DataType::Int64, false),
                    Field::new(
                        "body_embedding",
                        DataType::new_fixed_size_list(DataType::Float32, 10, false),
                        false,
                    ),
                    Field::new("a_number", DataType::Int64, false).with_metadata(HashMap::from([
                        ("filterable".to_string(), "true".to_string()),
                    ])),
                    Field::new("not_where", DataType::Utf8, false).with_metadata(HashMap::from([
                        ("filterable".to_string(), "false".to_string()),
                    ])),
                ]),
            )) as Arc<dyn VectorIndex>),
        )
        .expect("could not make 'VectorScanTableProvider'");

        let provider: Arc<dyn TableProvider> = Arc::new(p);

        test_explain(
            Arc::clone(&provider),
            TableReference::parse_str("my_vectored_table"),
            "SELECT pk, body_embedding from my_vectored_table ORDER BY pk desc LIMIT 5",
            "scan_table_basic_metadata",
        )
        .await?;

        test_explain(
            Arc::clone(&provider),
            TableReference::parse_str("my_vectored_table"),
            "SELECT pk, another_column, body_embedding from my_vectored_table ORDER BY pk desc LIMIT 5",
            "scan_table_join_for_projection_metadata",
        )
        .await?;

        test_explain(
            Arc::clone(&provider),
            TableReference::parse_str("my_vectored_table"),
            "SELECT pk, another_column, not_where, body_embedding from my_vectored_table ORDER BY pk desc LIMIT 5",
            "scan_table_join_for_projection_use_metadata",
        )
        .await?;

        test_explain(
            Arc::clone(&provider),
            TableReference::parse_str("my_vectored_table"),
            "SELECT pk, body_embedding from my_vectored_table WHERE another_column != 'something' AND a_number > 0 ORDER BY pk desc LIMIT 5",
            "scan_table_join_for_filter_use_metadata",
        )
        .await?;

        test_explain(
            Arc::clone(&provider),
            TableReference::parse_str("my_vectored_table"),
            "SELECT pk, not_where, body_embedding from my_vectored_table ORDER BY pk desc LIMIT 5",
            "scan_table_no_join_for_metadata_projection",
        )
        .await?;

        test_explain(
            Arc::clone(&provider),
            TableReference::parse_str("my_vectored_table"),
            "SELECT pk, body_embedding from my_vectored_table WHERE a_number > 0 ORDER BY pk desc LIMIT 5",
            "scan_table_no_join_for_metadata_filter",
        )
        .await?;

        test_explain(
            Arc::clone(&provider),
            TableReference::parse_str("my_vectored_table"),
            "SELECT pk, a_number from my_vectored_table ORDER BY pk desc LIMIT 5",
            "scan_table_no_embedding_no_join",
        )
        .await?;

        Ok(())
    }

    #[tokio::test]
    pub async fn test_vector_scan_index_multicolumn_pk() -> Result<(), String> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("pk1", DataType::Int64, false),
            Field::new("pk2", DataType::Boolean, false),
            Field::new("pk3", DataType::Utf8, false),
            Field::new("body", DataType::Utf8, false),
            Field::new("another_column", DataType::Utf8, false),
            Field::new("a_number", DataType::Int64, false),
            Field::new("not_where", DataType::Utf8, false),
        ]));
        let p = VectorScanTableProvider::try_new(
            Arc::new(ExplainMemTable(
                MemTable::try_new(
                    Arc::clone(&schema),
                    vec![vec![one_row_default_record_batch_for_schema(&schema)]],
                )
                .expect("could not make MemTable"),
                "BaseTable",
            )),
            &(Arc::new(PretendVectorIndex::new(
                "body".to_string(),
                vec![
                    Field::new("pk1", DataType::Int64, false),
                    Field::new("pk2", DataType::Boolean, false),
                    Field::new("pk3", DataType::Utf8, false),
                ],
                Schema::new(vec![
                    Field::new("pk1", DataType::Int64, false),
                    Field::new("pk2", DataType::Boolean, false),
                    Field::new("pk3", DataType::Utf8, false),
                    Field::new(
                        "body_embedding",
                        DataType::new_fixed_size_list(DataType::Float32, 10, false),
                        false,
                    ),
                    Field::new("a_number", DataType::Int64, false).with_metadata(HashMap::from([
                        ("filterable".to_string(), "true".to_string()),
                    ])),
                    Field::new("not_where", DataType::Utf8, false).with_metadata(HashMap::from([
                        ("filterable".to_string(), "false".to_string()),
                    ])),
                ]),
            )) as Arc<dyn VectorIndex>),
        )
        .expect("could not make 'VectorScanTableProvider'");

        let provider: Arc<dyn TableProvider> = Arc::new(p);

        test_explain(
            Arc::clone(&provider),
            TableReference::parse_str("my_vectored_table"),
            "SELECT pk1, pk2, pk3, body_embedding from my_vectored_table LIMIT 5",
            "scan_table_basic_multiple_pk",
        )
        .await?;

        test_explain(
            Arc::clone(&provider),
            TableReference::parse_str("my_vectored_table"),
            "SELECT pk1, pk2, pk3, another_column, body_embedding from my_vectored_table LIMIT 5",
            "scan_table_join_for_projection_metadata_multiple_pk",
        )
        .await?;

        test_explain(
            Arc::clone(&provider),
            TableReference::parse_str("my_vectored_table"),
            "SELECT pk1, pk2, pk3, another_column, not_where, body_embedding from my_vectored_table LIMIT 5",
            "scan_table_join_for_projection_use_metadata_multiple_pk",
        )
        .await?;

        test_explain(
            Arc::clone(&provider),
            TableReference::parse_str("my_vectored_table"),
            "SELECT pk1, pk2, pk3, body_embedding from my_vectored_table WHERE another_column != 'something' AND a_number > 0 LIMIT 5",
            "scan_table_join_for_filter_use_metadata_multiple_pk",
        )
        .await?;

        test_explain(
            Arc::clone(&provider),
            TableReference::parse_str("my_vectored_table"),
            "SELECT pk1, pk2, pk3, not_where, body_embedding from my_vectored_table LIMIT 5",
            "scan_table_no_join_for_metadata_projection_multiple_pk",
        )
        .await?;

        test_explain(
            Arc::clone(&provider),
            TableReference::parse_str("my_vectored_table"),
            "SELECT pk1, pk2, pk3, body_embedding from my_vectored_table WHERE a_number > 0 LIMIT 5",
            "scan_table_no_join_for_metadata_filter_multiple_pk",
        )
        .await?;

        Ok(())
    }
}
