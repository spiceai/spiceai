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

#[cfg(feature = "s3_vectors")]
pub mod s3;
pub mod table;

#[cfg(test)]
pub mod tests {
    use std::{any::Any, sync::Arc};

    use arrow::{
        array::{
            ArrayData, ArrayRef, BooleanArray, FixedSizeListArray, Float32Array, Float64Array,
            Int8Array, Int16Array, Int32Array, Int64Array, RecordBatch, StringArray, UInt8Array,
            UInt16Array, UInt32Array, UInt64Array, new_null_array,
        },
        buffer::Buffer,
        util::pretty,
    };
    use arrow_schema::{DataType, Field, Schema, SchemaRef};
    use async_trait::async_trait;
    use datafusion::{
        catalog::{MemTable, Session, TableProvider},
        datasource::{DefaultTableSource, TableType},
        error::DataFusionError,
        logical_expr::TableProviderFilterPushDown,
        physical_plan::{DisplayAs, ExecutionPlan},
        prelude::{Expr, SessionConfig, SessionContext},
        sql::TableReference,
    };
    use datafusion_expr::{LogicalPlan, TableScan};
    use runtime_datafusion_index::Index;
    use search::index::VectorIndex;
    use search::{generation::util::append_fields, index::SearchIndex};
    use snafu::ResultExt;

    use crate::embedding_col;

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
                .filter(|c| *c.name() != embedding_col!(self.search_column()))
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

    #[allow(
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
                    ArrayData::builder(list_data_type.clone())
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
    #[allow(clippy::missing_panics_doc)]
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
}
