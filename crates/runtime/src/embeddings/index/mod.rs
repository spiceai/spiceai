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

use std::{collections::HashSet, sync::Arc};

use arrow::array::RecordBatch;
use arrow_schema::{ArrowError, Field, Fields, SchemaRef};
use async_trait::async_trait;

use data_components::s3_vectors::MetadataColumns;

use datafusion::{
    catalog::TableProvider, error::DataFusionError, logical_expr::LogicalPlan, prelude::Expr,
};

pub(crate) mod query_table;
mod retry_client;
pub mod s3;
pub(crate) mod scan_table;
pub use query_table::VectorQueryTableProvider;
pub use scan_table::VectorScanTableProvider;

/// A [`VectorIndex`] is a table index that can provide vector similarity results for arbitrary queries (see [`VectorIndex::query_table_provider`]).
///
/// A [`VectorIndex`] can have additional metadata columns to improve the filter capabilities of
/// [`VectorIndex::query_table_provider`], or to reduce the need for joining the [`TableProvider`]s
///  of the vector index and underlying table.
#[async_trait]
pub trait VectorIndex: std::fmt::Debug + Send + Sync {
    /// The name of the column, in the underlying table, of the column for which vector similarity is performed against.
    fn embedded_column(&self) -> String;

    /// All [`Field`]s that define a primary key between the underlying table and the [`VectorIndex`].
    ///
    fn primary_fields(&self) -> Vec<Field>;

    /// A [`TableProvider`] containing the [`VectorIndex::primary_fields`], additional metadata
    /// columns and the associated embedding vectors of the [`VectorIndex::embedded_column`].
    ///
    /// The associated embedding vector column will be [`VectorIndex::embedded_column`] with `_embedding` appended (e.g. `body_embedding`).
    fn list_table_provider(
        &self,
    ) -> Result<Arc<dyn TableProvider>, Box<dyn std::error::Error + Send + Sync>>;

    /// The additional columns available in the [`VectorIndex`].
    fn metadata_columns(&self) -> &MetadataColumns;

    /// Update the index based on a [`RecordBatch`] from the underlying table.
    async fn write(
        &self,
        record: RecordBatch,
    ) -> Result<RecordBatch, Box<dyn std::error::Error + Send + Sync>>;

    /// A [`TableProvider`] containing the [`VectorIndex::primary_fields`], additional metadata
    /// columns, the associated embedding vectors of the [`VectorIndex::embedded_column`] and the
    ///  similarity score between `query` and the [`VectorIndex::embedded_column`].
    async fn query_table_provider(
        &self,
        query: &str,
    ) -> Result<Arc<dyn TableProvider>, Box<dyn std::error::Error + Send + Sync>>;
}

// Returns true if the vector index table has all requested columns and can handle all filters (i.e. filters pertain to vector index column, even if they must be post-applied in DataFusion).
pub(super) fn vector_index_table_is_sufficient(
    source_table_schema: SchemaRef,
    vector_index_table: &LogicalPlan,
    projection: Option<&Vec<usize>>,
    filters: &[Expr],
) -> Result<bool, DataFusionError> {
    let vector_index_columns: HashSet<String> = vector_index_table
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().to_string())
        .collect();

    let full_projection =
        vector_index_has_full_projection(source_table_schema, &vector_index_columns, projection)?;
    let vector_index_filters = vector_index_filters(&vector_index_columns, filters);

    Ok(full_projection && vector_index_filters.len() == filters.len())
}

/// Returns true if the projection (relative to [`VectorQueryTableProvider`]) can be handled by the given vector index schema.
pub(super) fn vector_index_has_full_projection(
    source_table_schema: SchemaRef,
    vector_index_columns: &HashSet<String>,
    projection: Option<&Vec<usize>>,
) -> Result<bool, ArrowError> {
    let source_table_schema = match projection {
        None => source_table_schema,
        Some(indices) => Arc::new(source_table_schema.project(indices)?),
    };
    let columns_requested: HashSet<String> = source_table_schema
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();

    Ok(vector_index_columns.is_superset(&columns_requested))
}

/// Returns all filters that can be handled by the given vector index columns.
///
/// This does not require that associated [`TableProvider::supports_filters_pushdown`] is
/// [`TableProviderFilterPushDown::Unsupported`] for all filters, only that the columns
/// referenced in the filters, are those available in the `vector_index_table`.
pub(super) fn vector_index_filters(
    vector_index_columns: &HashSet<String>,
    filters: &[Expr],
) -> Vec<Expr> {
    filters
        .iter()
        .filter(|f| {
            let filter_columns = f
                .column_refs()
                .iter()
                .map(|c| c.name().to_string())
                .collect::<HashSet<_>>();
            vector_index_columns.is_superset(&filter_columns)
        })
        .cloned()
        .collect()
}

// Returns a new projection without `columns` in the projection.
//
// The order of `table_fields` must be consistent with projection.
fn projection_without_columns(
    table_fields: &Fields,
    columns: &[String],
    projection: Option<&Vec<usize>>,
) -> Vec<usize> {
    table_fields
        .iter()
        .enumerate()
        .filter_map(|(i, f)| {
            if columns.contains(f.name()) {
                return None;
            }

            // Don't include if not in projection input.
            if let Some(p) = projection.as_ref() {
                if !p.contains(&i) {
                    return None;
                }
            }
            Some(i)
        })
        .collect()
}

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
    use data_components::s3_vectors::{MetadataColumn, MetadataColumns};
    use datafusion::{
        catalog::{MemTable, Session, TableProvider},
        datasource::TableType,
        error::DataFusionError,
        logical_expr::TableProviderFilterPushDown,
        physical_plan::{DisplayAs, ExecutionPlan},
        prelude::{Expr, SessionConfig, SessionContext},
        sql::TableReference,
    };
    use search::generation::util::append_fields;
    use snafu::ResultExt;

    use crate::{embedding_col, embeddings::index::VectorIndex};

    /// This is just a [`MemTable`] that pretends it can support all filter pushdowns.
    /// This is useful for testing explain plans.
    #[derive(Debug)]
    pub struct ExplainMemTable(MemTable);

    /// Wraps a [`ExecutionPlan`] with a new [`DisplayAs`] to show what filters have been pushed down.
    /// This is useful for testing explain plans.
    #[derive(Debug)]
    pub struct ExplainExecutionPlan(
        Arc<dyn ExecutionPlan>,
        Vec<Expr>,
        Option<usize>,
        Option<Vec<usize>>,
    );

    impl ExecutionPlan for ExplainExecutionPlan {
        fn name(&self) -> &'static str {
            "ExplainExecutionPlan"
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
            )))
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
                "ExplainExecutionPlan: projection={columns:?} filter={:?} limit={:?}",
                self.1, self.2,
            )?;
            Ok(())
        }
    }

    #[async_trait::async_trait]
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
            )) as Arc<dyn ExecutionPlan>)
        }

        fn supports_filters_pushdown(
            &self,
            filters: &[&Expr],
        ) -> Result<Vec<TableProviderFilterPushDown>, DataFusionError> {
            Ok(vec![TableProviderFilterPushDown::Exact; filters.len()])
        }
    }

    /// An implementation of [`VectorIndex`] that has one row. Useful for testing explain plans.
    #[derive(Debug)]
    pub struct PretendVectorIndex {
        embedded_column: String,
        primary_columns: Vec<Field>,
        schema: Schema,
        metadata: MetadataColumns,
    }
    impl PretendVectorIndex {
        #[must_use]
        pub fn new(embedded_column: String, primary_columns: Vec<Field>, schema: Schema) -> Self {
            let primary_key_names: Vec<_> =
                primary_columns.iter().map(|f| f.name().clone()).collect();
            let cols = schema
                .fields()
                .iter()
                .filter_map(|f| {
                    if primary_key_names.contains(f.name())
                        || *f.name() == embedding_col!(embedded_column)
                    {
                        return None;
                    }
                    if f.metadata().get("filterable") == Some(&"true".to_string()) {
                        Some(MetadataColumn::Filterable(Arc::clone(f)))
                    } else {
                        Some(MetadataColumn::NonFilterable(Arc::clone(f)))
                    }
                })
                .collect::<Vec<_>>();

            Self {
                embedded_column,
                primary_columns,
                schema,
                metadata: MetadataColumns::from(cols),
            }
        }
    }

    #[async_trait::async_trait]
    impl VectorIndex for PretendVectorIndex {
        fn embedded_column(&self) -> String {
            self.embedded_column.clone()
        }

        fn primary_fields(&self) -> Vec<Field> {
            self.primary_columns.clone()
        }

        fn list_table_provider(
            &self,
        ) -> Result<Arc<dyn TableProvider>, Box<dyn std::error::Error + Send + Sync>> {
            let mem_table = MemTable::try_new(
                Arc::new(self.schema.clone()),
                vec![vec![one_row_default_record_batch_for_schema(&Arc::new(
                    self.schema.clone(),
                ))]],
            )
            .boxed()?;
            Ok(Arc::new(ExplainMemTable(mem_table)))
        }

        fn metadata_columns(&self) -> &MetadataColumns {
            &self.metadata
        }

        async fn write(
            &self,
            record: RecordBatch,
        ) -> Result<RecordBatch, Box<dyn std::error::Error + Send + Sync>> {
            Ok(record)
        }

        async fn query_table_provider(
            &self,
            _query: &str,
        ) -> Result<Arc<dyn TableProvider>, Box<dyn std::error::Error + Send + Sync>> {
            let schema = append_fields(
                &Arc::new(self.schema.clone()),
                vec![Arc::new(Field::new("score", DataType::Float64, false))],
            );

            Ok(Arc::new(ExplainMemTable(
                MemTable::try_new(
                    Arc::clone(&schema),
                    vec![vec![one_row_default_record_batch_for_schema(&schema)]],
                )
                .boxed()?,
            )) as Arc<dyn TableProvider>)
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
