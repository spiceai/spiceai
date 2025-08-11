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

use std::{any::Any, collections::HashSet, sync::Arc};

use arrow::array::RecordBatch;
use arrow_schema::{ArrowError, Field, SchemaRef};
use async_openai::types::EmbeddingInput;
use async_trait::async_trait;
use data_components::s3_vectors::{
    MetadataColumns, list_provider::S3VectorsListTable, query_provider::S3VectorsQueryTable,
};
use llms::embeddings::Embed;
use runtime_datafusion_index::Index;
use snafu::ResultExt;

use crate::model::EmbeddingModelStore;
use datafusion::{
    catalog::TableProvider, error::DataFusionError, logical_expr::LogicalPlan, prelude::Expr,
};
use tokio::sync::RwLock;

pub(crate) mod query_table;
mod retry_client;
pub mod s3;
pub(crate) mod scan_table;
pub use query_table::VectorQueryTableProvider;
pub use scan_table::VectorScanTableProvider;

#[derive(Debug, Clone)]
pub struct IndexEmbeddingConfig {
    pub model_name: String,
    pub embedding_models: Arc<RwLock<EmbeddingModelStore>>,
}

#[async_trait]
pub trait VectorIndex: std::fmt::Debug + Send + Sync {
    fn embedded_column(&self) -> String;
    fn primary_fields(&self) -> Vec<Field>;
    fn list_table_provider(&self) -> Arc<dyn TableProvider>;
    fn metadata_columns(&self) -> &MetadataColumns;
    fn augment_table(self: Arc<Self>, table: Arc<dyn TableProvider>) -> Arc<dyn TableProvider>;
    async fn write(&self, record: &RecordBatch);
    async fn query_table_provider(
        &self,
        query: &str,
    ) -> Result<Arc<dyn TableProvider>, Box<dyn std::error::Error + Send + Sync>>;
}

/// Implementations of indexes that can produce embedding vectors for a column in the associated [`IndexedTableProvider`], and some, provide efficient search mechanism for it.
#[derive(Debug, Clone)]
pub struct S3Vector {
    index: s3::S3VectorIndex,
    cfg: IndexEmbeddingConfig,
}

impl S3Vector {
    #[must_use]
    pub fn new(index: s3::S3VectorIndex, cfg: IndexEmbeddingConfig) -> Self {
        Self { index, cfg }
    }

    pub async fn embedding_model(&self) -> Option<Arc<dyn Embed>> {
        let model_lock = self.cfg.embedding_models.read().await;
        let model = model_lock.get(&self.cfg.model_name)?;
        Some(Arc::clone(model))
    }
}

#[async_trait]
impl VectorIndex for S3Vector {
    fn embedded_column(&self) -> String {
        self.index.embedded_column.clone()
    }

    fn primary_fields(&self) -> Vec<Field> {
        self.index.primary_key.clone()
    }

    fn list_table_provider(&self) -> Arc<dyn TableProvider> {
        Arc::new(S3VectorsListTable::from(self.index.table.clone()))
    }

    fn metadata_columns(&self) -> &MetadataColumns {
        &self.index.metadata_columns
    }

    fn augment_table(self: Arc<Self>, table: Arc<dyn TableProvider>) -> Arc<dyn TableProvider> {
        Arc::new(VectorScanTableProvider::new(table, self))
    }

    async fn write(&self, record: &RecordBatch) {
        s3::write(&self.index, &self.cfg, record).await;
    }

    async fn query_table_provider(
        &self,
        query: &str,
    ) -> Result<Arc<dyn TableProvider>, Box<dyn std::error::Error + Send + Sync>> {
        let models = self.cfg.embedding_models.read().await;
        let Some(embedding_model) = models.get(&self.cfg.model_name) else {
            return Err(Box::from(format!(
                "Vector index requires '{}' embedding model, but is not available.",
                self.cfg.model_name
            )));
        };
        let mut resp = embedding_model
            .embed(EmbeddingInput::String(query.to_string()))
            .await
            .boxed()?;
        let Some(query_vector) = resp.pop() else {
            return Err(Box::from(format!(
                "Embedding model '{}' produced no embedding for the query '{query}'.",
                self.cfg.model_name,
            )));
        };

        Ok(Arc::new(S3VectorsQueryTable::new(
            self.index.table.clone(),
            query_vector,
        )))
    }
}

#[async_trait]
impl Index for S3Vector {
    fn name(&self) -> &'static str {
        "s3_vector_index"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn required_columns(&self) -> Vec<String> {
        self.index.required_columns()
    }

    async fn compute_index(&self, batches: Vec<RecordBatch>) {
        for rb in batches {
            self.write(&rb).await;
        }
    }
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
    use data_components::s3_vectors::{
        MetadataColumn, MetadataColumns, S3_VECTOR_EMBEDDING_NAME, S3_VECTOR_PRIMARY_KEY_NAME,
        query_provider::S3_VECTOR_DISTANCE_NAME,
    };
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

    use crate::embeddings::index::VectorIndex;

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
            let cols = schema
                .fields()
                .iter()
                .filter_map(|f| {
                    if f.name() == S3_VECTOR_PRIMARY_KEY_NAME
                        || f.name() == S3_VECTOR_EMBEDDING_NAME
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

        fn list_table_provider(&self) -> Arc<dyn TableProvider> {
            Arc::new(ExplainMemTable(
                MemTable::try_new(
                    Arc::new(self.schema.clone()),
                    vec![vec![one_row_default_record_batch_for_schema(&Arc::new(
                        self.schema.clone(),
                    ))]],
                )
                .expect("Could not build PretendVectorIndex::list_table_provider"),
            ))
        }

        fn metadata_columns(&self) -> &MetadataColumns {
            &self.metadata
        }

        fn augment_table(self: Arc<Self>, table: Arc<dyn TableProvider>) -> Arc<dyn TableProvider> {
            table
        }

        async fn write(&self, _record: &RecordBatch) {}
        async fn query_table_provider(
            &self,
            _query: &str,
        ) -> Result<Arc<dyn TableProvider>, Box<dyn std::error::Error + Send + Sync>> {
            let schema = append_fields(
                &Arc::new(self.schema.clone()),
                vec![Arc::new(Field::new(
                    S3_VECTOR_DISTANCE_NAME,
                    DataType::Float64,
                    false,
                ))],
            );
            println!("In query_table_provider schema={:?}", schema);
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
