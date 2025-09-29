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

use std::{any::Any, sync::Arc};

use arrow::array::RecordBatch;
use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;
use data_components::s3_vectors::{
    S3_VECTOR_EMBEDDING_NAME, S3_VECTOR_PRIMARY_KEY_NAME, S3VectorsTable,
    list_provider::S3VectorsListTable, query_provider::S3VectorsQueryTable,
};
use futures::future::try_join_all;
use llms::embeddings::Embed;
use runtime_datafusion_index::Index;
use search::SEARCH_SCORE_COLUMN_NAME;
use search::index::{SearchIndex, VectorIndex};
use search::metadata::{MetadataColumn, MetadataColumns};
use snafu::ResultExt;

use crate::embeddings::index::s3::compute_vector::ComputeQuery;
use crate::{embedding_col, embeddings::index::s3::write, model::EmbeddingModelStore};
use datafusion::{
    catalog::TableProvider,
    common::Column,
    datasource::DefaultTableSource,
    error::DataFusionError,
    functions::core::{arrow_cast::ArrowCastFunc, union_extract::UnionExtractFun},
    logical_expr::{
        BinaryExpr, Cast, LogicalPlan, Operator, Projection, ScalarUDF, TableScan,
        expr::{Alias, ScalarFunction},
    },
    prelude::{Expr, lit},
    scalar::ScalarValue,
    sql::TableReference,
};
use tokio::sync::RwLock;

#[derive(Debug, Clone)]
pub struct S3Vector {
    pub table: S3VectorsTable,

    /// The name of the column in the associated [`TableProvider`] that produces the `data` column in [`S3VectorsTable`].
    pub embedded_column: String,

    /// The ordered fields that comprise the underlying unique `key` in [`S3VectorsTable`]
    pub primary_key: Vec<Field>,

    /// Additional columns to add as metadata to the S3 vector index from the original dataset columns.
    pub metadata_columns: MetadataColumns,

    pub model_name: String,

    pub embedding_models: Arc<RwLock<EmbeddingModelStore>>,
}

impl S3Vector {
    #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
    #[must_use]
    pub fn new(
        table: S3VectorsTable,
        embedded_column: String,
        primary_key: Vec<Field>,
        metadata_columns: MetadataColumns,
        model_name: String,
        embedding_models: Arc<RwLock<EmbeddingModelStore>>,
    ) -> Self {
        Self {
            table,
            embedded_column,
            primary_key,
            metadata_columns,
            model_name,
            embedding_models,
        }
    }

    /// Add extra metadata columns to the `S3Vector` table schema.
    #[must_use]
    pub fn add_metadata(mut self, cols: Vec<MetadataColumn>) -> Self {
        // Add to schema too.
        let mut fields: Vec<_> = self.table.schema.fields().into_iter().cloned().collect();
        fields.extend(
            cols.iter()
                .map(|c| Arc::clone(&c.field()))
                .collect::<Vec<_>>(),
        );
        self.table.schema = Schema::new(fields).into();

        let mut new: Vec<_> = self.metadata_columns.into_iter().collect();
        new.extend(cols);
        self.metadata_columns = new.into();

        self
    }

    pub async fn embedding_model(&self) -> Option<Arc<dyn Embed>> {
        let model_lock = self.embedding_models.read().await;
        let model = model_lock.get(&self.model_name)?;
        Some(Arc::clone(model))
    }
}

#[async_trait]
impl SearchIndex for S3Vector {
    fn search_column(&self) -> String {
        self.embedded_column.clone()
    }

    fn primary_fields(&self) -> Vec<Field> {
        self.primary_key.clone()
    }

    fn metadata_columns(&self) -> &MetadataColumns {
        &self.metadata_columns
    }

    async fn write(
        &self,
        record: RecordBatch,
    ) -> Result<RecordBatch, Box<dyn std::error::Error + Send + Sync>> {
        write::write(self, record).await.boxed()
    }

    fn as_vector_index(self: Arc<Self>) -> Option<Arc<dyn VectorIndex>> {
        Some(Arc::clone(&self) as Arc<dyn VectorIndex>)
    }

    fn query_table_provider(&self, query: &str) -> Result<Arc<LogicalPlan>, DataFusionError> {
        let mut projection = s3_vectors_primary_key_cast(&self.primary_fields());
        projection.extend(vec![
            Expr::Alias(Alias::new(
                Expr::Column(Column::new_unqualified(S3_VECTOR_EMBEDDING_NAME)),
                None::<TableReference>,
                embedding_col!(self.search_column()),
            )),
            Expr::Alias(Alias::new(
                Expr::BinaryExpr(BinaryExpr::new(
                    Box::new(lit(1.0)),
                    Operator::Minus,
                    Box::new(Expr::Column(Column::new_unqualified("distance"))),
                )),
                None::<TableReference>,
                SEARCH_SCORE_COLUMN_NAME,
            )),
        ]);
        projection.extend(metadata_columns_to_exprs(&self.metadata_columns));

        table_with_projection(
            Arc::new(S3VectorsQueryTable::new(
                self.table.clone(),
                Arc::new(ComputeQuery {
                    model_name: self.model_name.clone(),
                    embedding_models: Arc::clone(&self.embedding_models),
                }),
                query.to_string(),
            )),
            projection,
        )
        .map(Arc::new)
    }
}

impl VectorIndex for S3Vector {
    fn dimension(&self) -> i32 {
        self.table
            .schema
            .column_with_name(S3_VECTOR_EMBEDDING_NAME)
            .map(|(_, f)| {
                match f.data_type() {
                    DataType::FixedSizeList(_, dim) => *dim,
                    _ => unreachable!("S3 vector index schema is missing a 'FixedSizeList' field named '{S3_VECTOR_EMBEDDING_NAME}'")
                }
            })
            .unwrap_or_default()
    }

    /// Use a [`S3VectorsListTable`] and then:
    ///   1. Convert the primary key to its appropriate name and data type
    ///   2. Rename [`S3_VECTOR_EMBEDDING_NAME`] appropriately
    fn list_table_provider(&self) -> Result<LogicalPlan, Box<dyn std::error::Error + Send + Sync>> {
        let mut projection: Vec<_> = metadata_columns_to_exprs(&self.metadata_columns);
        projection.extend(s3_vectors_primary_key_cast(&self.primary_fields()));
        projection.push(Expr::Alias(Alias::new(
            Expr::Column(datafusion::common::Column::new_unqualified(
                S3_VECTOR_EMBEDDING_NAME,
            )),
            None::<TableReference>,
            embedding_col!(self.search_column()),
        )));

        table_with_projection(
            Arc::new(S3VectorsListTable::from(self.table.clone())),
            projection,
        )
        .boxed()
    }
}

/// Convert a [`MetadataColumns`] into a set of [`Expr`]s suitable for a projection.
#[must_use]
pub(super) fn metadata_columns_to_exprs(metadata_columns: &MetadataColumns) -> Vec<Expr> {
    metadata_columns
        .iter()
        .map(|c| Expr::Column(Column::new_unqualified(c.name())))
        .collect()
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
        let mut pks: Vec<_> = self
            .primary_key
            .iter()
            .map(arrow_schema::Field::name)
            .cloned()
            .collect();
        pks.push(self.embedded_column.clone());
        pks.extend(
            self.metadata_columns
                .iter()
                .filter(|c| *c.name() != embedding_col!(self.embedded_column))
                .map(|c| c.name().to_string()),
        );

        pks
    }

    async fn compute_index(
        &self,
        batches: Vec<RecordBatch>,
    ) -> Result<Vec<RecordBatch>, DataFusionError> {
        let futs = batches
            .into_iter()
            .map(|rb| async { self.write(rb).await.map_err(DataFusionError::External) });
        try_join_all(futs).await
    }
}

fn table_with_projection(
    tbl: Arc<dyn TableProvider>,
    projection: Vec<Expr>,
) -> Result<LogicalPlan, DataFusionError> {
    Ok(LogicalPlan::Projection(Projection::try_new(
        projection,
        Arc::new(LogicalPlan::TableScan(TableScan::try_new(
            "tbl",
            Arc::new(DefaultTableSource::new(tbl)),
            None,
            vec![],
            None,
        )?)),
    )?))
}

/// For a given data type, determine the variant within the JSON `Union(_, Sparse)` that would be populated from the associated [`datafusion_functions_json::udfs::json_get_udf`].
fn data_type_to_union_variant(dt: &DataType) -> &str {
    match dt {
        DataType::Null => "null",
        DataType::Boolean => "bool",
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => "int",
        DataType::Float16 | DataType::Float32 | DataType::Float64 => "float",
        DataType::BinaryView | DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => "str",
        DataType::LargeList(_) | DataType::List(_) => "array",
        _ => "",
    }
}

pub fn s3_vectors_primary_key_cast(primary_key: &[Field]) -> Vec<Expr> {
    match primary_key {
        [f] => vec![Expr::Alias(Alias::new(
            Expr::Cast(Cast::new(
                Box::new(Expr::Column(Column::new_unqualified(
                    S3_VECTOR_PRIMARY_KEY_NAME,
                ))),
                f.data_type().clone(),
            )),
            None::<TableReference>,
            f.name().clone(),
        ))],
        [] => vec![],
        cols => cols
            .iter()
            .map(|f| {
                let col_name = f.name();
                let data_type = f.data_type().clone();
                Expr::Alias(Alias::new(
                    Expr::Cast(Cast::new(
                        Box::new(Expr::ScalarFunction(ScalarFunction {
                            func: Arc::new(ScalarUDF::new_from_impl(ArrowCastFunc::default())),
                            args: vec![
                                Expr::ScalarFunction(ScalarFunction {
                                    func: Arc::new(ScalarUDF::new_from_impl(
                                        UnionExtractFun::default(),
                                    )),
                                    args: vec![
                                        Expr::ScalarFunction(ScalarFunction {
                                            func: datafusion_functions_json::udfs::json_get_udf(),

                                            args: vec![
                                                Expr::Column(Column::new_unqualified(
                                                    S3_VECTOR_PRIMARY_KEY_NAME,
                                                )),
                                                Expr::Literal(
                                                    ScalarValue::Utf8(Some(col_name.clone())),
                                                    None,
                                                ),
                                            ],
                                        }),
                                        Expr::Literal(
                                            ScalarValue::Utf8(Some(
                                                data_type_to_union_variant(&data_type).to_string(),
                                            )),
                                            None,
                                        ),
                                    ],
                                }),
                                Expr::Literal(ScalarValue::Utf8(Some(data_type.to_string())), None),
                            ],
                        })),
                        data_type,
                    )),
                    None::<TableReference>,
                    col_name.clone(),
                ))
            })
            .collect(),
    }
}
