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
use arrow_schema::Field;
use async_openai::types::EmbeddingInput;
use async_trait::async_trait;
use data_components::s3_vectors::{
    MetadataColumns, S3_VECTOR_EMBEDDING_NAME, S3_VECTOR_PRIMARY_KEY_NAME, S3VectorsTable,
    list_provider::S3VectorsListTable, query_provider::S3VectorsQueryTable,
};
use llms::embeddings::Embed;
use runtime_datafusion_index::Index;
use search::SEARCH_SCORE_COLUMN_NAME;
use snafu::ResultExt;

use crate::{
    embedding_col,
    embeddings::index::{VectorIndex, query_table::metadata_columns_to_exprs, s3::write},
    model::EmbeddingModelStore,
};
use datafusion::{
    catalog::TableProvider,
    common::Column,
    datasource::{DefaultTableSource, ViewTable},
    error::DataFusionError,
    logical_expr::{BinaryExpr, Cast, LogicalPlan, Operator, Projection, TableScan, expr::Alias},
    prelude::{Expr, lit},
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

    pub async fn embedding_model(&self) -> Option<Arc<dyn Embed>> {
        let model_lock = self.embedding_models.read().await;
        let model = model_lock.get(&self.model_name)?;
        Some(Arc::clone(model))
    }

    pub async fn query_vector(
        &self,
        query: &str,
    ) -> Result<Vec<f32>, Box<dyn std::error::Error + Send + Sync>> {
        let models = self.embedding_models.read().await;
        let Some(embedding_model) = models.get(&self.model_name) else {
            return Err(Box::from(format!(
                "Vector index requires '{}' embedding model, but is not available.",
                self.model_name
            )));
        };
        let mut resp = embedding_model
            .embed(EmbeddingInput::String(query.to_string()))
            .await
            .boxed()?;
        let Some(query_vector) = resp.pop() else {
            return Err(Box::from(format!(
                "Embedding model '{}' produced no embedding for the query '{query}'.",
                self.model_name,
            )));
        };

        Ok(query_vector)
    }
}

#[async_trait]
impl VectorIndex for S3Vector {
    fn embedded_column(&self) -> String {
        self.embedded_column.clone()
    }

    fn primary_fields(&self) -> Vec<Field> {
        self.primary_key.clone()
    }

    /// Use a [`S3VectorsListTable`] and then:
    ///   1. Convert the primary key to its appropriate name and data type
    ///   2. Rename [`S3_VECTOR_EMBEDDING_NAME`] appropriately
    fn list_table_provider(
        &self,
    ) -> Result<Arc<dyn TableProvider>, Box<dyn std::error::Error + Send + Sync>> {
        let Some((pk_name, pk_data_type)) = self
            .primary_fields()
            .first()
            .map(|f| (f.name().clone(), f.data_type().clone()))
        else {
            return Err(Box::from(
                "Vector indexes defined without a primary key cannot be used for retrieving vectors"
                    .to_string(),
            ));
        };

        let mut projection = metadata_columns_to_exprs(self.metadata_columns());
        projection.extend(vec![
            Expr::Alias(Alias::new(
                Expr::Cast(Cast::new(
                    Box::new(Expr::Column(datafusion::common::Column::new_unqualified(
                        S3_VECTOR_PRIMARY_KEY_NAME,
                    ))),
                    pk_data_type,
                )),
                None::<TableReference>,
                pk_name,
            )),
            Expr::Alias(Alias::new(
                Expr::Column(datafusion::common::Column::new_unqualified(
                    S3_VECTOR_EMBEDDING_NAME,
                )),
                None::<TableReference>,
                embedding_col!(self.embedded_column()),
            )),
        ]);

        table_with_projection(
            Arc::new(S3VectorsListTable::from(self.table.clone())),
            projection,
        )
        .boxed()
    }

    fn metadata_columns(&self) -> &MetadataColumns {
        &self.metadata_columns
    }

    async fn write(&self, record: &RecordBatch) {
        write::write(self, record).await
    }

    async fn query_table_provider(
        &self,
        query: &str,
    ) -> Result<Arc<dyn TableProvider>, Box<dyn std::error::Error + Send + Sync>> {
        let Some((pk_name, pk_data_type)) = self
            .primary_fields()
            .first()
            .map(|f| (f.name().clone(), f.data_type().clone()))
        else {
            return Err(Box::from(
                "Vector indexes defined without a primary key cannot be used for querying vectors"
                    .to_string(),
            ));
        };

        let mut projection = vec![
            Expr::Alias(Alias::new(
                Expr::Cast(Cast::new(
                    Box::new(Expr::Column(Column::new_unqualified(
                        S3_VECTOR_PRIMARY_KEY_NAME,
                    ))),
                    pk_data_type.clone(),
                )),
                None::<TableReference>,
                pk_name,
            )),
            Expr::Alias(Alias::new(
                Expr::Column(Column::new_unqualified(S3_VECTOR_EMBEDDING_NAME)),
                None::<TableReference>,
                embedding_col!(self.embedded_column()),
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
        ];
        projection.extend(metadata_columns_to_exprs(self.metadata_columns()));

        // TODO: Restructure [`S3VectorsQueryTable`] to take an async function (probably a trait)
        // like `async fn(&str) -> vec<f32>`, to avoid early embedding request.
        let vector = self.query_vector(query).await?;
        let tp = Arc::new(S3VectorsQueryTable::new(self.table.clone(), vector));

        table_with_projection(tp, projection).boxed()
    }
}

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
        pks.extend(self.metadata_columns.iter().map(|c| c.name().to_string()));

        pks
    }
}

fn table_with_projection(
    tbl: Arc<dyn TableProvider>,
    projection: Vec<Expr>,
) -> Result<Arc<dyn TableProvider>, DataFusionError> {
    let scan = TableScan::try_new(
        "tbl",
        Arc::new(DefaultTableSource::new(tbl)),
        None,
        vec![],
        None,
    )?;
    Ok(Arc::new(ViewTable::new(
        LogicalPlan::Projection(Projection::try_new(
            projection,
            Arc::new(LogicalPlan::TableScan(scan)),
        )?),
        None,
    )) as Arc<dyn TableProvider>)
}
