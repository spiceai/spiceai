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
use arrow_schema::{DataType, SchemaRef};
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::{Constraints, Statistics};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::Result as DataFusionResult;
use datafusion::logical_expr::TableProviderFilterPushDown;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::Expr;
use snafu::Snafu;
use std::any::Any;
use std::sync::Arc;

use crate::search::util::get_primary_keys;

pub struct TableWithFullText {
    base_table: Arc<dyn TableProvider>,
    search_field: String,
    index: Arc<tantivy::Index>,
}

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("",))]
    Bad {
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    #[snafu(display("Full text search requires a primary key, and the table did not have one.",))]
    NoPrimaryKey,

    #[snafu(display("",))]
    PrimaryKeyInvalidType { column: String, data_type: DataType },
}

impl TableWithFullText {
    pub async fn try_new(
        inner: Arc<dyn TableProvider>,
        search_field: String,
        primary_key_override: Option<Vec<String>>,
    ) -> Result<Self, Error> {
        let pks = match (
            primary_key_override,
            get_primary_keys(Arc::clone(&inner)).await,
        ) {
            (Some(pks), _) => pks,
            (None, Ok(pks)) if !pks.is_empty() => pks,
            (None, _) => {
                return Err(Error::NoPrimaryKey);
            }
        };

        let index =
            Self::create_index(Arc::clone(&inner), search_field.as_str(), pks.as_slice()).await?;

        Ok(Self {
            base_table: inner,
            search_field,
            index,
        })
    }

    #[must_use] pub fn underlying_table(&self) -> Arc<dyn TableProvider> {
        Arc::clone(&self.base_table)
    }

    async fn create_index(
        base_table: Arc<dyn TableProvider>,
        search_field: &str,
        primary_key: &[String],
    ) -> Result<Arc<tantivy::Index>, Error> {
        let schema = base_table.schema();
        let mut schema_builder = tantivy::schema::Schema::builder();
        for p in primary_key {
            let Some((_, field)) = schema.column_with_name(p) else {
                continue;
            };
            match field.data_type() {
                DataType::Float16 | DataType::Float32 | DataType::Float64 => {
                    schema_builder.add_f64_field(p.as_str(), tantivy::schema::STORED);
                }
                DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
                    schema_builder.add_u64_field(p.as_str(), tantivy::schema::STORED);
                }
                DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
                    schema_builder.add_i64_field(p.as_str(), tantivy::schema::STORED);
                }
                DataType::Boolean => {
                    schema_builder.add_bool_field(p.as_str(), tantivy::schema::STORED);
                }

                DataType::Date32 | DataType::Date64 => {
                    schema_builder.add_date_field(p.as_str(), tantivy::schema::STORED);
                }
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
                    schema_builder.add_text_field(p.as_str(), tantivy::schema::STORED);
                }
                DataType::Binary | DataType::LargeBinary | DataType::BinaryView => {
                    schema_builder.add_bytes_field(p.as_str(), tantivy::schema::STORED);
                }
                dt => {
                    return Err(Error::PrimaryKeyInvalidType {
                        data_type: dt.clone(),
                        column: p.clone(),
                    });
                }
            }
        }
        schema_builder.add_text_field(
            search_field,
            tantivy::schema::STORED | tantivy::schema::TEXT,
        );
        let schema = schema_builder.build();
        let index = tantivy::Index::create_in_ram(schema);
        // let mut index_writer: tantivy::IndexWriter = index
        //     .writer(15_000_000) // cannot be less than 15_000_000 for in memory
        //     .expect("Failed to make index writer");

        // TODO write data.
        //
        Ok(Arc::new(index))
    }
}

impl std::fmt::Debug for TableWithFullText {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TableWithFullText")
            .field("base_table", &self.base_table)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl TableProvider for TableWithFullText {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn constraints(&self) -> Option<&Constraints> {
        self.base_table.constraints()
    }

    fn table_type(&self) -> TableType {
        self.base_table.table_type()
    }

    fn get_column_default(&self, column: &str) -> Option<&Expr> {
        self.base_table.get_column_default(column)
    }

    fn schema(&self) -> SchemaRef {
        self.base_table.schema()
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        self.base_table
            .scan(state, projection, filters, limit)
            .await
    }

    /// Any filter in [`filters`] can still be exact
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        self.base_table.supports_filters_pushdown(filters)
    }

    fn statistics(&self) -> Option<Statistics> {
        self.base_table.statistics()
    }

    async fn insert_into(
        &self,
        state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        overwrite: InsertOp,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        self.base_table.insert_into(state, input, overwrite).await
    }
}
