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

use async_trait::async_trait;
use bigdecimal::BigDecimal;
use snafu::{ResultExt, Snafu};

use arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion::{
    catalog::Session,
    datasource::{TableProvider, TableType},
    physical_plan::ExecutionPlan,
    prelude::Expr,
    sql::TableReference,
};

use std::{any::Any, sync::Arc};

use crate::oracle::{
    connection::OracleConnectionPool, convert::map_oracle_type_to_arrow_type,
    execution_plan::OracleExecPlan,
};
pub mod connection;
mod convert;
mod execution_plan;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to establish connection: {source}"))]
    ConnectionError { source: oracle::Error },

    #[snafu(display("Failed to establish connection: {source}"))]
    ConnectionPoolError {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Error executing query: {source}"))]
    QueryError { source: oracle::Error },

    #[snafu(display("Failed to retrieve table schema: {source}"))]
    SchemaRetrieval { source: oracle::Error },

    #[snafu(display("Failed to retrieve schema: table '{table}' does not exist"))]
    SchemaRetrievalTableNotFound { table: String },

    #[snafu(display("Failed to retrieve schema for '{table}': catalogs are not supported"))]
    SchemaRetrievalCatalogsUnsupported { table: String },

    #[snafu(display("Unsupported data type: {data_type}"))]
    UnsupportedType { data_type: String },

    #[snafu(display("Failed to build record batch: {source}"))]
    FailedToBuildRecordBatch { source: arrow::error::ArrowError },

    #[snafu(display("No column found for index {index}"))]
    NoColumnForIndex { index: usize },

    #[snafu(display("Failed to downcast builder for column '{column}' of type '{native_type}'"))]
    FailedToDowncastBuilder { native_type: String, column: String },

    #[snafu(display(
        "Failed to retrieve value of type '{native_type}' for column '{column}': {source}"
    ))]
    FailedToRetrieveValue {
        source: Box<dyn std::error::Error + Send + Sync>,
        native_type: String,
        column: String,
    },

    #[snafu(display("Cannot represent BigDecimal as i128: {big_decimal}"))]
    FailedToConvertBigDecimalToI128 { big_decimal: BigDecimal },

    #[snafu(display("Failed to parse decimal string as BigInterger {value}: {source}"))]
    FailedToParseBigDecimal {
        value: String,
        source: bigdecimal::ParseBigDecimalError,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug)]
pub struct OracleTableProvider {
    conn: Arc<OracleConnectionPool>,
    schema: SchemaRef,
    table: TableReference,
}

impl OracleTableProvider {
    pub async fn new(conn: Arc<OracleConnectionPool>, table: &TableReference) -> Result<Self> {
        let schema = Self::get_schema(Arc::clone(&conn), table).await?;

        Ok(Self {
            conn,
            schema,
            table: table.clone(),
        })
    }

    pub async fn get_schema(
        conn: Arc<OracleConnectionPool>,
        table: &TableReference,
    ) -> Result<SchemaRef> {
        if table.catalog().is_some() {
            return Err(Error::SchemaRetrievalCatalogsUnsupported {
                table: table.to_string(),
            });
        }

        let table_name = table.table();

        let (columns_meta_query, params) = match table.schema() {
            Some(schema_name) => (
                "SELECT COLUMN_NAME, DATA_TYPE, DATA_PRECISION, DATA_SCALE \
                    FROM ALL_TAB_COLUMNS \
                    WHERE TABLE_NAME = :1 AND OWNER = :2"
                    .to_string(),
                vec![table_name, schema_name],
            ),
            // In Oracle, the default schema is the user's schema that is used to connect when no specific schema is provided in a SQL statement.
            // We use SYS_CONTEXT to get the current schema name.
            None => (
                "SELECT COLUMN_NAME, DATA_TYPE, DATA_PRECISION, DATA_SCALE \
                    FROM ALL_TAB_COLUMNS \
                    WHERE TABLE_NAME = :1 AND OWNER = SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA')"
                    .to_string(),
                vec![table_name],
            ),
        };

        tracing::debug!("Executing schema query for dataset {table}:\n{columns_meta_query}");

        let conn = conn.get().await?;

        let params: Vec<&dyn oracle::sql_type::ToSql> = params
            .iter()
            .map(|s| s as &dyn oracle::sql_type::ToSql)
            .collect();

        let query_res = conn
            .query(&columns_meta_query, &params)
            .context(QuerySnafu)?;

        let mut fields = Vec::new();

        for row_result in query_res {
            let row = row_result.context(QuerySnafu)?;

            let column_name: String = row.get(0).context(SchemaRetrievalSnafu)?;
            let data_type: String = row.get(1).context(SchemaRetrievalSnafu)?;
            let numeric_precision: Option<u8> = row.get(2).context(SchemaRetrievalSnafu)?;
            let numeric_scale: Option<i8> = row.get(3).context(SchemaRetrievalSnafu)?;

            let Some(arrow_data_type) =
                map_oracle_type_to_arrow_type(&data_type, numeric_precision, numeric_scale)
            else {
                tracing::warn!(
                    "Column '{column_name}' of dataset {table} has unsupported data type '{data_type}' and will be ignored"
                );
                continue;
            };

            fields.push(Field::new(column_name, arrow_data_type, true));
        }

        if fields.is_empty() {
            return Err(Error::SchemaRetrievalTableNotFound {
                table: table.to_string(),
            });
        }

        tracing::trace!("Retrieved dataset {table} schema: {fields:?}");

        Ok(Arc::new(Schema::new(fields)))
    }
}

#[async_trait]
impl TableProvider for OracleTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(OracleExecPlan::new(
            projection,
            &self.schema,
            &self.table,
            Arc::clone(&self.conn),
            filters,
            limit,
        )?))
    }
}
