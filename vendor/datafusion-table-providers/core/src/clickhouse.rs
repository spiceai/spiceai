/*
Copyright 2024 The Spice.ai OSS Authors

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

use clickhouse::Client;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::TableProvider;
use datafusion::sql::sqlparser::ast::{Expr, Value};
use datafusion::sql::unparser;
use datafusion::{common::Constraints, sql::TableReference};
use std::sync::Arc;

use crate::sql::db_connection_pool::clickhousepool::ClickHouseConnectionPool;
use crate::sql::db_connection_pool::dbconnection::AsyncDbConnection;

#[cfg(feature = "clickhouse-federation")]
mod federation;
mod sql_table;

pub struct ClickHouseTableFactory {
    pool: Arc<ClickHouseConnectionPool>,
}

impl ClickHouseTableFactory {
    pub fn new(pool: impl Into<Arc<ClickHouseConnectionPool>>) -> Self {
        Self { pool: pool.into() }
    }

    pub async fn table_provider(
        &self,
        table_reference: TableReference,
        args: Option<Vec<(String, Arg)>>,
    ) -> Result<Arc<dyn TableProvider + 'static>, Box<dyn std::error::Error + Send + Sync + 'static>>
    {
        let client: &dyn AsyncDbConnection<Client, ()> = &self.pool.client();
        let schema = client.get_schema(&table_reference).await?;
        let table_provider = Arc::new(ClickHouseTable::new(
            table_reference,
            args,
            self.pool.clone(),
            schema,
            Constraints::default(),
        ));

        #[cfg(feature = "clickhouse-federation")]
        let table_provider = Arc::new(
            table_provider
                .create_federated_table_provider()
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?,
        );

        Ok(table_provider)
    }
}

#[derive(Debug, Clone)]
pub enum Arg {
    Unsigned(u64),
    Signed(i64),
    String(String),
}

impl From<String> for Arg {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

impl From<u64> for Arg {
    fn from(value: u64) -> Self {
        Self::Unsigned(value)
    }
}

impl From<i64> for Arg {
    fn from(value: i64) -> Self {
        Self::Signed(value)
    }
}

impl From<Arg> for Expr {
    fn from(value: Arg) -> Self {
        Expr::value(match value {
            Arg::Unsigned(x) => Value::Number(x.to_string(), false),
            Arg::Signed(x) => Value::Number(x.to_string(), false),
            Arg::String(x) => Value::SingleQuotedString(x),
        })
    }
}

pub struct ClickHouseTable {
    table_reference: TableReference,
    /// The actual table expression to use in SQL queries
    /// For regular tables: same as table_reference
    /// For parameterized views: includes function call syntax like "view_name(param='value')"
    table_expr: String,
    pool: Arc<ClickHouseConnectionPool>,
    schema: SchemaRef,
    constraints: Constraints,
    #[allow(dead_code)] // Used by federation feature
    dialect: Arc<dyn unparser::dialect::Dialect>,
}

impl std::fmt::Debug for ClickHouseTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClickHouseTable")
            .field("table_name", &self.table_reference)
            .field("table_expr", &self.table_expr)
            .field("schema", &self.schema)
            .field("constraints", &self.constraints)
            .finish()
    }
}

impl ClickHouseTable {
    pub fn new(
        table_reference: TableReference,
        args: Option<Vec<(String, Arg)>>,
        pool: Arc<ClickHouseConnectionPool>,
        schema: SchemaRef,
        constraints: Constraints,
    ) -> Self {
        // For parameterized views, compute the table expression once at construction
        let table_expr = if let Some(args) = args {
            let params = args
                .iter()
                .map(|(name, value)| {
                    let val_str = match value {
                        Arg::Unsigned(x) => x.to_string(),
                        Arg::Signed(x) => x.to_string(),
                        Arg::String(x) => format!("'{}'", x.replace('\'', "\\'")),
                    };
                    format!("{}={}", name, val_str)
                })
                .collect::<Vec<_>>()
                .join(", ");
            format!("{}({})", table_reference, params)
        } else {
            table_reference.to_string()
        };

        Self {
            table_reference,
            table_expr,
            pool,
            schema,
            constraints,
            dialect: Arc::new(unparser::dialect::DefaultDialect {}),
        }
    }
}
