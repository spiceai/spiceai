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

use arrow::datatypes::{DataType, SchemaRef};
use async_trait::async_trait;

use std::{any::Any, collections::HashMap, pin::Pin, sync::Arc};

use crate::component::dataset::Dataset;
use datafusion::{
    config::ConfigOptions,
    datasource::{TableProvider, TableType},
    error::{DataFusionError, Result as DataFusionResult},
    execution::context::SessionState,
    logical_expr::{AggregateUDF, Expr, ScalarUDF, TableSource, WindowUDF},
    physical_plan::{empty::EmptyExec, ExecutionPlan},
    sql::{
        planner::{ContextProvider, SqlToRel},
        sqlparser::{self, ast::Statement, dialect::PostgreSqlDialect, parser::Parser},
        TableReference,
    },
};
use futures::Future;
use secrets::Secret;
use snafu::prelude::*;

use super::{DataConnector, DataConnectorFactory};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(r#"Missing required parameter "schema": The localhost connector requires specifying the schema up-front as a SQL CREATE TABLE statement."#))]
    MissingSchemaParameter,

    #[snafu(display(
        "Unable to parse schema as a valid SQL statement: {source}\nSchema:\n{schema}"
    ))]
    UnableToParseSchema {
        schema: String,
        source: sqlparser::parser::ParserError,
    },

    #[snafu(display("Schema must be a single SQL statement"))]
    OneStatementExpected,

    #[snafu(display("Schema must be specified as a CREATE TABLE statement"))]
    CreateTableStatementExpected,

    #[snafu(display("Unable to parse schema from column definitions: {source}"))]
    UnableToParseSchemaFromColumnDefinitions { source: DataFusionError },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// A no-op connector that allows for Spice to act as a "sink" for data.
///
/// Configure an accelerator to store data - the localhost connector itself does nothing.
#[allow(clippy::module_name_repetitions)]
#[derive(Debug, Clone)]
pub struct LocalhostConnector {
    schema: SchemaRef,
}

impl LocalhostConnector {
    #[must_use]
    pub fn new(schema: SchemaRef) -> Self {
        Self { schema }
    }
}

impl DataConnectorFactory for LocalhostConnector {
    fn create(
        _secret: Option<Secret>,
        params: Arc<HashMap<String, String>>,
    ) -> Pin<Box<dyn Future<Output = super::NewDataConnectorResult> + Send>> {
        Box::pin(async move {
            let schema = params.get("schema").ok_or(Error::MissingSchemaParameter)?;

            let statements = Parser::parse_sql(&PostgreSqlDialect {}, schema).context(
                UnableToParseSchemaSnafu {
                    schema: schema.clone(),
                },
            )?;
            ensure!(statements.len() == 1, OneStatementExpectedSnafu);

            let statement = statements[0].clone();

            let columns = match statement {
                Statement::CreateTable { columns, .. } => columns,
                _ => CreateTableStatementExpectedSnafu.fail()?,
            };

            let schema = SqlToRel::new(&LocalhostContextProvider::new())
                .build_schema(columns)
                .context(UnableToParseSchemaFromColumnDefinitionsSnafu)?;

            Ok(Arc::new(LocalhostConnector {
                schema: Arc::new(schema),
            }) as Arc<dyn DataConnector>)
        })
    }
}

#[async_trait]
impl DataConnector for LocalhostConnector {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn read_provider(
        &self,
        _dataset: &Dataset,
    ) -> super::DataConnectorResult<Arc<dyn TableProvider>> {
        Ok(Arc::new(self.clone()))
    }

    async fn read_write_provider(
        &self,
        _dataset: &Dataset,
    ) -> Option<super::DataConnectorResult<Arc<dyn TableProvider>>> {
        Some(Ok(Arc::new(self.clone())))
    }
}

struct LocalhostContextProvider {
    options: ConfigOptions,
}

impl LocalhostContextProvider {
    pub fn new() -> Self {
        Self {
            options: ConfigOptions::default(),
        }
    }
}

impl ContextProvider for LocalhostContextProvider {
    fn get_table_source(&self, _name: TableReference) -> DataFusionResult<Arc<dyn TableSource>> {
        Err(DataFusionError::NotImplemented(
            "LocalhostContextProvider::get_table_source".to_string(),
        ))
    }

    fn get_function_meta(&self, _name: &str) -> Option<Arc<ScalarUDF>> {
        None
    }

    fn get_aggregate_meta(&self, _name: &str) -> Option<Arc<AggregateUDF>> {
        None
    }

    fn get_variable_type(&self, _variable_names: &[String]) -> Option<DataType> {
        None
    }

    fn options(&self) -> &ConfigOptions {
        &self.options
    }

    fn get_window_meta(&self, _name: &str) -> Option<Arc<WindowUDF>> {
        None
    }

    fn udf_names(&self) -> Vec<String> {
        Vec::new()
    }

    fn udaf_names(&self) -> Vec<String> {
        Vec::new()
    }

    fn udwf_names(&self) -> Vec<String> {
        Vec::new()
    }
}

#[async_trait]
impl TableProvider for LocalhostConnector {
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
        _state: &SessionState,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(EmptyExec::new(Arc::clone(&self.schema))))
    }

    async fn insert_into(
        &self,
        _state: &SessionState,
        _input: Arc<dyn ExecutionPlan>,
        _overwrite: bool,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(EmptyExec::new(Arc::clone(&self.schema))))
    }
}
