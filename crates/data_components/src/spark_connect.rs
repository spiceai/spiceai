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

use std::fmt;
use std::sync::Arc;

use crate::Read;
use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use async_stream::stream;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::project_schema;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::TableProviderFilterPushDown;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionMode};
use datafusion::physical_plan::{Partitioning, PlanProperties};
use datafusion::{
    datasource::{TableProvider, TableType},
    error::Result,
    logical_expr::Expr,
    physical_plan::ExecutionPlan,
    sql::TableReference,
};
use datafusion_table_providers::sql::sql_provider_datafusion::expr::{self, Engine};
use futures::Stream;
use spark_connect_rs::errors::SparkError;
use spark_connect_rs::{
    client::ChannelBuilder, functions::col, DataFrame, SparkSession, SparkSessionBuilder,
};

use std::error::Error;

pub mod federation;

#[derive(Clone)]
pub struct SparkConnect {
    session: Arc<SparkSession>,
    join_push_down_context: String,
}

impl SparkConnect {
    pub fn validate_connection_string(
        connection: &str,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        ChannelBuilder::parse_connection_string(connection)?;
        Ok(())
    }

    pub async fn from_connection(connection: &str) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let session = Arc::new(SparkSessionBuilder::remote(connection)?.build().await?);

        let (host, port, options) = ChannelBuilder::parse_connection_string(connection)?;
        let options = options.unwrap_or_default();

        // it's safe to use default value here for options as session is already established.
        // ignore token and session_id
        let join_push_down_context = format!(
            "sc://{}:{}/;user_id={};x-databricks-cluster-id={};use_ssl={}",
            host,
            port,
            options.get("user_id").cloned().unwrap_or_default(),
            options
                .get("x-databricks-cluster-id")
                .cloned()
                .unwrap_or_default(),
            options.get("use_ssl").cloned().unwrap_or_default()
        );

        Ok(Self {
            session,
            join_push_down_context,
        })
    }
}

#[async_trait]
impl Read for SparkConnect {
    async fn table_provider(
        &self,
        table_reference: TableReference,
        schema: Option<SchemaRef>,
    ) -> Result<Arc<dyn TableProvider + 'static>, Box<dyn Error + Send + Sync>> {
        let provider = get_table_provider(
            Arc::clone(&self.session),
            table_reference,
            schema,
            self.join_push_down_context.clone(),
        )
        .await?;
        let provider = Arc::new(
            provider
                .create_federated_table_provider()
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?,
        );
        Ok(provider)
    }
}

async fn get_table_provider(
    spark_session: Arc<SparkSession>,
    table_reference: TableReference,
    schema: Option<SchemaRef>,
    join_push_down_context: String,
) -> Result<Arc<SparkConnectTableProvider>, Box<dyn Error + Send + Sync>> {
    let spark_table_reference = match table_reference {
        TableReference::Bare { ref table } => format!("`{table}`"),
        TableReference::Partial {
            ref table,
            ref schema,
        } => format!("`{schema}`.`{table}`"),
        TableReference::Full {
            ref catalog,
            ref schema,
            ref table,
        } => {
            format!("`{catalog}`.`{schema}`.`{table}`")
        }
    };
    let dataframe = spark_session.table(spark_table_reference.as_str())?;
    let arrow_schema = match schema {
        Some(schema) => schema,
        None => dataframe.clone().limit(0).collect().await?.schema(),
    };
    Ok(Arc::new(SparkConnectTableProvider {
        dataframe,
        table_reference: spark_table_reference.into(),
        join_push_down_context,
        schema: arrow_schema,
    }))
}

#[derive(Debug)]
struct SparkConnectTableProvider {
    table_reference: TableReference,
    dataframe: DataFrame,
    join_push_down_context: String,
    schema: SchemaRef,
}

#[async_trait]
impl TableProvider for SparkConnectTableProvider {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        let mut filter_push_down = vec![];
        for filter in filters {
            match expr::to_sql(filter) {
                Ok(_) => filter_push_down.push(TableProviderFilterPushDown::Exact),
                Err(_) => filter_push_down.push(TableProviderFilterPushDown::Unsupported),
            }
        }

        Ok(filter_push_down)
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(SparkConnectExecutionPlan::new(
            self.dataframe.clone(),
            Arc::clone(&self.schema),
            projection,
            filters,
            limit,
        )?))
    }
}

#[derive(Debug)]
struct SparkConnectExecutionPlan {
    dataframe: DataFrame,
    projected_schema: SchemaRef,
    filters: Vec<String>,
    limit: Option<i32>,
    properties: PlanProperties,
}

impl SparkConnectExecutionPlan {
    #[allow(clippy::needless_pass_by_value)]
    pub fn new(
        dataframe: DataFrame,
        schema: SchemaRef,
        projections: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Self> {
        let projected_schema = project_schema(&schema, projections)?;
        let limit = limit
            .map(|u| {
                let Ok(u) = u32::try_from(u) else {
                    return Err(DataFusionError::Execution(
                        "Value is too large to fit in a u32".to_string(),
                    ));
                };
                if let Ok(u) = i32::try_from(u) {
                    Ok(u)
                } else {
                    Err(DataFusionError::Execution(
                        "Value is too large to fit in an i32".to_string(),
                    ))
                }
            })
            .transpose()?;
        Ok(Self {
            dataframe,
            projected_schema: Arc::clone(&projected_schema),
            filters: filters
                .iter()
                .map(|f| expr::to_sql_with_engine(f, Some(Engine::Spark)))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| DataFusionError::Execution(e.to_string()))?,
            limit,
            properties: PlanProperties::new(
                EquivalenceProperties::new(projected_schema),
                Partitioning::UnknownPartitioning(1),
                ExecutionMode::Bounded,
            ),
        })
    }
}

impl DisplayAs for SparkConnectExecutionPlan {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> std::fmt::Result {
        let columns = self
            .projected_schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect::<Vec<_>>();
        let filters = self
            .filters
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>();
        write!(
            f,
            "SparkConnectExecutionPlan projection=[{}] filters=[{}]",
            columns.join(", "),
            filters.join(", "),
        )
    }
}

impl ExecutionPlan for SparkConnectExecutionPlan {
    fn name(&self) -> &'static str {
        "SparkConnectExecutionPlan"
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.projected_schema)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let filtered_columns = self
            .projected_schema
            .fields()
            .iter()
            .map(|f| col(f.name()))
            .collect::<Vec<_>>();
        tracing::trace!("projected_schema {:#?}", self.projected_schema);
        tracing::trace!("sql columns {:#?}", filtered_columns);
        tracing::trace!("filters {:#?}", self.filters);
        let df = self
            .filters
            .iter()
            .fold(self.dataframe.clone(), |df, filter| {
                df.filter(filter.as_str())
            })
            .select(filtered_columns);
        let df = match self.limit {
            Some(limit) => df.limit(limit),
            None => df,
        };
        let stream_adapter = RecordBatchStreamAdapter::new(self.schema(), dataframe_to_stream(df));
        Ok(Box::pin(stream_adapter))
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

fn dataframe_to_stream(dataframe: DataFrame) -> impl Stream<Item = DataFusionResult<RecordBatch>> {
    stream! {
        let data = dataframe
            .collect()
            .await
            .map_err(map_error_to_datafusion_err)?;
        yield (Ok(data))
    }
}

fn map_error_to_datafusion_err(e: SparkError) -> datafusion::error::DataFusionError {
    datafusion::error::DataFusionError::External(Box::new(e))
}
