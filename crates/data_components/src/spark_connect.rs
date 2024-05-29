use std::fmt;
use std::sync::Arc;

use crate::{Read, ReadWrite};
use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use async_stream::stream;
use async_trait::async_trait;
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
    execution::context::SessionState,
    logical_expr::Expr,
    physical_plan::ExecutionPlan,
    sql::TableReference,
};
use futures::Stream;
use spark_connect_rs::{
    client::ChannelBuilder, functions::col, DataFrame, SparkSession, SparkSessionBuilder,
};
use sql_provider_datafusion::expr::{self, Engine};

use std::error::Error;

#[derive(Clone)]
#[allow(clippy::module_name_repetitions)]
pub struct SparkConnect {
    session: Arc<SparkSession>,
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
        Ok(Self { session })
    }
}

#[async_trait]
impl ReadWrite for SparkConnect {
    async fn table_provider(
        &self,
        table_reference: TableReference,
    ) -> Result<Arc<dyn TableProvider + 'static>, Box<dyn Error + Send + Sync>> {
        let provider = get_table_provider(Arc::clone(&self.session), table_reference).await?;
        Ok(provider)
    }
}

#[async_trait]
impl Read for SparkConnect {
    async fn table_provider(
        &self,
        table_reference: TableReference,
    ) -> Result<Arc<dyn TableProvider + 'static>, Box<dyn Error + Send + Sync>> {
        let provider = get_table_provider(Arc::clone(&self.session), table_reference).await?;
        Ok(provider)
    }
}

async fn get_table_provider(
    spark_session: Arc<SparkSession>,
    table_reference: TableReference,
) -> Result<Arc<dyn TableProvider + 'static>, Box<dyn Error + Send + Sync>> {
    let dataframe = spark_session.table(table_reference.to_string().as_str())?;
    let arrow_schema = dataframe.clone().limit(0).collect().await?.schema();
    Ok(Arc::new(SparkConnectTableProvider {
        dataframe,
        schema: arrow_schema,
    }))
}

struct SparkConnectTableProvider {
    dataframe: DataFrame,
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
        _state: &SessionState,
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

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
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
        let data = dataframe.collect().await.map_err(|e| DataFusionError::Execution(e.to_string()))?;
        yield(Ok(data))
    }
}
