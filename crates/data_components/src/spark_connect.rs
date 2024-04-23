use std::fmt;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::Field;
use arrow::datatypes::{self, Schema, SchemaRef, TimeUnit};
use async_stream::stream;
use async_trait::async_trait;
use datafusion::common::project_schema;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionMode};
use datafusion::physical_plan::{Partitioning, PlanProperties};
use datafusion::{
    common::OwnedTableReference,
    datasource::{TableProvider, TableType},
    error::Result,
    execution::context::SessionState,
    logical_expr::Expr,
    physical_plan::ExecutionPlan,
};
use futures::Stream;
use spark_connect_rs::{
    spark::{data_type, DataType},
    DataFrame, SparkSession,
};

use std::error::Error;

pub async fn get_table_provider(
    spark_session: Arc<SparkSession>,
    table_reference: OwnedTableReference,
) -> Result<Arc<dyn TableProvider + 'static>, Box<dyn Error + Send + Sync>> {
    if let Some(catalog_name) = table_reference.catalog() {
        let spark_session = Arc::clone(&spark_session);
        spark_session.setCatalog(catalog_name).collect().await?;
    }
    if let Some(database) = table_reference.schema() {
        let spark_session = Arc::clone(&spark_session);
        spark_session.setDatabase(database).collect().await?;
    }
    let dataframe = spark_session.table(table_reference.table())?;
    let schema = dataframe.clone().schema().await?;
    let arrow_schema = datatype_as_arrow_schema(schema)?;
    Ok(Arc::new(SparkConnectTablePovider {
        dataframe,
        schema: arrow_schema,
    }))
}

fn arrow_field_datatype_from_spark_connect_field_datatype(
    spark_connect_datatype: Option<DataType>,
) -> Result<datatypes::DataType, DataFusionError> {
    let kind = spark_connect_datatype.and_then(|datatype| datatype.kind);
    match kind {
        Some(data_type::Kind::Boolean(_)) => Ok(datatypes::DataType::Boolean),
        Some(data_type::Kind::Byte(_)) => Ok(datatypes::DataType::Int8),
        Some(data_type::Kind::Short(_)) => Ok(datatypes::DataType::Int16),
        Some(data_type::Kind::Integer(_)) => Ok(datatypes::DataType::Int32),
        Some(data_type::Kind::Long(_)) => Ok(datatypes::DataType::Int64),
        Some(data_type::Kind::Float(_)) => Ok(datatypes::DataType::Float32),
        Some(data_type::Kind::Double(_)) => Ok(datatypes::DataType::Float64),
        Some(data_type::Kind::String(_)) => Ok(datatypes::DataType::Utf8),
        Some(data_type::Kind::Binary(_)) => Ok(datatypes::DataType::Binary),
        Some(data_type::Kind::Date(_)) => Ok(datatypes::DataType::Date32),
        Some(data_type::Kind::Timestamp(_)) => {
            Ok(datatypes::DataType::Timestamp(TimeUnit::Millisecond, None))
        }
        Some(data_type::Kind::Array(boxed_array)) => {
            match boxed_array.element_type {
                Some(data_type) => {
                    let arrow_inner_type =
                        arrow_field_datatype_from_spark_connect_field_datatype(Some(*data_type))?;
                    // Very smelly
                    let field = Field::new("", arrow_inner_type, false);
                    Ok(datatypes::DataType::List(Arc::new(field)))
                }
                None => Err(DataFusionError::Execution(
                    "Unsupported data type".to_string(),
                )),
            }
        }
        Some(data_type::Kind::Map(boxed_map)) => match (boxed_map.key_type, boxed_map.value_type) {
            (Some(key_type), Some(value_type)) => {
                let arrow_key_type =
                    arrow_field_datatype_from_spark_connect_field_datatype(Some(*key_type))?;
                let arrow_value_type =
                    arrow_field_datatype_from_spark_connect_field_datatype(Some(*value_type))?;
                Ok(datatypes::DataType::Dictionary(
                    Box::new(arrow_key_type),
                    Box::new(arrow_value_type),
                ))
            }
            _ => Err(DataFusionError::Execution(
                "Unsupported data type".to_string(),
            )),
        },
        Some(data_type::Kind::Struct(struct_type)) => {
            let fields = struct_type
                .fields
                .iter()
                .map(|field| {
                    let field_datatype = arrow_field_datatype_from_spark_connect_field_datatype(
                        field.data_type.clone(),
                    )?;
                    let arrow_field =
                        Field::new(field.name.clone(), field_datatype, field.nullable);
                    Ok(arrow_field)
                })
                .collect::<Result<Vec<_>, DataFusionError>>()?;
            Ok(datatypes::DataType::Struct(fields.into()))
        }
        _ => Err(DataFusionError::Execution(
            "Unsupported data type".to_string(),
        )),
    }
}

fn datatype_as_arrow_schema(data_type: DataType) -> Result<SchemaRef, DataFusionError> {
    if let Some(data_type::Kind::Struct(arrow_struct)) = data_type.kind {
        let fields = arrow_struct
            .fields
            .iter()
            .map(|field| {
                let field_datatype = arrow_field_datatype_from_spark_connect_field_datatype(
                    field.data_type.clone(),
                )?;
                let arrow_field = Field::new(field.name.clone(), field_datatype, field.nullable);
                Ok(arrow_field)
            })
            .collect::<Result<Vec<_>, DataFusionError>>()?;
        return Ok(Arc::new(Schema::new(fields)));
    }
    Err(DataFusionError::Execution(
        "Unsupported data type".to_string(),
    ))
}

struct SparkConnectTablePovider {
    dataframe: DataFrame,
    schema: SchemaRef,
}

#[async_trait]
impl TableProvider for SparkConnectTablePovider {
    fn as_any(&self) -> &dyn std::any::Any {
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
    filters: Vec<Expr>,
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
            filters: filters.to_vec(),
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
        let filters: Vec<String> = self
            .filters
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>();
        write!(
            f,
            "SparkConnectExecutionPlan projected_schema={} filters={}",
            self.projected_schema,
            filters.join(",")
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
        let columns = self
            .projected_schema
            .fields()
            .iter()
            .map(|f| format!("\"{}\"", f.name()))
            .collect::<Vec<_>>();
        tracing::trace!("projected_schema {:#?}", self.projected_schema);
        tracing::trace!("sql columns {:#?}", columns);
        tracing::trace!("filters {:#?}", self.filters);
        let df = self
            .filters
            .iter()
            .fold(self.dataframe.clone(), |df, filter| {
                df.filter(filter.to_string().as_str())
            });
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
