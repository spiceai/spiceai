use std::f64::consts::E;
use std::sync::Arc;

use arrow::datatypes::Field;
use arrow::datatypes::{self, Schema, SchemaRef, TimeUnit};
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::{
    common::OwnedTableReference,
    datasource::{TableProvider, TableType},
    error::Result,
    execution::context::SessionState,
    logical_expr::Expr,
    physical_plan::ExecutionPlan,
};
use spark_connect_rs::{
    spark::{data_type, DataType},
    DataFrame, SparkSession,
};
use tonic::async_trait;

use std::{collections::HashMap, error::Error};

pub async fn get_table_provider(
    spark_session: SparkSession,
    table_reference: OwnedTableReference,
) -> Result<Arc<dyn TableProvider + 'static>, Box<dyn Error + Send + Sync>> {
    let dataframe = spark_session.table(table_reference.table())?;
    let schema = dataframe.clone().schema().await?;
    let arrow_schema = datatype_as_arrow_schema(schema)?;
    Ok(Arc::new(
        SparkConnectTablePovider{
        dataframe: dataframe,
        schema: arrow_schema,
        table_reference,
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
                    let field = Field::new("",arrow_inner_type, false); 
                    Ok(datatypes::DataType::List(Arc::new(field)))
                }, 
                None => Err(DataFusionError::Execution(
                    "Unsupported data type".to_string(),
                )),
            }

        }
        Some(data_type::Kind::Map(boxed_map)) => {
            match (boxed_map.key_type, boxed_map.value_type) {
                (Some(key_type), Some(value_type)) => {
                    let arrow_key_type =
                        arrow_field_datatype_from_spark_connect_field_datatype(Some(*key_type))?;
                    let arrow_value_type =
                        arrow_field_datatype_from_spark_connect_field_datatype(Some(*value_type))?;
                    Ok(datatypes::DataType::Dictionary(
                        Box::new(arrow_key_type),
                        Box::new(arrow_value_type),
                    ))
                },
                _  => Err(DataFusionError::Execution(
                    "Unsupported data type".to_string(),
                )),
            }
        }
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
                let field_datatype =
                    arrow_field_datatype_from_spark_connect_field_datatype(field.data_type.clone())?;
                let arrow_field = Field::new(field.name.clone(), field_datatype, field.nullable);
                Ok(arrow_field)
            })
            .collect::<Result<Vec<_>, DataFusionError>>()?;
        return Ok(Arc::new(Schema::new(fields)));
    }
    return Err(DataFusionError::Execution(
        "Unsupported data type".to_string(),
    ));
}

struct SparkConnectTablePovider {
    dataframe: DataFrame,
    schema: SchemaRef,
    table_reference: OwnedTableReference,
}

#[async_trait]
impl TableProvider for SparkConnectTablePovider {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        return self.schema.clone();
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
        todo!();
    }
}
