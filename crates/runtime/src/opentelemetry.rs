use std::i64::MAX;
use std::net::SocketAddr;
use std::sync::Arc;

use arrow::array::ArrayBuilder;
use arrow::array::ArrayRef;
use arrow::array::BinaryBuilder;
use arrow::array::BooleanBuilder;
use arrow::array::Float64Builder;
use arrow::array::Int64Builder;
use arrow::array::StringBuilder;
use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use indexmap::IndexMap;
use opentelemetry_proto::tonic::collector::metrics::v1::metrics_service_server::MetricsService;
use opentelemetry_proto::tonic::collector::metrics::v1::metrics_service_server::MetricsServiceServer;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsPartialSuccess;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceResponse;
use opentelemetry_proto::tonic::common::v1::any_value;
use opentelemetry_proto::tonic::common::v1::KeyValue;
use opentelemetry_proto::tonic::metrics::v1::metric::Data;
use opentelemetry_proto::tonic::metrics::v1::number_data_point::Value;
use opentelemetry_proto::tonic::metrics::v1::NumberDataPoint;
use snafu::prelude::*;
use tonic_0_9_0::async_trait;
use tonic_0_9_0::codec::CompressionEncoding;
use tonic_0_9_0::transport::Server;
use tonic_0_9_0::Request;
use tonic_0_9_0::Response;
use tonic_0_9_0::Status;

use crate::datafusion::DataFusion;
use crate::dataupdate::DataUpdate;
use crate::dataupdate::UpdateType;

type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to serve"))]
    UnableToServe {
        source: tonic_0_9_0::transport::Error,
    },

    #[snafu(display("Failed to build record batch from OpenTelemetry metrics: {}", source))]
    FailedToBuildRecordBatch { source: arrow::error::ArrowError },

    #[snafu(display("Unsupported metric data type"))]
    UnsupportedMetricDataType {},

    #[snafu(display("Unsupported metric attribute type"))]
    UnsupportedMetricAttributeType {},

    #[snafu(display("Metric with no data points"))]
    MetricWithNoDataPoints {},
}

const VALUE_COLUMN_NAME: &str = "value";

pub struct Service {
    data_fusion: Arc<DataFusion>,
}

#[async_trait]
impl MetricsService for Service {
    async fn export(
        &self,
        request: Request<ExportMetricsServiceRequest>,
    ) -> std::result::Result<Response<ExportMetricsServiceResponse>, Status> {
        let mut rejected_data_points = 0;
        let mut total_data_points = 0;
        for resource_metric in request.into_inner().resource_metrics {
            for scope_metric in resource_metric.scope_metrics {
                for metric in scope_metric.metrics {
                    if let Some(data) = metric.data {
                        let schema = match self
                            .data_fusion
                            .get_arrow_schema(metric.name.as_str())
                            .await
                        {
                            Ok(schema) => Some(schema),
                            Err(_) => None,
                        };
                        let (record_batch_result, data_points_count) =
                            metric_data_to_record_batch(metric.name.as_str(), &data, &schema);
                        total_data_points += data_points_count;

                        match record_batch_result {
                            Ok(record_batch) => {
                                let Some(backend) =
                                    self.data_fusion.get_backend(metric.name.as_str())
                                else {
                                    tracing::warn!(
                                        "No dataset defined for metric {}, skipping",
                                        metric.name
                                    );
                                    rejected_data_points += data_points_count;
                                    continue;
                                };

                                let add_data_future = backend.add_data(DataUpdate {
                                    log_sequence_number: None,
                                    data: vec![record_batch],
                                    update_type: UpdateType::Append,
                                });
                                // We need to await the Future here in case it adds new columns to the schema and later metrics will need
                                // to respect that schema.
                                if let Err(e) = add_data_future.await {
                                    rejected_data_points += data_points_count;
                                    tracing::error!(
                                        "Failed to add OpenTelemetry data to backend: {}",
                                        e
                                    );
                                }
                            }
                            Err(e) => {
                                tracing::error!(
                                    "Failed to build arrow data from OpenTelemetry metrics: {}",
                                    e
                                );
                            }
                        }
                    }
                }
            }
        }

        if rejected_data_points >= total_data_points {
            return Err(Status::invalid_argument("All data points were rejected"));
        }

        let partial_success = if rejected_data_points == 0 {
            None
        } else {
            Some(ExportMetricsPartialSuccess {
                error_message: "Some data points were rejected".to_string(),
                rejected_data_points: rejected_data_points.try_into().unwrap_or(MAX),
            })
        };
        Ok(Response::new(ExportMetricsServiceResponse {
            partial_success,
        }))
    }
}

fn metric_data_to_record_batch(
    metric: &str,
    data: &Data,
    schema: &Option<Schema>,
) -> (Result<RecordBatch>, u64) {
    match data {
        Data::Gauge(gauge) => (
            number_data_points_to_record_batch(metric, &gauge.data_points, schema),
            gauge.data_points.len() as u64,
        ),
        Data::Sum(sum) => (
            number_data_points_to_record_batch(metric, &sum.data_points, schema),
            sum.data_points.len() as u64,
        ),
        // TODO: Support other metric data types
        _ => (UnsupportedMetricDataTypeSnafu.fail(), 0),
    }
}

macro_rules! append_value {
    ($values_builder:expr, $data_points_type:expr, $value:expr, $builder_type:ty, $data_type:expr) => {
        match &mut $values_builder {
            Some(builder) => {
                if let Some(typed_builder) = builder.as_any_mut().downcast_mut::<$builder_type>() {
                    typed_builder.append_value(*$value);
                } else {
                    tracing::error!("Metric has data points with multiple types");
                    continue;
                }
            }
            None => {
                let mut new_builder = <$builder_type>::new();
                new_builder.append_value(*$value);
                $values_builder = Some(Box::new(new_builder));
                $data_points_type = $data_type;
            }
        }
    };
}

fn number_data_points_to_record_batch(
    metric: &str,
    data_points: &Vec<NumberDataPoint>,
    schema: &Option<Schema>,
) -> Result<RecordBatch> {
    let mut values_builder: Option<Box<dyn ArrayBuilder>> = None;
    let mut data_points_type = DataType::Null;
    let mut attributes = Vec::new();

    if let Some(s) = schema {
        if let Ok(value_field) = s.field_with_name(VALUE_COLUMN_NAME) {
            match value_field.data_type() {
                DataType::Float64 => {
                    values_builder = Some(Box::new(Float64Builder::new()));
                    data_points_type = DataType::Float64;
                }
                DataType::Int64 => {
                    values_builder = Some(Box::new(Int64Builder::new()));
                    data_points_type = DataType::Int64;
                }
                _ => {}
            }
        }
    }

    for data_point in data_points {
        if let Some(value) = &data_point.value {
            match value {
                Value::AsDouble(double_value) => {
                    append_value!(
                        values_builder,
                        data_points_type,
                        double_value,
                        Float64Builder,
                        DataType::Float64
                    );
                }
                Value::AsInt(int_value) => {
                    append_value!(
                        values_builder,
                        data_points_type,
                        int_value,
                        Int64Builder,
                        DataType::Int64
                    );
                }
            }
        } else if let Some(builder) = &mut values_builder {
            if let Some(float_64_builder) = builder.as_any_mut().downcast_mut::<Float64Builder>() {
                float_64_builder.append_null();
            } else if let Some(int_64_builder) = builder.as_any_mut().downcast_mut::<Int64Builder>()
            {
                int_64_builder.append_null();
            }
        }
        attributes.push(data_point.attributes.as_slice());
    }

    let mut columns: Vec<ArrayRef>;
    let mut fields: Vec<Arc<Field>>;
    if let Some(builder) = &mut values_builder {
        fields = vec![Arc::new(Field::new(
            VALUE_COLUMN_NAME,
            data_points_type,
            true,
        ))];
        columns = vec![Arc::new(builder.finish())];
    } else {
        return MetricWithNoDataPointsSnafu.fail();
    }

    let (attribute_fields_map, attribute_columns_map) =
        attributes_to_fields_and_columns(metric, attributes.as_slice(), schema);
    fields.extend(
        attribute_fields_map
            .into_iter()
            .map(|(_, v)| v)
            .collect::<Vec<Arc<Field>>>(),
    );
    columns.extend(
        attribute_columns_map
            .into_iter()
            .map(|(_, mut v)| v.finish()),
    );

    match RecordBatch::try_new(Arc::new(Schema::new(fields)), columns) {
        Ok(record_batch) => Ok(record_batch),
        Err(e) => Err(e).context(FailedToBuildRecordBatchSnafu),
    }
}

macro_rules! append_attribute {
    ($columns:expr, $fields:expr, $key:expr, $value:expr, $builder_type:ty, $data_type:expr, $metric:expr) => {{
        let key_str = $key.as_str();
        match $columns.get_mut(key_str) {
            None => {
                $fields.insert(
                    $key.clone(),
                    Arc::new(Field::new(key_str, $data_type, true)),
                );
                let mut builder = <$builder_type>::new();
                builder.append_value($value);
                $columns.insert($key.clone(), Box::new(builder));
            }
            Some(column) => {
                if let Some(builder) = column.as_any_mut().downcast_mut::<$builder_type>() {
                    builder.append_value($value);
                } else {
                    tracing::error!(
                        "Attribute defined multiple times with multiple types: {}.{}",
                        $metric,
                        key_str
                    );
                }
            }
        };
    }};
}

#[allow(clippy::type_complexity)]
fn attributes_to_fields_and_columns(
    metric: &str,
    attributes: &[&[KeyValue]],
    schema: &Option<Schema>,
) -> (
    IndexMap<String, Arc<Field>>,
    IndexMap<String, Box<dyn ArrayBuilder>>,
) {
    let mut fields: IndexMap<String, Arc<Field>> = IndexMap::new();
    let mut columns: IndexMap<String, Box<dyn ArrayBuilder>> = IndexMap::new();

    initialize_attribute_schema(&mut fields, &mut columns, schema);

    for (i, inner_attributes) in attributes.iter().enumerate() {
        for attribute in *inner_attributes {
            let key_str = attribute.key.as_str();
            match &attribute.value {
                Some(any_value) => match &any_value.value {
                    Some(value) => match value {
                        any_value::Value::StringValue(string_value) => {
                            append_attribute!(
                                columns,
                                fields,
                                attribute.key,
                                string_value,
                                StringBuilder,
                                DataType::Utf8,
                                metric
                            );
                        }
                        any_value::Value::BoolValue(bool_value) => {
                            append_attribute!(
                                columns,
                                fields,
                                attribute.key,
                                *bool_value,
                                BooleanBuilder,
                                DataType::Boolean,
                                metric
                            );
                        }
                        any_value::Value::IntValue(int_value) => {
                            append_attribute!(
                                columns,
                                fields,
                                attribute.key,
                                *int_value,
                                Int64Builder,
                                DataType::Int64,
                                metric
                            );
                        }
                        any_value::Value::DoubleValue(double_value) => {
                            append_attribute!(
                                columns,
                                fields,
                                attribute.key,
                                *double_value,
                                Float64Builder,
                                DataType::Float64,
                                metric
                            );
                        }
                        any_value::Value::BytesValue(bytes_value) => {
                            append_attribute!(
                                columns,
                                fields,
                                attribute.key,
                                bytes_value,
                                BinaryBuilder,
                                DataType::Binary,
                                metric
                            );
                        }
                        // TODO: Support List and Map attribute types
                        _ => {
                            tracing::error!(
                                "Unsupported metric attribute type for {metric}.{:?}",
                                attribute
                            );
                            append_null(&mut fields, &mut columns, key_str);
                        }
                    },
                    None => {
                        append_null(&mut fields, &mut columns, key_str);
                    }
                },
                None => {
                    append_null(&mut fields, &mut columns, key_str);
                }
            };
        }

        // If an attribute previously existed but is missing from this metric, append a null value.
        let mut needs_null = Vec::new();
        for (column_name, column_values) in columns.as_slice() {
            if column_values.len() < i + 1 {
                needs_null.push(column_name.clone());
            }
        }
        for column_name in needs_null {
            append_null(&mut fields, &mut columns, column_name.as_str());
        }
    }

    (fields, columns)
}

fn initialize_attribute_schema(
    fields: &mut IndexMap<String, Arc<Field>>,
    columns: &mut IndexMap<String, Box<dyn ArrayBuilder>>,
    schema: &Option<Schema>,
) {
    if let Some(s) = schema {
        for field in s.fields() {
            fields.insert(field.name().clone(), field.clone());
            match field.data_type() {
                DataType::Utf8 => {
                    columns.insert(
                        field.name().clone(),
                        Box::new(StringBuilder::new()) as Box<dyn ArrayBuilder>,
                    );
                }
                DataType::Boolean => {
                    columns.insert(
                        field.name().clone(),
                        Box::new(BooleanBuilder::new()) as Box<dyn ArrayBuilder>,
                    );
                }
                DataType::Int64 => {
                    columns.insert(
                        field.name().clone(),
                        Box::new(Int64Builder::new()) as Box<dyn ArrayBuilder>,
                    );
                }
                DataType::Float64 => {
                    columns.insert(
                        field.name().clone(),
                        Box::new(Float64Builder::new()) as Box<dyn ArrayBuilder>,
                    );
                }
                DataType::Binary => {
                    columns.insert(
                        field.name().clone(),
                        Box::new(BinaryBuilder::new()) as Box<dyn ArrayBuilder>,
                    );
                }
                _ => {}
            }
        }

        // Remove value field and column if it exists since it is not an attribute and is already handled.
        fields.shift_remove(VALUE_COLUMN_NAME);
        columns.shift_remove(VALUE_COLUMN_NAME);
    }
}

macro_rules! append_null {
    ($columns:expr, $key:expr, $builder_type:ty) => {
        if let Some(column) = $columns.get_mut($key) {
            if let Some(builder) = column.as_any_mut().downcast_mut::<$builder_type>() {
                builder.append_null();
            }
        }
    };
}

fn append_null(
    fields: &mut IndexMap<String, Arc<Field>>,
    columns: &mut IndexMap<String, Box<dyn ArrayBuilder>>,
    key: &str,
) {
    if let Some(field) = fields.get(key) {
        match field.data_type() {
            DataType::Utf8 => append_null!(columns, key, StringBuilder),
            DataType::Boolean => append_null!(columns, key, BooleanBuilder),
            DataType::Int64 => append_null!(columns, key, Int64Builder),
            DataType::Float64 => append_null!(columns, key, Float64Builder),
            DataType::Binary => append_null!(columns, key, BinaryBuilder),
            _ => {}
        }
    }
}

pub async fn start(bind_address: SocketAddr, data_fusion: Arc<DataFusion>) -> Result<()> {
    let service = Service { data_fusion };
    let svc = MetricsServiceServer::new(service).accept_compressed(CompressionEncoding::Gzip);

    tracing::info!("Spice Runtime OpenTelemetry listening on {bind_address}");

    Server::builder()
        .add_service(svc)
        .serve(bind_address)
        .await
        .context(UnableToServeSnafu)?;

    Ok(())
}
