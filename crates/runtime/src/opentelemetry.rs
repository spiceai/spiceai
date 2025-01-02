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

use std::net::SocketAddr;
use std::sync::Arc;

use arrow::array::ArrayBuilder;
use arrow::array::ArrayRef;
use arrow::array::BinaryBuilder;
use arrow::array::BooleanBuilder;
use arrow::array::Float64Builder;
use arrow::array::Int64Builder;
use arrow::array::StringBuilder;
use arrow::array::UInt64Builder;
use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use datafusion::sql::TableReference;
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
use opentelemetry_proto::tonic::metrics::v1::DataPointFlags;
use opentelemetry_proto::tonic::metrics::v1::NumberDataPoint;
use runtime_auth::layer::grpc::make_interceptor;
use runtime_auth::GrpcAuth;
use secrecy::ExposeSecret;
use snafu::prelude::*;
use tonic::async_trait;
use tonic::codec::CompressionEncoding;
use tonic::service::interceptor;
use tonic::transport::{Identity, Server, ServerTlsConfig};
use tonic::Request;
use tonic::Response;
use tonic::Status;
use tonic_health::pb::health_server::Health;
use tonic_health::pb::health_server::HealthServer;

use crate::datafusion::DataFusion;
use crate::dataupdate::DataUpdate;
use crate::dataupdate::UpdateType;
use crate::tls::TlsConfig;
use crate::{tracers::OnceTracer, warn_once};

type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to serve: {source}"))]
    UnableToServe { source: tonic::transport::Error },

    #[snafu(display("Failed to build record batch from OpenTelemetry metrics: {source}"))]
    FailedToBuildRecordBatch { source: arrow::error::ArrowError },

    #[snafu(display("Unsupported metric data type"))]
    UnsupportedMetricDataType {},

    #[snafu(display("Unsupported metric attribute type"))]
    UnsupportedMetricAttributeType {},

    #[snafu(display("Metric with no data points"))]
    MetricWithNoDataPoints {},

    #[snafu(display(
        "Existing table for metric {metric} has unsupported `value` column data type {data_type} for data point type {data_point_type}"
    ))]
    UnsupportedExistingMetricValueColumnType {
        metric: String,
        data_type: DataType,
        data_point_type: String,
    },

    #[snafu(display(
        "First data point for metric {metric} has no value and therefore is not valid for establishing schema"
    ))]
    FirstMetricDataPointHasNoValue { metric: String },

    #[snafu(display("Unable to configure TLS on the Flight server: {source}"))]
    UnableToConfigureTls { source: tonic::transport::Error },
}

const VALUE_COLUMN_NAME: &str = "value";
const TIME_UNIX_NANO_COLUMN_NAME: &str = "time_unix_nano";
const START_TIME_UNIX_NANO_COLUMN_NAME: &str = "start_time_unix_nano";

pub struct Service {
    datafusion: Arc<DataFusion>,
    once_tracer: OnceTracer,
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
                        let existing_schema =
                            match self.datafusion.get_arrow_schema(metric.name.as_str()).await {
                                Ok(schema) => Some(schema),
                                Err(_) => None,
                            };
                        let (record_batch_result, data_points_count) = metric_data_to_record_batch(
                            metric.name.as_str(),
                            &data,
                            existing_schema.as_ref(),
                        );
                        total_data_points += data_points_count;

                        match record_batch_result {
                            Ok(record_batch) => {
                                if !self
                                    .datafusion
                                    .is_writable(&TableReference::bare(metric.name.to_string()))
                                {
                                    warn_once!(
                                        self.once_tracer,
                                        "No writable dataset defined for metric {}, skipping",
                                        metric.name
                                    );
                                    rejected_data_points += data_points_count;
                                    continue;
                                };

                                let schema = record_batch.schema();
                                let data_update = DataUpdate {
                                    data: vec![record_batch],
                                    schema,
                                    update_type: UpdateType::Append,
                                };

                                let mut write_failed = false;
                                if let Err(e) = self
                                    .datafusion
                                    .write_data(
                                        &TableReference::bare(metric.name.as_str()),
                                        data_update,
                                    )
                                    .await
                                {
                                    write_failed = true;
                                    tracing::debug!("Failed to add OpenTelemetry data: {e}");
                                };

                                if write_failed {
                                    rejected_data_points += data_points_count;
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
                rejected_data_points: rejected_data_points.try_into().unwrap_or(i64::MAX),
            })
        };
        Ok(Response::new(ExportMetricsServiceResponse {
            partial_success,
        }))
    }
}

async fn create_health_service() -> HealthServer<impl Health> {
    let (mut health_reporter, health_service) = tonic_health::server::health_reporter();
    health_reporter
        .set_serving::<MetricsServiceServer<Service>>()
        .await;
    health_service
}

pub fn metric_data_to_record_batch(
    metric: &str,
    data: &Data,
    existing_schema: Option<&Schema>,
) -> (Result<RecordBatch>, u64) {
    match data {
        Data::Gauge(gauge) => (
            number_data_points_to_record_batch(metric, &gauge.data_points, existing_schema),
            gauge.data_points.len() as u64,
        ),
        Data::Sum(sum) => (
            number_data_points_to_record_batch(metric, &sum.data_points, existing_schema),
            sum.data_points.len() as u64,
        ),
        // TODO: Support other metric data types
        _ => (UnsupportedMetricDataTypeSnafu.fail(), 0),
    }
}

macro_rules! append_value {
    ($values_builder:expr, $data_points_type:expr, $value:expr, $builder_type:ty, $data_type:expr, $metric:expr) => {
        match &mut $values_builder {
            Some(builder) => {
                if let Some(typed_builder) = builder.as_any_mut().downcast_mut::<$builder_type>() {
                    typed_builder.append_value(*$value);
                } else {
                    tracing::warn!("Metric {} has data points with different types, skipping data point that introduces new type", $metric);
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

#[allow(clippy::too_many_lines)]
fn number_data_points_to_record_batch(
    metric: &str,
    data_points: &Vec<NumberDataPoint>,
    existing_schema: Option<&Schema>,
) -> Result<RecordBatch> {
    let mut values_builder: Option<Box<dyn ArrayBuilder>> = None;
    let mut values_type = DataType::Null;
    let mut time_unix_nano_builder = UInt64Builder::new();
    let mut start_time_unix_nano_builder = UInt64Builder::new();
    let mut attributes = Vec::new();

    if let Some(s) = existing_schema {
        if let Ok(value_field) = s.field_with_name(VALUE_COLUMN_NAME) {
            match value_field.data_type() {
                DataType::Float64 => {
                    values_builder = Some(Box::new(Float64Builder::new()));
                    values_type = DataType::Float64;
                }
                DataType::Int64 => {
                    values_builder = Some(Box::new(Int64Builder::new()));
                    values_type = DataType::Int64;
                }
                _ => {
                    return UnsupportedExistingMetricValueColumnTypeSnafu {
                        metric,
                        data_type: value_field.data_type().clone(),
                        data_point_type: "NumberDataPoint",
                    }
                    .fail();
                }
            }
        }
    }

    for data_point in data_points {
        if let Some(value) = &data_point.value {
            match value {
                Value::AsDouble(double_value) => {
                    append_value!(
                        values_builder,
                        values_type,
                        double_value,
                        Float64Builder,
                        DataType::Float64,
                        metric
                    );
                }
                Value::AsInt(int_value) => {
                    append_value!(
                        values_builder,
                        values_type,
                        int_value,
                        Int64Builder,
                        DataType::Int64,
                        metric
                    );
                }
            }
        } else if let Some(builder) = &mut values_builder {
            if (data_point.flags & DataPointFlags::NoRecordedValueMask as u32)
                != DataPointFlags::NoRecordedValueMask as u32
            {
                tracing::warn!(
                    "Metric {} has data point with no recorded value without flag set to indicate no recorded value, skipping",
                    metric
                );
                continue;
            }

            if let Some(float_64_builder) = builder.as_any_mut().downcast_mut::<Float64Builder>() {
                float_64_builder.append_null();
            } else if let Some(int_64_builder) = builder.as_any_mut().downcast_mut::<Int64Builder>()
            {
                int_64_builder.append_null();
            }
        } else {
            return FirstMetricDataPointHasNoValueSnafu { metric }.fail();
        }
        attributes.push(data_point.attributes.as_slice());
        time_unix_nano_builder.append_value(data_point.time_unix_nano);
        start_time_unix_nano_builder.append_value(data_point.start_time_unix_nano);
    }

    let mut columns: Vec<ArrayRef>;
    let mut fields: Vec<Arc<Field>>;
    if let Some(builder) = &mut values_builder {
        fields = vec![
            Arc::new(Field::new(VALUE_COLUMN_NAME, values_type, true)),
            Arc::new(Field::new(
                TIME_UNIX_NANO_COLUMN_NAME,
                DataType::UInt64,
                true,
            )),
            Arc::new(Field::new(
                START_TIME_UNIX_NANO_COLUMN_NAME,
                DataType::UInt64,
                true,
            )),
        ];
        columns = vec![
            Arc::new(builder.finish()),
            Arc::new(time_unix_nano_builder.finish()),
            Arc::new(start_time_unix_nano_builder.finish()),
        ];
    } else {
        return MetricWithNoDataPointsSnafu.fail();
    }

    let (attribute_fields_map, attribute_columns_map) =
        attributes_to_fields_and_columns(metric, attributes.as_slice(), existing_schema);
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
                    tracing::warn!(
                        "Metric {} has attribute {} with different types, appending null for attribute that introduces new type",
                        $metric,
                        key_str
                    );
                    append_null(&mut $fields, &mut $columns, key_str);
                }
            }
        };
    }};
}

#[allow(clippy::type_complexity)]
fn attributes_to_fields_and_columns(
    metric: &str,
    attributes: &[&[KeyValue]],
    existing_schema: Option<&Schema>,
) -> (
    IndexMap<String, Arc<Field>>,
    IndexMap<String, Box<dyn ArrayBuilder>>,
) {
    let mut fields: IndexMap<String, Arc<Field>> = IndexMap::new();
    let mut columns: IndexMap<String, Box<dyn ArrayBuilder>> = IndexMap::new();

    initialize_attribute_schema(&mut fields, &mut columns, existing_schema);

    for (i, inner_attributes) in attributes.iter().enumerate() {
        for attribute in *inner_attributes {
            let key_str = attribute.key.as_str();
            if let Some(any_value) = &attribute.value {
                if let Some(value) = &any_value.value {
                    match value {
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
                            tracing::warn!(
                                "Metric {metric} has attribute {key_str} with unsupported type, appending null for attribute if possible"
                            );
                            append_null(&mut fields, &mut columns, key_str);
                        }
                    }
                } else {
                    tracing::warn!(
                        "Metric {metric} has attribute {key_str} with no value, appending null for attribute if possible"
                    );
                    append_null(&mut fields, &mut columns, key_str);
                }
            } else {
                tracing::warn!(
                    "Metric {metric} has attribute {key_str} with no value, appending null for attribute if possible"
                );
                append_null(&mut fields, &mut columns, key_str);
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
    existing_schema: Option<&Schema>,
) {
    if let Some(s) = existing_schema {
        for field in s.fields() {
            // Skip value and time fields because they are not attributes and are already handled.
            if field.name() == VALUE_COLUMN_NAME
                || field.name() == TIME_UNIX_NANO_COLUMN_NAME
                || field.name() == START_TIME_UNIX_NANO_COLUMN_NAME
            {
                continue;
            }

            fields.insert(field.name().clone(), Arc::clone(field));
            match field.data_type() {
                DataType::Utf8 => {
                    columns.insert(field.name().clone(), Box::new(StringBuilder::new()));
                }
                DataType::Boolean => {
                    columns.insert(field.name().clone(), Box::new(BooleanBuilder::new()));
                }
                DataType::Int64 => {
                    columns.insert(field.name().clone(), Box::new(Int64Builder::new()));
                }
                DataType::Float64 => {
                    columns.insert(field.name().clone(), Box::new(Float64Builder::new()));
                }
                DataType::Binary => {
                    columns.insert(field.name().clone(), Box::new(BinaryBuilder::new()));
                }
                _ => {}
            }
        }
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

pub async fn start(
    bind_address: SocketAddr,
    datafusion: Arc<DataFusion>,
    tls_config: Option<Arc<TlsConfig>>,
    grpc_auth: Option<Arc<dyn GrpcAuth + Send + Sync>>,
) -> Result<()> {
    let service = Service {
        datafusion,
        once_tracer: OnceTracer::new(),
    };
    let svc = MetricsServiceServer::new(service).accept_compressed(CompressionEncoding::Gzip);

    tracing::info!("Spice Runtime OpenTelemetry listening on {bind_address}");

    let mut server = Server::builder();

    if let Some(ref tls_config) = tls_config {
        let server_tls_config = ServerTlsConfig::new().identity(Identity::from_pem(
            tls_config.cert.expose_secret(),
            tls_config.key.expose_secret(),
        ));
        server = server
            .tls_config(server_tls_config)
            .context(UnableToConfigureTlsSnafu)?;
    }

    server
        .layer(interceptor(make_interceptor(grpc_auth)))
        .add_service(create_health_service().await)
        .add_service(svc)
        .serve(bind_address)
        .await
        .context(UnableToServeSnafu)?;

    Ok(())
}
