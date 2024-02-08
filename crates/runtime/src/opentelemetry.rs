use std::net::SocketAddr;
use std::sync::Arc;

use arrow::array::Float64Array;
use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use opentelemetry_proto::tonic::collector::metrics::v1::metrics_service_server::MetricsService;
use opentelemetry_proto::tonic::collector::metrics::v1::metrics_service_server::MetricsServiceServer;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsPartialSuccess;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceResponse;
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

    #[snafu(display("Failed to build record batch"))]
    FailedToBuildRecordBatch { source: arrow::error::ArrowError },

    #[snafu(display("Unsupported metric data type"))]
    UnsupportedMetricDataType {},
}

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
        let mut results = Vec::new();
        for resource_metric in request.into_inner().resource_metrics {
            for scope_metric in resource_metric.scope_metrics {
                for metric in scope_metric.metrics {
                    if let Some(data) = metric.data {
                        let (record_batch_result, accepted_points, rejected_points) =
                            metric_data_to_record_batch(&data);
                        total_data_points += accepted_points + rejected_points;
                        rejected_data_points += rejected_points;

                        match record_batch_result {
                            Ok(record_batch) => {
                                let Some(backend) =
                                    self.data_fusion.get_backend(metric.name.as_str())
                                else {
                                    tracing::warn!("No backend found for metric {}", metric.name);
                                    rejected_data_points += accepted_points;
                                    continue;
                                };

                                tracing::info!("Original {} {:?}", metric.name, data);
                                tracing::info!(
                                    "Adding data to backend {} {:?}",
                                    metric.name,
                                    record_batch
                                );
                                results.push((
                                    backend.add_data(DataUpdate {
                                        log_sequence_number: None,
                                        data: vec![record_batch],
                                        update_type: UpdateType::Append,
                                    }),
                                    accepted_points,
                                ));
                            }
                            Err(e) => {
                                tracing::error!(
                                    "Failed to add OpenTelemetry data to backend: {}",
                                    e
                                );
                            }
                        }
                    }
                }
            }
        }

        for (result, accepted_points) in results {
            if let Err(e) = result.await {
                rejected_data_points += accepted_points;
                tracing::error!("Failed to add OpenTelemetry data to backend: {}", e);
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
                rejected_data_points,
            })
        };
        Ok(Response::new(ExportMetricsServiceResponse {
            partial_success,
        }))
    }
}

pub fn metric_data_to_record_batch(data: &Data) -> (Result<RecordBatch>, i64, i64) {
    match data {
        Data::Gauge(gauge) => number_data_points_to_record_batch(&gauge.data_points),
        Data::Sum(sum) => number_data_points_to_record_batch(&sum.data_points),
        _ => (UnsupportedMetricDataTypeSnafu.fail(), 0, 0),
    }
}

fn number_data_points_to_record_batch(
    data_points: &Vec<NumberDataPoint>,
) -> (Result<RecordBatch>, i64, i64) {
    let mut accepted_data_points = 0;
    let mut rejected_data_points = 0;

    let mut values = Vec::new();
    for data_point in data_points {
        if let Some(value) = &data_point.value {
            accepted_data_points += 1;
            match value {
                Value::AsDouble(double_value) => {
                    values.push(*double_value);
                }
                Value::AsInt(int_value) => {
                    values.push(*int_value as f64);
                }
            }
        } else {
            rejected_data_points += 1;
            continue;
        }
    }

    match RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Float64,
            false,
        )])),
        vec![Arc::new(Float64Array::from(values))],
    ) {
        Ok(record_batch) => (Ok(record_batch), accepted_data_points, rejected_data_points),
        Err(e) => (
            Err(e).context(FailedToBuildRecordBatchSnafu),
            0,
            accepted_data_points + rejected_data_points,
        ),
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
