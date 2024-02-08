use std::net::SocketAddr;

use opentelemetry_proto::tonic::collector::metrics::v1::metrics_service_server::MetricsService;
use opentelemetry_proto::tonic::collector::metrics::v1::metrics_service_server::MetricsServiceServer;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsPartialSuccess;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceResponse;
use snafu::prelude::*;
use tonic::async_trait;
use tonic_0_9_0::codec::CompressionEncoding;
use tonic_0_9_0::transport::Server;
use tonic_0_9_0::Request;
use tonic_0_9_0::Response;
use tonic_0_9_0::Status;

type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to serve"))]
    UnableToServe {
        source: tonic_0_9_0::transport::Error,
    },
}

pub struct Service;

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
                    total_data_points += 1;
                    match metric.data {
                        Some(data) => {
                            // TODO: Check if localhost table exists and write to Databackend
                            tracing::info!("Data: {:?}", data);
                        }
                        None => {
                            rejected_data_points += 1;
                        }
                    }
                }
            }
        }

        if rejected_data_points == total_data_points {
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

pub async fn start(bind_address: SocketAddr) -> Result<()> {
    let service = Service;
    let svc = MetricsServiceServer::new(service).accept_compressed(CompressionEncoding::Gzip);

    tracing::info!("Spice Runtime OpenTelemetry listening on {bind_address}");

    Server::builder()
        .add_service(svc)
        .serve(bind_address)
        .await
        .context(UnableToServeSnafu)?;

    Ok(())
}
