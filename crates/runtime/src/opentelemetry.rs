use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;

use opentelemetry_proto::tonic::collector::metrics::v1::metrics_service_server::MetricsService;
use opentelemetry_proto::tonic::collector::metrics::v1::metrics_service_server::MetricsServiceServer;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsPartialSuccess;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceResponse;
use snafu::prelude::*;
use tonic_0_9_0::codec::CompressionEncoding;
use tonic_0_9_0::transport::Server;
use tonic_0_9_0::Request;
use tonic_0_9_0::Response;
use tonic_0_9_0::Status;

use crate::datafusion::DataFusion;

type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to serve"))]
    UnableToServe {
        source: tonic_0_9_0::transport::Error,
    },
}

pub struct Service {
    datafusion: Arc<DataFusion>,
}

impl MetricsService for Service {
    fn export<'life0, 'async_trait>(
        &'life0 self,
        request: Request<ExportMetricsServiceRequest>,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Response<ExportMetricsServiceResponse>, Status>>
                + Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        let mut rejected_data_points = 0;
        let mut total_data_points = 0;
        for resource_metric in request.into_inner().resource_metrics {
            for scope_metric in resource_metric.scope_metrics {
                for metric in scope_metric.metrics {
                    total_data_points += 1;
                    if !self
                        .datafusion
                        .ctx
                        .table_exist(metric.name.clone())
                        .unwrap_or(false)
                    {
                        rejected_data_points += 1;
                    }
                    match metric.data {
                        Some(data) => {
                            // TODO: Write to DataFusion table
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
            return Box::pin(async {
                Err(Status::invalid_argument("All data points were rejected"))
            });
        }

        let partial_success = if rejected_data_points == 0 {
            None
        } else {
            Some(ExportMetricsPartialSuccess {
                error_message: "Some data points were rejected".to_string(),
                rejected_data_points,
            })
        };

        Box::pin(async {
            Ok(Response::new(ExportMetricsServiceResponse {
                partial_success,
            }))
        })
    }
}

pub async fn start(bind_address: SocketAddr, df: Arc<DataFusion>) -> Result<()> {
    let service = Service { datafusion: df };
    let svc = MetricsServiceServer::new(service).accept_compressed(CompressionEncoding::Gzip);

    tracing::info!("Spice Runtime OpenTelemetry listening on {bind_address}");

    Server::builder()
        .add_service(svc)
        .serve(bind_address)
        .await
        .context(UnableToServeSnafu)?;

    Ok(())
}
