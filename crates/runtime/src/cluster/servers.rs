/*
Copyright 2025 The Spice.ai OSS Authors

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

use super::ClusterTlsConfig;
use crate::cluster::ClusterServiceImpl;
use crate::cluster::executor_registry::ExecutorRegistry;
use crate::flight::{Error, is_address_in_use_error};
use crate::{Runtime, metrics as runtime_metrics};
use ballista_core::serde::protobuf::scheduler_grpc_server::SchedulerGrpcServer;
use ballista_executor::flight_service::BallistaFlightService;
use runtime_proto::cluster_service_server::ClusterServiceServer;
use std::net::ToSocketAddrs;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tonic::transport::{Server, ServerTlsConfig};

type ClusterServerResult<T> = std::result::Result<T, Error>;

/// Configures a tonic server with mTLS using the cluster TLS configuration.
///
/// This enables mutual TLS: the server presents its certificate and requires
/// clients to present valid certificates signed by the cluster CA.
fn server_with_cluster_mtls(
    server: Server,
    tls_config: &ClusterTlsConfig,
) -> Result<Server, tonic::transport::Error> {
    let server_tls_config = ServerTlsConfig::new()
        .identity(tls_config.server_identity.clone())
        .client_ca_root(tls_config.ca_certificate.clone());

    server.tls_config(server_tls_config)
}

/// Starts the internal cluster gRPC server for scheduler mode.
///
/// This server handles internal cluster communication:
/// - `SchedulerGrpcServer`: Ballista task scheduling protocol (executor registration, task dispatch)
/// - `ClusterServiceServer`: Spice-specific RPCs (`GetAppDefinition`, `ExpandSecret`)
///
/// This server should only be started when running in scheduler mode.
pub async fn start_internal_cluster_server(
    rt: Arc<Runtime>,
    shutdown_signal: Option<CancellationToken>,
    executor_registry: Arc<ExecutorRegistry>,
) -> ClusterServerResult<()> {
    let bind_address = rt.df.cluster_config.node_bind_address();

    let Some(scheduler) = rt
        .df
        .scheduler_server
        .read()
        .ok()
        .and_then(|r| r.iter().next().cloned())
    else {
        return Err(Error::ClusterSchedulerNotInitialized {});
    };

    let tls_config = rt.df.cluster_config.tls_config();
    let mut server = Server::builder();

    if let Some(tls_config) = tls_config {
        server = server_with_cluster_mtls(server, tls_config)
            .map_err(|source| Error::UnableToConfigureTls { source })?;
        tracing::info!("Cluster mTLS enabled for internal cluster server");
    } else if !rt.df.cluster_config.allow_insecure_connections() {
        return Err(Error::InsecureConfiguration {
            message: "Cluster mode without mTLS requires the --allow-insecure-connections flag"
                .to_string(),
        });
    } else {
        tracing::warn!(
            "Cluster mTLS disabled for internal cluster server (--allow-insecure-connections flag is set)"
        );
    }

    let scheduler_grpc_server = SchedulerGrpcServer::from_arc(scheduler)
        .max_decoding_message_size(usize::MAX)
        .max_encoding_message_size(usize::MAX);

    let advertise_address = rt
        .df
        .cluster_config
        .scheduler_url_string()
        .map(str::to_string)
        .or_else(|| {
            rt.df
                .cluster_config
                .node_advertise_address()
                .map(str::to_string)
        })
        .unwrap_or_else(|| bind_address.to_string());

    // Use the shared executor stream registry if available (created during scheduler init).
    // This allows the scheduler callback to broadcast PollNow to connected executors.
    let cluster_service = if let Some(executor_streams) = rt.df.executor_stream_registry() {
        ClusterServiceImpl::with_executor_streams(
            Arc::clone(&rt.app),
            Arc::clone(&rt.secrets),
            advertise_address,
            rt.scheduler_peers(),
            Arc::clone(&rt.df),
            Arc::clone(&executor_registry),
            rt.metrics_reader().cloned(),
            executor_streams,
        )
    } else {
        ClusterServiceImpl::new(
            Arc::clone(&rt.app),
            Arc::clone(&rt.secrets),
            advertise_address,
            rt.scheduler_peers(),
            Arc::clone(&rt.df),
            Arc::clone(&executor_registry),
            rt.metrics_reader().cloned(),
        )
    };
    let cluster_service_server = ClusterServiceServer::new(cluster_service);

    let server = server
        .add_service(scheduler_grpc_server)
        .add_service(cluster_service_server);

    tracing::info!("Spice Runtime internal cluster server listening on {bind_address}");

    if let Some(token) = shutdown_signal {
        server
            .serve_with_shutdown(bind_address, token.cancelled())
            .await
    } else {
        server.serve(bind_address).await
    }
    .map_err(|e| {
        if is_address_in_use_error(&e) {
            return Error::AddressAlreadyInUse {
                addr: bind_address.to_string(),
            };
        }
        Error::UnableToStartClusterServer { source: e }
    })?;

    tracing::debug!("Spice Runtime internal cluster server stopped");

    Ok(())
}

/// Starts the executor Ballista Flight server used for receiving query fragments.
///
/// mTLS is optional when `--allow-insecure-connections` is used.
pub async fn start_executor_flight_server(
    bind_address: std::net::SocketAddr,
    rt: Arc<Runtime>,
    shutdown_signal: Option<CancellationToken>,
) -> ClusterServerResult<()> {
    let tls_config = rt.df.cluster_config.tls_config();
    let mut server = Server::builder();

    if let Some(tls_config) = tls_config {
        server = server_with_cluster_mtls(server, tls_config)
            .map_err(|source| Error::UnableToConfigureTls { source })?;
        tracing::info!("Cluster mTLS enabled for executor flight server");
    } else if !rt.df.cluster_config.allow_insecure_connections() {
        return Err(Error::InsecureConfiguration {
            message: "Cluster mode without mTLS requires the --allow-insecure-connections flag"
                .to_string(),
        });
    } else {
        tracing::warn!(
            "Cluster mTLS disabled for executor flight server (--allow-insecure-connections flag is set)"
        );
    }

    // Executor: serve only BallistaFlightService for receiving query fragments.
    // No OTel service needed on executors.
    let server = server.add_service(
        arrow_flight::flight_service_server::FlightServiceServer::new(BallistaFlightService::new())
            .max_decoding_message_size(usize::MAX)
            .max_encoding_message_size(usize::MAX),
    );

    // Use the executor's bound address if it was dynamically assigned during registration.
    #[expect(clippy::cast_possible_truncation)]
    let bind_address = rt
        .df
        .executor
        .read()
        .ok()
        .and_then(|maybe_executor| {
            maybe_executor
                .as_ref()
                .and_then(|e| e.metadata.host.clone().map(|h| (h, e.metadata.port as u16)))
        })
        .and_then(|spec| {
            let (host, port) = &spec;
            tokio::task::block_in_place(|| match spec.to_socket_addrs() {
                Ok(sa) => Some(sa),
                Err(e) => {
                    tracing::error!("Unable to resolve bound executor host {host}:{port}: {e}");
                    None
                }
            })
        })
        .and_then(|mut addrs| addrs.next())
        .unwrap_or(bind_address);

    tracing::info!("Spice Runtime executor Flight listening on {bind_address}");
    runtime_metrics::spiced_runtime::FLIGHT_SERVER_START.add(1, &[]);

    if let Some(token) = shutdown_signal {
        server
            .serve_with_shutdown(bind_address, token.cancelled())
            .await
    } else {
        server.serve(bind_address).await
    }
    .map_err(|e| {
        if is_address_in_use_error(&e) {
            return Error::AddressAlreadyInUse {
                addr: bind_address.to_string(),
            };
        }
        Error::UnableToStartFlightServer { source: e }
    })?;

    tracing::debug!("Spice Runtime executor Flight stopped");

    Ok(())
}
