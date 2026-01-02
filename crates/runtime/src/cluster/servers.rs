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
use crate::cluster::{ClusterServiceImpl, ExecutorServiceImpl};
use crate::flight::{Error, is_address_in_use_error};
use crate::{Runtime, metrics as runtime_metrics};
use ballista_core::serde::protobuf::executor_grpc_server::ExecutorGrpcServer;
use ballista_core::serde::protobuf::scheduler_grpc_client::SchedulerGrpcClient;
use ballista_core::serde::protobuf::scheduler_grpc_server::SchedulerGrpcServer;
use ballista_core::utils::create_grpc_client_endpoint;
use ballista_executor::executor_server::register_executor_with_scheduler;
use ballista_executor::flight_service::BallistaFlightService;
use runtime_proto::cluster_service_server::ClusterServiceServer;
use runtime_proto::executor_service_server::ExecutorServiceServer;
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

    let cluster_service = ClusterServiceImpl::new(Arc::clone(&rt.app), Arc::clone(&rt.secrets));
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

    // Executor: serve BallistaFlightService for receiving query fragments
    // and ExecutorService for scheduler-driven discovery.
    let executor_service = ExecutorServiceImpl::new(Arc::clone(&rt.df));
    let executor_service_server = ExecutorServiceServer::new(executor_service);
    let executor_grpc = rt
        .df
        .executor_grpc
        .read()
        .ok()
        .and_then(|maybe_executor| maybe_executor.clone())
        .ok_or(Error::ClusterExecutorNotInitialized {})?;
    let executor_grpc_server = ExecutorGrpcServer::new(executor_grpc.as_ref().clone())
        .max_decoding_message_size(usize::MAX)
        .max_encoding_message_size(usize::MAX);

    let server = server
        .add_service(
            arrow_flight::flight_service_server::FlightServiceServer::new(
                BallistaFlightService::new(),
            )
            .max_decoding_message_size(usize::MAX)
            .max_encoding_message_size(usize::MAX),
        )
        .add_service(executor_service_server)
        .add_service(executor_grpc_server);

    // Use the executor's bound address if it was dynamically assigned during registration.
    let cluster_bind_ip = rt.df.cluster_config.node_bind_address().ip();
    let bind_address = rt
        .df
        .executor
        .read()
        .ok()
        .and_then(|maybe_executor| {
            maybe_executor
                .as_ref()
                .map(|e| e.metadata.port)
                .and_then(|port| u16::try_from(port).ok())
        })
        .map_or(bind_address, |port| {
            std::net::SocketAddr::new(cluster_bind_ip, port)
        });

    tracing::info!("Spice Runtime executor Flight listening on {bind_address}");
    runtime_metrics::spiced_runtime::FLIGHT_SERVER_START.add(1, &[]);

    let server_shutdown = shutdown_signal.unwrap_or_default();
    let server_shutdown_token = server_shutdown.clone();
    let server_handle = tokio::spawn(async move {
        server
            .serve_with_shutdown(bind_address, server_shutdown_token.cancelled())
            .await
    });

    if let Some(scheduler_url) = rt.df.cluster_config.scheduler_address() {
        let mut endpoint = create_grpc_client_endpoint(scheduler_url.to_string()).map_err(|e| {
            Error::FailedToRegisterExecutor {
                source: Box::new(e),
            }
        })?;
        if let Some(tls_config) = rt.df.cluster_config.client_tls_config().cloned() {
            endpoint =
                endpoint
                    .tls_config(tls_config)
                    .map_err(|e| Error::FailedToRegisterExecutor {
                        source: Box::new(e),
                    })?;
        }
        let channel = endpoint
            .connect()
            .await
            .map_err(|e| Error::FailedToRegisterExecutor {
                source: Box::new(e),
            })?;
        let mut scheduler = SchedulerGrpcClient::new(channel)
            .max_encoding_message_size(usize::MAX)
            .max_decoding_message_size(usize::MAX);

        let executor = rt
            .df
            .executor
            .read()
            .ok()
            .and_then(|maybe_executor| maybe_executor.clone())
            .ok_or(Error::ClusterExecutorNotInitialized {})?;

        if let Err(e) = register_executor_with_scheduler(&mut scheduler, executor).await {
            server_shutdown.cancel();
            let _ = server_handle.await;
            return Err(Error::FailedToRegisterExecutor {
                source: Box::new(e),
            });
        }
    }

    server_handle
        .await
        .map_err(|e| Error::FlightServerTaskFailed {
            source: Box::new(e),
        })?
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
