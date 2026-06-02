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

use super::composite_flight_service::CompositeFlightService;
use crate::auth::EndpointAuth;
use crate::cluster::ExecutorRegistry;
use crate::cluster::{ClusterServiceImpl, SchedulerPeers};
use crate::flight::middleware::{RequestContextLayer, WriteRateLimitLayer};
use crate::flight::{Error, Service as SpiceFlightService, is_address_in_use_error, session_auth};
use crate::tls::flight_incoming::tls_incoming;
use crate::{Runtime, metrics as runtime_metrics};
use ballista_core::serde::protobuf::scheduler_grpc_server::SchedulerGrpcServer;
use governor::RateLimiter;
use runtime_auth::layer::flight::BasicAuthLayer;
use runtime_proto::cluster_service_server::ClusterServiceServer;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tonic::transport::Server;

type ClusterServerResult<T> = std::result::Result<T, Error>;

/// Bind a TCP listener for the cluster server, mapping `AddrInUse` to the
/// dedicated error variant so the caller's logs stay consistent.
async fn bind_cluster_listener(bind_address: SocketAddr) -> Result<TcpListener, Error> {
    TcpListener::bind(bind_address).await.map_err(|source| {
        if source.kind() == std::io::ErrorKind::AddrInUse {
            Error::AddressAlreadyInUse {
                addr: bind_address.to_string(),
            }
        } else {
            Error::UnableToBindClusterListener { source }
        }
    })
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
    scheduler_peers: Arc<RwLock<SchedulerPeers>>,
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

    let cluster_server_config = rt.df.cluster_config.cluster_server_config();
    let mtls_enabled = cluster_server_config.is_some();
    let mut server = Server::builder();

    if mtls_enabled {
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
            scheduler_peers,
            Arc::clone(&rt.df),
            Arc::clone(&executor_registry),
            rt.metrics_reader().cloned(),
            executor_streams,
            mtls_enabled,
        )
    } else {
        ClusterServiceImpl::new(
            Arc::clone(&rt.app),
            Arc::clone(&rt.secrets),
            advertise_address,
            scheduler_peers,
            Arc::clone(&rt.df),
            Arc::clone(&executor_registry),
            rt.metrics_reader().cloned(),
            mtls_enabled,
        )
    };

    let cluster_service_server = ClusterServiceServer::new(cluster_service);

    let server = server
        .add_service(scheduler_grpc_server)
        .add_service(cluster_service_server);

    let serve_result = if let Some(server_config) = cluster_server_config {
        let listener = bind_cluster_listener(bind_address).await?;
        tracing::info!("Spice Runtime internal cluster server listening on {bind_address}");
        let incoming = tls_incoming(listener, server_config);
        if let Some(token) = shutdown_signal {
            server
                .serve_with_incoming_shutdown(incoming, token.cancelled())
                .await
        } else {
            server.serve_with_incoming(incoming).await
        }
    } else if let Some(token) = shutdown_signal {
        tracing::info!("Spice Runtime internal cluster server listening on {bind_address}");
        server
            .serve_with_shutdown(bind_address, token.cancelled())
            .await
    } else {
        tracing::info!("Spice Runtime internal cluster server listening on {bind_address}");
        server.serve(bind_address).await
    };

    serve_result.map_err(|e| {
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

/// Starts the executor Flight server for both Ballista shuffle data and Spice SQL queries.
///
/// This server uses a composite Flight service that routes:
/// - Ballista-format requests (`FetchPartition`, `IO_BLOCK_TRANSPORT`) to `BallistaFlightService`
/// - SQL and `FlightSQL` requests to Spice's Flight service
///
/// mTLS is optional when `--allow-insecure-connections` is used.
pub async fn start_executor_flight_server(
    bind_address: std::net::SocketAddr,
    rt: Arc<Runtime>,
    endpoint_auth: EndpointAuth,
    shutdown_signal: Option<CancellationToken>,
) -> ClusterServerResult<()> {
    let cluster_server_config = rt.df.cluster_config.cluster_server_config();
    let has_flight_auth = endpoint_auth.flight_basic_auth.is_some();

    // In executor mode, never allow unauthenticated Flight DoPut without mTLS.
    // Scheduler-trusted forwarding mode requires authenticated scheduler identity via mTLS.
    if !has_flight_auth && cluster_server_config.is_none() {
        return Err(Error::InsecureConfiguration {
            message: "Executor Flight server requires either API key auth or cluster mTLS. Configure endpoint auth or enable mTLS for scheduler-trusted forwarding.".to_string(),
        });
    }

    let server = Server::builder();

    if cluster_server_config.is_some() {
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

    if !has_flight_auth {
        tracing::debug!(
            "Executor Flight API key auth is disabled; accepting scheduler-trusted DoPut requires cluster mTLS"
        );
    }

    // Create composite Flight service that handles both Ballista and Spice protocols
    let spice_service = SpiceFlightService::new(
        endpoint_auth.flight_basic_auth.as_ref().map(Arc::clone),
        rt.datafusion().data_update_broadcaster(),
    );
    let session_store = spice_service.session_store();
    let ballista_work_dir = rt
        .df
        .executor
        .read()
        .map_err(|_| Error::ClusterExecutorNotInitialized {})?
        .as_ref()
        .ok_or(Error::ClusterExecutorNotInitialized {})?
        .work_dir
        .clone();
    let composite_service = CompositeFlightService::new(spice_service, ballista_work_dir);

    // Get app for request context
    let app = rt.app.read().await.as_ref().map(Arc::clone);

    // Wrap the auth in session-awareness to accept session IDs as bearer tokens
    let session_aware_auth = session_auth::with_session_awareness(
        endpoint_auth.flight_basic_auth,
        session_store.clone(),
    );
    let auth_layer = tower::ServiceBuilder::new()
        .layer(BasicAuthLayer::new(session_aware_auth))
        .into_inner();

    // Get job executor if available (cluster mode)
    let job_executor = rt.job_executor();
    let flight_write_rate_limit_enabled = rt.flight_write_rate_limit_enabled();

    // Add middleware layers for request context, auth, and rate limiting
    let rate_limits = &rt.rate_limits;
    let mut server = server
        .layer(
            RequestContextLayer::new(app, rt.datafusion(), session_store, rt.secrets())
                .with_job_executor(job_executor),
        )
        .layer(auth_layer)
        .layer(WriteRateLimitLayer::new(
            RateLimiter::direct(rate_limits.flight_write_limit),
            flight_write_rate_limit_enabled,
        ));

    let server = server.add_service(
        arrow_flight::flight_service_server::FlightServiceServer::new(composite_service)
            .max_decoding_message_size(usize::MAX)
            .max_encoding_message_size(usize::MAX),
    );

    // Use the executor's bound address if it was dynamically assigned during registration.
    #[expect(clippy::cast_possible_truncation)]
    let bind_address = match rt.df.executor.read().ok().and_then(|maybe_executor| {
        maybe_executor
            .as_ref()
            .and_then(|e| e.metadata.host.clone().map(|h| (h, e.metadata.port as u16)))
    }) {
        Some((host, port)) => match tokio::net::lookup_host((&*host, port)).await {
            Ok(mut addrs) => addrs.next().unwrap_or(bind_address),
            Err(e) => {
                tracing::error!("Unable to resolve bound executor host {host}:{port}: {e}");
                bind_address
            }
        },
        None => bind_address,
    };

    let serve_result = if let Some(server_config) = cluster_server_config {
        let listener = bind_cluster_listener(bind_address).await?;
        // Bind succeeded — emit started log + metric only now.
        tracing::info!("Spice Runtime executor Flight listening on {bind_address}");
        runtime_metrics::spiced_runtime::FLIGHT_SERVER_START.add(1, &[]);
        let incoming = tls_incoming(listener, server_config);
        if let Some(token) = shutdown_signal {
            server
                .serve_with_incoming_shutdown(incoming, token.cancelled())
                .await
        } else {
            server.serve_with_incoming(incoming).await
        }
    } else if let Some(token) = shutdown_signal {
        tracing::info!("Spice Runtime executor Flight listening on {bind_address}");
        runtime_metrics::spiced_runtime::FLIGHT_SERVER_START.add(1, &[]);
        server
            .serve_with_shutdown(bind_address, token.cancelled())
            .await
    } else {
        tracing::info!("Spice Runtime executor Flight listening on {bind_address}");
        runtime_metrics::spiced_runtime::FLIGHT_SERVER_START.add(1, &[]);
        server.serve(bind_address).await
    };

    serve_result.map_err(|e| {
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
