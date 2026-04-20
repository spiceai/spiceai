/*
Copyright 2026 The Spice.ai OSS Authors

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
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use app::{App, AppBuilder};
use clap::Parser;
use runtime::Runtime;
use runtime::auth::EndpointAuth;
use runtime::cluster::ResolvedClusterConfig;
use runtime::config::{ClusterConfig, ClusterRole, Config};
use runtime::datafusion::builder::DEFAULT_DATAFUSION_CONFIG;
use rustls::crypto::{CryptoProvider, aws_lc_rs};

#[derive(Parser, Debug)]
#[clap(rename_all = "kebab-case")]
struct Args {
    #[arg(long)]
    role: ClusterRole,

    #[arg(long)]
    http_bind: SocketAddr,

    #[arg(long)]
    flight_bind: SocketAddr,

    #[arg(long)]
    cluster_bind: SocketAddr,

    #[arg(long)]
    scheduler_address: Option<String>,

    #[arg(long)]
    node_advertise_address: Option<String>,

    #[arg(long)]
    node_mtls_ca_certificate_file: Option<String>,

    #[arg(long)]
    node_mtls_certificate_file: Option<String>,

    #[arg(long)]
    node_mtls_key_file: Option<String>,

    #[arg(long)]
    metrics_bind: Option<SocketAddr>,

    #[arg(long)]
    app_json: Option<PathBuf>,
}

type AnyError = Box<dyn std::error::Error + Send + Sync>;

fn boxed_error(message: impl Into<String>) -> AnyError {
    std::io::Error::other(message.into()).into()
}

#[tokio::main]
async fn main() -> Result<(), AnyError> {
    let args = Args::parse();

    let _ = CryptoProvider::install_default(aws_lc_rs::default_provider());
    configure_test_datafusion_defaults()?;

    let app = load_app(args.app_json.as_deref())?;

    let config = Config {
        http_bind_address: args.http_bind,
        flight_bind_address: args.flight_bind,
        cluster: ClusterConfig {
            role: Some(args.role),
            node_bind_address: args.cluster_bind,
            scheduler_address: args.scheduler_address,
            node_advertise_address: args.node_advertise_address,
            node_mtls_ca_certificate_file: args.node_mtls_ca_certificate_file,
            node_mtls_certificate_file: args.node_mtls_certificate_file,
            node_mtls_key_file: args.node_mtls_key_file,
            ..Default::default()
        },
        ..Default::default()
    };

    let resolved_cluster_config = ResolvedClusterConfig::try_new(config.cluster.clone())?;

    let mut builder = Runtime::builder()
        .with_runtime_config(config.clone())
        .with_resolved_cluster_config(resolved_cluster_config)
        .with_app(app);

    if let Some(metrics_bind) = args.metrics_bind {
        builder = builder.with_metrics_server(metrics_bind, prometheus::Registry::new());
    }

    let runtime = Arc::new(builder.build().await);

    let server_runtime = Arc::clone(&runtime);
    let mut server_task = tokio::spawn(async move {
        server_runtime
            .start_servers(config, None, EndpointAuth::no_auth())
            .await
    });

    tokio::select! {
        () = tokio::time::sleep(Duration::from_secs(60)) => {
            return Err(boxed_error("timed out waiting for runtime components to load"));
        }
        result = &mut server_task => {
            return Err(boxed_error(match result {
                Ok(Ok(())) => "server task exited unexpectedly".to_string(),
                Ok(Err(e)) => format!("server task failed to start: {e}"),
                Err(e) => format!("server task panicked: {e}"),
            }));
        }
        () = Arc::clone(&runtime).load_components() => {}
    }

    wait_for_runtime_ready(&runtime, Duration::from_secs(120)).await?;

    tokio::select! {
        result = &mut server_task => {
            match result {
                Ok(Ok(())) => Ok(()),
                Ok(Err(e)) => Err(boxed_error(format!("server task failed: {e}"))),
                Err(e) => Err(boxed_error(format!("server task panicked: {e}"))),
            }
        }
        _ = wait_for_shutdown_signal() => {
            runtime.shutdown().await;
            let _ = server_task.await;
            Ok(())
        }
    }
}

fn load_app(path: Option<&std::path::Path>) -> Result<App, AnyError> {
    if let Some(path) = path {
        let bytes = std::fs::read(path)?;
        let app = serde_json::from_slice(&bytes)?;
        Ok(app)
    } else {
        Ok(AppBuilder::new("cluster_test_node").build())
    }
}

fn configure_test_datafusion_defaults() -> Result<(), AnyError> {
    let mut config = DEFAULT_DATAFUSION_CONFIG
        .write()
        .map_err(|_| boxed_error("failed to lock DEFAULT_DATAFUSION_CONFIG for writing"))?;

    config.options_mut().execution.target_partitions = 3;
    config.options_mut().execution.coalesce_batches = false;
    config.options_mut().optimizer.repartition_joins = false;

    Ok(())
}

async fn wait_for_runtime_ready(runtime: &Runtime, timeout: Duration) -> Result<(), AnyError> {
    let start = std::time::Instant::now();
    while start.elapsed() <= timeout {
        if runtime.status().is_ready() {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    Err(boxed_error(format!(
        "timed out waiting for runtime ready state after {timeout:?}"
    )))
}

async fn wait_for_shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};

        match (
            signal(SignalKind::interrupt()),
            signal(SignalKind::terminate()),
        ) {
            (Ok(mut interrupt), Ok(mut terminate)) => {
                tokio::select! {
                    _ = interrupt.recv() => {}
                    _ = terminate.recv() => {}
                    _ = tokio::signal::ctrl_c() => {}
                }
            }
            _ => {
                let _ = tokio::signal::ctrl_c().await;
            }
        }
    }

    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}
