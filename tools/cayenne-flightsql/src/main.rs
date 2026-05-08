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

use std::collections::HashSet;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use cayenne::{CayenneCatalogProvider, CayenneCatalogProviderConfig, CayenneSchemaProvider};
use clap::Parser;
use data_components::RefreshableCatalogProvider as _;
use datafusion::{
    catalog::{CatalogProvider as _, SchemaProvider},
    execution::SessionStateBuilder,
    prelude::{SessionConfig, SessionContext},
};
use datafusion_ddl::{DdlAnalyzerRule, DdlExtensionPlanner, new_shared_store};
use datafusion_flightsql::FlightSqlService;
use runtime_datafusion::extension::ExtensionPlanQueryPlanner;
use snafu::prelude::*;
use tonic::transport::Server;

#[derive(Debug, Parser)]
#[command(
    name = "cayenne-flightsql",
    about = "Standalone Arrow Flight SQL server backed by one Cayenne catalog"
)]
struct Args {
    /// Flight SQL listen address.
    #[arg(long, env = "FLIGHTSQL_ADDR", default_value = "127.0.0.1:50051")]
    addr: std::net::SocketAddr,

    /// `DataFusion` default catalog name (the Cayenne catalog will be registered under this name).
    #[arg(long, env = "FLIGHTSQL_CATALOG", default_value = "cayenne")]
    catalog: String,

    /// `DataFusion` default schema for unqualified table references.
    #[arg(long, env = "FLIGHTSQL_DEFAULT_SCHEMA", default_value = "public")]
    default_schema: String,

    /// Base directory used when data and metadata directories are not explicitly provided.
    #[arg(
        long,
        env = "CAYENNE_SPICE_DATA_BASE_PATH",
        default_value = ".spice/data"
    )]
    spice_data_base_path: String,

    /// Directory for Cayenne Vortex data files.
    #[arg(long, env = "CAYENNE_DATA_DIR")]
    cayenne_data_dir: Option<String>,

    /// Directory for Cayenne `SQLite` metadata files.
    #[arg(long, env = "CAYENNE_METADATA_DIR")]
    cayenne_metadata_dir: Option<String>,

    /// Vortex footer cache size in MB.
    #[arg(long, env = "CAYENNE_FOOTER_CACHE_MB")]
    cayenne_footer_cache_mb: Option<usize>,

    /// Vortex segment cache size in MB.
    #[arg(long, env = "CAYENNE_SEGMENT_CACHE_MB")]
    cayenne_segment_cache_mb: Option<usize>,

    /// Target Vortex file size in MB.
    #[arg(long, env = "CAYENNE_TARGET_FILE_SIZE_MB")]
    cayenne_target_file_size_mb: Option<usize>,

    /// Periodic catalog refresh interval (seconds). If omitted, refresh runs only at startup.
    #[arg(long, env = "CAYENNE_REFRESH_INTERVAL_SECS")]
    refresh_interval_secs: Option<u64>,
}

#[derive(Debug, Snafu)]
enum Error {
    #[snafu(display("Failed to initialize tracing subscriber: {source}"))]
    TracingInit {
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },

    #[snafu(display("Failed to initialize Cayenne catalog provider: {source}"))]
    CayenneCatalogInit {
        source: cayenne::catalog_provider::Error,
    },

    #[snafu(display("Failed to refresh Cayenne catalog: {source}"))]
    CayenneCatalogRefresh {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "Failed to create default schema '{default_schema}' in Cayenne catalog: {source}"
    ))]
    CreateDefaultSchema {
        default_schema: String,
        source: datafusion::error::DataFusionError,
    },

    #[snafu(display("Flight SQL server failed: {source}"))]
    FlightServer { source: tonic::transport::Error },

    #[snafu(display("Failed to shutdown Cayenne catalog cleanly: {source}"))]
    CatalogShutdown {
        source: cayenne::catalog::CatalogError,
    },
}

type Result<T, E = Error> = std::result::Result<T, E>;

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    tracing_subscriber::fmt()
        .with_env_filter("cayenne_flightsql=info,datafusion_flightsql=info,info")
        .try_init()
        .map_err(|source| Error::TracingInit { source })?;

    let mut session_config = SessionConfig::new()
        .with_information_schema(true)
        .with_create_default_catalog_and_schema(false);
    {
        let catalog_options = &mut session_config.options_mut().catalog;
        catalog_options.default_catalog.clone_from(&args.catalog);
        catalog_options
            .default_schema
            .clone_from(&args.default_schema);
    }

    // Register DdlExtensionPlanner so that CREATE TABLE / DROP TABLE / CREATE SCHEMA
    // executed via Flight SQL create real Cayenne (Vortex) tables, not in-memory ones.
    let state = SessionStateBuilder::new()
        .with_config(session_config)
        .with_default_features()
        .with_query_planner(Arc::new(
            ExtensionPlanQueryPlanner::from_extension_planners(vec![Arc::new(DdlExtensionPlanner)]),
        ))
        .build();
    let ctx = Arc::new(SessionContext::new_with_state(state));

    let provider = Arc::new(
        CayenneCatalogProvider::try_new(
            CayenneCatalogProviderConfig {
                data_dir: args.cayenne_data_dir.clone(),
                metadata_dir: args.cayenne_metadata_dir.clone(),
                spice_data_base_path: args.spice_data_base_path.clone(),
                footer_cache_mb: args.cayenne_footer_cache_mb,
                segment_cache_mb: args.cayenne_segment_cache_mb,
                target_file_size_mb: args.cayenne_target_file_size_mb,
                compression_strategy: None,
            },
            ctx.runtime_env(),
        )
        .await
        .context(CayenneCatalogInitSnafu)?,
    );

    provider
        .refresh()
        .await
        .map_err(|source| Error::CayenneCatalogRefresh { source })?;

    ensure_default_schema_exists(&provider, &args.default_schema)?;

    ctx.register_catalog(
        &args.catalog,
        Arc::clone(&provider) as Arc<dyn datafusion::catalog::CatalogProvider>,
    );

    // Register CayenneDdlHandler so that CREATE TABLE / DROP TABLE / CREATE SCHEMA
    // executed via Flight SQL create real Cayenne (Vortex) tables, not in-memory ones.
    {
        let ddl_enabled_catalogs = Arc::new(RwLock::new(HashSet::from([args.catalog.clone()])));
        let ddl_store = new_shared_store(&args.catalog, &args.default_schema);
        let ddl_handler =
            Arc::new(cayenne::CayenneDdlHandler {}) as Arc<dyn datafusion_ddl::CatalogDdlHandler>;

        ctx.add_analyzer_rule(Arc::new(DdlAnalyzerRule::new(
            &ctx.state().catalog_list(),
            &ddl_enabled_catalogs,
            ddl_store,
            ddl_handler,
            &args.default_schema,
            &args.catalog,
        )));
    }

    let schema_names = provider.schema_names();
    tracing::info!(
        catalog = %args.catalog,
        default_schema = %args.default_schema,
        loaded_schemas = ?schema_names,
        "Registered Cayenne catalog"
    );

    let refresh_task = args
        .refresh_interval_secs
        .filter(|interval| *interval > 0)
        .map(|interval| {
            let provider = Arc::clone(&provider);
            let default_schema = args.default_schema.clone();
            tokio::spawn(async move {
                let mut ticker = tokio::time::interval(Duration::from_secs(interval));
                loop {
                    ticker.tick().await;
                    if let Err(err) = provider.refresh().await {
                        tracing::warn!("Periodic Cayenne catalog refresh failed: {err}");
                        continue;
                    }

                    if let Err(err) = ensure_default_schema_exists(&provider, &default_schema) {
                        tracing::warn!("Failed to ensure default schema after refresh: {err}");
                    }
                }
            })
        });

    tracing::info!(addr = %args.addr, "Starting Flight SQL service");

    let server_result = Server::builder()
        .add_service(
            FlightSqlService::new(Arc::clone(&ctx))
                .into_server()
                .max_decoding_message_size(256 * 1024 * 1024),
        )
        .serve_with_shutdown(args.addr, util::shutdown_signal())
        .await;

    if let Some(task) = refresh_task {
        task.abort();
        let Err(err) = task.await;
        if !err.is_cancelled() {
            tracing::warn!("Periodic refresh task terminated unexpectedly: {err}");
        }
    }

    let shutdown_result = provider
        .metadata_catalog()
        .shutdown()
        .await
        .context(CatalogShutdownSnafu);

    if let Err(source) = server_result {
        if let Err(err) = shutdown_result {
            tracing::error!(
                "Failed to shutdown Cayenne catalog cleanly after Flight SQL server error: {err}"
            );
        }
        return Err(Error::FlightServer { source });
    }

    shutdown_result?;

    tracing::info!("Flight SQL service stopped");
    Ok(())
}

fn ensure_default_schema_exists(
    provider: &Arc<CayenneCatalogProvider>,
    default_schema: &str,
) -> Result<()> {
    if provider.schema_provider(default_schema).is_some() {
        return Ok(());
    }

    provider
        .register_schema_provider(
            default_schema,
            Arc::new(CayenneSchemaProvider::new_empty(
                Arc::clone(provider.metadata_catalog()),
                default_schema.to_string(),
                Arc::clone(provider.runtime_env()),
            )) as Arc<dyn SchemaProvider>,
        )
        .map_err(|source| Error::CreateDefaultSchema {
            default_schema: default_schema.to_string(),
            source,
        })?;

    tracing::info!(
        default_schema = %default_schema,
        "Created missing default schema in Cayenne catalog"
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_default_args() {
        let args =
            Args::try_parse_from(["cayenne-flightsql"]).expect("default CLI args should parse");

        assert_eq!(args.catalog, "cayenne");
        assert_eq!(args.default_schema, "public");
        assert_eq!(args.spice_data_base_path, ".spice/data");
        assert_eq!(args.refresh_interval_secs, None);
    }

    #[test]
    fn parses_custom_args() {
        let args = Args::try_parse_from([
            "cayenne-flightsql",
            "--addr",
            "0.0.0.0:60061",
            "--catalog",
            "lake",
            "--default-schema",
            "analytics",
            "--spice-data-base-path",
            "/tmp/spice-data",
            "--refresh-interval-secs",
            "30",
        ])
        .expect("custom CLI args should parse");

        assert_eq!(args.addr.to_string(), "0.0.0.0:60061");
        assert_eq!(args.catalog, "lake");
        assert_eq!(args.default_schema, "analytics");
        assert_eq!(args.spice_data_base_path, "/tmp/spice-data");
        assert_eq!(args.refresh_interval_secs, Some(30));
    }

    #[tokio::test]
    async fn creates_missing_default_schema() {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("current time should be after unix epoch")
            .as_nanos();
        let base_path = std::env::temp_dir().join(format!("cayenne_flightsql_test_{unique}"));

        tokio::fs::create_dir_all(&base_path)
            .await
            .expect("test base path should be created");

        let ctx = SessionContext::new();
        let provider = Arc::new(
            CayenneCatalogProvider::try_new(
                CayenneCatalogProviderConfig {
                    data_dir: None,
                    metadata_dir: None,
                    spice_data_base_path: base_path.to_string_lossy().to_string(),
                    footer_cache_mb: None,
                    segment_cache_mb: None,
                    target_file_size_mb: None,
                    compression_strategy: None,
                },
                ctx.runtime_env(),
            )
            .await
            .expect("catalog provider should initialize"),
        );

        provider
            .refresh()
            .await
            .expect("catalog provider refresh should succeed");

        assert!(provider.schema_provider("public").is_none());

        ensure_default_schema_exists(&provider, "public")
            .expect("missing schema should be created successfully");
        ensure_default_schema_exists(&provider, "public")
            .expect("ensuring an existing schema should be idempotent");

        assert!(provider.schema_provider("public").is_some());

        tokio::fs::remove_dir_all(&base_path)
            .await
            .expect("test base path should be removable");
    }
}
