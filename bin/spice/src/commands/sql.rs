/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! SQL command implementation - starts an interactive SQL REPL or runs one query.

use crate::context::RuntimeContext;
use crate::error::Result;
use clap::Args;
use spice_cloud_client::endpoints::flight_endpoint as spice_cloud_flight_endpoint;

/// Arguments for the sql command.
#[derive(Args, Debug)]
#[command(
    about = "Run SQL queries against the Spice.ai runtime",
    long_about = r#"Run SQL queries against the Spice.ai runtime

Examples:
  $ spice -sql "show tables"
  # Runs a single SQL query and exits.

  $ spice sql --query "select * from taxi_trips limit 10"
  # Runs a single SQL query through the sql command and exits.

  $ spice sql
  Welcome to the Spice.ai SQL REPL! Type 'help' for help.

  show tables;  -- list available tables

  $ spice sql --expanded
  # Starts the REPL in expanded view (column-per-line). Toggle at runtime with `.expanded`.

See more at: https://spiceai.org/docs/"#
)]
pub struct SqlArgs {
    /// SQL query to run directly instead of opening the interactive REPL.
    #[arg(long, value_name = "SQL")]
    pub query: Option<String>,

    /// Specifies the remote Spice instance endpoint.
    /// Supports http://, https://, grpc://, or grpc+tls:// schemes.
    /// If not provided, uses local spiced runtime.
    #[arg(long)]
    endpoint: Option<String>,

    /// (Deprecated) Specifies the remote Spice instance Flight endpoint (treated as gRPC endpoint)
    #[arg(long)]
    flight_endpoint: Option<String>,

    /// Control whether the results cache is used for queries
    #[arg(long, default_value = "cache")]
    cache_control: String,

    /// The path to the root certificate file used to verify the Spice.ai runtime server certificate
    #[arg(long)]
    tls_root_certificate_file: Option<String>,

    /// The path to the client certificate file for mTLS authentication.
    /// Required when connecting directly to a cluster node that enforces mutual TLS.
    /// Must be used together with --client-tls-key-file.
    #[arg(long, requires = "client_tls_key_file")]
    client_tls_certificate_file: Option<String>,

    /// The path to the client private key file for mTLS authentication.
    /// Required when connecting directly to a cluster node that enforces mutual TLS.
    /// Must be used together with --client-tls-certificate-file.
    #[arg(long, requires = "client_tls_certificate_file")]
    client_tls_key_file: Option<String>,

    /// Custom HTTP headers in format 'Key:Value' (can be specified multiple times)
    #[arg(long = "headers", value_name = "KEY:VALUE")]
    custom_headers: Vec<String>,

    /// Start the REPL in expanded view, rendering each column on its own line
    /// per record. Useful for wide tables; can be toggled at runtime with `.expanded`.
    #[arg(long, short = 'x')]
    expanded: bool,
}

/// Execute the sql command.
pub async fn execute(ctx: &RuntimeContext, args: &SqlArgs) -> Result<()> {
    let repl_config = build_repl_config(ctx, args);
    if let Some(query) = &args.query {
        repl::run_query(repl_config, query)
            .await
            .map_err(|e| crate::error::Error::Repl {
                message: e.to_string(),
            })?;
        return Ok(());
    }

    repl::run(repl_config)
        .await
        .map_err(|e| crate::error::Error::Repl {
            message: e.to_string(),
        })?;

    Ok(())
}

/// Build the REPL configuration from CLI args.
fn build_repl_config(ctx: &RuntimeContext, args: &SqlArgs) -> repl::ReplConfig {
    let flight_endpoint = args
        .endpoint
        .clone()
        .or_else(|| args.flight_endpoint.clone())
        .map_or_else(
            || {
                if let Some(region) = ctx.cloud_region() {
                    spice_cloud_flight_endpoint(region)
                } else {
                    "http://localhost:50051".to_string()
                }
            },
            |e| {
                // Convert scheme if needed
                if e.starts_with("grpc://") {
                    e.replace("grpc://", "http://")
                } else if e.starts_with("grpc+tls://") {
                    e.replace("grpc+tls://", "https://")
                } else {
                    e
                }
            },
        );

    let http_endpoint = ctx.http_endpoint().to_string();

    let cache_control = match args.cache_control.as_str() {
        "no-cache" => repl::cache_control::CacheControl::NoCache,
        _ => repl::cache_control::CacheControl::Cache,
    };

    repl::ReplConfig {
        repl_flight_endpoint: flight_endpoint,
        http_endpoint,
        tls_root_certificate_file: args.tls_root_certificate_file.clone(),
        client_tls_certificate_file: args.client_tls_certificate_file.clone(),
        client_tls_key_file: args.client_tls_key_file.clone(),
        api_key: ctx.api_key().map(String::from),
        user_agent: Some(ctx.user_agent().to_string()),
        cache_control,
        custom_headers: args.custom_headers.clone(),
        expanded: args.expanded,
    }
}
