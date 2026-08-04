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

use crate::context::{DEFAULT_HTTP_ENDPOINT, RuntimeContext};
use crate::error::Result;
use crate::output::OutputFormat;
use clap::Args;
use spice_cloud_client::endpoints::flight_endpoint as spice_cloud_flight_endpoint;
use std::net::IpAddr;

/// Arguments for the sql command.
#[derive(Args, Debug)]
#[command(
    about = "Run SQL queries against the Spice.ai runtime",
    long_about = r#"Run SQL queries against the Spice.ai runtime

The `-sql` form is a root-level shortcut for a one-shot query. Quote multi-word
queries so the shell passes them as one argument.

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

    /// Specifies the remote Spice instance's Arrow Flight (gRPC) endpoint — the runtime's
    /// flight port, 50051 by default, not its HTTP API.
    /// Supports http://, https://, grpc://, or grpc+tls:// schemes (http:// being plaintext
    /// gRPC); behind a proxy or ingress it needs a gRPC-capable route.
    /// If not provided, uses local spiced runtime.
    /// `nql` additionally needs --http-endpoint pointed at the same runtime.
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

    /// Output format for direct query mode
    #[arg(long, short = 'o', default_value = "table")]
    pub output: OutputFormat,
}

/// Execute the sql command.
pub async fn execute(ctx: &RuntimeContext, args: &SqlArgs) -> Result<()> {
    let repl_config = build_repl_config(ctx, args);
    if let Some(query) = &args.query {
        if args.output == OutputFormat::Json {
            repl::run_query_json(repl_config, query)
                .await
                .map_err(|e| crate::error::Error::Repl {
                    message: e.to_string(),
                })?;
        } else {
            repl::run_query(repl_config, query)
                .await
                .map_err(|e| crate::error::Error::Repl {
                    message: e.to_string(),
                })?;
        }
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

    let another_runtime = may_be_another_runtime(&http_endpoint, &flight_endpoint);

    repl::ReplConfig {
        repl_flight_endpoint: flight_endpoint,
        http_endpoint,
        http_endpoint_may_be_another_runtime: another_runtime,
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

/// Whether the REPL's HTTP endpoint addresses a different runtime than its SQL queries do.
///
/// `--endpoint` moves only the Flight endpoint; the HTTP endpoint comes from the global
/// `--http-endpoint`, whose default is this machine. Point the REPL at a remote runtime and the
/// two disagree — SQL goes to the remote, `nql` to whatever is listening locally.
///
/// A non-default HTTP endpoint is taken at its word: the user set it, and cloud mode derives it
/// from the region alongside the Flight endpoint (so `data.` vs `flight.` hosts are not a
/// mismatch). Only the untouched local default paired with a non-loopback Flight target is one.
fn may_be_another_runtime(http_endpoint: &str, flight_endpoint: &str) -> bool {
    if http_endpoint != DEFAULT_HTTP_ENDPOINT {
        return false;
    }

    !is_loopback_endpoint(flight_endpoint)
}

/// Whether `endpoint`'s host is this machine, for endpoints in `scheme://host:port/…` form.
fn is_loopback_endpoint(endpoint: &str) -> bool {
    let mut authority = endpoint;
    if let Some((_scheme, rest)) = authority.split_once("://") {
        authority = rest;
    }
    if let Some(path_start) = authority.find(['/', '?', '#']) {
        authority = &authority[..path_start];
    }
    if let Some((_userinfo, host)) = authority.rsplit_once('@') {
        authority = host;
    }

    // An IPv6 literal is bracketed, so its own colons are not port separators.
    let host = if let Some(rest) = authority.strip_prefix('[') {
        rest.split_once(']').map_or(rest, |(host, _rest)| host)
    } else {
        let split = authority.split_once(':');
        split.map_or(authority, |(host, _port)| host)
    };

    if host.eq_ignore_ascii_case("localhost") {
        return true;
    }

    host.parse::<IpAddr>().is_ok_and(|ip| ip.is_loopback())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// #11005: `spice sql --endpoint <remote>` moves only the Flight endpoint, so the REPL's
    /// HTTP-backed `nql` kept asking the local runtime — silently answering from a different
    /// instance than the one every SQL query in the same session went to.
    #[test]
    fn a_remote_flight_endpoint_with_the_default_http_endpoint_is_another_runtime() {
        assert!(may_be_another_runtime(
            DEFAULT_HTTP_ENDPOINT,
            "http://spice-test.127.0.0.1.nip.io:50051"
        ));
        assert!(may_be_another_runtime(
            DEFAULT_HTTP_ENDPOINT,
            "https://spiced.internal:50051"
        ));
    }

    /// The default local session, and an explicitly named local Flight endpoint, both reach the
    /// same runtime the default HTTP endpoint does — `nql` must keep working untouched.
    #[test]
    fn a_loopback_flight_endpoint_is_the_same_runtime() {
        for flight_endpoint in [
            "http://localhost:50051",
            "http://LOCALHOST:50051",
            "http://127.0.0.1:50051",
            "http://127.7.7.7:50051",
            "https://[::1]:50051",
        ] {
            assert!(
                !may_be_another_runtime(DEFAULT_HTTP_ENDPOINT, flight_endpoint),
                "{flight_endpoint} is this machine"
            );
        }
    }

    /// An HTTP endpoint the user (or cloud mode) chose is taken at its word, even when its host
    /// differs from the Flight host — Cloud serves the two APIs from `data.` and `flight.`.
    #[test]
    fn a_chosen_http_endpoint_is_never_reported_as_another_runtime() {
        assert!(!may_be_another_runtime(
            "http://spiced.internal:8090",
            "http://spiced.internal:50051"
        ));
        assert!(!may_be_another_runtime(
            "https://us-east-1.data.spiceai.io",
            "https://us-east-1.flight.spiceai.io"
        ));
        // Same host as the default but a different port: still chosen, still trusted.
        assert!(!may_be_another_runtime(
            "http://127.0.0.1:9090",
            "http://spiced.internal:50051"
        ));
    }

    #[test]
    fn a_host_without_a_port_or_scheme_is_still_classified() {
        assert!(is_loopback_endpoint("http://localhost"));
        assert!(is_loopback_endpoint("localhost:50051"));
        assert!(is_loopback_endpoint("http://user@127.0.0.1:50051/path"));
        assert!(!is_loopback_endpoint("spice-test.127.0.0.1.nip.io"));
        assert!(!is_loopback_endpoint("http://192.168.1.10:50051"));
    }
}
