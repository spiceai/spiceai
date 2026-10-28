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

//! Status command implementation.

use crate::context::RuntimeContext;
use crate::error::{self, InvalidResponseSnafu, Result};
use crate::output::{OutputFormat, write_json};
use clap::Args;
use serde::{Deserialize, Serialize};

/// Arguments for the status command.
#[derive(Args, Debug)]
#[command(
    about = "Show component status reported by a running Spice runtime",
    long_about = r#"Query the runtime's `/v1/status` endpoint and print the status
of each registered component (HTTP, Flight, OpenTelemetry, metrics, ...).

The runtime must be running and reachable at `--http-endpoint`
(default `http://127.0.0.1:8090`).

EXAMPLES
  spice status
  spice status -o json
  spice --http-endpoint http://prod:8090 status
"#
)]
pub struct StatusArgs {
    /// Output format.
    #[arg(long, short = 'o', default_value = "table", alias = "format")]
    pub output: OutputFormat,
}

/// A single component status entry returned by /v1/status.
#[derive(Debug, Serialize, Deserialize)]
struct ComponentStatus {
    name: String,
    endpoint: String,
    status: String,
}

/// Execute the status command.
pub async fn execute(ctx: &RuntimeContext, args: &StatusArgs) -> Result<()> {
    let response = ctx
        .get("/v1/status")
        .await
        .map_err(|e| error::runtime_unreachable(ctx.http_endpoint(), &e))?;

    let response = error::check_response(response, ctx.http_endpoint()).await?;

    let components: Vec<ComponentStatus> = response.json().await.map_err(|e| {
        InvalidResponseSnafu {
            message: format!("Failed to parse status response: {e}"),
        }
        .build()
    })?;

    match args.output {
        OutputFormat::Table => {
            for c in &components {
                println!("{:<20} {:<30} {}", c.name, c.endpoint, c.status);
            }
        }
        OutputFormat::Json => {
            write_json(&components)?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::TlsServer;

    const STATUS_BODY: &str = r#"[{"name":"http","endpoint":"127.0.0.1:8090","status":"Ready"}]"#;

    fn ctx_for(server: &TlsServer, root_cert: Option<String>) -> RuntimeContext {
        RuntimeContext::with_args(Some(server.url().to_string()), None, None, root_cert)
            .expect("context should build")
    }

    fn table() -> StatusArgs {
        StatusArgs {
            output: OutputFormat::Table,
        }
    }

    /// `--tls-root-certificate-file` is a global flag, so the HTTP commands have to trust it
    /// as `spice sql` does, not only the system roots.
    #[tokio::test]
    async fn status_trusts_the_tls_root_certificate_file() {
        let server = TlsServer::answering(STATUS_BODY);
        let ctx = ctx_for(&server, Some(server.ca_file()));

        execute(&ctx, &table())
            .await
            .expect("status should succeed against a runtime whose CA was passed");
    }

    /// A certificate the client does not trust is a TLS problem with a known remedy, not a
    /// runtime that is down.
    #[tokio::test]
    async fn status_reports_a_certificate_failure_rather_than_an_unavailable_runtime() {
        let server = TlsServer::answering(STATUS_BODY);

        for root_cert in [Some(server.unrelated_ca_file()), None] {
            let ctx = ctx_for(&server, root_cert.clone());
            let message = execute(&ctx, &table())
                .await
                .expect_err("status should fail when the certificate is not trusted")
                .to_string();

            assert!(
                message.contains("TLS certificate verification failed")
                    && message.contains(server.url())
                    && message.contains("--tls-root-certificate-file"),
                "root cert {root_cert:?}: expected a certificate error naming the endpoint and the flag, got: {message}"
            );
            assert!(
                !message.contains("Is it running?"),
                "root cert {root_cert:?}: a certificate failure is not an unavailable runtime, got: {message}"
            );
        }
    }
}
