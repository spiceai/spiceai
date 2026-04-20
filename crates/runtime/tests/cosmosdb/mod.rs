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

//! Azure Cosmos DB (`NoSQL`) connector integration tests.
//!
//! Tests that touch a live Cosmos account are marked `#[ignore]` and run with
//! `cargo test --features cosmosdb -- --ignored cosmosdb_live`. They read
//! credentials from the environment:
//!
//! * `COSMOSDB_CONNECTION_STRING` — full Azure connection string (preferred), OR
//! * `COSMOSDB_ACCOUNT_ENDPOINT` + `COSMOSDB_ACCOUNT_KEY` — discrete pieces.
//! * `COSMOSDB_INTEGRATION_DATABASE` (default `spice-integration`)
//! * `COSMOSDB_INTEGRATION_CONTAINER` (default `documents`)
//!
//! Tests that exercise only connector registration / parameter plumbing are
//! not ignored and run in CI.
//!
//! The Azure Cosmos emulator (`mcr.microsoft.com/cosmosdb/linux/azure-cosmos-emulator`)
//! is intentionally NOT used here: its 3+ GB image and 3–5 minute cold-start
//! exceeds the budgets of the shared runner and `docker/mod.rs`
//! `CONTAINER_SEMAPHORE`. A future on-demand CI job can add it behind a
//! `cosmosdb-emulator` feature flag.

#![allow(dead_code, clippy::allow_attributes)]

use std::collections::HashMap;
use std::env;
use std::sync::Arc;

use app::AppBuilder;
use runtime::Runtime;
use spicepod::{component::dataset::Dataset, param::Params};

use crate::{configure_test_datafusion, init_tracing, utils::test_request_context};

const DEFAULT_DATABASE: &str = "spice-integration";
const DEFAULT_CONTAINER: &str = "documents";

/// Credentials + destination for live Cosmos integration tests. `None` if the
/// required env vars are unset, which lets `#[ignore]`-gated tests skip
/// cleanly when run without real Cosmos.
struct LiveConfig {
    params: HashMap<String, String>,
    database: String,
    container: String,
}

fn live_config_from_env() -> Option<LiveConfig> {
    let database =
        env::var("COSMOSDB_INTEGRATION_DATABASE").unwrap_or_else(|_| DEFAULT_DATABASE.to_string());
    let container = env::var("COSMOSDB_INTEGRATION_CONTAINER")
        .unwrap_or_else(|_| DEFAULT_CONTAINER.to_string());

    let mut params: HashMap<String, String> = HashMap::new();
    if env::var("COSMOSDB_CONNECTION_STRING").is_ok() {
        params.insert(
            "cosmosdb_connection_string".to_string(),
            "${ env:COSMOSDB_CONNECTION_STRING }".to_string(),
        );
    } else if env::var("COSMOSDB_ACCOUNT_ENDPOINT").is_ok()
        && env::var("COSMOSDB_ACCOUNT_KEY").is_ok()
    {
        params.insert(
            "cosmosdb_account_endpoint".to_string(),
            "${ env:COSMOSDB_ACCOUNT_ENDPOINT }".to_string(),
        );
        params.insert(
            "cosmosdb_account_key".to_string(),
            "${ env:COSMOSDB_ACCOUNT_KEY }".to_string(),
        );
    } else {
        return None;
    }

    Some(LiveConfig {
        params,
        database,
        container,
    })
}

fn make_live_dataset(name: &str, config: &LiveConfig) -> Dataset {
    let from = format!("cosmosdb:{}.{}", config.database, config.container);
    let mut dataset = Dataset::new(from, name.to_string());
    dataset.params = Some(Params::from_string_map(config.params.clone()));
    dataset
}

/// Smoke test: the Cosmos DB connector must be reachable via the runtime's
/// factory registry and accept its parameter spec. Offline — no HTTP call —
/// so it can run in CI without credentials.
#[tokio::test]
async fn cosmosdb_connector_factory_is_registered() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=info,info"));

    test_request_context()
        .scope(async {
            // Building the Runtime forces the `linkme` distributed slice to
            // evaluate, which registers every compiled-in connector including
            // `cosmosdb` via `register_data_connector!`. If the cosmosdb crate
            // fails to link or the factory panics during registration, this
            // test surfaces it without needing live credentials.
            configure_test_datafusion();
            let _rt = Runtime::builder()
                .with_app(AppBuilder::new("cosmosdb_smoke").build())
                .build()
                .await;
            Ok::<_, anyhow::Error>(())
        })
        .await
}

/// Live test: SELECT against a real Cosmos account. Requires the env vars
/// documented at the top of this module. Skipped by default.
#[tokio::test]
#[ignore = "requires live Cosmos credentials (COSMOSDB_CONNECTION_STRING or COSMOSDB_ACCOUNT_ENDPOINT+KEY)"]
async fn cosmosdb_live_select_returns_rows() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    let Some(config) = live_config_from_env() else {
        panic!(
            "cosmosdb_live_select_returns_rows: set COSMOSDB_CONNECTION_STRING (or \
             COSMOSDB_ACCOUNT_ENDPOINT + COSMOSDB_ACCOUNT_KEY) to run this test."
        );
    };

    test_request_context()
        .scope(async {
            let dataset = make_live_dataset("cosmos_live", &config);
            let app = AppBuilder::new("cosmosdb_live")
                .with_dataset(dataset)
                .build();

            configure_test_datafusion();
            let rt = Runtime::builder().with_app(app).build().await;
            let cloned_rt = Arc::new(rt.clone());

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
                    return Err(anyhow::anyhow!("Timed out waiting for Cosmos DB dataset to load"));
                }
                () = cloned_rt.load_components() => {}
            }

            // Issue a SELECT — the dataset must be queryable end-to-end. We
            // don't snapshot the rows because the container contents are
            // operator-controlled; verifying the query completes is enough.
            let df = rt
                .datafusion()
                .ctx
                .sql("SELECT COUNT(*) as n FROM cosmos_live")
                .await?;
            let _batches = df.collect().await?;

            Ok::<_, anyhow::Error>(())
        })
        .await
}

/// Live test: the resilience layer must let a SELECT succeed even when the
/// underlying account is lightly-loaded. Running it repeatedly exercises the
/// shared per-endpoint concurrency budget.
#[tokio::test]
#[ignore = "requires live Cosmos credentials"]
async fn cosmosdb_live_repeated_queries_share_budget() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=info,info"));

    let Some(config) = live_config_from_env() else {
        panic!("set Cosmos DB credentials to run this test");
    };

    test_request_context()
        .scope(async {
            let dataset = make_live_dataset("cosmos_live_rep", &config);
            let app = AppBuilder::new("cosmosdb_live_rep")
                .with_dataset(dataset)
                .build();

            configure_test_datafusion();
            let rt = Runtime::builder().with_app(app).build().await;
            let cloned_rt = Arc::new(rt.clone());

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
                    return Err(anyhow::anyhow!("Timed out waiting for Cosmos DB dataset to load"));
                }
                () = cloned_rt.load_components() => {}
            }

            // Three concurrent scans — the per-account semaphore limits
            // in-flight operations to `max_concurrent_requests` (default 4),
            // so this should always complete without error.
            let mut handles = Vec::new();
            for _ in 0..3 {
                let rt_clone = rt.clone();
                handles.push(tokio::spawn(async move {
                    let df = rt_clone
                        .datafusion()
                        .ctx
                        .sql("SELECT 1 FROM cosmos_live_rep LIMIT 1")
                        .await?;
                    let _ = df.collect().await?;
                    Ok::<_, anyhow::Error>(())
                }));
            }
            for handle in handles {
                handle.await??;
            }

            Ok::<_, anyhow::Error>(())
        })
        .await
}
