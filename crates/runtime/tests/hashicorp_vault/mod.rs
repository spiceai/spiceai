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

//! End-to-end integration tests for the `hashicorp_vault` secret store.
//!
//! These spin up a real Vault dev server and a real Postgres container,
//! write the Postgres password into Vault, then start a Spice runtime
//! configured with a `hashicorp_vault` secret store and a Postgres
//! dataset whose `pg_pass` is sourced via `${vault:password}`. We then
//! query the dataset through `DataFusion` and assert the rows come back.
//!
//! That mirrors the same flow the bring-up demo
//! (`spicepod_test/260429-hashicorp-vault-demo`) walks through manually
//! and exercises:
//!   - Secret-store init at runtime build time.
//!   - Param-string interpolation via `${ <store_name>:<key> }`.
//!   - The Vault REST round-trip (KV v2 + token auth, KV v2 + `AppRole`
//!     auth) inside the connector init path.
//!
//! Kubernetes and JWT auth are intentionally out of scope here — they
//! require either an in-cluster service-account token or pre-minted
//! RSA-signed JWTs and are covered by the live tests under
//! `crates/runtime-secrets/tests/hashicorp_vault_live.rs`.

use std::sync::Arc;

use app::AppBuilder;
use arrow::array::{Int32Array, RecordBatch, StringArray};
use futures::TryStreamExt;
use runtime::Runtime;
use serde_json::json;
use spicepod::component::dataset::Dataset;
use spicepod::component::secret::Secret;
use spicepod::param::Params;

use crate::postgres::common as pg_common;
use crate::{
    configure_test_datafusion, init_tracing,
    utils::{register_test_connectors, test_request_context},
};

mod common;

/// Path under the default `secret/` mount where the Postgres password
/// is stored. This matches the `secret/spice/postgres` layout used by
/// the demo, and the `AppRole` policy granting read on that path.
const PG_SECRET_PATH: &str = "spice/postgres";

fn make_pg_dataset(port: u16, pg_pass_ref: &str) -> Dataset {
    let mut dataset = Dataset::new("postgres:public.orders".to_string(), "orders".to_string());
    dataset.params = Some(Params::from_string_map(
        [
            ("pg_host".to_string(), "localhost".to_string()),
            ("pg_port".to_string(), port.to_string()),
            ("pg_user".to_string(), "postgres".to_string()),
            ("pg_db".to_string(), "postgres".to_string()),
            ("pg_pass".to_string(), pg_pass_ref.to_string()),
            ("pg_sslmode".to_string(), "disable".to_string()),
        ]
        .into_iter()
        .collect(),
    ));
    dataset
}

/// Seed three rows into `public.orders` so that the SELECT after
/// runtime startup has something to return.
async fn seed_orders(port: u16) -> Result<(), anyhow::Error> {
    let pool = pg_common::get_postgres_connection_pool(port.into(), None).await?;
    let conn = pool
        .connect_direct()
        .await
        .map_err(|e| anyhow::anyhow!("connect to postgres: {e}"))?;
    conn.conn
        .execute(
            "CREATE TABLE IF NOT EXISTS orders (
                 id INT PRIMARY KEY,
                 customer TEXT NOT NULL
             );",
            &[],
        )
        .await?;
    conn.conn.execute("TRUNCATE orders;", &[]).await?;
    conn.conn
        .execute(
            "INSERT INTO orders (id, customer) VALUES \
             (1, 'Acme Corp'), (2, 'Globex'), (3, 'Initech');",
            &[],
        )
        .await?;
    Ok(())
}

async fn assert_orders_query(rt: &Runtime) -> Result<(), anyhow::Error> {
    let result = rt
        .datafusion()
        .query_builder("SELECT id, customer FROM orders ORDER BY id")
        .build()
        .run()
        .await
        .map_err(|e| anyhow::anyhow!("query failed: {e}"))?;

    let batches: Vec<RecordBatch> = result
        .data
        .try_collect()
        .await
        .map_err(|e| anyhow::anyhow!("collect failed: {e}"))?;
    let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(
        total_rows, 3,
        "expected 3 rows from orders; got {total_rows}"
    );

    // Sanity-check the first row's contents so a regression where the
    // password resolves to an empty string (and we silently get back
    // some other table or an empty result) trips the assertion.
    let first = batches.first().expect("at least one batch");
    let ids = first
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("id column is Int32");
    let customers = first
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("customer column is Utf8");
    assert_eq!(ids.value(0), 1);
    assert_eq!(customers.value(0), "Acme Corp");
    Ok(())
}

#[tokio::test]
async fn vault_token_auth_resolves_pg_password() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,runtime_secrets=debug,info"));

    test_request_context()
        .scope(async {
            // 1. Spin up Postgres and seed it.
            let pg_port = pg_common::get_random_port()?;
            let pg = pg_common::start_postgres_docker_container(pg_port).await?;
            seed_orders(pg_port.try_into()?).await?;

            // 2. Spin up Vault dev mode and write the Postgres password.
            let vault_port: u16 = pg_common::get_random_port()?.try_into()?;
            let vault = common::start_vault_docker_container(vault_port).await?;
            let mut data = serde_json::Map::new();
            data.insert(
                "password".to_string(),
                json!(pg_common::PG_PASSWORD.to_string()),
            );
            common::write_kv_v2_secret(vault_port, common::VAULT_ROOT_TOKEN, PG_SECRET_PATH, data)
                .await?;

            // 3. Build a Spicepod app: hashicorp_vault store named `vault`
            //    + Postgres dataset whose pg_pass references it.
            let secret = Secret {
                from: format!("hashicorp_vault:{PG_SECRET_PATH}"),
                name: "vault".to_string(),
                description: None,
                params: Some(Params::from_string_map(
                    [
                        (
                            "hashicorp_vault_address".to_string(),
                            format!("http://127.0.0.1:{vault_port}"),
                        ),
                        (
                            "hashicorp_vault_auth_method".to_string(),
                            "token".to_string(),
                        ),
                        (
                            "hashicorp_vault_token".to_string(),
                            common::VAULT_ROOT_TOKEN.to_string(),
                        ),
                    ]
                    .into_iter()
                    .collect(),
                )),
            };

            let pg_port_u16: u16 = pg_port.try_into()?;
            let app = AppBuilder::new("hashicorp_vault_token_it")
                .with_secret(secret)
                .with_dataset(make_pg_dataset(pg_port_u16, "${ vault:password }"))
                .build();

            // 4. Boot the runtime and run the dataset query.
            register_test_connectors().await;
            configure_test_datafusion();
            let rt = Runtime::builder().with_app(app).build().await;

            let cloned_rt = Arc::new(rt.clone());
            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_mins(1)) => {
                    return Err(anyhow::anyhow!("Timed out waiting for datasets to load"));
                }
                () = cloned_rt.load_components() => {}
            }

            assert_orders_query(&rt).await?;

            vault.remove().await?;
            pg.remove().await?;
            Ok(())
        })
        .await
}

#[tokio::test]
async fn vault_approle_auth_resolves_pg_password() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,runtime_secrets=debug,info"));

    test_request_context()
        .scope(async {
            let pg_port = pg_common::get_random_port()?;
            let pg = pg_common::start_postgres_docker_container(pg_port).await?;
            seed_orders(pg_port.try_into()?).await?;

            let vault_port: u16 = pg_common::get_random_port()?.try_into()?;
            let vault = common::start_vault_docker_container(vault_port).await?;

            let mut data = serde_json::Map::new();
            data.insert(
                "password".to_string(),
                json!(pg_common::PG_PASSWORD.to_string()),
            );
            common::write_kv_v2_secret(vault_port, common::VAULT_ROOT_TOKEN, PG_SECRET_PATH, data)
                .await?;

            // Configure AppRole + a read-only policy on the demo path,
            // then mint role_id / secret_id. After this the Spice runtime
            // never sees the root token.
            let creds = common::configure_approle(
                vault_port,
                common::VAULT_ROOT_TOKEN,
                "spice-pg",
                PG_SECRET_PATH,
            )
            .await?;

            let secret = Secret {
                from: format!("hashicorp_vault:{PG_SECRET_PATH}"),
                name: "vault".to_string(),
                description: None,
                params: Some(Params::from_string_map(
                    [
                        (
                            "hashicorp_vault_address".to_string(),
                            format!("http://127.0.0.1:{vault_port}"),
                        ),
                        (
                            "hashicorp_vault_auth_method".to_string(),
                            "approle".to_string(),
                        ),
                        ("hashicorp_vault_role_id".to_string(), creds.role_id),
                        ("hashicorp_vault_secret_id".to_string(), creds.secret_id),
                    ]
                    .into_iter()
                    .collect(),
                )),
            };

            let pg_port_u16: u16 = pg_port.try_into()?;
            let app = AppBuilder::new("hashicorp_vault_approle_it")
                .with_secret(secret)
                .with_dataset(make_pg_dataset(pg_port_u16, "${ vault:password }"))
                .build();

            register_test_connectors().await;
            configure_test_datafusion();
            let rt = Runtime::builder().with_app(app).build().await;

            let cloned_rt = Arc::new(rt.clone());
            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_mins(1)) => {
                    return Err(anyhow::anyhow!("Timed out waiting for datasets to load"));
                }
                () = cloned_rt.load_components() => {}
            }

            assert_orders_query(&rt).await?;

            vault.remove().await?;
            pg.remove().await?;
            Ok(())
        })
        .await
}
