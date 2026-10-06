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

use std::{
    collections::HashMap,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::Arc,
    time::Duration,
};

use anyhow::Context;
use arrow::{record_batch::RecordBatch, util::display::array_value_to_string};
use datafusion::prelude::SessionContext;
use futures::TryStreamExt;
use iceberg::{
    Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent,
    io::{FileIO, LocalFsStorageFactory},
    memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder},
    spec::{NestedField, PrimitiveType, Schema, Type},
    table::StaticTable,
};
use iceberg_datafusion::IcebergTableProvider;
use rand::RngExt;
use runtime::{Runtime, auth::EndpointAuth, config::Config};
use spicepod::component::dataset::Dataset;
use url::Url;

use crate::{
    init_tracing,
    utils::{register_test_connectors, test_request_context, wait_until_true},
};

const LOCALHOST: IpAddr = IpAddr::V4(Ipv4Addr::LOCALHOST);

pub fn get_s3_dictionary_dataset(name: &str) -> Dataset {
    Dataset::new(
        "s3://spiceai-public-datasets/dictionary_example/dictionary_example.parquet",
        name,
    )
}

/// A dataset that does not read from an Iceberg table cannot be handed to an
/// Iceberg client as one: `loadTable` refuses it with the reason and the fix,
/// rather than describing it with metadata the client would read as an empty
/// table.
#[tokio::test]
async fn test_iceberg_api_refuses_a_table_that_is_not_iceberg() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    let _ = rustls::crypto::CryptoProvider::install_default(
        rustls::crypto::aws_lc_rs::default_provider(),
    );
    register_test_connectors().await;

    test_request_context()
        .scope(async {
            let span = tracing::info_span!("test_iceberg_api_refuses_a_table_that_is_not_iceberg");
            let _span_guard = span.enter();

            let mut rng = rand::rng();
            let http_port: u16 = rng.random_range(50000..60000);
            let flight_port: u16 = http_port + 1;

            tracing::debug!(
                "Iceberg API Ports: http: {http_port}, flight: {flight_port}"
            );

            let api_config = Config::new()
                .with_http_bind_address(SocketAddr::new(LOCALHOST, http_port))
                .with_flight_bind_address(SocketAddr::new(LOCALHOST, flight_port));

            let app = app::AppBuilder::new("test_app")
                .with_dataset(get_s3_dictionary_dataset("dictionary_example"))
                .build();

            let rt = Arc::new(Runtime::builder().with_app(app).build().await);
            let cloned_rt = Arc::clone(&rt);

            // Start the servers
            tokio::spawn(async move {
                Box::pin(cloned_rt.start_servers(api_config, None, EndpointAuth::no_auth()))
                    .await
            });

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_mins(1)) => {
                    return Err(anyhow::anyhow!("Timed out waiting for datasets to load"));
                }
                () = Arc::clone(&rt).load_components() => {}
            }

            // Connect to the server
            let http_client = reqwest::Client::builder().build()?;

            tracing::info!("Waiting for servers to start...");
            wait_until_true(Duration::from_secs(10), || async {
                http_client
                    .get(format!("http://127.0.0.1:{http_port}/ready"))
                    .send()
                    .await
                    .is_ok()
            })
            .await;

            let http_url =
                format!("http://127.0.0.1:{http_port}/v1/namespaces/spice%1Fpublic/tables/dictionary_example");
            let response = http_client
                .get(&http_url)
                .send()
                .await
                .expect("valid response");
            assert_eq!(response.status(), reqwest::StatusCode::BAD_REQUEST);
            let body = serde_json::from_str::<serde_json::Value>(&response.text().await?)?;
            assert_eq!(body["error"]["type"], "BadRequestException");
            assert_eq!(body["error"]["code"], 400);
            let message = body["error"]["message"].as_str().expect("the error has a message");
            assert!(
                message.starts_with(
                    "Failed to load table 'spice.public.dictionary_example' as an Iceberg table: it does not read from an Iceberg table"
                ),
                "{message}"
            );
            assert!(message.contains("Query it with SQL through Spice instead"), "{message}");

            let missing = http_client
                .get(format!("http://127.0.0.1:{http_port}/v1/namespaces/spice%1Fpublic/tables/missing"))
                .send()
                .await
                .expect("valid response");
            assert_eq!(missing.status(), reqwest::StatusCode::NOT_FOUND);
            let body = serde_json::from_str::<serde_json::Value>(&missing.text().await?)?;
            assert_eq!(body["error"]["type"], "NoSuchTableException");

            Ok(())
        })
        .await
}

/// An Iceberg table Spice reads unchanged is served to Iceberg clients as
/// itself: `loadTable` returns the table's current metadata file, UUID and
/// snapshot, and a client that reads the table from that metadata sees exactly
/// the rows Spice returns for it.
#[tokio::test]
async fn test_iceberg_api_serves_a_federated_iceberg_table_as_itself() -> Result<(), anyhow::Error>
{
    let _tracing = init_tracing(Some("integration=debug,info"));
    let _ = rustls::crypto::CryptoProvider::install_default(
        rustls::crypto::aws_lc_rs::default_provider(),
    );
    register_test_connectors().await;

    test_request_context()
        .scope(async {
            let warehouse_dir = tempfile::tempdir()?;
            let warehouse = Url::from_directory_path(warehouse_dir.path())
                .map_err(|()| anyhow::anyhow!("the temporary directory is not an absolute path"))?;
            // The catalog joins a table's location to the warehouse with `/`.
            let warehouse = warehouse.as_str().trim_end_matches('/').to_string();
            let (catalog, ident) = iceberg_table_with_rows(&warehouse).await?;
            let source = catalog.load_table(&ident).await?;
            let source_snapshot = source
                .metadata()
                .current_snapshot_id()
                .context("the rows written are in a snapshot")?;

            let mut rng = rand::rng();
            let http_port: u16 = rng.random_range(50000..60000);
            let flight_port: u16 = http_port + 1;
            let api_config = Config::new()
                .with_http_bind_address(SocketAddr::new(LOCALHOST, http_port))
                .with_flight_bind_address(SocketAddr::new(LOCALHOST, flight_port));

            let app = app::AppBuilder::new("test_app")
                .with_dataset(Dataset::new(
                    format!("iceberg:{warehouse}/sales/orders"),
                    "orders",
                ))
                .build();

            let rt = Arc::new(Runtime::builder().with_app(app).build().await);
            let cloned_rt = Arc::clone(&rt);
            tokio::spawn(async move {
                Box::pin(cloned_rt.start_servers(api_config, None, EndpointAuth::no_auth())).await
            });

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_mins(1)) => {
                    return Err(anyhow::anyhow!("Timed out waiting for datasets to load"));
                }
                () = Arc::clone(&rt).load_components() => {}
            }

            let http_client = reqwest::Client::builder().build()?;
            wait_until_true(Duration::from_secs(10), || async {
                http_client
                    .get(format!("http://127.0.0.1:{http_port}/ready"))
                    .send()
                    .await
                    .is_ok()
            })
            .await;

            let response = http_client
                .get(format!(
                    "http://127.0.0.1:{http_port}/v1/namespaces/spice%1Fpublic/tables/orders"
                ))
                .send()
                .await?;
            let status = response.status();
            let body = response.text().await?;
            assert_eq!(status, reqwest::StatusCode::OK, "{body}");
            let served: iceberg_catalog_rest::LoadTableResult = serde_json::from_str(&body)?;
            assert_eq!(
                served.metadata_location.as_deref(),
                source.metadata_location()
            );
            assert_eq!(served.metadata.uuid(), source.metadata().uuid());
            assert_eq!(served.metadata.current_snapshot_id(), Some(source_snapshot));

            // An Iceberg client reads the data files itself, from the metadata
            // file it was served.
            let served_location = served
                .metadata_location
                .as_deref()
                .context("the response names the table's metadata file")?;
            let client_table =
                StaticTable::from_metadata_file(served_location, ident, FileIO::new_with_fs())
                    .await?;
            let client_batches: Vec<RecordBatch> = client_table
                .scan()
                .build()?
                .to_arrow()
                .await?
                .try_collect()
                .await?;
            let client_rows = id_name_rows(&client_batches)?;
            assert_eq!(
                client_rows,
                vec![
                    (1, "a".to_string()),
                    (2, "b".to_string()),
                    (3, "c".to_string())
                ]
            );

            let sql = http_client
                .post(format!("http://127.0.0.1:{http_port}/v1/sql"))
                .header("Content-Type", "text/plain")
                .header("Accept", "application/json")
                .body("SELECT id, name FROM orders ORDER BY id")
                .send()
                .await?;
            let status = sql.status();
            let body = sql.text().await?;
            assert_eq!(status, reqwest::StatusCode::OK, "{body}");
            let spice_rows = serde_json::from_str::<Vec<serde_json::Value>>(&body)?
                .iter()
                .map(|row| {
                    Ok((
                        row["id"].as_i64().context("id is an integer")?,
                        row["name"]
                            .as_str()
                            .context("name is a string")?
                            .to_string(),
                    ))
                })
                .collect::<Result<Vec<_>, anyhow::Error>>()?;
            assert_eq!(spice_rows, client_rows);

            Ok(())
        })
        .await
}

/// A three-row Iceberg table `sales.orders` written to a warehouse on the local
/// filesystem, so it has a current snapshot and metadata files on disk.
async fn iceberg_table_with_rows(
    warehouse: &str,
) -> Result<(Arc<dyn Catalog>, TableIdent), anyhow::Error> {
    let catalog = MemoryCatalogBuilder::default()
        .with_storage_factory(Arc::new(LocalFsStorageFactory))
        .load(
            "memory",
            HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), warehouse.to_string())]),
        )
        .await?;
    let namespace = NamespaceIdent::new("sales".to_string());
    catalog.create_namespace(&namespace, HashMap::new()).await?;
    let schema = Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::optional(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::optional(2, "name", Type::Primitive(PrimitiveType::String)).into(),
        ])
        .build()?;
    catalog
        .create_table(
            &namespace,
            TableCreation::builder()
                .name("orders".to_string())
                .schema(schema)
                .build(),
        )
        .await?;
    let catalog: Arc<dyn Catalog> = Arc::new(catalog);

    let ctx = SessionContext::new();
    ctx.register_table(
        "orders",
        Arc::new(
            IcebergTableProvider::try_new(
                Arc::clone(&catalog),
                namespace.clone(),
                "orders".to_string(),
            )
            .await?,
        ),
    )?;
    ctx.sql("INSERT INTO orders VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        .await?
        .collect()
        .await?;

    Ok((catalog, TableIdent::new(namespace, "orders".to_string())))
}

/// The `(id, name)` rows of `batches`, in `id` order.
fn id_name_rows(batches: &[RecordBatch]) -> Result<Vec<(i64, String)>, anyhow::Error> {
    let mut rows = Vec::new();
    for batch in batches {
        let ids = batch
            .column_by_name("id")
            .context("the batch has an id column")?;
        let names = batch
            .column_by_name("name")
            .context("the batch has a name column")?;
        for row in 0..batch.num_rows() {
            rows.push((
                array_value_to_string(ids, row)?.parse()?,
                array_value_to_string(names, row)?,
            ));
        }
    }
    rows.sort();
    Ok(rows)
}
