/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

use crate::Read;
use crate::unity_catalog::credential_vending::vended_delta_table;
use crate::unity_catalog::{UCTable, UnityCatalog};
use crate::{delta_lake::DeltaTable, unity_catalog::Endpoint};
use async_trait::async_trait;
use datafusion::common::TableReference;
use datafusion::datasource::TableProvider;
use secrecy::{ExposeSecret, SecretString};
use snafu::prelude::*;
use std::{collections::HashMap, sync::Arc};
use token_provider::TokenProvider;
use tokio::runtime::Handle;

#[derive(Clone)]
pub struct DatabricksDelta {
    endpoint: Endpoint,
    token_provider: Arc<dyn TokenProvider>,
    storage_options: HashMap<String, SecretString>,
    io_runtime: Handle,
    credential_vending_client: Option<Arc<UnityCatalog>>,
}

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "A storage location for the Databricks table '{table_reference}' must be provided. Specify a storage location, and try again."
    ))]
    TableDoesNotHaveStorageLocation { table_reference: TableReference },
    #[snafu(display(
        "Failed to find the Databricks table '{table_reference}'. Verify the table exists, and try again."
    ))]
    TableDoesNotExist { table_reference: TableReference },
    #[snafu(display(
        "Failed to read the Databricks table '{table_reference}': {reason}. Point the table at object storage, and try again. See: https://spiceai.org/docs/components/data-connectors/databricks"
    ))]
    UnsupportedStorageLocation {
        table_reference: TableReference,
        reason: String,
    },
}

/// Returns the table's storage location, refusing one that is absent or outside
/// object storage. The location comes from Unity Catalog, not from the Spicepod.
fn object_storage_location(
    table: &UCTable,
    table_reference: TableReference,
) -> Result<String, Error> {
    if let Some(reason) = table.unsupported_storage_location_reason() {
        return Err(Error::UnsupportedStorageLocation {
            table_reference,
            reason,
        });
    }
    table
        .storage_location
        .clone()
        .ok_or(Error::TableDoesNotHaveStorageLocation { table_reference })
}

impl DatabricksDelta {
    pub fn new(
        endpoint: Endpoint,
        storage_options: HashMap<String, SecretString>,
        token_provider: Arc<dyn TokenProvider>,
        io_runtime: Handle,
    ) -> Self {
        Self {
            endpoint,
            token_provider,
            storage_options,
            io_runtime,
            credential_vending_client: None,
        }
    }

    /// Enables Unity Catalog credential vending: each table's storage
    /// credentials are vended through `client` instead of taken from the
    /// configured storage options.
    #[must_use]
    pub fn with_credential_vending(mut self, client: Arc<UnityCatalog>) -> Self {
        self.credential_vending_client = Some(client);
        self
    }

    /// Returns `true` when Unity Catalog credential vending is enabled.
    #[must_use]
    pub fn credential_vending_enabled(&self) -> bool {
        self.credential_vending_client.is_some()
    }

    async fn get_delta_table(
        &self,
        table_reference: TableReference,
    ) -> Result<Arc<dyn TableProvider>, Box<dyn std::error::Error + Send + Sync>> {
        let table = self.get_uc_table(&table_reference).await?;
        // Checked before vending, because a table that cannot be vended falls back to
        // `DeltaTable::open`, which would open any location.
        let table_uri = object_storage_location(&table, table_reference)?;

        if let Some(client) = &self.credential_vending_client {
            let aws_region = self
                .storage_options
                .get("aws_region")
                .map(|s| s.expose_secret().to_string());
            match vended_delta_table(Arc::clone(client), &table, &table_uri, aws_region).await {
                Ok(delta) => return Ok(Arc::new(delta) as Arc<dyn TableProvider>),
                Err(err) => {
                    tracing::warn!(
                        table = %table.full_name(),
                        "Unable to use Unity Catalog credential vending for table; falling back to configured storage credentials: {err}"
                    );
                }
            }
        }

        let mut storage_options = HashMap::new();
        for (key, value) in &self.storage_options {
            match key.as_ref() {
                "token" | "endpoint" => {}
                "client_timeout" => {
                    storage_options.insert("timeout".into(), value.clone());
                }
                _ => {
                    storage_options.insert(key.clone(), value.clone());
                }
            }
        }

        let delta_table = DeltaTable::open(table_uri, storage_options, &self.io_runtime)
            .await
            .boxed()?;

        Ok(Arc::new(delta_table) as Arc<dyn TableProvider>)
    }

    async fn get_uc_table(
        &self,
        table_reference: &TableReference,
    ) -> Result<UCTable, Box<dyn std::error::Error + Send + Sync>> {
        // Reuse the vending client when present: it carries the connector's
        // rate-control configuration and avoids rebuilding an HTTP client for
        // every table lookup.
        let table_opt = if let Some(client) = &self.credential_vending_client {
            client.get_table(table_reference).await.boxed()?
        } else {
            let uc_client = UnityCatalog::new(
                self.endpoint.clone(),
                Some(Arc::clone(&self.token_provider)),
                None,
            )
            .boxed()?;
            uc_client.get_table(table_reference).await.boxed()?
        };

        table_opt.ok_or_else(|| {
            Error::TableDoesNotExist {
                table_reference: table_reference.clone(),
            }
            .into()
        })
    }

    pub async fn resolve_table_uri(
        &self,
        table_reference: TableReference,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let table = self.get_uc_table(&table_reference).await?;
        Ok(object_storage_location(&table, table_reference)?)
    }
}

#[async_trait]
impl Read for DatabricksDelta {
    async fn table_provider(
        &self,
        table_reference: TableReference,
    ) -> Result<Arc<dyn TableProvider + 'static>, Box<dyn std::error::Error + Send + Sync>> {
        self.get_delta_table(table_reference).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{RecordBatch, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::parquet::arrow::ArrowWriter;
    use std::path::Path;
    use token_provider::StaticTokenProvider;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    /// Writes a readable Delta table with one `value` row at `dir`.
    fn write_delta_table(dir: &Path) {
        std::fs::create_dir_all(dir.join("_delta_log")).expect("create _delta_log");
        let schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Utf8, true)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(StringArray::from(vec!["LOCAL_FILE_ROW"]))],
        )
        .expect("batch builds");
        let data_file = dir.join("part-0.parquet");
        let mut writer = ArrowWriter::try_new(
            std::fs::File::create(&data_file).expect("create data file"),
            schema,
            None,
        )
        .expect("parquet writer");
        writer.write(&batch).expect("write batch");
        writer.close().expect("close parquet file");
        let size = std::fs::metadata(&data_file).expect("data file").len();
        let commit = [
            serde_json::json!({"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}}),
            serde_json::json!({"metaData": {
                "id": "00000000-0000-0000-0000-000000000000",
                "format": {"provider": "parquet", "options": {}},
                "schemaString": r#"{"type":"struct","fields":[{"name":"value","type":"string","nullable":true,"metadata":{}}]}"#,
                "partitionColumns": [],
                "configuration": {},
                "createdTime": 0
            }}),
            serde_json::json!({"add": {
                "path": "part-0.parquet",
                "partitionValues": {},
                "size": size,
                "modificationTime": 0,
                "dataChange": true
            }}),
        ]
        .iter()
        .map(serde_json::Value::to_string)
        .collect::<Vec<_>>()
        .join("\n");
        std::fs::write(dir.join("_delta_log/00000000000000000000.json"), commit)
            .expect("write commit");
    }

    async fn mock_table(server: &MockServer, name: &str, storage_location: &str) {
        Mock::given(method("GET"))
            .and(path(format!(
                "/api/2.1/unity-catalog/tables/cat.sch.{name}"
            )))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "name": name,
                "catalog_name": "cat",
                "schema_name": "sch",
                "table_type": "EXTERNAL",
                "data_source_format": "DELTA",
                "storage_location": storage_location,
                "table_id": format!("{name}-id"),
            })))
            .mount(server)
            .await;
    }

    fn databricks_delta(server: &MockServer) -> DatabricksDelta {
        DatabricksDelta::new(
            Endpoint(server.uri()),
            HashMap::new(),
            Arc::new(StaticTokenProvider::new(SecretString::from("token"))),
            Handle::current(),
        )
    }

    fn refusal(table: &str, scheme: &str) -> String {
        format!(
            "Failed to read the Databricks table 'cat.sch.{table}': its storage location uses the '{scheme}' scheme, and catalog tables are only read from object storage (s3, s3a, abfss, abfs, az, azure, wasbs, wasb, gs). Point the table at object storage, and try again. See: https://spiceai.org/docs/components/data-connectors/databricks"
        )
    }

    #[tokio::test]
    async fn refuses_a_storage_location_outside_object_storage() {
        let dir = tempfile::tempdir().expect("tempdir");
        write_delta_table(dir.path());
        let local = url::Url::from_directory_path(dir.path())
            .expect("absolute path")
            .to_string();
        let server = MockServer::start().await;
        mock_table(&server, "local", &local).await;
        mock_table(&server, "remote", "s3://bucket/tables/remote/").await;
        Mock::given(method("POST"))
            .and(path("/api/2.1/unity-catalog/temporary-table-credentials"))
            .respond_with(ResponseTemplate::new(500))
            .expect(0)
            .mount(&server)
            .await;

        let static_credentials = databricks_delta(&server);
        let vended = databricks_delta(&server).with_credential_vending(Arc::new(
            UnityCatalog::new(Endpoint(server.uri()), None, None).expect("client"),
        ));
        for delta in [&static_credentials, &vended] {
            let error = delta
                .table_provider(TableReference::parse_str("cat.sch.local"))
                .await
                .expect_err("a table stored on the Spice host must not be read");
            assert_eq!(error.to_string(), refusal("local", "file"));
            let error = delta
                .resolve_table_uri(TableReference::parse_str("cat.sch.local"))
                .await
                .expect_err("a table stored on the Spice host must not be resolved");
            assert_eq!(error.to_string(), refusal("local", "file"));
            assert_eq!(
                delta
                    .resolve_table_uri(TableReference::parse_str("cat.sch.remote"))
                    .await
                    .expect("a table stored in object storage resolves"),
                "s3://bucket/tables/remote/"
            );
        }
    }
}
