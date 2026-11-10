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

// use std::{collections::HashMap, sync::Arc};

use std::{fs, path::PathBuf, sync::Arc};

use snafu::prelude::*;

use async_trait::async_trait;
use runtime::{
    Runtime,
    extension::{Error as ExtensionError, Extension, ExtensionFactory, ExtensionManifest, Result},
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("{source}"))]
    OpenDuckDb {
        source: data_components::adbc_helpers::ManagedDuckDbError,
    },

    #[snafu(display("Failed to generate the {benchmark} data with DuckDB: {source}"))]
    Generate {
        benchmark: String,
        source: adbc_core::error::Error,
    },

    #[snafu(display("The TPC data generation task failed: {source}"))]
    Join { source: tokio::task::JoinError },

    #[snafu(display(
        "Invalid benchmark type. Must be either 'tpch' or 'tpcds', got: {}",
        benchmark
    ))]
    InvalidBenchmark { benchmark: String },
}

pub struct TpcExtension {
    manifest: ExtensionManifest,
}

impl TpcExtension {
    #[must_use]
    pub fn new(manifest: ExtensionManifest) -> Self {
        TpcExtension { manifest }
    }
}

impl Default for TpcExtension {
    fn default() -> Self {
        TpcExtension::new(ExtensionManifest::default())
    }
}

#[async_trait]
impl Extension for TpcExtension {
    fn name(&self) -> &'static str {
        "tpc"
    }

    async fn initialize(&mut self, _runtime: &Runtime) -> Result<()> {
        if !self.manifest.enabled {
            return Ok(());
        }

        Ok(())
    }

    async fn on_start(&self, _runtime: Arc<Runtime>) -> Result<()> {
        if !self.manifest.enabled {
            return Ok(());
        }

        let benchmark = self
            .manifest
            .params
            .get("benchmark")
            .map_or(String::from("tpch"), std::string::ToString::to_string);

        if benchmark != "tpch" && benchmark != "tpcds" {
            return Err(ExtensionError::UnableToStartExtension {
                source: Box::new(Error::InvalidBenchmark { benchmark }),
            });
        }

        let path = self.manifest.params.get("path").map_or(
            format!(".spice/data/{benchmark}.db"),
            std::string::ToString::to_string,
        );

        if let Some(parent) = PathBuf::from(&path).parent() {
            fs::create_dir_all(parent).map_err(|e| ExtensionError::UnableToStartExtension {
                source: Box::new(e),
            })?;
        }

        let scale_factor = self
            .manifest
            .params
            .get("scale_factor")
            .and_then(|v| v.parse::<i32>().ok())
            .unwrap_or(1);

        tracing::info!("TPC extension is loading");
        tracing::info!(
            "Setting up {benchmark} benchmark datasets with scale factor {scale_factor}, using file {path}"
        );

        let gen_func = match benchmark.as_str() {
            "tpch" => "dbgen",
            "tpcds" => "dsdgen",
            _ => {
                return Err(ExtensionError::UnableToStartExtension {
                    source: Box::new(Error::InvalidBenchmark {
                        benchmark: benchmark.clone(),
                    }),
                });
            }
        };
        let query = format!(
            r"
            INSTALL {benchmark};
            LOAD {benchmark};
            CALL {gen_func}(sf = {scale_factor});"
        );

        // Generating the data runs inside the installed DuckDB library and can take
        // minutes, so it runs on a blocking thread.
        let generate_benchmark = benchmark.clone();
        tokio::task::spawn_blocking(move || generate(&path, &query, generate_benchmark))
            .await
            .context(JoinSnafu)
            .and_then(|result| result)
            .boxed()
            .map_err(|source| ExtensionError::UnableToStartExtension { source })?;

        tracing::info!("{benchmark} data loaded");

        Ok(())
    }
}

/// Writes the benchmark's tables into the `DuckDB` database at `path`, through the
/// installed `DuckDB` library.
fn generate(path: &str, query: &str, benchmark: String) -> Result<(), Error> {
    use adbc_core::{Connection as _, Database as _, Statement as _};
    use data_components::adbc_helpers::{
        DUCKDB_ADBC_ENTRYPOINT, DUCKDB_DRIVER, ManagedDuckDbError, check_duckdb_version,
        load_adbc_database, managed_duckdb_options,
    };

    let database = load_adbc_database(
        DUCKDB_DRIVER,
        Some(DUCKDB_ADBC_ENTRYPOINT),
        managed_duckdb_options(path, false),
    )
    .map_err(|source| ManagedDuckDbError::MissingLibrary {
        connector: "duckdb",
        source,
    })
    .context(OpenDuckDbSnafu)?;
    check_duckdb_version(&database, "duckdb").context(OpenDuckDbSnafu)?;

    let generate = || -> Result<(), adbc_core::error::Error> {
        let mut connection = database.new_connection()?;
        let mut statement = connection.new_statement()?;
        statement.set_sql_query(query)?;
        statement.execute_update()?;
        Ok(())
    };
    generate().context(GenerateSnafu { benchmark })
}

#[derive(Clone, Default)]
pub struct TpcExtensionFactory {
    manifest: ExtensionManifest,
}

impl TpcExtensionFactory {
    #[must_use]
    pub fn new(manifest: ExtensionManifest) -> Self {
        TpcExtensionFactory { manifest }
    }
}

impl ExtensionFactory for TpcExtensionFactory {
    fn create(&self) -> Box<dyn Extension> {
        Box::new(TpcExtension {
            manifest: self.manifest.clone(),
        })
    }
}
