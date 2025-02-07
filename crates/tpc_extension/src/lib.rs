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

use std::{fs, path::PathBuf};

// use datafusion::catalog::TableProvider;
use duckdb::Connection;
use snafu::prelude::*;

use async_trait::async_trait;
use runtime::{
    extension::{Error as ExtensionError, Extension, ExtensionFactory, ExtensionManifest, Result},
    Runtime,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to setup DuckDB connection: {}", source))]
    UnableToSetupDuckDBConnection { source: duckdb::Error },

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

    async fn on_start(&self, _runtime: &Runtime) -> Result<()> {
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
                source: Box::new(Error::InvalidBenchmark {
                    benchmark: benchmark.clone(),
                }),
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

        let connection = Connection::open(path.clone())
            .boxed()
            .map_err(|source| ExtensionError::UnableToStartExtension { source })?;

        tracing::info!("Setting up {benchmark} benchamrk datasets with scale factor {scale_factor}, using file {path}");

        let gen_func = match benchmark.as_str() {
            "tpch" => String::from("dbgen"),
            "tpcds" => String::from("dsdgen"),
            _ => {
                return Err(ExtensionError::UnableToStartExtension {
                    source: Box::new(Error::InvalidBenchmark {
                        benchmark: benchmark.clone(),
                    }),
                })
            }
        };

        let query = format!(
            r"
            INSTALL {benchmark};
            LOAD {benchmark};
            CALL {gen_func}(sf = {scale_factor});"
        );

        connection
            .execute_batch(query.as_str())
            .boxed()
            .map_err(|source| ExtensionError::UnableToStartExtension { source })?;

        connection
            .close()
            .map_err(|(_, err)| ExtensionError::UnableToStartExtension {
                source: Box::new(err),
            })?;

        tracing::info!("{benchmark} data loaded");

        Ok(())
    }
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
