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

#![allow(clippy::missing_errors_doc)]

use std::{io, path::PathBuf, sync::Arc};

use async_trait::async_trait;
use object_store::{DynObjectStore, ObjectStore};
use snafu::prelude::*;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to open path {}: {source}", path.display()))]
    UnableToOpenPath {
        source: std::io::Error,
        path: PathBuf,
    },

    #[snafu(display("Unable to open object store path {}: {source}", path))]
    UnableToOpenObjectStorePath {
        source: object_store::Error,
        path: String,
    },

    #[snafu(display("Unable to parse object store path {}: {source}", path))]
    UnableToParseObjectStorePath {
        source: object_store::path::Error,
        path: String,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Trait for objects that can open a path for reading.
#[async_trait]
pub trait ReadablePath {
    /// Opens the given path and returns an object that implements `Read`.
    async fn open(&self, path: PathBuf) -> Result<Box<dyn io::Read + Send + Sync>>;
}

pub struct StdFileSystem;

#[async_trait]
impl ReadablePath for StdFileSystem {
    async fn open(&self, path: PathBuf) -> Result<Box<dyn io::Read + Send + Sync>> {
        let file =
            std::fs::File::open(&path).context(UnableToOpenPathSnafu { path: path.clone() })?;
        Ok(Box::new(file))
    }
}

#[async_trait]
pub trait ReadableYaml: ReadablePath {
    async fn open_yaml(
        &self,
        base_path: PathBuf,
        basename: &str,
    ) -> Option<Box<dyn io::Read + Send + Sync>> {
        let yaml_files = vec![format!("{basename}.yaml"), format!("{basename}.yml")];

        for yaml_file in yaml_files {
            let yaml_path = base_path.join(&yaml_file);
            if let Ok(yaml_file) = self.open(yaml_path.clone()).await {
                return Some(yaml_file);
            }
        }

        None
    }

    async fn open_exact_yaml(&self, filename: PathBuf) -> Result<Box<dyn io::Read + Send + Sync>> {
        self.open(filename.clone()).await
    }
}

impl ReadableYaml for StdFileSystem {}

pub struct ObjectStoreFilesystem {
    store: Arc<DynObjectStore>,
}

impl ObjectStoreFilesystem {
    pub fn new(store: Arc<DynObjectStore>) -> Self {
        Self { store }
    }
}

#[async_trait]
impl ReadablePath for ObjectStoreFilesystem {
    async fn open(&self, path: PathBuf) -> Result<Box<dyn io::Read + Send + Sync>> {
        let path_str = path.to_string_lossy().to_string();

        let object_path = object_store::path::Path::parse(&path_str).context(
            UnableToParseObjectStorePathSnafu {
                path: path_str.clone(),
            },
        )?;

        let bytes = self
            .store
            .get(&object_path)
            .await
            .context(UnableToOpenObjectStorePathSnafu {
                path: path_str.clone(),
            })?
            .bytes()
            .await
            .context(UnableToOpenObjectStorePathSnafu { path: path_str })?;

        Ok(Box::new(std::io::Cursor::new(bytes)))
    }
}

impl ReadableYaml for ObjectStoreFilesystem {}
