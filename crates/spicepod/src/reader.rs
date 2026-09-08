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

use std::{io, path::PathBuf};

#[cfg(feature = "object-store")]
use std::sync::Arc;

use async_trait::async_trait;
#[cfg(feature = "object-store")]
use object_store::{DynObjectStore, ObjectStoreExt};
use snafu::prelude::*;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to open path {}: {source}", path.display()))]
    UnableToOpenPath {
        source: std::io::Error,
        path: PathBuf,
    },

    #[cfg(feature = "object-store")]
    #[snafu(display("Unable to open object store path {}: {source}", path))]
    UnableToOpenObjectStorePath {
        source: object_store::Error,
        path: String,
    },

    #[cfg(feature = "object-store")]
    #[snafu(display("Unable to parse object store path {}: {source}", path))]
    UnableToParseObjectStorePath {
        source: object_store::path::Error,
        path: String,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

impl Error {
    #[must_use]
    pub fn is_not_found(&self) -> bool {
        match self {
            Self::UnableToOpenPath { source, .. } => source.kind() == std::io::ErrorKind::NotFound,
            #[cfg(feature = "object-store")]
            Self::UnableToOpenObjectStorePath { source, .. } => {
                matches!(source, object_store::Error::NotFound { .. })
            }
            #[cfg(feature = "object-store")]
            Self::UnableToParseObjectStorePath { .. } => false,
        }
    }
}

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
            tokio::fs::File::open(&path)
                .await
                .map_err(|source| Error::UnableToOpenPath {
                    source,
                    path: path.clone(),
                })?;

        let file = file.into_std().await;

        Ok(Box::new(file))
    }
}

#[async_trait]
pub trait ReadableYaml: ReadablePath {
    async fn open_yaml(
        &self,
        base_path: PathBuf,
        basename: &str,
    ) -> Result<Option<Box<dyn io::Read + Send + Sync>>> {
        let yaml_files = vec![format!("{basename}.yaml"), format!("{basename}.yml")];

        for yaml_file in yaml_files {
            let yaml_path = base_path.join(&yaml_file);
            match self.open(yaml_path).await {
                Ok(yaml_file) => return Ok(Some(yaml_file)),
                Err(err) if err.is_not_found() => {}
                Err(err) => return Err(err),
            }
        }

        Ok(None)
    }

    async fn open_exact_yaml(&self, filename: PathBuf) -> Result<Box<dyn io::Read + Send + Sync>> {
        self.open(filename.clone()).await
    }
}

impl ReadableYaml for StdFileSystem {}

#[cfg(feature = "object-store")]
pub struct ObjectStoreFilesystem {
    store: Arc<DynObjectStore>,
}

#[cfg(feature = "object-store")]
impl ObjectStoreFilesystem {
    pub fn new(store: Arc<DynObjectStore>) -> Self {
        Self { store }
    }
}

#[cfg(feature = "object-store")]
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

#[cfg(feature = "object-store")]
impl ReadableYaml for ObjectStoreFilesystem {}

#[cfg(test)]
mod tests {
    use super::*;

    struct FailingFilesystem(std::io::ErrorKind);

    #[async_trait]
    impl ReadablePath for FailingFilesystem {
        async fn open(&self, path: PathBuf) -> Result<Box<dyn io::Read + Send + Sync>> {
            Err(Error::UnableToOpenPath {
                source: std::io::Error::from(self.0),
                path,
            })
        }
    }

    impl ReadableYaml for FailingFilesystem {}

    #[tokio::test]
    async fn open_yaml_treats_only_not_found_as_absent() {
        let missing = FailingFilesystem(std::io::ErrorKind::NotFound)
            .open_yaml(PathBuf::from("/app"), "spicepod")
            .await
            .expect("not found is a normal candidate miss");
        assert!(missing.is_none());

        let Err(err) = FailingFilesystem(std::io::ErrorKind::PermissionDenied)
            .open_yaml(PathBuf::from("/app"), "spicepod")
            .await
        else {
            panic!("permission denied must not be reported as absent");
        };
        assert!(!err.is_not_found());
        assert!(matches!(
            err,
            Error::UnableToOpenPath { source, .. }
                if source.kind() == std::io::ErrorKind::PermissionDenied
        ));
    }
}
