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

use snafu::prelude::*;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to open path {}: {source}", path.display()))]
    UnableToOpenPath {
        source: std::io::Error,
        path: PathBuf,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Trait for objects that can open a path for reading.
pub trait ReadablePath<T> {
    /// Opens the given path and returns an object that implements `Read`.
    fn open(&self, path: impl Into<PathBuf>) -> Result<Box<dyn io::Read>>;
}

pub struct StdFileSystem;

impl<T> ReadablePath<T> for StdFileSystem {
    fn open(&self, path: impl Into<PathBuf>) -> Result<Box<dyn io::Read>> {
        let path = path.into();
        let file =
            std::fs::File::open(&path).context(UnableToOpenPathSnafu { path: path.clone() })?;
        Ok(Box::new(file))
    }
}

pub trait ReadableYaml<T>: ReadablePath<T> {
    fn open_yaml(
        &self,
        base_path: impl Into<PathBuf>,
        basename: &str,
    ) -> Option<Box<dyn std::io::Read>> {
        let yaml_files = vec![format!("{basename}.yaml"), format!("{basename}.yml")];
        let base_path = base_path.into();

        for yaml_file in yaml_files {
            let yaml_path = base_path.join(&yaml_file);
            if let Ok(yaml_file) = self.open(yaml_path.to_str()?) {
                return Some(yaml_file);
            }
        }

        None
    }

    fn open_exact_yaml(&self, filename: &PathBuf) -> Result<Box<dyn std::io::Read>> {
        self.open(filename)
    }
}

impl<T: ReadablePath<T>> ReadableYaml<T> for T {}
