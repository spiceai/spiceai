#![allow(clippy::missing_errors_doc)]

use std::{io, path::PathBuf};

use snafu::prelude::*;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to open path {}", path.display()))]
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
}

impl<T: ReadablePath<T>> ReadableYaml<T> for T {}
