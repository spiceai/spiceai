#![allow(clippy::missing_errors_doc)]

use std::{io, path::Path};

use snafu::prelude::*;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to open path {}", path))]
    UnableToOpenPath {
        source: std::io::Error,
        path: String,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Trait for objects that can open a path for reading.
pub trait ReadablePath {
    /// Opens the given path and returns an object that implements `Read`.
    fn open(&self, path: &str) -> Result<Box<dyn io::Read>>;
}

pub struct StdFileSystem;

impl ReadablePath for StdFileSystem {
    fn open(&self, path: &str) -> Result<Box<dyn io::Read>> {
        let file = std::fs::File::open(path).context(UnableToOpenPathSnafu {
            path: path.to_string(),
        })?;
        Ok(Box::new(file))
    }
}

pub trait ReadableYaml: ReadablePath {
    fn open_yaml(&self, base_path: &str, basename: &str) -> Option<Box<dyn std::io::Read>> {
        let yaml_files = vec![format!("{basename}.yaml"), format!("{basename}.yml")];
        let base_path = Path::new(base_path);

        for yaml_file in yaml_files {
            let yaml_path = base_path.join(&yaml_file);
            if let Ok(yaml_file) = self.open(yaml_path.to_str()?) {
                return Some(yaml_file);
            }
        }

        None
    }
}

impl<T: ReadablePath> ReadableYaml for T {}
