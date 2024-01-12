#![allow(clippy::missing_errors_doc)]

use std::io;

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
