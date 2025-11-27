#![cfg_attr(docsrs, feature(doc_auto_cfg))]

use serde::{Deserialize, Serialize};
use snafu::prelude::*;

pub mod common;
pub mod sql;
pub mod util;

#[cfg(feature = "adbc")]
pub mod adbc;
#[cfg(feature = "clickhouse")]
pub mod clickhouse;
#[cfg(feature = "duckdb")]
pub mod duckdb;
#[cfg(feature = "flight")]
pub mod flight;
#[cfg(feature = "mongodb")]
pub mod mongodb;
#[cfg(feature = "mysql")]
pub mod mysql;
#[cfg(feature = "odbc")]
pub mod odbc;
#[cfg(feature = "postgres")]
pub mod postgres;
#[cfg(feature = "sqlite")]
pub mod sqlite;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("The database file path is not within the current directory: {path}"))]
    FileNotInDirectory { path: String },
    #[snafu(display("The database file is a symlink: {path}"))]
    FileIsSymlink { path: String },
    #[snafu(display("Error reading file: {source}"))]
    FileReadError { source: std::io::Error },
}

#[derive(PartialEq, Eq, Clone, Copy, Default, Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum UnsupportedTypeAction {
    /// Refuse to create the table if any unsupported types are found
    #[default]
    Error,
    /// Log a warning for any unsupported types
    Warn,
    /// Ignore any unsupported types (i.e. skip them)
    Ignore,
    /// Attempt to convert any unsupported types to a string
    String,
}
