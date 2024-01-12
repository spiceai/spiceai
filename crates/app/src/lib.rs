#![allow(clippy::missing_errors_doc)]

use std::path::PathBuf;

use snafu::prelude::*;
use spicepod::component::dataset::Dataset;

#[derive(Debug)]
pub struct App {
    pub name: String,

    pub datasets: Vec<Dataset>,
}

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to read directory contents from {}", path.display()))]
    UnableToReadDirectoryContents {
        source: std::io::Error,
        path: PathBuf,
    },
    NotImplemented,
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

// This will load an app's spicepod.yaml file and all of its components and dependencies.
pub fn load(path: impl Into<PathBuf>) -> Result<App> {
    let _path = path.into();
    NotImplementedSnafu.fail()
}
