#![allow(clippy::missing_errors_doc)]

use snafu::prelude::*;
use spicepod::{component::dataset::Dataset, Spicepod};

#[derive(Debug)]
pub struct App {
    pub name: String,

    pub datasets: Vec<Dataset>,
}

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to load spicepod {}", path))]
    UnableToLoadSpicepod {
        source: spicepod::Error,
        path: String,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

impl App {
    // This will load an app's spicepod.yaml file and all of its components and dependencies.
    pub fn new(path: &str) -> Result<Self> {
        let spicepod_root = Spicepod::load(path).context(UnableToLoadSpicepodSnafu { path })?;

        Ok(App {
            name: spicepod_root.name,
            datasets: spicepod_root.datasets,
        })
    }
}
