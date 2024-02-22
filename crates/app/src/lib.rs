#![allow(clippy::missing_errors_doc)]

use std::path::PathBuf;

use snafu::prelude::*;
use spicepod::{component::dataset::Dataset, component::model::Model, Spicepod};

#[derive(Debug, PartialEq)]
pub struct App {
    pub name: String,

    pub datasets: Vec<Dataset>,

    pub models: Vec<Model>,

    pub spicepods: Vec<Spicepod>,
}

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to load spicepod {}: {source}", path.display()))]
    UnableToLoadSpicepod {
        source: spicepod::Error,
        path: PathBuf,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

impl App {
    // This will load an app's spicepod.yaml file and all of its components and dependencies.
    pub fn new(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();

        let spicepod_root =
            match Spicepod::load(&path).context(UnableToLoadSpicepodSnafu { path: path.clone() }) {
                Ok(spicepod) => spicepod,
                Err(e) => {
                    tracing::error!("{}", e);
                    None
                }
            };

        let mut datasets: Vec<Dataset> = vec![];
        let mut models: Vec<Model> = vec![];
        let mut spicepods: Vec<Spicepod> = vec![];
        let mut root_spicepod_name = String::new();

        if let Some(spicepod_root) = spicepod_root {
            for dataset in &spicepod_root.datasets {
                datasets.push(dataset.clone());
            }

            for model in &spicepod_root.models {
                models.push(model.clone());
            }

            root_spicepod_name = spicepod_root.name.clone();

            for dependency in &spicepod_root.dependencies {
                let dependency_path = path.join("spicepods").join(dependency);
                let dependent_spicepod = Spicepod::load(&dependency_path)
                    .context(UnableToLoadSpicepodSnafu {
                        path: &dependency_path,
                    })?
                    .unwrap();
                for dataset in &dependent_spicepod.datasets {
                    datasets.push(dataset.clone());
                }
                for model in &dependent_spicepod.models {
                    models.push(model.clone());
                }
                spicepods.push(dependent_spicepod);
            }

            spicepods.push(spicepod_root);
        }

        Ok(App {
            name: root_spicepod_name,
            datasets,
            models,
            spicepods,
        })
    }
}
