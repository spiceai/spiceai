#![allow(clippy::missing_errors_doc)]

use std::path::PathBuf;

use snafu::prelude::*;
use spicepod::{
    component::{dataset::Dataset, model::Model, secrets::Secrets},
    Spicepod,
};

#[derive(Debug, PartialEq)]
pub struct App {
    pub name: String,

    pub secrets: Secrets,

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
            Spicepod::load(&path).context(UnableToLoadSpicepodSnafu { path: path.clone() })?;
        let secrets = spicepod_root.secrets.clone();
        let mut datasets: Vec<Dataset> = vec![];
        let mut models: Vec<Model> = vec![];
        for dataset in &spicepod_root.datasets {
            datasets.push(dataset.clone());
        }

        for model in &spicepod_root.models {
            models.push(model.clone());
        }

        let root_spicepod_name = spicepod_root.name.clone();
        let mut spicepods: Vec<Spicepod> = vec![];

        for dependency in &spicepod_root.dependencies {
            let dependency_path = path.join("spicepods").join(dependency);
            let dependent_spicepod =
                Spicepod::load(&dependency_path).context(UnableToLoadSpicepodSnafu {
                    path: &dependency_path,
                })?;
            for dataset in &dependent_spicepod.datasets {
                datasets.push(dataset.clone());
            }
            for model in &dependent_spicepod.models {
                models.push(model.clone());
            }
            spicepods.push(dependent_spicepod);
        }

        spicepods.push(spicepod_root);

        Ok(App {
            name: root_spicepod_name,
            secrets,
            datasets,
            models,
            spicepods,
        })
    }
}
