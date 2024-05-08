/*
Copyright 2024 The Spice.ai OSS Authors

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

use crate::{
    modelruntime::{self, ModelRuntime, Runnable},
    modelsource::{path, source, Error as ModelSourceError, ModelSource},
};
use arrow::record_batch::RecordBatch;
use secrets::Secret;
use snafu::prelude::*;
use std::sync::Arc;

pub struct Model {
    runnable: Box<dyn Runnable>,
    pub model: spicepod::component::model::Model,
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unknown model source: {source}"))]
    UnknownModelSource { source: ModelSourceError },

    #[snafu(display("Unable to load model from path: {source}"))]
    UnableToLoadModel { source: ModelSourceError },

    #[snafu(display("Unable to init model: {source}"))]
    UnableToInitModel { source: modelruntime::Error },

    #[snafu(display("Unable to run model: {source}"))]
    UnableToRunModel { source: modelruntime::Error },

    #[snafu(display("Unable to load required secrets"))]
    UnableToLoadRequiredSecrets {},
}

impl Model {
    pub async fn load(
        model: spicepod::component::model::Model,
        secret: Option<Secret>,
    ) -> Result<Self> {
        let source = source(&model.from);

        let Some(secret) = secret else {
            tracing::warn!(
                "Unable to load model {}: unable to get secret for source {}",
                model.name,
                source.to_string()
            );
            return UnableToLoadRequiredSecretsSnafu {}.fail();
        };

        let mut params = std::collections::HashMap::new();
        params.insert("name".to_string(), model.name.to_string());
        params.insert("path".to_string(), path(&model.from));
        params.insert("from".to_string(), path(&model.from));
        params.insert("files".to_string(), model.files.join(",").to_string());

        let tract = modelruntime::tract::Tract {
            path: Into::<Box<dyn ModelSource>>::into(source)
                .pull(secret, Arc::new(Option::from(params)))
                .await
                .context(UnableToLoadModelSnafu)?
                .clone()
                .to_string(),
        }
        .load()
        .context(UnableToInitModelSnafu {})?;

        Ok(Self {
            runnable: tract,
            model: model.clone(),
        })
    }

    pub fn run(&self, data: Vec<RecordBatch>) -> Result<RecordBatch> {
        let result = self.runnable.run(data).context(UnableToRunModelSnafu {})?;
        Ok(result)
    }
}
