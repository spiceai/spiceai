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

use crate::{
    modelformat::from_path as format_from_path,
    modelruntime::{supported_runtime_for_path, Error as ModelRuntimeError, Runnable},
    modelsource::{path, Error as ModelSourceError, ModelSource, ModelSourceType},
};
use arrow::record_batch::RecordBatch;
use secrecy::SecretString;
use snafu::prelude::*;
use std::{collections::HashMap, sync::Arc};

pub struct Model {
    runnable: Box<dyn Runnable>,
    pub model: spicepod::component::model::Model,
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("{source}"))]
    UnknownModelSource { source: ModelSourceError },

    #[snafu(display("{source}"))]
    UnableToLoadModel { source: ModelSourceError },

    #[snafu(display("{source}"))]
    UnableToInitModel { source: ModelRuntimeError },

    #[snafu(display("{source}"))]
    UnableToRunModel { source: ModelRuntimeError },

    #[snafu(display("Unable to load required secrets.\nReport a bug on GitHub: https://github.com/spiceai/spiceai/issues"))]
    UnableToLoadRequiredSecrets {},
}

impl Model {
    pub async fn load(
        model: spicepod::component::model::Model,
        mut params: HashMap<String, SecretString>,
    ) -> Result<Self> {
        let Ok(source) = model.from.parse::<ModelSourceType>() else {
            return Err(Error::UnknownModelSource {
                source: ModelSourceError::UnknownModelSource { from: model.from },
            });
        };

        params.insert(
            "name".to_string(),
            SecretString::from(model.name.to_string()),
        );
        params.insert("path".to_string(), SecretString::from(path(&model.from)));
        params.insert("from".to_string(), SecretString::from(path(&model.from)));
        params.insert(
            "files".to_string(),
            SecretString::from(model.get_all_file_paths().join(",").to_string()),
        );

        let model_source: Option<Box<dyn ModelSource>> = source.into();
        if let Some(model_source) = model_source {
            let path = model_source
                .pull(Arc::new(params))
                .await
                .context(UnableToLoadModelSnafu)?;

            match format_from_path(path.as_str()) {
                Some(format) => match supported_runtime_for_path(path.as_str()) {
                    Ok(runtime) => {
                        let runnable = runtime.load().context(UnableToInitModelSnafu {})?;
                        Ok(Self {
                            runnable,
                            model: model.clone(),
                        })
                    }
                    Err(_) => Err(Error::UnableToLoadModel {
                        source: ModelSourceError::UnsupportedModelFormat {
                            model_format: format,
                        },
                    }),
                },
                None => Err(Error::UnknownModelSource {
                    source: ModelSourceError::UnknownModelSource { from: model.from },
                }),
            }
        } else {
            Err(Error::UnknownModelSource {
                source: ModelSourceError::UnknownModelSource {
                    from: source.to_string(),
                },
            })
        }
    }

    pub fn run(&self, data: Vec<RecordBatch>) -> Result<RecordBatch> {
        let result = self.runnable.run(data).context(UnableToRunModelSnafu {})?;
        Ok(result)
    }
}
