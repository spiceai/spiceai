use crate::modelruntime::ModelRuntime;
use crate::modelruntime::Runnable;
use crate::modelsource::ModelSource;
use snafu::prelude::*;
use std::sync::Arc;

pub struct Model {}

pub type Result<T, E = Error> = std::result::Result<T, E>;
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unknown data source: {model_source}"))]
    UnknownDataSource { model_source: String },

    #[snafu(display("Unable to load model"))]
    UnableToLoadModel { source: crate::modelruntime::Error },
}

impl Model {
    pub fn load(model: &spicepod::component::model::Model) -> Result<Box<dyn Runnable>> {
        let source = model.source();
        let source = source.as_str();

        let mut params = std::collections::HashMap::new();
        params.insert("name".to_string(), model.name.to_string());
        params.insert("from".to_string(), model.from.to_string());

        match source {
            "local" => {
                let local = crate::modelsource::local::Local {};
                let path = local.pull(Arc::new(Option::from(params)));

                let path = path.unwrap().clone();

                return crate::modelruntime::tract::Tract {
                    path: path.to_string(),
                }
                .load()
                .context(UnableToLoadModelSnafu {});
            }
            _ => UnknownDataSourceSnafu {
                model_source: source,
            }
            .fail()?,
        }
    }
}
