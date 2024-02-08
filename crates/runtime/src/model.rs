use crate::modelruntime::ModelRuntime;
use crate::DataFusion;
use crate::modelruntime::Runnable;
use crate::modelsource::ModelSource;
use arrow::datatypes::{Field,Schema,DataType};
use arrow::record_batch::RecordBatch;
use snafu::prelude::*;
use arrow::array::Float32Array;
use std::sync::Arc;

pub struct Model {
    runnable: Box<dyn Runnable>,
    spicepod_model: spicepod::component::model::Model,
    path: String,
    inference_template: String,
    datasets: Vec<String>,
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unknown data source: {model_source}"))]
    UnknownDataSource { model_source: String },

    #[snafu(display("Unable to load model"))]
    UnableToLoadModel { source: crate::modelruntime::Error },
}

impl Model {
    pub fn load(model: &spicepod::component::model::Model) -> Result<Self> {
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

                let tract = crate::modelruntime::tract::Tract {
                    path: path.to_string(),
                }
                .load()
                .context(UnableToLoadModelSnafu {})?;

                return Ok(Self {
                    runnable: tract,
                    spicepod_model: model.clone(),
                    path,
                    inference_template: "select * from {{dataset}}".to_string(),
                    datasets: model.datasets.clone(),
                });
            }
            _ => UnknownDataSourceSnafu {
                model_source: source,
            }
            .fail()?,
        }
    }

    pub fn run(&self, _: Arc<DataFusion>) -> RecordBatch {
        let result = self.runnable.run();

        tracing::info!("result: {:?}", result);
        let id_array = Float32Array::from(vec![1.0, 2.0, 3.0, 4.0, 5.0]);
        let schema = Schema::new(vec![
                Field::new("result", DataType::Float32, false),]);
        return RecordBatch::try_new(Arc::new(schema), vec![Arc::new(id_array)]).unwrap();
    }
}
