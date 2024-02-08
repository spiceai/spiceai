use crate::modelruntime::ModelRuntime;
use crate::modelruntime::Runnable;
use crate::modelsource::ModelSource;
use crate::DataFusion;
use arrow::record_batch::RecordBatch;
use snafu::prelude::*;
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

    pub async fn run(&self, df: Arc<DataFusion>) -> RecordBatch {
        let sql = "select number as ts, (number::double / 100) as y, (number::double) / 100 as y2 from datafusion.public.eth_blocks limit 100";

        let data = df.ctx.sql(sql).await.unwrap().collect().await.unwrap();

        tracing::info!("{:?}", data);

        return self.runnable.run(data).unwrap();
    }
}
