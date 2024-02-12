use crate::modelruntime::ModelRuntime;
use crate::modelruntime::Runnable;
use crate::modelsource::ModelSource;
use crate::DataFusion;
use arrow::record_batch::RecordBatch;
use snafu::prelude::*;
use std::sync::Arc;

pub struct Model {
    runnable: Box<dyn Runnable>,
    datasets: Vec<String>,
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unknown data source: {model_source}"))]
    UnknownDataSource { model_source: String },

    #[snafu(display("Unable to load model"))]
    UnableToLoadModel { source: crate::modelruntime::Error },

    #[snafu(display("Unable to query"))]
    UnableToQuery {
        source: datafusion::error::DataFusionError,
    },
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
                    datasets: model.datasets.clone(),
                });
            }
            _ => UnknownDataSourceSnafu {
                model_source: source,
            }
            .fail()?,
        }
    }

    pub async fn run(&self, df: Arc<DataFusion>) -> Result<RecordBatch> {
        let sql = format!("select * from datafusion.public.{}", self.datasets[0]);

        let data = df
            .ctx
            .sql(&sql)
            .await
            .context(UnableToQuerySnafu {})?
            .collect()
            .await
            .context(UnableToQuerySnafu {})?;

        return Ok(self.runnable.run(data).unwrap());
    }
}
