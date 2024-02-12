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

    #[snafu(display("Unable to load model from path"))]
    UnableToLoadModelFromPath { source: crate::modelsource::Error },

    #[snafu(display("Unable to init model"))]
    UnableToInitModel { source: crate::modelruntime::Error },

    #[snafu(display("Unable to query"))]
    UnableToQuery {
        source: datafusion::error::DataFusionError,
    },

    #[snafu(display("Unable to run model"))]
    UnableToRunModel { source: crate::modelruntime::Error },
}

impl Model {
    pub fn load(model: &spicepod::component::model::Model) -> Result<Self> {
        let source = model.source();
        let source = source.as_str();

        let mut params = std::collections::HashMap::new();
        params.insert("name".to_string(), model.name.to_string());
        params.insert("from".to_string(), model.from.to_string());

        match source {
            "localhost" => {
                let local = crate::modelsource::local::Local {};
                let path = local
                    .pull(Arc::new(Option::from(params)))
                    .context(UnableToLoadModelFromPathSnafu {})?;

                let path = path.clone();

                let tract = crate::modelruntime::tract::Tract {
                    path: path.to_string(),
                }
                .load()
                .context(UnableToInitModelSnafu {})?;

                Ok(Self {
                    runnable: tract,
                    datasets: model.datasets.clone(),
                })
            }
            _ => UnknownDataSourceSnafu {
                model_source: source,
            }
            .fail()?,
        }
    }

    pub async fn run(&self, df: Arc<DataFusion>, lookback_size: usize) -> Result<RecordBatch> {
        let sql = format!("select * from datafusion.public.{} order by ts asc", self.datasets[0]);

        let data = df
            .ctx
            .sql(&sql)
            .await
            .context(UnableToQuerySnafu {})?
            .collect()
            .await
            .context(UnableToQuerySnafu {})?;

        let result = self
            .runnable
            .run(data, lookback_size)
            .context(UnableToRunModelSnafu {})?;

        Ok(result)
    }
}
