use crate::auth::AuthProvider;
use crate::modelruntime::ModelRuntime;
use crate::modelruntime::Runnable;
use crate::modelsource::create_source_from;
use crate::DataFusion;
use arrow::record_batch::RecordBatch;
use snafu::prelude::*;
use std::sync::Arc;
use tokio::sync::RwLock;

pub struct Model {
    runnable: Box<dyn Runnable>,
    datasets: Vec<String>,
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unknown model source: {source}"))]
    UnknownModelSource { source: crate::modelsource::Error },

    #[snafu(display("Unable to load model from path: {source}"))]
    UnableToLoadModel { source: crate::modelsource::Error },

    #[snafu(display("Unable to init model: {source}"))]
    UnableToInitModel { source: crate::modelruntime::Error },

    #[snafu(display("Unable to query: {source}"))]
    UnableToQuery {
        source: datafusion::error::DataFusionError,
    },

    #[snafu(display("Unable to run model: {source}"))]
    UnableToRunModel { source: crate::modelruntime::Error },
}

impl Model {
    pub fn load(model: &spicepod::component::model::Model, auth: AuthProvider) -> Result<Self> {
        let source = model.source();
        let source = source.as_str();

        let mut params = std::collections::HashMap::new();
        params.insert("name".to_string(), model.name.to_string());
        params.insert("from".to_string(), model.from.to_string());

        let tract = crate::modelruntime::tract::Tract {
            path: create_source_from(source)
                .context(UnknownModelSourceSnafu)?
                .pull(auth, Arc::new(Option::from(params)))
                .context(UnableToLoadModelSnafu)?
                .clone()
                .to_string(),
        }
        .load()
        .context(UnableToInitModelSnafu {})?;

        Ok(Self {
            runnable: tract,
            datasets: model.datasets.clone(),
        })
    }

    pub async fn run(
        &self,
        df: Arc<RwLock<DataFusion>>,
        lookback_size: usize,
    ) -> Result<RecordBatch> {
        let data = df
            .read()
            .await
            .ctx
            .sql(
                &(format!(
                    "select * from datafusion.public.{} order by ts asc",
                    self.datasets[0]
                )),
            )
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
