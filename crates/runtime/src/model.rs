use crate::secretstore::AuthProvider;
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
    model: spicepod::component::model::Model,
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
    pub async fn load(
        model: &spicepod::component::model::Model,
        auth: AuthProvider,
    ) -> Result<Self> {
        let source = source(&model.from);
        let source = source.as_str();

        let mut params = std::collections::HashMap::new();
        params.insert("name".to_string(), model.name.to_string());
        params.insert("path".to_string(), path(&model.from));

        let tract = crate::modelruntime::tract::Tract {
            path: create_source_from(source)
                .context(UnknownModelSourceSnafu)?
                .pull(auth, Arc::new(Option::from(params)))
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
                    self.model.datasets[0]
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

#[must_use]
pub(crate) fn source(from: &str) -> String {
    match from {
        s if s.starts_with("spiceai:") => "spiceai".to_string(),
        s if s.starts_with("file:/") => "localhost".to_string(),
        _ => "spiceai".to_string(),
    }
}

#[must_use]
pub(crate) fn path(from: &str) -> String {
    let sources = vec!["spiceai:"];

    for source in &sources {
        if from.starts_with(source) {
            match from.find(':') {
                Some(index) => return from[index + 1..].to_string(),
                None => return from.to_string(),
            }
        }
    }

    from.to_string()
}

#[must_use]
pub fn version(from: &str) -> String {
    let path = path(from);
    path.split(':').last().unwrap_or("").to_string()
}
