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

use std::{collections::HashMap, fmt::Debug, net::SocketAddr, sync::Arc};

use app::App;
use model_components::model::Model;
use snafu::prelude::*;
use tokio::{
    net::{TcpListener, ToSocketAddrs},
    sync::RwLock,
};

use crate::{
    config,
    datafusion::DataFusion,
    embeddings::vector_search::{self, compute_primary_keys},
    model::LLMModelStore,
    EmbeddingModelStore,
};

mod routes;
mod v1;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to bind to address: {source}"))]
    UnableToBindServerToPort { source: std::io::Error },

    #[snafu(display("Unable to start HTTP server: {source}"))]
    UnableToStartHttpServer { source: std::io::Error },
}

type Result<T, E = Error> = std::result::Result<T, E>;

#[allow(clippy::too_many_arguments)]
pub(crate) async fn start<A>(
    bind_address: A,
    app: Arc<RwLock<Option<App>>>,
    df: Arc<DataFusion>,
    models: Arc<RwLock<HashMap<String, Model>>>,
    llms: Arc<RwLock<LLMModelStore>>,
    embeddings: Arc<RwLock<EmbeddingModelStore>>,
    config: Arc<config::Config>,
    with_metrics: Option<SocketAddr>,
) -> Result<()>
where
    A: ToSocketAddrs + Debug,
{
    let vsearch = Arc::new(vector_search::VectorSearch::new(
        Arc::clone(&df),
        Arc::clone(&embeddings),
        compute_primary_keys(Arc::clone(&app)).await,
    ));
    let routes = routes::routes(
        app,
        df,
        models,
        llms,
        embeddings,
        config,
        with_metrics,
        vsearch,
    );

    let listener = TcpListener::bind(&bind_address)
        .await
        .context(UnableToBindServerToPortSnafu)?;
    tracing::info!("Spice Runtime HTTP listening on {bind_address:?}");

    metrics::counter!("spiced_runtime_http_server_start").increment(1);

    axum::serve(listener, routes)
        .await
        .context(UnableToStartHttpServerSnafu)?;
    Ok(())
}
