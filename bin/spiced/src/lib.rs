#![allow(clippy::missing_errors_doc)]

use std::net::SocketAddr;
use std::sync::Arc;

use app::App;
use clap::Parser;
use runtime::config::Config as RuntimeConfig;
use runtime::datasource::DataSource;

use runtime::modelruntime::ModelRuntime;
use runtime::modelsource::ModelSource;
use runtime::{databackend, datasource, modelruntime, modelsource, Runtime};
use snafu::prelude::*;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to construct spice app"))]
    UnableToConstructSpiceApp { source: app::Error },

    #[snafu(display("Unable to start Spice Runtime servers"))]
    UnableToStartServers { source: runtime::Error },

    #[snafu(display("Unable to attach data source: {data_source}"))]
    UnableToAttachDataSource {
        source: runtime::datafusion::Error,
        data_source: String,
    },

    #[snafu(display("Unable to attach view"))]
    UnableToAttachView { source: runtime::datafusion::Error },

    #[snafu(display("Unable to initialize data source: {data_source}"))]
    UnableToInitializeDataSource {
        source: runtime::datasource::Error,
        data_source: String,
    },

    #[snafu(display(
        "A required parameter ({parameter}) is missing for data source: {data_source}",
    ))]
    RequiredParameterMissing {
        parameter: &'static str,
        data_source: String,
    },

    #[snafu(display("Unknown data source: {data_source}"))]
    UnknownDataSource { data_source: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Parser)]
#[clap(about = "Spice.ai OSS Runtime")]
pub struct Args {
    /// Enable Prometheus metrics. (disabled by default)
    #[arg(long, value_name = "BIND_ADDRESS", help_heading = "Metrics")]
    pub metrics: Option<SocketAddr>,

    /// Print the version and exit.
    #[arg(long)]
    pub version: bool,

    /// All runtime related arguments
    #[clap(flatten)]
    pub runtime: RuntimeConfig,
}

pub async fn run(args: Args) -> Result<()> {
    let app = App::new(".").context(UnableToConstructSpiceAppSnafu)?;

    let mut auth = runtime::auth::AuthProviders::default();
    match auth.parse_from_config() {
        Ok(()) => {}
        Err(e) => {
            tracing::warn!(
                "Unable to parse auth from config, proceeding without auth: {}",
                e
            );
        }
    }

    let mut df = runtime::datafusion::DataFusion::new();

    for model in &app.models {
        let source = model.source();
        let source = source.as_str();

        // create a params contains model name and model from

        let mut params = std::collections::HashMap::new();

        params.insert("name".to_string(), model.name.to_string());
        params.insert("from".to_string(), model.from.to_string());

        match source {
            "local" => {
                let local = modelsource::local::Local {};
                let path = local.pull(Arc::new(Option::from(params)));

                let path = path.unwrap().clone();

                let model = modelruntime::tract::Tract {
                    path: path.to_string(),
                }
                .load();
                match model {
                    Ok(m) => {
                        let result = m.run();
                        tracing::info!("Model loaded: {:?}", result);
                    }
                    Err(e) => {
                        tracing::error!("Error loading model: {:?}", e);
                    }
                }
            }
            _ => UnknownDataSourceSnafu {
                data_source: source,
            }
            .fail()?,
        }
    }

    for ds in &app.datasets {
        let source = ds.source();
        let source = source.as_str();
        let params = Arc::new(ds.params.clone());
        let data_source: Option<Box<dyn DataSource>> = match source {
            "spice.ai" => Some(Box::new(
                datasource::spiceai::SpiceAI::new(auth.get(source), params)
                    .await
                    .context(UnableToInitializeDataSourceSnafu {
                        data_source: source,
                    })?,
            )),
            "dremio" => Some(Box::new(
                datasource::dremio::Dremio::new(auth.get(source), params)
                    .await
                    .context(UnableToInitializeDataSourceSnafu {
                        data_source: source,
                    })?,
            )),
            "localhost" | "" => None,
            "debug" => Some(Box::new(datasource::debug::DebugSource {})),
            _ => UnknownDataSourceSnafu {
                data_source: source,
            }
            .fail()?,
        };

        match data_source {
            Some(data_source) => {
                let data_source = Box::leak(data_source);

                df.attach(ds, data_source, databackend::DataBackendType::default())
                    .context(UnableToAttachDataSourceSnafu {
                        data_source: source,
                    })?;
            }
            None => match &ds.sql {
                Some(_) => {
                    df.attach_view(ds).context(UnableToAttachViewSnafu)?;
                }
                None => {
                    df.attach_backend(&ds.name, databackend::DataBackendType::default())
                        .context(UnableToAttachDataSourceSnafu {
                            data_source: source,
                        })?;
                }
            },
        }

        tracing::info!("Loaded dataset: {}", ds.name);
    }

    let rt: Runtime = Runtime::new(args.runtime, app, df);

    rt.start_servers()
        .await
        .context(UnableToStartServersSnafu)?;

    Ok(())
}
