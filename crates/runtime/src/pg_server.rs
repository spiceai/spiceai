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

use std::sync::Arc;

use datafusion::execution::context::SessionContext;
use pgwire::{api::{auth::noop::NoopStartupHandler, query::PlaceholderExtendedQueryHandler, StatelessMakeHandler}, tokio::process_socket};
use snafu::Snafu;

use pgwire::api::MakeHandler;

use tokio::{net::TcpListener, sync::RwLock};



use crate::datafusion::DataFusion;

mod handlers;
mod datatypes;

#[derive(Debug, Snafu)]
pub enum Error {
    // #[snafu(display("Unable to register parquet file: {source}"))]
    // RegisterParquet { source: crate::datafusion::Error },

    // #[snafu(display("{source}"))]
    // DataFusion {
    //     source: datafusion::error::DataFusionError,
    // },

    // #[snafu(display("Unable to start Flight server: {source}"))]
    // UnableToStartFlightServer { source: tonic::transport::Error },
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub async fn start(bind_address: std::net::SocketAddr, df: Arc<RwLock<DataFusion>>) -> Result<()> {

    let processor = Arc::new(StatelessMakeHandler::new(Arc::new(
        handlers::DfSessionService::new(df),
    )));
    let authenticator = Arc::new(StatelessMakeHandler::new(Arc::new(NoopStartupHandler)));

    let placeholder = Arc::new(StatelessMakeHandler::new(Arc::new(
        PlaceholderExtendedQueryHandler,
    )));

    let listener = TcpListener::bind(bind_address).await.unwrap();
    tracing::info!("Spice PostgreSQL compatible layer listening on {bind_address}");
    //metrics::counter!("spiced_runtime_pg_server_start").increment(1);
    loop {
        let incoming_socket = listener.accept().await.unwrap();
        let authenticator_ref = authenticator.make();
        let processor_ref = processor.make();
        tokio::spawn(async move {
            process_socket(
                incoming_socket.0,
                None,
                authenticator_ref,
                processor_ref.clone(),
                processor_ref,
            )
            .await
        });
    }

    Ok(())
}