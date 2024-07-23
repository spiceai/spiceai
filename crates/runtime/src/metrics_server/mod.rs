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

use metrics_exporter_prometheus::PrometheusHandle;
use snafu::prelude::*;
use std::fmt::Debug;
use std::net::{TcpListener, ToSocketAddrs};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to bind to address: {source}"))]
    UnableToBindServerToPort { source: std::io::Error },

    #[snafu(display("Unable to start HTTP server: {source}"))]
    UnableToStartHttpServer { source: std::io::Error },
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub(crate) async fn start<A>(
    bind_address: Option<A>,
    handle: Option<PrometheusHandle>,
) -> Result<()>
where
    A: ToSocketAddrs + Debug,
{
    let (bind_address, handle) = match (bind_address, handle) {
        (Some(bind_address), Some(handle)) => (bind_address, handle),
        _ => return Ok(futures::pending!()),
    };

    let listener = std::net::TcpListener::bind(bind_address)
        .and_then(|listener| {
            listener.set_nonblocking(true)?;
            Ok(listener)
        })
        .map_err(|e| BuildError::FailedToCreateHTTPListener(e.to_string()))?;
    let listener = TcpListener::from_std(listener).unwrap();

    let exporter = HttpListeningExporter {
        handle,
        allowed_addresses,
    };

    Ok(Box::pin(async move { exporter.serve(listener).await }))
}
