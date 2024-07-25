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

use bytes::Bytes;
use http::{HeaderValue, Request, Response};
use http_body_util::Full;
use hyper::{
    body::{self, Incoming},
    header::CONTENT_TYPE,
    server::conn::http1::Builder,
};
use hyper_util::rt::TokioIo;
use metrics_exporter_prometheus::PrometheusHandle;
use snafu::prelude::*;
use std::fmt::Debug;
use std::net::ToSocketAddrs;
use tokio::net::{TcpListener, TcpStream};

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
    A: ToSocketAddrs + Debug + Clone + Copy,
{
    let (Some(bind_address), Some(handle)) = (bind_address, handle) else {
        return Ok(());
    };

    let listener = std::net::TcpListener::bind(bind_address)
        .and_then(|listener| {
            listener.set_nonblocking(true)?;
            Ok(listener)
        })
        .context(UnableToBindServerToPortSnafu)?;
    let listener = TcpListener::from_std(listener).context(UnableToBindServerToPortSnafu)?;
    tracing::info!("Spice Runtime Metrics listening on {:?}", bind_address);

    loop {
        let stream = match listener.accept().await {
            Ok((stream, _)) => stream,
            Err(e) => {
                tracing::warn!(
                    "Error accepting connection to serve Prometheus metrics request: {e}"
                );
                continue;
            }
        };

        process_tcp_stream(stream, handle.clone());
    }
}

fn process_tcp_stream(stream: TcpStream, handle: PrometheusHandle) {
    let service = hyper::service::service_fn(move |req: Request<body::Incoming>| {
        let handle = handle.clone();
        async move { Ok::<_, hyper::Error>(handle_http_request(&handle, &req)) }
    });

    tokio::spawn(async move {
        if let Err(err) = Builder::new()
            .serve_connection(TokioIo::new(stream), service)
            .await
        {
            tracing::warn!(error = ?err, "Error serving Prometheus metrics connection.");
        }
    });
}

fn handle_http_request(
    handle: &PrometheusHandle,
    req: &Request<Incoming>,
) -> Response<Full<Bytes>> {
    let mut response = Response::new(match req.uri().path() {
        "/health" => "OK".into(),
        _ => handle.render().into(),
    });
    response
        .headers_mut()
        .append(CONTENT_TYPE, HeaderValue::from_static("text/plain"));
    response
}
