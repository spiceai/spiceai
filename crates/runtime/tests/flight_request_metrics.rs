/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! One Flight RPC must record exactly one `flight_requests` increment.
//!
//! Two shapes of miscount are invisible to the compiler and to lints, and both
//! shipped at once:
//!
//! - A timer in the `FlightService` impl *and* a timer in the handler it
//!   delegates to record two samples for the same RPC. The counter then reports
//!   twice the real request rate, and because the handler's timer spans the
//!   response stream while the outer one ends when the stream is merely
//!   constructed, `flight_request_duration_ms` pairs every real latency with a
//!   shorter prefix of itself and reported quantiles read low.
//! - `let _start = track_flight_request(..)` without `.await` binds the future,
//!   not the measurement. The future is dropped unpolled, so nothing is recorded
//!   at all — not even the counter. Binding to a named `_start` rather than `_`
//!   suppresses `unused_must_use`, and `clippy::let_underscore_future` only
//!   fires on the `let _` form.
//!
//! Asserting the delta per RPC is what separates one, two, and zero. A test that
//! only asserted the series exists passes under all three.
//!
//! Regression test for <https://github.com/spiceai/spiceai/issues/12844>.
//!
//! This lives in its own test binary because the meter install is global: the
//! instruments are `LazyLock`s over `global::meter("flight")`, so they bind to
//! whichever provider is installed when first touched, and a count assertion is
//! only stable when nothing else is recording on the same series.

use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::{Arc, LazyLock},
    time::Duration,
};

use arrow_flight::{FlightClient, FlightDescriptor, Ticket};
use futures::{StreamExt, TryStreamExt};
use opentelemetry::global;
use opentelemetry_sdk::{Resource, metrics::SdkMeterProvider};
use runtime::{Runtime, auth::EndpointAuth, config::Config};
use tonic::transport::Channel;

const LOCALHOST: IpAddr = IpAddr::V4(Ipv4Addr::LOCALHOST);

/// The one `MeterProvider` for this binary, installed before any instrument
/// exists. Every test forces it first, so the install wins the race however the
/// harness schedules them: `cargo nextest` gives each test its own process,
/// `cargo test` gives them threads, and `LazyLock` covers both.
static PROMETHEUS: LazyLock<prometheus::Registry> =
    LazyLock::new(install_prometheus_meter_provider);

/// Installs a `MeterProvider` backed by a scrapable Prometheus registry, as
/// `spiced` does for the `--metrics` endpoint.
fn install_prometheus_meter_provider() -> prometheus::Registry {
    let registry = prometheus::Registry::new();

    let exporter = opentelemetry_prometheus::exporter()
        .with_registry(registry.clone())
        .without_scope_info()
        .without_units()
        .without_counter_suffixes()
        .without_target_info()
        .build()
        .expect("to build the prometheus exporter");

    let provider = SdkMeterProvider::builder()
        .with_resource(Resource::builder().build())
        .with_reader(exporter)
        .build();
    global::set_meter_provider(provider);

    registry
}

/// Every `flight_requests` increment recorded so far, summed across label sets.
///
/// Summed rather than read per-series because the fix moves where a sample is
/// recorded, and with it which labels the sample carries: the handler that knows
/// the command adds a `command` label the `FlightService` impl cannot. A
/// per-series read would pass a duplicate off as a relabelling.
fn flight_requests_total(registry: &prometheus::Registry) -> f64 {
    registry
        .gather()
        .iter()
        .filter(|family| family.name() == "flight_requests")
        .flat_map(|family| {
            family
                .get_metric()
                .iter()
                .map(|metric| metric.get_counter().value())
        })
        .sum()
}

/// Asserts `rpc` recorded exactly one `flight_requests` increment.
///
/// `#[track_caller]` so a failure names the RPC rather than this line.
#[track_caller]
fn assert_recorded_once(registry: &prometheus::Registry, before: f64, rpc: &str) {
    let recorded = flight_requests_total(registry) - before;
    assert!(
        (recorded - 1.0).abs() < f64::EPSILON,
        "{rpc} must record exactly one flight_requests increment, recorded {recorded}"
    );
}

/// Starts a runtime with no datasets and returns a channel to its Flight server.
///
/// No dataset is needed: every RPC below either fails before planning or runs a
/// literal query, and a dataset load would only add series to the registry.
async fn start_flight_server() -> Result<Channel, anyhow::Error> {
    // A fixed port would collide with a concurrently running test binary.
    let flight_listener = std::net::TcpListener::bind(SocketAddr::new(LOCALHOST, 0))?;
    let flight_port = flight_listener.local_addr()?.port();
    let http_listener = std::net::TcpListener::bind(SocketAddr::new(LOCALHOST, 0))?;
    let http_port = http_listener.local_addr()?.port();
    drop(flight_listener);
    drop(http_listener);

    let app = app::AppBuilder::new("flight_request_metrics").build();
    let rt = Arc::new(Runtime::builder().with_app(app).build().await);
    Arc::clone(&rt).load_components().await;

    let api_config = Config::new()
        .with_http_bind_address(SocketAddr::new(LOCALHOST, http_port))
        .with_flight_bind_address(SocketAddr::new(LOCALHOST, flight_port));

    let serving = Arc::clone(&rt);
    tokio::spawn(async move {
        Box::pin(serving.start_servers(api_config, None, EndpointAuth::default())).await
    });

    // Poll the listener rather than sleeping: the server binds asynchronously and
    // a connect refused before it does is not a failure.
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        match Channel::from_shared(format!("http://127.0.0.1:{flight_port}"))?
            .connect()
            .await
        {
            Ok(channel) => return Ok(channel),
            Err(e) if std::time::Instant::now() >= deadline => {
                return Err(anyhow::anyhow!(
                    "Flight server did not accept a connection on port {flight_port} within 30s: {e}"
                ));
            }
            Err(_) => tokio::time::sleep(Duration::from_millis(50)).await,
        }
    }
}

#[tokio::test]
async fn each_flight_rpc_records_exactly_one_request() -> Result<(), anyhow::Error> {
    let registry = &*PROMETHEUS;
    let channel = start_flight_server().await?;
    let mut client = FlightClient::new(channel);

    // `handshake` was recorded twice under two *different* method names —
    // `do_handshake` from the service impl and `handshake` from the handler — so
    // neither series carried the real rate on its own.
    let before = flight_requests_total(registry);
    client
        .handshake("flight_request_metrics")
        .await
        .map_err(|e| anyhow::anyhow!("handshake: {e}"))?;
    assert_recorded_once(registry, before, "handshake");

    // A ticket that is not a FlightSQL command is served by `do_get_simple`. The
    // stream is drained because the handler's measurement spans the drain; the
    // counter increments before it, so this only pins the ordering the histogram
    // depends on.
    let before = flight_requests_total(registry);
    let stream = client
        .do_get(Ticket::new("SELECT 1"))
        .await
        .map_err(|e| anyhow::anyhow!("do_get: {e}"))?;
    let batches: Vec<_> = stream
        .try_collect()
        .await
        .map_err(|e| anyhow::anyhow!("do_get stream: {e}"))?;
    assert_eq!(batches.len(), 1, "SELECT 1 returns a single batch");
    assert_recorded_once(registry, before, "do_get");

    let before = flight_requests_total(registry);
    client
        .get_schema(FlightDescriptor::new_cmd("SELECT 1"))
        .await
        .map_err(|e| anyhow::anyhow!("get_schema: {e}"))?;
    assert_recorded_once(registry, before, "get_schema");

    let before = flight_requests_total(registry);
    let actions: Vec<_> = client
        .list_actions()
        .await
        .map_err(|e| anyhow::anyhow!("list_actions: {e}"))?
        .try_collect()
        .await
        .map_err(|e| anyhow::anyhow!("list_actions stream: {e}"))?;
    assert!(
        !actions.is_empty(),
        "list_actions advertises the FlightSQL actions"
    );
    assert_recorded_once(registry, before, "list_actions");

    // The rejection paths in `do_put::handle` are the ones that recorded nothing:
    // each bound the tracking future to `_start` and never awaited it, so the
    // service impl's timer was their only coverage. With that timer gone, a
    // re-dropped future reads as zero here, not as a duplicate.
    let before = flight_requests_total(registry);
    let empty = futures::stream::empty::<arrow_flight::error::Result<arrow_flight::FlightData>>();
    let put = client.do_put(empty).await;
    let refused = match put {
        Err(e) => e.to_string(),
        Ok(mut stream) => match stream.next().await {
            Some(Err(e)) => e.to_string(),
            other => {
                return Err(anyhow::anyhow!(
                    "do_put with no flight data must be refused, got {other:?}"
                ));
            }
        },
    };
    assert!(
        refused.contains("No flight data provided"),
        "do_put with no flight data must be refused for the reason under test, got: {refused}"
    );
    assert_recorded_once(registry, before, "do_put with no flight data");

    Ok(())
}
