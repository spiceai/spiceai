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

//! One Spicepod, three clients: the micro queries run against a runtime built from a
//! Spicepod, in-process, over HTTP `/v1/sql` and over Flight SQL, and every answer is
//! compared with standalone `DuckDB` and standalone `SQLite` loaded with the same rows.
//!
//! Unlike the micro tests beside this module, the SQL is planned by the runtime, so
//! filters, aggregates, joins and limits reach the accelerator through the accelerated
//! table, and the answer crosses the HTTP JSON and Flight encoders on its way back. The
//! source files are deleted once the acceleration is loaded, so a query that read the
//! source instead of the acceleration fails rather than passing on the source's rows.

use std::fmt::Write as _;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use arrow::array::RecordBatch;
use arrow::datatypes::Schema;
use arrow_flight::sql::client::FlightSqlServiceClient;
use futures::TryStreamExt;
use runtime::{Runtime, auth::EndpointAuth, config::Config};
use tonic::transport::Channel;

use super::{
    compare, duckdb_accel, make_dim, make_fact, micro_queries, sqlite_accel, write_parquet,
};
use crate::configure_test_datafusion;
use crate::utils::{
    register_test_connectors, runtime_ready_check, test_request_context, wait_until_true,
};

const LOCALHOST: IpAddr = IpAddr::V4(Ipv4Addr::LOCALHOST);
const FACT_ROWS: usize = 2_048;
const DIM_ROWS: usize = 256;

/// A Spicepod accelerating `t` and `d` from parquet files in `dir` with `engine`.
fn spicepod(engine: &str, dir: &std::path::Path) -> String {
    let mut datasets = String::new();
    for name in ["t", "d"] {
        // Cayenne runs in `mode: file`, as the runtime's other Cayenne refresh tests do, and
        // one process keeps one Cayenne metastore, so both datasets share its directory.
        // Every other engine runs in memory.
        let storage = if engine == "cayenne" {
            format!(
                "      mode: file\n      params:\n        cayenne_file_path: {data}\n        cayenne_metadata_dir: {meta}\n",
                data = dir.join(format!("cayenne_{name}_data")).display(),
                meta = dir.join("cayenne_metadata").display(),
            )
        } else {
            "      mode: memory\n".to_string()
        };
        write!(
            datasets,
            "  - from: file://{path}\n    name: {name}\n    acceleration:\n      enabled: true\n      engine: {engine}\n      refresh_mode: full\n{storage}",
            path = dir.join(format!("{name}.parquet")).display(),
        )
        .expect("writing to a String cannot fail");
    }
    format!("version: v1\nkind: Spicepod\nname: surfaces_{engine}\ndatasets:\n{datasets}")
}

fn free_port() -> u16 {
    std::net::TcpListener::bind(SocketAddr::new(LOCALHOST, 0))
        .and_then(|listener| listener.local_addr())
        .expect("an ephemeral port must be available")
        .port()
}

async fn in_process(rt: &Runtime, sql: &str) -> Result<Vec<RecordBatch>, String> {
    let result = rt
        .datafusion()
        .query_builder(sql)
        .build()
        .run()
        .await
        .map_err(|e| e.to_string())?;
    result.data.try_collect().await.map_err(|e| e.to_string())
}

/// Decodes `/v1/sql`'s `application/vnd.spiceai.sql.v1+json` answer with the Arrow schema
/// the response carries, so the comparison sees the types the server reported.
async fn over_http(
    client: &reqwest::Client,
    port: u16,
    sql: &str,
) -> Result<Vec<RecordBatch>, String> {
    let response = client
        .post(format!("http://{LOCALHOST}:{port}/v1/sql"))
        .header("Accept", "application/vnd.spiceai.sql.v1+json")
        .header("Content-Type", "text/plain")
        .body(sql.to_string())
        .send()
        .await
        .map_err(|e| e.to_string())?;
    let status = response.status();
    let body = response.text().await.map_err(|e| e.to_string())?;
    if !status.is_success() {
        return Err(format!("HTTP {status}: {body}"));
    }
    let body: serde_json::Value = serde_json::from_str(&body).map_err(|e| e.to_string())?;
    let rows = body["data"]
        .as_array()
        .ok_or_else(|| format!("no `data` array in {body}"))?;
    let row_count = body["row_count"].as_u64();
    if row_count != Some(rows.len() as u64) {
        return Err(format!(
            "`row_count` {row_count:?} disagrees with the {} rows in `data`",
            rows.len()
        ));
    }
    if rows.is_empty() {
        return Ok(vec![]);
    }
    let schema: Schema =
        serde_json::from_value(body["schema"].clone()).map_err(|e| e.to_string())?;
    let mut decoder = arrow_json::ReaderBuilder::new(Arc::new(schema))
        .build_decoder()
        .map_err(|e| e.to_string())?;
    decoder.serialize(rows).map_err(|e| e.to_string())?;
    Ok(decoder
        .flush()
        .map_err(|e| e.to_string())?
        .into_iter()
        .collect())
}

async fn over_flight_sql(channel: &Channel, sql: &str) -> Result<Vec<RecordBatch>, String> {
    let mut client = FlightSqlServiceClient::new(channel.clone());
    let info = client
        .execute(sql.to_string(), None)
        .await
        .map_err(|e| e.to_string())?;
    let mut batches = Vec::new();
    for endpoint in info.endpoint {
        let ticket = endpoint
            .ticket
            .ok_or_else(|| "a Flight SQL endpoint carried no ticket".to_string())?;
        let stream = client.do_get(ticket).await.map_err(|e| e.to_string())?;
        let mut part: Vec<RecordBatch> = stream.try_collect().await.map_err(|e| e.to_string())?;
        batches.append(&mut part);
    }
    Ok(batches)
}

fn row_count(batches: &[RecordBatch]) -> usize {
    batches.iter().map(RecordBatch::num_rows).sum()
}

/// Loads `engine` from a Spicepod, deletes the source files, starts the HTTP and Flight
/// servers, and compares every micro query on every client with both standalone oracles.
async fn surfaces_match_standalone_oracles(engine: &str) {
    let fact = make_fact(FACT_ROWS, 64);
    let dim = make_dim(DIM_ROWS);

    let dir = tempfile::tempdir().expect("temp dir");
    let t_path = dir.path().join("t.parquet");
    let d_path = dir.path().join("d.parquet");
    write_parquet(&t_path, &fact);
    write_parquet(&d_path, &dim);
    std::fs::write(
        dir.path().join("spicepod.yaml"),
        spicepod(engine, dir.path()),
    )
    .expect("the spicepod must be written");

    let (_sqlite_dir, sqlite) =
        sqlite_accel::load_standalone_sqlite(&[("t", fact.clone()), ("d", dim.clone())]);
    let (_duckdb_dir, duckdb) = duckdb_accel::load_standalone_duckdb(&[("t", fact), ("d", dim)]);

    register_test_connectors().await;
    configure_test_datafusion();
    let app = app::AppBuilder::build_from_path(dir.path().to_path_buf())
        .await
        .expect("the spicepod must load");
    let rt = Arc::new(Runtime::builder().with_app(app).build().await);
    tokio::time::timeout(Duration::from_mins(2), Arc::clone(&rt).load_components())
        .await
        .expect("components must load");
    runtime_ready_check(&rt).await;

    // From here on only the acceleration holds the rows.
    std::fs::remove_file(&t_path).expect("remove t.parquet");
    std::fs::remove_file(&d_path).expect("remove d.parquet");

    let http_port = free_port();
    let flight_port = free_port();
    let config = Config::new()
        .with_http_bind_address(SocketAddr::new(LOCALHOST, http_port))
        .with_flight_bind_address(SocketAddr::new(LOCALHOST, flight_port));
    let server_rt = Arc::clone(&rt);
    tokio::spawn(async move {
        Box::pin(server_rt.start_servers(config, None, EndpointAuth::default())).await
    });

    let http = reqwest::Client::new();
    assert!(
        wait_until_true(Duration::from_secs(30), || async {
            http.get(format!("http://{LOCALHOST}:{http_port}/health"))
                .send()
                .await
                .is_ok()
        })
        .await,
        "the HTTP server on port {http_port} did not start within 30s"
    );
    // HTTP readiness does not mean the Flight server is listening; connect until it is.
    let endpoint = Channel::from_shared(format!("http://{LOCALHOST}:{flight_port}"))
        .expect("a valid Flight URI");
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    let flight = loop {
        match endpoint.connect().await {
            Ok(channel) => break channel,
            Err(e) => {
                assert!(
                    std::time::Instant::now() < deadline,
                    "the Flight server on port {flight_port} did not start within 30s: {e}"
                );
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    };

    let mut fails = Vec::new();
    let mut compared = 0usize;
    for q in micro_queries() {
        let oracles = [
            (
                "standalone-duckdb",
                duckdb_accel::standalone_query(&duckdb, &q.sql)
                    .unwrap_or_else(|e| panic!("duckdb sql {}: {e}", q.name)),
            ),
            (
                "standalone-sqlite",
                sqlite_accel::standalone_query(&sqlite, &q.sql)
                    .unwrap_or_else(|e| panic!("sqlite sql {}: {e}", q.name)),
            ),
        ];
        let surfaces = [
            ("in-process", in_process(&rt, &q.sql).await),
            ("http", over_http(&http, http_port, &q.sql).await),
            ("flight-sql", over_flight_sql(&flight, &q.sql).await),
        ];
        for (surface, spice) in &surfaces {
            let spice = match spice {
                Ok(batches) => batches,
                Err(e) => {
                    fails.push(format!("{engine}/{surface}/{}: query failed: {e}", q.name));
                    continue;
                }
            };
            for (oracle, expected) in &oracles {
                // Every micro query returns rows, so an empty answer on either side is a
                // load or a fallback that did not happen, not an agreement.
                if row_count(spice) == 0 || row_count(expected) == 0 {
                    fails.push(format!(
                        "{engine}/{surface} vs {oracle}/{}: empty answer (spice {} rows, oracle {} rows)",
                        q.name,
                        row_count(spice),
                        row_count(expected)
                    ));
                    continue;
                }
                let status = compare(&q, spice, expected);
                eprintln!("{engine}/{surface} vs {oracle} / {} -> {status}", q.name);
                compared += 1;
                if !status.starts_with("PASS") {
                    fails.push(format!(
                        "{engine}/{surface} vs {oracle}/{}: {status}",
                        q.name
                    ));
                }
            }
        }
    }
    assert!(
        fails.is_empty(),
        "Spice {engine} acceleration vs standalone oracles, {compared} cells compared, failures: {fails:#?}"
    );
    assert_eq!(
        compared,
        micro_queries().len() * 3 * 2,
        "every query must be compared on three clients against two oracles"
    );
}

async fn run(engine: &'static str) {
    test_request_context()
        .scope(surfaces_match_standalone_oracles(engine))
        .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn arrow_surfaces_vs_standalone_engines() {
    run("arrow").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn duckdb_surfaces_vs_standalone_engines() {
    run("duckdb").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn sqlite_surfaces_vs_standalone_engines() {
    run("sqlite").await;
}

#[cfg(feature = "turso")]
#[tokio::test(flavor = "multi_thread")]
async fn turso_surfaces_vs_standalone_engines() {
    run("turso").await;
}

#[cfg(not(windows))]
#[tokio::test(flavor = "multi_thread")]
async fn cayenne_surfaces_vs_standalone_engines() {
    run("cayenne").await;
}
