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

//! The ways a user sends SQL to a runtime: in-process, HTTP `/v1/sql`, and
//! Flight SQL. A test starts a runtime's servers with [`SqlClients::start`] and
//! sends the same SQL each [`Surface`] way, so an answer that a wire encoder
//! changes shows up as rows that differ from the in-process ones.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use arrow::array::RecordBatch;
use arrow::datatypes::Schema;
use arrow_flight::sql::client::FlightSqlServiceClient;
use futures::TryStreamExt;
use runtime::{Runtime, auth::EndpointAuth, config::Config};
use tonic::transport::Channel;

use super::{free_ports, run_query, wait_until_true};

const LOCALHOST: IpAddr = IpAddr::V4(Ipv4Addr::LOCALHOST);

/// A way a user sends SQL to the runtime.
#[derive(Clone, Copy)]
pub(crate) enum Surface {
    InProcess,
    Http,
    FlightSql,
}

pub(crate) const SURFACES: [Surface; 3] = [Surface::InProcess, Surface::Http, Surface::FlightSql];

impl Surface {
    pub(crate) fn label(self) -> &'static str {
        match self {
            Surface::InProcess => "in-process",
            Surface::Http => "http",
            Surface::FlightSql => "flight-sql",
        }
    }
}

/// One runtime, with an HTTP client and a Flight SQL channel connected to its
/// servers.
pub(crate) struct SqlClients {
    rt: Arc<Runtime>,
    http: reqwest::Client,
    http_port: u16,
    flight: Channel,
}

impl SqlClients {
    /// Starts `rt`'s HTTP and Flight servers on free local ports, and waits
    /// until both accept connections.
    pub(crate) async fn start(rt: &Arc<Runtime>) -> Result<Self, String> {
        let [http_port, flight_port] = free_ports();
        let config = Config::new()
            .with_http_bind_address(SocketAddr::new(LOCALHOST, http_port))
            .with_flight_bind_address(SocketAddr::new(LOCALHOST, flight_port));
        let server_rt = Arc::clone(rt);
        tokio::spawn(async move {
            Box::pin(server_rt.start_servers(config, None, EndpointAuth::default())).await
        });

        let http = reqwest::Client::new();
        let endpoint = Channel::from_shared(format!("http://{LOCALHOST}:{flight_port}"))
            .map_err(|e| e.to_string())?;
        // HTTP readiness does not mean the Flight server is listening, so wait for both.
        let ready = wait_until_true(Duration::from_secs(30), || async {
            http.get(format!("http://{LOCALHOST}:{http_port}/health"))
                .send()
                .await
                .is_ok()
                && endpoint.connect().await.is_ok()
        })
        .await;
        if !ready {
            return Err(format!(
                "the HTTP server on port {http_port} and the Flight server on port {flight_port} did not both start within 30s"
            ));
        }
        let flight = endpoint.connect().await.map_err(|e| e.to_string())?;
        Ok(Self {
            rt: Arc::clone(rt),
            http,
            http_port,
            flight,
        })
    }

    /// The runtime's answer to `sql` sent the `surface` way.
    pub(crate) async fn query(
        &self,
        surface: Surface,
        sql: &str,
    ) -> Result<Vec<RecordBatch>, String> {
        match surface {
            Surface::InProcess => run_query(&self.rt, sql).await.map_err(|e| e.to_string()),
            Surface::Http => self.over_http(sql).await,
            Surface::FlightSql => self.over_flight_sql(sql).await,
        }
    }

    /// `sql` over HTTP `/v1/sql`, its `application/vnd.spiceai.sql.v1+json`
    /// answer decoded with the Arrow schema the response carries, so a
    /// comparison sees the types the server reported.
    async fn over_http(&self, sql: &str) -> Result<Vec<RecordBatch>, String> {
        let response = self
            .http
            .post(format!("http://{LOCALHOST}:{}/v1/sql", self.http_port))
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

    /// `sql` over Flight SQL: `CommandStatementQuery`, then a `DoGet` for each
    /// endpoint it returns.
    async fn over_flight_sql(&self, sql: &str) -> Result<Vec<RecordBatch>, String> {
        let mut client = FlightSqlServiceClient::new(self.flight.clone());
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
            let mut part: Vec<RecordBatch> =
                stream.try_collect().await.map_err(|e| e.to_string())?;
            batches.append(&mut part);
        }
        Ok(batches)
    }
}
