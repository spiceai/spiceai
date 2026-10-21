/*
Copyright 2026 The Spice.ai OSS Authors

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

//! Starts a Flight SQL server backed by an in-memory `DataFusion`
//! [`SessionContext`] and blocks until Ctrl-C.
//!
//! ```text
//! cargo run -p datafusion-flightsql --example serve_and_query
//! ```
//!
//! Once running, connect with any Arrow Flight SQL client.  For example,
//! using the Python `flightsql-dbapi` library:
//!
//! ```python
//! from flightsql import FlightSQLClient
//! client = FlightSQLClient(host="localhost", port=50051, insecure=True)
//! reader = client.execute("SELECT * FROM employees ORDER BY salary DESC")
//! print(reader.read_all().to_pandas())
//! ```
//!
//! Or with the `arrow-flight` Rust client:
//!
//! ```rust,no_run
//! use arrow_flight::sql::client::FlightSqlServiceClient;
//! use futures::TryStreamExt;
//! use tonic::transport::Channel;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let channel = Channel::from_static("http://localhost:50051").connect().await?;
//!     let mut client = FlightSqlServiceClient::new(channel);
//!     client.handshake("", "").await?;
//!
//!     let mut stmt = client.prepare("SELECT * FROM employees".to_string(), None).await?;
//!     let info = stmt.execute().await?;
//!     for endpoint in info.endpoint {
//!         if let Some(ticket) = endpoint.ticket {
//!             let batches: Vec<_> = client.do_get(ticket).await?.try_collect().await?;
//!             println!("{}", arrow::util::pretty::pretty_format_batches(&batches)?);
//!         }
//!     }
//!     Ok(())
//! }
//! ```

use std::sync::Arc;

use arrow::array::{Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::prelude::SessionContext;
use datafusion_flightsql::FlightSqlService;
use tonic::transport::Server;

const ADDR: &str = "0.0.0.0:50051";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter("datafusion_flightsql=debug,info")
        .init();

    // ── Build the SessionContext ──────────────────────────────────────────────

    let ctx = Arc::new(SessionContext::new());

    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("department", DataType::Utf8, false),
            Field::new("salary", DataType::Int32, false),
        ])),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
            Arc::new(StringArray::from(vec![
                "Alice", "Bob", "Carol", "Dave", "Eve",
            ])),
            Arc::new(StringArray::from(vec![
                "Engineering",
                "Marketing",
                "Engineering",
                "HR",
                "Engineering",
            ])),
            Arc::new(Int32Array::from(vec![
                120_000, 85_000, 110_000, 75_000, 130_000,
            ])),
        ],
    )?;

    ctx.register_batch("employees", batch)?;
    tracing::info!("Registered 'employees' table");

    // You can register additional tables, views, or UDFs on `ctx` here before
    // the server starts.  Everything registered on the context is immediately
    // queryable by connected clients.

    // ── Start the Flight SQL server ───────────────────────────────────────────

    let addr = ADDR.parse()?;
    tracing::info!("Flight SQL server listening on {addr}  (press Ctrl-C to stop)");

    Server::builder()
        .add_service(FlightSqlService::new(ctx).into_server())
        .serve_with_shutdown(addr, shutdown_signal())
        .await?;

    tracing::info!("Server stopped");
    Ok(())
}

async fn shutdown_signal() {
    tokio::signal::ctrl_c()
        .await
        .expect("failed to listen for Ctrl-C");
    tracing::info!("Received Ctrl-C, shutting down");
}
