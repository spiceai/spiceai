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

//! An executor's Flight server takes writes only from the scheduler, which it
//! recognizes by the certificate the scheduler presents on cluster mTLS. A
//! `DoPut` and a Flight SQL `CommandStatementUpdate` from any other cluster node
//! are refused and leave the table as it was, and SQL from another node that
//! calls `executor_table()` is refused while it is planned, before the executor
//! connects to the endpoint the call names.
//!
//! Every request goes through the executor's real Flight server over TLS with a
//! certificate from the harness PKI, so the role is decided from the identity
//! the connection presented and the refusal comes from the handler.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use arrow::array::{Int32Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::util::display::array_value_to_string;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::sql::{
    CommandStatementQuery, CommandStatementUpdate, DoPutUpdateResult, ProstMessageExt,
};
use arrow_flight::{FlightData, FlightDescriptor, PutResult, Ticket};
use datafusion::common::TableReference;
use futures::{TryStreamExt, stream};
use parking_lot::Mutex;
use prost::Message;
use runtime::Runtime;
use runtime::component::dataset::acceleration::{Acceleration, Engine};
use runtime::datafusion::query::QueryBuilder;
use runtime::internal_table::create_internal_accelerated_table;
use runtime_secrets::Secrets;
use rustls::crypto::{CryptoProvider, aws_lc_rs};
use test_framework::pki::PkiConfig;
use tokio::io::AsyncReadExt;
use tokio::net::TcpListener;
use tokio::sync::RwLock;
use tonic::Status;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity};

use crate::{configure_test_datafusion, init_tracing, utils::test_request_context};

use super::harness::ClusterHarness;

/// The node the requests come from when they do not come from the scheduler:
/// the identity the harness PKI gives a second executor.
const PEER_EXECUTOR: &str = "executor1";

/// A table the executor serves writes to.
const TABLE: &str = "public.my_table";

/// A TLS channel to `address` that presents the certificate the harness PKI
/// holds for `node_name`, as that node connects to another node.
async fn connect_presenting(
    pki: &PkiConfig,
    node_name: &str,
    address: SocketAddr,
) -> Result<Channel, anyhow::Error> {
    let tls = ClientTlsConfig::new()
        .ca_certificate(Certificate::from_pem(std::fs::read(&pki.ca_cert_path)?))
        .identity(Identity::from_pem(
            std::fs::read(pki.client_cert_path(node_name))?,
            std::fs::read(pki.client_key_path(node_name))?,
        ))
        // Every harness certificate names 127.0.0.1, where the nodes advertise.
        .domain_name("127.0.0.1");
    Ok(Endpoint::from_shared(format!("https://{address}"))?
        .tls_config(tls)?
        .connect()
        .await?)
}

fn table_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int32, false),
        Field::new("b", DataType::Utf8, false),
    ]))
}

fn rows(a: &[i32], b: &[&str]) -> Result<RecordBatch, anyhow::Error> {
    Ok(RecordBatch::try_new(
        table_schema(),
        vec![
            Arc::new(Int32Array::from(a.to_vec())),
            Arc::new(StringArray::from(b.to_vec())),
        ],
    )?)
}

/// Registers [`TABLE`] on `executor` as a writable, empty Arrow table.
async fn register_writable_table(executor: &Arc<Runtime>) -> Result<(), anyhow::Error> {
    let datafusion = executor.datafusion();
    let table = create_internal_accelerated_table(
        datafusion.runtime_status(),
        TableReference::parse_str(TABLE),
        table_schema(),
        None,
        Acceleration {
            engine: Engine::Arrow,
            ..Acceleration::default()
        },
        None,
        Arc::new(RwLock::new(Secrets::default())),
        Arc::clone(executor),
    )
    .await?;
    datafusion
        .register_table_as_writable_and_with_schema(
            TableReference::parse_str(TABLE),
            table.into_table(),
        )
        .map_err(anyhow::Error::from)
}

/// The rows of [`TABLE`] as `executor` reads them, ordered by `a`.
async fn table_rows(executor: &Runtime) -> Result<Vec<String>, anyhow::Error> {
    let batches: Vec<RecordBatch> = QueryBuilder::new(
        &format!("SELECT a, b FROM {TABLE} ORDER BY a"),
        executor.datafusion(),
    )
    .build()
    .run()
    .await
    .map_err(|e| anyhow::Error::msg(format!("reading {TABLE} failed: {e}")))?
    .data
    .try_collect()
    .await
    .map_err(|e| anyhow::Error::msg(format!("reading {TABLE} failed: {e}")))?;
    let mut read = Vec::new();
    for batch in &batches {
        for row in 0..batch.num_rows() {
            read.push(format!(
                "row ({}, {})",
                array_value_to_string(batch.column(0), row)?,
                array_value_to_string(batch.column(1), row)?
            ));
        }
    }
    Ok(read)
}

/// The messages of a `DoPut` that writes `batch` to [`TABLE`], with the path
/// descriptor the scheduler forwards a write with.
async fn put_messages(batch: RecordBatch) -> Result<Vec<FlightData>, anyhow::Error> {
    let descriptor = FlightDescriptor::new_path(vec![
        "spice".to_string(),
        "public".to_string(),
        "my_table".to_string(),
    ]);
    Ok(FlightDataEncoderBuilder::new()
        .with_flight_descriptor(Some(descriptor))
        .build(stream::iter([Ok(batch)]))
        .try_collect()
        .await?)
}

async fn do_put(channel: &Channel, messages: Vec<FlightData>) -> Result<String, Status> {
    let _acknowledgements: Vec<PutResult> = FlightServiceClient::new(channel.clone())
        .do_put(stream::iter(messages))
        .await?
        .into_inner()
        .try_collect()
        .await?;
    Ok("accepted".to_string())
}

/// Sends `sql` as a Flight SQL `CommandStatementUpdate`, which a `DoPut`
/// carries in its descriptor.
async fn statement_update(channel: &Channel, sql: &str) -> Result<String, Status> {
    let command = CommandStatementUpdate {
        query: sql.to_string(),
        transaction_id: None,
    };
    let message = FlightData::new()
        .with_descriptor(FlightDescriptor::new_cmd(command.as_any().encode_to_vec()));
    let results: Vec<PutResult> = FlightServiceClient::new(channel.clone())
        .do_put(stream::iter([message]))
        .await?
        .into_inner()
        .try_collect()
        .await?;
    Ok(match results.as_slice() {
        [result] => match DoPutUpdateResult::decode(result.app_metadata.clone()) {
            Ok(updated) => format!("updated {} rows", updated.record_count),
            Err(e) => format!("accepted with an unreadable result: {e}"),
        },
        other => format!("accepted with {} results", other.len()),
    })
}

/// `sql` as a Flight SQL `CommandStatementQuery`, the form `get_flight_info`
/// takes in its descriptor and `do_get` in its ticket.
fn statement_query(sql: &str) -> Vec<u8> {
    CommandStatementQuery {
        query: sql.to_string(),
        transaction_id: None,
    }
    .as_any()
    .encode_to_vec()
}

/// Plans `sql` through `get_flight_info`, returning the ticket that runs it.
async fn plan(channel: &Channel, sql: &str) -> Result<Ticket, Status> {
    let info = FlightServiceClient::new(channel.clone())
        .get_flight_info(FlightDescriptor::new_cmd(statement_query(sql)))
        .await?
        .into_inner();
    info.endpoint
        .into_iter()
        .find_map(|endpoint| endpoint.ticket)
        .ok_or_else(|| Status::internal("the planned statement has no ticket"))
}

async fn run(channel: &Channel, ticket: Vec<u8>) -> Result<String, Status> {
    let messages: Vec<FlightData> = FlightServiceClient::new(channel.clone())
        .do_get(Ticket {
            ticket: ticket.into(),
        })
        .await?
        .into_inner()
        .try_collect()
        .await?;
    Ok(format!("served {} messages", messages.len()))
}

/// What `request` returned: what was done, or the code and message it was
/// refused with.
fn outcome(request: &str, result: Result<String, Status>) -> String {
    match result {
        Ok(done) => format!("{request}: {done}"),
        Err(refused) => format!("{request}: {:?}: {}", refused.code(), refused.message()),
    }
}

/// The three ways a node sends an executor SQL: planned through
/// `get_flight_info`, or run through `do_get` as a Flight SQL statement or as
/// the plain SQL ticket.
async fn sql_outcomes(channel: &Channel, sql: &str) -> Vec<String> {
    vec![
        outcome(
            "get_flight_info",
            plan(channel, sql).await.map(|_| "planned".to_string()),
        ),
        outcome("do_get statement", run(channel, statement_query(sql)).await),
        outcome("do_get sql", run(channel, sql.as_bytes().to_vec()).await),
    ]
}

#[tokio::test(flavor = "multi_thread")]
async fn an_executor_takes_writes_and_executor_table_sql_only_from_the_scheduler()
-> Result<(), anyhow::Error> {
    let _ = CryptoProvider::install_default(aws_lc_rs::default_provider());
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async move {
            configure_test_datafusion();
            let harness = ClusterHarness::builder().executors(1).start().await?;
            let result = async {
                harness.wait_for_executors(Duration::from_secs(30)).await?;
                let executor = &harness.executors[0];
                register_writable_table(executor).await?;

                // The executor's Flight server listens at its cluster address.
                let address = executor.datafusion().cluster_config.node_bind_address();
                let pki = harness
                    .cluster_pki()
                    .expect("harness runs with cluster mTLS");
                pki.create_client_cert(PEER_EXECUTOR)?;
                let scheduler = connect_presenting(pki, "scheduler", address).await?;
                let peer = connect_presenting(pki, PEER_EXECUTOR, address).await?;

                // (1) A DoPut with the scheduler's certificate, as the scheduler
                //     forwards a write, is written.
                assert_eq!(table_rows(executor).await?, Vec::<String>::new());
                let written = do_put(
                    &scheduler,
                    put_messages(rows(&[1, 2, 3], &["a", "b", "c"])?).await?,
                )
                .await;
                assert_eq!(
                    outcome("scheduler DoPut", written),
                    "scheduler DoPut: accepted"
                );
                assert_eq!(
                    table_rows(executor).await?,
                    ["row (1, a)", "row (2, b)", "row (3, c)"]
                );

                // (2) The same DoPut, and a statement update, from a peer
                //     executor are refused, and the table keeps its rows.
                //     Every outcome is collected first, so a failure shows what
                //     each request did and what the table then held.
                let mut observed = vec![
                    outcome(
                        "peer DoPut",
                        do_put(&peer, put_messages(rows(&[4], &["d"])?).await?).await,
                    ),
                    outcome(
                        "peer statement update",
                        statement_update(&peer, &format!("INSERT INTO {TABLE} VALUES (5, 'e')"))
                            .await,
                    ),
                ];
                observed.extend(table_rows(executor).await?);
                assert_eq!(
                    observed,
                    [
                        "peer DoPut: Unauthenticated: Flight DoPut requires authentication.\nFor auth details, visit https://spiceai.org/docs/api/auth",
                        "peer statement update: Unauthenticated: Flight DoPut requires authentication.\nFor auth details, visit https://spiceai.org/docs/api/auth",
                        "row (1, a)",
                        "row (2, b)",
                        "row (3, c)",
                    ],
                    "a peer executor's writes are refused and the table is unchanged"
                );

                // (3) SQL that calls `executor_table()` passes its role check
                //     when the scheduler sends it, and stops at the next check:
                //     this table is not in a Cayenne catalog.
                let sql = format!(
                    "SELECT * FROM executor_table('https://127.0.0.1:9', 'spice.{TABLE}')"
                );
                assert_eq!(
                    sql_outcomes(&scheduler, &sql).await,
                    [
                        "get_flight_info: InvalidArgument: executor_table: schema 'spice.public' is not a Cayenne catalog schema",
                        "do_get statement: InvalidArgument: executor_table: schema 'spice.public' is not a Cayenne catalog schema",
                        "do_get sql: InvalidArgument: executor_table: schema 'spice.public' is not a Cayenne catalog schema",
                    ],
                    "the scheduler's executor_table() SQL passes the role check"
                );

                // The same SQL from a peer executor is refused by the role check,
                // however it is sent.
                assert_eq!(
                    sql_outcomes(&peer, &sql).await,
                    [
                        "get_flight_info: InvalidArgument: executor_table() is not available: it is used only in the queries the cluster scheduler sends to executors",
                        "do_get statement: InvalidArgument: Failed to parse SQL: Error during planning: executor_table() is not available: it is used only in the queries the cluster scheduler sends to executors",
                        "do_get sql: InvalidArgument: Failed to parse SQL: Error during planning: executor_table() is not available: it is used only in the queries the cluster scheduler sends to executors",
                    ],
                    "a peer executor's executor_table() SQL is refused"
                );
                Ok::<(), anyhow::Error>(())
            }
            .await;
            harness.shutdown().await;
            result
        })
        .await
}

/// A TCP listener that records each connection made to it, and the first two
/// bytes the connection sent.
struct ConnectionRecorder {
    address: SocketAddr,
    accepted: Arc<AtomicUsize>,
    heads: Arc<Mutex<Vec<Vec<u8>>>>,
    task: tokio::task::JoinHandle<()>,
}

impl ConnectionRecorder {
    async fn start() -> Result<Self, anyhow::Error> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let accepted = Arc::new(AtomicUsize::new(0));
        let heads = Arc::new(Mutex::new(Vec::new()));
        let task = tokio::spawn({
            let accepted = Arc::clone(&accepted);
            let heads = Arc::clone(&heads);
            async move {
                while let Ok((mut socket, _)) = listener.accept().await {
                    accepted.fetch_add(1, Ordering::SeqCst);
                    let mut head = [0_u8; 2];
                    let read =
                        tokio::time::timeout(Duration::from_secs(5), socket.read_exact(&mut head))
                            .await;
                    heads.lock().push(match read {
                        Ok(Ok(_)) => head.to_vec(),
                        _ => Vec::new(),
                    });
                }
            }
        });
        Ok(Self {
            address,
            accepted,
            heads,
            task,
        })
    }

    fn accepted(&self) -> usize {
        self.accepted.load(Ordering::SeqCst)
    }

    /// The first bytes of the first connection, once one has sent them.
    async fn first_head(&self, timeout: Duration) -> Option<Vec<u8>> {
        let start = tokio::time::Instant::now();
        loop {
            if let Some(head) = self.heads.lock().first() {
                return Some(head.clone());
            }
            if start.elapsed() > timeout {
                return None;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

impl Drop for ConnectionRecorder {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// `executor_table()` opens a connection to the endpoint it names only in SQL
/// the scheduler sends: the same SQL from a peer executor is refused while it is
/// planned, and the endpoint records no connection.
#[tokio::test(flavor = "multi_thread")]
#[cfg_attr(
    not(feature = "spicebench"),
    ignore = "requires the spicebench feature"
)]
async fn a_peer_executor_table_call_opens_no_connection() -> Result<(), anyhow::Error> {
    let _ = CryptoProvider::install_default(aws_lc_rs::default_provider());
    let _tracing = init_tracing(Some("integration=debug,info"));

    let temp_dir = tempfile::tempdir()?;
    let data_dir = temp_dir.path().join("data");
    let metadata_dir = temp_dir.path().join("metadata");

    test_request_context()
        .scope(async move {
            configure_test_datafusion();
            // `executor_table()` reads only a table of a Cayenne catalog, which
            // every node declares.
            let catalog = super::distributed_cayenne_catalog::make_cayenne_catalog(
                "xtcat",
                &data_dir.to_string_lossy(),
                &metadata_dir.to_string_lossy(),
            );
            let harness = ClusterHarness::builder()
                .scheduler(
                    app::AppBuilder::new("executor_table_authorization")
                        .with_catalog(catalog.clone())
                        .build(),
                )
                .executor_with_app(
                    app::AppBuilder::new("executor_table_authorization_executor")
                        .with_catalog(catalog)
                        .build(),
                )
                .start()
                .await?;
            let result = async {
                harness.wait_for_executors(Duration::from_secs(30)).await?;
                harness.query("CREATE SCHEMA xtcat.s").await?;
                harness
                    .query(
                        "CREATE TABLE xtcat.s.dim (id BIGINT NOT NULL, name VARCHAR NOT NULL) \
                         PARTITION BY id",
                    )
                    .await?;

                let executor = &harness.executors[0];
                let address = executor.datafusion().cluster_config.node_bind_address();
                let pki = harness
                    .cluster_pki()
                    .expect("harness runs with cluster mTLS");
                pki.create_client_cert(PEER_EXECUTOR)?;
                let scheduler = connect_presenting(pki, "scheduler", address).await?;
                let peer = connect_presenting(pki, PEER_EXECUTOR, address).await?;

                // The endpoint `executor_table()` names records every connection.
                let endpoint = ConnectionRecorder::start().await?;
                let sql = format!(
                    "SELECT id, name FROM executor_table('https://{}', 'xtcat.s.dim')",
                    endpoint.address
                );

                // (1) From a peer executor, the SQL is refused however it is
                //     sent, and the endpoint records no connection.
                let mut observed = sql_outcomes(&peer, &sql).await;
                observed.push(format!("connections: {}", endpoint.accepted()));
                assert_eq!(
                    observed,
                    [
                        "get_flight_info: InvalidArgument: executor_table() is not available: it is used only in the queries the cluster scheduler sends to executors",
                        "do_get statement: InvalidArgument: Failed to parse SQL: Error during planning: executor_table() is not available: it is used only in the queries the cluster scheduler sends to executors",
                        "do_get sql: InvalidArgument: Failed to parse SQL: Error during planning: executor_table() is not available: it is used only in the queries the cluster scheduler sends to executors",
                        "connections: 0",
                    ],
                    "a peer executor's executor_table() SQL is refused before any connection"
                );

                // (2) The same SQL from the scheduler connects to the endpoint:
                //     the executor opens a TLS connection there, which the
                //     recorder sees as a TLS handshake record (0x16, 0x03).
                let ticket = plan(&scheduler, &sql).await?;
                let running = tokio::spawn({
                    let scheduler = scheduler.clone();
                    async move { run(&scheduler, ticket.ticket.to_vec()).await }
                });
                let head = endpoint.first_head(Duration::from_secs(30)).await;
                running.abort();
                assert_eq!(
                    head,
                    Some(vec![0x16, 0x03]),
                    "the scheduler's executor_table() SQL connects to the endpoint it names"
                );
                Ok::<(), anyhow::Error>(())
            }
            .await;
            harness.shutdown().await;
            result
        })
        .await
}
