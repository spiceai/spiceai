/*
Copyright 2026 The Spice.ai OSS Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

//! Real Flight round trips through the configured sidecar connector. The input
//! table and direct local evaluation are the round-trip oracle.

use std::{collections::HashMap, sync::Arc, time::Duration};

use app::AppBuilder;
use arrow::{
    array::{Int64Array, RecordBatch, StringArray},
    util::pretty::pretty_format_batches,
};
use data_components::flight::FlightFactory;
use datafusion::{
    common::TableReference, prelude::SessionContext, sql::unparser::dialect::DefaultDialect,
};
use flight_client::{Credentials, FlightClient};
use futures::TryStreamExt;
use runtime::{
    Runtime,
    config::{ClusterConfig, Config, Role},
    datafusion::query::QueryBuilder,
};
use runtime_auth::{FlightBasicAuth, api_key::ApiKeyAuth};
use secrecy::SecretString;
use spicepod::{
    component::{dataset::Dataset, runtime::ApiKey},
    param::Params,
};

use super::start_spice_test_app_with_metrics_port;
use crate::utils::test_request_context;

/// The upstream's only API key. The sidecar's datasets pass it explicitly, so
/// the connector never autoloads `spiceai_api_key` from the environment or a
/// `.env` file, where CI keeps a Spice Cloud key the upstream would reject.
const API_KEY: &str = "sidecar-roundtrip";

fn assert_same_rows(
    actual: &[RecordBatch],
    expected: &[RecordBatch],
    sql: &str,
) -> anyhow::Result<()> {
    assert_eq!(
        pretty_format_batches(actual)?.to_string(),
        pretty_format_batches(expected)?.to_string(),
        "{sql}"
    );
    Ok(())
}

async fn rows(runtime: &Arc<Runtime>, sql: &str) -> anyhow::Result<Vec<RecordBatch>> {
    Ok(QueryBuilder::new(sql, runtime.datafusion())
        .build()
        .run()
        .await?
        .into_record_batch_stream()
        .try_collect()
        .await?)
}

#[tokio::test]
async fn sidecar_substrait_connector_roundtrip() -> anyhow::Result<()> {
    test_request_context().scope(async {
        let auth = Arc::new(ApiKeyAuth::new(vec![ApiKey::parse_str(API_KEY)]))
            as Arc<dyn FlightBasicAuth + Send + Sync>;
        let (_channel, upstream, metrics_port) =
            start_spice_test_app_with_metrics_port(Some(auth), None, None, None).await?;
        let endpoint = format!("http://127.0.0.1:{}", metrics_port - 1);
        let input = RecordBatch::try_from_iter(vec![
            ("id", Arc::new(Int64Array::from(vec![1, 2, 3, 4])) as arrow::array::ArrayRef),
            ("value", Arc::new(Int64Array::from(vec![Some(11), None, Some(-7), Some(0)]))),
            ("label", Arc::new(StringArray::from(vec![Some("雪"), Some(""), None, Some("x")]))),
        ])?;
        upstream.ctx.register_batch("remote_orders", input.clone())?;
        let shadow_add = datafusion::logical_expr::create_udf(
            "add", vec![arrow::datatypes::DataType::Int64; 2],
            arrow::datatypes::DataType::Int64,
            datafusion::logical_expr::Volatility::Immutable,
            Arc::new(|_| Ok(datafusion::logical_expr::ColumnarValue::Scalar(
                datafusion::common::ScalarValue::Int64(Some(-999)),
            ))),
        );
        upstream.ctx.register_udf(shadow_add);
        upstream.ctx.register_udf(datafusion::logical_expr::create_udf(
            "is_nan", vec![arrow::datatypes::DataType::Float64],
            arrow::datatypes::DataType::Boolean,
            datafusion::logical_expr::Volatility::Immutable,
            Arc::new(|_| Ok(datafusion::logical_expr::ColumnarValue::Scalar(
                datafusion::common::ScalarValue::Boolean(Some(true)),
            ))),
        ));
        let oracle = SessionContext::new();
        oracle.register_batch("orders", input.clone())?;
        oracle.register_batch("other_orders", input.clone())?;

        let mut app = AppBuilder::new("sidecar_roundtrip");
        for (prefix, name) in [("spice.ai", "orders"), ("spiceai", "other_orders")] {
            let mut dataset = Dataset::new(format!("{prefix}:remote_orders"), name.to_string());
            dataset.params = Some(Params::from_string_map(HashMap::from([
                ("spiceai_endpoint".to_string(), endpoint.clone()),
                ("spiceai_api_key".to_string(), API_KEY.to_string()),
            ])));
            app = app.with_dataset(dataset);
        }
        let sidecar = Arc::new(
            Runtime::builder()
                .with_app(app.build())
                .with_runtime_config(Config {
                    cluster: ClusterConfig::default().with_role(Role::Sidecar),
                    ..Config::default()
                })
                .build()
                .await,
        );
        assert_eq!(sidecar.role(), Role::Sidecar);
        tokio::time::timeout(Duration::from_secs(30), Arc::clone(&sidecar).load_components()).await?;
        assert_eq!(sidecar.datafusion().ctx.state().config().target_partitions(), 1);

        let local_function = datafusion::logical_expr::create_udf(
            "sidecar_roundtrip_identity",
            vec![arrow::datatypes::DataType::Int64],
            arrow::datatypes::DataType::Int64,
            datafusion::logical_expr::Volatility::Immutable,
            Arc::new(|arguments| Ok(arguments[0].clone())),
        );
        runtime_udfs_api::add_user_function("sidecar_roundtrip_identity");
        sidecar.datafusion().ctx.register_udf(local_function.clone());
        oracle.register_udf(local_function);

        let corpus = [
            "SELECT id, value, label FROM orders ORDER BY id",
            "SELECT id + CAST(1 AS BIGINT) AS n FROM orders ORDER BY id",
            "SELECT id, label FROM other_orders WHERE value IS NULL ORDER BY id",
            "SELECT id FROM orders WHERE value < 0 OR value = 0 ORDER BY id",
            "SELECT COUNT(*) AS n, COUNT(value) AS nn, SUM(value) AS total FROM orders",
            "SELECT label, SUM(value) AS total FROM orders GROUP BY label ORDER BY label NULLS FIRST",
            "SELECT l.id, l.label, r.value FROM orders l JOIN other_orders r ON l.id = r.id WHERE l.id > 1 ORDER BY l.id",
            "SELECT id FROM orders ORDER BY id DESC LIMIT 2 OFFSET 1",
            "SELECT id FROM orders WHERE id = 999",
            "SELECT id FROM orders WHERE value NOT IN (SELECT value FROM other_orders) ORDER BY id",
            "SELECT id FROM orders WHERE NOT (value IN (SELECT value FROM other_orders)) ORDER BY id",
            "SELECT id FROM orders WHERE value NOT IN (SELECT value FROM other_orders WHERE id < 0) ORDER BY id",
            "SELECT CASE WHEN value IS NULL THEN 99 ELSE value END AS v FROM orders ORDER BY id",
            "SELECT arrow_cast(label, 'Utf8View') AS label FROM orders ORDER BY id",
            "SELECT CAST(value AS DECIMAL(20, 3)) AS decimal_value FROM orders ORDER BY id",
            "SELECT make_array(id, value) AS numbers FROM orders ORDER BY id",
            "SELECT named_struct('key', id, 'value', label) AS nested FROM orders ORDER BY id",
            "SELECT map(make_array('key', 'value'), make_array(id, value)) AS attributes FROM orders ORDER BY id",
            "SELECT to_timestamp_seconds(id) AS ts FROM orders ORDER BY id",
            "SELECT id, isnan(CAST(id AS DOUBLE)) AS n FROM orders ORDER BY id",
            "SELECT id, log(CAST(2 AS DOUBLE), CAST(id AS DOUBLE)) AS n FROM orders WHERE id > 0 ORDER BY id",
            "SELECT sidecar_roundtrip_identity(value) AS value FROM orders ORDER BY id",
        ];
        for sql in corpus {
            let expected_frame = oracle.sql(sql).await?;
            let result = QueryBuilder::new(sql, sidecar.datafusion()).build().run().await?;
            assert_eq!(result.data.schema().as_ref(), expected_frame.schema().as_arrow(), "{sql}");
            let expected = expected_frame.collect().await?;
            let actual: Vec<RecordBatch> = result.into_record_batch_stream().try_collect().await?;
            assert_same_rows(&actual, &expected, sql)?;
            if let (Some(actual), Some(expected)) = (actual.first(), expected.first()) {
                assert_eq!(actual.schema(), expected.schema(), "{sql}");
            }
        }
        let unsupported = "SELECT id, value IN (SELECT value FROM other_orders) AS present FROM orders ORDER BY id";
        let oracle_error = oracle.sql(unsupported).await?.collect().await.expect_err("projection IN is unsupported");
        let remote_error = rows(&sidecar, unsupported).await.expect_err("projection IN is unsupported");
        assert!(oracle_error.to_string().contains("Physical plan does not support logical expression InSubquery"), "{oracle_error}");
        assert!(remote_error.to_string().contains("Physical plan does not support logical expression InSubquery"), "{remote_error}");

        let explained = rows(&sidecar,
            "EXPLAIN SELECT l.id FROM orders l JOIN other_orders r ON l.id = r.id").await?;
        let explain = pretty_format_batches(&explained)?.to_string();
        assert_eq!(explain.matches("SubstraitExec:").count(), 1, "{explain}");
        assert!(!explain.contains("FlightExec sql="), "{explain}");

        // An ordinary DataFusion session has no federation analyzer. Its scan
        // must still send a Substrait plan rather than taking the SQL path.
        let credentials = Credentials::new("", SecretString::from(API_KEY));
        let client = FlightClient::try_new(endpoint.into(), credentials, None, None).await?;
        #[expect(clippy::disallowed_methods, reason = "the test connects to a real Spice runtime")]
        let factory = FlightFactory::new("spice.ai", client, Arc::new(DefaultDialect {})).with_substrait(true);
        let provider = factory.table_provider(TableReference::bare("remote_orders")).await
            .map_err(anyhow::Error::from_boxed)?;
        let fallback = SessionContext::new();
        fallback.register_table("orders", provider)?;
        for sql in [
            "SELECT id FROM orders WHERE value IS NULL ORDER BY id",
            "SELECT COUNT(*) AS n FROM orders",
            "SELECT 1 AS constant FROM orders",
        ] {
            let actual = fallback.sql(sql).await?.collect().await?;
            let expected = oracle.sql(sql).await?.collect().await?;
            assert_same_rows(&actual, &expected, sql)?;
        }

        sidecar.shutdown().await;
        runtime_udfs_api::remove_user_function("sidecar_roundtrip_identity");
        Ok(())
    }).await
}

/// A Flight proxy in front of an upstream Spice runtime that counts the
/// Substrait tickets it forwards by kind, and plays an upstream of a chosen
/// [`ticket_proxy::Mode`]; the mode can change while it runs.
mod ticket_proxy {
    use std::{
        pin::Pin,
        sync::{
            Arc,
            atomic::{AtomicU8, AtomicUsize, Ordering},
        },
    };

    use arrow_flight::{
        Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
        HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
        flight_service_client::FlightServiceClient,
        flight_service_server::{FlightService, FlightServiceServer},
        sql::{Any, CommandGetSqlInfo, CommandStatementSubstraitPlan, ProstMessageExt},
    };
    use flight_client::{
        parameterized_ticket::{
            PARAMETERIZED_SUBSTRAIT_PLAN_TYPE_URL, SQL_INFO_PARAMETERIZED_SUBSTRAIT,
        },
        traced_ticket::TRACED_TICKET_TYPE_URL,
    };
    use futures::{Stream, StreamExt};
    use prost::Message;
    use tonic::{Request, Response, Status, Streaming, transport::Channel};

    type BoxStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send>>;

    /// The upstream the proxy plays.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    #[repr(u8)]
    pub(super) enum Mode {
        /// The upstream as it is: it advertises and takes parameterized plans.
        Current = 0,
        /// An upstream that predates parameterized plans: its `GetSqlInfo`
        /// answer has no code 10,000, and it refuses the ticket as unknown.
        Old = 1,
        /// Advertises parameterized plans, then refuses them as unknown, as a
        /// rolling upgrade can when the probe reaches a new runtime and the
        /// query an old one.
        AdvertisesButRefuses = 2,
        /// Accepts the credential exchange and never answers it.
        StallHandshake = 3,
    }

    #[derive(Default)]
    pub(super) struct Counts {
        pub(super) parameterized: AtomicUsize,
        pub(super) plain: AtomicUsize,
        mode: AtomicU8,
    }

    impl Counts {
        pub(super) fn set_mode(&self, mode: Mode) {
            self.mode.store(mode as u8, Ordering::SeqCst);
        }

        fn mode(&self) -> Mode {
            match self.mode.load(Ordering::SeqCst) {
                0 => Mode::Current,
                1 => Mode::Old,
                2 => Mode::AdvertisesButRefuses,
                _ => Mode::StallHandshake,
            }
        }
    }

    impl Counts {
        pub(super) fn get(&self) -> (usize, usize) {
            (
                self.parameterized.load(Ordering::SeqCst),
                self.plain.load(Ordering::SeqCst),
            )
        }
    }

    struct Proxy {
        upstream: FlightServiceClient<Channel>,
        counts: Arc<Counts>,
    }

    /// Starts a proxy to `upstream` playing `mode`, and returns its endpoint
    /// and counts.
    pub(super) async fn start(upstream: &str, mode: Mode) -> anyhow::Result<(String, Arc<Counts>)> {
        let channel = Channel::from_shared(upstream.to_string())?
            .connect()
            .await?;
        let counts = Arc::new(Counts::default());
        counts.set_mode(mode);
        let proxy = Proxy {
            upstream: FlightServiceClient::new(channel),
            counts: Arc::clone(&counts),
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("http://{}", listener.local_addr()?);
        tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(FlightServiceServer::new(proxy))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
        );
        Ok((endpoint, counts))
    }

    fn forward<T>(request: Request<T>) -> Request<T> {
        let (metadata, _, message) = request.into_parts();
        Request::from_parts(metadata, tonic::Extensions::default(), message)
    }

    fn boxed<T: Send + 'static>(response: Response<Streaming<T>>) -> Response<BoxStream<T>> {
        let (metadata, stream, extensions) = response.into_parts();
        Response::from_parts(metadata, stream.boxed(), extensions)
    }

    /// The `type_url` of the command a ticket carries, under any trace envelope.
    fn command_type(ticket: &Ticket) -> Option<String> {
        let any = Any::decode(ticket.ticket.clone()).ok()?;
        if any.type_url == TRACED_TICKET_TYPE_URL {
            return Any::decode(any.value.slice(32..))
                .ok()
                .map(|any| any.type_url);
        }
        Some(any.type_url)
    }

    #[tonic::async_trait]
    impl FlightService for Proxy {
        type HandshakeStream = BoxStream<HandshakeResponse>;
        type ListFlightsStream = BoxStream<FlightInfo>;
        type DoGetStream = BoxStream<FlightData>;
        type DoPutStream = BoxStream<PutResult>;
        type DoActionStream = BoxStream<arrow_flight::Result>;
        type ListActionsStream = BoxStream<ActionType>;
        type DoExchangeStream = BoxStream<FlightData>;

        async fn handshake(
            &self,
            request: Request<Streaming<HandshakeRequest>>,
        ) -> Result<Response<Self::HandshakeStream>, Status> {
            if self.counts.mode() == Mode::StallHandshake {
                std::future::pending::<()>().await;
            }
            let (metadata, _, stream) = request.into_parts();
            // The client sends one handshake message; forward what arrives.
            let messages: Vec<HandshakeRequest> = stream
                .collect::<Vec<_>>()
                .await
                .into_iter()
                .collect::<Result<_, _>>()?;
            let stream = futures::stream::iter(messages);
            let request = Request::from_parts(metadata, tonic::Extensions::default(), stream);
            Ok(boxed(self.upstream.clone().handshake(request).await?))
        }

        async fn list_flights(
            &self,
            request: Request<Criteria>,
        ) -> Result<Response<Self::ListFlightsStream>, Status> {
            Ok(boxed(
                self.upstream.clone().list_flights(forward(request)).await?,
            ))
        }

        async fn get_flight_info(
            &self,
            request: Request<FlightDescriptor>,
        ) -> Result<Response<FlightInfo>, Status> {
            let mut request = forward(request);
            // An old upstream does not know code 10,000; the ticket the
            // upstream answers with carries the rewritten command.
            if self.counts.mode() == Mode::Old
                && let Ok(any) = Any::decode(request.get_ref().cmd.clone())
                && let Ok(Some(mut command)) = any.unpack::<CommandGetSqlInfo>()
            {
                command
                    .info
                    .retain(|code| *code != SQL_INFO_PARAMETERIZED_SUBSTRAIT);
                request.get_mut().cmd = command.as_any().encode_to_vec().into();
            }
            self.upstream.clone().get_flight_info(request).await
        }

        async fn poll_flight_info(
            &self,
            request: Request<FlightDescriptor>,
        ) -> Result<Response<PollInfo>, Status> {
            self.upstream
                .clone()
                .poll_flight_info(forward(request))
                .await
        }

        async fn get_schema(
            &self,
            request: Request<FlightDescriptor>,
        ) -> Result<Response<SchemaResult>, Status> {
            self.upstream.clone().get_schema(forward(request)).await
        }

        async fn do_get(
            &self,
            request: Request<Ticket>,
        ) -> Result<Response<Self::DoGetStream>, Status> {
            match command_type(request.get_ref()).as_deref() {
                Some(PARAMETERIZED_SUBSTRAIT_PLAN_TYPE_URL) => {
                    self.counts.parameterized.fetch_add(1, Ordering::SeqCst);
                    if self.counts.mode() != Mode::Current {
                        return Err(Status::unimplemented(format!(
                            "Unknown command type: {PARAMETERIZED_SUBSTRAIT_PLAN_TYPE_URL}"
                        )));
                    }
                }
                Some(url) if url == CommandStatementSubstraitPlan::type_url() => {
                    self.counts.plain.fetch_add(1, Ordering::SeqCst);
                }
                _ => {}
            }
            Ok(boxed(self.upstream.clone().do_get(forward(request)).await?))
        }

        async fn do_put(
            &self,
            _: Request<Streaming<FlightData>>,
        ) -> Result<Response<Self::DoPutStream>, Status> {
            Err(Status::unimplemented("do_put"))
        }

        async fn do_action(
            &self,
            request: Request<Action>,
        ) -> Result<Response<Self::DoActionStream>, Status> {
            Ok(boxed(
                self.upstream.clone().do_action(forward(request)).await?,
            ))
        }

        async fn list_actions(
            &self,
            request: Request<Empty>,
        ) -> Result<Response<Self::ListActionsStream>, Status> {
            Ok(boxed(
                self.upstream.clone().list_actions(forward(request)).await?,
            ))
        }

        async fn do_exchange(
            &self,
            _: Request<Streaming<FlightData>>,
        ) -> Result<Response<Self::DoExchangeStream>, Status> {
            Err(Status::unimplemented("do_exchange"))
        }
    }
}

/// An upstream serving `remote_lookups`, and the rows it holds.
async fn lookup_upstream() -> anyhow::Result<(String, RecordBatch)> {
    lookup_upstream_labelled(["雪", "", "", "x"]).await
}

/// [`lookup_upstream`] with the labels of ids 1 to 4 (id 3's is NULL).
async fn lookup_upstream_labelled(labels: [&str; 4]) -> anyhow::Result<(String, RecordBatch)> {
    let auth = Arc::new(ApiKeyAuth::new(vec![ApiKey::parse_str(API_KEY)]))
        as Arc<dyn FlightBasicAuth + Send + Sync>;
    let (_channel, upstream, metrics_port) =
        start_spice_test_app_with_metrics_port(Some(auth), None, None, None).await?;
    let input = RecordBatch::try_from_iter(vec![
        (
            "id",
            Arc::new(Int64Array::from(vec![1, 2, 3, 4])) as arrow::array::ArrayRef,
        ),
        (
            "label",
            Arc::new(StringArray::from(vec![
                Some(labels[0]),
                Some(labels[1]),
                None,
                Some(labels[3]),
            ])),
        ),
        (
            "active",
            Arc::new(arrow::array::BooleanArray::from(vec![
                Some(true),
                Some(false),
                None,
                Some(true),
            ])),
        ),
    ])?;
    upstream
        .ctx
        .register_batch("remote_lookups", input.clone())?;
    Ok((format!("http://127.0.0.1:{}", metrics_port - 1), input))
}

/// A sidecar with a `spice.ai:remote_lookups` dataset per `(name, endpoint)`,
/// and an oracle holding `input` under each name.
async fn lookup_sidecar(
    tables: &[(&str, &str)],
    input: &RecordBatch,
) -> anyhow::Result<(Arc<Runtime>, SessionContext)> {
    let oracle = SessionContext::new();
    let mut app = AppBuilder::new("sidecar_parameterized_lookups");
    for (name, endpoint) in tables {
        oracle.register_batch(*name, input.clone())?;
        let mut dataset = Dataset::new("spice.ai:remote_lookups".to_string(), (*name).to_string());
        dataset.params = Some(Params::from_string_map(HashMap::from([
            ("spiceai_endpoint".to_string(), (*endpoint).to_string()),
            ("spiceai_api_key".to_string(), API_KEY.to_string()),
        ])));
        app = app.with_dataset(dataset);
    }
    let sidecar = Arc::new(
        Runtime::builder()
            .with_app(app.build())
            .with_runtime_config(Config {
                cluster: ClusterConfig::default().with_role(Role::Sidecar),
                ..Config::default()
            })
            .build()
            .await,
    );
    tokio::time::timeout(
        Duration::from_secs(30),
        Arc::clone(&sidecar).load_components(),
    )
    .await?;
    Ok((sidecar, oracle))
}

async fn assert_lookup(
    sidecar: &Arc<Runtime>,
    oracle: &SessionContext,
    sql: &str,
) -> anyhow::Result<()> {
    let expected = oracle.sql(sql).await?.collect().await?;
    assert_same_rows(&rows(sidecar, sql).await?, &expected, sql)
}

/// Lookups that differ only in their key values plan one template per query
/// shape and bind each request's values to it, returning each value's own
/// rows. An upstream that advertises parameterized plans in `GetSqlInfo` is
/// sent every lookup as one; an upstream that does not is sent none. One that
/// advertises them and then refuses each as unknown has each lookup resent
/// with its values inlined, and is offered the next one parameterized again.
#[tokio::test]
async fn sidecar_lookups_bind_their_values_to_one_planned_template() -> anyhow::Result<()> {
    test_request_context()
        .scope(async {
            use ticket_proxy::Mode;
            let (upstream, input) = lookup_upstream().await?;
            let (current, current_counts) = ticket_proxy::start(&upstream, Mode::Current).await?;
            let (legacy, legacy_counts) = ticket_proxy::start(&upstream, Mode::Old).await?;
            let (refusing, refusing_counts) =
                ticket_proxy::start(&upstream, Mode::AdvertisesButRefuses).await?;
            let (sidecar, oracle) = lookup_sidecar(
                &[
                    ("lookups", &current),
                    ("legacy_lookups", &legacy),
                    ("refusing_lookups", &refusing),
                ],
                &input,
            )
            .await?;
            let stats = || sidecar.datafusion().federated_template_stats();
            let refused_before = data_components::flight::parameterized_plans_refused();

            // Each group is one query shape: the first lookup plans its template and
            // every other lookup binds its own values to it.
            let shapes: [&[&str]; 4] = [
                &[
                    "SELECT 1 AS present FROM {table} WHERE label = 'x' LIMIT 1",
                    "SELECT 1 AS present FROM {table} WHERE label = '雪' LIMIT 1",
                    "SELECT 1 AS present FROM {table} WHERE label = 'missing' LIMIT 1",
                ],
                &[
                    "SELECT label FROM {table} WHERE id = 2",
                    "SELECT label FROM {table} WHERE id = 3",
                ],
                &[
                    "SELECT id FROM {table} WHERE id IN (1, 4) ORDER BY id",
                    "SELECT id FROM {table} WHERE id IN (2, 3) ORDER BY id",
                ],
                &[
                    "SELECT id FROM {table} WHERE active = true ORDER BY id",
                    "SELECT id FROM {table} WHERE active = false ORDER BY id",
                ],
            ];
            let lookups: usize = shapes.iter().map(|shape| shape.len()).sum();
            for table in ["lookups", "legacy_lookups", "refusing_lookups"] {
                for shape in shapes {
                    let before = stats();
                    for sql in shape {
                        assert_lookup(&sidecar, &oracle, &sql.replace("{table}", table)).await?;
                    }
                    let after = stats();
                    assert_eq!(
                        (after.planned - before.planned, after.reused - before.reused),
                        (1, shape.len() as u64 - 1),
                        "{table}: {shape:?}"
                    );
                }
            }
            assert_eq!(
                current_counts.get(),
                (lookups, 0),
                "every lookup is sent parameterized"
            );
            assert_eq!(
                legacy_counts.get(),
                (0, lookups),
                "an upstream that does not advertise parameterized plans is sent none"
            );
            assert_eq!(
                refusing_counts.get(),
                (lookups, lookups),
                "every refused lookup is resent inlined, and the next offered parameterized again"
            );
            assert_eq!(
                data_components::flight::parameterized_plans_refused() - refused_before,
                lookups as u64,
                "each refusal is counted"
            );

            // A plan folding `now()` into a constant is never a template.
            let before = stats();
            let sql = "SELECT id FROM lookups WHERE id = 2 AND now() > to_timestamp(0)";
            assert_lookup(&sidecar, &oracle, sql).await?;
            assert_eq!(stats(), before, "{sql}");

            // An IN subquery is evaluated in the sidecar, so the shape plans as a
            // join of two Substrait leaves, not a template: it is planned once,
            // declined from then on, and never counted as reused.
            let before = stats();
            for id in [1, 4] {
                let sql = format!(
                    "SELECT id FROM lookups WHERE id = {id} AND label IN (SELECT label FROM lookups WHERE active = true)"
                );
                assert_lookup(&sidecar, &oracle, &sql).await?;
            }
            let after = stats();
            assert_eq!(
                (
                    after.planned - before.planned,
                    after.reused - before.reused,
                    after.declined - before.declined
                ),
                (1, 0, 1),
                "a shape that is not a template"
            );

            sidecar.shutdown().await;
            Ok(())
        })
        .await
}

/// What a probe found stands only until it expires: an upstream upgraded in
/// place to take parameterized plans is sent them once its earlier answer
/// expires, without a sidecar restart.
#[tokio::test]
async fn sidecar_probes_an_upgraded_upstream_again() -> anyhow::Result<()> {
    test_request_context()
        .scope(async {
            use ticket_proxy::Mode;
            // Other tests' upstreams are probed more often while this is set; that
            // changes no answer they get.
            data_components::flight::set_parameterized_probe_intervals_for_tests(
                data_components::flight::ParameterizedProbeIntervals {
                    answered: Duration::from_millis(300),
                    failed: Duration::from_millis(300),
                    timeout: Duration::from_secs(10),
                },
            );
            let (upstream, input) = lookup_upstream().await?;
            let (endpoint, counts) = ticket_proxy::start(&upstream, Mode::Old).await?;
            let (sidecar, oracle) = lookup_sidecar(&[("lookups", &endpoint)], &input).await?;

            assert_lookup(&sidecar, &oracle, "SELECT label FROM lookups WHERE id = 2").await?;
            assert_eq!(counts.get(), (0, 1), "an old upstream is sent a plain plan");

            counts.set_mode(Mode::Current);
            // Each lookup's text is new, so none is answered from the sidecar's
            // results cache without reaching the upstream.
            let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
            let mut bound = 1000;
            while counts.get().0 == 0 {
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "the upgraded upstream was never sent a parameterized plan: {:?}",
                    counts.get()
                );
                bound += 1;
                assert_lookup(
                    &sidecar,
                    &oracle,
                    &format!("SELECT label FROM lookups WHERE id = 2 AND id < {bound}"),
                )
                .await?;
                tokio::time::sleep(Duration::from_millis(50)).await;
            }

            sidecar.shutdown().await;
            Ok(())
        })
        .await
}

/// Two plans that read the same table name, with the same schema, from two
/// different upstreams are two query shapes: a template planned for the first
/// upstream must never answer the second's lookup. Plans built from providers
/// (`QueryBuilder::from_plan`) name the table alike whichever upstream it
/// reads, and `TableScan` equality ignores the provider.
#[tokio::test]
async fn a_template_is_never_shared_between_upstreams() -> anyhow::Result<()> {
    test_request_context()
        .scope(async {
            use datafusion::{
                datasource::provider_as_source,
                logical_expr::{LogicalPlanBuilder, col, lit},
            };
            let (first, _) = lookup_upstream_labelled(["a-one", "a-two", "", "a-four"]).await?;
            let (second, _) = lookup_upstream_labelled(["b-one", "b-two", "", "b-four"]).await?;
            let empty = RecordBatch::new_empty(Arc::new(arrow::datatypes::Schema::empty()));
            let (sidecar, _) = lookup_sidecar(&[], &empty).await?;

            let lookup = |endpoint: String, id: i64| {
                let df = sidecar.datafusion();
                async move {
                    let credentials = Credentials::new("", SecretString::from(API_KEY));
                    let client =
                        FlightClient::try_new(endpoint.into(), credentials, None, None).await?;
                    #[expect(
                        clippy::disallowed_methods,
                        reason = "the test connects to a real Spice runtime"
                    )]
                    let factory =
                        FlightFactory::new("spice.ai", client, Arc::new(DefaultDialect {}))
                            .with_substrait(true);
                    let provider = factory
                        .table_provider(TableReference::bare("remote_lookups"))
                        .await
                        .map_err(anyhow::Error::from_boxed)?;
                    let plan =
                        LogicalPlanBuilder::scan("lookups", provider_as_source(provider), None)?
                            .filter(col("id").eq(lit(id)))?
                            .project(vec![col("label")])?
                            .build()?;
                    let batches: Vec<RecordBatch> =
                        QueryBuilder::from_plan(plan, format!("lookup {id}"), df)
                            .build()
                            .run()
                            .await?
                            .into_record_batch_stream()
                            .try_collect()
                            .await?;
                    Ok::<_, anyhow::Error>(pretty_format_batches(&batches)?.to_string())
                }
            };
            let label =
                |label: &str| format!("+-------+\n| label |\n+-------+\n| {label} |\n+-------+");
            assert_eq!(lookup(first, 1).await?, label("a-one"));
            assert_eq!(lookup(second, 2).await?, label("b-two"));

            sidecar.shutdown().await;
            Ok(())
        })
        .await
}

/// A probe is bounded as a whole: an upstream that accepts the credential
/// exchange and never answers it is found not to take parameterized plans
/// within the probe timeout, and asked again once the failed-probe interval
/// passes.
#[tokio::test]
async fn a_probe_whose_handshake_stalls_fails_within_its_timeout() -> anyhow::Result<()> {
    test_request_context().scope(a_stalled_probe()).await
}

async fn a_stalled_probe() -> anyhow::Result<()> {
    use ticket_proxy::Mode;
    let (upstream, _) = lookup_upstream().await?;
    let (endpoint, counts) = ticket_proxy::start(&upstream, Mode::StallHandshake).await?;
    data_components::flight::set_parameterized_probe_intervals_for_tests(
        data_components::flight::ParameterizedProbeIntervals {
            answered: Duration::from_millis(300),
            failed: Duration::from_millis(300),
            timeout: Duration::from_millis(500),
        },
    );
    let credentials = Credentials::new("", SecretString::from(API_KEY));
    let client = FlightClient::try_new(endpoint.into(), credentials, None, None).await?;

    let probe = tokio::time::timeout(
        Duration::from_secs(5),
        data_components::flight::probe_takes_parameterized_plans_for_tests(&client),
    )
    .await
    .map_err(|_| anyhow::anyhow!("the probe outlived its timeout"))?;
    assert!(!probe, "a stalled probe is a failed one");

    counts.set_mode(Mode::Current);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while !data_components::flight::probe_takes_parameterized_plans_for_tests(&client).await {
        assert!(
            tokio::time::Instant::now() < deadline,
            "a failed probe was never retried"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    Ok(())
}

/// Concurrent lookups bound to one cached template each return their own key's
/// row: no execution shares another's values or node state.
#[tokio::test]
async fn concurrent_lookups_bound_to_one_template_keep_their_own_values() -> anyhow::Result<()> {
    test_request_context()
        .scope(async {
            let auth = Arc::new(ApiKeyAuth::new(vec![ApiKey::parse_str(API_KEY)]))
                as Arc<dyn FlightBasicAuth + Send + Sync>;
            let (_channel, upstream, metrics_port) =
                start_spice_test_app_with_metrics_port(Some(auth), None, None, None).await?;
            let ids: Vec<i64> = (1..=64).collect();
            let input = RecordBatch::try_from_iter(vec![
                (
                    "id",
                    Arc::new(Int64Array::from(ids.clone())) as arrow::array::ArrayRef,
                ),
                (
                    "label",
                    Arc::new(StringArray::from(
                        ids.iter()
                            .map(|id| format!("label-{id}"))
                            .collect::<Vec<_>>(),
                    )),
                ),
            ])?;
            upstream
                .ctx
                .register_batch("remote_lookups", input.clone())?;
            let endpoint = format!("http://127.0.0.1:{}", metrics_port - 1);
            let (sidecar, _) = lookup_sidecar(&[("lookups", &endpoint)], &input).await?;
            let stats = || sidecar.datafusion().federated_template_stats();

            let lookup = |id: i64| {
                let sidecar = Arc::clone(&sidecar);
                test_request_context().scope(async move {
                    let batches = rows(
                        &sidecar,
                        &format!("SELECT label FROM lookups WHERE id = {id}"),
                    )
                    .await?;
                    let mut labels = Vec::new();
                    for batch in &batches {
                        for row in 0..batch.num_rows() {
                            labels.push(arrow::util::display::array_value_to_string(
                                batch.column(0),
                                row,
                            )?);
                        }
                    }
                    Ok::<_, anyhow::Error>((id, labels))
                })
            };
            lookup(1).await?;
            let before = stats();
            let answers =
                futures::future::try_join_all((2..=17).map(|id| tokio::spawn(lookup(id)))).await?;
            for answer in answers {
                let (id, labels) = answer?;
                assert_eq!(labels, vec![format!("label-{id}")], "id {id}");
            }
            let after = stats();
            assert_eq!(
                (after.planned - before.planned, after.reused - before.reused),
                (0, 16),
                "every lookup bound the one template"
            );

            sidecar.shutdown().await;
            Ok(())
        })
        .await
}
