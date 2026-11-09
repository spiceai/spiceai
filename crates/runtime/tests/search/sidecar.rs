/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! Search a sidecar delegates to the upstream runtime that holds a dataset's
//! embeddings and full-text index. The upstream answering the same query over
//! its own index is the oracle.

use std::{
    collections::HashMap,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::Path,
    sync::Arc,
    time::Duration,
};

use app::AppBuilder;
use arrow::{array::RecordBatch, util::pretty::pretty_format_batches};
use futures::TryStreamExt;
use runtime::{
    Runtime,
    auth::EndpointAuth,
    config::{ClusterConfig, Config, Role},
    datafusion::query::QueryBuilder,
};
use runtime_auth::{FlightBasicAuth, api_key::ApiKeyAuth};
use spicepod::{
    component::{dataset::Dataset, runtime::ApiKey},
    param::Params,
};

use crate::{
    configure_test_datafusion,
    utils::{register_test_connectors, runtime_ready_check, test_request_context},
};

/// The upstream's only API key. The sidecar's dataset passes it explicitly, so
/// the connector never autoloads `spiceai_api_key` from the environment or a
/// `.env` file, where CI keeps a Spice Cloud key the upstream would reject.
const API_KEY: &str = "sidecar-search";

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

/// A `--role sidecar` runtime serving `dataset`, loaded.
async fn start_sidecar(name: &str, dataset: Dataset) -> anyhow::Result<Arc<Runtime>> {
    let sidecar = Arc::new(
        Runtime::builder()
            .with_app(AppBuilder::new(name).with_dataset(dataset).build())
            .with_runtime_config(Config {
                cluster: ClusterConfig::default().with_role(Role::Sidecar),
                ..Config::default()
            })
            .build()
            .await,
    );
    tokio::time::timeout(
        Duration::from_secs(60),
        Arc::clone(&sidecar).load_components(),
    )
    .await?;
    Ok(sidecar)
}

/// An upstream serving `docs` with model2vec embeddings and a full-text index
/// on `label`, over Flight on the returned port and behind [`API_KEY`].
async fn start_search_upstream(dir: &Path) -> anyhow::Result<(Arc<Runtime>, u16)> {
    let csv = dir.join("docs.csv");
    std::fs::write(
        &csv,
        "id,value,label\n1,11,snow on the mountain\n2,,rain in the valley\n3,-7,snowfall at night\n4,0,sunny beach\n5,3,fresh snow and ice\n",
    )?;
    std::fs::write(
        dir.join("spicepod.yaml"),
        format!(
            "version: v1
kind: Spicepod
name: search_upstream
embeddings:
  - from: model2vec:minishlab/potion-base-2M
    name: text_embed
datasets:
  - from: file:{csv}
    name: docs
    acceleration: {{ enabled: true, engine: arrow }}
    columns:
      - name: label
        embeddings:
          - from: text_embed
        full_text_search:
          enabled: true
          row_id: [id]
  - from: file:{csv}
    name: notes
    acceleration: {{ enabled: true, engine: arrow }}
    columns:
      - name: label
        full_text_search:
          enabled: true
          row_id: [id]
  - from: file:{csv}
    name: upstream_only_secret
",
            csv = csv.display()
        ),
    )?;
    register_test_connectors().await;
    configure_test_datafusion();
    let upstream = Arc::new(
        Runtime::builder()
            .with_app(AppBuilder::build_from_path(dir.to_path_buf()).await?)
            .build()
            .await,
    );
    tokio::time::timeout(
        Duration::from_mins(3),
        Arc::clone(&upstream).load_components(),
    )
    .await?;
    runtime_ready_check(&upstream).await;

    let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    let flight_port = listener.local_addr()?.port();
    let http_listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    let http_port = http_listener.local_addr()?.port();
    drop((listener, http_listener));
    let localhost = IpAddr::V4(Ipv4Addr::LOCALHOST);
    let config = Config::new()
        .with_http_bind_address(SocketAddr::new(localhost, http_port))
        .with_flight_bind_address(SocketAddr::new(localhost, flight_port));
    let auth = Arc::new(ApiKeyAuth::new(vec![ApiKey::parse_str(API_KEY)]))
        as Arc<dyn FlightBasicAuth + Send + Sync>;
    let server = Arc::clone(&upstream);
    tokio::spawn(async move {
        Box::pin(server.start_servers(
            config,
            None,
            EndpointAuth::default().with_flight_basic_auth(auth),
        ))
        .await
    });
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    while tokio::net::TcpStream::connect((localhost, flight_port))
        .await
        .is_err()
    {
        anyhow::ensure!(
            std::time::Instant::now() < deadline,
            "the upstream Flight server did not listen on {flight_port} within 30 s"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    Ok((upstream, flight_port))
}

/// A sidecar's Spice dataset has no index of its own. `vector_search`,
/// `text_search` and `rrf` over it return exactly what the upstream returns
/// searching its own embeddings and full-text index, with filters, limits and
/// projections run there. Plain reads of the dataset, whose upstream schema has
/// an embedding column, return the upstream's rows too.
#[tokio::test]
async fn sidecar_delegates_search_to_the_upstream_index() -> anyhow::Result<()> {
    test_request_context()
        .scope(async {
            let dir = tempfile::tempdir()?;
            let (upstream, flight_port) = start_search_upstream(dir.path()).await?;
            let mut dataset = Dataset::new("spice.ai:docs", "docs".to_string());
            dataset.params = Some(Params::from_string_map(HashMap::from([
                (
                    "spiceai_endpoint".to_string(),
                    format!("http://127.0.0.1:{flight_port}"),
                ),
                ("spiceai_api_key".to_string(), API_KEY.to_string()),
            ])));
            let sidecar = start_sidecar("search_sidecar", dataset).await?;

            for sql in [
                "SELECT id, label, _score FROM vector_search(docs, 'snow') ORDER BY _score DESC, id",
                "SELECT id, label, _score FROM text_search(docs, 'snow') ORDER BY _score DESC, id",
                "SELECT id, label FROM vector_search(docs, 'snow') WHERE value >= 0 ORDER BY id",
                "SELECT id, label FROM vector_search(docs, 'snow', label, 2) ORDER BY id",
                "SELECT * FROM vector_search(docs, 'snow', label, 2, false) ORDER BY id",
                "SELECT label FROM text_search(docs, 'snow', label) WHERE id > 1 ORDER BY label",
                "SELECT id, label FROM rrf(vector_search(docs, 'snow'), text_search(docs, 'snow', label), join_key => 'id') ORDER BY _fused_score DESC, id",
                "SELECT s.id, d.value FROM vector_search(docs, 'snow') s JOIN docs d ON s.id = d.id ORDER BY s.id",
                // Plain reads of a dataset with an embedding column: the read
                // names only the columns it uses, never the embedding.
                "SELECT id, value FROM docs ORDER BY id",
                "SELECT COUNT(*) AS n, SUM(value) AS total FROM docs",
                "SELECT label FROM docs WHERE value > 0 ORDER BY label",
            ] {
                let expected = rows(&upstream, sql).await?;
                assert!(
                    expected.iter().map(RecordBatch::num_rows).sum::<usize>() > 0,
                    "the upstream answers {sql} with rows"
                );
                let actual = rows(&sidecar, sql).await?;
                assert_same_rows(&actual, &expected, sql)?;
            }

            // A filter's subquery is answered by the sidecar's own catalog, never
            // sent upstream inside the search, so the application cannot probe
            // for an upstream table the sidecar does not declare.
            for table in ["upstream_only_secret", "docs"] {
                let probe = format!(
                    "SELECT s.id FROM (SELECT d.id, EXISTS (SELECT 1 FROM information_schema.tables \
                     WHERE table_name = '{table}') AS e FROM text_search(docs, 'snow') d) s WHERE s.e"
                );
                let found = rows(&sidecar, &probe).await?;
                let local = rows(
                    &sidecar,
                    &format!("SELECT 1 FROM information_schema.tables WHERE table_name = '{table}'"),
                )
                .await?;
                assert_eq!(
                    found.iter().map(RecordBatch::num_rows).sum::<usize>() > 0,
                    local.iter().map(RecordBatch::num_rows).sum::<usize>() > 0,
                    "{probe}"
                );
            }

            // The search ran on the upstream: the sidecar plans a Substrait
            // request, with the filter inside it rather than in the sidecar.
            let explain = pretty_format_batches(
                &rows(
                    &sidecar,
                    "EXPLAIN SELECT id FROM vector_search(docs, 'snow') WHERE value >= 0",
                )
                .await?,
            )?
            .to_string();
            assert!(explain.contains("SubstraitExec"), "{explain}");
            assert!(!explain.contains("FilterExec"), "{explain}");

            sidecar.shutdown().await;
            upstream.shutdown().await;
            Ok(())
        })
        .await
}

/// A sidecar's accelerated dataset holds the rows its `refresh_sql` selects. A
/// search is not delegated past it to the whole upstream table, which would
/// return rows the dataset does not hold; with no local index it is refused.
#[tokio::test]
async fn sidecar_does_not_delegate_search_past_an_accelerated_working_set() -> anyhow::Result<()> {
    test_request_context()
        .scope(async {
            let dir = tempfile::tempdir()?;
            let (upstream, flight_port) = start_search_upstream(dir.path()).await?;
            let mut dataset = Dataset::new("spice.ai:notes", "notes".to_string());
            dataset.params = Some(Params::from_string_map(HashMap::from([
                (
                    "spiceai_endpoint".to_string(),
                    format!("http://127.0.0.1:{flight_port}"),
                ),
                ("spiceai_api_key".to_string(), API_KEY.to_string()),
            ])));
            dataset.acceleration = Some(spicepod::acceleration::Acceleration {
                enabled: true,
                engine: Some("cayenne".to_string()),
                mode: spicepod::acceleration::Mode::Memory,
                refresh_sql: Some("SELECT * FROM notes WHERE id = 1".to_string()),
                ..Default::default()
            });
            let sidecar = start_sidecar("scoped_sidecar", dataset).await?;

            let deadline = std::time::Instant::now() + Duration::from_secs(60);
            let held = loop {
                match rows(&sidecar, "SELECT id FROM notes ORDER BY id").await {
                    Ok(held) => break held,
                    Err(error) => {
                        anyhow::ensure!(
                            std::time::Instant::now() < deadline,
                            "the scoped acceleration did not load within 60 s: {error}"
                        );
                        tokio::time::sleep(Duration::from_millis(250)).await;
                    }
                }
            };
            assert_eq!(
                pretty_format_batches(&held)?.to_string(),
                "+----+\n| id |\n+----+\n| 1  |\n+----+"
            );
            let upstream_rows =
                rows(&upstream, "SELECT id FROM text_search(notes, 'snow')").await?;
            assert!(
                upstream_rows
                    .iter()
                    .map(RecordBatch::num_rows)
                    .sum::<usize>()
                    > 0,
                "the upstream answers the search"
            );
            let error = rows(&sidecar, "SELECT id FROM text_search(notes, 'snow')")
                .await
                .expect_err("a search past the working set is refused");
            assert!(
                error
                    .to_string()
                    .contains("does not have a full text search index"),
                "{error}"
            );

            sidecar.shutdown().await;
            upstream.shutdown().await;
            Ok(())
        })
        .await
}
