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

//! `vector_search(tbl, query, k)` returns the `k` best rows by `_score`, best first, on the
//! chunked and multi-vector paths that have no vector index (regression test for #14819).
//!
//! Embeddings come from a local OpenAI-compatible stub whose vectors are a fixed function of the
//! text, so the expected ranking is computed here, independently of the runtime.

use std::{collections::HashMap, fmt::Write as _, net::SocketAddr, sync::Arc};

use anyhow::Context as _;
use app::AppBuilder;
use arrow::array::{Array, Float64Array, Int64Array, ListBuilder, RecordBatch, StringBuilder};
use arrow_schema::{DataType, Field, Schema};
use axum::{Json, Router, routing::post};
use datafusion::parquet::arrow::ArrowWriter;
use runtime::Runtime;
use serde_json::{Value, json};
use spicepod::component::dataset::Dataset;
use spicepod::component::embeddings::{EmbeddingChunkConfig, Embeddings};
use spicepod::param::Params;
use spicepod::semantic::{Column, ColumnLevelEmbeddingConfig};

use crate::utils::{
    register_test_connectors, run_query, runtime_ready_check, test_request_context,
};
use crate::{configure_test_datafusion, init_tracing};

const MODEL: &str = "stub";
const QUERY: &str = "HTTP/2";

/// The stub's embedding of `text`: one dimension per keyword, plus a length-derived component
/// that breaks ties between texts sharing the same keywords.
fn stub_embedding(text: &str) -> Vec<f32> {
    let has = |needle: &str| {
        if text.contains(needle) { 1.0 } else { 0.0 }
    };
    #[expect(clippy::cast_precision_loss, reason = "text length is small")]
    let len = (text.chars().count() % 13) as f32 / 13.0;
    vec![has("HTTP/2"), has("HTTP"), has("proxy"), len, 0.1]
}

fn cosine(a: &[f32], b: &[f32]) -> f64 {
    let dot: f64 = a
        .iter()
        .zip(b)
        .map(|(x, y)| f64::from(*x) * f64::from(*y))
        .sum();
    let norm = |v: &[f32]| v.iter().map(|x| f64::from(*x).powi(2)).sum::<f64>().sqrt();
    dot / (norm(a) * norm(b))
}

async fn embeddings_handler(Json(body): Json<Value>) -> Json<Value> {
    let inputs: Vec<String> = match &body["input"] {
        Value::String(s) => vec![s.clone()],
        Value::Array(items) => items
            .iter()
            .map(|v| v.as_str().unwrap_or_default().to_string())
            .collect(),
        _ => Vec::new(),
    };
    let data: Vec<Value> = inputs
        .iter()
        .enumerate()
        .map(|(i, t)| json!({"object": "embedding", "index": i, "embedding": stub_embedding(t)}))
        .collect();
    Json(json!({
        "object": "list",
        "model": body["model"],
        "data": data,
        "usage": {"prompt_tokens": 1, "total_tokens": 1}
    }))
}

async fn start_stub() -> Result<SocketAddr, anyhow::Error> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let router = Router::new().route("/v1/embeddings", post(embeddings_handler));
    tokio::spawn(async move { axum::serve(listener, router).await });
    Ok(addr)
}

/// Two files, so the scan has two partitions.
const DOCS: [&[(i64, &str)]; 2] = [
    &[
        (1, "HTTP/2 stream reset handling"),
        (2, "TLS certificate rotation"),
        (3, "Upstream keepalive connections"),
        (4, "Gzip compression levels"),
        (5, "Rate limiting by client address"),
    ],
    &[
        (6, "HTTP/2 server push removal"),
        (7, "QUIC and HTTP/3 support"),
        (8, "Access log buffering"),
        (9, "Proxy cache key design for the proxy"),
        (10, "WebSocket proxying over HTTP"),
    ],
];

/// Write [`DOCS`] twice: as CSV files for the `body` text column, and as Parquet files whose
/// `tags` list column holds each body split into words, for the multi-vector cases.
fn write_datasets(dir: &std::path::Path) -> Result<(), anyhow::Error> {
    let chunked = dir.join("chunked");
    let multi = dir.join("multi");
    std::fs::create_dir_all(&chunked)?;
    std::fs::create_dir_all(&multi)?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(
            "tags",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            true,
        ),
    ]));
    for (part, rows) in DOCS.iter().enumerate() {
        let mut csv = String::from("id,body\n");
        for (id, body) in *rows {
            writeln!(csv, "{id},{body}")?;
        }
        std::fs::write(chunked.join(format!("part{part}.csv")), csv)?;

        let mut tags = ListBuilder::new(StringBuilder::new());
        for (_, body) in *rows {
            for word in body.split(' ') {
                tags.values().append_value(word);
            }
            tags.append(true);
        }
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from_iter_values(rows.iter().map(|(id, _)| *id))),
                Arc::new(tags.finish()),
            ],
        )?;
        let file = std::fs::File::create(multi.join(format!("part{part}.parquet")))?;
        let mut writer = ArrowWriter::try_new(file, Arc::clone(&schema), None)?;
        writer.write(&batch)?;
        writer.close()?;
    }
    Ok(())
}

fn dataset(from: String, name: &str, format: &str, column: &str, chunked: bool) -> Dataset {
    let mut ds = Dataset::new(from, name);
    ds.params = Some(Params::from_string_map(HashMap::from([(
        "file_format".to_string(),
        format.to_string(),
    )])));
    ds.columns = vec![Column {
        name: column.to_string(),
        embeddings: vec![ColumnLevelEmbeddingConfig {
            model: MODEL.to_string(),
            row_ids: Some(vec!["id".to_string()]),
            chunking: chunked.then(|| EmbeddingChunkConfig::enabled().target_chunk_size(256)),
            vector_size: None,
            engine: None,
            params: None,
            aggregation: None,
            max_elements_per_row: None,
        }],
        description: None,
        full_text_search: None,
        r#type: None,
        nullable: None,
        metadata: HashMap::new(),
    }];
    ds
}

/// `(id, _score)` for every row, in the order the query returned them.
async fn ids_and_scores(rt: &Arc<Runtime>, sql: &str) -> Result<Vec<(i64, f64)>, anyhow::Error> {
    let mut rows = Vec::new();
    for batch in run_query(rt, sql)
        .await
        .with_context(|| format!("query failed: {sql}"))?
    {
        let ids = batch
            .column_by_name("id")
            .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
            .context("id column")?;
        let scores = batch
            .column_by_name("_score")
            .and_then(|c| c.as_any().downcast_ref::<Float64Array>())
            .context("_score column")?;
        for i in 0..batch.num_rows() {
            rows.push((ids.value(i), scores.value(i)));
        }
    }
    Ok(rows)
}

/// The `k` best documents for the query, scoring each with `score` over the stub's vectors.
/// Asserts that the `k`-th and `k+1`-th scores are apart, so which rows make the cut is not
/// decided by a tie or by float rounding.
fn expected_top_k(k: usize, score: impl Fn(&str) -> f64) -> Vec<(i64, f64)> {
    let mut scored: Vec<(i64, f64)> = DOCS
        .iter()
        .flat_map(|rows| rows.iter())
        .map(|(id, body)| (*id, score(body)))
        .collect();
    scored.sort_by(|a, b| b.1.total_cmp(&a.1));
    assert!(
        scored[k - 1].1 - scored[k].1 > 1e-3,
        "fixture must separate rank {k} from rank {}: {scored:?}",
        k + 1
    );
    scored.truncate(k);
    scored
}

/// `got` holds exactly the ids of `want`, in descending `_score` order. Rows that tie on score
/// may come back in either order. Only the ranking is compared: the runtime's `_score` scale is
/// its own (`1 - cosine_distance`), while `want` scores by plain cosine similarity.
fn assert_top_k(got: &[(i64, f64)], want: &[(i64, f64)], case: &str) {
    assert!(
        got.windows(2).all(|w| w[0].1 >= w[1].1),
        "{case}: rows are not in descending _score order: {got:?}"
    );
    let mut got_ids: Vec<i64> = got.iter().map(|(id, _)| *id).collect();
    let mut want_ids: Vec<i64> = want.iter().map(|(id, _)| *id).collect();
    got_ids.sort_unstable();
    want_ids.sort_unstable();
    assert_eq!(
        got_ids, want_ids,
        "{case}: expected the rows {want:?}, got {got:?}"
    );
}

/// Start a runtime over the [`DOCS`] datasets, embedded by the local stub, and run `check`
/// against it inside a request context.
async fn with_runtime(
    check: impl AsyncFnOnce(Arc<Runtime>) -> Result<(), anyhow::Error>,
) -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(None);
    test_request_context()
        .scope(async {
            let stub = start_stub().await?;
            let dir = tempfile::tempdir()?;
            write_datasets(dir.path())?;

            let mut embedding = Embeddings::new(format!("openai:{MODEL}"), MODEL);
            embedding.params = HashMap::from([
                ("endpoint".to_string(), format!("http://{stub}/v1").into()),
                (
                    "openai_api_key".to_string(),
                    "sk-not-a-real-key".to_string().into(),
                ),
            ]);
            let chunked = format!("file:{}/", dir.path().join("chunked").display());
            let multi = format!("file:{}/", dir.path().join("multi").display());
            let app = AppBuilder::new("vector_search_top_k")
                .with_embedding(embedding)
                .with_dataset(dataset(chunked.clone(), "docs", "csv", "body", true))
                .with_dataset(dataset(chunked, "plain", "csv", "body", false))
                .with_dataset(dataset(multi, "tagged", "parquet", "tags", false))
                .build();

            register_test_connectors().await;
            configure_test_datafusion();
            let rt = Arc::new(Runtime::builder().with_app(app).build().await);
            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_mins(2)) => {
                    anyhow::bail!("Timed out waiting for components to load");
                }
                () = Arc::clone(&rt).load_components() => {}
            }
            runtime_ready_check(&rt).await;

            check(rt).await
        })
        .await
}

fn words(body: &str) -> Vec<Vec<f32>> {
    body.split(' ').map(stub_embedding).collect()
}

/// A chunked scalar column. Each body is a single chunk, so its score is the body's.
#[tokio::test(flavor = "multi_thread")]
async fn vector_search_returns_top_k_on_a_chunked_column() -> Result<(), anyhow::Error> {
    with_runtime(async |rt| {
        let q = stub_embedding(QUERY);
        let want = expected_top_k(2, |body| cosine(&q, &stub_embedding(body)));
        let got = ids_and_scores(
            &rt,
            &format!("SELECT id, _score FROM vector_search(docs, '{QUERY}', 2)"),
        )
        .await?;
        assert_top_k(&got, &want, "chunked");

        // `k` counts the rows a filter on a base column keeps, as on the unchunked path.
        let want = expected_top_k(1, |body| {
            if body.starts_with("HTTP/2") {
                f64::MIN
            } else {
                cosine(&q, &stub_embedding(body))
            }
        });
        let got = ids_and_scores(
            &rt,
            &format!(
                "SELECT id, _score FROM vector_search(docs, '{QUERY}', 1) \
                 WHERE body NOT LIKE 'HTTP/2%'"
            ),
        )
        .await?;
        assert_top_k(&got, &want, "chunked with a filter");

        // A filter on a column the search adds, such as the matched chunk `_value`, applies to
        // the `k` rows the search returns, so it can only drop rows from them.
        let top = expected_top_k(2, |body| cosine(&q, &stub_embedding(body)));
        let got = ids_and_scores(
            &rt,
            &format!(
                "SELECT id, _score FROM vector_search(docs, '{QUERY}', 2) \
                 WHERE _value NOT LIKE 'HTTP/2%'"
            ),
        )
        .await?;
        let top_ids: Vec<i64> = top.iter().map(|(id, _)| *id).collect();
        assert!(
            got.iter().all(|(id, _)| top_ids.contains(id)),
            "chunked with a `_value` filter: expected a subset of the top k {top:?}, got {got:?}"
        );

        // An enclosing operator that needs no order still sees only `k` rows, chunked or not
        // (the unchunked shape is #14818).
        for table in ["docs", "plain"] {
            let counted = run_query(
                &rt,
                &format!("SELECT COUNT(*) AS c FROM vector_search({table}, '{QUERY}', 2)"),
            )
            .await?;
            let c = counted[0]
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .context("count column")?
                .value(0);
            assert_eq!(c, 2, "{table}: COUNT(*) over k = 2");
        }
        Ok(())
    })
    .await
}

/// A multi-vector column, scored by the default `max` aggregation over its elements.
#[tokio::test(flavor = "multi_thread")]
async fn vector_search_returns_top_k_on_a_multi_vector_column() -> Result<(), anyhow::Error> {
    with_runtime(async |rt| {
        let q = stub_embedding(QUERY);
        let want = expected_top_k(3, |body| {
            words(body)
                .iter()
                .map(|w| cosine(&q, w))
                .fold(f64::MIN, f64::max)
        });
        let got = ids_and_scores(
            &rt,
            &format!("SELECT id, _score FROM vector_search(tagged, '{QUERY}', 3)"),
        )
        .await?;
        assert_top_k(&got, &want, "multi-vector");
        Ok(())
    })
    .await
}

/// Multi-query late interaction: the sum over queries of each query's best element score.
#[tokio::test(flavor = "multi_thread")]
async fn vector_search_returns_top_k_for_late_interaction() -> Result<(), anyhow::Error> {
    with_runtime(async |rt| {
        let queries = ["HTTP/2", "proxy"];
        let want = expected_top_k(4, |body| {
            let ws = words(body);
            queries
                .iter()
                .map(|qs| {
                    let qv = stub_embedding(qs);
                    ws.iter().map(|w| cosine(&qv, w)).fold(f64::MIN, f64::max)
                })
                .sum()
        });
        let got = ids_and_scores(
            &rt,
            "SELECT id, _score FROM vector_search(tagged, ['HTTP/2', 'proxy'], tags, 4)",
        )
        .await?;
        assert_top_k(&got, &want, "late interaction");
        Ok(())
    })
    .await
}
