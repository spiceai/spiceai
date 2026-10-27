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

//! A dataset with column embeddings sends only the rows a query keeps to the embedding model,
//! when the source cannot apply the query's filter or `refresh_data_window` exactly (regression
//! test for #15632).
//!
//! The source is a Parquet file in one row group. Parquet uses a filter only to skip row groups,
//! so it reports the filter as inexact (as the GitHub search API does) and returns every row.
//! The embeddings come from a local OpenAI-compatible stub that records every text it embeds;
//! the test compares those texts with the rows the query returned, and checks each row's vector
//! is the one the stub computed for its text.

use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{Arc, Mutex},
};

use anyhow::Context as _;
use app::AppBuilder;
use arrow::array::{
    Array, FixedSizeListArray, Float32Array, Int64Array, RecordBatch, StringArray,
    TimestampMicrosecondArray,
};
use arrow::compute::cast;
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use axum::{Json, Router, extract::State, routing::post};
use datafusion::parquet::arrow::ArrowWriter;
use runtime::Runtime;
use serde_json::{Value, json};
use spicepod::acceleration::Acceleration;
use spicepod::component::dataset::Dataset;
use spicepod::component::embeddings::Embeddings;
use spicepod::semantic::{Column, ColumnLevelEmbeddingConfig};

use crate::utils::{
    register_test_connectors, run_query, runtime_ready_check, test_request_context,
};
use crate::{configure_test_datafusion, init_tracing};

const MODEL: &str = "stub";
const ROWS: i64 = 1000;
/// Minutes between consecutive rows' `created_at`.
const SPACING_MINUTES: i64 = 144;

/// The source rows: every even `id` is `CLOSED`.
fn state(id: i64) -> &'static str {
    if id % 2 == 0 { "CLOSED" } else { "OPEN" }
}

fn body(id: i64) -> String {
    format!("issue {id}")
}

/// How old row `id` is: the last row is an hour old and each earlier row [`SPACING_MINUTES`]
/// older, so 1000 rows span about 100 days. A 7-day window boundary falls an hour or more away
/// from any row, so the rows inside it do not depend on how long the test takes to start.
fn age_minutes(id: i64) -> i64 {
    (ROWS - 1 - id) * SPACING_MINUTES + 60
}

/// The stub's vector for `text`: the issue id it names, so a vector attached to the wrong row
/// is detected. Any other text (such as a probe for the model's dimension) maps to `-1`.
fn stub_embedding(text: &str) -> Vec<f32> {
    let id = text
        .strip_prefix("issue ")
        .and_then(|id| id.parse::<f32>().ok())
        .unwrap_or(-1.0);
    vec![id, 1.0]
}

type Embedded = Arc<Mutex<Vec<String>>>;

async fn embeddings_handler(
    State(embedded): State<Embedded>,
    Json(body): Json<Value>,
) -> Json<Value> {
    let inputs: Vec<String> = match &body["input"] {
        Value::String(s) => vec![s.clone()],
        Value::Array(items) => items
            .iter()
            .map(|v| v.as_str().unwrap_or_default().to_string())
            .collect(),
        _ => Vec::new(),
    };
    embedded
        .lock()
        .expect("embedded texts lock")
        .extend(inputs.iter().cloned());
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

async fn start_stub(embedded: Embedded) -> Result<SocketAddr, anyhow::Error> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let router = Router::new()
        .route("/v1/embeddings", post(embeddings_handler))
        .with_state(embedded);
    tokio::spawn(async move { axum::serve(listener, router).await });
    Ok(addr)
}

/// One Parquet file with one row group, so the scan has one partition and Parquet can skip no
/// rows for a filter on `state` or `created_at`.
fn write_issues(path: &std::path::Path) -> Result<(), anyhow::Error> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("state", DataType::Utf8, false),
        Field::new("body", DataType::Utf8, false),
        Field::new(
            "created_at",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
    ]));
    let now = chrono::Utc::now().timestamp_micros();
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from_iter_values(0..ROWS)),
            Arc::new(StringArray::from_iter_values((0..ROWS).map(state))),
            Arc::new(StringArray::from_iter_values((0..ROWS).map(body))),
            Arc::new(TimestampMicrosecondArray::from_iter_values(
                (0..ROWS).map(|id| now - age_minutes(id) * 60_000_000),
            )),
        ],
    )?;
    let mut writer = ArrowWriter::try_new(std::fs::File::create(path)?, schema, None)?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}

fn cayenne() -> Acceleration {
    Acceleration {
        enabled: true,
        engine: Some("cayenne".to_string()),
        ..Acceleration::default()
    }
}

/// Start a runtime over the issues file as dataset `issues`, embedded by a local stub and then
/// adjusted by `configure`, and run `check` against it. Returns what `check` returned and the
/// issue texts the stub embedded, in sorted order.
async fn with_runtime<T>(
    configure: impl FnOnce(&mut Dataset),
    check: impl AsyncFnOnce(Arc<Runtime>) -> Result<T, anyhow::Error>,
) -> Result<(T, Vec<String>), anyhow::Error> {
    let _tracing = init_tracing(None);
    test_request_context()
        .scope(async {
            let embedded = Embedded::default();
            let stub = start_stub(Arc::clone(&embedded)).await?;
            let dir = tempfile::tempdir()?;
            let file = dir.path().join("issues.parquet");
            write_issues(&file)?;

            let mut embedding = Embeddings::new(format!("openai:{MODEL}"), MODEL);
            embedding.params = HashMap::from([
                ("endpoint".to_string(), format!("http://{stub}/v1").into()),
                (
                    "openai_api_key".to_string(),
                    "sk-not-a-real-key".to_string().into(),
                ),
            ]);
            let mut dataset = Dataset::new(format!("file:{}", file.display()), "issues");
            dataset.columns = vec![
                Column::new("body")
                    .with_embedding(ColumnLevelEmbeddingConfig::model(MODEL).with_row_id("id")),
            ];
            configure(&mut dataset);
            let app = AppBuilder::new("embedding_filter_limit")
                .with_embedding(embedding)
                .with_dataset(dataset)
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

            let checked = check(rt).await?;

            let mut issues: Vec<String> = embedded
                .lock()
                .expect("embedded texts lock")
                .iter()
                .filter(|text| text.starts_with("issue "))
                .cloned()
                .collect();
            issues.sort_unstable();
            Ok((checked, issues))
        })
        .await
}

/// `(id, body)` of every row `sql` returns, in order. When the result has a `body_embedding`
/// column, asserts each row's vector is the stub's vector for its body.
async fn rows(rt: &Arc<Runtime>, sql: &str) -> Result<Vec<(i64, String)>, anyhow::Error> {
    let mut rows = Vec::new();
    for batch in run_query(rt, sql)
        .await
        .with_context(|| format!("query failed: {sql}"))?
    {
        let ids = batch
            .column_by_name("id")
            .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
            .context("id column")?;
        let bodies = batch
            .column_by_name("body")
            .map(|c| cast(c, &DataType::Utf8))
            .transpose()?
            .context("body column")?;
        let bodies = bodies
            .as_any()
            .downcast_ref::<StringArray>()
            .context("body column as Utf8")?;
        let vectors = batch
            .column_by_name("body_embedding")
            .map(|c| {
                c.as_any()
                    .downcast_ref::<FixedSizeListArray>()
                    .context("body_embedding column")
            })
            .transpose()?;
        for i in 0..batch.num_rows() {
            let body = bodies.value(i).to_string();
            if let Some(vectors) = vectors {
                let vector = vectors.value(i);
                let vector = vector
                    .as_any()
                    .downcast_ref::<Float32Array>()
                    .context("body_embedding values")?;
                assert_eq!(
                    vector.values().to_vec(),
                    stub_embedding(&body),
                    "{sql}: row {} carries another row's embedding",
                    ids.value(i)
                );
            }
            rows.push((ids.value(i), body));
        }
    }
    Ok(rows)
}

fn sorted_bodies(rows: &[(i64, String)]) -> Vec<String> {
    let mut bodies: Vec<String> = rows.iter().map(|(_, body)| body.clone()).collect();
    bodies.sort_unstable();
    bodies
}

/// `refresh_sql` with a filter and no `LIMIT`: the rows the filter removes are not embedded.
#[tokio::test(flavor = "multi_thread")]
async fn refresh_sql_filter_embeds_only_the_rows_kept() -> Result<(), anyhow::Error> {
    let (kept, embedded) = with_runtime(
        |ds| {
            ds.acceleration = Some(Acceleration {
                refresh_sql: Some("SELECT * FROM issues WHERE state = 'CLOSED'".to_string()),
                ..cayenne()
            });
        },
        async |rt| {
            rows(
                &rt,
                "SELECT id, body, body_embedding FROM issues ORDER BY id",
            )
            .await
        },
    )
    .await?;

    let want: Vec<(i64, String)> = (0..ROWS)
        .filter(|id| state(*id) == "CLOSED")
        .map(|id| (id, body(id)))
        .collect();
    assert_eq!(kept, want, "the refresh must keep exactly the CLOSED rows");
    assert_eq!(
        embedded,
        sorted_bodies(&want),
        "only the kept rows may be embedded"
    );
    Ok(())
}

/// `refresh_data_window` on the dataset's `time_column`: the refresh keeps the last 7 days and
/// embeds only those rows, not every row of the file.
#[tokio::test(flavor = "multi_thread")]
async fn refresh_data_window_embeds_only_the_rows_kept() -> Result<(), anyhow::Error> {
    let (kept, embedded) = with_runtime(
        |ds| {
            ds.time_column = Some("created_at".to_string());
            ds.acceleration = Some(Acceleration {
                refresh_data_window: Some("7d".to_string()),
                ..cayenne()
            });
        },
        async |rt| {
            rows(
                &rt,
                "SELECT id, body, body_embedding FROM issues ORDER BY id",
            )
            .await
        },
    )
    .await?;

    let want: Vec<(i64, String)> = (0..ROWS)
        .filter(|id| age_minutes(*id) < 7 * 24 * 60)
        .map(|id| (id, body(id)))
        .collect();
    assert_eq!(
        want.len(),
        70,
        "fixture must put 70 rows in the last 7 days"
    );
    assert_eq!(kept, want, "the refresh must keep exactly the last 7 days");
    assert_eq!(
        embedded,
        sorted_bodies(&want),
        "only the kept rows may be embedded"
    );
    Ok(())
}

/// A query against the source, without acceleration, returns exactly the rows that match its
/// filters — whether or not it reads the embedding column, and when a filter reads a column the
/// query does not return — and embeds only the rows it returns.
#[tokio::test(flavor = "multi_thread")]
async fn federated_filter_embeds_only_the_rows_returned() -> Result<(), anyhow::Error> {
    let want: Vec<(i64, String)> = [0, 2, 4, 6, 8]
        .into_iter()
        .map(|id| (id, body(id)))
        .collect();
    let ((), embedded) = with_runtime(|_| {}, async |rt| {
        let without_embeddings = rows(
            &rt,
            "SELECT id, body FROM issues WHERE state = 'CLOSED' AND id < 10 ORDER BY id",
        )
        .await?;
        assert_eq!(without_embeddings, want);

        let with_embeddings = rows(
            &rt,
            "SELECT id, body, body_embedding FROM issues WHERE state = 'CLOSED' AND id < 10 ORDER BY id",
        )
        .await?;
        assert_eq!(with_embeddings, want);

        let counts = run_query(&rt, "SELECT count(*) AS n FROM issues WHERE state = 'CLOSED'")
            .await?;
        let n = counts
            .first()
            .and_then(|b| b.column(0).as_any().downcast_ref::<Int64Array>())
            .map(|c| c.value(0))
            .context("count(*) result")?;
        assert_eq!(n, ROWS / 2);
        Ok(())
    })
    .await?;

    assert_eq!(
        embedded,
        sorted_bodies(&want),
        "only the rows returned with `body_embedding` may be embedded"
    );
    Ok(())
}

/// `refresh_sql` with a filter and a `LIMIT`: the refresh keeps 10 rows and embeds only those,
/// where it embedded every row of the source before the limit was reached.
#[tokio::test(flavor = "multi_thread")]
async fn refresh_sql_limit_embeds_only_the_rows_kept() -> Result<(), anyhow::Error> {
    let (kept, embedded) = with_runtime(
        |ds| {
            ds.acceleration = Some(Acceleration {
                refresh_sql: Some(
                    "SELECT * FROM issues WHERE state = 'CLOSED' LIMIT 10".to_string(),
                ),
                ..cayenne()
            });
        },
        async |rt| rows(&rt, "SELECT id, body, body_embedding FROM issues").await,
    )
    .await?;

    assert_eq!(kept.len(), 10, "LIMIT 10 must keep 10 rows: {kept:?}");
    assert!(
        kept.iter().all(|(id, _)| state(*id) == "CLOSED"),
        "every kept row must match the filter: {kept:?}"
    );
    assert_eq!(
        embedded,
        sorted_bodies(&kept),
        "only the kept rows may be embedded"
    );
    Ok(())
}
