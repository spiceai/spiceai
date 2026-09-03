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

//! Integration tests for `refresh_mode: changes` composed with column embeddings
//! and/or full-text search.
//!
//! These assert that a row arriving on the changes stream is *searchable*
//! afterwards, not merely that the dataset started. Startup-only coverage passes
//! whenever the connector wrappers hand the accelerator a changes stream that
//! skips the indexing transform, which leaves vector and full-text search
//! silently stale instead of failing loudly.
//!
//! Every dataset here carries spicepod metadata (a column `description`). That is
//! what inserts a `MetadataEnrichedTableProvider` into the source provider stack,
//! and the wrappers have to see through it to build the changes stream.

#![cfg(all(feature = "debezium", feature = "models"))]
#![allow(clippy::expect_used)]

use std::{collections::HashMap, sync::Arc, time::Duration};

use app::AppBuilder;
use futures::TryStreamExt;
use runtime::Runtime;
use spicepod::{
    acceleration::{Acceleration, OnConflictBehavior, RefreshMode},
    component::{dataset::Dataset, embeddings::Embeddings},
    semantic::{Column, ColumnLevelEmbeddingConfig, FullTextSearchConfig},
};

use crate::{
    configure_test_datafusion, init_tracing,
    utils::{runtime_ready_check, test_request_context, wait_until_true},
};

const SEARCH_TIMEOUT: Duration = Duration::from_secs(30);

fn docs_dataset(with_embeddings: bool, with_fts: bool) -> Dataset {
    let mut dataset = Dataset::new("cdc:docs", "docs");

    // The `description` is load-bearing: it is what makes
    // `table_provider_with_spicepod_metadata` insert a metadata layer into the
    // source provider stack, which is the condition under which the changes
    // stream used to be dropped.
    let mut content = Column {
        r#type: Some("utf8".to_string()),
        nullable: Some(true),
        description: Some("document body".to_string()),
        ..Column::new("content")
    };
    if with_embeddings {
        content = content
            .with_embedding(ColumnLevelEmbeddingConfig::model("test_embed").with_row_id("id"));
    }
    if with_fts {
        content = content.with_full_text_search(FullTextSearchConfig::enabled().with_row_id("id"));
    }

    dataset.columns = vec![
        Column {
            r#type: Some("int64".to_string()),
            nullable: Some(true),
            description: Some("document id".to_string()),
            ..Column::new("id")
        },
        content,
    ];

    let mut on_conflict = HashMap::new();
    on_conflict.insert("id".to_string(), OnConflictBehavior::Upsert);
    dataset.acceleration = Some(Acceleration {
        enabled: true,
        engine: Some("arrow".to_string()),
        refresh_mode: Some(RefreshMode::Changes),
        primary_key: Some("id".to_string()),
        on_conflict,
        ..Acceleration::default()
    });
    dataset
}

async fn start(with_embeddings: bool, with_fts: bool) -> anyhow::Result<Arc<Runtime>> {
    configure_test_datafusion();
    let mut app =
        AppBuilder::new("cdc_search_test").with_dataset(docs_dataset(with_embeddings, with_fts));
    if with_embeddings {
        app = app.with_embedding(Embeddings::new(
            "model2vec:minishlab/potion-base-2M",
            "test_embed",
        ));
    }

    let rt = Arc::new(Runtime::builder().with_app(app.build()).build().await);
    let load_rt = Arc::clone(&rt);
    tokio::select! {
        () = tokio::time::sleep(Duration::from_mins(3)) => {
            anyhow::bail!("timed out loading components");
        }
        () = load_rt.load_components() => {}
    }

    // A dataset whose changes stream failed to attach never reaches Ready — it
    // errors with "A changes stream is required" during registration.
    runtime_ready_check(&rt).await;
    Ok(rt)
}

/// One Debezium change event: the op, and the row before and after it.
type Event<'a> = (&'a str, Option<(i64, &'a str)>, Option<(i64, &'a str)>);

fn render_event((op, before, after): &Event<'_>) -> String {
    let render = |row: Option<(i64, &str)>| match row {
        Some((id, content)) => format!(r#"{{"id":{id},"content":"{content}"}}"#),
        None => "null".to_string(),
    };
    format!(
        r#"{{"before":{},"after":{},"op":"{op}","ts_ms":1,"source":{{}}}}"#,
        render(*before),
        render(*after)
    )
}

/// Push one request body onto the dataset's changes stream and wait for the
/// accelerator to ack the apply.
async fn push_body(body: String) -> anyhow::Result<()> {
    let registered = wait_until_true(SEARCH_TIMEOUT, || async {
        runtime::dataconnector::cdc_ingest::lookup("docs").is_some()
    })
    .await;
    anyhow::ensure!(registered, "CDC ingest handle never registered for `docs`");

    let handle =
        runtime::dataconnector::cdc_ingest::lookup("docs").expect("cdc ingest handle registered");
    handle
        .ingest(
            "docs",
            data_components::debezium::decode::CdcFormat::Json,
            body.as_bytes(),
            None,
            Duration::from_mins(1),
        )
        .await
        .map_err(|e| anyhow::anyhow!("cdc ingest failed: {e}"))?;
    Ok(())
}

/// Push one Debezium change event and wait for the accelerator to ack the apply.
async fn push_change(
    op: &str,
    before: Option<(i64, &str)>,
    after: Option<(i64, &str)>,
) -> anyhow::Result<()> {
    push_body(render_event(&(op, before, after))).await
}

/// Push several change events as **one** request body, which is what a source
/// producing more than one change for a row inside a single poll looks like: a
/// Debezium JSON array decodes to one `ChangeBatch`, so every event reaches the
/// indexes in the same batch rather than one batch each.
async fn push_events(events: &[Event<'_>]) -> anyhow::Result<()> {
    let body = format!(
        "[{}]",
        events
            .iter()
            .map(render_event)
            .collect::<Vec<_>>()
            .join(",")
    );
    push_body(body).await
}

async fn query_ids(rt: &Arc<Runtime>, sql: &str) -> anyhow::Result<Vec<i64>> {
    let batches = rt
        .datafusion()
        .query_builder(sql)
        .build()
        .run()
        .await?
        .data
        .try_collect::<Vec<_>>()
        .await?;

    let mut ids = Vec::new();
    for batch in &batches {
        let column = batch.column_by_name("id").ok_or_else(|| {
            anyhow::anyhow!(
                "query `{sql}` returned no `id` column: {:?}",
                batch.schema()
            )
        })?;
        let values = column
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .ok_or_else(|| anyhow::anyhow!("`id` column is not Int64"))?;
        for i in 0..batch.num_rows() {
            ids.push(values.value(i));
        }
    }
    Ok(ids)
}

/// Poll `sql` until it returns `id`, then report whether it ever did.
async fn eventually_finds(rt: &Arc<Runtime>, sql: &str, id: i64) -> bool {
    wait_until_true(SEARCH_TIMEOUT, || {
        let rt = Arc::clone(rt);
        let sql = sql.to_string();
        async move {
            query_ids(&rt, &sql)
                .await
                .is_ok_and(|ids| ids.contains(&id))
        }
    })
    .await
}

/// Assert the changed row landed in the accelerator, separately from whether it
/// was indexed — so a failure says which half broke.
async fn assert_row_applied(rt: &Arc<Runtime>, id: i64, content: &str) -> anyhow::Result<()> {
    let sql = format!("SELECT id FROM docs WHERE content = '{content}'");
    anyhow::ensure!(
        eventually_finds(rt, &sql, id).await,
        "row {id} never reached the accelerator via the changes stream (`{sql}`)"
    );
    Ok(())
}

/// Create then update a row, and confirm the update was applied.
async fn apply_create_then_update(rt: &Arc<Runtime>) -> anyhow::Result<()> {
    push_change("c", None, Some((1, "the quick brown fox"))).await?;
    push_change(
        "u",
        Some((1, "the quick brown fox")),
        Some((1, "a peregrine falcon in flight")),
    )
    .await?;
    assert_row_applied(rt, 1, "a peregrine falcon in flight").await
}

#[tokio::test]
async fn cdc_with_embeddings_makes_changed_row_vector_searchable() -> anyhow::Result<()> {
    let _tracing = init_tracing(Some("integration=debug,runtime=debug,info"));
    test_request_context()
        .scope(async {
            let rt = start(true, false).await?;
            apply_create_then_update(&rt).await?;

            anyhow::ensure!(
                eventually_finds(
                    &rt,
                    "SELECT id FROM vector_search(docs, 'peregrine falcon', content) LIMIT 4",
                    1,
                )
                .await,
                "the updated row is in the accelerator but vector_search cannot find it — \
                 the changes stream reached the accelerator without computing embeddings"
            );
            Ok(())
        })
        .await
}

#[tokio::test]
async fn cdc_with_full_text_search_makes_changed_row_text_searchable() -> anyhow::Result<()> {
    let _tracing = init_tracing(Some("integration=debug,runtime=debug,info"));
    test_request_context()
        .scope(async {
            let rt = start(false, true).await?;
            apply_create_then_update(&rt).await?;

            anyhow::ensure!(
                eventually_finds(
                    &rt,
                    "SELECT id FROM text_search(docs, 'peregrine', content) LIMIT 4",
                    1,
                )
                .await,
                "the updated row is in the accelerator but text_search cannot find it — \
                 the changes stream reached the accelerator without updating the full-text index"
            );
            Ok(())
        })
        .await
}

/// Both wrappers on one dataset: `FullTextConnector` over `EmbeddingConnector`.
/// The FTS connector unwraps its `IndexedTableProvider` and hands the inner
/// provider to the embeddings connector, so both transforms have to compose.
#[tokio::test]
async fn cdc_with_embeddings_and_full_text_search_indexes_changed_row() -> anyhow::Result<()> {
    let _tracing = init_tracing(Some("integration=debug,runtime=debug,info"));
    test_request_context()
        .scope(async {
            let rt = start(true, true).await?;
            apply_create_then_update(&rt).await?;

            anyhow::ensure!(
                eventually_finds(
                    &rt,
                    "SELECT id FROM vector_search(docs, 'peregrine falcon', content) LIMIT 4",
                    1,
                )
                .await,
                "vector_search cannot find the updated row on a dataset with both \
                 embeddings and full_text_search"
            );
            anyhow::ensure!(
                eventually_finds(
                    &rt,
                    "SELECT id FROM text_search(docs, 'peregrine', content) LIMIT 4",
                    1,
                )
                .await,
                "text_search cannot find the updated row on a dataset with both \
                 embeddings and full_text_search"
            );
            Ok(())
        })
        .await
}

/// A source can produce more than one change for a row inside a single poll, and the
/// whole poll reaches the indexes as one batch. The accelerated table resolves such a
/// key to its last change; the index has to agree, or a search answers for text the
/// table no longer holds.
#[tokio::test]
async fn one_batch_with_two_changes_for_a_row_indexes_only_the_last() -> anyhow::Result<()> {
    let _tracing = init_tracing(Some("integration=debug,runtime=debug,info"));
    test_request_context()
        .scope(async {
            let rt = start(false, true).await?;

            // Both changes in ONE body, so they arrive in one batch. Pushed as `c` then
            // `u` because that is what a create followed by an update in the same poll
            // looks like; the index sees one two-row batch either way.
            push_events(&[
                ("c", None, Some((1, "the quick brown fox"))),
                (
                    "u",
                    Some((1, "the quick brown fox")),
                    Some((1, "a peregrine falcon in flight")),
                ),
            ])
            .await?;

            // The accelerator resolved the duplicated key last-write-wins, which is the
            // state the index is an index *of*.
            assert_row_applied(&rt, 1, "a peregrine falcon in flight").await?;

            // Asserted first, so the negative below is read against an index that has
            // certainly processed this batch rather than one that has not started.
            anyhow::ensure!(
                eventually_finds(
                    &rt,
                    "SELECT id FROM text_search(docs, 'peregrine', content) LIMIT 4",
                    1,
                )
                .await,
                "the surviving change is in the accelerator but text_search cannot find it"
            );

            // `quick` occurs only in the superseded text. A hit here is the index
            // disagreeing with its table: the row is searchable by a word its current
            // content does not contain.
            let stale = query_ids(
                &rt,
                "SELECT id FROM text_search(docs, 'quick', content) LIMIT 4",
            )
            .await?;
            anyhow::ensure!(
                stale.is_empty(),
                "text_search matched the superseded text of row(s) {stale:?}: both changes \
                 for the key were indexed, so the index holds content the table replaced"
            );

            Ok(())
        })
        .await
}
