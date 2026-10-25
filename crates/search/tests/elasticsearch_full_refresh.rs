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
#![expect(clippy::expect_used, reason = "integration-test helpers")]

//! Regression tests for #12413 against a real Elasticsearch (`ELASTICSEARCH_URL`, default
//! `http://localhost:9200`): a replacing write — a `refresh_mode: full` refresh — has to remove
//! the documents of rows it no longer sends, and nothing else.
//!
//! Each test drives an [`ElasticsearchTextIndex`] through the lifecycle the accelerated-table
//! sink runs: `on_write_start(window)`, `compute_index`, then `on_write_complete` or
//! `on_write_failed`.

use std::collections::BTreeSet;
use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use elasticsearch::{Client, Elasticsearch};
use search::index::elasticsearch::{
    ElasticsearchIndexWriteMaintenance, ElasticsearchIndexWriteOptions, ElasticsearchTextIndex,
};
use spice_table::{Index, WriteWindow};

fn es_url() -> String {
    std::env::var("ELASTICSEARCH_URL").unwrap_or_else(|_| "http://localhost:9200".to_string())
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("body", DataType::Utf8, true),
    ]))
}

fn rows(ids: impl IntoIterator<Item = i64>, text: &str) -> RecordBatch {
    let ids: Vec<i64> = ids.into_iter().collect();
    let bodies: Vec<String> = ids.iter().map(|id| format!("{text} {id}")).collect();
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(bodies)),
        ],
    )
    .expect("valid test batch")
}

/// Waits for the cluster to answer, so a test started alongside the container does not fail
/// on its first request.
async fn wait_for_es(http: &reqwest::Client) {
    for _ in 0..30 {
        if let Ok(resp) = http
            .get(format!("{}/_cluster/health", es_url()))
            .send()
            .await
            && resp.status().is_success()
        {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
    panic!("Elasticsearch not available at {}", es_url());
}

struct TestIndex {
    http: reqwest::Client,
    client: Arc<Client>,
    name: String,
    index: ElasticsearchTextIndex,
}

impl TestIndex {
    async fn new(test: &str) -> Self {
        Self::with_options(test, ElasticsearchIndexWriteOptions::default()).await
    }

    async fn with_options(test: &str, options: ElasticsearchIndexWriteOptions) -> Self {
        let http = reqwest::Client::new();
        wait_for_es(&http).await;
        let name = format!("search-12413-{test}-{}", std::process::id());
        let _ = http.delete(format!("{}/{name}", es_url())).send().await;
        let client = Arc::new(Client::new(&es_url(), None, None).expect("valid Elasticsearch URL"));
        let index = ElasticsearchTextIndex {
            client: Arc::clone(&client) as Arc<dyn Elasticsearch>,
            es_index: name.clone(),
            search_column_name: "body".to_string(),
            search_fields: vec!["body".to_string()],
            primary_key: vec![Field::new("id", DataType::Int64, false)],
            source_schema: schema(),
            batch_write_rows: 500,
            write_maintenance: Arc::new(ElasticsearchIndexWriteMaintenance::new(options)),
        };
        Self {
            http,
            client,
            name,
            index,
        }
    }

    /// Runs one complete write the way the accelerated-table sink does.
    async fn write(&self, window: WriteWindow, batch: RecordBatch) {
        self.index
            .on_write_start(window)
            .await
            .expect("on_write_start succeeds");
        self.finish_write(batch).await;
    }

    /// Sends `batch` and completes the write, as the sink does after `on_write_start` returns,
    /// whether that start succeeded or failed best-effort.
    async fn finish_write(&self, batch: RecordBatch) {
        self.index
            .compute_index(vec![batch])
            .await
            .expect("the write lands");
        self.index
            .on_write_complete()
            .await
            .expect("on_write_complete succeeds");
    }

    /// Starts a write and sends `batch`, then abandons it without `on_write_complete` or
    /// `on_write_failed`, as the refresh runner does when a new refresh request arrives
    /// mid-refresh.
    async fn dropped_write(&self, window: WriteWindow, batch: RecordBatch) {
        self.index
            .on_write_start(window)
            .await
            .expect("on_write_start succeeds");
        self.index
            .compute_index(vec![batch])
            .await
            .expect("the dropped write's rows land");
    }

    /// Every document `_id` the index holds, after a refresh makes the last write visible.
    async fn ids(&self) -> BTreeSet<i64> {
        self.client
            .refresh_index(&self.name)
            .await
            .expect("refresh request");
        let resp = self
            .client
            .search_raw(
                &self.name,
                &serde_json::json!({ "size": 10_000, "_source": false }),
            )
            .await
            .expect("search request");
        resp.hits
            .hits
            .iter()
            .map(|hit| {
                hit.id
                    .parse::<i64>()
                    .expect("integer key renders as its own _id")
            })
            .collect()
    }

    async fn drop_index(&self) {
        let _ = self
            .http
            .delete(format!("{}/{}", es_url(), self.name))
            .send()
            .await;
    }
}

#[tokio::test]
async fn a_full_refresh_removes_the_documents_of_rows_it_dropped() {
    let t = TestIndex::new("dropped").await;
    t.write(WriteWindow::ReplaceAll, rows([1, 2, 3, 4], "first"))
        .await;
    assert_eq!(t.ids().await, BTreeSet::from([1, 2, 3, 4]));

    t.write(WriteWindow::ReplaceAll, rows([1, 3], "second"))
        .await;

    assert_eq!(
        t.ids().await,
        BTreeSet::from([1, 3]),
        "rows 2 and 4 are gone from the source, so their documents must be gone from the index"
    );
    t.drop_index().await;
}

#[tokio::test]
async fn a_full_refresh_that_sends_nothing_empties_the_index() {
    let t = TestIndex::new("empty").await;
    t.write(WriteWindow::ReplaceAll, rows([1, 2], "first"))
        .await;

    t.write(WriteWindow::ReplaceAll, rows([], "second")).await;

    assert_eq!(t.ids().await, BTreeSet::new());
    t.drop_index().await;
}

/// More documents than one reconcile page holds, so the snapshot is read across pages.
#[tokio::test]
async fn a_full_refresh_reconciles_across_snapshot_pages() {
    let t = TestIndex::new("pages").await;
    t.write(WriteWindow::ReplaceAll, rows(0..2_500, "first"))
        .await;
    assert_eq!(t.ids().await.len(), 2_500);

    let kept: BTreeSet<i64> = (0..2_500).filter(|id| id % 3 == 0).collect();
    t.write(
        WriteWindow::ReplaceAll,
        rows(kept.iter().copied(), "second"),
    )
    .await;

    let ids = t.ids().await;
    assert!(
        ids == kept,
        "expected exactly the {} rows the refresh sent; the index holds {} documents, {} of them for dropped rows and {} kept rows missing",
        kept.len(),
        ids.len(),
        ids.difference(&kept).count(),
        kept.difference(&ids).count(),
    );
    t.drop_index().await;
}

#[tokio::test]
async fn an_append_removes_nothing() {
    let t = TestIndex::new("append").await;
    t.write(WriteWindow::ReplaceAll, rows([1, 2, 3], "first"))
        .await;

    t.write(WriteWindow::Append, rows([4], "second")).await;

    assert_eq!(t.ids().await, BTreeSet::from([1, 2, 3, 4]));
    t.drop_index().await;
}

/// A failed write proves nothing about which rows the source still has.
#[tokio::test]
async fn a_failed_full_refresh_removes_nothing() {
    let t = TestIndex::new("failed").await;
    t.write(WriteWindow::ReplaceAll, rows([1, 2, 3], "first"))
        .await;

    t.index
        .on_write_start(WriteWindow::ReplaceAll)
        .await
        .expect("on_write_start succeeds");
    t.index
        .compute_index(vec![rows([1], "second")])
        .await
        .expect("the write lands");
    t.index
        .on_write_failed()
        .await
        .expect("on_write_failed succeeds");

    assert_eq!(t.ids().await, BTreeSet::from([1, 2, 3]));
    t.drop_index().await;
}

/// A document rewritten while the refresh runs — by a change stream, say — is newer than the
/// refresh's snapshot and is not the refresh's to delete.
#[tokio::test]
async fn a_document_written_during_the_refresh_by_another_writer_is_kept() {
    let t = TestIndex::new("concurrent").await;
    t.write(WriteWindow::ReplaceAll, rows([1, 2, 3], "first"))
        .await;

    t.index
        .on_write_start(WriteWindow::ReplaceAll)
        .await
        .expect("on_write_start succeeds");
    t.index
        .compute_index(vec![rows([1], "refresh")])
        .await
        .expect("the refresh's rows land");
    // Another writer rewrites row 3 before the refresh completes.
    t.index
        .compute_index(vec![rows([3], "concurrent")])
        .await
        .expect("the concurrent write lands");
    t.index
        .on_write_complete()
        .await
        .expect("on_write_complete succeeds");

    assert_eq!(
        t.ids().await,
        BTreeSet::from([1, 3]),
        "row 2 was dropped; row 3 was rewritten after the snapshot and must survive"
    );
    t.drop_index().await;
}

/// The refresh runner drops a superseded refresh mid-write, without `on_write_failed`. The next
/// full refresh must not reconcile against the abandoned write's snapshot: that snapshot predates
/// the abandoned write's rows, so a row it rewrote would survive the next refresh dropping it.
#[tokio::test]
async fn a_full_refresh_after_a_cancelled_one_removes_the_rows_it_dropped() {
    let t = TestIndex::new("cancelled").await;
    t.write(WriteWindow::ReplaceAll, rows([1, 2, 3], "first"))
        .await;

    // A refresh starts, rewrites row 1, and is dropped before it completes.
    t.dropped_write(WriteWindow::ReplaceAll, rows([1], "cancelled"))
        .await;

    // The next refresh no longer has row 1.
    t.write(WriteWindow::ReplaceAll, rows([2, 3], "second"))
        .await;

    assert_eq!(
        t.ids().await,
        BTreeSet::from([2, 3]),
        "row 1 is gone from the source; the cancelled refresh rewriting it must not keep it"
    );
    t.drop_index().await;
}

/// A refresh request can override the refresh mode, so an append can be the request that
/// supersedes a full refresh mid-write.
#[tokio::test]
async fn an_append_after_a_cancelled_full_refresh_removes_nothing() {
    let t = TestIndex::new("append-after-cancel").await;
    t.write(WriteWindow::ReplaceAll, rows([1, 2, 3], "first"))
        .await;

    // A full refresh starts, rewrites row 1, and is dropped before it completes.
    t.dropped_write(WriteWindow::ReplaceAll, rows([1], "cancelled"))
        .await;

    t.write(WriteWindow::Append, rows([4], "appended")).await;

    assert_eq!(
        t.ids().await,
        BTreeSet::from([1, 2, 3, 4]),
        "an append removes nothing, whatever snapshot a dropped full refresh left behind"
    );
    t.drop_index().await;
}

/// When Elasticsearch rejects the `refresh_interval` override, the sink logs the failure and
/// writes anyway, and that write is still a full refresh.
#[tokio::test]
async fn a_full_refresh_whose_refresh_interval_override_is_rejected_removes_the_rows_it_dropped() {
    let t = TestIndex::with_options(
        "rejected-override",
        ElasticsearchIndexWriteOptions {
            // Not a time value, so Elasticsearch rejects the settings update.
            refresh_interval_during_write: Some("not-a-duration".to_string()),
            force_merge_segments: None,
        },
    )
    .await;
    // The first write creates the index, so there is no index yet to read settings from.
    t.index
        .on_write_start(WriteWindow::ReplaceAll)
        .await
        .expect_err("the index does not exist yet, so its refresh_interval cannot be read");
    t.finish_write(rows([1, 2, 3], "first")).await;
    assert_eq!(t.ids().await, BTreeSet::from([1, 2, 3]));

    t.index
        .on_write_start(WriteWindow::ReplaceAll)
        .await
        .expect_err("Elasticsearch rejects the refresh_interval override");
    t.finish_write(rows([1, 3], "second")).await;

    assert_eq!(
        t.ids().await,
        BTreeSet::from([1, 3]),
        "row 2 is gone from the source; a rejected override must not cost the refresh its reconcile"
    );
    t.drop_index().await;
}

/// The configured index name can be an alias, and a version read through it belongs to the
/// concrete index behind it. Here the alias moves to a fresh index during the refresh, and the
/// refresh's rewrite of row 1 lands there at the same `(seq_no, primary_term)` the snapshot
/// recorded in the old index. A delete addressed to the alias would remove that live document.
#[tokio::test]
async fn a_full_refresh_across_an_alias_switch_keeps_the_rows_it_rewrote() {
    let t = TestIndex::new("alias").await;
    let (old, new) = (format!("{}-old", t.name), format!("{}-new", t.name));
    for index in [&old, &new] {
        let _ = t.http.delete(format!("{}/{index}", es_url())).send().await;
        let resp = t
            .http
            .put(format!("{}/{index}", es_url()))
            .json(&serde_json::json!({ "settings": { "number_of_shards": 1 } }))
            .send()
            .await
            .expect("create index request");
        assert!(
            resp.status().is_success(),
            "create {index}: {}",
            resp.status()
        );
    }
    let point_alias = |from: Option<&str>, to: &str| {
        let mut actions = vec![serde_json::json!({ "add": { "index": to, "alias": t.name } })];
        if let Some(from) = from {
            actions.insert(
                0,
                serde_json::json!({ "remove": { "index": from, "alias": t.name } }),
            );
        }
        let request = t
            .http
            .post(format!("{}/_aliases", es_url()))
            .json(&serde_json::json!({ "actions": actions }));
        async move {
            let resp = request.send().await.expect("alias request");
            assert!(
                resp.status().is_success(),
                "alias update: {}",
                resp.status()
            );
        }
    };
    point_alias(None, &old).await;
    t.write(WriteWindow::Append, rows([1], "first")).await;
    assert_eq!(t.ids().await, BTreeSet::from([1]));

    t.index
        .on_write_start(WriteWindow::ReplaceAll)
        .await
        .expect("on_write_start succeeds");
    point_alias(Some(&old), &new).await;
    t.finish_write(rows([1], "second")).await;

    assert_eq!(
        t.ids().await,
        BTreeSet::from([1]),
        "the refresh rewrote row 1 into the index the alias now names, so it must stay searchable"
    );
    for index in [&old, &new] {
        let _ = t.http.delete(format!("{}/{index}", es_url())).send().await;
    }
}
