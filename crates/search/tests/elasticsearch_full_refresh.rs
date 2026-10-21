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
use search::index::elasticsearch::{ElasticsearchIndexWriteMaintenance, ElasticsearchTextIndex};
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

struct TestIndex {
    http: reqwest::Client,
    name: String,
    index: ElasticsearchTextIndex,
}

impl TestIndex {
    async fn new(test: &str) -> Self {
        let http = reqwest::Client::new();
        let name = format!("search-12413-{test}-{}", std::process::id());
        let _ = http.delete(format!("{}/{name}", es_url())).send().await;
        let client: Arc<dyn Elasticsearch> =
            Arc::new(Client::new(&es_url(), None, None).expect("valid Elasticsearch URL"));
        let index = ElasticsearchTextIndex {
            client,
            es_index: name.clone(),
            search_column_name: "body".to_string(),
            search_fields: vec!["body".to_string()],
            primary_key: vec![Field::new("id", DataType::Int64, false)],
            source_schema: schema(),
            batch_write_rows: 500,
            write_maintenance: Arc::new(ElasticsearchIndexWriteMaintenance::default()),
        };
        Self { http, name, index }
    }

    /// Runs one complete write the way the accelerated-table sink does.
    async fn write(&self, window: WriteWindow, batch: RecordBatch) {
        self.index
            .on_write_start(window)
            .await
            .expect("on_write_start succeeds");
        self.index
            .compute_index(vec![batch])
            .await
            .expect("the write lands");
        self.index
            .on_write_complete()
            .await
            .expect("on_write_complete succeeds");
    }

    /// Every document `_id` the index holds, after a refresh makes the last write visible.
    async fn ids(&self) -> BTreeSet<i64> {
        let url = es_url();
        self.http
            .post(format!("{url}/{}/_refresh", self.name))
            .send()
            .await
            .expect("refresh request");
        let resp: serde_json::Value = self
            .http
            .post(format!("{url}/{}/_search", self.name))
            .json(&serde_json::json!({ "size": 10_000, "_source": false }))
            .send()
            .await
            .expect("search request")
            .json()
            .await
            .expect("search response");
        resp["hits"]["hits"]
            .as_array()
            .expect("hits array")
            .iter()
            .map(|hit| {
                hit["_id"]
                    .as_str()
                    .expect("string _id")
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
