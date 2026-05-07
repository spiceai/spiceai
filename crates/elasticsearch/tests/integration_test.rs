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

//! Integration tests for the Elasticsearch client crate.
//!
//! These tests require a running Elasticsearch instance. Set the `ELASTICSEARCH_URL`
//! environment variable (default: `http://localhost:9200`). Run with:
//!
//! ```sh
//! docker run -d --name es-test -p 9200:9200 \
//!   -e "discovery.type=single-node" \
//!   -e "xpack.security.enabled=false" \
//!   docker.elastic.co/elasticsearch/elasticsearch:8.17.0
//!
//! cargo test -p elasticsearch --test integration_test
//! ```

#![allow(clippy::expect_used)]

use elasticsearch::{Client, SearchRequest};

fn es_url() -> String {
    std::env::var("ELASTICSEARCH_URL").unwrap_or_else(|_| "http://localhost:9200".to_string())
}

async fn wait_for_es() {
    let http = reqwest::Client::new();
    for i in 0..30 {
        if let Ok(resp) = http
            .get(format!("{}/_cluster/health", es_url()))
            .send()
            .await
            && resp.status().is_success()
        {
            return;
        }
        assert!(i != 29, "Elasticsearch not available at {}", es_url());
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
}

async fn setup_test_index(index: &str) {
    // Delete if exists (ignore errors)
    let _ = reqwest::Client::new()
        .delete(format!("{}/{index}", es_url()))
        .send()
        .await;

    // Create index with explicit mappings
    let mapping = serde_json::json!({
        "mappings": {
            "properties": {
                "title": { "type": "text" },
                "category": { "type": "keyword" },
                "score": { "type": "integer" },
                "rating": { "type": "float" },
                "active": { "type": "boolean" },
                "embedding": {
                    "type": "dense_vector",
                    "dims": 3,
                    "similarity": "cosine"
                }
            }
        }
    });

    let resp = reqwest::Client::new()
        .put(format!("{}/{index}", es_url()))
        .json(&mapping)
        .send()
        .await
        .expect("create index");
    assert!(
        resp.status().is_success(),
        "Failed to create index: {}",
        resp.text().await.unwrap_or_default()
    );
}

async fn index_test_docs(client: &Client, index: &str) {
    let docs = vec![
        (
            Some("1".to_string()),
            serde_json::json!({
                "title": "Introduction to Elasticsearch",
                "category": "tutorial",
                "score": 95,
                "rating": 4.8,
                "active": true,
                "embedding": [0.1, 0.2, 0.3]
            }),
        ),
        (
            Some("2".to_string()),
            serde_json::json!({
                "title": "Advanced Vector Search Techniques",
                "category": "research",
                "score": 88,
                "rating": 4.5,
                "active": true,
                "embedding": [0.9, 0.8, 0.7]
            }),
        ),
        (
            Some("3".to_string()),
            serde_json::json!({
                "title": "Full-Text Search with BM25",
                "category": "tutorial",
                "score": 72,
                "rating": 3.9,
                "active": false,
                "embedding": [0.5, 0.5, 0.5]
            }),
        ),
    ];

    client
        .bulk_index(index, &docs)
        .await
        .expect("bulk index failed");

    // Refresh so docs are searchable immediately
    let _ = reqwest::Client::new()
        .post(format!("{}/{index}/_refresh", es_url()))
        .send()
        .await;
}

#[tokio::test]
#[ignore = "requires a running Elasticsearch instance"]
async fn test_get_mapping() {
    let client = Client::new(&es_url(), None, None).expect("client");
    wait_for_es().await;

    let index = "test_mapping";
    setup_test_index(index).await;

    let mapping = client.get_mapping(index).await.expect("get_mapping");
    let index_mapping = mapping.get(index).expect("index entry");
    let props = &index_mapping.mappings.properties;

    assert!(props.contains_key("title"));
    assert_eq!(props["title"].field_type.as_deref(), Some("text"));
    assert!(props.contains_key("embedding"));
    assert_eq!(
        props["embedding"].field_type.as_deref(),
        Some("dense_vector")
    );
    assert_eq!(props["embedding"].dims, Some(3));
}

#[tokio::test]
#[ignore = "requires a running Elasticsearch instance"]
async fn test_search_match_all() {
    let client = Client::new(&es_url(), None, None).expect("client");
    wait_for_es().await;

    let index = "test_search_all";
    setup_test_index(index).await;
    index_test_docs(&client, index).await;

    let req = SearchRequest {
        query: Some(elasticsearch::match_all_query()),
        size: Some(10),
        ..Default::default()
    };
    let resp = client.search(index, &req).await.expect("search");
    assert_eq!(
        resp.hits
            .total
            .as_ref()
            .expect("search response should include total hits")
            .value,
        3
    );
    assert_eq!(resp.hits.hits.len(), 3);
}

#[tokio::test]
#[ignore = "requires a running Elasticsearch instance"]
async fn test_search_text_match() {
    let client = Client::new(&es_url(), None, None).expect("client");
    wait_for_es().await;

    let index = "test_search_text";
    setup_test_index(index).await;
    index_test_docs(&client, index).await;

    let req = SearchRequest {
        query: Some(elasticsearch::match_query("title", "elasticsearch")),
        size: Some(10),
        ..Default::default()
    };
    let resp = client.search(index, &req).await.expect("search");
    assert!(
        !resp.hits.hits.is_empty(),
        "Should match at least one document"
    );
    assert_eq!(resp.hits.hits[0].id, "1");
}

#[tokio::test]
#[ignore = "requires a running Elasticsearch instance"]
async fn test_knn_search() {
    let client = Client::new(&es_url(), None, None).expect("client");
    wait_for_es().await;

    let index = "test_knn";
    setup_test_index(index).await;
    index_test_docs(&client, index).await;

    let req = SearchRequest {
        knn: Some(elasticsearch::knn_query(
            "embedding",
            vec![0.9, 0.8, 0.7],
            3,
            10,
        )),
        size: Some(3),
        ..Default::default()
    };
    let resp = client.search(index, &req).await.expect("knn search");
    assert!(!resp.hits.hits.is_empty());
    // The closest vector to [0.9, 0.8, 0.7] should be doc "2"
    assert_eq!(resp.hits.hits[0].id, "2");
    assert!(resp.hits.hits[0].score.is_some());
}

#[tokio::test]
#[ignore = "requires a running Elasticsearch instance"]
async fn test_index_document_and_retrieve() {
    let client = Client::new(&es_url(), None, None).expect("client");
    wait_for_es().await;

    let index = "test_index_doc";
    setup_test_index(index).await;

    let doc = serde_json::json!({
        "title": "Test Document",
        "category": "test",
        "score": 100,
        "rating": 5.0,
        "active": true,
        "embedding": [1.0, 0.0, 0.0]
    });
    client
        .index_document(index, "test_1", &doc)
        .await
        .expect("index_document");

    // Refresh
    let _ = reqwest::Client::new()
        .post(format!("{}/{index}/_refresh", es_url()))
        .send()
        .await;

    let req = SearchRequest {
        query: Some(elasticsearch::match_all_query()),
        size: Some(10),
        ..Default::default()
    };
    let resp = client.search(index, &req).await.expect("search");
    assert_eq!(
        resp.hits
            .total
            .as_ref()
            .expect("search response should include total hits")
            .value,
        1
    );
    assert_eq!(resp.hits.hits[0].id, "test_1");
}

#[tokio::test]
#[ignore = "requires a running Elasticsearch instance"]
async fn test_multi_match_query() {
    let client = Client::new(&es_url(), None, None).expect("client");
    wait_for_es().await;

    let index = "test_multi_match";
    setup_test_index(index).await;
    index_test_docs(&client, index).await;

    let req = SearchRequest {
        query: Some(elasticsearch::multi_match_query(
            &["title", "category"],
            "tutorial",
        )),
        size: Some(10),
        ..Default::default()
    };
    let resp = client.search(index, &req).await.expect("search");
    // "tutorial" appears in the category of docs 1 and 3
    assert!(resp.hits.hits.len() >= 2);
}

#[tokio::test]
#[ignore = "requires a running Elasticsearch instance"]
async fn test_error_on_missing_index() {
    let client = Client::new(&es_url(), None, None).expect("client");
    wait_for_es().await;

    let result = client.get_mapping("nonexistent_index_12345").await;
    result.expect_err("expected error for nonexistent index");
}
