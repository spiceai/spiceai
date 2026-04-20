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

//! Integration tests for the Elasticsearch data components (query table, schema, search table).
//!
//! Requires a running Elasticsearch instance (see `ELASTICSEARCH_URL` env var, default `http://localhost:9200`).
//!
//! ```sh
//! cargo test -p data_components --features elasticsearch --test elasticsearch_test
//! ```

#![cfg(feature = "elasticsearch")]
#![allow(clippy::expect_used)]

use std::sync::Arc;

use arrow::datatypes::DataType;
use arrow::util::pretty::pretty_format_batches;
use data_components::elasticsearch::query_table::ElasticsearchQueryTable;
use data_components::elasticsearch::schema::mapping_to_schema;
use data_components::elasticsearch::search_table::{
    ElasticsearchKnnTable, ElasticsearchTextSearchTable, search_result_schema,
};
use datafusion::prelude::*;
use elasticsearch::{Client, Elasticsearch};

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

async fn setup_test_index(index: &str) -> Arc<dyn Elasticsearch> {
    let client = Client::new(&es_url(), None, None).expect("client");
    wait_for_es().await;

    // Delete if exists
    let _ = reqwest::Client::new()
        .delete(format!("{}/{index}", es_url()))
        .send()
        .await;

    // Create index
    let mapping = serde_json::json!({
        "mappings": {
            "properties": {
                "title": { "type": "text" },
                "category": { "type": "keyword" },
                "count": { "type": "integer" },
                "price": { "type": "float" },
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
    assert!(resp.status().is_success(), "Failed to create index");

    // Index test documents
    let docs = vec![
        (
            Some("1".to_string()),
            serde_json::json!({
                "title": "Rust Programming",
                "category": "programming",
                "count": 42,
                "price": 29.99,
                "active": true,
                "embedding": [0.1, 0.2, 0.3]
            }),
        ),
        (
            Some("2".to_string()),
            serde_json::json!({
                "title": "Data Engineering with Arrow",
                "category": "data",
                "count": 18,
                "price": 39.99,
                "active": true,
                "embedding": [0.9, 0.8, 0.7]
            }),
        ),
        (
            Some("3".to_string()),
            serde_json::json!({
                "title": "Search Engine Design",
                "category": "programming",
                "count": 7,
                "price": 24.99,
                "active": false,
                "embedding": [0.5, 0.5, 0.5]
            }),
        ),
    ];

    client
        .bulk_index(index, &docs)
        .await
        .expect("bulk index failed");

    // Refresh
    let _ = reqwest::Client::new()
        .post(format!("{}/{index}/_refresh", es_url()))
        .send()
        .await;

    Arc::new(client) as Arc<dyn Elasticsearch>
}

#[tokio::test]
#[ignore = "requires a running Elasticsearch instance"]
async fn test_schema_from_mapping() {
    let client = Client::new(&es_url(), None, None).expect("client");
    wait_for_es().await;

    let index = "test_dc_schema";
    let _ = setup_test_index(index).await;

    let mapping = client.get_mapping(index).await.expect("get_mapping");
    let index_mapping = mapping.get(index).expect("index mapping");
    let schema = mapping_to_schema(&index_mapping.mappings.properties);

    // Verify schema field types
    let title_field = schema.field_with_name("title").expect("title field");
    assert_eq!(title_field.data_type(), &DataType::Utf8);

    let count_field = schema.field_with_name("count").expect("count field");
    assert_eq!(count_field.data_type(), &DataType::Int32);

    let price_field = schema.field_with_name("price").expect("price field");
    assert_eq!(price_field.data_type(), &DataType::Float32);

    let active_field = schema.field_with_name("active").expect("active field");
    assert_eq!(active_field.data_type(), &DataType::Boolean);

    let embed_field = schema
        .field_with_name("embedding")
        .expect("embedding field");
    assert!(
        matches!(embed_field.data_type(), DataType::FixedSizeList(_, 3)),
        "Expected FixedSizeList with dim 3, got {:?}",
        embed_field.data_type()
    );
}

#[tokio::test]
#[ignore = "requires a running Elasticsearch instance"]
async fn test_query_table_scan() {
    let index = "test_dc_query_table";
    let client = setup_test_index(index).await;

    let mapping_client = Client::new(&es_url(), None, None).expect("client");
    let mapping = mapping_client
        .get_mapping(index)
        .await
        .expect("get_mapping");
    let schema = mapping_to_schema(&mapping.get(index).expect("mapping").mappings.properties);

    let table = ElasticsearchQueryTable::new(client, index.to_string(), schema);
    let ctx = SessionContext::new();
    ctx.register_table("es_table", Arc::new(table))
        .expect("register");

    let df = ctx
        .sql("SELECT title, count, active FROM es_table")
        .await
        .expect("sql");
    let batches = df.collect().await.expect("collect");

    let display = pretty_format_batches(&batches).expect("format").to_string();
    assert!(display.contains("Rust Programming"));
    assert!(display.contains("42"));

    // Check total row count
    let total_rows: usize = batches
        .iter()
        .map(arrow::array::RecordBatch::num_rows)
        .sum();
    assert_eq!(total_rows, 3);
}

#[tokio::test]
#[ignore = "requires a running Elasticsearch instance"]
async fn test_query_table_with_limit() {
    let index = "test_dc_query_limit";
    let client = setup_test_index(index).await;

    let mapping_client = Client::new(&es_url(), None, None).expect("client");
    let mapping = mapping_client
        .get_mapping(index)
        .await
        .expect("get_mapping");
    let schema = mapping_to_schema(&mapping.get(index).expect("mapping").mappings.properties);

    let table = ElasticsearchQueryTable::new(client, index.to_string(), schema);
    let ctx = SessionContext::new();
    ctx.register_table("es_table", Arc::new(table))
        .expect("register");

    let df = ctx
        .sql("SELECT title FROM es_table LIMIT 1")
        .await
        .expect("sql");
    let batches = df.collect().await.expect("collect");
    let total_rows: usize = batches
        .iter()
        .map(arrow::array::RecordBatch::num_rows)
        .sum();
    assert_eq!(total_rows, 1);
}

#[tokio::test]
#[ignore = "requires a running Elasticsearch instance"]
async fn test_knn_table_search() {
    let index = "test_dc_knn";
    let client = setup_test_index(index).await;

    let mapping_client = Client::new(&es_url(), None, None).expect("client");
    let mapping = mapping_client
        .get_mapping(index)
        .await
        .expect("get_mapping");
    let source_schema =
        mapping_to_schema(&mapping.get(index).expect("mapping").mappings.properties);

    // Build a result schema with _id + _score
    let primary_fields = vec![arrow::datatypes::Field::new("_id", DataType::Utf8, true)];
    let schema = search_result_schema(&primary_fields, &[]);

    let table = ElasticsearchKnnTable {
        client,
        index: index.to_string(),
        vector_field: "embedding".to_string(),
        query_vector: vec![0.9, 0.8, 0.7],
        k: 3,
        schema,
        source_schema,
        query_text: None,
        embedder: None,
    };

    let ctx = SessionContext::new();
    ctx.register_table("knn_results", Arc::new(table))
        .expect("register");

    let df = ctx
        .sql("SELECT _id, _score FROM knn_results")
        .await
        .expect("sql");
    let batches = df.collect().await.expect("collect");

    let display = pretty_format_batches(&batches).expect("format").to_string();
    // Doc "2" has embedding [0.9, 0.8, 0.7] — closest match
    assert!(
        display.contains('2'),
        "Expected doc '2' in results: {display}"
    );
}

#[tokio::test]
#[ignore = "requires a running Elasticsearch instance"]
async fn test_text_search_table() {
    let index = "test_dc_text_search";
    let client = setup_test_index(index).await;

    let mapping_client = Client::new(&es_url(), None, None).expect("client");
    let mapping = mapping_client
        .get_mapping(index)
        .await
        .expect("get_mapping");
    let source_schema =
        mapping_to_schema(&mapping.get(index).expect("mapping").mappings.properties);

    let primary_fields = vec![arrow::datatypes::Field::new("_id", DataType::Utf8, true)];
    let schema = search_result_schema(&primary_fields, &[]);

    let table = ElasticsearchTextSearchTable {
        client,
        index: index.to_string(),
        search_fields: vec!["title".to_string()],
        query_text: "rust programming".to_string(),
        limit: 10,
        schema,
        source_schema,
    };

    let ctx = SessionContext::new();
    ctx.register_table("text_results", Arc::new(table))
        .expect("register");

    let df = ctx
        .sql("SELECT _id, _score FROM text_results")
        .await
        .expect("sql");
    let batches = df.collect().await.expect("collect");
    let total_rows: usize = batches
        .iter()
        .map(arrow::array::RecordBatch::num_rows)
        .sum();
    assert!(total_rows > 0, "Expected at least one text search result");

    let display = pretty_format_batches(&batches).expect("format").to_string();
    assert!(
        display.contains('1'),
        "Expected doc '1' (Rust Programming) in results: {display}"
    );
}
