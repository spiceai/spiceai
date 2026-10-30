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

//! Integration tests against a live Qdrant server.
//!
//! Requires `QDRANT_ENDPOINT` (default `http://localhost:6334`). Run in CI by
//! the `integration tests (qdrant)` workflow, which starts a Qdrant container.

#![allow(clippy::expect_used)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use qdrant::payload::PointData;
use qdrant::proto::value::Kind;
use qdrant::proto::{Condition, Distance, FieldType, Filter, PointId, RetrievedPoint, Value};
use qdrant::{Qdrant, QdrantConnection, QdrantStore};

fn endpoint() -> String {
    std::env::var("QDRANT_ENDPOINT").unwrap_or_else(|_| "http://127.0.0.1:6334".to_string())
}

fn string_value(s: &str) -> Value {
    Value {
        kind: Some(Kind::StringValue(s.to_string())),
    }
}

fn unique_collection(prefix: &str) -> String {
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before Unix epoch")
        .as_millis();
    format!("{prefix}-{stamp}")
}

async fn connect() -> Arc<dyn QdrantStore> {
    let connection = QdrantConnection {
        endpoint: endpoint(),
        api_key: None,
        connect_timeout: Some(Duration::from_secs(10)),
    };
    let client = Qdrant::new(&connection).expect("construct Qdrant client");

    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        match client.collection_exists("__connectivity_probe__").await {
            Ok(_) => return Arc::new(client),
            Err(err) => {
                assert!(
                    std::time::Instant::now() < deadline,
                    "Qdrant server did not become reachable in time: {err}"
                );
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        }
    }
}

fn point(id_key: &str, vector: Vec<f32>, tag: &str) -> PointData {
    let mut payload = HashMap::new();
    payload.insert("tag".to_string(), string_value(tag));
    PointData {
        id: Some(qdrant::payload::point_id_from_values(&[id_key.to_string()])),
        payload,
        vector,
    }
}

fn ids_of(points: &[RetrievedPoint]) -> Vec<Option<PointId>> {
    points.iter().map(|p| p.id.clone()).collect()
}

#[tokio::test]
async fn collection_lifecycle_upsert_search_scroll_delete() {
    let store = connect().await;
    let collection = unique_collection("spice-qdrant-it");

    store
        .ensure_collection(&collection, 4, Distance::Cosine)
        .await
        .expect("ensure collection");
    assert!(
        store
            .collection_exists(&collection)
            .await
            .expect("collection_exists")
    );

    let id_a = qdrant::payload::point_id_from_values(&["a".to_string()]);
    let id_b = qdrant::payload::point_id_from_values(&["b".to_string()]);
    let id_c = qdrant::payload::point_id_from_values(&["c".to_string()]);

    store
        .upsert(
            &collection,
            vec![
                point("a", vec![1.0, 0.0, 0.0, 0.0], "a"),
                point("b", vec![0.0, 1.0, 0.0, 0.0], "b"),
                point("c", vec![0.9, 0.1, 0.0, 0.0], "b"),
            ],
            2,
        )
        .await
        .expect("upsert points");

    store
        .create_field_index(&collection, "tag", FieldType::Keyword)
        .await
        .expect("create payload field index");

    store
        .ensure_collection(&collection, 4, Distance::Cosine)
        .await
        .expect("re-ensure with identical parameters");

    let err = store
        .ensure_collection(&collection, 3, Distance::Cosine)
        .await
        .expect_err("dimension mismatch must fail");
    assert!(
        matches!(err, qdrant::Error::CollectionMismatch { .. }),
        "expected a CollectionMismatch error, got {err}"
    );
    let err = store
        .ensure_collection(&collection, 4, Distance::Euclid)
        .await
        .expect_err("distance mismatch must fail");
    assert!(
        matches!(err, qdrant::Error::CollectionMismatch { .. }),
        "expected a CollectionMismatch error, got {err}"
    );

    let results = store
        .search(&collection, vec![1.0, 0.0, 0.0, 0.0], 3, None)
        .await
        .expect("vector search");
    assert_eq!(results.len(), 3);
    assert_eq!(results[0].id, id_a);
    assert_eq!(results[1].id, id_c);
    assert_eq!(results[2].id, id_b);

    let nearest = &results[0];
    assert_eq!(
        nearest.payload.get("tag").and_then(|v| v.kind.as_ref()),
        Some(&Kind::StringValue("a".to_string()))
    );
    let vector = nearest.vector.as_ref().expect("returned vector");
    assert_eq!(vector.len(), 4);

    let mut seen: Vec<Option<PointId>> = Vec::new();
    let mut offset: Option<PointId> = None;
    loop {
        let page = store
            .scroll(&collection, 2, offset.clone(), None)
            .await
            .expect("scroll");
        seen.extend(ids_of(&page.points));
        offset = page.next_page_offset.clone();
        if offset.is_none() || page.points.is_empty() {
            break;
        }
    }
    assert_eq!(seen.len(), 3);
    assert!(seen.contains(&Some(id_a.clone())));
    assert!(seen.contains(&Some(id_b.clone())));
    assert!(seen.contains(&Some(id_c.clone())));

    let filter = Filter::must([Condition::matches("tag", "b".to_string())]);
    let tagged_b = store
        .search(&collection, vec![1.0, 0.0, 0.0, 0.0], 10, Some(filter))
        .await
        .expect("filtered search");
    assert_eq!(tagged_b.len(), 2);

    store
        .delete_by_ids(&collection, vec![id_a.clone()])
        .await
        .expect("delete by id");
    let remaining = store
        .search(&collection, vec![1.0, 0.0, 0.0, 0.0], 10, None)
        .await
        .expect("search after delete");
    assert_eq!(remaining.len(), 2);
    let remaining_ids: Vec<PointId> = remaining.into_iter().map(|r| r.id).collect();
    assert!(remaining_ids.contains(&id_b));
    assert!(remaining_ids.contains(&id_c));

    store
        .upsert(&collection, Vec::new(), 0)
        .await
        .expect("empty upsert");

    store
        .upsert(
            &collection,
            vec![point("b", vec![1.0, 0.0, 0.0, 0.0], "updated")],
            0,
        )
        .await
        .expect("replace point");
    let replaced = store
        .search(&collection, vec![1.0, 0.0, 0.0, 0.0], 1, None)
        .await
        .expect("search replaced point");
    assert_eq!(replaced.len(), 1);
    assert_eq!(replaced[0].id, id_b);
    assert_eq!(
        replaced[0].payload.get("tag").and_then(|v| v.kind.as_ref()),
        Some(&Kind::StringValue("updated".to_string()))
    );
}

async fn all_ids(store: &Arc<dyn QdrantStore>, collection: &str) -> Vec<PointId> {
    let mut ids = Vec::new();
    let mut offset = None;
    loop {
        let page = store
            .scroll(collection, 1000, offset, None)
            .await
            .expect("scroll");
        ids.extend(page.points.into_iter().filter_map(|p| p.id));
        match page.next_page_offset {
            Some(next) => offset = Some(next),
            None => return ids,
        }
    }
}

fn owned_point(id_key: &str, dataset: &str, generation: i64) -> PointData {
    let mut point = point(id_key, vec![1.0, 0.0], "a");
    point.payload.insert(
        qdrant::payload::DATASET_KEY.to_string(),
        string_value(dataset),
    );
    point.payload.insert(
        qdrant::payload::GENERATION_KEY.to_string(),
        generation.into(),
    );
    point
}

#[tokio::test]
async fn filtered_deletes_and_scrolls_stay_inside_one_dataset() {
    let store = connect().await;
    let collection = unique_collection("spice-qdrant-delete-it");
    store
        .ensure_collection(&collection, 2, Distance::Dot)
        .await
        .expect("ensure collection");
    let mut points: Vec<PointData> = (0..2500)
        .map(|i| owned_point(&format!("docs-{i}"), "docs", if i < 2000 { 1 } else { 2 }))
        .collect();
    points.push(owned_point("other-0", "other", 1));
    points.push(point("foreign-0", vec![1.0, 0.0], "a"));
    store
        .upsert(&collection, points, 0)
        .await
        .expect("upsert points");
    assert_eq!(all_ids(&store, &collection).await.len(), 2502);

    let docs = Condition::matches(qdrant::payload::DATASET_KEY, "docs".to_string());
    let page = store
        .scroll(&collection, 5000, None, Some(Filter::must([docs.clone()])))
        .await
        .expect("filtered scroll");
    assert_eq!(page.points.len(), 2500);

    let err = store
        .delete_by_filter(&collection, Filter::default())
        .await
        .expect_err("an empty filter must be refused");
    assert!(
        matches!(err, qdrant::Error::EmptyDeleteFilter { .. }),
        "expected EmptyDeleteFilter, got {err}"
    );
    assert_eq!(all_ids(&store, &collection).await.len(), 2502);

    let mut stale = Filter::must([docs.clone()]);
    stale
        .must_not
        .push(Condition::matches(qdrant::payload::GENERATION_KEY, 2));
    store
        .delete_by_filter(&collection, stale)
        .await
        .expect("delete older generations of 'docs'");
    let ids = all_ids(&store, &collection).await;
    assert_eq!(
        ids.len(),
        502,
        "500 current 'docs' points, 'other', and the foreign point"
    );
    for kept in ["docs-2000", "docs-2499", "other-0", "foreign-0"] {
        assert!(
            ids.contains(&qdrant::payload::point_id_from_values(&[kept.to_string()])),
            "{kept} must survive"
        );
    }

    store
        .delete_by_filter(&collection, Filter::must([docs]))
        .await
        .expect("delete every 'docs' point");
    let ids = all_ids(&store, &collection).await;
    assert_eq!(ids.len(), 2);

    store
        .delete_by_ids(&collection, ids)
        .await
        .expect("delete by id");
    assert!(all_ids(&store, &collection).await.is_empty());
}

#[tokio::test]
async fn concurrent_ensure_collection_calls_all_succeed() {
    let store = connect().await;
    let collection = unique_collection("spice-qdrant-race-it");
    let attempts = (0..8).map(|_| {
        let store = Arc::clone(&store);
        let collection = collection.clone();
        tokio::spawn(async move {
            store
                .ensure_collection(&collection, 4, Distance::Cosine)
                .await
        })
    });
    for attempt in futures::future::join_all(attempts).await {
        attempt
            .expect("task")
            .expect("every concurrent ensure_collection must succeed");
    }
    let err = store
        .ensure_collection(&collection, 3, Distance::Cosine)
        .await
        .expect_err("a mismatched collection still fails");
    assert!(
        matches!(err, qdrant::Error::CollectionMismatch { .. }),
        "{err}"
    );
}
