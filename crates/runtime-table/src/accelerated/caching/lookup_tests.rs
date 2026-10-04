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

use arrow::datatypes::{DataType, Field, Schema};
use datafusion::prelude::{col, lit};

use super::{cache_lookup_filters, writer};

fn schema() -> Schema {
    Schema::new(vec![
        Field::new("request_path", DataType::Utf8, false),
        Field::new("request_query", DataType::Utf8, true),
        Field::new("request_body", DataType::Utf8, true),
        Field::new("content", DataType::Utf8, false),
        Field::new("response_status", DataType::UInt16, false),
    ])
}

#[test]
fn response_predicate_preserves_absent_request_identity() {
    let path = col("request_path").eq(lit("/items"));
    let content = col("content").eq(lit("ok"));
    assert_eq!(
        cache_lookup_filters(&schema(), &[path.clone(), content.clone()]),
        vec![
            path,
            col("request_query").is_null(),
            col("request_body").is_null(),
            content
        ],
    );
}

#[test]
fn nested_conjunction_retains_all_response_predicates() {
    let path = col("request_path").eq(lit("/items"));
    let content = col("content").eq(lit("ok"));
    let status = col("response_status").eq(lit(200_u16));
    assert_eq!(
        cache_lookup_filters(
            &schema(),
            &[path.clone().and(content.clone().and(status.clone()))]
        ),
        vec![
            path,
            col("request_query").is_null(),
            col("request_body").is_null(),
            content,
            status
        ],
    );
}

#[test]
fn explicit_empty_query_and_body_remain_distinct_from_absence() {
    let path = col("request_path").eq(lit("/items"));
    let query = col("request_query").eq(lit(""));
    let body = col("request_body").eq(lit(""));
    let content = col("content").eq(lit("ok"));
    let filters = vec![path, query, body, content];
    assert_eq!(cache_lookup_filters(&schema(), &filters), filters);
}

#[test]
fn explicit_query_and_response_predicate_preserve_base_path_default() {
    let query = col("request_query").eq(lit("page=2"));
    let content = col("content").eq(lit("ok"));
    assert_eq!(
        cache_lookup_filters(&schema(), &[query.clone(), content.clone()]),
        vec![
            col("request_path").eq(lit("")),
            query,
            col("request_body").is_null(),
            content
        ],
    );
}

#[test]
fn scans_without_request_predicates_keep_their_filters() {
    assert!(cache_lookup_filters(&schema(), &[]).is_empty());
    let filters = vec![
        col("content")
            .eq(lit("ok"))
            .and(col("response_status").eq(lit(200_u16))),
    ];
    assert_eq!(cache_lookup_filters(&schema(), &filters), filters);
}

#[test]
fn ambiguous_request_predicates_keep_their_filters() {
    let path = col("request_path").eq(lit("/items"));
    for filters in [
        vec![
            path.clone(),
            col("request_query").like(lit("page=%")),
            col("content").eq(lit("ok")),
        ],
        vec![path.clone().or(col("content").eq(lit("ok")))],
        vec![
            path,
            col("request_path").eq(lit("/other")),
            col("content").eq(lit("ok")),
        ],
    ] {
        assert_eq!(cache_lookup_filters(&schema(), &filters), filters);
    }
}

#[test]
fn missing_storage_metadata_does_not_remove_response_predicates() {
    let schema = Schema::new(vec![Field::new("request_path", DataType::Utf8, false)]);
    let filters = vec![
        col("request_path").eq(lit("/items")),
        col("content").eq(lit("ok")),
    ];
    assert_eq!(cache_lookup_filters(&schema, &filters), filters);
}

#[test]
fn lookup_constraints_do_not_authorize_filtered_replacement() {
    let filters = vec![
        col("request_path").eq(lit("/items")),
        col("content").eq(lit("ok")),
    ];
    assert_eq!(cache_lookup_filters(&schema(), &filters).len(), 4);
    assert!(writer::canonical_request_filters(&filters).is_none());
}
