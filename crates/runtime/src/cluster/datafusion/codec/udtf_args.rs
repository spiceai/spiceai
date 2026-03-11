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

//! Serializable argument types for UDTFs (User-Defined Table Functions).
//!
//! This module re-exports the protobuf-generated types from `runtime_proto` and
//! provides convenience conversions for UDTF arguments during distributed query
//! execution with Ballista.
//!
//! When a query containing a UDTF is distributed across a cluster, the UDTF
//! arguments are serialized using protobuf and sent to executors so they can
//! reconstruct the `TableProvider`.

// Re-export the protobuf types for use in the codec
pub use runtime_proto::{
    ListUdfsArgs, RrfArgs, RrfNestedQuery, RrfTextSearchQuery, RrfVectorSearchQuery,
    TextSearchArgs, UdtfArgs, VectorSearchArgs,
};

use runtime_proto::rrf_nested_query::Query;
use runtime_proto::udtf_args::Args;

/// Extension trait for `UdtfArgs` to provide convenient construction.
pub trait UdtfArgsExt {
    /// Create a `ListUdfs` variant.
    fn list_udfs() -> UdtfArgs;

    /// Create a `TextSearch` variant.
    fn text_search(args: TextSearchArgs) -> UdtfArgs;

    /// Create a `VectorSearch` variant.
    fn vector_search(args: VectorSearchArgs) -> UdtfArgs;

    /// Create an `Rrf` variant.
    fn rrf(args: RrfArgs) -> UdtfArgs;
}

impl UdtfArgsExt for UdtfArgs {
    fn list_udfs() -> UdtfArgs {
        UdtfArgs {
            args: Some(Args::ListUdfs(ListUdfsArgs {})),
        }
    }

    fn text_search(args: TextSearchArgs) -> UdtfArgs {
        UdtfArgs {
            args: Some(Args::TextSearch(args)),
        }
    }

    fn vector_search(args: VectorSearchArgs) -> UdtfArgs {
        UdtfArgs {
            args: Some(Args::VectorSearch(args)),
        }
    }

    fn rrf(args: RrfArgs) -> UdtfArgs {
        UdtfArgs {
            args: Some(Args::Rrf(args)),
        }
    }
}

/// Extension trait for `RrfNestedQuery` to provide convenient construction.
pub trait RrfNestedQueryExt {
    /// Create a text search nested query.
    fn text_search(args: TextSearchArgs, rank_weight: Option<f64>) -> RrfNestedQuery;

    /// Create a vector search nested query.
    fn vector_search(args: VectorSearchArgs, rank_weight: Option<f64>) -> RrfNestedQuery;
}

impl RrfNestedQueryExt for RrfNestedQuery {
    fn text_search(args: TextSearchArgs, rank_weight: Option<f64>) -> RrfNestedQuery {
        RrfNestedQuery {
            query: Some(Query::TextSearch(RrfTextSearchQuery {
                args: Some(args),
                rank_weight,
            })),
        }
    }

    fn vector_search(args: VectorSearchArgs, rank_weight: Option<f64>) -> RrfNestedQuery {
        RrfNestedQuery {
            query: Some(Query::VectorSearch(RrfVectorSearchQuery {
                args: Some(args),
                rank_weight,
            })),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message;

    #[test]
    fn test_list_udfs_serialization() {
        let args = UdtfArgs::list_udfs();
        let bytes = args.encode_to_vec();
        let deserialized = UdtfArgs::decode(bytes.as_slice()).expect("decode");
        assert!(matches!(deserialized.args, Some(Args::ListUdfs(_))));
    }

    #[test]
    fn test_text_search_serialization() {
        let args = UdtfArgs::text_search(TextSearchArgs {
            table: "my_catalog.my_schema.my_table".to_string(),
            query: "hello world".to_string(),
            column: Some("content".to_string()),
            limit: Some(10),
            include_score: Some(true),
        });
        let bytes = args.encode_to_vec();
        let deserialized = UdtfArgs::decode(bytes.as_slice()).expect("decode");

        if let Some(Args::TextSearch(text_args)) = deserialized.args {
            assert_eq!(text_args.table, "my_catalog.my_schema.my_table");
            assert_eq!(text_args.query, "hello world");
            assert_eq!(text_args.column, Some("content".to_string()));
            assert_eq!(text_args.limit, Some(10));
            assert_eq!(text_args.include_score, Some(true));
        } else {
            panic!("Expected TextSearch variant");
        }
    }

    #[test]
    fn test_vector_search_serialization() {
        let args = UdtfArgs::vector_search(VectorSearchArgs {
            table: "embeddings_table".to_string(),
            query: "semantic query".to_string(),
            column: None,
            limit: Some(5),
            include_score: None,
        });
        let bytes = args.encode_to_vec();
        let deserialized = UdtfArgs::decode(bytes.as_slice()).expect("decode");

        if let Some(Args::VectorSearch(vector_args)) = deserialized.args {
            assert_eq!(vector_args.table, "embeddings_table");
            assert_eq!(vector_args.query, "semantic query");
            assert_eq!(vector_args.column, None);
            assert_eq!(vector_args.limit, Some(5));
            assert_eq!(vector_args.include_score, None);
        } else {
            panic!("Expected VectorSearch variant");
        }
    }

    #[test]
    fn test_rrf_serialization() {
        let args = UdtfArgs::rrf(RrfArgs {
            queries: vec![
                RrfNestedQuery::text_search(
                    TextSearchArgs {
                        table: "docs".to_string(),
                        query: "rust programming".to_string(),
                        column: None,
                        limit: Some(10),
                        include_score: Some(true),
                    },
                    Some(1.0),
                ),
                RrfNestedQuery::vector_search(
                    VectorSearchArgs {
                        table: "docs".to_string(),
                        query: "rust programming".to_string(),
                        column: Some("embedding".to_string()),
                        limit: Some(10),
                        include_score: Some(true),
                    },
                    Some(2.0),
                ),
            ],
            k: Some(60.0),
            join_key: Some("id".to_string()),
            time_column: None,
            recency_decay: None,
            decay_constant: None,
            decay_scale_secs: None,
            decay_window_secs: None,
        });
        let bytes = args.encode_to_vec();
        let deserialized = UdtfArgs::decode(bytes.as_slice()).expect("decode");

        if let Some(Args::Rrf(rrf_args)) = deserialized.args {
            assert_eq!(rrf_args.queries.len(), 2);
            assert_eq!(rrf_args.k, Some(60.0));
            assert_eq!(rrf_args.join_key, Some("id".to_string()));
        } else {
            panic!("Expected Rrf variant");
        }
    }
}
