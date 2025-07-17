/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

#[cfg(feature = "bedrock")]
pub(crate) mod embeddings {
    use super::super::embedding::{EmbeddingTestCase, run_embedding_tests};
    use async_openai::types::EmbeddingInput;
    use spicepod::component::embeddings::Embeddings;
    use std::collections::HashMap;

    // Test data for Bedrock embeddings
    const TEST_TEXT: &str = "The food was delicious and the waiter...";
    const TEST_TEXTS: &[&str] = &[
        "The food was delicious and the waiter was very friendly.",
        "I had a great experience at this restaurant.",
        "The service was outstanding and the atmosphere was perfect.",
    ];

    #[must_use]
    pub fn create_titan_v1_embedding() -> Embeddings {
        let mut params = HashMap::new();
        params.insert("aws_region".to_string(), "us-east-1".to_string());
        params.insert("normalize".to_string(), "true".to_string());
        params.insert("dimensions".to_string(), "1024".to_string());

        Embeddings {
            from: "bedrock:amazon.titan-embed-text-v1".to_string(),
            name: "titan-v1".to_string(),
            files: vec![],
            params: with_auth(params),
            datasets: vec![],
            depends_on: vec![],
            metrics: None,
        }
    }

    #[must_use]
    pub fn create_titan_v2_embedding() -> Embeddings {
        let mut params = HashMap::new();
        params.insert("aws_region".to_string(), "us-east-1".to_string());
        params.insert("normalize".to_string(), "true".to_string());
        params.insert("dimensions".to_string(), "512".to_string());

        Embeddings {
            from: "bedrock:amazon.titan-embed-text-v2:0".to_string(),
            name: "titan-v2".to_string(),
            files: vec![],
            params: with_auth(params),
            datasets: vec![],
            depends_on: vec![],
            metrics: None,
        }
    }

    #[must_use]
    pub fn create_cohere_english_embedding() -> Embeddings {
        let mut params = HashMap::new();
        params.insert("aws_region".to_string(), "us-east-1".to_string());
        params.insert("input_type".to_string(), "search_document".to_string());
        params.insert("truncate".to_string(), "END".to_string());

        Embeddings {
            from: "bedrock:cohere.embed-english-v3".to_string(),
            name: "cohere-english".to_string(),
            files: vec![],
            params: with_auth(params),
            datasets: vec![],
            depends_on: vec![],
            metrics: None,
        }
    }

    #[must_use]
    pub fn create_cohere_multilingual_embedding() -> Embeddings {
        let mut params = HashMap::new();
        params.insert("aws_region".to_string(), "us-east-1".to_string());
        params.insert("input_type".to_string(), "classification".to_string());
        params.insert("truncate".to_string(), "NONE".to_string());

        Embeddings {
            from: "bedrock:cohere.embed-multilingual-v3".to_string(),
            name: "cohere-multilingual".to_string(),
            files: vec![],
            params: with_auth(params),
            datasets: vec![],
            depends_on: vec![],
            metrics: None,
        }
    }

    fn with_auth(mut params: HashMap<String, String>) -> HashMap<String, String> {
        params.insert(
            "aws_access_key_id".to_string(),
            "${env:AWS_BEDROCK_KEY}".to_string(),
        );
        params.insert(
            "aws_secret_access_key".to_string(),
            "${env:AWS_BEDROCK_SECRET}".to_string(),
        );
        params
    }

    #[tokio::test]
    #[cfg_attr(
        not(feature = "extended_tests"),
        ignore = "Extended test - run with --features extended_tests"
    )]
    async fn test_titan_v1_embeddings() {
        let model = create_titan_v1_embedding();
        let tests = vec![
            EmbeddingTestCase {
                input: EmbeddingInput::String(TEST_TEXT.to_string()),
                model_name: "titan-v1",
                encoding_format: Some("float"),
                user: None,
                dimensions: Some(1024),
                test_id: "single_text_float",
            },
            EmbeddingTestCase {
                input: EmbeddingInput::String(TEST_TEXT.to_string()),
                model_name: "titan-v1",
                encoding_format: Some("base64"),
                user: None,
                dimensions: Some(1024),
                test_id: "single_text_base64",
            },
            EmbeddingTestCase {
                input: EmbeddingInput::StringArray(
                    TEST_TEXTS.iter().map(|s| (*s).to_string()).collect(),
                ),
                model_name: "titan-v1",
                encoding_format: Some("float"),
                user: None,
                dimensions: Some(1024),
                test_id: "multiple_texts_float",
            },
        ];

        run_embedding_tests(vec![model], tests)
            .await
            .expect("Titan V1 embedding tests should pass");
    }

    #[tokio::test]
    #[cfg_attr(
        not(feature = "extended_tests"),
        ignore = "Extended test - run with --features extended_tests"
    )]
    async fn test_titan_v2_embeddings() {
        let model = create_titan_v2_embedding();
        let tests = vec![
            EmbeddingTestCase {
                input: EmbeddingInput::String(TEST_TEXT.to_string()),
                model_name: "titan-v2",
                encoding_format: Some("float"),
                user: None,
                dimensions: Some(512),
                test_id: "single_text_float_512",
            },
            EmbeddingTestCase {
                input: EmbeddingInput::StringArray(
                    TEST_TEXTS.iter().map(|s| (*s).to_string()).collect(),
                ),
                model_name: "titan-v2",
                encoding_format: Some("float"),
                user: None,
                dimensions: Some(512),
                test_id: "multiple_texts_float_512",
            },
        ];

        run_embedding_tests(vec![model], tests)
            .await
            .expect("Titan V2 embedding tests should pass");
    }

    #[tokio::test]
    #[cfg_attr(
        not(feature = "extended_tests"),
        ignore = "Extended test - run with --features extended_tests"
    )]
    async fn test_cohere_english_embeddings() {
        let model = create_cohere_english_embedding();
        let tests = vec![
            EmbeddingTestCase {
                input: EmbeddingInput::String(TEST_TEXT.to_string()),
                model_name: "cohere-english",
                encoding_format: Some("float"),
                user: None,
                dimensions: None, // Cohere models have fixed dimensions
                test_id: "single_text_float",
            },
            EmbeddingTestCase {
                input: EmbeddingInput::StringArray(
                    TEST_TEXTS.iter().map(|s| (*s).to_string()).collect(),
                ),
                model_name: "cohere-english",
                encoding_format: Some("float"),
                user: None,
                dimensions: None,
                test_id: "multiple_texts_float",
            },
        ];

        run_embedding_tests(vec![model], tests)
            .await
            .expect("Cohere English embedding tests should pass");
    }

    #[tokio::test]
    #[cfg_attr(
        not(feature = "extended_tests"),
        ignore = "Extended test - run with --features extended_tests"
    )]
    async fn test_cohere_multilingual_embeddings() {
        let model = create_cohere_multilingual_embedding();
        let tests = vec![
            EmbeddingTestCase {
                input: EmbeddingInput::String("Bonjour, comment ça va?".to_string()),
                model_name: "cohere-multilingual",
                encoding_format: Some("float"),
                user: None,
                dimensions: None,
                test_id: "french_text_float",
            },
            EmbeddingTestCase {
                input: EmbeddingInput::StringArray(vec![
                    "Hello, how are you?".to_string(),
                    "Hola, ¿cómo estás?".to_string(),
                    "Bonjour, comment ça va?".to_string(),
                ]),
                model_name: "cohere-multilingual",
                encoding_format: Some("float"),
                user: None,
                dimensions: None,
                test_id: "multilingual_texts_float",
            },
        ];

        run_embedding_tests(vec![model], tests)
            .await
            .expect("Cohere Multilingual embedding tests should pass");
    }

    #[tokio::test]
    #[cfg_attr(
        not(feature = "extended_tests"),
        ignore = "Extended test - run with --features extended_tests"
    )]
    async fn test_all_bedrock_models() {
        let models = vec![
            create_titan_v1_embedding(),
            create_titan_v2_embedding(),
            create_cohere_english_embedding(),
            create_cohere_multilingual_embedding(),
        ];

        let tests = vec![
            EmbeddingTestCase {
                input: EmbeddingInput::String(TEST_TEXT.to_string()),
                model_name: "titan-v1",
                encoding_format: Some("float"),
                user: None,
                dimensions: Some(1024),
                test_id: "comparison_test",
            },
            EmbeddingTestCase {
                input: EmbeddingInput::String(TEST_TEXT.to_string()),
                model_name: "titan-v2",
                encoding_format: Some("float"),
                user: None,
                dimensions: Some(512),
                test_id: "comparison_test",
            },
            EmbeddingTestCase {
                input: EmbeddingInput::String(TEST_TEXT.to_string()),
                model_name: "cohere-english",
                encoding_format: Some("float"),
                user: None,
                dimensions: None,
                test_id: "comparison_test",
            },
            EmbeddingTestCase {
                input: EmbeddingInput::String(TEST_TEXT.to_string()),
                model_name: "cohere-multilingual",
                encoding_format: Some("float"),
                user: None,
                dimensions: None,
                test_id: "comparison_test",
            },
        ];

        run_embedding_tests(models, tests)
            .await
            .expect("All Bedrock embedding models should work");
    }

    /// Test for handling various input types and edge cases
    #[tokio::test]
    #[cfg_attr(
        not(feature = "extended_tests"),
        ignore = "Extended test - run with --features extended_tests"
    )]
    async fn test_bedrock_edge_cases() {
        let model = create_titan_v1_embedding();
        let tests = vec![
            // Empty string test
            EmbeddingTestCase {
                input: EmbeddingInput::String(String::new()),
                model_name: "titan-v1",
                encoding_format: Some("float"),
                user: None,
                dimensions: Some(1024),
                test_id: "empty_string",
            },
            // Very long string test (should be truncated)
            EmbeddingTestCase {
                input: EmbeddingInput::String("a".repeat(10000)),
                model_name: "titan-v1",
                encoding_format: Some("float"),
                user: None,
                dimensions: Some(1024),
                test_id: "long_string",
            },
            // Special characters test
            EmbeddingTestCase {
                input: EmbeddingInput::String(
                    "Special chars: !@#$%^&*()_+-=[]{}|;':,.<>?".to_string(),
                ),
                model_name: "titan-v1",
                encoding_format: Some("float"),
                user: None,
                dimensions: Some(1024),
                test_id: "special_chars",
            },
            // Unicode test
            EmbeddingTestCase {
                input: EmbeddingInput::String("Unicode: 你好世界 🌍 αβγδε".to_string()),
                model_name: "titan-v1",
                encoding_format: Some("float"),
                user: None,
                dimensions: Some(1024),
                test_id: "unicode",
            },
        ];

        run_embedding_tests(vec![model], tests)
            .await
            .expect("Bedrock edge case tests should pass");
    }

    /// Test different dimension configurations for Titan models
    #[tokio::test]
    #[cfg_attr(
        not(feature = "extended_tests"),
        ignore = "Extended test - run with --features extended_tests"
    )]
    async fn test_titan_dimensions() {
        let mut titan_256 = create_titan_v1_embedding();
        titan_256.name = "titan-256".to_string();
        titan_256
            .params
            .insert("dimensions".to_string(), "256".to_string());

        let mut titan_512 = create_titan_v1_embedding();
        titan_512.name = "titan-512".to_string();
        titan_512
            .params
            .insert("dimensions".to_string(), "512".to_string());

        let mut titan_1024 = create_titan_v1_embedding();
        titan_1024.name = "titan-1024".to_string();
        titan_1024
            .params
            .insert("dimensions".to_string(), "1024".to_string());

        let models = vec![titan_256, titan_512, titan_1024];
        let tests = vec![
            EmbeddingTestCase {
                input: EmbeddingInput::String(TEST_TEXT.to_string()),
                model_name: "titan-256",
                encoding_format: Some("float"),
                user: None,
                dimensions: Some(256),
                test_id: "dim_256",
            },
            EmbeddingTestCase {
                input: EmbeddingInput::String(TEST_TEXT.to_string()),
                model_name: "titan-512",
                encoding_format: Some("float"),
                user: None,
                dimensions: Some(512),
                test_id: "dim_512",
            },
            EmbeddingTestCase {
                input: EmbeddingInput::String(TEST_TEXT.to_string()),
                model_name: "titan-1024",
                encoding_format: Some("float"),
                user: None,
                dimensions: Some(1024),
                test_id: "dim_1024",
            },
        ];

        run_embedding_tests(models, tests)
            .await
            .expect("Titan dimension tests should pass");
    }
}
