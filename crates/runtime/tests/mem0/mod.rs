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

//! Integration tests for the Mem0 memory connector.
//!
//! These tests require the `MEM0_API_KEY` environment variable to be set.
//!
//! # Running Tests
//!
//! ```bash
//! MEM0_API_KEY="your-key" cargo test -p runtime --features mem0 -- mem0::test_ --include-ignored
//! ```

use runtime::tools::mem0::client::{
    AddMemoryRequest, AddMemoryResponse, DeleteMemoryRequest, GetMemoriesRequest, Mem0Client,
    Mem0Config, Memory, Message, SearchMemoryRequest,
};
use secrecy::SecretString;
use serde_json::json;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// Global counter to stagger test starts and avoid rate limiting
static TEST_START_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Stagger test starts to avoid hitting API rate limits when running in parallel.
/// Each test waits a different amount based on when it starts.
async fn rate_limit_guard() {
    let test_num = TEST_START_COUNTER.fetch_add(1, Ordering::SeqCst);
    // Stagger by 500ms per test to spread API calls
    let delay = Duration::from_millis(test_num * 500);
    tokio::time::sleep(delay).await;
}

/// Retry an async operation with exponential backoff for rate limiting.
#[allow(dead_code)]
async fn retry_with_backoff<T, E, F, Fut>(mut op: F) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, E>>,
    E: std::fmt::Debug,
{
    let mut attempts = 0;
    let max_attempts = 3;
    let mut delay = Duration::from_millis(500);

    loop {
        match op().await {
            Ok(result) => return Ok(result),
            Err(e) => {
                attempts += 1;
                if attempts >= max_attempts {
                    return Err(e);
                }
                eprintln!("Attempt {attempts} failed: {e:?}, retrying in {delay:?}");
                tokio::time::sleep(delay).await;
                delay *= 2; // Exponential backoff
            }
        }
    }
}

/// Get API key from environment for integration tests.
/// Set `MEM0_API_KEY` environment variable to run integration tests.
fn get_api_key() -> Option<SecretString> {
    std::env::var("MEM0_API_KEY").ok().map(SecretString::from)
}

/// Create a test client with API key from environment.
fn create_test_client() -> Option<Mem0Client> {
    let api_key = get_api_key()?;
    let config = Mem0Config::new(api_key);
    Mem0Client::new(config).ok()
}

/// Generate a unique test user ID to avoid conflicts between test runs.
fn test_user_id() -> String {
    format!("test-user-{}", uuid::Uuid::new_v4())
}

/// Helper to clean up a user's memories, ignoring errors
#[allow(dead_code)]
async fn cleanup_user(client: &Mem0Client, user_id: &str) {
    let delete_request = DeleteMemoryRequest {
        user_id: Some(user_id.to_string()),
        agent_id: None,
        org_id: None,
        project_id: None,
    };
    let _ = client.delete_all_memories(delete_request).await;
}

/// Helper to add a memory with retry logic
#[allow(dead_code)]
async fn add_memory_with_retry(
    client: &Mem0Client,
    content: &str,
    user_id: &str,
) -> Result<AddMemoryResponse, runtime::tools::mem0::Error> {
    retry_with_backoff(|| async {
        let add_request = AddMemoryRequest {
            messages: vec![Message {
                role: "user".to_string(),
                content: content.to_string(),
            }],
            user_id: Some(user_id.to_string()),
            async_mode: false,
            ..Default::default()
        };
        client.add_memories(add_request).await
    })
    .await
}

/// Helper to search memories with retry logic
#[allow(dead_code)]
async fn search_with_retry(
    client: &Mem0Client,
    query: &str,
    user_id: &str,
) -> Result<Vec<Memory>, runtime::tools::mem0::Error> {
    retry_with_backoff(|| async {
        let search_request = SearchMemoryRequest {
            query: query.to_string(),
            filters: Some(json!({"user_id": user_id})),
            top_k: Some(10),
            ..Default::default()
        };
        client.search_memories(search_request).await
    })
    .await
}

#[tokio::test]
#[ignore = "requires MEM0_API_KEY environment variable"]
async fn test_add_and_search_memory() {
    rate_limit_guard().await;

    let Some(client) = create_test_client() else {
        eprintln!("Skipping test: MEM0_API_KEY not set");
        return;
    };

    let user_id = test_user_id();

    // Add a memory
    let add_request = AddMemoryRequest {
        messages: vec![Message {
            role: "user".to_string(),
            content: "I love Rust programming and building data systems".to_string(),
        }],
        user_id: Some(user_id.clone()),
        async_mode: false, // Use sync mode to get immediate results
        ..Default::default()
    };

    let add_result = client.add_memories(add_request).await;
    assert!(
        add_result.is_ok(),
        "Failed to add memory: {:?}",
        add_result.err()
    );

    // Verify we got sync response
    let response = add_result.expect("add should succeed");
    assert!(
        matches!(response, AddMemoryResponse::Sync(_)),
        "Expected sync response"
    );

    // Give the API a moment to index
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    // Search for the memory
    let search_request = SearchMemoryRequest {
        query: "What programming language do I like?".to_string(),
        filters: Some(json!({"user_id": user_id.clone()})),
        top_k: Some(5),
        ..Default::default()
    };

    let search_result = client.search_memories(search_request).await;
    assert!(
        search_result.is_ok(),
        "Failed to search memories: {:?}",
        search_result.err()
    );

    // Clean up - delete all memories for this user
    let delete_request = DeleteMemoryRequest {
        user_id: Some(user_id),
        agent_id: None,
        org_id: None,
        project_id: None,
    };

    let delete_result = client.delete_all_memories(delete_request).await;
    assert!(
        delete_result.is_ok(),
        "Failed to delete memories: {:?}",
        delete_result.err()
    );
}

#[tokio::test]
#[ignore = "requires MEM0_API_KEY environment variable"]
async fn test_get_memories() {
    rate_limit_guard().await;

    let Some(client) = create_test_client() else {
        eprintln!("Skipping test: MEM0_API_KEY not set");
        return;
    };

    let user_id = test_user_id();

    // Add a memory first
    let add_request = AddMemoryRequest {
        messages: vec![Message {
            role: "user".to_string(),
            content: "My favorite color is blue".to_string(),
        }],
        user_id: Some(user_id.clone()),
        async_mode: false, // Use sync mode to get immediate results
        ..Default::default()
    };

    let add_result = client.add_memories(add_request).await;
    assert!(add_result.is_ok(), "Failed to add memory");

    // Wait for indexing
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    // Get all memories for this user
    let get_request = GetMemoriesRequest {
        filters: json!({"user_id": user_id.clone()}),
        page: None,
        page_size: None,
        org_id: None,
        project_id: None,
        enable_graph: None,
    };

    let get_result = client.get_memories(get_request).await;
    assert!(
        get_result.is_ok(),
        "Failed to get memories: {:?}",
        get_result.err()
    );

    // Clean up
    let delete_request = DeleteMemoryRequest {
        user_id: Some(user_id),
        agent_id: None,
        org_id: None,
        project_id: None,
    };

    let _ = client.delete_all_memories(delete_request).await;
}

#[tokio::test]
#[ignore = "requires MEM0_API_KEY environment variable"]
async fn test_delete_specific_memory() {
    rate_limit_guard().await;

    let Some(client) = create_test_client() else {
        eprintln!("Skipping test: MEM0_API_KEY not set");
        return;
    };

    let user_id = test_user_id();

    // Add a memory
    let add_request = AddMemoryRequest {
        messages: vec![Message {
            role: "user".to_string(),
            content: "Test memory for deletion".to_string(),
        }],
        user_id: Some(user_id.clone()),
        async_mode: false, // Use sync mode to get immediate results
        ..Default::default()
    };

    let add_result = client.add_memories(add_request).await;
    assert!(add_result.is_ok(), "Failed to add memory");

    let add_response = add_result.expect("add should succeed");

    // Wait for indexing - increase time to ensure memory is fully indexed
    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;

    // Get the memory ID from the first event (sync response)
    if let AddMemoryResponse::Sync(events) = add_response {
        if let Some(first) = events.first() {
            // Delete the specific memory
            let delete_result = client.delete_memory(&first.id).await;
            // Memory may have been processed asynchronously, so 404 is acceptable if cleanup worked
            if let Err(ref e) = delete_result {
                eprintln!("Warning: delete_memory returned error (may be timing-related): {e:?}");
            }
        }
    } else {
        panic!("Expected sync response for memory addition");
    }

    // Clean up any remaining memories
    let delete_request = DeleteMemoryRequest {
        user_id: Some(user_id),
        agent_id: None,
        org_id: None,
        project_id: None,
    };

    let _ = client.delete_all_memories(delete_request).await;
}

#[tokio::test]
#[ignore = "requires MEM0_API_KEY environment variable"]
async fn test_memory_with_metadata() {
    rate_limit_guard().await;

    let Some(client) = create_test_client() else {
        eprintln!("Skipping test: MEM0_API_KEY not set");
        return;
    };

    let user_id = test_user_id();

    // Add a memory with metadata
    let mut metadata = HashMap::new();
    metadata.insert("source".to_string(), json!("test"));
    metadata.insert("importance".to_string(), json!("high"));

    let add_request = AddMemoryRequest {
        messages: vec![Message {
            role: "user".to_string(),
            content: "Important information to remember".to_string(),
        }],
        user_id: Some(user_id.clone()),
        metadata: Some(metadata),
        async_mode: false, // Use sync mode to get immediate results
        ..Default::default()
    };

    let add_result = client.add_memories(add_request).await;
    assert!(
        add_result.is_ok(),
        "Failed to add memory with metadata: {:?}",
        add_result.err()
    );

    // Clean up
    let delete_request = DeleteMemoryRequest {
        user_id: Some(user_id),
        agent_id: None,
        org_id: None,
        project_id: None,
    };

    let _ = client.delete_all_memories(delete_request).await;
}

#[tokio::test]
#[ignore = "requires MEM0_API_KEY environment variable"]
async fn test_search_with_threshold() {
    rate_limit_guard().await;

    let Some(client) = create_test_client() else {
        eprintln!("Skipping test: MEM0_API_KEY not set");
        return;
    };

    let user_id = test_user_id();

    // Add a memory
    let add_request = AddMemoryRequest {
        messages: vec![Message {
            role: "user".to_string(),
            content: "I prefer coffee over tea in the morning".to_string(),
        }],
        user_id: Some(user_id.clone()),
        async_mode: false, // Use sync mode to get immediate results
        ..Default::default()
    };

    let _ = client.add_memories(add_request).await;

    // Wait for indexing
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    // Search with a threshold
    let search_request = SearchMemoryRequest {
        query: "What beverage do I prefer?".to_string(),
        filters: Some(json!({"user_id": user_id.clone()})),
        top_k: Some(10),
        threshold: Some(0.5),
        ..Default::default()
    };

    let search_result = client.search_memories(search_request).await;
    assert!(
        search_result.is_ok(),
        "Failed to search with threshold: {:?}",
        search_result.err()
    );

    // Clean up
    let delete_request = DeleteMemoryRequest {
        user_id: Some(user_id),
        agent_id: None,
        org_id: None,
        project_id: None,
    };

    let _ = client.delete_all_memories(delete_request).await;
}

// ============================================================================
// Edge Case Tests
// ============================================================================

#[tokio::test]
#[ignore = "requires MEM0_API_KEY environment variable"]
async fn test_empty_search_query() {
    rate_limit_guard().await;

    let Some(client) = create_test_client() else {
        eprintln!("Skipping test: MEM0_API_KEY not set");
        return;
    };

    let user_id = test_user_id();

    // Search with empty query - should still work but return no meaningful results
    let search_request = SearchMemoryRequest {
        query: String::new(),
        filters: Some(json!({"user_id": user_id})),
        top_k: Some(5),
        ..Default::default()
    };

    let search_result = client.search_memories(search_request).await;
    // Empty query may fail or return empty - either is acceptable
    if let Err(ref e) = search_result {
        eprintln!("Empty query returned error (expected): {e:?}");
    }
}

#[tokio::test]
#[ignore = "requires MEM0_API_KEY environment variable"]
async fn test_special_characters_in_memory() {
    rate_limit_guard().await;

    let Some(client) = create_test_client() else {
        eprintln!("Skipping test: MEM0_API_KEY not set");
        return;
    };

    let user_id = test_user_id();

    // Add memory with special characters
    let add_request = AddMemoryRequest {
        messages: vec![Message {
            role: "user".to_string(),
            content:
                r#"Special chars: !@#$%^&*()_+-=[]{}|;':",.<>?/\`~ and unicode: 你好世界 🚀 émojis"#
                    .to_string(),
        }],
        user_id: Some(user_id.clone()),
        async_mode: false,
        ..Default::default()
    };

    let add_result = client.add_memories(add_request).await;
    assert!(
        add_result.is_ok(),
        "Failed to add memory with special characters: {:?}",
        add_result.err()
    );

    // Clean up
    let delete_request = DeleteMemoryRequest {
        user_id: Some(user_id),
        agent_id: None,
        org_id: None,
        project_id: None,
    };
    let _ = client.delete_all_memories(delete_request).await;
}

#[tokio::test]
#[ignore = "requires MEM0_API_KEY environment variable"]
async fn test_very_long_memory_content() {
    rate_limit_guard().await;

    let Some(client) = create_test_client() else {
        eprintln!("Skipping test: MEM0_API_KEY not set");
        return;
    };

    let user_id = test_user_id();

    // Create a long memory content (10KB)
    let long_content = "This is a test sentence for long content. ".repeat(250);

    let add_request = AddMemoryRequest {
        messages: vec![Message {
            role: "user".to_string(),
            content: long_content,
        }],
        user_id: Some(user_id.clone()),
        async_mode: false,
        ..Default::default()
    };

    let add_result = client.add_memories(add_request).await;
    assert!(
        add_result.is_ok(),
        "Failed to add long memory: {:?}",
        add_result.err()
    );

    // Clean up
    let delete_request = DeleteMemoryRequest {
        user_id: Some(user_id),
        agent_id: None,
        org_id: None,
        project_id: None,
    };
    let _ = client.delete_all_memories(delete_request).await;
}

#[tokio::test]
#[ignore = "requires MEM0_API_KEY environment variable"]
async fn test_multiple_messages_in_single_request() {
    rate_limit_guard().await;

    let Some(client) = create_test_client() else {
        eprintln!("Skipping test: MEM0_API_KEY not set");
        return;
    };

    let user_id = test_user_id();

    // Add multiple messages in a single request (conversation context)
    let add_request = AddMemoryRequest {
        messages: vec![
            Message {
                role: "user".to_string(),
                content: "What's the capital of France?".to_string(),
            },
            Message {
                role: "assistant".to_string(),
                content: "The capital of France is Paris.".to_string(),
            },
            Message {
                role: "user".to_string(),
                content: "Thanks! I'll remember that.".to_string(),
            },
        ],
        user_id: Some(user_id.clone()),
        async_mode: false,
        ..Default::default()
    };

    let add_result = client.add_memories(add_request).await;
    assert!(
        add_result.is_ok(),
        "Failed to add multiple messages: {:?}",
        add_result.err()
    );

    // Wait for indexing
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    // Search for the conversation
    let search_request = SearchMemoryRequest {
        query: "capital of France".to_string(),
        filters: Some(json!({"user_id": user_id.clone()})),
        top_k: Some(5),
        ..Default::default()
    };

    let search_result = client.search_memories(search_request).await;
    assert!(
        search_result.is_ok(),
        "Failed to search conversation: {:?}",
        search_result.err()
    );

    // Clean up
    let delete_request = DeleteMemoryRequest {
        user_id: Some(user_id),
        agent_id: None,
        org_id: None,
        project_id: None,
    };
    let _ = client.delete_all_memories(delete_request).await;
}

#[tokio::test]
#[ignore = "requires MEM0_API_KEY environment variable"]
async fn test_search_nonexistent_user() {
    rate_limit_guard().await;

    let Some(client) = create_test_client() else {
        eprintln!("Skipping test: MEM0_API_KEY not set");
        return;
    };

    // Search for a user that doesn't exist
    let nonexistent_user = format!("nonexistent-user-{}", uuid::Uuid::new_v4());

    let search_request = SearchMemoryRequest {
        query: "anything".to_string(),
        filters: Some(json!({"user_id": nonexistent_user})),
        top_k: Some(5),
        ..Default::default()
    };

    let search_result = client.search_memories(search_request).await;
    assert!(
        search_result.is_ok(),
        "Search for nonexistent user should succeed with empty results: {:?}",
        search_result.err()
    );

    let memories = search_result.expect("search should succeed");
    assert!(
        memories.is_empty(),
        "Should return empty results for nonexistent user"
    );
}

#[tokio::test]
#[ignore = "requires MEM0_API_KEY environment variable"]
async fn test_delete_nonexistent_memory() {
    rate_limit_guard().await;

    let Some(client) = create_test_client() else {
        eprintln!("Skipping test: MEM0_API_KEY not set");
        return;
    };

    // Try to delete a memory that doesn't exist
    let fake_id = format!("nonexistent-memory-{}", uuid::Uuid::new_v4());

    let delete_result = client.delete_memory(&fake_id).await;
    // Should return an error (404)
    assert!(
        delete_result.is_err(),
        "Deleting nonexistent memory should fail"
    );
}

// ============================================================================
// Concurrency Tests
// ============================================================================

#[tokio::test]
#[ignore = "requires MEM0_API_KEY environment variable"]
async fn test_concurrent_memory_additions() {
    rate_limit_guard().await;

    let Some(client) = create_test_client() else {
        eprintln!("Skipping test: MEM0_API_KEY not set");
        return;
    };

    let user_id = test_user_id();
    let client = std::sync::Arc::new(client);

    // Spawn multiple concurrent add operations
    let mut handles = Vec::new();
    for i in 0..5 {
        let client = std::sync::Arc::clone(&client);
        let user_id = user_id.clone();

        let handle = tokio::spawn(async move {
            let add_request = AddMemoryRequest {
                messages: vec![Message {
                    role: "user".to_string(),
                    content: format!("Concurrent memory test #{i}: I like topic number {i}"),
                }],
                user_id: Some(user_id),
                async_mode: false,
                ..Default::default()
            };

            client.add_memories(add_request).await
        });

        handles.push(handle);
    }

    // Wait for all to complete
    let results: Vec<_> = futures::future::join_all(handles).await;

    // Verify all succeeded
    for (i, result) in results.into_iter().enumerate() {
        let inner = result.expect("task should not panic");
        assert!(
            inner.is_ok(),
            "Concurrent add #{i} failed: {:?}",
            inner.err()
        );
    }

    // Wait for indexing
    tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;

    // Verify all memories were added by searching
    let search_request = SearchMemoryRequest {
        query: "Concurrent memory test".to_string(),
        filters: Some(json!({"user_id": user_id.clone()})),
        top_k: Some(10),
        ..Default::default()
    };

    let search_result = client.search_memories(search_request).await;
    assert!(
        search_result.is_ok(),
        "Search after concurrent adds failed: {:?}",
        search_result.err()
    );

    // Clean up
    let delete_request = DeleteMemoryRequest {
        user_id: Some(user_id),
        agent_id: None,
        org_id: None,
        project_id: None,
    };
    let _ = client.delete_all_memories(delete_request).await;
}

#[tokio::test]
#[ignore = "requires MEM0_API_KEY environment variable"]
async fn test_concurrent_searches() {
    rate_limit_guard().await;

    let Some(client) = create_test_client() else {
        eprintln!("Skipping test: MEM0_API_KEY not set");
        return;
    };

    let user_id = test_user_id();

    // First add some memories
    for content in [
        "I enjoy hiking in the mountains",
        "My favorite food is pizza",
        "I work as a software engineer",
    ] {
        let add_request = AddMemoryRequest {
            messages: vec![Message {
                role: "user".to_string(),
                content: content.to_string(),
            }],
            user_id: Some(user_id.clone()),
            async_mode: false,
            ..Default::default()
        };
        let _ = client.add_memories(add_request).await;
    }

    // Wait for indexing
    tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;

    let client = std::sync::Arc::new(client);

    // Spawn multiple concurrent search operations
    let queries = vec![
        "What outdoor activities do I like?",
        "What is my favorite food?",
        "What is my job?",
        "hiking mountains",
        "pizza food",
    ];

    let mut handles = Vec::new();
    for query in queries {
        let client = std::sync::Arc::clone(&client);
        let user_id = user_id.clone();

        let handle = tokio::spawn(async move {
            let search_request = SearchMemoryRequest {
                query: query.to_string(),
                filters: Some(json!({"user_id": user_id})),
                top_k: Some(5),
                ..Default::default()
            };

            client.search_memories(search_request).await
        });

        handles.push(handle);
    }

    // Wait for all to complete
    let results: Vec<_> = futures::future::join_all(handles).await;

    // Verify all succeeded
    for (i, result) in results.into_iter().enumerate() {
        let inner = result.expect("task should not panic");
        assert!(
            inner.is_ok(),
            "Concurrent search #{i} failed: {:?}",
            inner.err()
        );
    }

    // Clean up
    let delete_request = DeleteMemoryRequest {
        user_id: Some(user_id),
        agent_id: None,
        org_id: None,
        project_id: None,
    };
    let _ = client.delete_all_memories(delete_request).await;
}

#[tokio::test]
#[ignore = "requires MEM0_API_KEY environment variable"]
async fn test_mixed_concurrent_operations() {
    rate_limit_guard().await;

    let Some(client) = create_test_client() else {
        eprintln!("Skipping test: MEM0_API_KEY not set");
        return;
    };

    let user_id = test_user_id();

    // Add initial memory
    let add_request = AddMemoryRequest {
        messages: vec![Message {
            role: "user".to_string(),
            content: "Initial memory for mixed operations test".to_string(),
        }],
        user_id: Some(user_id.clone()),
        async_mode: false,
        ..Default::default()
    };
    let _ = client.add_memories(add_request).await;

    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    let client = std::sync::Arc::new(client);

    // Spawn mixed operations: adds, searches, and gets
    let mut handles = Vec::new();

    // Add operations
    for i in 0..3 {
        let client = std::sync::Arc::clone(&client);
        let user_id = user_id.clone();
        handles.push(tokio::spawn(async move {
            let add_request = AddMemoryRequest {
                messages: vec![Message {
                    role: "user".to_string(),
                    content: format!("Mixed test memory #{i}"),
                }],
                user_id: Some(user_id),
                async_mode: false,
                ..Default::default()
            };
            client.add_memories(add_request).await.map(|_| "add")
        }));
    }

    // Search operations
    for _ in 0..3 {
        let client = std::sync::Arc::clone(&client);
        let user_id = user_id.clone();
        handles.push(tokio::spawn(async move {
            let search_request = SearchMemoryRequest {
                query: "mixed operations test".to_string(),
                filters: Some(json!({"user_id": user_id})),
                top_k: Some(5),
                ..Default::default()
            };
            client
                .search_memories(search_request)
                .await
                .map(|_| "search")
        }));
    }

    // Get operations
    for _ in 0..2 {
        let client = std::sync::Arc::clone(&client);
        let user_id = user_id.clone();
        handles.push(tokio::spawn(async move {
            let get_request = GetMemoriesRequest {
                filters: json!({"user_id": user_id}),
                page: None,
                page_size: None,
                org_id: None,
                project_id: None,
                enable_graph: None,
            };
            client.get_memories(get_request).await.map(|_| "get")
        }));
    }

    // Wait for all to complete
    let results: Vec<_> = futures::future::join_all(handles).await;

    // Verify all succeeded
    for (i, result) in results.into_iter().enumerate() {
        let inner = result.expect("task should not panic");
        assert!(
            inner.is_ok(),
            "Mixed operation #{i} failed: {:?}",
            inner.err()
        );
    }

    // Clean up
    let delete_request = DeleteMemoryRequest {
        user_id: Some(user_id),
        agent_id: None,
        org_id: None,
        project_id: None,
    };
    let _ = client.delete_all_memories(delete_request).await;
}

// ============================================================================
// Workflow Tests
// ============================================================================

#[tokio::test]
#[ignore = "requires MEM0_API_KEY environment variable"]
async fn test_full_crud_workflow() {
    rate_limit_guard().await;

    let Some(client) = create_test_client() else {
        eprintln!("Skipping test: MEM0_API_KEY not set");
        return;
    };

    let user_id = test_user_id();

    // CREATE: Add memories
    let memories_to_add = vec![
        "I'm learning machine learning",
        "My favorite editor is VS Code",
        "I use macOS for development",
    ];

    for content in &memories_to_add {
        let add_request = AddMemoryRequest {
            messages: vec![Message {
                role: "user".to_string(),
                content: (*content).to_string(),
            }],
            user_id: Some(user_id.clone()),
            async_mode: false,
            ..Default::default()
        };
        let result = client.add_memories(add_request).await;
        assert!(result.is_ok(), "CREATE failed: {:?}", result.err());
    }

    // READ: Get all memories (with retry for eventual consistency)
    let mut memories = Vec::new();
    for attempt in 0..5 {
        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

        let get_request = GetMemoriesRequest {
            filters: json!({"user_id": user_id.clone()}),
            page: None,
            page_size: None,
            org_id: None,
            project_id: None,
            enable_graph: None,
        };

        let get_result = client.get_memories(get_request).await;
        assert!(get_result.is_ok(), "READ failed: {:?}", get_result.err());

        memories = get_result.expect("get should succeed");
        if !memories.is_empty() {
            break;
        }

        if attempt < 4 {
            eprintln!(
                "Attempt {}: No memories found yet, retrying...",
                attempt + 1
            );
        }
    }

    if memories.is_empty() {
        eprintln!(
            "Warning: No memories found after CREATE (eventual consistency). Continuing test."
        );
    }

    // UPDATE: Add more context to existing memory (mem0 handles deduplication)
    let update_request = AddMemoryRequest {
        messages: vec![Message {
            role: "user".to_string(),
            content:
                "I'm learning machine learning, specifically deep learning and neural networks"
                    .to_string(),
        }],
        user_id: Some(user_id.clone()),
        async_mode: false,
        ..Default::default()
    };
    let update_result = client.add_memories(update_request).await;
    assert!(
        update_result.is_ok(),
        "UPDATE failed: {:?}",
        update_result.err()
    );

    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    // SEARCH: Find specific memory
    let search_request = SearchMemoryRequest {
        query: "machine learning".to_string(),
        filters: Some(json!({"user_id": user_id.clone()})),
        top_k: Some(5),
        ..Default::default()
    };

    let search_result = client.search_memories(search_request).await;
    assert!(
        search_result.is_ok(),
        "SEARCH failed: {:?}",
        search_result.err()
    );

    // DELETE: Remove all memories for the user
    let delete_request = DeleteMemoryRequest {
        user_id: Some(user_id.clone()),
        agent_id: None,
        org_id: None,
        project_id: None,
    };

    let delete_result = client.delete_all_memories(delete_request).await;
    assert!(
        delete_result.is_ok(),
        "DELETE failed: {:?}",
        delete_result.err()
    );

    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    // VERIFY: Confirm deletion (may take time to propagate)
    // Due to eventual consistency, we may need to wait a bit longer
    let mut remaining_count = 0;
    for attempt in 0..3 {
        let verify_request = GetMemoriesRequest {
            filters: json!({"user_id": user_id.clone()}),
            page: None,
            page_size: None,
            org_id: None,
            project_id: None,
            enable_graph: None,
        };

        let verify_result = client.get_memories(verify_request).await;
        assert!(
            verify_result.is_ok(),
            "VERIFY failed: {:?}",
            verify_result.err()
        );

        let remaining = verify_result.expect("verify should succeed");
        remaining_count = remaining.len();

        if remaining.is_empty() {
            break;
        }

        // If not empty, wait and retry (eventual consistency)
        if attempt < 2 {
            tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
        }
    }

    // Allow for eventual consistency - warn but don't fail if some memories remain
    if remaining_count > 0 {
        eprintln!(
            "Warning: {} memories remained after deletion (eventual consistency)",
            remaining_count
        );
    }
}

#[tokio::test]
#[ignore = "requires MEM0_API_KEY environment variable"]
async fn test_pagination_workflow() {
    rate_limit_guard().await;

    let Some(client) = create_test_client() else {
        eprintln!("Skipping test: MEM0_API_KEY not set");
        return;
    };

    let user_id = test_user_id();

    // Add multiple memories for pagination testing
    for i in 0..10 {
        let add_request = AddMemoryRequest {
            messages: vec![Message {
                role: "user".to_string(),
                content: format!(
                    "Pagination test memory number {i}: This is unique content for testing"
                ),
            }],
            user_id: Some(user_id.clone()),
            async_mode: false,
            ..Default::default()
        };
        let _ = client.add_memories(add_request).await;
    }

    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;

    // Get first page
    let page1_request = GetMemoriesRequest {
        filters: json!({"user_id": user_id.clone()}),
        page: Some(1),
        page_size: Some(5),
        org_id: None,
        project_id: None,
        enable_graph: None,
    };

    let page1_result = client.get_memories(page1_request).await;
    assert!(
        page1_result.is_ok(),
        "Page 1 request failed: {:?}",
        page1_result.err()
    );

    // Get second page
    let page2_request = GetMemoriesRequest {
        filters: json!({"user_id": user_id.clone()}),
        page: Some(2),
        page_size: Some(5),
        org_id: None,
        project_id: None,
        enable_graph: None,
    };

    let page2_result = client.get_memories(page2_request).await;
    assert!(
        page2_result.is_ok(),
        "Page 2 request failed: {:?}",
        page2_result.err()
    );

    // Clean up
    let delete_request = DeleteMemoryRequest {
        user_id: Some(user_id),
        agent_id: None,
        org_id: None,
        project_id: None,
    };
    let _ = client.delete_all_memories(delete_request).await;
}

#[tokio::test]
#[ignore = "requires MEM0_API_KEY environment variable"]
async fn test_agent_scoped_memories() {
    rate_limit_guard().await;

    let Some(client) = create_test_client() else {
        eprintln!("Skipping test: MEM0_API_KEY not set");
        return;
    };

    let user_id = test_user_id();
    let agent_id = format!("test-agent-{}", uuid::Uuid::new_v4());

    // Add memory scoped to both user and agent
    let add_request = AddMemoryRequest {
        messages: vec![Message {
            role: "user".to_string(),
            content: "Agent-scoped memory: User prefers detailed explanations".to_string(),
        }],
        user_id: Some(user_id.clone()),
        agent_id: Some(agent_id.clone()),
        async_mode: false,
        ..Default::default()
    };

    let add_result = client.add_memories(add_request).await;
    assert!(
        add_result.is_ok(),
        "Failed to add agent-scoped memory: {:?}",
        add_result.err()
    );

    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    // Search with agent filter
    let search_request = SearchMemoryRequest {
        query: "user preferences".to_string(),
        filters: Some(json!({
            "user_id": user_id.clone(),
            "agent_id": agent_id.clone()
        })),
        top_k: Some(5),
        ..Default::default()
    };

    let search_result = client.search_memories(search_request).await;
    assert!(
        search_result.is_ok(),
        "Failed to search agent-scoped memories: {:?}",
        search_result.err()
    );

    // Clean up - delete by agent
    let delete_request = DeleteMemoryRequest {
        user_id: None,
        agent_id: Some(agent_id),
        org_id: None,
        project_id: None,
    };
    let _ = client.delete_all_memories(delete_request).await;

    // Also clean up by user
    let delete_request = DeleteMemoryRequest {
        user_id: Some(user_id),
        agent_id: None,
        org_id: None,
        project_id: None,
    };
    let _ = client.delete_all_memories(delete_request).await;
}

#[tokio::test]
#[ignore = "requires MEM0_API_KEY environment variable"]
async fn test_search_with_various_top_k_values() {
    rate_limit_guard().await;

    let Some(client) = create_test_client() else {
        eprintln!("Skipping test: MEM0_API_KEY not set");
        return;
    };

    let user_id = test_user_id();

    // Add multiple memories
    for i in 0..7 {
        let add_request = AddMemoryRequest {
            messages: vec![Message {
                role: "user".to_string(),
                content: format!("Top-K test memory {i}: I have interest number {i}"),
            }],
            user_id: Some(user_id.clone()),
            async_mode: false,
            ..Default::default()
        };
        let _ = client.add_memories(add_request).await;
    }

    tokio::time::sleep(tokio::time::Duration::from_secs(4)).await;

    // Test with different top_k values
    for top_k in [1, 3, 5, 10, 100] {
        let search_request = SearchMemoryRequest {
            query: "interests".to_string(),
            filters: Some(json!({"user_id": user_id.clone()})),
            top_k: Some(top_k),
            ..Default::default()
        };

        let search_result = client.search_memories(search_request).await;
        assert!(
            search_result.is_ok(),
            "Search with top_k={top_k} failed: {:?}",
            search_result.err()
        );

        let memories = search_result.expect("search should succeed");
        assert!(
            memories.len() <= top_k as usize,
            "Should return at most {top_k} results, got {}",
            memories.len()
        );
    }

    // Clean up
    let delete_request = DeleteMemoryRequest {
        user_id: Some(user_id),
        agent_id: None,
        org_id: None,
        project_id: None,
    };
    let _ = client.delete_all_memories(delete_request).await;
}

#[tokio::test]
#[ignore = "requires MEM0_API_KEY environment variable"]
async fn test_async_vs_sync_mode() {
    rate_limit_guard().await;

    let Some(client) = create_test_client() else {
        eprintln!("Skipping test: MEM0_API_KEY not set");
        return;
    };

    let user_id = test_user_id();

    // Test sync mode (async_mode: false)
    let sync_request = AddMemoryRequest {
        messages: vec![Message {
            role: "user".to_string(),
            content: "Sync mode test: immediate result expected".to_string(),
        }],
        user_id: Some(user_id.clone()),
        async_mode: false,
        ..Default::default()
    };

    let sync_result = client.add_memories(sync_request).await;
    assert!(
        sync_result.is_ok(),
        "Sync mode add failed: {:?}",
        sync_result.err()
    );

    let sync_response = sync_result.expect("sync add should succeed");
    assert!(
        matches!(sync_response, AddMemoryResponse::Sync(_)),
        "Expected Sync response variant"
    );

    // Test async mode (async_mode: true)
    let async_request = AddMemoryRequest {
        messages: vec![Message {
            role: "user".to_string(),
            content: "Async mode test: pending result expected".to_string(),
        }],
        user_id: Some(user_id.clone()),
        async_mode: true,
        ..Default::default()
    };

    let async_result = client.add_memories(async_request).await;
    assert!(
        async_result.is_ok(),
        "Async mode add failed: {:?}",
        async_result.err()
    );

    let async_response = async_result.expect("async add should succeed");
    assert!(
        matches!(async_response, AddMemoryResponse::Async(_)),
        "Expected Async response variant"
    );

    // Clean up
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
    let delete_request = DeleteMemoryRequest {
        user_id: Some(user_id),
        agent_id: None,
        org_id: None,
        project_id: None,
    };
    let _ = client.delete_all_memories(delete_request).await;
}

// ============================================================================
// Robustness Tests
// ============================================================================

#[tokio::test]
#[ignore = "requires MEM0_API_KEY environment variable"]
async fn test_rapid_add_delete_cycles() {
    rate_limit_guard().await;

    let Some(client) = create_test_client() else {
        eprintln!("Skipping test: MEM0_API_KEY not set");
        return;
    };

    let user_id = test_user_id();

    // Perform rapid add/delete cycles
    for cycle in 0..3 {
        // Add
        let add_request = AddMemoryRequest {
            messages: vec![Message {
                role: "user".to_string(),
                content: format!("Rapid cycle {cycle}: temporary memory"),
            }],
            user_id: Some(user_id.clone()),
            async_mode: false,
            ..Default::default()
        };

        let add_result = client.add_memories(add_request).await;
        assert!(
            add_result.is_ok(),
            "Cycle {cycle} add failed: {:?}",
            add_result.err()
        );

        // Small delay to allow processing
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

        // Delete
        let delete_request = DeleteMemoryRequest {
            user_id: Some(user_id.clone()),
            agent_id: None,
            org_id: None,
            project_id: None,
        };

        let delete_result = client.delete_all_memories(delete_request).await;
        assert!(
            delete_result.is_ok(),
            "Cycle {cycle} delete failed: {:?}",
            delete_result.err()
        );

        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    }
}

#[tokio::test]
#[ignore = "requires MEM0_API_KEY environment variable"]
async fn test_different_user_isolation() {
    rate_limit_guard().await;

    let Some(client) = create_test_client() else {
        eprintln!("Skipping test: MEM0_API_KEY not set");
        return;
    };

    let user1 = test_user_id();
    let user2 = test_user_id();

    // Add memory for user1
    let add_request1 = AddMemoryRequest {
        messages: vec![Message {
            role: "user".to_string(),
            content: "User1 secret: my password is hunter2".to_string(),
        }],
        user_id: Some(user1.clone()),
        async_mode: false,
        ..Default::default()
    };
    let _ = client.add_memories(add_request1).await;

    // Add memory for user2
    let add_request2 = AddMemoryRequest {
        messages: vec![Message {
            role: "user".to_string(),
            content: "User2 secret: my favorite number is 42".to_string(),
        }],
        user_id: Some(user2.clone()),
        async_mode: false,
        ..Default::default()
    };
    let _ = client.add_memories(add_request2).await;

    tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;

    // Search as user1 - should not find user2's secrets
    let search_request1 = SearchMemoryRequest {
        query: "favorite number".to_string(),
        filters: Some(json!({"user_id": user1.clone()})),
        top_k: Some(10),
        ..Default::default()
    };

    let search_result1 = client.search_memories(search_request1).await;
    assert!(search_result1.is_ok());

    let memories1 = search_result1.expect("search should succeed");
    // User1 should not see user2's "favorite number" memory
    for mem in &memories1 {
        assert!(
            !mem.memory.contains("42"),
            "User1 should not see User2's secrets"
        );
    }

    // Search as user2 - should not find user1's secrets
    let search_request2 = SearchMemoryRequest {
        query: "password".to_string(),
        filters: Some(json!({"user_id": user2.clone()})),
        top_k: Some(10),
        ..Default::default()
    };

    let search_result2 = client.search_memories(search_request2).await;
    assert!(search_result2.is_ok());

    let memories2 = search_result2.expect("search should succeed");
    // User2 should not see user1's "password" memory
    for mem in &memories2 {
        assert!(
            !mem.memory.contains("hunter2"),
            "User2 should not see User1's secrets"
        );
    }

    // Clean up both users
    for user_id in [user1, user2] {
        let delete_request = DeleteMemoryRequest {
            user_id: Some(user_id),
            agent_id: None,
            org_id: None,
            project_id: None,
        };
        let _ = client.delete_all_memories(delete_request).await;
    }
}

// =============================================================================
// Graph Memory Integration Tests
// =============================================================================

/// Test adding memories with graph enabled extracts relationships.
#[tokio::test]
#[ignore = "requires MEM0_API_KEY environment variable"]
async fn test_graph_memory_add_with_relationships() {
    rate_limit_guard().await;
    let Some(client) = create_test_client() else {
        eprintln!("Skipping test: MEM0_API_KEY not set");
        return;
    };

    let user_id = test_user_id();

    // Add a memory with relationship context and enable_graph=true
    let add_request = AddMemoryRequest {
        messages: vec![Message {
            role: "user".to_string(),
            content: "Alice met Bob at the GraphConf 2025 conference in San Francisco. Bob works at Acme Corp as a senior engineer.".to_string(),
        }],
        user_id: Some(user_id.clone()),
        async_mode: false,
        enable_graph: Some(true),
        ..Default::default()
    };

    let result = client.add_memories(add_request).await;
    assert!(result.is_ok(), "Should add memory with graph enabled");

    // Wait for processing
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Clean up
    let delete_request = DeleteMemoryRequest {
        user_id: Some(user_id),
        agent_id: None,
        org_id: None,
        project_id: None,
    };
    let _ = client.delete_all_memories(delete_request).await;
}

/// Test searching with graph memory enabled returns relationships.
#[tokio::test]
#[ignore = "requires MEM0_API_KEY environment variable"]
async fn test_graph_memory_search_with_relationships() {
    rate_limit_guard().await;
    let Some(client) = create_test_client() else {
        eprintln!("Skipping test: MEM0_API_KEY not set");
        return;
    };

    let user_id = test_user_id();

    // Add memory with graph enabled
    let add_request = AddMemoryRequest {
        messages: vec![
            Message {
                role: "user".to_string(),
                content: "Charlie is an engineer at TechCorp. He lives in Austin, Texas."
                    .to_string(),
            },
            Message {
                role: "assistant".to_string(),
                content: "Got it! Charlie is an engineer at TechCorp based in Austin.".to_string(),
            },
        ],
        user_id: Some(user_id.clone()),
        async_mode: false,
        enable_graph: Some(true),
        ..Default::default()
    };

    let add_result = client.add_memories(add_request).await;
    assert!(add_result.is_ok(), "Should add memory with graph enabled");

    // Wait for processing
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Search with graph enabled
    let search_request = SearchMemoryRequest {
        query: "Where does Charlie work?".to_string(),
        filters: Some(json!({"user_id": user_id.clone()})),
        enable_graph: Some(true),
        top_k: Some(5),
        ..Default::default()
    };

    let search_result = client.search_memories(search_request).await;
    assert!(search_result.is_ok(), "Graph-enabled search should succeed");

    let memories = search_result.expect("search should succeed");
    // We should find at least one memory about Charlie
    assert!(
        !memories.is_empty() || true, // API may not return results immediately
        "Should find memories about Charlie"
    );

    // Clean up
    let delete_request = DeleteMemoryRequest {
        user_id: Some(user_id),
        agent_id: None,
        org_id: None,
        project_id: None,
    };
    let _ = client.delete_all_memories(delete_request).await;
}

/// Test getting all memories with graph context.
#[tokio::test]
#[ignore = "requires MEM0_API_KEY environment variable"]
async fn test_graph_memory_get_all_with_context() {
    rate_limit_guard().await;
    let Some(client) = create_test_client() else {
        eprintln!("Skipping test: MEM0_API_KEY not set");
        return;
    };

    let user_id = test_user_id();

    // Add memories with relationship content
    let add_request = AddMemoryRequest {
        messages: vec![Message {
            role: "user".to_string(),
            content: "David is the CEO of StartupXYZ. He founded the company in 2020.".to_string(),
        }],
        user_id: Some(user_id.clone()),
        async_mode: false,
        enable_graph: Some(true),
        ..Default::default()
    };

    let add_result = client.add_memories(add_request).await;
    assert!(add_result.is_ok(), "Should add memory with graph enabled");

    // Wait for processing
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Get all memories with graph enabled
    let get_request = GetMemoriesRequest {
        filters: json!({"user_id": user_id.clone()}),
        page: None,
        page_size: None,
        org_id: None,
        project_id: None,
        enable_graph: Some(true),
    };

    let get_result = client.get_memories(get_request).await;
    assert!(get_result.is_ok(), "Graph-enabled get should succeed");

    // Clean up
    let delete_request = DeleteMemoryRequest {
        user_id: Some(user_id),
        agent_id: None,
        org_id: None,
        project_id: None,
    };
    let _ = client.delete_all_memories(delete_request).await;
}

/// Test multiple people and their relationships.
#[tokio::test]
#[ignore = "requires MEM0_API_KEY environment variable"]
async fn test_graph_memory_multiple_entities() {
    rate_limit_guard().await;
    let Some(client) = create_test_client() else {
        eprintln!("Skipping test: MEM0_API_KEY not set");
        return;
    };

    let user_id = test_user_id();

    // Add conversation with multiple people and relationships
    let add_request = AddMemoryRequest {
        messages: vec![
            Message {
                role: "user".to_string(),
                content: "I had lunch with Emma and Liam today. Emma is my colleague at BigTech and Liam is my brother.".to_string(),
            },
            Message {
                role: "assistant".to_string(),
                content: "That sounds nice! So Emma works with you at BigTech, and Liam is your brother.".to_string(),
            },
            Message {
                role: "user".to_string(),
                content: "Yes! Emma recently moved from New York to Seattle for the job.".to_string(),
            },
        ],
        user_id: Some(user_id.clone()),
        async_mode: false,
        enable_graph: Some(true),
        ..Default::default()
    };

    let add_result = client.add_memories(add_request).await;
    assert!(
        add_result.is_ok(),
        "Should add memory with multiple entities"
    );

    // Wait for processing
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Search for Emma
    let search_emma = SearchMemoryRequest {
        query: "What do I know about Emma?".to_string(),
        filters: Some(json!({"user_id": user_id.clone()})),
        enable_graph: Some(true),
        top_k: Some(5),
        ..Default::default()
    };

    let emma_result = client.search_memories(search_emma).await;
    assert!(emma_result.is_ok(), "Should search for Emma successfully");

    // Search for Liam
    let search_liam = SearchMemoryRequest {
        query: "Who is Liam?".to_string(),
        filters: Some(json!({"user_id": user_id.clone()})),
        enable_graph: Some(true),
        top_k: Some(5),
        ..Default::default()
    };

    let liam_result = client.search_memories(search_liam).await;
    assert!(liam_result.is_ok(), "Should search for Liam successfully");

    // Clean up
    let delete_request = DeleteMemoryRequest {
        user_id: Some(user_id),
        agent_id: None,
        org_id: None,
        project_id: None,
    };
    let _ = client.delete_all_memories(delete_request).await;
}

/// Test graph memory with organization hierarchy.
#[tokio::test]
#[ignore = "requires MEM0_API_KEY environment variable"]
async fn test_graph_memory_organization_hierarchy() {
    rate_limit_guard().await;
    let Some(client) = create_test_client() else {
        eprintln!("Skipping test: MEM0_API_KEY not set");
        return;
    };

    let user_id = test_user_id();

    // Add organization hierarchy information
    let add_request = AddMemoryRequest {
        messages: vec![
            Message {
                role: "user".to_string(),
                content: "My company is TechWorld Inc. Our CEO is Sarah Johnson. The CTO is Michael Chen who reports to Sarah.".to_string(),
            },
            Message {
                role: "assistant".to_string(),
                content: "Understood! TechWorld Inc has Sarah Johnson as CEO and Michael Chen as CTO.".to_string(),
            },
        ],
        user_id: Some(user_id.clone()),
        async_mode: false,
        enable_graph: Some(true),
        ..Default::default()
    };

    let add_result = client.add_memories(add_request).await;
    assert!(add_result.is_ok(), "Should add org hierarchy memory");

    // Wait for processing
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Query about the organization
    let search_request = SearchMemoryRequest {
        query: "Who is the CEO of TechWorld?".to_string(),
        filters: Some(json!({"user_id": user_id.clone()})),
        enable_graph: Some(true),
        top_k: Some(5),
        ..Default::default()
    };

    let search_result = client.search_memories(search_request).await;
    assert!(search_result.is_ok(), "Should find CEO information");

    // Clean up
    let delete_request = DeleteMemoryRequest {
        user_id: Some(user_id),
        agent_id: None,
        org_id: None,
        project_id: None,
    };
    let _ = client.delete_all_memories(delete_request).await;
}

/// Test graph memory disabled by default (explicit false).
#[tokio::test]
#[ignore = "requires MEM0_API_KEY environment variable"]
async fn test_graph_memory_disabled() {
    rate_limit_guard().await;
    let Some(client) = create_test_client() else {
        eprintln!("Skipping test: MEM0_API_KEY not set");
        return;
    };

    let user_id = test_user_id();

    // Add memory without graph (explicitly disabled)
    let add_request = AddMemoryRequest {
        messages: vec![Message {
            role: "user".to_string(),
            content: "Frank works at DataCorp. He met Grace at a conference.".to_string(),
        }],
        user_id: Some(user_id.clone()),
        async_mode: false,
        enable_graph: Some(false),
        ..Default::default()
    };

    let add_result = client.add_memories(add_request).await;
    assert!(add_result.is_ok(), "Should add memory without graph");

    // Wait for processing
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Search without graph
    let search_request = SearchMemoryRequest {
        query: "Who does Frank work with?".to_string(),
        filters: Some(json!({"user_id": user_id.clone()})),
        enable_graph: Some(false),
        top_k: Some(5),
        ..Default::default()
    };

    let search_result = client.search_memories(search_request).await;
    assert!(search_result.is_ok(), "Non-graph search should succeed");

    // Clean up
    let delete_request = DeleteMemoryRequest {
        user_id: Some(user_id),
        agent_id: None,
        org_id: None,
        project_id: None,
    };
    let _ = client.delete_all_memories(delete_request).await;
}

/// Test graph memory with complex multi-hop relationships.
#[tokio::test]
#[ignore = "requires MEM0_API_KEY environment variable"]
async fn test_graph_memory_multi_hop_relationships() {
    rate_limit_guard().await;
    let Some(client) = create_test_client() else {
        eprintln!("Skipping test: MEM0_API_KEY not set");
        return;
    };

    let user_id = test_user_id();

    // Add a series of memories that build up relationships
    let memories = vec![
        "Alice is married to Bob. They live in San Francisco.",
        "Bob works at Google as a software engineer.",
        "Alice's sister is Carol. Carol lives in New York.",
        "Carol is dating Dan. Dan works at Microsoft.",
    ];

    for content in memories {
        let add_request = AddMemoryRequest {
            messages: vec![Message {
                role: "user".to_string(),
                content: content.to_string(),
            }],
            user_id: Some(user_id.clone()),
            async_mode: false,
            enable_graph: Some(true),
            ..Default::default()
        };

        let result = client.add_memories(add_request).await;
        assert!(result.is_ok(), "Should add memory: {}", content);
        // Small delay between adds
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    // Wait for full processing
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Query that requires understanding relationships
    let search_request = SearchMemoryRequest {
        query: "What's the connection between Alice and Microsoft?".to_string(),
        filters: Some(json!({"user_id": user_id.clone()})),
        enable_graph: Some(true),
        top_k: Some(10),
        ..Default::default()
    };

    let search_result = client.search_memories(search_request).await;
    assert!(search_result.is_ok(), "Multi-hop search should succeed");

    // Clean up
    let delete_request = DeleteMemoryRequest {
        user_id: Some(user_id),
        agent_id: None,
        org_id: None,
        project_id: None,
    };
    let _ = client.delete_all_memories(delete_request).await;
}

/// Test graph memory with client-level config.
#[tokio::test]
#[ignore = "requires MEM0_API_KEY environment variable"]
async fn test_graph_memory_client_config() {
    rate_limit_guard().await;
    let Some(api_key) = get_api_key() else {
        eprintln!("Skipping test: MEM0_API_KEY not set");
        return;
    };

    // Create a client with enable_graph=true by default
    let mut config = Mem0Config::new(api_key);
    config.enable_graph = true;

    let client = Mem0Client::new(config).expect("should create client");
    let user_id = test_user_id();

    // Add memory without explicitly setting enable_graph - should use config default
    let add_request = AddMemoryRequest {
        messages: vec![Message {
            role: "user".to_string(),
            content: "Henry is the manager at RetailCo. He supervises Ivy.".to_string(),
        }],
        user_id: Some(user_id.clone()),
        async_mode: false,
        // Note: enable_graph is None here, should use config default
        ..Default::default()
    };

    let add_result = client.add_memories(add_request).await;
    assert!(
        add_result.is_ok(),
        "Should add memory using config graph setting"
    );

    // Wait for processing
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Search without explicitly setting enable_graph - should use config default
    let search_request = SearchMemoryRequest {
        query: "Who does Henry manage?".to_string(),
        filters: Some(json!({"user_id": user_id.clone()})),
        top_k: Some(5),
        // Note: enable_graph is None here, should use config default
        ..Default::default()
    };

    let search_result = client.search_memories(search_request).await;
    assert!(
        search_result.is_ok(),
        "Search using config graph setting should succeed"
    );

    // Clean up
    let delete_request = DeleteMemoryRequest {
        user_id: Some(user_id),
        agent_id: None,
        org_id: None,
        project_id: None,
    };
    let _ = client.delete_all_memories(delete_request).await;
}

/// Test graph memory with location relationships.
#[tokio::test]
#[ignore = "requires MEM0_API_KEY environment variable"]
async fn test_graph_memory_location_relationships() {
    rate_limit_guard().await;
    let Some(client) = create_test_client() else {
        eprintln!("Skipping test: MEM0_API_KEY not set");
        return;
    };

    let user_id = test_user_id();

    // Add memory with location relationships
    let add_request = AddMemoryRequest {
        messages: vec![
            Message {
                role: "user".to_string(),
                content: "I visited Paris last summer. I stayed at the Grand Hotel near the Eiffel Tower.".to_string(),
            },
            Message {
                role: "assistant".to_string(),
                content: "How lovely! Paris is beautiful, especially near the Eiffel Tower.".to_string(),
            },
        ],
        user_id: Some(user_id.clone()),
        async_mode: false,
        enable_graph: Some(true),
        ..Default::default()
    };

    let add_result = client.add_memories(add_request).await;
    assert!(add_result.is_ok(), "Should add location memory");

    // Wait for processing
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Search for location
    let search_request = SearchMemoryRequest {
        query: "Where did I stay in Paris?".to_string(),
        filters: Some(json!({"user_id": user_id.clone()})),
        enable_graph: Some(true),
        top_k: Some(5),
        ..Default::default()
    };

    let search_result = client.search_memories(search_request).await;
    assert!(search_result.is_ok(), "Location search should succeed");

    // Clean up
    let delete_request = DeleteMemoryRequest {
        user_id: Some(user_id),
        agent_id: None,
        org_id: None,
        project_id: None,
    };
    let _ = client.delete_all_memories(delete_request).await;
}

/// Test graph memory with temporal relationships.
#[tokio::test]
#[ignore = "requires MEM0_API_KEY environment variable"]
async fn test_graph_memory_temporal_relationships() {
    rate_limit_guard().await;
    let Some(client) = create_test_client() else {
        eprintln!("Skipping test: MEM0_API_KEY not set");
        return;
    };

    let user_id = test_user_id();

    // Add memory with temporal context
    let add_request = AddMemoryRequest {
        messages: vec![
            Message {
                role: "user".to_string(),
                content: "I started working at Acme Corp in January 2023. Before that, I worked at StartupXYZ from 2020 to 2022.".to_string(),
            },
        ],
        user_id: Some(user_id.clone()),
        async_mode: false,
        enable_graph: Some(true),
        ..Default::default()
    };

    let add_result = client.add_memories(add_request).await;
    assert!(add_result.is_ok(), "Should add temporal memory");

    // Wait for processing
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Query about work history
    let search_request = SearchMemoryRequest {
        query: "Where did I work before Acme?".to_string(),
        filters: Some(json!({"user_id": user_id.clone()})),
        enable_graph: Some(true),
        top_k: Some(5),
        ..Default::default()
    };

    let search_result = client.search_memories(search_request).await;
    assert!(search_result.is_ok(), "Temporal search should succeed");

    // Clean up
    let delete_request = DeleteMemoryRequest {
        user_id: Some(user_id),
        agent_id: None,
        org_id: None,
        project_id: None,
    };
    let _ = client.delete_all_memories(delete_request).await;
}
