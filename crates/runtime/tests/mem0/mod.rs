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

use runtime::tools::mem0::client::{
    AddMemoryRequest, DeleteMemoryRequest, GetMemoriesRequest, Mem0Client, Mem0Config, Message,
    SearchMemoryRequest,
};
use secrecy::SecretString;
use serde_json::json;
use std::collections::HashMap;

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

#[tokio::test]
#[ignore = "requires MEM0_API_KEY environment variable"]
async fn test_add_and_search_memory() {
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
        ..Default::default()
    };

    let add_result = client.add_memories(add_request).await;
    assert!(
        add_result.is_ok(),
        "Failed to add memory: {:?}",
        add_result.err()
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
        ..Default::default()
    };

    let add_result = client.add_memories(add_request).await;
    assert!(add_result.is_ok(), "Failed to add memory");

    let add_response = add_result.expect("add should succeed");

    // Wait for indexing
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    // Get the memory ID from the first event
    if let Some(first) = add_response.first() {
        // Delete the specific memory
        let delete_result = client.delete_memory(&first.id).await;
        assert!(
            delete_result.is_ok(),
            "Failed to delete memory: {:?}",
            delete_result.err()
        );
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
