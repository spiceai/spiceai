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

#![allow(clippy::expect_used)]

use google_genai::{
    generate::GenerateContentRequest,
    types::{CachedContent, Content},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _api_key =
        std::env::var("GEMINI_API_KEY").expect("GEMINI_API_KEY environment variable not set");

    println!("Example: Using cached content for optimized repeated queries\n");
    println!("Note: This example demonstrates the API structure.");
    println!("To use cached content, you must first create a cache using the");
    println!("cachedContents.create endpoint (not shown in this example).\n");

    let cached_content = CachedContent {
        name: Some("cachedContents/your-cache-id".to_string()),
    };

    let request = GenerateContentRequest::new(vec![Content::user(
        "Based on the cached context, what are the key points?",
    )])
    .with_cached_content(cached_content);

    println!("Request structure:");
    println!("{}", serde_json::to_string_pretty(&request)?);
    println!("\nNote: This request would fail without a valid cache ID.");
    println!("Cached content helps reduce latency and costs for repeated queries");
    println!("with the same large context (like long documents or conversation history).");

    Ok(())
}
