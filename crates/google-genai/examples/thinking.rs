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
    Client,
    generate::GenerateContentRequest,
    types::{Content, GenerationConfig, ThinkingConfig},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let api_key =
        std::env::var("GEMINI_API_KEY").expect("GEMINI_API_KEY environment variable not set");

    let client = Client::new(api_key)?;

    println!("Example: Using ThinkingConfig for complex reasoning tasks\n");

    let thinking_config = ThinkingConfig {
        include_thoughts: Some(true),
        thinking_budget: Some(128),
    };

    let generation_config = GenerationConfig {
        thinking_config: Some(thinking_config),
        temperature: Some(0.7),
        ..Default::default()
    };

    let request = GenerateContentRequest::new(vec![Content::user(
        "Explain Occam's Razor principle and provide a practical example.",
    )])
    .with_generation_config(generation_config);

    println!("Sending request with thinking enabled...\n");

    let response = client.generate_content("gemini-2.5-pro", request).await?;

    if let Some(candidate) = response.candidates.first() {
        for part in &candidate.content.parts {
            match part {
                google_genai::types::Part::Text { text } => {
                    println!("Response:\n{text}\n");
                }
                _ => {
                    println!("Other part: {part:?}\n");
                }
            }
        }
    }

    if let Some(usage) = response.usage_metadata {
        println!("Token usage:");
        println!("  Prompt tokens: {}", usage.prompt_token_count);
        if let Some(candidate_tokens) = usage.candidates_token_count {
            println!("  Candidate tokens: {candidate_tokens}");
        }
        println!("  Total tokens: {}", usage.total_token_count);
    }

    Ok(())
}
