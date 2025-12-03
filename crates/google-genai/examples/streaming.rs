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

use futures::StreamExt;
use google_genai::{Client, generate::GenerateContentRequest, types::Content};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let api_key =
        std::env::var("GEMINI_API_KEY").expect("GEMINI_API_KEY environment variable not set");

    let client = Client::new(api_key)?;

    let request = GenerateContentRequest::new(vec![Content::user(
        "Write a haiku about programming in Rust",
    )]);

    println!("Streaming response from Gemini API...\n");

    let mut stream = client
        .stream_generate_content("gemini-2.0-flash", request)
        .await?;

    while let Some(result) = stream.next().await {
        match result {
            Ok(response) => {
                if let Some(candidate) = response.candidates.first() {
                    for part in &candidate.content.parts {
                        if let google_genai::types::Part::Text { text } = part {
                            print!("{text}");
                        }
                    }
                }
            }
            Err(e) => {
                eprintln!("Error: {e}");
                break;
            }
        }
    }

    println!("\n\nStreaming complete!");

    Ok(())
}
