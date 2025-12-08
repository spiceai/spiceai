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
    embeddings::{EmbedContentRequest, TaskType},
    types::Content,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let api_key =
        std::env::var("GEMINI_API_KEY").expect("GEMINI_API_KEY environment variable not set");

    let client = Client::new(api_key)?;

    let texts = [
        "What is the meaning of life?",
        "How does gravity work?",
        "Explain quantum mechanics",
    ];

    println!("Generating embeddings for {} texts...\n", texts.len());

    for (i, text) in texts.iter().enumerate() {
        let request = EmbedContentRequest::new(Content::user(*text))
            .with_output_dimensionality(768)
            .with_task_type(TaskType::RetrievalQuery);

        let response = client.embed_content("text-embedding-004", request).await?;

        println!("Text {}: \"{}\"", i + 1, text);
        println!("  Embedding dimension: {}", response.embedding.values.len());
        println!(
            "  First 5 values: {:?}\n",
            &response.embedding.values[..5.min(response.embedding.values.len())]
        );
    }

    Ok(())
}
