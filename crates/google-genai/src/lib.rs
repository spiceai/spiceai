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

//! Google Generative AI (Gemini) REST API client
//!
//! This crate provides a Rust client for the Google Generative AI API (Gemini).
//! It supports:
//! - Text generation and multi-turn conversations (chat)
//! - Embeddings
//! - Streaming responses
//! - Function calling (tools)
//! - Structured output (response schema)
//! - Reasoning (thinking mode)

pub mod client;
pub mod embeddings;
pub mod error;
pub mod generate;
pub mod types;

pub use client::Client;
pub use error::{Error, Result};
