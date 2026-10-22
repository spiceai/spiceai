/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! The Hugging Face Hub client every Spice component reads the Hub through: models,
//! embeddings, rerankers and their files, and the Hugging Face data connector.
//!
//! - [`client`] speaks the Hub's API: it resolves revisions to commits, lists and describes
//!   files, and reads them, retrying transient failures and waiting out rate limits, and it
//!   sends the token only to the Hub's own origin.
//! - [`location`] holds repository ids and the `hf://` locations that name files in them.

pub mod client;
pub mod location;

pub use client::{Commit, EntryKind, Error, FileRead, Hub, HubConfig, Result, TreeEntry};
pub use location::{RepoId, RepoType};
