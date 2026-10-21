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

//! Spice model provider — models served by the Spice.ai Cloud Platform, or by another Spice
//! runtime (a Spice-to-Spice connection).
//!
//! Both serve an `OpenAI`-compatible API, so models are driven through the shared
//! [`crate::openai::Openai`] client rather than a bespoke protocol.

#![allow(clippy::missing_errors_doc)]

mod chat;
mod list_models;

pub use chat::{DEFAULT_ENDPOINT, api_base, is_cloud_platform, new_spiceai_client};
pub use list_models::SpiceAiModelLister;
