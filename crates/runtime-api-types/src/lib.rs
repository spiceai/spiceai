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

//! Shared API types for Spice runtime HTTP endpoints and CLI.
//!
//! This crate defines the canonical API response types used by:
//! - The runtime's HTTP API handlers (for serialization)
//! - The CLI (for deserialization)
//!
//! Within a major API version (e.g., `/v1`), these types should not have breaking changes.

pub mod v1;
