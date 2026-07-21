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

//! Typed spicepod `params` for vector-store and full-text-store engines.
//!
//! Each engine declares a plain Rust struct deriving
//! [`runtime_parameters::TypedParams`] that deserializes the secret-resolved
//! params map of a `vector_engine` / `full_text_search` component — replacing
//! hand-rolled `ParameterSpec` lists and per-key string parsing with typed
//! fields, real enums for `one_of` values, and typo-suggestion warnings.

#[cfg(feature = "duckdb")]
pub mod duckdb;
#[cfg(feature = "elasticsearch")]
pub mod elasticsearch;
#[cfg(feature = "s3_vectors")]
pub mod s3;
