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

//! `data_components`' integration tests, linked into one binary.
//!
//! Each `tests/<name>.rs` is a module here rather than its own test target, so
//! the crate's dependencies are linked once instead of once per file.

#[cfg(feature = "elasticsearch")]
mod elasticsearch_test;
mod hadoop_catalog_test;
mod http_provider_test;
mod http_response_limit_test;
