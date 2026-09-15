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

//! Apache ORC [`datafusion::datasource::file_format::FileFormat`] built on
//! [`orc_rust`] for Spice listing connectors (S3, GCS, ABFS, file, …).
//!
//! This crate is the in-repo equivalent of `dataformat-json`. It does **not**
//! depend on `datafusion-orc`.

mod file_format;
mod object_store_reader;
mod source;

pub use file_format::OrcFormat;
pub use source::OrcSource;

#[cfg(test)]
mod test_support;
