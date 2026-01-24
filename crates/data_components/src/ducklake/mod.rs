/*
Copyright 2026 The Spice.ai OSS Authors

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

//! `DuckLake` catalog provider implementation.
//!
//! `DuckLake` is an open Lakehouse format that stores metadata in SQL tables and data in Parquet files.
//! This module provides a catalog provider that connects to a `DuckLake` catalog using `DuckDB`
//! with the `ducklake` extension.

pub mod provider;
