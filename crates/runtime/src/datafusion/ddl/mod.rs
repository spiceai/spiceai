/*
Copyright 2026, Spice AI, Inc.

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

//! Generic DDL support: shared types, preprocessing, and option parsing for
//! `CREATE TABLE` statements across catalog integrations (Iceberg, Cayenne, etc.).
//!
//! This module provides:
//! - [`CreateTableStatementExtension`]: DDL extensions (acceleration, dataset options,
//!   partitioning) extracted from `CREATE TABLE` statements.
//! - [`DdlExtensionStore`]: Thread-safe store keyed by table name, consumed by
//!   catalog-specific analyzer rules.
//! - [`preprocess`]: SQL pre-processing to extract `WITH (...)` options and
//!   `PARTITION BY` clauses before DataFusion planning.

pub mod acceleration_options;
pub mod preprocess;
