/*
Copyright 2025 The Spice.ai OSS Authors

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

#![deny(missing_docs)]

//! Cayenne: A minimal `DuckLake`-inspired lakehouse format using `SQLite` for metadata
//! and Vortex files as the data lake.
//!
//! This module provides a lakehouse format that combines:
//! - `SQLite` for transactional metadata management (schemas, tables, files)
//! - Vortex files for efficient columnar data storage
//!
//! # Architecture
//!
//! Cayenne follows the `DuckLake` specification with these key components:
//! - **Metadata Catalog**: `SQLite` database storing table metadata and file references
//! - **Data Lake**: Directory of Vortex files containing the actual data
//!
//! # Virtual Files Concept
//!
//! An initial design principle in Cayenne is that "files" are **virtual files** - they are not
//! single physical files, but rather Vortex `ListingTables` at unique directories. Each
//! `DataFile` entry in the catalog represents:
//!
//! - A unique directory path (e.g., `table_dir/file_000001/`)
//! - A Vortex `ListingTable` that manages Vortex files within that directory
//! - Metadata (row count, size) cached from the `ListingTable`'s statistics
//!
//! **Operations delegate to `ListingTables`:**
//! - **Reading a file**: Query the `ListingTable` at that directory
//! - **Appending to a file**: Write via the `ListingTable` (creates new Vortex files)
//! - **Deleting a file**: Delete the `ListingTable`'s directory
//! - **Getting stats**: Query the `ListingTable`'s statistics
//!
//! This design allows Cayenne to leverage Vortex's columnar format and `DataFusion`'s
//! `ListingTable` capabilities while maintaining transactional metadata in `SQLite`.
//!
//! # Core Concepts
//!
//! - **Tables**: Metadata about table schemas and structure
//! - **Data Files**: Metadata for virtual files (Vortex `ListingTables` at unique directories)

pub mod catalog;
pub mod cayenne_catalog;
pub mod deletion;
pub mod metadata;
pub mod metastore;
pub mod optimizer_rules;
pub mod provider;

pub use catalog::MetadataCatalog;
pub use cayenne_catalog::CayenneCatalog;
pub use metadata::{DataFile, DeleteFile, PartitionMetadata, TableMetadata};
pub use provider::CayenneTableProvider;
