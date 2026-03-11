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

//! Type-safe object storage with optimistic concurrency control (OCC).
//!
//! This crate provides a typed wrapper around [`object_store::ObjectStore`] that uses
//! conditional writes (ETags/If-Match headers) to detect concurrent modifications.

mod state;

use snafu::Snafu;

pub use state::{InsertResult, ObjectState, UpdateResult, WriteResult};

/// Errors that can occur during object state operations.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum Error {
    #[snafu(display("Failed to serialize object for key {key}: {source}"))]
    Serialization {
        key: String,
        source: serde_json::Error,
    },

    #[snafu(display("Failed to deserialize object for key {key}: {source}"))]
    Deserialization {
        key: String,
        source: serde_json::Error,
    },

    #[snafu(display("Object store operation '{operation}' failed for key {key}: {source}"))]
    ObjectStore {
        key: String,
        operation: &'static str,
        source: object_store::Error,
    },
    #[snafu(display("Unexpected deletion of object with key {key}"))]
    UnexpectedDeletionError { key: String },
}

impl Error {
    /// Convert this error into an `object_store::Error`, by either retrieving the internal
    /// `object_store::Error`, or providing a generic error.
    #[must_use]
    pub fn into_object_store(self, store: &'static str) -> object_store::Error {
        match self {
            Error::Deserialization { source, .. } | Error::Serialization { source, .. } => {
                object_store::Error::Generic {
                    store,
                    source: Box::new(source),
                }
            }
            Error::ObjectStore { source, .. } => source,
            Error::UnexpectedDeletionError { .. } => object_store::Error::Generic {
                store,
                source: Box::new(self),
            },
        }
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
