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

use snafu::prelude::*;

pub mod hadoop;
pub mod rest;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "An unknown error occurred while interacting with the Iceberg catalog.\nReport an issue at https://github.com/spiceai/spiceai/issues\n{source}"
    ))]
    Unknown { source: iceberg::Error },

    #[snafu(display(
        "The data in the Iceberg table is invalid. The table may be corrupted or incomplete.\n{source}"
    ))]
    DataInvalid { source: iceberg::Error },

    #[snafu(display(
        "This Iceberg feature is not yet supported.\nReport an issue at https://github.com/spiceai/spiceai/issues\n{source}"
    ))]
    FeatureUnsupported { source: iceberg::Error },

    #[snafu(display(
        "The namespace '{namespace}' does not exist in the Iceberg catalog, verify the namespace name and try again."
    ))]
    NamespaceDoesNotExist { namespace: String },

    #[snafu(display(
        "Failed to connect to the Iceberg catalog or object store at {url}, verify the Iceberg catalog is accessible and try again."
    ))]
    FailedToConnect { url: String, source: iceberg::Error },

    #[snafu(display(
        "Internal error: could not acquire a semaphore permit for concurrency control: {source}"
    ))]
    SemaphoreError { source: tokio::sync::AcquireError },
}
