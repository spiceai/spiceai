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

use snafu::prelude::*;

pub type Result<T, E = Error> = std::result::Result<T, E>;

const DOCS: &str = "https://spiceai.org/docs/components/data-connectors";

/// Whether retrying an identical delivery could plausibly succeed.
///
/// This is the axis that decides what a delivery failure does to the change
/// stream, independent of the configured policy: a `Transient` failure is worth
/// waiting out, a `Permanent` one never is — retrying a malformed payload or an
/// unsupported operation just stalls replication forever without progress.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Retryable {
    /// The target was unreachable, timed out, or reported a server-side fault.
    Transient,
    /// The request itself was rejected, or the change cannot be represented in
    /// Drasi at all. An identical retry produces an identical rejection.
    Permanent,
}

#[derive(Snafu, Debug)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display(
        "Failed to configure Drasi forwarding for dataset {dataset} (drasi): Missing required parameter '{parameter}'. \
        Add it under 'drasi.params'. See: {DOCS}"
    ))]
    MissingParameter {
        dataset: String,
        parameter: &'static str,
    },

    #[snafu(display(
        "Failed to configure Drasi forwarding for dataset {dataset} (drasi): Parameter '{parameter}' is not a valid URL ({value}): {source}. \
        See: {DOCS}"
    ))]
    InvalidUrl {
        dataset: String,
        parameter: &'static str,
        value: String,
        source: url::ParseError,
    },

    #[snafu(display(
        "Failed to configure Drasi forwarding for dataset {dataset} (drasi): {message} \
        See: {DOCS}"
    ))]
    InvalidConfiguration { dataset: String, message: String },

    #[snafu(display(
        "Failed to build the Drasi client for dataset {dataset} (drasi): {source}. \
        See: {DOCS}"
    ))]
    BuildClient {
        dataset: String,
        source: reqwest::Error,
    },

    #[snafu(display(
        "Failed to forward changes for dataset {dataset} (drasi): Primary key column '{column}' is not present in the change data. \
        Set 'drasi.labels' and confirm the source table's replica identity includes its primary key. See: {DOCS}"
    ))]
    PrimaryKeyColumnMissing { dataset: String, column: String },

    #[snafu(display(
        "Failed to forward changes for dataset {dataset} (drasi): Primary key column '{column}' is NULL, so no stable Drasi element id can be derived. \
        Confirm the source table's replica identity publishes its primary key on every change. See: {DOCS}"
    ))]
    PrimaryKeyValueNull { dataset: String, column: String },

    #[snafu(display(
        "Failed to forward changes for dataset {dataset} (drasi): The change stream reported no primary key for a row, so no stable Drasi element id can be derived. \
        Declare a primary key on the source table, or remove the 'drasi' block from this dataset. See: {DOCS}"
    ))]
    PrimaryKeyMissing { dataset: String },

    #[snafu(display(
        "Failed to forward changes for dataset {dataset} (drasi): Change operation '{operation}' has no Drasi equivalent and would leave the Drasi graph inconsistent with the source. \
        Avoid '{operation}' on the source table, or remove the 'drasi' block from this dataset. See: {DOCS}"
    ))]
    UnsupportedOperation { dataset: String, operation: String },

    #[snafu(display(
        "Failed to forward changes for dataset {dataset} (drasi): Could not encode the change as JSON: {message}. \
        See: {DOCS}"
    ))]
    EncodeChange { dataset: String, message: String },

    #[snafu(display(
        "Failed to deliver changes for dataset {dataset} to the Drasi source {source_id} at {endpoint}: {message} \
        Confirm the Drasi source is running and reachable. See: {DOCS}"
    ))]
    Delivery {
        dataset: String,
        source_id: String,
        endpoint: String,
        message: String,
        retryable: Retryable,
    },
}

impl Error {
    /// Whether an identical retry of the operation that produced this error
    /// could plausibly succeed.
    ///
    /// Everything except a [`Error::Delivery`] carrying [`Retryable::Transient`]
    /// is permanent: a configuration or mapping fault is deterministic, so the
    /// stream must surface it rather than retry into an unbounded stall.
    #[must_use]
    pub fn retryable(&self) -> Retryable {
        match self {
            Error::Delivery { retryable, .. } => *retryable,
            _ => Retryable::Permanent,
        }
    }
}
