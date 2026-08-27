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

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum Error {
    #[snafu(display("Failed to construct the Qdrant client for endpoint '{endpoint}': {source}"))]
    ClientBuild {
        endpoint: String,
        source: qdrant_client::QdrantError,
    },

    #[snafu(display("Failed to query Qdrant: {source}"))]
    Qdrant { source: qdrant_client::QdrantError },

    #[snafu(display(
        "Unsupported Arrow type for a Qdrant payload value: {arrow_type}. \
        Store the column with a supported type (boolean, integer, float, string, or fixed-size list of floats), \
        or exclude it from the index payload."
    ))]
    UnsupportedPayloadType { arrow_type: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
