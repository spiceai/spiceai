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
        Store the column as a boolean, integer, float, decimal, string, date, time, timestamp, \
        or fixed-size list, or exclude it from the index payload. See: https://spiceai.org/docs/components/vectors"
    ))]
    UnsupportedPayloadType { arrow_type: String },

    #[snafu(display(
        "Refused to delete from Qdrant collection '{collection}' with an empty filter, which would delete every point. \
        This is a bug in Spice. Report it at https://github.com/spiceai/spiceai/issues"
    ))]
    EmptyDeleteFilter { collection: String },

    #[snafu(display(
        "Failed to read column '{column}' from Qdrant: a stored payload value does not match the column type {arrow_type} ({detail}). \
        Recreate the Qdrant collection, or set a new `qdrant_collection`, so Spice writes every point with the dataset's current schema. \
        See: https://spiceai.org/docs/components/vectors"
    ))]
    PayloadDecode {
        column: String,
        arrow_type: String,
        detail: String,
    },

    #[snafu(display(
        "Failed to read an embedding from Qdrant: the exact vector stored in payload key `spice_vector` is malformed ({detail}). \
        Recreate the Qdrant collection, or set a new `qdrant_collection`, so Spice writes every point again. \
        See: https://spiceai.org/docs/components/vectors"
    ))]
    VectorDecode { detail: String },

    #[snafu(display(
        "Qdrant collection '{collection}' already exists with {actual}, but the index requires {expected}. \
        Use a different `qdrant_collection` or recreate the collection with matching vector parameters."
    ))]
    CollectionMismatch {
        collection: String,
        expected: String,
        actual: String,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
