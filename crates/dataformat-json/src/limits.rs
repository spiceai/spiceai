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

//! The error for a JSON file that a probe could not hold in memory.
//!
//! A probe that must parse a file whole reads it through
//! [`http_body_limit::read_to_end`] under a reservation on the query memory pool, so a
//! file the pool cannot hold is refused instead of exhausting the process. A compressed
//! file is charged by its decompressed size, as that is what the probe holds.

use std::error::Error as StdError;

use datafusion::error::DataFusionError;
use object_store::path::Path;

/// The error for a JSON file the runtime refused to hold, naming the file and the fix.
pub(crate) fn refused(location: &Path, refusal: &http_body_limit::Error) -> DataFusionError {
    DataFusionError::ResourcesExhausted(format!(
        "Failed to read JSON file '{location}' to infer its schema: {refusal}. Raise \
         `runtime.query.memory_limit`, or split the file into smaller files. See: \
         https://spiceai.org/docs/reference/memory"
    ))
}

/// `error`, or the error for a refused file when a read under a reservation refused it.
pub(crate) fn locate_refusal(location: &Path, error: DataFusionError) -> DataFusionError {
    match find_refusal(&error) {
        Some(refusal) => refused(location, refusal),
        None => error,
    }
}

/// The refusal anywhere in `error`'s source chain.
fn find_refusal<'a>(error: &'a (dyn StdError + 'static)) -> Option<&'a http_body_limit::Error> {
    let mut current: Option<&(dyn StdError + 'static)> = Some(error);
    while let Some(error) = current {
        if let Some(refusal) = error.downcast_ref::<http_body_limit::Error>() {
            return Some(refusal);
        }
        current = error.source();
    }
    None
}
