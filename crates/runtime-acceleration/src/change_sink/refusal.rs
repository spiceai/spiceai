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

//! Proven pre-mutation refusals. Every unmarked failure requires reconciliation.

use datafusion::error::DataFusionError;
use snafu::Snafu;

#[derive(Debug, Snafu)]
#[snafu(display("{source}"))]
pub struct BeforeMutation {
    source: DataFusionError,
}

/// Mark a refusal only when this submission has not changed storage or indexes.
/// Planning a provider write is not proof: some wrappers mutate while planning.
#[must_use]
pub fn before_mutation(source: DataFusionError) -> DataFusionError {
    DataFusionError::External(Box::new(BeforeMutation { source }))
}

/// Inspect typed error sources, without relying on error messages or recovery policy.
#[must_use]
pub fn is_before_mutation(error: &DataFusionError) -> bool {
    let mut current: &(dyn std::error::Error + 'static) = error;
    loop {
        if current.is::<BeforeMutation>() {
            return true;
        }
        let Some(source) = current.source() else {
            return false;
        };
        current = source;
    }
}
