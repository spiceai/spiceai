/*
Copyright 2024 The Spice.ai OSS Authors

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

//! Spice specific errors that are returned as part of `DataFusionError::External`.

use std::fmt::Display;

#[derive(Debug)]
pub enum SpiceExternalError {
    AccelerationNotReady { dataset_name: String },
}

impl SpiceExternalError {
    #[must_use]
    pub fn acceleration_not_ready(
        dataset_name: String,
    ) -> Box<dyn std::error::Error + Send + Sync> {
        Box::new(Self::AccelerationNotReady { dataset_name })
    }
}

impl std::error::Error for SpiceExternalError {}

impl Display for SpiceExternalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SpiceExternalError::AccelerationNotReady { dataset_name } => write!(
                f,
                "Acceleration not ready; loading initial data for {dataset_name}"
            ),
        }
    }
}
