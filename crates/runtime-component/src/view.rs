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

use datafusion::sql::TableReference;
use spicepod::vector::VectorStore;
use std::{collections::HashMap, sync::Arc, time::Duration};

use crate::dataset::{ReadyState, TimeFormat, acceleration};
use spicepod::semantic::Column;

/// Config-only core of a view — every declared field of a
/// `runtime::component::view::View` except the runtime handles (`app`/`runtime`).
/// The runtime wrapper holds `Self` plus those handles and `Deref`s to it.
#[derive(Clone)]
pub struct ViewSpec {
    pub name: TableReference,
    pub sql: Arc<str>,
    pub metadata: HashMap<String, String>,
    pub columns: Vec<Column>,
    /// Column carrying the row's time value, used by the acceleration /
    /// warm-tier data-window logic (e.g. retention), mirroring datasets.
    pub time_column: Option<String>,
    /// Encoding of `time_column`'s values.
    pub time_format: Option<TimeFormat>,
    pub acceleration: Option<acceleration::Acceleration>,
    pub ready_state: ReadyState,
    pub vectors: Option<VectorStore>,
    pub params: HashMap<String, String>,
}

impl PartialEq for ViewSpec {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
            && self.sql == other.sql
            && self.metadata == other.metadata
            && self.columns == other.columns
            && self.time_column == other.time_column
            && self.time_format == other.time_format
            && self.acceleration == other.acceleration
            && self.vectors == other.vectors
            && self.params == other.params
            && self.ready_state == other.ready_state
    }
}

impl std::fmt::Debug for ViewSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ViewSpec")
            .field("name", &self.name)
            .field("sql", &self.sql)
            .field("metadata", &self.metadata)
            .field("columns", &self.columns)
            .field("time_column", &self.time_column)
            .field("time_format", &self.time_format)
            .field("acceleration", &self.acceleration)
            .field("ready_state", &self.ready_state)
            .field("vectors", &self.vectors)
            .field("params", &self.params)
            .finish_non_exhaustive()
    }
}

impl ViewSpec {
    #[must_use]
    pub fn is_accelerated(&self) -> bool {
        if let Some(acceleration) = &self.acceleration {
            return acceleration.enabled;
        }

        false
    }

    #[must_use]
    pub fn refresh_check_interval(&self) -> Option<Duration> {
        if let Some(acceleration) = &self.acceleration {
            return acceleration.refresh_check_interval;
        }
        None
    }

    #[must_use]
    pub fn refresh_max_jitter(&self) -> Option<Duration> {
        if let Some(acceleration) = &self.acceleration
            && acceleration.refresh_jitter_enabled
        {
            // If `refresh_jitter_max` is not set, use 10% of `refresh_check_interval`.
            return match acceleration.refresh_jitter_max {
                Some(jitter) => Some(jitter),
                None => self.refresh_check_interval().map(|i| i.mul_f64(0.1)),
            };
        }
        None
    }

    #[must_use]
    pub fn refresh_retry_enabled(&self) -> bool {
        if let Some(acceleration) = &self.acceleration {
            return acceleration.refresh_retry_enabled;
        }
        false
    }

    #[must_use]
    pub fn refresh_retry_max_attempts(&self) -> Option<usize> {
        if let Some(acceleration) = &self.acceleration {
            return acceleration.refresh_retry_max_attempts;
        }
        None
    }

    #[must_use]
    pub fn has_embeddings(&self) -> bool {
        self.columns.iter().any(|c| !c.embeddings.is_empty())
    }

    #[must_use]
    pub fn has_full_text_column(&self) -> bool {
        self.columns
            .iter()
            .any(|c| c.full_text_search.as_ref().is_some_and(|cfg| cfg.enabled))
    }
}
