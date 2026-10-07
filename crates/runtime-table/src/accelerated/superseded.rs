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

//! `dataset_acceleration_rows_superseded`: the rows a refresh or a user's
//! statement received but the accelerated table did not keep (#14576).

use runtime_metrics::acceleration as metrics;
use util::session_state::{SupersededReason, SupersededRows};

use super::refresh_task::DatasetMetricLabels;

/// Record the rows `superseded` counts against the dataset `labels` name, by reason.
pub(crate) fn record(labels: &DatasetMetricLabels, superseded: &SupersededRows) {
    for reason in SupersededReason::ALL {
        let rows = superseded.get(reason);
        if rows > 0 {
            metrics::ROWS_SUPERSEDED.add(rows, &labels.tagged("reason", reason.label()));
        }
    }
}
