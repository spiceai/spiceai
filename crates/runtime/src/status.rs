/*
 Copyright 2024 Spice AI, Inc.

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

use std::fmt::Display;

use metrics::gauge;
use serde::{Deserialize, Serialize};

#[allow(clippy::module_name_repetitions)]
#[derive(Debug, PartialEq, Eq, Copy, Clone, Serialize, Deserialize)]
pub enum ComponentStatus {
    Initializing = 1,
    Ready = 2,
    Disabled = 3,
    Error = 4,
    Refreshing = 5,
}

impl Display for ComponentStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ComponentStatus::Initializing => write!(f, "Initializing"),
            ComponentStatus::Ready => write!(f, "Ready"),
            ComponentStatus::Disabled => write!(f, "Disabled"),
            ComponentStatus::Error => write!(f, "Error"),
            ComponentStatus::Refreshing => write!(f, "Refreshing"),
        }
    }
}

pub fn update_dataset(ds_name: String, status: ComponentStatus) {
    gauge!("dataset/status", "dataset" => ds_name).set(f64::from(status as u32));
}

pub fn update_model(model_name: String, status: ComponentStatus) {
    gauge!("model/status", "model" => model_name).set(f64::from(status as u32));
}
