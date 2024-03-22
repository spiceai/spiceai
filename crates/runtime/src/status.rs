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
