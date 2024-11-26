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

use std::sync::{Arc, LazyLock};

mod context;
mod user_agent;

pub use context::*;
pub use user_agent::*;

#[derive(Debug, Copy, Clone)]
pub enum Protocol {
    Http,
    Flight,
    FlightSQL,
    Internal,
}

static HTTP: LazyLock<Arc<str>> = LazyLock::new(|| "http".into());
static FLIGHT: LazyLock<Arc<str>> = LazyLock::new(|| "flight".into());
static FLIGHTSQL: LazyLock<Arc<str>> = LazyLock::new(|| "flightsql".into());
static INTERNAL: LazyLock<Arc<str>> = LazyLock::new(|| "internal".into());

impl Protocol {
    #[must_use]
    pub fn as_arc_str(&self) -> Arc<str> {
        match self {
            Protocol::Http => Arc::clone(&HTTP),
            Protocol::Flight => Arc::clone(&FLIGHT),
            Protocol::FlightSQL => Arc::clone(&FLIGHTSQL),
            Protocol::Internal => Arc::clone(&INTERNAL),
        }
    }
}

impl std::fmt::Display for Protocol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Protocol::Http => write!(f, "http"),
            Protocol::Flight => write!(f, "flight"),
            Protocol::FlightSQL => write!(f, "flightsql"),
            Protocol::Internal => write!(f, "internal"),
        }
    }
}
