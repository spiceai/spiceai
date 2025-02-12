/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

use std::sync::atomic::AtomicU8;

#[derive(Debug, Copy, Clone)]
#[repr(u8)]
pub enum Protocol {
    Invalid = 0,
    Http = 1,
    Flight = 2,
    FlightSQL = 3,
    Internal = 4,
}

static HTTP: &str = "http";
static FLIGHT: &str = "flight";
static FLIGHTSQL: &str = "flightsql";
static INTERNAL: &str = "internal";

impl Protocol {
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            Protocol::Http => HTTP,
            Protocol::Flight => FLIGHT,
            Protocol::FlightSQL => FLIGHTSQL,
            Protocol::Internal => INTERNAL,
            Protocol::Invalid => unreachable!(),
        }
    }
}

impl From<AtomicU8> for Protocol {
    fn from(value: AtomicU8) -> Self {
        Protocol::from(value.load(std::sync::atomic::Ordering::Relaxed))
    }
}

impl From<u8> for Protocol {
    fn from(value: u8) -> Self {
        match value {
            1 => Protocol::Http,
            2 => Protocol::Flight,
            3 => Protocol::FlightSQL,
            4 => Protocol::Internal,
            _ => Protocol::Invalid,
        }
    }
}

impl std::fmt::Display for Protocol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Protocol::Http => write!(f, "{HTTP}"),
            Protocol::Flight => write!(f, "{FLIGHT}"),
            Protocol::FlightSQL => write!(f, "{FLIGHTSQL}"),
            Protocol::Internal => write!(f, "{INTERNAL}"),
            Protocol::Invalid => unreachable!(),
        }
    }
}
