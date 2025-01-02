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

use std::{
    collections::{HashMap, HashSet},
    sync::Mutex,
    time::{Duration, Instant},
};

/// Traces a log with a given parameter once, to prevent log spam.
///
/// Not suitable for high-frequency logs.
pub struct OnceTracer {
    pub logged_values: Mutex<HashSet<String>>,
}

impl OnceTracer {
    pub fn new() -> Self {
        OnceTracer {
            logged_values: Mutex::new(HashSet::new()),
        }
    }
}

#[macro_export]
macro_rules! warn_once {
    ($tracer:expr, $msg:expr, $value:expr) => {{
        let mut logged_values = $tracer.logged_values.lock().unwrap_or_else(|poisoned| {
            tracing::error!("Lock poisoned while logging: {poisoned}");
            poisoned.into_inner()
        });
        if !logged_values.contains(&$value) {
            logged_values.insert($value.clone());
            tracing::warn!($msg, $value);
        }
    }};
}

/// Traces a log with a given parameter at most once every N seconds, to prevent log spam.
///
/// Suitable for controlling log frequency.
pub struct SpacedTracer {
    pub logged_times: Mutex<HashMap<String, Instant>>,
    pub interval: Duration,
}

impl SpacedTracer {
    pub fn new(interval: Duration) -> Self {
        SpacedTracer {
            logged_times: Mutex::new(HashMap::new()),
            interval,
        }
    }
}

#[macro_export]
macro_rules! info_spaced {
    ($tracer:expr, $msg:expr, $key:expr) => {{
        let mut logged_times = $tracer.logged_times.lock().unwrap_or_else(|poisoned| {
            tracing::error!("Lock poisoned while logging: {poisoned}");
            poisoned.into_inner()
        });

        let now = std::time::Instant::now();
        let mut should_log = true;
        if let Some(last_time) = logged_times.get($key) {
            if now.duration_since(*last_time) < $tracer.interval {
                // If the interval hasn't elapsed, do not log.
                should_log = false;
            }
        }

        if should_log {
            // Update the last logged time and log the message.
            logged_times.insert($key.to_string(), now);
            tracing::info!($msg, $key);
        }
    }};
}

#[macro_export]
macro_rules! warn_spaced {
    ($tracer:expr, $msg:expr, $key:expr) => {{
        let mut logged_times = $tracer.logged_times.lock().unwrap_or_else(|poisoned| {
            tracing::error!("Lock poisoned while logging: {poisoned}");
            poisoned.into_inner()
        });

        let now = std::time::Instant::now();
        let mut should_log = true;
        if let Some(last_time) = logged_times.get($key) {
            if now.duration_since(*last_time) < $tracer.interval {
                // If the interval hasn't elapsed, do not log.
                should_log = false;
            }
        }

        if should_log {
            // Update the last logged time and log the message.
            logged_times.insert($key.to_string(), now);
            tracing::warn!($msg, $key);
        }
    }};
}

#[macro_export]
macro_rules! error_spaced {
    ($tracer:expr, $msg:expr, $key:expr) => {{
        let mut logged_times = $tracer.logged_times.lock().unwrap_or_else(|poisoned| {
            tracing::error!("Lock poisoned while logging: {poisoned}");
            poisoned.into_inner()
        });

        let now = std::time::Instant::now();
        let mut should_log = true;
        if let Some(last_time) = logged_times.get($key) {
            if now.duration_since(*last_time) < $tracer.interval {
                should_log = false;
            }
        }

        if should_log {
            logged_times.insert($key.to_string(), now);
            tracing::error!($msg, $key);
        }
    }};
}
