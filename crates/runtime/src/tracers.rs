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

use datafusion::common::runtime::JoinSetTracer;
use futures::FutureExt;
use futures::future::BoxFuture;
use std::{
    any::Any,
    collections::{HashMap, HashSet},
    sync::Mutex,
    time::{Duration, Instant},
};
use tracing::{Instrument, Span};

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
        let msg = format!($msg, $value);
        if !logged_values.contains(&msg) {
            logged_values.insert(msg.clone());
            tracing::warn!("{}", msg);
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

/// A tracer that ensures spawned tasks and blocking closures inherit the current span context.
///
/// When `DataFusion` spawns tasks internally (e.g., for parallel query execution), without this
/// tracer the span context is lost when crossing thread boundaries. This tracer ensures that:
///
/// 1. Async futures spawned via `DataFusion`'s `JoinSet` run with the current span as their parent
/// 2. Blocking closures run within the current span's scope
///
/// This is essential for proper trace hierarchies like:
/// ```text
/// sql_query
///   └── ai (UDF call)
///         └── model_call (which emits ai_completion spans)
/// ```
///
/// Without this tracer, the `model_call` and `ai_completion` spans would not properly parent under `sql_query`.
///
/// ## Scope
///
/// This tracer **only affects `DataFusion`'s internal task spawning** via `JoinSet`. For other
/// async operations (e.g., direct `tokio::spawn` calls), use `.instrument(span)` or capture
/// the span context manually before entering async boundaries.
///
/// ## Usage
///
/// This tracer is automatically initialized during runtime startup via `init_datafusion_tracer()`.
/// No additional configuration is needed for `DataFusion` operations to properly propagate spans.
///
/// For non-DataFusion async code emitting to `target: "task_history"`:
/// - Use `.instrument(span)` on futures before spawning them
/// - Or capture `Span::current()` before async boundaries and use `parent: &span` in child spans
pub struct TaskHistorySpanTracer;

impl JoinSetTracer for TaskHistorySpanTracer {
    /// Instruments a boxed future to run in the current span.
    ///
    /// The future's return type is erased to `Box<dyn Any + Send>`, which we
    /// run inside the current span context using `in_current_span()`.
    fn trace_future(
        &self,
        fut: BoxFuture<'static, Box<dyn Any + Send>>,
    ) -> BoxFuture<'static, Box<dyn Any + Send>> {
        fut.in_current_span().boxed()
    }

    /// Instruments a boxed blocking closure by running it inside the current span's scope.
    ///
    /// Captures the current span and returns a new closure that runs the original
    /// closure within that span's scope.
    fn trace_block(
        &self,
        f: Box<dyn FnOnce() -> Box<dyn Any + Send> + Send>,
    ) -> Box<dyn FnOnce() -> Box<dyn Any + Send> + Send> {
        let span = Span::current();
        Box::new(move || span.in_scope(f))
    }
}

/// Initializes the global `JoinSetTracer` for `DataFusion`.
///
/// This should be called once during runtime initialization, before any `DataFusion`
/// queries are executed. It sets up the `TaskHistorySpanTracer` globally so that
/// all `DataFusion` operations will properly propagate span context.
///
/// # Errors
///
/// Returns an error if a tracer has already been set. This is a programming error
/// and indicates the function was called multiple times.
pub fn init_datafusion_tracer() -> Result<(), Box<dyn std::error::Error>> {
    datafusion::common::runtime::set_join_set_tracer(&TaskHistorySpanTracer)
        .map_err(|e| format!("Failed to set DataFusion JoinSet tracer: {e}").into())
}
