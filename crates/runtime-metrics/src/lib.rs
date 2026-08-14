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

// Re-exported so submodules can write `use super::{Counter, Gauge, ...}`.
pub use opentelemetry::{
    global,
    metrics::{Counter, Gauge, Histogram, Meter, UpDownCounter},
};
pub use std::sync::LazyLock;

pub mod acceleration;
pub mod catalogs;
pub mod cluster;
pub mod component;
pub mod components;
pub mod datasets;
pub mod embeddings;
pub mod http;
pub mod llms;
pub mod models;
pub mod query;
pub mod rerankers;
pub mod secrets;
pub mod spiced_runtime;
pub mod telemetry;
pub mod tools;
pub mod views;
pub mod workers;

/// Publishes every component counter at zero.
///
/// A counter that never fires exports no series at all, and an absent series reads
/// as a broken exporter rather than as zero (#12687). Each module owns its list,
/// beside the declarations, so a new counter cannot be forgotten here.
///
/// Qualifying is narrow. The emission must be unlabelled: an unlabelled zero in a
/// labelled family like `dataset_active_count{engine}` is a phantom series nothing
/// updates, and adds an empty group to `sum by (..)`. Those need one zero per label
/// value, as `cache::metrics::EvictionReason::ALL` does. Gauges qualify only where
/// zero is the true initial reading — `results_cache_hit_ratio` does,
/// `dataset_load_state` does not, since 0 there means `Initializing`.
///
/// Call after the meter provider is installed (#12667).
pub fn publish_component_counters_at_zero() {
    catalogs::publish_counters_at_zero();
    components::publish_counters_at_zero();
    datasets::publish_counters_at_zero();
    embeddings::publish_counters_at_zero();
    models::publish_counters_at_zero();
    rerankers::publish_counters_at_zero();
    tools::publish_counters_at_zero();
    views::publish_counters_at_zero();
}
