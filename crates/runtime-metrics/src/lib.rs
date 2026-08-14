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
/// A `LazyLock` counter that has never fired exports no series at all, and an
/// absent series reads as a broken exporter rather than as zero (#12687).
///
/// Only counters whose real emission is unlabelled qualify. An unlabelled zero in
/// a family that is otherwise labelled — `dataset_active_count{engine}`,
/// `dataset_acceleration_refresh_errors{dataset}` — is a phantom series that no
/// record will ever update, and it puts an empty group into any `sum by (..)`.
/// Publishing those needs one zero per known label value, at the point the label
/// is known, the way `cache::metrics::EvictionReason::ALL` does it.
///
/// Gauges qualify only where zero is the genuine initial reading:
/// `results_cache_hit_ratio` does, `dataset_load_state` does not, because 0 there
/// means `Initializing`.
///
/// Must be called after the operator's meter provider is installed (#12667).
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
