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
