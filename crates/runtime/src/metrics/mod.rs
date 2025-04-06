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

use std::sync::LazyLock;

use opentelemetry::{
    global,
    metrics::{Counter, Gauge, Histogram, Meter, UpDownCounter},
};

pub(crate) mod catalogs;
pub(crate) mod components;
pub(crate) mod datasets;
#[allow(dead_code)]
pub(crate) mod embeddings;
pub(crate) mod llms;
pub(crate) mod models;
pub(crate) mod secrets;
pub(crate) mod spiced_runtime;
pub(crate) mod telemetry;
pub(crate) mod tools;
pub(crate) mod views;
