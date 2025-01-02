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

use std::sync::{Arc, LazyLock, OnceLock};

use opentelemetry::metrics::{Meter, MeterProvider};

use crate::noop::NoopMeterProvider;

pub(crate) static METER_PROVIDER_ONCE: OnceLock<Arc<dyn MeterProvider + Send + Sync>> =
    OnceLock::new();

/// If the meter provider isn't initialized for anonymous telemetry, use a `NoopMeterProvider`.
///
/// This allows the instrumented code to not require any changes when anonymous telemetry is disabled/compiled out.
static METER_PROVIDER: LazyLock<&'static Arc<dyn MeterProvider + Send + Sync>> =
    LazyLock::new(|| METER_PROVIDER_ONCE.get_or_init(|| Arc::new(NoopMeterProvider::new())));

pub(crate) static METER: LazyLock<Meter> = LazyLock::new(|| METER_PROVIDER.meter("oss_telemetry"));
