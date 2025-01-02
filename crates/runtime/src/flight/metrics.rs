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
    metrics::{Counter, Histogram, Meter},
    KeyValue,
};

use crate::{
    request::{AsyncMarker, RequestContext},
    timing::TimeMeasurement,
};

static METER: LazyLock<Meter> = LazyLock::new(|| global::meter("flight"));

pub(crate) static FLIGHT_REQUESTS: LazyLock<Counter<u64>> =
    LazyLock::new(|| METER.u64_counter("flight_requests").build());

pub(crate) static FLIGHT_REQUEST_DURATION_MS: LazyLock<Histogram<f64>> = LazyLock::new(|| {
    METER
        .f64_histogram("flight_request_duration_ms")
        .with_unit("ms")
        .build()
});

pub async fn track_flight_request(method: &str, command: Option<&str>) -> TimeMeasurement {
    let request_context = RequestContext::current(AsyncMarker::new().await);

    let mut dimensions = vec![KeyValue::new("method", method.to_string())];

    if let Some(method) = command {
        dimensions.push(KeyValue::new("command", method.to_string()));
    }

    dimensions.extend(request_context.to_dimensions().into_iter());

    FLIGHT_REQUESTS.add(1, dimensions.as_slice());
    TimeMeasurement::new(&FLIGHT_REQUEST_DURATION_MS, dimensions.as_slice())
}

pub(crate) static DO_EXCHANGE_DATA_UPDATES_SENT: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("flight_do_exchange_data_updates_sent")
        .build()
});
