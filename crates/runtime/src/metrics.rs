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

pub(crate) mod spiced_runtime {
    use super::{global, Counter, LazyLock, Meter};

    pub(crate) static RUNTIME_METER: LazyLock<Meter> = LazyLock::new(|| global::meter("runtime"));

    pub(crate) static FLIGHT_SERVER_START: LazyLock<Counter<u64>> = LazyLock::new(|| {
        RUNTIME_METER
            .u64_counter("runtime_flight_server_started")
            .with_description("Indicates the runtime Flight server has started.")
            .build()
    });

    pub(crate) static HTTP_SERVER_START: LazyLock<Counter<u64>> = LazyLock::new(|| {
        RUNTIME_METER
            .u64_counter("runtime_http_server_started")
            .with_description("Indicates the runtime HTTP server has started.")
            .build()
    });
}

pub(crate) mod secrets {
    use super::{global, Histogram, LazyLock, Meter};

    pub(crate) static SECRETS_METER: LazyLock<Meter> =
        LazyLock::new(|| global::meter("secrets_store"));

    pub(crate) static STORES_LOAD_DURATION_MS: LazyLock<Histogram<f64>> = LazyLock::new(|| {
        SECRETS_METER
            .f64_histogram("secrets_store_load_duration_ms")
            .with_description("Duration in milliseconds to load the secret stores.")
            .with_unit("ms")
            .build()
    });
}

pub(crate) mod datasets {
    use super::{global, Counter, Gauge, LazyLock, Meter, UpDownCounter};

    pub(crate) static DATASETS_METER: LazyLock<Meter> = LazyLock::new(|| global::meter("dataset"));

    pub(crate) static UNAVAILABLE_TIME_MS: LazyLock<Gauge<f64>> = LazyLock::new(|| {
        DATASETS_METER
            .f64_gauge("dataset_unavailable_time_ms")
            .with_description("Time dataset went offline in milliseconds.")
            .with_unit("ms")
            .build()
    });

    pub(crate) static LOAD_ERROR: LazyLock<Counter<u64>> = LazyLock::new(|| {
        DATASETS_METER
            .u64_counter("dataset_load_errors")
            .with_description("Number of errors loading the dataset.")
            .build()
    });

    pub(crate) static COUNT: LazyLock<UpDownCounter<i64>> = LazyLock::new(|| {
        DATASETS_METER
            .i64_up_down_counter("dataset_active_count")
            .with_description("Number of currently loaded datasets.")
            .build()
    });

    pub(crate) static STATUS: LazyLock<Gauge<u64>> = LazyLock::new(|| {
        DATASETS_METER
            .u64_gauge("dataset_load_state")
            .with_description("Status of the dataset. 1=Initializing, 2=Ready, 3=Disabled, 4=Error, 5=Refreshing.")
            .build()
    });
}

pub(crate) mod catalogs {
    use super::{global, Counter, Gauge, LazyLock, Meter};

    pub(crate) static CATALOGS_METER: LazyLock<Meter> = LazyLock::new(|| global::meter("catalog"));

    pub(crate) static LOAD_ERROR: LazyLock<Counter<u64>> = LazyLock::new(|| {
        CATALOGS_METER
            .u64_counter("catalog_load_errors")
            .with_description("Number of errors loading the catalog provider.")
            .build()
    });

    pub(crate) static STATUS: LazyLock<Gauge<u64>> = LazyLock::new(|| {
        CATALOGS_METER
            .u64_gauge("catalog_load_state")
            .with_description("Status of the catalog provider. 1=Initializing, 2=Ready, 3=Disabled, 4=Error, 5=Refreshing.")
            .build()
    });
}

pub(crate) mod views {
    use super::{global, Counter, Gauge, LazyLock, Meter};

    pub(crate) static VIEWS_METER: LazyLock<Meter> = LazyLock::new(|| global::meter("view"));

    pub(crate) static LOAD_ERROR: LazyLock<Counter<u64>> = LazyLock::new(|| {
        VIEWS_METER
            .u64_counter("view_load_errors")
            .with_description("Number of errors loading the view.")
            .build()
    });
    pub(crate) static STATUS: LazyLock<Gauge<u64>> = LazyLock::new(|| {
        VIEWS_METER
            .u64_gauge("view_load_state")
            .with_description(
                "Status of the views. 1=Initializing, 2=Ready, 3=Disabled, 4=Error, 5=Refreshing.",
            )
            .build()
    });
}

#[allow(dead_code)]
pub(crate) mod embeddings {
    use super::{global, Counter, Gauge, LazyLock, Meter, UpDownCounter};

    pub(crate) static EMBEDDINGS_METER: LazyLock<Meter> =
        LazyLock::new(|| global::meter("embeddings"));

    pub(crate) static LOAD_ERROR: LazyLock<Counter<u64>> = LazyLock::new(|| {
        EMBEDDINGS_METER
            .u64_counter("embeddings_load_errors")
            .with_description("Number of errors loading the embedding.")
            .build()
    });

    pub(crate) static COUNT: LazyLock<UpDownCounter<i64>> = LazyLock::new(|| {
        EMBEDDINGS_METER
            .i64_up_down_counter("embeddings_active_count")
            .with_description("Number of currently loaded embeddings.")
            .build()
    });

    pub(crate) static STATUS: LazyLock<Gauge<u64>> = LazyLock::new(|| {
        EMBEDDINGS_METER
            .u64_gauge("embeddings_load_state")
            .with_description("Status of the embedding. 1=Initializing, 2=Ready, 3=Disabled, 4=Error, 5=Refreshing.")
            .build()
    });
}

pub(crate) mod models {
    use super::{global, Counter, Gauge, Histogram, LazyLock, Meter, UpDownCounter};

    pub(crate) static MODELS_METER: LazyLock<Meter> = LazyLock::new(|| global::meter("model"));

    pub(crate) static LOAD_ERROR: LazyLock<Counter<u64>> = LazyLock::new(|| {
        MODELS_METER
            .u64_counter("model_load_errors")
            .with_description("Number of errors loading the model.")
            .build()
    });

    pub(crate) static LOAD_DURATION_MS: LazyLock<Histogram<f64>> = LazyLock::new(|| {
        MODELS_METER
            .f64_histogram("model_load_duration_ms")
            .with_description("Duration in milliseconds to load the model.")
            .with_unit("ms")
            .build()
    });

    pub(crate) static COUNT: LazyLock<UpDownCounter<i64>> = LazyLock::new(|| {
        MODELS_METER
            .i64_up_down_counter("model_active_count")
            .with_description("Number of currently loaded models.")
            .build()
    });

    pub(crate) static STATUS: LazyLock<Gauge<u64>> = LazyLock::new(|| {
        MODELS_METER
            .u64_gauge("model_load_state")
            .with_description(
                "Status of the model. 1=Initializing, 2=Ready, 3=Disabled, 4=Error, 5=Refreshing.",
            )
            .build()
    });
}

pub(crate) mod llms {
    use super::{global, Gauge, LazyLock, Meter};

    pub(crate) static LLMS_METER: LazyLock<Meter> = LazyLock::new(|| global::meter("llm"));

    pub(crate) static STATUS: LazyLock<Gauge<u64>> = LazyLock::new(|| {
        LLMS_METER
            .u64_gauge("llm_load_state")
            .with_description(
                "Status of the LLM model. 1=Initializing, 2=Ready, 3=Disabled, 4=Error, 5=Refreshing.",
            )
            .build()
    });
}

pub(crate) mod tools {
    use super::{global, Counter, Gauge, LazyLock, Meter, UpDownCounter};

    pub(crate) static TOOLS_METER: LazyLock<Meter> = LazyLock::new(|| global::meter("tool"));

    pub(crate) static COUNT: LazyLock<UpDownCounter<i64>> = LazyLock::new(|| {
        TOOLS_METER
            .i64_up_down_counter("tool_active_count")
            .with_description("Number of currently loaded LLM tools.")
            .build()
    });

    pub(crate) static STATUS: LazyLock<Gauge<u64>> = LazyLock::new(|| {
        TOOLS_METER
            .u64_gauge("tool_load_state")
            .with_description(
                "Status of the LLM tools. 1=Initializing, 2=Ready, 3=Disabled, 4=Error, 5=Refreshing.",
            )
            .build()
    });

    pub(crate) static LOAD_ERROR: LazyLock<Counter<u64>> = LazyLock::new(|| {
        TOOLS_METER
            .u64_counter("tool_load_errors")
            .with_description("Number of errors loading the LLM tool.")
            .build()
    });
}

pub(crate) mod telemetry {
    use std::time::Duration;

    use opentelemetry::{metrics::Histogram, KeyValue};

    use super::{global, Counter, LazyLock, Meter};

    pub(crate) static TELEMETRY_METER: LazyLock<Meter> =
        LazyLock::new(|| global::meter("telemetry"));

    static QUERY_COUNT: LazyLock<Counter<u64>> = LazyLock::new(|| {
        TELEMETRY_METER
            .u64_counter("query_executions")
            .with_description("Number of query executions.")
            .with_unit("queries")
            .build()
    });

    pub fn track_query_count(dimensions: &[KeyValue]) {
        telemetry::track_query_count(dimensions);
        QUERY_COUNT.add(1, dimensions);
    }

    static BYTES_PROCESSED: LazyLock<Counter<u64>> = LazyLock::new(|| {
        TELEMETRY_METER
            .u64_counter("query_processed_bytes")
            .with_description("Number of bytes processed by the runtime.")
            .with_unit("By")
            .build()
    });

    pub fn track_bytes_processed(bytes: u64, dimensions: &[KeyValue]) {
        telemetry::track_bytes_processed(bytes, dimensions);
        BYTES_PROCESSED.add(bytes, dimensions);
    }

    static BYTES_RETURNED: LazyLock<Counter<u64>> = LazyLock::new(|| {
        TELEMETRY_METER
            .u64_counter("query_returned_bytes")
            .with_description("Number of bytes returned to query clients.")
            .with_unit("By")
            .build()
    });

    pub fn track_bytes_returned(bytes: u64, dimensions: &[KeyValue]) {
        telemetry::track_bytes_returned(bytes, dimensions);
        BYTES_RETURNED.add(bytes, dimensions);
    }

    static QUERY_DURATION_MS: LazyLock<Histogram<f64>> = LazyLock::new(|| {
        TELEMETRY_METER
            .f64_histogram("query_duration_ms")
            .with_description(
                "The total amount of time spent planning and executing queries in milliseconds.",
            )
            .with_unit("ms")
            .build()
    });

    pub fn track_query_duration(duration: Duration, dimensions: &[KeyValue]) {
        telemetry::track_query_duration(duration, dimensions);
        QUERY_DURATION_MS.record(duration.as_secs_f64() * 1000.0, dimensions);
    }

    static QUERY_EXECUTION_DURATION_MS: LazyLock<Histogram<f64>> = LazyLock::new(|| {
        TELEMETRY_METER
        .f64_histogram("query_execution_duration_ms")
        .with_description(
            "The total amount of time spent only executing queries. This is 0 for cached queries.",
        )
        .with_unit("ms")
        .build()
    });

    pub fn track_query_execution_duration(duration: Duration, dimensions: &[KeyValue]) {
        telemetry::track_query_execution_duration(duration, dimensions);
        QUERY_EXECUTION_DURATION_MS.record(duration.as_secs_f64() * 1000.0, dimensions);
    }
}
