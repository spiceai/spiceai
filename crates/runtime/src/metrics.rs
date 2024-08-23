/*
Copyright 2024 The Spice.ai OSS Authors

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

    pub(crate) static RUNTIME_METER: LazyLock<Meter> =
        LazyLock::new(|| global::meter("spiced_runtime"));

    pub(crate) static FLIGHT_SERVER_START: LazyLock<Counter<u64>> = LazyLock::new(|| {
        RUNTIME_METER
            .u64_counter("spiced_runtime_flight_server_start")
            .with_description("Indicates the runtime Flight server has started.")
            .init()
    });

    pub(crate) static HTTP_SERVER_START: LazyLock<Counter<u64>> = LazyLock::new(|| {
        RUNTIME_METER
            .u64_counter("spiced_runtime_http_server_start")
            .with_description("Indicates the runtime HTTP server has started.")
            .init()
    });
}

pub(crate) mod secrets {
    use super::{global, Histogram, LazyLock, Meter};

    pub(crate) static SECRETS_METER: LazyLock<Meter> = LazyLock::new(|| global::meter("secrets"));

    pub(crate) static STORES_LOAD_DURATION_MS: LazyLock<Histogram<f64>> = LazyLock::new(|| {
        SECRETS_METER
            .f64_histogram("secrets_stores_load_duration_ms")
            .with_description("Duration in milliseconds to load the secret stores.")
            .with_unit("ms")
            .init()
    });
}

pub(crate) mod datasets {
    use super::{global, Counter, Gauge, LazyLock, Meter, UpDownCounter};

    pub(crate) static DATASETS_METER: LazyLock<Meter> = LazyLock::new(|| global::meter("datasets"));

    pub(crate) static UNAVAILABLE_TIME: LazyLock<Gauge<f64>> = LazyLock::new(|| {
        DATASETS_METER
            .f64_gauge("datasets_unavailable_time")
            .with_description("Time since the dataset went offline in seconds.")
            .with_unit("s")
            .init()
    });

    pub(crate) static LOAD_ERROR: LazyLock<Counter<u64>> = LazyLock::new(|| {
        DATASETS_METER
            .u64_counter("datasets_load_error")
            .with_description("Number of errors loading the dataset.")
            .init()
    });

    pub(crate) static COUNT: LazyLock<UpDownCounter<i64>> = LazyLock::new(|| {
        DATASETS_METER
            .i64_up_down_counter("datasets_count")
            .with_description("Number of currently loaded datasets.")
            .init()
    });

    pub(crate) static STATUS: LazyLock<Gauge<u64>> = LazyLock::new(|| {
        DATASETS_METER
            .u64_gauge("datasets_status")
            .with_description("Status of the dataset. 1=Initializing, 2=Ready, 3=Disabled, 4=Error, 5=Refreshing.")
            .init()
    });
}

pub(crate) mod catalogs {
    use super::{global, Counter, Gauge, LazyLock, Meter};

    pub(crate) static CATALOGS_METER: LazyLock<Meter> = LazyLock::new(|| global::meter("catalogs"));

    pub(crate) static LOAD_ERROR: LazyLock<Counter<u64>> = LazyLock::new(|| {
        CATALOGS_METER
            .u64_counter("catalogs_load_error")
            .with_description("Number of errors loading the catalog provider.")
            .init()
    });

    pub(crate) static STATUS: LazyLock<Gauge<u64>> = LazyLock::new(|| {
        CATALOGS_METER
            .u64_gauge("catalogs_status")
            .with_description("Status of the catalog provider. 1=Initializing, 2=Ready, 3=Disabled, 4=Error, 5=Refreshing.")
            .init()
    });
}

pub(crate) mod views {
    use super::{global, Counter, LazyLock, Meter};

    pub(crate) static VIEWS_METER: LazyLock<Meter> = LazyLock::new(|| global::meter("views"));

    pub(crate) static LOAD_ERROR: LazyLock<Counter<u64>> = LazyLock::new(|| {
        VIEWS_METER
            .u64_counter("views_load_error")
            .with_description("Number of errors loading the view.")
            .init()
    });
}

#[allow(dead_code)]
pub(crate) mod embeddings {
    use super::{global, Counter, Gauge, LazyLock, Meter, UpDownCounter};

    pub(crate) static EMBEDDINGS_METER: LazyLock<Meter> =
        LazyLock::new(|| global::meter("embeddings"));

    pub(crate) static LOAD_ERROR: LazyLock<Counter<u64>> = LazyLock::new(|| {
        EMBEDDINGS_METER
            .u64_counter("embeddings_load_error")
            .with_description("Number of errors loading the embedding.")
            .init()
    });

    pub(crate) static COUNT: LazyLock<UpDownCounter<i64>> = LazyLock::new(|| {
        EMBEDDINGS_METER
            .i64_up_down_counter("embeddings_count")
            .with_description("Number of currently loaded embeddings.")
            .init()
    });

    pub(crate) static STATUS: LazyLock<Gauge<u64>> = LazyLock::new(|| {
        EMBEDDINGS_METER
            .u64_gauge("embeddings_status")
            .with_description("Status of the embedding. 1=Initializing, 2=Ready, 3=Disabled, 4=Error, 5=Refreshing.")
            .init()
    });
}

pub(crate) mod models {
    use super::{global, Counter, Gauge, Histogram, LazyLock, Meter, UpDownCounter};

    pub(crate) static MODELS_METER: LazyLock<Meter> = LazyLock::new(|| global::meter("models"));

    pub(crate) static LOAD_ERROR: LazyLock<Counter<u64>> = LazyLock::new(|| {
        MODELS_METER
            .u64_counter("models_load_error")
            .with_description("Number of errors loading the model.")
            .init()
    });

    pub(crate) static LOAD_DURATION_MS: LazyLock<Histogram<f64>> = LazyLock::new(|| {
        MODELS_METER
            .f64_histogram("models_load_duration_ms")
            .with_description("Duration in milliseconds to load the model.")
            .with_unit("ms")
            .init()
    });

    pub(crate) static COUNT: LazyLock<UpDownCounter<i64>> = LazyLock::new(|| {
        MODELS_METER
            .i64_up_down_counter("models_count")
            .with_description("Number of currently loaded models.")
            .init()
    });

    pub(crate) static STATUS: LazyLock<Gauge<u64>> = LazyLock::new(|| {
        MODELS_METER
            .u64_gauge("models_status")
            .with_description(
                "Status of the model. 1=Initializing, 2=Ready, 3=Disabled, 4=Error, 5=Refreshing.",
            )
            .init()
    });
}

pub(crate) mod llms {
    use super::{global, Gauge, LazyLock, Meter};

    pub(crate) static LLMS_METER: LazyLock<Meter> = LazyLock::new(|| global::meter("llms"));

    pub(crate) static STATUS: LazyLock<Gauge<u64>> = LazyLock::new(|| {
        LLMS_METER
            .u64_gauge("llms_status")
            .with_description(
                "Status of the LLM model. 1=Initializing, 2=Ready, 3=Disabled, 4=Error, 5=Refreshing.",
            )
            .init()
    });
}

pub(crate) mod tools {
    use super::{global, Counter, Gauge, LazyLock, Meter, UpDownCounter};

    pub(crate) static TOOLS_METER: LazyLock<Meter> = LazyLock::new(|| global::meter("tools"));

    pub(crate) static COUNT: LazyLock<UpDownCounter<i64>> = LazyLock::new(|| {
        TOOLS_METER
            .i64_up_down_counter("tool_count")
            .with_description("Number of currently loaded LLM tools.")
            .init()
    });

    pub(crate) static STATUS: LazyLock<Gauge<u64>> = LazyLock::new(|| {
        TOOLS_METER
            .u64_gauge("tools_status")
            .with_description(
                "Status of the LLM tools. 1=Initializing, 2=Ready, 3=Disabled, 4=Error, 5=Refreshing.",
            )
            .init()
    });

    pub(crate) static LOAD_ERROR: LazyLock<Counter<u64>> = LazyLock::new(|| {
        TOOLS_METER
            .u64_counter("tool_load_error")
            .with_description("Number of errors loading the LLM tool.")
            .init()
    });
}
