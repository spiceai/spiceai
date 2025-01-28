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

use std::{sync::LazyLock, time::Duration};

use async_openai::types::{CreateEmbeddingRequest, EncodingFormat};
use opentelemetry::{
    global,
    metrics::{Counter, Histogram, Meter},
    Key, KeyValue, Value,
};

static METER: LazyLock<Meter> = LazyLock::new(|| global::meter("embeddings"));

pub(crate) static EMBEDDING_REQUESTS: LazyLock<Counter<u64>> =
    LazyLock::new(|| METER.u64_counter("embeddings_requests").build());

pub(crate) static FAILURES: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("embeddings_failures")
        .with_description("Number of embedding failures.")
        .build()
});

pub(crate) static EMBEDDING_INTERNAL_DURATION_MS: LazyLock<Histogram<f64>> = LazyLock::new(|| {
    METER
        .f64_histogram("embeddings_internal_request_duration_ms")
        .with_unit("ms")
        .with_description("The duration of running an embedding(s) internally.")
        .build()
});

pub(crate) fn simple_labels(model: &str, encoding_format: &EncodingFormat) -> Vec<KeyValue> {
    let encoding = match encoding_format {
        EncodingFormat::Float => "float",
        EncodingFormat::Base64 => "base64",
    };
    vec![
        KeyValue::new(Key::new("model"), Value::String(model.to_string().into())),
        KeyValue::new(Key::new("encoding_format"), Value::String(encoding.into())),
    ]
}

pub(crate) fn request_labels(req: &CreateEmbeddingRequest) -> Vec<KeyValue> {
    let encoding = match req.encoding_format {
        None | Some(EncodingFormat::Float) => "float",
        Some(EncodingFormat::Base64) => "base64",
    };
    let mut labels = vec![
        KeyValue::new(
            Key::new("model"),
            Value::String(req.model.to_string().into()),
        ),
        KeyValue::new(Key::new("encoding_format"), Value::String(encoding.into())),
    ];
    if let Some(ref user) = req.user {
        labels.push(KeyValue::new(
            Key::new("user"),
            Value::String(user.to_string().into()),
        ));
    };

    if let Some(ref dim) = req.dimensions {
        labels.push(KeyValue::new(
            Key::new("dimensions"),
            Value::I64(i64::from(*dim)),
        ));
    }

    labels
}

pub(crate) fn handle_metrics(duration: Duration, is_failure: bool, labels: &[KeyValue]) {
    EMBEDDING_REQUESTS.add(1, labels);

    EMBEDDING_INTERNAL_DURATION_MS.record(duration.as_secs_f64(), labels);

    if is_failure {
        FAILURES.add(1, labels);
    }
}
