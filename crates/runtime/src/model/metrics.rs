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

use async_openai::types::{
    ChatCompletionNamedToolChoice, ChatCompletionToolChoiceOption, CreateChatCompletionRequest,
};
use opentelemetry::{
    global,
    metrics::{Counter, Histogram, Meter},
    Key, KeyValue, StringValue, Value,
};

static METER: LazyLock<Meter> = LazyLock::new(|| global::meter("llms"));

pub(crate) static LLM_REQUESTS: LazyLock<Counter<u64>> =
    LazyLock::new(|| METER.u64_counter("llm_requests").build());

pub(crate) static FAILURES: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("llm_failures")
        .with_description("Number of embedding failures.")
        .build()
});

pub(crate) static LLM_INTERNAL_DURATION_MS: LazyLock<Histogram<f64>> = LazyLock::new(|| {
    METER
        .f64_histogram("llm_internal_request_duration_ms")
        .with_unit("ms")
        .with_description("The duration of running an embedding(s) internally.")
        .build()
});

pub(crate) fn request_labels(req: &CreateChatCompletionRequest) -> Vec<KeyValue> {
    #[allow(clippy::cast_possible_wrap)]
    let mut labels = vec![
        KeyValue::new(
            Key::new("stream"),
            Value::Bool(req.stream.unwrap_or_default()),
        ),
        KeyValue::new(
            Key::new("request_level_tools"),
            Value::I64(req.tools.as_deref().unwrap_or_default().len() as i64),
        ),
        KeyValue::new(Key::new("model"), Value::String(req.model.clone().into())),
    ];

    if let Some(ref choice) = req.tool_choice {
        let choice_str: StringValue = match choice {
            ChatCompletionToolChoiceOption::None => "none".into(),
            ChatCompletionToolChoiceOption::Auto => "auto".into(),
            ChatCompletionToolChoiceOption::Required => "required".into(),
            ChatCompletionToolChoiceOption::Named(ChatCompletionNamedToolChoice {
                function,
                ..
            }) => format!("function:{}", function.name).into(),
        };
        labels.push(KeyValue::new(
            Key::new("tool_choice"),
            Value::String(choice_str),
        ));
    };

    if let Some(ref user) = req.user {
        labels.push(KeyValue::new(
            Key::new("user"),
            Value::String(user.clone().into()),
        ));
    };

    if let Some(ref metadata) = req.metadata {
        labels.push(KeyValue::new(
            Key::new("metadata"),
            Value::String(metadata.to_string().into()),
        ));
    };

    labels
}

pub(crate) fn handle_metrics(duration: Duration, is_failure: bool, labels: &[KeyValue]) {
    LLM_REQUESTS.add(1, labels);
    LLM_INTERNAL_DURATION_MS.record(duration.as_secs_f64(), labels);
    if is_failure {
        FAILURES.add(1, labels);
    }
}
