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

use std::sync::Arc;

use crate::component::metrics::{MetricSpec, MetricType, MetricsProvider, ObserveMetricCallback};
use opentelemetry::{KeyValue, metrics::UpDownCounter};
use snafu::prelude::*;

use super::{LazyLock, Meter, global};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Internal error. Report an issue at https://github.com/spiceai/spiceai/issues. Metric callback not implemented for metric {} with type {:?}", metric.name, metric.metric_type))]
    MetricCallbackNotImplemented { metric: MetricSpec },

    #[snafu(display("Internal error. Report an issue at https://github.com/spiceai/spiceai/issues. Metric {} callback has wrong type. Expected {}, got {}", metric.name, expected_type, actual_type))]
    MetricCallbackWrongType {
        metric: MetricSpec,
        expected_type: &'static str,
        actual_type: &'static str,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub(crate) static COMPONENTS_METER: LazyLock<Meter> = LazyLock::new(|| global::meter("component"));

pub(crate) static REGISTERED_COUNT: LazyLock<UpDownCounter<i64>> = LazyLock::new(|| {
    COMPONENTS_METER
        .i64_up_down_counter("component_metric_registered_count")
        .with_description("Number of currently registered component metrics.")
        .build()
});

pub(crate) fn register_component_metric(
    metric_provider: &Arc<dyn MetricsProvider>,
    metric: MetricSpec,
    instance_name: &str,
) -> Result<()> {
    let metric_name = format!(
        "{}_{}_{}",
        metric_provider.component_type(),
        metric_provider.component_name(),
        metric.name
    );

    let attributes = vec![KeyValue::new("name", instance_name.to_string())];

    match metric.metric_type {
        MetricType::ObservableCounterU64 => {
            let mut counter = COMPONENTS_METER.u64_observable_counter(metric_name);
            if let Some(description) = metric.description {
                counter = counter.with_description(description);
            }
            if let Some(unit) = metric.unit {
                counter = counter.with_unit(unit);
            }
            let metric_callback = metric_provider
                .callback_to_observe_metric(&metric, attributes)
                .context(MetricCallbackNotImplementedSnafu { metric })?;
            let callback_type = metric_callback_type(&metric_callback);
            let ObserveMetricCallback::U64(callback) = metric_callback else {
                return Err(Error::MetricCallbackWrongType {
                    metric,
                    expected_type: "u64",
                    actual_type: callback_type,
                });
            };
            let _ = counter.with_callback(callback).build();
            REGISTERED_COUNT.add(1, &[]);
            Ok(())
        }
        MetricType::ObservableGaugeU64 => {
            let mut gauge = COMPONENTS_METER.u64_observable_gauge(metric_name);
            if let Some(description) = metric.description {
                gauge = gauge.with_description(description);
            }
            if let Some(unit) = metric.unit {
                gauge = gauge.with_unit(unit);
            }
            let metric_callback = metric_provider
                .callback_to_observe_metric(&metric, attributes)
                .context(MetricCallbackNotImplementedSnafu { metric })?;
            let callback_type = metric_callback_type(&metric_callback);
            let ObserveMetricCallback::U64(callback) = metric_callback else {
                return Err(Error::MetricCallbackWrongType {
                    metric,
                    expected_type: "u64",
                    actual_type: callback_type,
                });
            };
            let _ = gauge.with_callback(callback).build();
            REGISTERED_COUNT.add(1, &[]);
            Ok(())
        }
    }
}

fn metric_callback_type(metric_callback: &ObserveMetricCallback) -> &'static str {
    match metric_callback {
        ObserveMetricCallback::U64(_) => "u64",
        ObserveMetricCallback::I64(_) => "i64",
        ObserveMetricCallback::F64(_) => "f64",
    }
}
