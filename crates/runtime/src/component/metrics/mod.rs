/*
Copyright 2025 The Spice.ai OSS Authors

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

use std::{fmt::Debug, sync::Arc};

use super::ComponentType;
use opentelemetry::{KeyValue, metrics::Callback};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MetricType {
    ObservableCounterU64,
    ObservableGaugeU64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct MetricSpec {
    pub name: &'static str,
    pub metric_type: MetricType,
    pub description: Option<&'static str>,
    pub unit: Option<&'static str>,
}

impl MetricSpec {
    #[must_use]
    pub const fn new(name: &'static str, metric_type: MetricType) -> Self {
        Self {
            name,
            metric_type,
            description: None,
            unit: None,
        }
    }

    #[must_use]
    pub const fn description(mut self, description: &'static str) -> Self {
        self.description = Some(description);
        self
    }

    #[must_use]
    pub const fn unit(mut self, unit: &'static str) -> Self {
        self.unit = Some(unit);
        self
    }
}

pub trait MetricsProviderComponent: Debug + Send + Sync + 'static {
    /// Returns a `MetricsProvider` for the component.
    ///
    /// If the component does not support metrics, return `None`.
    fn metrics_provider(&self) -> Option<Arc<dyn MetricsProvider>>;
}

pub trait MetricsProvider: Debug + Send + Sync + 'static {
    fn component_type(&self) -> ComponentType;
    fn component_name(&self) -> &'static str;
    fn available_metrics(&self) -> &'static [MetricSpec];
    fn callback_to_observe_metric(
        &self,
        metric: &MetricSpec,
        attributes: Vec<KeyValue>,
    ) -> Option<ObserveMetricCallback>;
}

impl dyn MetricsProvider {
    pub fn get_metric(&self, metric_name: &str) -> Option<&MetricSpec> {
        self.available_metrics()
            .iter()
            .find(|metric| metric.name == metric_name)
    }
}

pub enum ObserveMetricCallback {
    U64(Callback<u64>),
    I64(Callback<i64>),
    F64(Callback<f64>),
}
