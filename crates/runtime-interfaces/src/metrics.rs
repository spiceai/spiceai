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

/// Minimal metrics traits re-exported for connectors/accelerators to report metrics
/// without depending on the runtime's full metrics module.
pub trait MetricsProvider: Send + Sync {
    fn specs(&self) -> Vec<MetricSpec>;
    fn update(&self, callback: ObserveMetricCallback);
}

pub type ObserveMetricCallback = Box<dyn Fn(MetricType) + Send + Sync>;

#[derive(Clone, Copy, Debug)]
pub enum MetricType {
    Counter,
    Gauge,
    Histogram,
}

#[derive(Clone, Debug)]
pub struct MetricSpec {
    pub name: &'static str,
    pub description: &'static str,
    pub metric_type: MetricType,
}
