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

use std::{collections::BTreeMap, fmt::Display, sync::Arc, time::Duration};

use anyhow::Result;
use arrow::{
    array::{
        ArrayRef, Float64Array, Float64Builder, RecordBatch, StringArray, StringBuilder,
        UInt64Array, UInt64Builder,
    },
    datatypes::{DataType, Field, Schema, SchemaRef},
    util::pretty::print_batches,
};
use uuid::Uuid;

use crate::TestType;

#[derive(Copy, Clone)]
pub enum QueryStatus {
    Passed,
    Failed,
}

impl Display for QueryStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            QueryStatus::Passed => write!(f, "Passed"),
            QueryStatus::Failed => write!(f, "Failed"),
        }
    }
}

pub struct QueryMetric<T: ExtendedMetrics> {
    pub query_name: String,
    pub query_status: QueryStatus,
    pub average_duration: f64,
    pub median_duration: f64,
    pub percentile_99_duration: f64,
    pub percentile_90_duration: f64,
    pub run_count: usize,
    pub extended_metrics: Option<T>,
}

impl<T: ExtendedMetrics> QueryMetric<T> {
    pub fn new_from_durations(name: &str, durations: &Vec<Duration>) -> Result<Self> {
        if durations.is_empty() {
            return Ok(Self::new(name).failed());
        }

        let durations = durations.statistical_set()?;
        Ok(Self {
            query_name: name.to_string(),
            query_status: QueryStatus::Passed,
            average_duration: durations.average()?.as_secs_f64(),
            median_duration: durations.median()?.as_secs_f64(),
            percentile_99_duration: durations.percentile(0.99)?.as_secs_f64(),
            percentile_90_duration: durations.percentile(0.90)?.as_secs_f64(),
            run_count: durations.len(),
            extended_metrics: None,
        })
    }

    #[must_use]
    pub fn failed(mut self) -> Self {
        self.query_status = QueryStatus::Failed;
        self
    }

    #[must_use]
    pub fn new(name: &str) -> Self {
        Self {
            query_name: name.to_string(),
            query_status: QueryStatus::Passed,
            average_duration: 0.0,
            median_duration: 0.0,
            percentile_99_duration: 0.0,
            percentile_90_duration: 0.0,
            run_count: 0,
            extended_metrics: None,
        }
    }
}

pub trait StatisticsCollector<T, C> {
    fn percentile(&self, percentile: f64) -> Result<T>;
    fn median(&self) -> Result<T>;
    fn average(&self) -> Result<T>;
    fn statistical_set(&self) -> Result<C>;
}

impl StatisticsCollector<Duration, Vec<Duration>> for Vec<Duration> {
    fn percentile(&self, percentile: f64) -> Result<Duration> {
        // safety: sorted_durations.len() cannot be negative, and is unlikely to be larger than u32::MAX
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        let index = ((percentile * f64::from(u32::try_from(self.len())?)).ceil() as usize)
            .saturating_sub(1)
            .min(self.len() - 1);
        Ok(self[index])
    }

    fn median(&self) -> Result<Duration> {
        let half = self.len() / 2;
        if self.len() % 2 == 0 {
            Ok((self[half - 1] + self[half]) / 2)
        } else {
            Ok(self[half])
        }
    }

    fn average(&self) -> Result<Duration> {
        let total: Duration = self.iter().sum();
        Ok(total / u32::try_from(self.len())?)
    }

    fn statistical_set(&self) -> Result<Vec<Duration>> {
        if self.is_empty() {
            return Ok(vec![]);
        }

        let mut sorted_durations = self.clone();
        sorted_durations.sort();

        // calculate the inter-quartile range to remove statistical outliers
        // safety: sorted_durations.len() cannot be negative, and is unlikely to be larger than u32::MAX
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        {
            let first_quartile_index =
                (f64::from(u32::try_from(sorted_durations.len())?) * 0.25).floor();
            let third_quartile_index =
                (f64::from(u32::try_from(sorted_durations.len())?) * 0.75).ceil();
            let first_quartile_secs = sorted_durations[first_quartile_index as usize].as_secs_f64();
            let third_quartile_secs = sorted_durations[third_quartile_index as usize].as_secs_f64();

            let iqr = third_quartile_secs - first_quartile_secs;
            let lower_bound = first_quartile_secs - 1.5 * iqr;
            let upper_bound = third_quartile_secs + 1.5 * iqr;

            sorted_durations.retain(|duration| {
                let duration_secs = duration.as_secs_f64();
                duration_secs >= lower_bound && duration_secs <= upper_bound
            });
        }

        Ok(if sorted_durations.is_empty() {
            self.clone() // if everything is an outlier, nothing is an outlier - keep everything
        } else {
            sorted_durations
        })
    }
}

impl StatisticsCollector<BTreeMap<String, Duration>, BTreeMap<String, Vec<Duration>>>
    for BTreeMap<String, Vec<Duration>>
{
    fn percentile(&self, percentile: f64) -> Result<BTreeMap<String, Duration>> {
        let mut percentiles = BTreeMap::new();
        for (query, durations) in self {
            if durations.is_empty() {
                continue;
            }
            percentiles.insert(query.clone(), durations.percentile(percentile)?);
        }
        Ok(percentiles)
    }

    fn median(&self) -> Result<BTreeMap<String, Duration>> {
        let mut medians = BTreeMap::new();
        for (query, durations) in self {
            if durations.is_empty() {
                continue;
            }
            medians.insert(query.clone(), durations.median()?);
        }
        Ok(medians)
    }

    fn average(&self) -> Result<BTreeMap<String, Duration>> {
        let mut averages = BTreeMap::new();
        for (query, durations) in self {
            if durations.is_empty() {
                continue;
            }
            averages.insert(query.clone(), durations.average()?);
        }
        Ok(averages)
    }

    fn statistical_set(&self) -> Result<BTreeMap<String, Vec<Duration>>> {
        let mut statistical_sets = BTreeMap::new();
        for (query, durations) in self {
            if durations.is_empty() {
                continue;
            }
            statistical_sets.insert(query.clone(), durations.statistical_set()?);
        }
        Ok(statistical_sets)
    }
}

pub struct QueryMetrics<T: ExtendedMetrics> {
    pub run_id: Uuid,
    pub run_name: String,
    pub commit_sha: String,
    pub branch_name: String,
    pub test_type: TestType,
    pub started_at: usize,
    pub finished_at: usize,
    pub metrics: Vec<QueryMetric<T>>,
}

impl<T: ExtendedMetrics> QueryMetrics<T> {
    #[must_use]
    pub fn schema() -> SchemaRef {
        let extended_fields = T::fields();

        let mut base_fields = vec![
            Field::new("run_id", DataType::Utf8, false),
            Field::new("run_name", DataType::Utf8, false),
            Field::new("commit_sha", DataType::Utf8, false),
            Field::new("branch_name", DataType::Utf8, false),
            Field::new("test_type", DataType::Utf8, false),
            Field::new("started_at", DataType::UInt64, false),
            Field::new("finished_at", DataType::UInt64, false),
            Field::new("query_name", DataType::Utf8, false),
            Field::new("query_status", DataType::Utf8, false),
            Field::new("average_duration", DataType::Float64, false),
            Field::new("median_duration", DataType::Float64, false),
            Field::new("percentile_99_duration", DataType::Float64, false),
            Field::new("percentile_90_duration", DataType::Float64, false),
            Field::new("run_count", DataType::UInt64, false),
        ];

        base_fields.extend(extended_fields);

        Arc::new(Schema::new(base_fields))
    }

    pub fn build_extended_metrics(&self) -> Result<BTreeMap<String, Builder>> {
        let mut extended_metrics_builders = T::builders();
        for e in self.metrics.iter().map(|m| m.extended_metrics.as_ref()) {
            if let Some(extended_metrics) = e {
                let extended_metrics = extended_metrics.build()?;
                for target in extended_metrics {
                    match target {
                        BuilderTarget::String((name, value)) => {
                            match extended_metrics_builders.get_mut(&name) {
                                Some(Builder::String(builder)) => builder.append_value(value),
                                Some(b) => {
                                    return Err(anyhow::anyhow!(
                                        "Invalid builder type for String: {b}"
                                    ))
                                }
                                None => {
                                    return Err(anyhow::anyhow!(
                                        "No builder found for String: {name}"
                                    ))
                                }
                            }
                        }
                        BuilderTarget::UInt64((name, value)) => {
                            match extended_metrics_builders.get_mut(&name) {
                                Some(Builder::UInt64(builder)) => builder.append_value(value),
                                Some(b) => {
                                    return Err(anyhow::anyhow!(
                                        "Invalid builder type for UInt64: {b}"
                                    ))
                                }
                                None => {
                                    return Err(anyhow::anyhow!(
                                        "No builder found for UInt64: {name}"
                                    ))
                                }
                            }
                        }
                        BuilderTarget::Float64((name, value)) => {
                            match extended_metrics_builders.get_mut(&name) {
                                Some(Builder::Float64(builder)) => builder.append_value(value),
                                Some(b) => {
                                    return Err(anyhow::anyhow!(
                                        "Invalid builder type for Float64: {b}"
                                    ))
                                }
                                None => {
                                    return Err(anyhow::anyhow!(
                                        "No builder found for Float64: {name}"
                                    ))
                                }
                            }
                        }
                    }
                }
            } else {
                extended_metrics_builders
                    .iter_mut()
                    .for_each(|(_, builder)| match builder {
                        Builder::String(builder) => builder.append_null(),
                        Builder::UInt64(builder) => builder.append_null(),
                        Builder::Float64(builder) => builder.append_null(),
                    });
            }
        }

        Ok(extended_metrics_builders)
    }

    pub fn build(&self) -> Result<Vec<RecordBatch>> {
        let run_id = vec![self.run_id.to_string(); self.metrics.len()];
        let run_name = vec![self.run_name.clone(); self.metrics.len()];
        let commit_sha = vec![self.commit_sha.clone(); self.metrics.len()];
        let branch_name = vec![self.branch_name.clone(); self.metrics.len()];
        let test_type = vec![self.test_type.to_string(); self.metrics.len()];
        let started_at = vec![self.started_at as u64; self.metrics.len()];
        let finished_at = vec![self.finished_at as u64; self.metrics.len()];

        let query_name = self
            .metrics
            .iter()
            .map(|metric| metric.query_name.clone())
            .collect::<Vec<_>>();
        let query_status = self
            .metrics
            .iter()
            .map(|metric| metric.query_status.to_string())
            .collect::<Vec<_>>();
        let average_duration = self
            .metrics
            .iter()
            .map(|metric| metric.average_duration)
            .collect::<Vec<_>>();
        let median_duration = self
            .metrics
            .iter()
            .map(|metric| metric.median_duration)
            .collect::<Vec<_>>();
        let percentile_99_duration = self
            .metrics
            .iter()
            .map(|metric| metric.percentile_99_duration)
            .collect::<Vec<_>>();
        let percentile_90_duration = self
            .metrics
            .iter()
            .map(|metric| metric.percentile_90_duration)
            .collect::<Vec<_>>();
        let run_count = self
            .metrics
            .iter()
            .map(|metric| metric.run_count as u64)
            .collect::<Vec<_>>();

        let extended_metrics_fields = T::fields();
        let extended_metrics_field_names = extended_metrics_fields
            .iter()
            .map(arrow::datatypes::Field::name)
            .collect::<Vec<_>>();

        let mut columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(run_id)),
            Arc::new(StringArray::from(run_name)),
            Arc::new(StringArray::from(commit_sha)),
            Arc::new(StringArray::from(branch_name)),
            Arc::new(StringArray::from(test_type)),
            Arc::new(UInt64Array::from(started_at)),
            Arc::new(UInt64Array::from(finished_at)),
            Arc::new(StringArray::from(query_name)),
            Arc::new(StringArray::from(query_status)),
            Arc::new(Float64Array::from(average_duration)),
            Arc::new(Float64Array::from(median_duration)),
            Arc::new(Float64Array::from(percentile_99_duration)),
            Arc::new(Float64Array::from(percentile_90_duration)),
            Arc::new(UInt64Array::from(run_count)),
        ];

        if !extended_metrics_fields.is_empty() {
            let mut extended_metrics_builders = self.build_extended_metrics()?;

            for field in extended_metrics_field_names {
                match extended_metrics_builders.get_mut(field) {
                    Some(Builder::String(builder)) => columns.push(Arc::new(builder.finish())),
                    Some(Builder::UInt64(builder)) => columns.push(Arc::new(builder.finish())),
                    Some(Builder::Float64(builder)) => columns.push(Arc::new(builder.finish())),
                    None => {
                        return Err(anyhow::anyhow!(
                            "No builder found for extended metric field: {field}"
                        ))
                    }
                }
            }
        }

        Ok(vec![RecordBatch::try_new(Self::schema(), columns)?])
    }

    pub fn show(&self) -> Result<()> {
        print_batches(&self.build()?)?;

        Ok(())
    }
}

pub trait MetricCollector<T: ExtendedMetrics> {
    fn collect(&self) -> Result<QueryMetrics<T>>;
}

#[derive(Debug)]
pub enum Builder {
    String(StringBuilder),
    UInt64(UInt64Builder),
    Float64(Float64Builder),
}

#[derive(Debug, Clone)]
pub enum BuilderTarget {
    String((String, String)),
    UInt64((String, u64)),
    Float64((String, f64)),
}

impl Display for Builder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Builder::String(_) => write!(f, "StringBuilder"),
            Builder::UInt64(_) => write!(f, "UInt64Builder"),
            Builder::Float64(_) => write!(f, "Float64Builder"),
        }
    }
}

pub trait ExtendedMetrics {
    fn fields() -> Vec<Field>;
    fn builders() -> BTreeMap<String, Builder>;
    fn build(&self) -> Result<Vec<BuilderTarget>>;
}

pub struct NoExtendedMetrics {}
impl ExtendedMetrics for NoExtendedMetrics {
    fn fields() -> Vec<Field> {
        vec![]
    }

    fn builders() -> BTreeMap<String, Builder> {
        BTreeMap::new()
    }

    fn build(&self) -> Result<Vec<BuilderTarget>> {
        Ok(vec![])
    }
}

pub struct ExampleHelloWorldMetrics {
    pub world: String,
}
impl ExtendedMetrics for ExampleHelloWorldMetrics {
    fn fields() -> Vec<Field> {
        vec![Field::new("hello", DataType::Utf8, false)]
    }

    fn builders() -> BTreeMap<String, Builder> {
        let mut builders = BTreeMap::new();
        builders.insert("hello".to_string(), Builder::String(StringBuilder::new()));
        builders
    }

    fn build(&self) -> Result<Vec<BuilderTarget>> {
        Ok(vec![BuilderTarget::String((
            "hello".to_string(),
            self.world.clone(),
        ))])
    }
}
