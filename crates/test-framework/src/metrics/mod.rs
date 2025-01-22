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

use std::{
    collections::BTreeMap,
    fmt::Display,
    sync::Arc,
    time::{Duration, SystemTime},
};

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

const FLOAT_ERROR_MARGIN: f64 = 0.0001;

#[derive(Copy, Clone, PartialEq, Eq, Default)]
pub enum QueryStatus {
    #[default]
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
    pub median_duration: f64,
    pub percentile_99_duration: f64,
    pub percentile_95_duration: f64,
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
            median_duration: durations.median()?.as_secs_f64(),
            percentile_99_duration: durations.percentile(99.0)?.as_secs_f64(),
            percentile_95_duration: durations.percentile(95.0)?.as_secs_f64(),
            percentile_90_duration: durations.percentile(90.0)?.as_secs_f64(),
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
            median_duration: 0.0,
            percentile_99_duration: 0.0,
            percentile_95_duration: 0.0,
            percentile_90_duration: 0.0,
            run_count: 0,
            extended_metrics: None,
        }
    }
}

pub trait StatisticsCollector<T, C> {
    fn percentile(&self, percentile: f64) -> Result<T>;
    fn median(&self) -> Result<T>;
    fn statistical_set(&self) -> Result<C>;
}

impl StatisticsCollector<Duration, Vec<Duration>> for Vec<Duration> {
    fn percentile(&self, percentile: f64) -> Result<Duration> {
        let mut sorted_durations = self.clone();
        sorted_durations.sort();

        // safety: sorted_durations.len() cannot be negative, and is unlikely to be larger than u32::MAX
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        {
            let rank =
                (percentile / 100.0) * (f64::from(u32::try_from(sorted_durations.len() - 1)?));
            if (rank - rank.floor()).abs() < FLOAT_ERROR_MARGIN {
                Ok(sorted_durations[rank as usize])
            } else {
                let lower_duration = sorted_durations[rank.floor() as usize];
                let upper_duration = sorted_durations[rank.ceil() as usize];
                Ok((lower_duration + upper_duration) / 2)
            }
        }
    }

    fn median(&self) -> Result<Duration> {
        let half = self.len() / 2;
        if self.len() % 2 == 0 {
            Ok((self[half - 1] + self[half]) / 2)
        } else {
            Ok(self[half])
        }
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
            let first_quartile_secs = sorted_durations.percentile(25.0)?.as_secs_f64();
            let third_quartile_secs = sorted_durations.percentile(75.0)?.as_secs_f64();

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

/// A collection of metrics for a single test run
/// A single instance of a ``QueryMetrics`` represents a single test run
/// The generics T and R represent additional metric information that can exist, both for individual queries and the test run as a whole
/// T and R may not always be equal, as the test run may have different metrics than the individual queries
///
/// For example, the throughput test uses ``NoExtendedMetrics`` for the individual queries, but ``ThroughputMetrics`` for the test run
pub struct QueryMetrics<T: ExtendedMetrics, R: ExtendedMetrics> {
    pub run_id: Uuid,
    pub run_name: String,
    pub commit_sha: String,
    pub branch_name: String,
    pub test_type: TestType,
    pub started_at: usize,
    pub finished_at: usize,
    pub metrics: Vec<QueryMetric<T>>,
    pub run_metric: Option<R>,
    pub memory_usage: Option<f64>,
}

// Macro to help extract values from metric vecs
macro_rules! extract_metric_values {
    // no clone or to_string, direct copy
    ($metrics:expr, $field:ident) => {
        $metrics
            .iter()
            .map(|metric| metric.$field)
            .collect::<Vec<_>>()
    };

    // clone
    ($metrics:expr, $field:ident, clone) => {
        $metrics
            .iter()
            .map(|metric| metric.$field.clone())
            .collect::<Vec<_>>()
    };

    // to_string
    ($metrics:expr, $field:ident, to_string) => {
        $metrics
            .iter()
            .map(|metric| metric.$field.to_string())
            .collect::<Vec<_>>()
    };

    // as u64
    ($metrics:expr, $field:ident, as_u64) => {
        $metrics
            .iter()
            .map(|metric| metric.$field as u64)
            .collect::<Vec<_>>()
    };
}

impl<T: ExtendedMetrics, R: ExtendedMetrics> QueryMetrics<T, R> {
    #[must_use]
    pub fn with_run_metric(mut self, run_metric: R) -> Self {
        self.run_metric = Some(run_metric);
        self
    }

    #[must_use]
    pub fn with_memory_usage(mut self, memory_usage: f64) -> Self {
        self.memory_usage = Some(memory_usage);
        self
    }

    #[must_use]
    pub fn run_schema() -> SchemaRef {
        let extended_fields = R::fields();

        let mut base_fields = vec![
            Field::new("run_id", DataType::Utf8, false),
            Field::new("run_name", DataType::Utf8, false),
            Field::new("commit_sha", DataType::Utf8, false),
            Field::new("branch_name", DataType::Utf8, false),
            Field::new("test_type", DataType::Utf8, false),
            Field::new("started_at", DataType::UInt64, false),
            Field::new("finished_at", DataType::UInt64, false),
            Field::new("status", DataType::Utf8, false),
            Field::new("query_execution_count", DataType::UInt64, false),
            Field::new("memory_usage", DataType::Float64, true),
        ];

        base_fields.extend(extended_fields);

        Arc::new(Schema::new(base_fields))
    }

    /// Records do not need the values from the main run, because they contain a reference to the run ID to retrieve them
    /// Runs are 1:N with records
    #[must_use]
    pub fn records_schema() -> SchemaRef {
        let extended_fields = T::fields();

        let mut base_fields = vec![
            Field::new("run_id", DataType::Utf8, false),
            Field::new("query_name", DataType::Utf8, false),
            Field::new("query_status", DataType::Utf8, false),
            Field::new("median_duration", DataType::Float64, false),
            Field::new("percentile_99_duration", DataType::Float64, false),
            Field::new("percentile_95_duration", DataType::Float64, false),
            Field::new("percentile_90_duration", DataType::Float64, false),
            Field::new("run_count", DataType::UInt64, false),
        ];

        base_fields.extend(extended_fields);

        Arc::new(Schema::new(base_fields))
    }

    pub fn build_extended_metrics<'a, M>(
        &self,
        metrics_iter: impl Iterator<Item = Option<&'a M>>,
    ) -> Result<BTreeMap<String, Builder>>
    where
        M: ExtendedMetrics + 'a,
    {
        let mut extended_metrics_builders = M::builders();
        for e in metrics_iter {
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

    /// Builds record batches for the individual metrics of this test run
    /// For example, a record would be a single query execution
    pub fn build_records(&self) -> Result<Vec<RecordBatch>> {
        let run_id = vec![self.run_id.to_string(); self.metrics.len()];

        let query_name = extract_metric_values!(self.metrics, query_name, clone);
        let query_status = extract_metric_values!(self.metrics, query_status, to_string);
        let median_duration = extract_metric_values!(self.metrics, median_duration);
        let percentile_99_duration = extract_metric_values!(self.metrics, percentile_99_duration);
        let percentile_95_duration = extract_metric_values!(self.metrics, percentile_95_duration);
        let percentile_90_duration = extract_metric_values!(self.metrics, percentile_90_duration);
        let run_count = extract_metric_values!(self.metrics, run_count, as_u64);

        let extended_metrics_fields = T::fields();
        let extended_metrics_field_names = extended_metrics_fields
            .iter()
            .map(arrow::datatypes::Field::name)
            .collect::<Vec<_>>();

        let mut columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(run_id)),
            Arc::new(StringArray::from(query_name)),
            Arc::new(StringArray::from(query_status)),
            Arc::new(Float64Array::from(median_duration)),
            Arc::new(Float64Array::from(percentile_99_duration)),
            Arc::new(Float64Array::from(percentile_95_duration)),
            Arc::new(Float64Array::from(percentile_90_duration)),
            Arc::new(UInt64Array::from(run_count)),
        ];

        if !extended_metrics_fields.is_empty() {
            let mut extended_metrics_builders = self
                .build_extended_metrics(self.metrics.iter().map(|m| m.extended_metrics.as_ref()))?;

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

        Ok(vec![RecordBatch::try_new(Self::records_schema(), columns)?])
    }

    /// Builds record batches for the test run
    /// The record batch is a single row, representing the run as a whole - which can pass or fail separately from individual queries
    pub fn build_run(&self, status: QueryStatus) -> Result<Vec<RecordBatch>> {
        let run_id = vec![self.run_id.to_string()];
        let run_name = vec![self.run_name.clone()];
        let commit_sha = vec![self.commit_sha.clone()];
        let branch_name = vec![self.branch_name.clone()];
        let test_type = vec![self.test_type.to_string()];
        let started_at = vec![self.started_at as u64];
        let finished_at = vec![self.finished_at as u64];
        // the test can only pass if all queries pass, and the input status is a pass
        let status = [
            if self
                .metrics
                .iter()
                .all(|m| m.query_status == QueryStatus::Passed)
                && status == QueryStatus::Passed
            {
                QueryStatus::Passed
            } else {
                QueryStatus::Failed
            },
        ];

        let query_execution_count =
            vec![self.metrics.iter().fold(0, |acc, m| acc + m.run_count) as u64];

        let memory_usage = vec![self.memory_usage];

        let extended_metrics_fields = R::fields();
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
            Arc::new(StringArray::from(
                status
                    .iter()
                    .map(std::string::ToString::to_string)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(UInt64Array::from(query_execution_count)),
            Arc::new(Float64Array::from(memory_usage)),
        ];

        if !extended_metrics_fields.is_empty() {
            let mut extended_metrics_builders =
                self.build_extended_metrics(vec![self.run_metric.as_ref()].into_iter())?;

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

        Ok(vec![RecordBatch::try_new(Self::run_schema(), columns)?])
    }

    pub fn show_records(&self) -> Result<()> {
        print_batches(&self.build_records()?)?;

        Ok(())
    }

    pub fn show_run(&self, status: Option<QueryStatus>) -> Result<()> {
        print_batches(&self.build_run(status.unwrap_or_default())?)?;

        Ok(())
    }
}

pub trait MetricCollector<T: ExtendedMetrics, R: ExtendedMetrics> {
    fn start_time(&self) -> SystemTime;
    fn end_time(&self) -> SystemTime;
    fn name(&self) -> String;
    fn metrics(&self) -> Result<Vec<QueryMetric<T>>>;
    fn collect(&self, test_type: TestType) -> Result<QueryMetrics<T, R>> {
        Ok(QueryMetrics {
            run_id: uuid::Uuid::new_v4(),
            run_name: self.name(),
            commit_sha: "TODO".to_string(),
            branch_name: "TODO".to_string(),
            test_type,
            started_at: usize::try_from(
                self.start_time()
                    .duration_since(SystemTime::UNIX_EPOCH)?
                    .as_secs(),
            )?,
            finished_at: usize::try_from(
                self.end_time()
                    .duration_since(SystemTime::UNIX_EPOCH)?
                    .as_secs(),
            )?,
            metrics: self.metrics()?,
            memory_usage: None,
            run_metric: None,
        })
    }
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

pub struct ThroughputMetrics {
    pub throughput: f64,
}
impl ExtendedMetrics for ThroughputMetrics {
    fn fields() -> Vec<Field> {
        vec![Field::new("throughput", DataType::Float64, false)]
    }

    fn builders() -> BTreeMap<String, Builder> {
        let mut builders = BTreeMap::new();
        builders.insert(
            "throughput".to_string(),
            Builder::Float64(Float64Builder::new()),
        );
        builders
    }

    fn build(&self) -> Result<Vec<BuilderTarget>> {
        Ok(vec![BuilderTarget::Float64((
            "throughput".to_string(),
            self.throughput,
        ))])
    }
}
impl ThroughputMetrics {
    #[must_use]
    pub fn new(throughput: f64) -> Self {
        Self { throughput }
    }
}

#[cfg(test)]
mod test {
    use crate::metrics::StatisticsCollector;

    #[test]
    fn test_normal_percentiles_are_correct() {
        let durations = vec![
            std::time::Duration::from_secs(1),
            std::time::Duration::from_secs(2),
            std::time::Duration::from_secs(3),
            std::time::Duration::from_secs(4),
            std::time::Duration::from_secs(5),
        ];

        let third_percentile = durations
            .percentile(75.0)
            .expect("percentile should calculate");
        assert_eq!(third_percentile, std::time::Duration::from_secs(4));

        let second_percentile = durations
            .percentile(50.0)
            .expect("percentile should calculate");
        assert_eq!(second_percentile, std::time::Duration::from_secs(3));

        let first_percentile = durations
            .percentile(25.0)
            .expect("percentile should calculate");
        assert_eq!(first_percentile, std::time::Duration::from_secs(2));

        let zero_percentile = durations
            .percentile(0.0)
            .expect("percentile should calculate");
        assert_eq!(zero_percentile, std::time::Duration::from_secs(1));

        let hundred_percentile = durations
            .percentile(100.0)
            .expect("percentile should calculate");
        assert_eq!(hundred_percentile, std::time::Duration::from_secs(5));
    }

    #[test]
    fn test_unordered_percentiles() {
        let durations = vec![
            std::time::Duration::from_secs(4),
            std::time::Duration::from_secs(3),
            std::time::Duration::from_secs(5),
            std::time::Duration::from_secs(2),
            std::time::Duration::from_secs(1),
        ];

        let third_percentile = durations
            .percentile(75.0)
            .expect("percentile should calculate");
        assert_eq!(third_percentile, std::time::Duration::from_secs(4));

        let second_percentile = durations
            .percentile(50.0)
            .expect("percentile should calculate");
        assert_eq!(second_percentile, std::time::Duration::from_secs(3));

        let first_percentile = durations
            .percentile(25.0)
            .expect("percentile should calculate");
        assert_eq!(first_percentile, std::time::Duration::from_secs(2));

        let zero_percentile = durations
            .percentile(0.0)
            .expect("percentile should calculate");
        assert_eq!(zero_percentile, std::time::Duration::from_secs(1));

        let hundred_percentile = durations
            .percentile(100.0)
            .expect("percentile should calculate");
        assert_eq!(hundred_percentile, std::time::Duration::from_secs(5));
    }

    #[test]
    fn test_midpoint_percentiles_are_correct() {
        let durations = vec![
            std::time::Duration::from_secs(1), // Q0 - Q1 is 1.5
            std::time::Duration::from_secs(2), // Q2 - Q3 is 2.5
            std::time::Duration::from_secs(3), // Q4
        ];

        let second_percentile = durations
            .percentile(50.0)
            .expect("percentile should calculate");
        assert_eq!(second_percentile, std::time::Duration::from_secs(2));

        let first_percentile = durations
            .percentile(25.0)
            .expect("percentile should calculate");
        assert_eq!(first_percentile, std::time::Duration::from_millis(1500));

        let third_percentile = durations
            .percentile(75.0)
            .expect("percentile should calculate");
        assert_eq!(third_percentile, std::time::Duration::from_millis(2500));
    }
}
