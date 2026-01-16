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

use std::{collections::BTreeMap, hash::Hash, time::Duration};

use crate::{
    metrics::{Builder, BuilderTarget, ExtendedMetrics},
    spicetest::text_to_sql::{
        parse::{attempt_parse_table_and_projection, extract_tables_and_projection},
        task_history::TaskHistoryMetrics,
    },
};
use anyhow::Result;
use arrow::{
    array::{Float64Builder, StringBuilder, UInt64Builder},
    datatypes::{DataType, Field},
};
use serde_json::Value;

#[derive(Debug, Clone)]
pub struct TextToSqlMetric {
    pub question: String,
    pub generated_sql: String,
    pub expected_sql: String,
    pub sample_data_enabled: bool,
    pub return_sql: bool,
    pub is_error: bool,

    // Non-functional metrics
    pub latency_ms: f64,
    pub sql_duration_ms: f64,
    pub sql_query_count: usize,
    pub llm_duration_ms: f64,
    pub llm_count: usize,
    pub llm_input_tokens: u64,
    pub llm_output_tokens: u64,

    // Functional metrics
    pub exact_match: u64,
    pub exact_logical_plan_match: u64,
    pub correct_tables: f64,
    pub correct_table_projections: f64,
    pub correct_output_schema: f64,
}

impl ExtendedMetrics for TextToSqlMetric {
    fn fields() -> Vec<Field> {
        vec![
            Field::new("generated_sql", DataType::Utf8, false),
            Field::new("expected_sql", DataType::Utf8, false),
            Field::new("sql_query_count", DataType::UInt64, false),
            Field::new("sample_data_enabled", DataType::Utf8, false),
            Field::new("return_sql", DataType::Utf8, false),
            Field::new("is_error", DataType::Utf8, false),
            // Non-functional metrics
            Field::new("latency_ms", DataType::Float64, false),
            Field::new("sql_duration_ms", DataType::Float64, false),
            Field::new("llm_duration_ms", DataType::Float64, false),
            Field::new("llm_count", DataType::UInt64, false),
            Field::new("llm_input_tokens", DataType::UInt64, false),
            Field::new("llm_output_tokens", DataType::UInt64, false),
            // Functional metrics
            Field::new("exact_match", DataType::UInt64, false),
            Field::new("exact_logical_plan_match", DataType::UInt64, false),
            Field::new("correct_tables", DataType::Float64, false),
            Field::new("correct_table_projections", DataType::Float64, false),
            Field::new("correct_output_schema", DataType::Float64, false),
        ]
    }

    fn builders() -> BTreeMap<String, Builder> {
        BTreeMap::from([
            (
                "generated_sql".to_string(),
                Builder::String(StringBuilder::new()),
            ),
            (
                "expected_sql".to_string(),
                Builder::String(StringBuilder::new()),
            ),
            (
                "sql_query_count".to_string(),
                Builder::UInt64(UInt64Builder::new()),
            ),
            (
                "sample_data_enabled".to_string(),
                Builder::String(StringBuilder::new()),
            ),
            (
                "return_sql".to_string(),
                Builder::String(StringBuilder::new()),
            ),
            (
                "is_error".to_string(),
                Builder::String(StringBuilder::new()),
            ),
            // Non-functional metrics
            (
                "latency_ms".to_string(),
                Builder::Float64(Float64Builder::new()),
            ),
            (
                "sql_duration_ms".to_string(),
                Builder::Float64(Float64Builder::new()),
            ),
            (
                "llm_duration_ms".to_string(),
                Builder::Float64(Float64Builder::new()),
            ),
            (
                "llm_count".to_string(),
                Builder::UInt64(UInt64Builder::new()),
            ),
            (
                "sql_query_count".to_string(),
                Builder::UInt64(UInt64Builder::new()),
            ),
            (
                "llm_input_tokens".to_string(),
                Builder::UInt64(UInt64Builder::new()),
            ),
            (
                "llm_output_tokens".to_string(),
                Builder::UInt64(UInt64Builder::new()),
            ),
            // Functional metrics
            (
                "exact_match".to_string(),
                Builder::UInt64(UInt64Builder::new()),
            ),
            (
                "exact_logical_plan_match".to_string(),
                Builder::UInt64(UInt64Builder::new()),
            ),
            (
                "correct_tables".to_string(),
                Builder::Float64(Float64Builder::new()),
            ),
            (
                "correct_table_projections".to_string(),
                Builder::Float64(Float64Builder::new()),
            ),
            (
                "correct_output_schema".to_string(),
                Builder::Float64(Float64Builder::new()),
            ),
        ])
    }

    fn build(&self) -> Result<Vec<BuilderTarget>> {
        Ok(vec![
            BuilderTarget::String(("generated_sql".to_string(), self.generated_sql.clone())),
            BuilderTarget::String(("expected_sql".to_string(), self.expected_sql.clone())),
            BuilderTarget::UInt64(("sql_query_count".to_string(), self.sql_query_count as u64)),
            BuilderTarget::String((
                "sample_data_enabled".to_string(),
                self.sample_data_enabled.to_string(),
            )),
            BuilderTarget::String(("return_sql".to_string(), self.return_sql.to_string())),
            BuilderTarget::String(("is_error".to_string(), self.is_error.to_string())),
            // Non-functional metrics
            BuilderTarget::Float64(("latency_ms".to_string(), self.latency_ms)),
            BuilderTarget::Float64(("sql_duration_ms".to_string(), self.sql_duration_ms)),
            BuilderTarget::Float64(("llm_duration_ms".to_string(), self.llm_duration_ms)),
            BuilderTarget::UInt64(("llm_count".to_string(), self.llm_count as u64)),
            BuilderTarget::UInt64(("llm_input_tokens".to_string(), self.llm_input_tokens)),
            BuilderTarget::UInt64(("llm_output_tokens".to_string(), self.llm_output_tokens)),
            // Functional metrics
            BuilderTarget::UInt64(("exact_match".to_string(), self.exact_match)),
            BuilderTarget::UInt64((
                "exact_logical_plan_match".to_string(),
                self.exact_logical_plan_match,
            )),
            BuilderTarget::Float64(("correct_tables".to_string(), self.correct_tables)),
            BuilderTarget::Float64((
                "correct_table_projections".to_string(),
                self.correct_table_projections,
            )),
            BuilderTarget::Float64((
                "correct_output_schema".to_string(),
                self.correct_output_schema,
            )),
        ])
    }
}

impl TextToSqlMetric {
    #[expect(clippy::too_many_arguments)]
    pub fn new(
        question: String,
        generated_sql: &str,
        expected_sql: &str,
        expected_logical_plan: &Value,
        generated_logical_plan: Option<&Value>,
        is_error: bool,
        duration: Duration,
        sample_data_enabled: bool,
        return_sql: bool,
        task_history_metrics: &TaskHistoryMetrics,
        correct_output_schema: f64,
    ) -> Self {
        let expected = extract_tables_and_projection(expected_logical_plan);
        let generated = generated_logical_plan
            .map(extract_tables_and_projection)
            .or_else(|| {
                attempt_parse_table_and_projection(generated_sql)
                    .inspect_err(|e| eprintln!("Error in 'attempt_parse_table_and_projection'.{e}"))
                    .ok()
            });
        Self {
            question,
            generated_sql: generated_sql.to_string(),
            expected_sql: expected_sql.to_string(),
            sql_query_count: task_history_metrics.sql_count,
            sample_data_enabled,
            return_sql,
            is_error,
            #[expect(clippy::cast_precision_loss)]
            latency_ms: duration.as_millis() as f64,
            sql_duration_ms: task_history_metrics.sql_duration_ms,
            llm_duration_ms: task_history_metrics.llm_duration_ms,
            llm_count: task_history_metrics.llm_count,
            llm_input_tokens: task_history_metrics.llm_input_tokens,
            llm_output_tokens: task_history_metrics.llm_output_tokens,
            exact_match: (generated_sql.trim() == expected_sql.trim()).into(),
            exact_logical_plan_match: generated_logical_plan
                .map(|g| (g == expected_logical_plan).into())
                .unwrap_or_default(),
            correct_tables: generated
                .as_ref()
                .map(|g| intersection_over_union(&expected.0, &g.0))
                .unwrap_or_default(),
            correct_table_projections: generated
                .as_ref()
                .map(|g| intersection_over_union(&expected.1, &g.1))
                .unwrap_or_default(),
            correct_output_schema,
        }
    }
}

pub struct TextToSqlRunMetric {
    pub p95_latency_ms: f64,
    pub median_latency_ms: f64,
    pub exact_match_rate: f64,
    pub error_rate: f64,

    // New aggregate metrics
    pub mean_sql_query_count: f64,
    pub mean_llm_input_tokens: f64,
    pub mean_llm_output_tokens: f64,

    pub exact_logical_plan_match_rate: f64,
    pub mean_correct_tables: f64,
    pub mean_correct_table_projections: f64,
    pub mean_correct_output_schema: f64,
}

impl ExtendedMetrics for TextToSqlRunMetric {
    fn fields() -> Vec<Field> {
        vec![
            Field::new("p95_latency_ms", DataType::Float64, false),
            Field::new("median_latency_ms", DataType::Float64, false),
            Field::new("exact_match_rate", DataType::Float64, false),
            Field::new("error_rate", DataType::Float64, false),
            Field::new("mean_sql_query_count", DataType::Float64, false),
            Field::new("mean_llm_input_tokens", DataType::Float64, false),
            Field::new("mean_llm_output_tokens", DataType::Float64, false),
            Field::new("exact_logical_plan_match_rate", DataType::Float64, false),
            Field::new("mean_correct_tables", DataType::Float64, false),
            Field::new("mean_correct_table_projections", DataType::Float64, false),
            Field::new("mean_correct_output_schema", DataType::Float64, false),
        ]
    }

    fn builders() -> BTreeMap<String, Builder> {
        BTreeMap::from([
            (
                "p95_latency_ms".to_string(),
                Builder::Float64(Float64Builder::new()),
            ),
            (
                "median_latency_ms".to_string(),
                Builder::Float64(Float64Builder::new()),
            ),
            (
                "exact_match_rate".to_string(),
                Builder::Float64(Float64Builder::new()),
            ),
            (
                "error_rate".to_string(),
                Builder::Float64(Float64Builder::new()),
            ),
            (
                "mean_sql_query_count".to_string(),
                Builder::Float64(Float64Builder::new()),
            ),
            (
                "mean_llm_input_tokens".to_string(),
                Builder::Float64(Float64Builder::new()),
            ),
            (
                "mean_llm_output_tokens".to_string(),
                Builder::Float64(Float64Builder::new()),
            ),
            (
                "exact_logical_plan_match_rate".to_string(),
                Builder::Float64(Float64Builder::new()),
            ),
            (
                "mean_correct_tables".to_string(),
                Builder::Float64(Float64Builder::new()),
            ),
            (
                "mean_correct_table_projections".to_string(),
                Builder::Float64(Float64Builder::new()),
            ),
            (
                "mean_correct_output_schema".to_string(),
                Builder::Float64(Float64Builder::new()),
            ),
        ])
    }

    fn build(&self) -> Result<Vec<BuilderTarget>> {
        Ok(vec![
            BuilderTarget::Float64(("p95_latency_ms".to_string(), self.p95_latency_ms)),
            BuilderTarget::Float64(("median_latency_ms".to_string(), self.median_latency_ms)),
            BuilderTarget::Float64(("exact_match_rate".to_string(), self.exact_match_rate)),
            BuilderTarget::Float64(("error_rate".to_string(), self.error_rate)),
            BuilderTarget::Float64((
                "mean_sql_query_count".to_string(),
                self.mean_sql_query_count,
            )),
            BuilderTarget::Float64((
                "mean_llm_input_tokens".to_string(),
                self.mean_llm_input_tokens,
            )),
            BuilderTarget::Float64((
                "mean_llm_output_tokens".to_string(),
                self.mean_llm_output_tokens,
            )),
            BuilderTarget::Float64((
                "exact_logical_plan_match_rate".to_string(),
                self.exact_logical_plan_match_rate,
            )),
            BuilderTarget::Float64(("mean_correct_tables".to_string(), self.mean_correct_tables)),
            BuilderTarget::Float64((
                "mean_correct_table_projections".to_string(),
                self.mean_correct_table_projections,
            )),
            BuilderTarget::Float64((
                "mean_correct_output_schema".to_string(),
                self.mean_correct_output_schema,
            )),
        ])
    }
}

impl TextToSqlRunMetric {
    #[must_use]
    #[expect(clippy::too_many_arguments)]
    pub fn new(
        p95_latency_ms: f64,
        median_latency_ms: f64,
        exact_match_rate: f64,
        error_rate: f64,
        mean_sql_query_count: f64,
        mean_llm_input_tokens: f64,
        mean_llm_output_tokens: f64,
        exact_logical_plan_match_rate: f64,
        mean_correct_tables: f64,
        mean_correct_table_projections: f64,
        mean_correct_output_schema: f64,
    ) -> Self {
        Self {
            p95_latency_ms,
            median_latency_ms,
            exact_match_rate,
            error_rate,
            mean_sql_query_count,
            mean_llm_input_tokens,
            mean_llm_output_tokens,
            exact_logical_plan_match_rate,
            mean_correct_tables,
            mean_correct_table_projections,
            mean_correct_output_schema,
        }
    }
}

/// Calculate the Intersection over Union (`IoU`) between two sets.
pub(crate) fn intersection_over_union<T: Eq + Hash>(
    set_a: &std::collections::HashSet<T>,
    set_b: &std::collections::HashSet<T>,
) -> f64 {
    let intersection: std::collections::HashSet<_> = set_a.intersection(set_b).collect();
    let union: std::collections::HashSet<_> = set_a.union(set_b).collect();

    if union.is_empty() {
        1.0 // Both sets are empty, consider them as perfectly matching
    } else {
        #[expect(clippy::cast_precision_loss)]
        let result = intersection.len() as f64 / union.len() as f64;
        result
    }
}
