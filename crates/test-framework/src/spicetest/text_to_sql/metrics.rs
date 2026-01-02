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

use std::collections::BTreeMap;

use crate::metrics::{Builder, BuilderTarget, ExtendedMetrics};
use anyhow::Result;
use arrow::{
    array::{Float64Builder, StringBuilder, UInt64Builder},
    datatypes::{DataType, Field},
};

pub struct TextToSqlMetric {
    pub generated_sql: String,
    pub expected_sql: String,
    pub number_of_attempts: usize,
    pub sample_data_enabled: bool,
    pub return_sql: bool,
    pub is_error: bool,
}

impl ExtendedMetrics for TextToSqlMetric {
    fn fields() -> Vec<Field> {
        vec![
            Field::new("generated_sql", DataType::Utf8, false),
            Field::new("expected_sql", DataType::Utf8, false),
            Field::new("number_of_attempts", DataType::UInt64, false),
            Field::new("sample_data_enabled", DataType::Utf8, false),
            Field::new("return_sql", DataType::Utf8, false),
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
                "number_of_attempts".to_string(),
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
        ])
    }

    fn build(&self) -> Result<Vec<BuilderTarget>> {
        Ok(vec![
            BuilderTarget::String(("generated_sql".to_string(), self.generated_sql.clone())),
            BuilderTarget::String(("expected_sql".to_string(), self.expected_sql.clone())),
            BuilderTarget::UInt64((
                "number_of_attempts".to_string(),
                self.number_of_attempts as u64,
            )),
            BuilderTarget::String((
                "sample_data_enabled".to_string(),
                self.sample_data_enabled.to_string(),
            )),
            BuilderTarget::String(("return_sql".to_string(), self.return_sql.to_string())),
        ])
    }
}

impl TextToSqlMetric {
    #[must_use]
    pub fn new(
        generated_sql: String,
        expected_sql: String,
        number_of_attempts: usize,
        sample_data_enabled: bool,
        return_sql: bool,
        is_error: bool,
    ) -> Self {
        Self {
            generated_sql,
            expected_sql,
            number_of_attempts,
            sample_data_enabled,
            return_sql,
            is_error,
        }
    }
}

pub struct TextToSqlRunMetric {
    pub p95_latency_ms: f64,
    pub median_latency_ms: f64,
    pub avg_attempts: f64,
    pub exact_match_rate: f64,
    pub error_rate: f64,
}

impl ExtendedMetrics for TextToSqlRunMetric {
    fn fields() -> Vec<Field> {
        vec![
            Field::new("p95_latency_ms", DataType::Float64, false),
            Field::new("median_latency_ms", DataType::Float64, false),
            Field::new("avg_attempts", DataType::Float64, false),
            Field::new("exact_match_rate", DataType::Float64, false),
            Field::new("error_rate", DataType::Float64, false),
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
                "avg_attempts".to_string(),
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
        ])
    }

    fn build(&self) -> Result<Vec<BuilderTarget>> {
        Ok(vec![
            BuilderTarget::Float64(("p95_latency_ms".to_string(), self.p95_latency_ms)),
            BuilderTarget::Float64(("median_latency_ms".to_string(), self.median_latency_ms)),
            BuilderTarget::Float64(("avg_attempts".to_string(), self.avg_attempts)),
            BuilderTarget::Float64(("exact_match_rate".to_string(), self.exact_match_rate)),
            BuilderTarget::Float64(("error_rate".to_string(), self.error_rate)),
        ])
    }
}

impl TextToSqlRunMetric {
    #[must_use]
    pub fn new(
        p95_latency_ms: f64,
        median_latency_ms: f64,
        avg_attempts: f64,
        exact_match_rate: f64,
        error_rate: f64,
    ) -> Self {
        Self {
            p95_latency_ms,
            median_latency_ms,
            avg_attempts,
            exact_match_rate,
            error_rate,
        }
    }
}
