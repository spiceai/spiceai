/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

use std::io::Write;
use std::path::Path;

use chrono::{DateTime, Utc};
use serde::Serialize;
use snafu::ResultExt;

use crate::error::{self, Result};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum TestStatus {
    Passed,
    Failed,
    Skipped,
    Error,
}

#[derive(Clone, Debug, Serialize)]
pub struct CaseResult {
    pub test_id: String,
    pub description: String,
    pub status: TestStatus,
    pub execution_time_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_message: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct ComplianceReport {
    pub suite_name: String,
    pub suite_version: String,
    pub engine_name: String,
    pub engine_version: String,
    pub mode: String,
    pub ibm_tag: String,
    pub datafusion_pin: String,
    pub start_time: DateTime<Utc>,
    pub end_time: DateTime<Utc>,
    pub passed: usize,
    pub failed: usize,
    pub skipped: usize,
    pub errored: usize,
    pub total: usize,
    pub pass_rate_pct: f64,
    pub results: Vec<CaseResult>,
}

pub struct ReportMeta {
    pub suite_name: String,
    pub suite_version: String,
    pub engine_name: String,
    pub engine_version: String,
    pub mode: String,
    pub ibm_tag: String,
    pub datafusion_pin: String,
    pub start_time: DateTime<Utc>,
}

impl ComplianceReport {
    pub fn finish(meta: ReportMeta, results: Vec<CaseResult>) -> Self {
        let ReportMeta {
            suite_name,
            suite_version,
            engine_name,
            engine_version,
            mode,
            ibm_tag,
            datafusion_pin,
            start_time,
        } = meta;
        let passed = results
            .iter()
            .filter(|r| r.status == TestStatus::Passed)
            .count();
        let failed = results
            .iter()
            .filter(|r| r.status == TestStatus::Failed)
            .count();
        let skipped = results
            .iter()
            .filter(|r| r.status == TestStatus::Skipped)
            .count();
        let errored = results
            .iter()
            .filter(|r| r.status == TestStatus::Error)
            .count();
        let total = results.len();
        let pass_rate_pct = if total == 0 {
            0.0
        } else {
            // TPC-H is 22 cases; the cast is exact well below the 52-bit mantissa.
            #[expect(clippy::cast_precision_loss)]
            {
                (passed as f64 / total as f64) * 100.0
            }
        };
        Self {
            suite_name,
            suite_version,
            engine_name,
            engine_version,
            mode,
            ibm_tag,
            datafusion_pin,
            start_time,
            end_time: Utc::now(),
            passed,
            failed,
            skipped,
            errored,
            total,
            pass_rate_pct,
            results,
        }
    }

    pub fn write_json(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).context(error::WriteFileSnafu {
                path: path.to_path_buf(),
            })?;
        }
        let json = serde_json::to_string_pretty(self).context(error::JsonSerializeSnafu)?;
        std::fs::write(path, json).context(error::WriteFileSnafu {
            path: path.to_path_buf(),
        })
    }

    pub fn write_csv(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).context(error::WriteFileSnafu {
                path: path.to_path_buf(),
            })?;
        }
        let mut file = std::fs::File::create(path).context(error::WriteFileSnafu {
            path: path.to_path_buf(),
        })?;
        writeln!(file, "test_id,status,execution_time_ms,error_message").context(
            error::WriteFileSnafu {
                path: path.to_path_buf(),
            },
        )?;
        for case in &self.results {
            let err = case
                .error_message
                .as_deref()
                .unwrap_or("")
                .replace('"', "\"\"");
            writeln!(
                file,
                "{},{:?},{},\"{}\"",
                case.test_id, case.status, case.execution_time_ms, err
            )
            .context(error::WriteFileSnafu {
                path: path.to_path_buf(),
            })?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pass_rate_includes_skips_in_denominator() {
        let results = vec![
            CaseResult {
                test_id: "q01".to_string(),
                description: String::new(),
                status: TestStatus::Passed,
                execution_time_ms: 1,
                error_message: None,
            },
            CaseResult {
                test_id: "q02".to_string(),
                description: String::new(),
                status: TestStatus::Skipped,
                execution_time_ms: 0,
                error_message: Some("no expected output".to_string()),
            },
        ];
        let report = ComplianceReport::finish(
            ReportMeta {
                suite_name: "tpch".to_string(),
                suite_version: "1.0.0".to_string(),
                engine_name: "DataFusion".to_string(),
                engine_version: "54.1".to_string(),
                mode: "mode-a".to_string(),
                ibm_tag: "v0.1.1".to_string(),
                datafusion_pin: "test".to_string(),
                start_time: Utc::now(),
            },
            results,
        );
        assert_eq!(report.passed, 1);
        assert_eq!(report.skipped, 1);
        assert_eq!(report.total, 2);
        assert!((report.pass_rate_pct - 50.0).abs() < f64::EPSILON);
    }
}
