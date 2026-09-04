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

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// IBM/substrait-compliance tag this harness is pinned to.
pub const IBM_SUBSTRAIT_COMPLIANCE_TAG: &str = "v0.1.1";

/// Commit SHA of `IBM_SUBSTRAIT_COMPLIANCE_TAG`.
pub const IBM_SUBSTRAIT_COMPLIANCE_COMMIT: &str = "7fff86d04a7124123a3f2692fa2a69de0b0a1704";

/// spiceai DataFusion fork revision (workspace `[patch.crates-io]` pin).
pub const SPICEAI_DATAFUSION_REV: &str = "f9a635e6b580d5fe6ed0a70975e36014ea86c476";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TestStatus {
    Passed,
    Failed,
    Skipped,
    Error,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CaseResult {
    pub id: String,
    pub description: String,
    pub status: TestStatus,
    pub execution_time_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComplianceReport {
    pub mode: String,
    pub engine: String,
    pub engine_version: String,
    pub datafusion_rev: String,
    pub ibm_substrait_compliance_tag: String,
    pub ibm_substrait_compliance_commit: String,
    pub suite: String,
    pub suite_version: String,
    pub started_at: DateTime<Utc>,
    pub finished_at: DateTime<Utc>,
    pub passed: usize,
    pub failed: usize,
    pub skipped: usize,
    pub error: usize,
    pub total: usize,
    pub pass_rate_pct: f64,
    pub cases: Vec<CaseResult>,
}

impl ComplianceReport {
    pub fn from_cases(
        mode: impl Into<String>,
        engine: impl Into<String>,
        engine_version: impl Into<String>,
        suite: impl Into<String>,
        suite_version: impl Into<String>,
        started_at: DateTime<Utc>,
        cases: Vec<CaseResult>,
    ) -> Self {
        let passed = cases
            .iter()
            .filter(|c| c.status == TestStatus::Passed)
            .count();
        let failed = cases
            .iter()
            .filter(|c| c.status == TestStatus::Failed)
            .count();
        let skipped = cases
            .iter()
            .filter(|c| c.status == TestStatus::Skipped)
            .count();
        let error = cases
            .iter()
            .filter(|c| c.status == TestStatus::Error)
            .count();
        let total = cases.len();
        let pass_rate_pct = if total == 0 {
            0.0
        } else {
            (passed as f64 / total as f64) * 100.0
        };

        Self {
            mode: mode.into(),
            engine: engine.into(),
            engine_version: engine_version.into(),
            datafusion_rev: SPICEAI_DATAFUSION_REV.to_string(),
            ibm_substrait_compliance_tag: IBM_SUBSTRAIT_COMPLIANCE_TAG.to_string(),
            ibm_substrait_compliance_commit: IBM_SUBSTRAIT_COMPLIANCE_COMMIT.to_string(),
            suite: suite.into(),
            suite_version: suite_version.into(),
            started_at,
            finished_at: Utc::now(),
            passed,
            failed,
            skipped,
            error,
            total,
            pass_rate_pct,
            cases,
        }
    }

    pub fn write_json(&self, path: &std::path::Path) -> crate::error::Result<()> {
        let text = serde_json::to_string_pretty(self)
            .map_err(|source| crate::error::Error::SerializeReport { source })?;
        std::fs::write(path, text).map_err(|source| crate::error::Error::WriteReport {
            path: path.display().to_string(),
            source,
        })?;
        Ok(())
    }

    pub fn print_summary(&self) {
        println!();
        println!("Substrait compliance report");
        println!("  mode:    {}", self.mode);
        println!("  engine:  {} {}", self.engine, self.engine_version);
        println!("  df rev:  {}", self.datafusion_rev);
        println!(
            "  ibm:     {} ({})",
            self.ibm_substrait_compliance_tag, self.ibm_substrait_compliance_commit
        );
        println!("  suite:   {} v{}", self.suite, self.suite_version);
        println!();
        println!("| status  | count |");
        println!("|---------|-------|");
        println!("| pass    | {:>5} |", self.passed);
        println!("| fail    | {:>5} |", self.failed);
        println!("| skip    | {:>5} |", self.skipped);
        println!("| error   | {:>5} |", self.error);
        println!("| total   | {:>5} |", self.total);
        println!("| rate    | {:>4.1}% |", self.pass_rate_pct);
        println!();
        println!("| id   | status  | ms   | detail |");
        println!("|------|---------|------|--------|");
        for case in &self.cases {
            let detail = case.error.as_deref().unwrap_or("");
            let detail = if detail.len() > 80 {
                format!("{}…", &detail[..77])
            } else {
                detail.to_string()
            };
            println!(
                "| {:<4} | {:<7} | {:>4} | {} |",
                case.id,
                format!("{:?}", case.status).to_lowercase(),
                case.execution_time_ms,
                detail
            );
        }
    }
}
