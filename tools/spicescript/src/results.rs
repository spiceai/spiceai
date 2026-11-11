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

//! `--results`: every script's outcome as JSON, passes included, so a
//! script's failure rate across CI runs can be read off the files of those
//! runs instead of parsed out of their logs. The file is rewritten as each
//! script finishes, so a run that is cancelled or times out still leaves the
//! scripts it finished; `complete` says whether it got to the end.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::Serialize;

use crate::{
    ledger::{self, Entry, Verdict},
    runner::{Failure, Report},
    script::Script,
};

#[derive(Serialize)]
pub struct Results {
    /// The `--partition` share this run was, as `I/N`.
    pub partition: Option<String>,
    /// Whether every selected script finished.
    pub complete: bool,
    pub seconds: f64,
    pub scripts: Vec<ScriptResult>,
}

#[derive(Serialize)]
pub struct ScriptResult {
    pub name: String,
    /// The script's file, as found from the command line.
    pub file: PathBuf,
    /// `passed` when nothing failed other than as the ledger expects.
    pub outcome: Outcome,
    /// How long it held slots.
    pub seconds: f64,
    pub checks_passed: usize,
    pub expected_failures: usize,
    /// Every check that failed, expected or not.
    pub failures: Vec<FailureResult>,
    /// Ledger entries whose check did not fail.
    pub no_longer_failing: Vec<Entry>,
    /// Each problem as the run prints it after `FAILED`.
    pub problems: Vec<String>,
    pub notes: Vec<String>,
}

#[derive(Serialize, Clone, Copy, PartialEq, Eq, Debug)]
#[serde(rename_all = "kebab-case")]
pub enum Outcome {
    Passed,
    Failed,
}

#[derive(Serialize)]
pub struct FailureResult {
    pub run: String,
    pub who: String,
    pub step: usize,
    pub what: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query: Option<u64>,
    /// Starting a source or an instance, not a step.
    pub setup: bool,
    pub ledger: LedgerStatus,
    pub message: String,
}

/// How a failure stands against the expected-failure ledger.
#[derive(Serialize, Clone, Copy, PartialEq, Eq, Debug)]
#[serde(rename_all = "kebab-case")]
pub enum LedgerStatus {
    /// Failed as recorded.
    Expected,
    /// Recorded, but failed another way.
    Changed,
    /// Not recorded.
    New,
    /// `--record-failures` recorded it.
    Recorded,
}

impl ScriptResult {
    /// A script's result, from its report and, unless the run records the
    /// ledger, its verdict against it.
    pub fn new(
        script: &Script,
        file: &Path,
        report: &Report,
        took: std::time::Duration,
        verdict: Option<&Verdict>,
        problems: &[String],
    ) -> Self {
        let status = |f: &Failure| match verdict {
            None => LedgerStatus::Recorded,
            Some(v) if v.new.iter().any(|n| ledger::same_check(n, f)) => LedgerStatus::New,
            Some(v) if v.changed.iter().any(|(n, _)| ledger::same_check(n, f)) => {
                LedgerStatus::Changed
            }
            Some(_) => LedgerStatus::Expected,
        };
        Self {
            name: script.name.clone(),
            file: file.to_path_buf(),
            outcome: if problems.is_empty() {
                Outcome::Passed
            } else {
                Outcome::Failed
            },
            seconds: took.as_secs_f64(),
            checks_passed: report.passed,
            expected_failures: verdict.map_or(0, |v| v.expected),
            failures: report
                .failures
                .iter()
                .map(|f| FailureResult {
                    run: f.run.clone(),
                    who: f.who.clone(),
                    step: f.step,
                    what: f.what.clone(),
                    query: f.query,
                    setup: f.is_outside_steps(),
                    ledger: status(f),
                    message: f.message.clone(),
                })
                .collect(),
            no_longer_failing: verdict.map(|v| v.gone.clone()).unwrap_or_default(),
            problems: problems.to_vec(),
            notes: verdict.map(|v| v.notes.clone()).unwrap_or_default(),
        }
    }
}

impl Results {
    /// Writes the file whole, through a temporary beside it, so a reader
    /// never sees half of one.
    pub fn write(&self, path: &Path) -> Result<()> {
        if let Some(dir) = path.parent().filter(|d| !d.as_os_str().is_empty()) {
            std::fs::create_dir_all(dir)
                .with_context(|| format!("Failed to create {}", dir.display()))?;
        }
        let tmp = path.with_extension("json.tmp");
        let text = serde_json::to_string_pretty(self)?;
        std::fs::write(&tmp, text).with_context(|| format!("Failed to write {}", tmp.display()))?;
        std::fs::rename(&tmp, path).with_context(|| format!("Failed to write {}", path.display()))
    }
}
