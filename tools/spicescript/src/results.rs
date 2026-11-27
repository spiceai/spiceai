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
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub summaries: Vec<ScriptSummary>,
    /// Scripts selected but not run, because a variable their `requires-env`
    /// names is unset. Kept apart from `scripts`, so nothing reading those
    /// counts one as passed or takes a run time from it.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub not_run: Vec<NotRun>,
}

#[derive(Serialize, Clone, Debug)]
pub struct NotRun {
    pub name: String,
    pub file: PathBuf,
    /// The `requires-env` variables that were unset or empty.
    pub missing_env: Vec<String>,
}

impl std::fmt::Display for NotRun {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} ({}): {} unset",
            self.name,
            self.file.display(),
            self.missing_env.join(", ")
        )
    }
}

#[derive(Serialize)]
pub struct ScriptResult {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub repetition: Option<u32>,
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

impl FailureResult {
    /// The failure's first line as the run prints it (`runner::Failure`'s
    /// `Display`), which also starts every ledger problem about this check.
    fn header(&self) -> String {
        let who = if self.who.is_empty() {
            String::new()
        } else {
            format!(" {}", self.who)
        };
        let query = self
            .query
            .map(|q| format!(" query {q}"))
            .unwrap_or_default();
        format!("{}{who} step {} {}{query}", self.run, self.step, self.what)
    }
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
            repetition: None,
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

/// Repetition outcomes, grouped by script file rather than display name.
#[derive(Serialize)]
pub struct ScriptSummary {
    pub name: String,
    pub file: PathBuf,
    pub passed: usize,
    pub total: usize,
    pub failure_groups: Vec<FailureGroup>,
}

#[derive(Serialize)]
pub struct FailureGroup {
    pub run: String,
    pub who: String,
    pub step: usize,
    pub what: String,
    pub count: usize,
    pub repetitions: Vec<u32>,
    pub message: String,
}

impl ScriptSummary {
    fn add_failure(
        &mut self,
        repetition: u32,
        run: &str,
        who: &str,
        step: usize,
        what: &str,
        message: &str,
    ) {
        let group = self.failure_groups.iter_mut().find(|group| {
            group.run == run && group.who == who && group.step == step && group.what == what
        });
        if let Some(group) = group {
            if !group.repetitions.contains(&repetition) {
                group.repetitions.push(repetition);
                group.repetitions.sort_unstable();
                group.count += 1;
            }
        } else {
            self.failure_groups.push(FailureGroup {
                run: run.to_string(),
                who: who.to_string(),
                step,
                what: what.to_string(),
                count: 1,
                repetitions: vec![repetition],
                message: message.lines().next().unwrap_or_default().to_string(),
            });
        }
    }
}

impl Results {
    pub fn update_summaries(&mut self) {
        let mut summaries = std::collections::BTreeMap::<PathBuf, ScriptSummary>::new();
        for result in &self.scripts {
            let Some(repetition) = result.repetition else {
                continue;
            };
            let summary = summaries
                .entry(result.file.clone())
                .or_insert_with(|| ScriptSummary {
                    name: result.name.clone(),
                    file: result.file.clone(),
                    passed: 0,
                    total: 0,
                    failure_groups: Vec::new(),
                });
            summary.total += 1;
            summary.passed += usize::from(result.outcome == Outcome::Passed);
            for failure in &result.failures {
                summary.add_failure(
                    repetition,
                    &failure.run,
                    &failure.who,
                    failure.step,
                    &failure.what,
                    &failure.message,
                );
            }
            // A problem about a failed check starts with that check's first
            // line and is already grouped above. The rest come from the ledger
            // alone (for example a required failure that passed) and must still
            // explain a failed repetition, whether or not another check failed.
            let failed_checks: std::collections::HashSet<String> =
                result.failures.iter().map(FailureResult::header).collect();
            for problem in &result.problems {
                let first_line = problem.lines().next().unwrap_or_default();
                if failed_checks.contains(first_line) {
                    continue;
                }
                summary.add_failure(
                    repetition,
                    &result.name,
                    "ledger",
                    0,
                    first_line,
                    problem.lines().nth(1).unwrap_or_default(),
                );
            }
        }
        self.summaries = summaries.into_values().collect();
        for summary in &mut self.summaries {
            summary.failure_groups.sort_by(|a, b| {
                (&a.run, &a.who, a.step, &a.what).cmp(&(&b.run, &b.who, b.step, &b.what))
            });
        }
    }
}

impl std::fmt::Display for ScriptSummary {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Summaries are created only after a repetition finishes.
        #[expect(
            clippy::cast_precision_loss,
            reason = "counts are used only for a displayed percentage"
        )]
        let rate = 100.0 * (self.total - self.passed) as f64 / self.total as f64;
        write!(
            f,
            "{}: {}/{} passed ({rate:.1}% failed)",
            self.name, self.passed, self.total
        )?;
        for group in &self.failure_groups {
            let indices = group
                .repetitions
                .iter()
                .map(|index| format!("#{index}"))
                .collect::<Vec<_>>()
                .join(", ");
            write!(
                f,
                "\n  {}x {} {} step {} {}  ({indices})\n     {}",
                group.count, group.run, group.who, group.step, group.what, group.message
            )?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn groups_by_combination_instance_step_and_text_and_sorts_repetitions() {
        let mut summary = ScriptSummary {
            name: "dynamodb-writes".into(),
            file: "script.yaml".into(),
            passed: 48,
            total: 50,
            failure_groups: Vec::new(),
        };
        for repetition in [31, 7, 7] {
            summary.add_failure(
                repetition,
                "dynamodb-writes",
                "as-written",
                16,
                "query: SELECT id, name FROM missing_streams",
                "still failing after 179 attempts over 90s: ...\nmore",
            );
        }
        assert_eq!(
            summary.to_string(),
            "dynamodb-writes: 48/50 passed (4.0% failed)\n  2x dynamodb-writes as-written step 16 query: SELECT id, name FROM missing_streams  (#7, #31)\n     still failing after 179 attempts over 90s: ..."
        );
        summary.add_failure(1, "other-combination", "as-written", 16, "query", "other");
        summary.add_failure(1, "dynamodb-writes", "other-instance", 16, "query", "other");
        summary.add_failure(1, "dynamodb-writes", "as-written", 17, "query", "other");
        summary.add_failure(
            1,
            "dynamodb-writes",
            "as-written",
            16,
            "other text",
            "other",
        );
        assert_eq!(summary.failure_groups.len(), 5);
    }

    /// A repetition whose failed check is expected but whose other expected
    /// failure passed fails on the ledger alone: its summary lists both, and
    /// the failed check once.
    #[test]
    fn summaries_list_ledger_problems_beside_failed_checks() {
        let script =
            Script::parse("name: s\nspicepod: {}\nsteps: [{start: {}}]").expect("script parses");
        let failed = crate::runner::Failure {
            run: "s".into(),
            who: "default".into(),
            step: 2,
            key: "query".into(),
            what: "query: SELECT 1".into(),
            query: None,
            labels: Vec::new(),
            message: "wrong answer".into(),
        };
        let gone = "s default step 4 query: SELECT 2\nThis expected failure (g) no longer fails: if it is fixed, move it out of the ledger with --record-failures (and its script out of known-failing/).".to_string();
        let report = Report {
            passed: 1,
            failures: vec![failed],
            ran: std::time::Duration::ZERO,
        };
        let problems = vec![report.failures[0].to_string(), gone];
        let mut result = ScriptResult::new(
            &script,
            Path::new("s.yaml"),
            &report,
            std::time::Duration::ZERO,
            None,
            &problems,
        );
        result.repetition = Some(1);
        let mut results = Results {
            not_run: Vec::new(),
            partition: None,
            complete: false,
            seconds: 0.0,
            scripts: vec![result],
            summaries: Vec::new(),
        };
        results.update_summaries();
        assert_eq!(
            results.summaries[0].to_string(),
            "s: 0/1 passed (100.0% failed)\n  1x s default step 2 query: SELECT 1  (#1)\n     wrong answer\n  1x s ledger step 0 s default step 4 query: SELECT 2  (#1)\n     This expected failure (g) no longer fails: if it is fixed, move it out of the ledger with --record-failures (and its script out of known-failing/)."
        );
    }

    #[test]
    fn summaries_preserve_ledger_outcomes_and_single_run_json() {
        let script =
            Script::parse("name: s\nspicepod: {}\nsteps: [{start: {}}]").expect("script parses");
        let report = Report::default();
        let mut results = Results {
            not_run: Vec::new(),
            partition: None,
            complete: false,
            seconds: 0.0,
            scripts: vec![ScriptResult::new(
                &script,
                Path::new("s.yaml"),
                &report,
                std::time::Duration::ZERO,
                None,
                &[],
            )],
            summaries: Vec::new(),
        };
        let json = serde_json::to_value(&results).expect("serializes");
        assert!(json.get("summaries").is_none());
        assert!(json["scripts"][0].get("repetition").is_none());
        results.scripts[0].repetition = Some(1);
        let mut failed = ScriptResult::new(
            &script,
            Path::new("s.yaml"),
            &report,
            std::time::Duration::ZERO,
            None,
            &["missing check\nexpected failure no longer fails".into()],
        );
        failed.repetition = Some(2);
        results.scripts.push(failed);
        results.update_summaries();
        assert_eq!(
            results.summaries[0].to_string(),
            "s: 1/2 passed (50.0% failed)\n  1x s ledger step 0 missing check  (#2)\n     expected failure no longer fails"
        );
        assert_eq!(results.summaries[0].total, 2);
    }

    #[test]
    fn formats_rates() {
        let mut summary = ScriptSummary {
            name: "s".into(),
            file: "s.yaml".into(),
            passed: 3,
            total: 3,
            failure_groups: Vec::new(),
        };
        assert_eq!(summary.to_string(), "s: 3/3 passed (0.0% failed)");
        summary.passed = 2;
        assert_eq!(summary.to_string(), "s: 2/3 passed (33.3% failed)");
        summary.passed = 0;
        assert_eq!(summary.to_string(), "s: 0/3 passed (100.0% failed)");
    }
}
