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

//! Run a test under an installed CPU budget.
//!
//! The budget is a process-wide `OnceLock`, so a test that installs one would
//! race every other test in the same binary that reads it. [`isolated_budget`]
//! re-runs the calling test alone in a child process and installs the budget
//! there.

use std::process::Command;

use crate::{CpuBudget, CpuConfig, HostReadings};

const CHILD_ENV: &str = "SPICE_CPU_BUDGET_TEST_CHILD";

/// Where the calling test is running.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Isolation {
    /// The parent: the child already ran the test and passed, so return.
    Parent,
    /// The child: a budget of `cores` is installed, so run the assertions.
    Child { cores: usize },
}

/// Re-run the test at `test_path` (its full libtest path, e.g.
/// `module::tests::name`) in a child process with a CPU budget installed.
///
/// The child's core count differs from the host's, so an assertion that a
/// value follows the budget cannot pass by matching the host default.
///
/// # Errors
///
/// In the parent, when the child cannot run, fails, or does not run exactly the
/// one test (a renamed test would otherwise pass without asserting anything).
/// In the child, when the budget cannot be installed.
pub fn isolated_budget(test_path: &str) -> Result<Isolation, String> {
    if std::env::var_os(CHILD_ENV).is_none() {
        let exe = std::env::current_exe().map_err(|e| format!("no test executable: {e}"))?;
        let output = Command::new(exe)
            .args([test_path, "--exact", "--nocapture"])
            .env(CHILD_ENV, "1")
            .output()
            .map_err(|e| format!("could not run the isolated test: {e}"))?;
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        if !output.status.success() || !stdout.contains("1 passed") {
            return Err(format!(
                "isolated run of '{test_path}' did not pass exactly one test\n{stdout}\n{stderr}"
            ));
        }
        return Ok(Isolation::Parent);
    }

    let host = HostReadings::detect();
    let cores = if host.affinity_cores == 2 { 3 } else { 2 };
    CpuBudget::resolve(
        &CpuConfig::from_sources(None, None, Some(&cores.to_string())),
        &host,
    )
    .and_then(CpuBudget::install)
    .map_err(|e| format!("could not install a {cores}-core CPU budget: {e}"))?;
    Ok(Isolation::Child { cores })
}
