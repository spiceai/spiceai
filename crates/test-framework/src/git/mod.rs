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

use std::process::Command;

// This should also append "-dirty" if there are uncommitted changes
#[must_use]
pub fn get_commit_sha() -> String {
    let short_sha = Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output()
        .map_or_else(
            |_| "unknown".to_string(),
            |output| String::from_utf8_lossy(&output.stdout).trim().to_string(),
        );
    format!(
        "{}{}",
        short_sha,
        if is_repo_dirty() { "-dirty" } else { "" }
    )
}

#[must_use]
pub fn get_branch_name() -> String {
    Command::new("git")
        .args(["rev-parse", "--abbrev-ref", "HEAD"])
        .output()
        .map_or_else(
            |_| "unknown".to_string(),
            |output| String::from_utf8_lossy(&output.stdout).trim().to_string(),
        )
}

#[expect(clippy::map_unwrap_or)]
fn is_repo_dirty() -> bool {
    let output = Command::new("git")
        .arg("status")
        .arg("--porcelain")
        .output()
        .map(|output| {
            std::str::from_utf8(&output.stdout)
                .map(ToString::to_string)
                .unwrap_or_else(|_| String::new())
        })
        .unwrap_or_else(|_| String::new());

    !output.trim().is_empty()
}
