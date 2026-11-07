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

use std::process::Command;

fn main() {
    rerun_when_head_moves();
    // A fixed 10 characters: `--short` alone scales with the repository's object
    // count, so a shallow CI checkout would print a shorter hash than a full clone
    // of the same commit.
    let hash = git(&["rev-parse", "--short=10", "HEAD"]).unwrap_or_else(|| "unknown".to_string());
    println!("cargo:rustc-env=GIT_COMMIT_HASH={hash}");
}

/// Output of `git` with these arguments, or `None` outside a git checkout.
fn git(args: &[&str]) -> Option<String> {
    Command::new("git")
        .args(args)
        .output()
        .ok()
        .filter(|output| output.status.success())
        .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_string())
        .filter(|out| !out.is_empty())
}

/// Reruns this script when the checked-out commit changes, so an incremental build
/// does not keep the previous commit's hash: `HEAD`, the branch it points at, and
/// `packed-refs`, where that branch may live instead. Only files that exist are
/// named, because cargo reruns on every build for a missing one. Outside a git
/// checkout none are, and cargo keeps its default of rerunning when a file of this
/// package changes.
fn rerun_when_head_moves() {
    let branch = git(&["symbolic-ref", "-q", "HEAD"]);
    let names = ["HEAD", "packed-refs"]
        .into_iter()
        .map(str::to_string)
        .chain(branch);
    for name in names {
        if let Some(path) = git(&["rev-parse", "--git-path", &name])
            && std::path::Path::new(&path).exists()
        {
            println!("cargo:rerun-if-changed={path}");
        }
    }
}
