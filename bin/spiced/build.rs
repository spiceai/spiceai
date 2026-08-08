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

fn main() {
    let git_hash: String = Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output()
        .map_or_else(
            |_| "unknown".to_string(),
            |output| String::from_utf8_lossy(&output.stdout).trim().to_string(),
        );

    println!("cargo:rustc-env=GIT_COMMIT_HASH={git_hash}");
    println!("cargo:rustc-env=SPICED_BUILD_PROFILE={}", build_profile());
    println!("cargo:rustc-env=SPICED_BUILD_FLAVOR={}", build_flavor());
}

/// The cargo profile the binary is being built with.
///
/// Cargo's own `PROFILE` collapses every profile that inherits `release` down to
/// `release`, so it cannot tell `release` from `release-lto` — which are the two
/// profiles that ship, and which differ in whether a crash report can be symbolized.
/// The profile directory can: `OUT_DIR` is `<target>/<profile>/build/<pkg>-<hash>/out`.
fn build_profile() -> String {
    std::env::var("OUT_DIR")
        .ok()
        .and_then(|dir| {
            let path = std::path::Path::new(&dir);
            Some(
                path.parent()?
                    .parent()?
                    .parent()?
                    .file_name()?
                    .to_str()?
                    .to_owned(),
            )
        })
        .unwrap_or_else(|| "unknown".to_owned())
}

/// Which release artifact this binary is, e.g. `default`, `models`, `nas`.
///
/// The feature set alone does not name it — several flavors differ only in features
/// that nothing in the binary reports — so the release workflows pass the artifact
/// suffix they are already packaging under.
fn build_flavor() -> String {
    println!("cargo:rerun-if-env-changed=SPICED_BUILD_FLAVOR");
    std::env::var("SPICED_BUILD_FLAVOR").unwrap_or_else(|_| "local".to_owned())
}
