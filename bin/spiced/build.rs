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
    // This script has no build dependencies, so it runs at the start of the build: a
    // native Windows build stops here with the supported path rather than on the first
    // dependency with no Windows code (the `compile_error!` in `lib.rs` is only reached
    // once every dependency has compiled).
    if std::env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("windows") {
        println!(
            "cargo::error=spiced does not build for native Windows. Run it under WSL or in Docker. See: https://spiceai.org/docs/installation"
        );
        return;
    }

    let git_hash: String = Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output()
        .map_or_else(
            |_| "unknown".to_string(),
            |output| String::from_utf8_lossy(&output.stdout).trim().to_string(),
        );

    println!("cargo:rustc-env=GIT_COMMIT_HASH={git_hash}");
    println!("cargo:rustc-env=SPICED_BUILD_PROFILE={}", build_profile());
    println!("cargo:rustc-env=SPICED_BUILD_FEATURES={}", build_features());

    // macOS exports every global symbol of an executable, and dyld binds a weak
    // definition (a C++ template instantiation, an inline function, a vtable)
    // to the first image that exports it. `duckdb:` and `ducklake:` load the
    // installed libduckdb through ADBC, so the bundled DuckDB's and ICU's
    // exports would replace parts of that library with code from another
    // DuckDB release and corrupt its state. Export nothing, as the Linux build
    // does: the bundled DuckDB links its extensions statically and needs no
    // exports of its own.
    if std::env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("macos") {
        println!("cargo:rustc-link-arg-bins=-Wl,-exported_symbol,__mh_execute_header");
    }
}

/// The cargo profile the binary is being built with.
///
/// Cargo's `PROFILE` reports `release` for every profile inheriting it, so it cannot
/// separate `release` from `release-lto`. `OUT_DIR` can: it is
/// `<target>/<profile>/build/<pkg>-<hash>/out`.
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

/// The optional features that tell one shipped artifact from another.
///
/// The version string encodes `models`, `metal` and `cuda`, but not `odbc`, `nfs` or
/// `smb`, which leaves enterprise `models` against `nas` and OSS `default` against
/// `odbc` identical at the same version. They compile to different code, so
/// symbolizing against the wrong one resolves to the wrong place.
///
/// Read from cargo rather than passed in by the workflows: `spiced` is built from
/// several places in each repository, and a label they must each remember to set
/// eventually goes wrong rather than merely missing.
fn build_features() -> String {
    let distinguishing = [
        "models",
        "metal",
        "cuda",
        "odbc",
        "nfs",
        "smb",
        "alloc-jemalloc",
        "alloc-jemalloc-profiling",
        "alloc-mimalloc",
        "alloc-system",
    ];
    let enabled: Vec<&str> = distinguishing
        .into_iter()
        .filter(|feature| {
            // Cargo uppercases the feature name and maps `-` to `_`.
            let var = format!("CARGO_FEATURE_{}", feature.to_uppercase().replace('-', "_"));
            std::env::var_os(var).is_some()
        })
        .collect();

    if enabled.is_empty() {
        return "none".to_owned();
    }
    enabled.join(",")
}
