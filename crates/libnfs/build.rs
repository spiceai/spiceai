/*
Copyright 2026 The Spice.ai OSS Authors

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

// Build scripts run at compile time and panicking is the standard way to report errors.
#![allow(clippy::expect_used)]

use std::env;
use std::path::{Path, PathBuf};
use std::process::Command;

fn main() {
    // Link to libnfs system library
    let link_static_env = env::var("LIBNFS_LINK_STATIC");
    match link_static_env {
        Ok(link_static) if link_static == "true" => {
            println!("cargo:rustc-link-lib=static=nfs");
        }
        _ => {
            println!("cargo:rustc-link-lib=nfs");
        }
    }

    // Allow custom library path
    if let Ok(lib_dir) = env::var("LIBNFS_LIB_PATH") {
        let lib_dir = Path::new(&lib_dir);
        println!("cargo:rustc-link-search=native={}", lib_dir.display());
    }

    // Generate bindings using bindgen
    let mut builder = bindgen::Builder::default()
        .header("wrapper.h")
        .parse_callbacks(Box::new(bindgen::CargoCallbacks::new()))
        .allowlist_function("nfs_.*")
        .allowlist_type("nfs_.*")
        .allowlist_type("nfsdir")
        .allowlist_type("nfsfh")
        .allowlist_type("ftype3")
        .allowlist_type("ftype3_.*")
        .allowlist_type("nfsdirent")
        .allowlist_type("statvfs")
        .allowlist_type("timeval")
        .allowlist_type("AUTH")
        .allowlist_var("ftype3_.*");

    // Allow custom include path
    if let Ok(include_path) = env::var("LIBNFS_INCLUDE_PATH") {
        let include_path = Path::new(&include_path);
        builder = builder.clang_arg(format!("-I{}", include_path.display()));
    } else {
        // On macOS with Homebrew, find the include path automatically
        #[cfg(target_os = "macos")]
        {
            if let Ok(output) = Command::new("brew").args(["--prefix", "libnfs"]).output()
                && output.status.success()
            {
                let prefix = String::from_utf8_lossy(&output.stdout);
                let prefix = prefix.trim();
                let include_path = format!("{prefix}/include");
                let lib_path = format!("{prefix}/lib");
                builder = builder.clang_arg(format!("-I{include_path}"));
                println!("cargo:rustc-link-search=native={lib_path}");
            }
        }
    }

    let bindings = match builder.generate() {
        Ok(bindings) => bindings,
        Err(e) => {
            // If we can't generate bindings, create a stub file that will cause a compile error
            // when the crate is actually used. This allows the crate to be a workspace member
            // without requiring libnfs-dev to be installed when the nfs feature isn't used.
            eprintln!("Warning: Could not generate libnfs bindings: {e}");
            eprintln!("The NFS feature will not be available. Install libnfs-dev to enable it.");

            let out_path = PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR not set"));
            std::fs::write(
                out_path.join("bindings.rs"),
                r#"
// libnfs bindings could not be generated because libnfs-dev is not installed.
// Install libnfs-dev (or libnfs on macOS via brew) to enable NFS support.
compile_error!("libnfs bindings not available: install libnfs-dev to enable NFS support");
"#,
            )
            .expect("Failed to write stub bindings file");
            return;
        }
    };

    let out_path = PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR not set"));
    bindings
        .write_to_file(out_path.join("bindings.rs"))
        .expect("Failed to write libnfs bindings");
}
