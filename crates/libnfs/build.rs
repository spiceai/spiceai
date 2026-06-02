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

fn main() {
    // Declare the libnfs_new_api cfg for conditional compilation
    println!("cargo::rustc-check-cfg=cfg(libnfs_new_api)");

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
            use std::process::Command;
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

    let bindings = builder.generate().unwrap_or_else(|e| {
        panic!(
            "Could not generate libnfs bindings: {e}. Install libnfs-dev (or libnfs via brew on macOS) to enable NFS support."
        );
    });

    let out_path = PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR not set"));
    let bindings_path = out_path.join("bindings.rs");
    bindings
        .write_to_file(&bindings_path)
        .expect("Failed to write libnfs bindings");

    // Detect libnfs API version by inspecting the generated bindings.
    // The new API (libnfs >= 4.0) has nfs_pread(nfs, fh, offset, count, buf)
    // The old API (libnfs < 4.0) has nfs_pread(nfs, fh, buf, count, offset)
    // We detect this by looking for "offset: u64" appearing before "buf:" in the signature.
    let bindings_content =
        std::fs::read_to_string(&bindings_path).expect("Failed to read generated bindings");

    // Find the nfs_pread function signature and check if offset comes before buf
    // New API pattern: "nfs_pread" followed by "offset:" before "buf:"
    // Old API pattern: "nfs_pread" followed by "buf:" before "offset:"
    if let Some(pread_pos) = bindings_content.find("pub fn nfs_pread(") {
        let signature_section =
            &bindings_content[pread_pos..pread_pos + 500.min(bindings_content.len() - pread_pos)];
        let offset_pos = signature_section.find("offset:");
        let buf_pos = signature_section.find("buf:");

        match (offset_pos, buf_pos) {
            (Some(o), Some(b)) if o < b => {
                // New API: offset comes before buf
                println!("cargo:rustc-cfg=libnfs_new_api");
            }
            _ => {
                // Old API: buf comes before offset (or couldn't determine)
            }
        }
    }
}
