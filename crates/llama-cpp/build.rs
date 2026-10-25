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

//! Builds llama.cpp from the `llama-cpp-src` package with `CMake`, compiles the MTP shim
//! against its common library, and generates the bindings.

// A build script reports a failure by panicking.
#![expect(clippy::expect_used, clippy::panic)]

use std::collections::BTreeMap;
use std::env;
use std::path::{Path, PathBuf};

/// The static libraries the build produces, in link order: each one before the libraries it
/// calls into, as a single-pass linker needs.
const LIBRARIES: &[&str] = &[
    "llama-common",
    "llama-common-base",
    "cpp-httplib",
    "llama",
    "ggml",
    "ggml-cuda",
    "ggml-metal",
    "ggml-cpu",
    "ggml-base",
];

fn main() {
    let root = PathBuf::from(
        env::var("DEP_LLAMA_CPP_SRC_ROOT")
            .expect("the llama-cpp-src package reports its directory"),
    );
    let release = env::var("DEP_LLAMA_CPP_SRC_RELEASE").unwrap_or_default();
    let cuda = env::var_os("CARGO_FEATURE_CUDA").is_some();
    let metal = env::var_os("CARGO_FEATURE_METAL").is_some();
    let target_os = env::var("CARGO_CFG_TARGET_OS").expect("Cargo sets CARGO_CFG_TARGET_OS");
    println!("cargo:rerun-if-changed=shim");
    println!("cargo:rerun-if-env-changed=CMAKE_CUDA_ARCHITECTURES");
    println!("cargo:rerun-if-env-changed=CUDA_COMPUTE_CAP");
    println!("cargo:rustc-env=LLAMA_CPP_RELEASE={release}");

    let build = cmake_build(&root, cuda, metal);
    compile_shim(&root);
    link_libraries(&build, cuda, metal);
    link_system(&target_os, cuda, metal);
    bindings(&root);
}

/// Configures and builds llama.cpp's common library and everything it links.
fn cmake_build(root: &Path, cuda: bool, metal: bool) -> PathBuf {
    let on_off = |on: bool| if on { "ON" } else { "OFF" };
    let mut config = cmake::Config::new(root);
    // A debug llama.cpp decodes many times slower, so it is optimized in every profile.
    config
        .profile("Release")
        .build_target("llama-common")
        .define("BUILD_SHARED_LIBS", "OFF")
        .define("LLAMA_BUILD_COMMON", "ON")
        .define("LLAMA_BUILD_TESTS", "OFF")
        .define("LLAMA_BUILD_TOOLS", "OFF")
        .define("LLAMA_BUILD_EXAMPLES", "OFF")
        .define("LLAMA_BUILD_SERVER", "OFF")
        .define("LLAMA_BUILD_APP", "OFF")
        .define("LLAMA_OPENSSL", "OFF")
        .define("LLAMA_SUBPROCESS", "OFF")
        .define("LLAMA_ALL_WARNINGS", "OFF")
        // A binary built for the build machine's CPU features may not run on another.
        .define("GGML_NATIVE", "OFF")
        .define("GGML_OPENMP", "OFF")
        .define("GGML_CCACHE", "OFF")
        .define("GGML_BLAS", "OFF")
        .define("GGML_CUDA", on_off(cuda))
        // NCCL only joins several GPUs in one machine, and ggml links it whenever the build
        // machine has it, which would make the binary need it too.
        .define("GGML_CUDA_NCCL", "OFF")
        .define("GGML_METAL", on_off(metal))
        .define("GGML_METAL_EMBED_LIBRARY", on_off(metal));
    if cuda {
        let arch = env::var("CMAKE_CUDA_ARCHITECTURES")
            .ok()
            .or_else(|| env::var("CUDA_COMPUTE_CAP").ok());
        if let Some(arch) = arch {
            config.define("CMAKE_CUDA_ARCHITECTURES", arch);
        }
    }
    config.build().join("build")
}

/// Links the static libraries the build produced, wherever `CMake` put them.
fn link_libraries(build: &Path, cuda: bool, metal: bool) {
    let mut found = BTreeMap::new();
    index_libraries(build, &mut found);
    for &name in LIBRARIES {
        let wanted = match name {
            "ggml-cuda" => cuda,
            "ggml-metal" => metal,
            _ => true,
        };
        if !wanted {
            continue;
        }
        let dir = found.get(name).unwrap_or_else(|| {
            panic!(
                "the llama.cpp build in {} produced no lib{name}.a; found {:?}",
                build.display(),
                found.keys().collect::<Vec<_>>()
            )
        });
        println!("cargo:rustc-link-search=native={}", dir.display());
        println!("cargo:rustc-link-lib=static={name}");
    }
}

fn index_libraries(dir: &Path, found: &mut BTreeMap<String, PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            index_libraries(&path, found);
        } else if let Some(name) = path
            .file_name()
            .and_then(|n| n.to_str())
            .and_then(|n| n.strip_prefix("lib"))
            .and_then(|n| n.strip_suffix(".a"))
        {
            found.insert(name.to_string(), dir.to_path_buf());
        }
    }
}

/// Compiles the C interfaces to llama.cpp's C++ speculative decoding and JSON-schema
/// conversion. They go on the link line before the libraries they call.
fn compile_shim(root: &Path) {
    cc::Build::new()
        .cpp(true)
        .std("c++17")
        .file("shim/spice_mtp.cpp")
        .file("shim/spice_grammar.cpp")
        .include("shim")
        .include(root.join("include"))
        .include(root.join("common"))
        .include(root.join("ggml/include"))
        .include(root.join("vendor"))
        .cpp_link_stdlib(None)
        .warnings(false)
        .compile("spice_llama_shim");
}

/// The system libraries llama.cpp's static libraries need, after them on the link line.
fn link_system(target_os: &str, cuda: bool, metal: bool) {
    if cuda {
        let cuda_root = env::var("CUDA_PATH")
            .or_else(|_| env::var("CUDA_HOME"))
            .unwrap_or_else(|_| "/usr/local/cuda".to_string());
        for dir in [
            "lib64",
            "lib64/stubs",
            "targets/sbsa-linux/lib",
            "targets/x86_64-linux/lib",
        ] {
            let dir = Path::new(&cuda_root).join(dir);
            if dir.is_dir() {
                println!("cargo:rustc-link-search=native={}", dir.display());
            }
        }
        for lib in ["cudart", "cublas", "cublasLt", "cuda"] {
            println!("cargo:rustc-link-lib=dylib={lib}");
        }
    }
    if metal {
        for framework in ["Foundation", "Metal", "MetalKit"] {
            println!("cargo:rustc-link-lib=framework={framework}");
        }
    }
    match target_os {
        "macos" | "ios" => {
            println!("cargo:rustc-link-lib=dylib=c++");
            // ggml's CPU backend calls Accelerate's vDSP on Apple platforms.
            println!("cargo:rustc-link-lib=framework=Accelerate");
        }
        _ => {
            println!("cargo:rustc-link-lib=dylib=stdc++");
            println!("cargo:rustc-link-lib=dylib=m");
            println!("cargo:rustc-link-lib=dylib=dl");
            println!("cargo:rustc-link-lib=dylib=pthread");
        }
    }
}

fn bindings(root: &Path) {
    let out = PathBuf::from(env::var("OUT_DIR").expect("Cargo sets OUT_DIR"));
    bindgen::Builder::default()
        .header("shim/wrapper.h")
        .clang_arg("-Ishim")
        .clang_arg(format!("-I{}", root.join("include").display()))
        .clang_arg(format!("-I{}", root.join("ggml/include").display()))
        .allowlist_function("llama_.*")
        .allowlist_function("spice_mtp_.*")
        .allowlist_function("spice_json_schema_to_grammar")
        .allowlist_function("spice_string_free")
        .allowlist_type("llama_.*")
        .allowlist_type("spice_mtp.*")
        .allowlist_type("ggml_log_level")
        .allowlist_var("LLAMA_.*")
        // llama.cpp is C++ behind a C API, and an exception it does not catch would unwind
        // into Rust, which only an unwinding ABI allows. Rust then aborts where it would
        // catch it, as llama.cpp's own unhandled exception would.
        .override_abi(bindgen::Abi::CUnwind, "(llama|spice)_.*")
        .layout_tests(false)
        .generate()
        .expect("bindgen generates the llama.cpp bindings")
        .write_to_file(out.join("bindings.rs"))
        .expect("the bindings are written to OUT_DIR");
}
