use std::env;

fn main() {
    // Force static linking for llama.cpp dependencies
    // This ensures the llama.cpp C++ libraries are statically linked
    // into the Rust binary, avoiding runtime dependency issues
    if cfg!(feature = "llama_cpp") {
        set_llama_cpp_static_linking();
    }

    if cfg!(feature = "cuda") {
        set_nvcc_flag();
    }
}

/// Configure llama.cpp to use static linking instead of shared libraries.
/// This avoids runtime dependency issues with the llama.cpp C++ libraries.
///
/// The `llama-cpp-sys-2` crate reads the `LLAMA_BUILD_SHARED_LIBS` environment variable
/// during its build process to determine whether to build shared or static libraries.
/// Setting this to "0" forces static linking.
fn set_llama_cpp_static_linking() {
    // Only set if not already configured by the user
    if env::var("LLAMA_BUILD_SHARED_LIBS").is_err() {
        // SAFETY: This is safe in a build.rs context as it's a single-threaded environment
        // and we're setting the variable before any other code reads it.
        // The variable is read by llama-cpp-sys-2's build script to configure CMake.
        unsafe {
            std::env::set_var("LLAMA_BUILD_SHARED_LIBS", "0");
        }
        println!(
            "cargo:warning=Configuring llama.cpp for static linking (LLAMA_BUILD_SHARED_LIBS=0)"
        );
    }
}

/// Set `-fPIE` in the `--compiler-options` when building CUDA bindings.
/// `-fPIE` builds position-independent executable, which is required for building shared libraries.
fn set_nvcc_flag() {
    let updated_flags = if let Ok(nvcc_flags) = env::var("CUDA_NVCC_FLAGS") {
        format!("{nvcc_flags} -fPIE")
    } else {
        "-fPIE".to_string()
    };

    println!("cargo:rustc-env=CUDA_NVCC_FLAGS={updated_flags}");
}
