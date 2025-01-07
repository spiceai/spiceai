use std::env;

fn main() {
    if cfg!(feature = "cuda") {
        set_nvcc_flag();
    }
}

/// Set `-fPIE` in the `--compiler-options` when building CUDA bindings.
/// `-fPIE` builds position-independent executable, which is required for building shared libraries.
fn set_nvcc_flag() {
    let nvcc_flags = env::var("CUDA_NVCC_FLAGS").unwrap_or_default();
    let updated_flags = if nvcc_flags.is_empty() {
        "-fPIE".to_string()
    } else {
        format!("{nvcc_flags} -fPIE")
    };

    println!("cargo:rustc-env=CUDA_NVCC_FLAGS={}", updated_flags);
}
