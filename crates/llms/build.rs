use std::env;

fn main() {
    if cfg!(feature = "cuda") {
        set_nvcc_flag();
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
