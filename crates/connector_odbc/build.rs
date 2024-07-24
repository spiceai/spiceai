fn main() {
    if cfg!(target_os = "macos") && cfg!(target_arch = "aarch64") {
        // Only execute this on macOS ARM (Apple Silicon)
        println!("cargo:rustc-link-search=native=/opt/homebrew/lib");
    }
}
