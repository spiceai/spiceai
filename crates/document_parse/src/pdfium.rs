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

//! Runtime provisioning of the `PDFium` shared library used by `liteparse`.
//!
//! On desktop targets `liteparse` does not statically link `PDFium`; it loads
//! the shared library at runtime via `dlopen` (`liteparse-pdfium-sys`'s
//! `dynamic` loader). That means the library has to be *found* at runtime. Two
//! deployment shapes are supported:
//!
//! - **Docker images** ship `libpdfium` next to the `spiced` binary, so the
//!   loader's "next to the executable" search path resolves it with no network
//!   access. This is the robust path for locked-down / air-gapped containers.
//! - **Standalone binaries** (the release archives stay lean and do not bundle
//!   `PDFium`) lazily download the matching `PDFium` build on first PDF parse
//!   and load it explicitly. Nothing has to be installed ahead of time.
//!
//! [`ensure_loaded`] wires both together: it first tries the discoverable
//! locations, and only downloads when `PDFium` is genuinely absent. When
//! `PDFium` cannot be loaded *or* downloaded (for example, an offline standalone
//! host) it returns a structured [`crate::Error`] instead of letting `liteparse`
//! panic.

use std::path::{Path, PathBuf};
use std::sync::LazyLock;
use std::sync::atomic::{AtomicBool, Ordering};

use tokio::sync::Mutex;

use crate::Error;

/// The `PDFium` release the bundled `liteparse-pdfium-sys` bindings were
/// generated against.
///
/// This MUST stay in lockstep with `PDFIUM_RELEASE_TAG` in
/// `liteparse-pdfium-sys`'s `build.rs` (currently `chromium/7897`). Bump it
/// whenever `liteparse` is upgraded — otherwise a standalone auto-download could
/// fetch a `PDFium` build whose ABI does not match the compiled-in bindings.
const PDFIUM_RELEASE_TAG: &str = "chromium/7897";

/// Base URL of the `PDFium` binaries the bindings target (a `run-llama` fork of
/// the well-known `bblanchon/pdfium-binaries`).
const PDFIUM_RELEASE_URL: &str = "https://github.com/run-llama/pdfium-binaries/releases/download";

/// Set once `PDFium` has been loaded so the common case is a single atomic read.
static PDFIUM_LOADED: AtomicBool = AtomicBool::new(false);

/// Serializes the (at most once) download so concurrent first parses do not all
/// fetch the archive.
static PROVISION_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

/// Ensure `PDFium` is loaded before the first parse.
///
/// Idempotent and safe to call concurrently. Returns
/// [`Error::PdfiumUnavailable`] if `PDFium` can neither be found in a search
/// path nor downloaded.
pub(crate) async fn ensure_loaded() -> Result<(), Error> {
    if PDFIUM_LOADED.load(Ordering::Acquire) {
        return Ok(());
    }

    // Fast path: PDFium is discoverable — bundled next to the executable in
    // Docker, on a system search path, or in the compile-time cache on a
    // machine that built the binary from source.
    if try_load_default().await {
        PDFIUM_LOADED.store(true, Ordering::Release);
        return Ok(());
    }

    // Standalone fallback: download on first use. Serialize so only one task
    // fetches, and re-check under the lock in case another task just finished.
    let _guard = PROVISION_LOCK.lock().await;
    if PDFIUM_LOADED.load(Ordering::Acquire) || try_load_default().await {
        PDFIUM_LOADED.store(true, Ordering::Release);
        return Ok(());
    }

    let lib_path = match tokio::task::spawn_blocking(provision_pdfium).await {
        Ok(Ok(path)) => path,
        Ok(Err(reason)) => return Err(Error::PdfiumUnavailable { reason }),
        Err(join_err) => {
            return Err(Error::PdfiumUnavailable {
                reason: format!("PDFium download task failed to run: {join_err}"),
            });
        }
    };

    match tokio::task::spawn_blocking(move || liteparse_pdfium_sys::dynamic::load(&lib_path)).await
    {
        Ok(Ok(())) => {
            PDFIUM_LOADED.store(true, Ordering::Release);
            Ok(())
        }
        Ok(Err(reason)) => Err(Error::PdfiumUnavailable { reason }),
        Err(join_err) => Err(Error::PdfiumUnavailable {
            reason: format!("PDFium load task failed to run: {join_err}"),
        }),
    }
}

/// Try to load `PDFium` from the loader's default search paths (env override,
/// compile-time cache, next to the executable, system paths). Runs the blocking
/// `dlopen` off the async worker.
async fn try_load_default() -> bool {
    matches!(
        tokio::task::spawn_blocking(liteparse_pdfium_sys::dynamic::load_default).await,
        Ok(Ok(()))
    )
}

/// Ensure the `PDFium` shared library exists in the cache, downloading and
/// extracting it if necessary, and return the path to load. Blocking.
fn provision_pdfium() -> Result<PathBuf, String> {
    let asset = pdfium_asset_stem()?;
    let dir = pdfium_cache_dir(asset)?;
    let lib_path = dir.join(lib_subdir()).join(dylib_file_name());

    if lib_path.exists() {
        return Ok(lib_path);
    }

    download_and_extract(asset, &dir)?;

    if !lib_path.exists() {
        return Err(format!(
            "downloaded PDFium archive for '{asset}' did not contain {}",
            lib_path.display()
        ));
    }
    Ok(lib_path)
}

/// Download `<asset>.tgz` from the pinned `PDFium` release and extract it into
/// `dest` (atomically, via a temp dir). Blocking network + IO.
fn download_and_extract(asset: &str, dest: &Path) -> Result<(), String> {
    let tag_encoded = PDFIUM_RELEASE_TAG.replace('/', "%2F");
    let url = format!("{PDFIUM_RELEASE_URL}/{tag_encoded}/{asset}.tgz");

    let reader = ureq::get(&url)
        .call()
        .map_err(|e| format!("failed to download PDFium from {url}: {e}"))?
        .into_reader();
    let gz = flate2::read::GzDecoder::new(reader);
    let mut archive = tar::Archive::new(gz);

    // Extract to a sibling temp dir, then rename into place atomically so a
    // partially-extracted directory is never observed as complete.
    let tmp = dest.with_extension("download.tmp");
    if tmp.exists() {
        std::fs::remove_dir_all(&tmp).ok();
    }
    std::fs::create_dir_all(&tmp)
        .map_err(|e| format!("failed to create {}: {e}", tmp.display()))?;
    archive
        .unpack(&tmp)
        .map_err(|e| format!("failed to extract PDFium archive from {url}: {e}"))?;

    if let Some(parent) = dest.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| format!("failed to create {}: {e}", parent.display()))?;
    }
    if dest.exists() {
        std::fs::remove_dir_all(dest).ok();
    }
    std::fs::rename(&tmp, dest)
        .map_err(|e| format!("failed to move PDFium into {}: {e}", dest.display()))?;
    Ok(())
}

/// Cache directory for the extracted `PDFium` build, namespaced by release tag
/// and asset so multiple targets/versions can coexist.
fn pdfium_cache_dir(asset: &str) -> Result<PathBuf, String> {
    let tag_safe = PDFIUM_RELEASE_TAG.replace('/', "_");
    Ok(base_cache_dir()?
        .join("spice")
        .join("pdfium")
        .join(tag_safe)
        .join(asset))
}

/// Platform cache root, mirroring `liteparse-pdfium-sys`'s own cache location so
/// the two never fight over the same tree.
fn base_cache_dir() -> Result<PathBuf, String> {
    if let Ok(xdg) = std::env::var("XDG_CACHE_HOME")
        && !xdg.is_empty()
    {
        return Ok(PathBuf::from(xdg));
    }

    if cfg!(target_os = "windows") {
        if let Ok(local_app_data) = std::env::var("LOCALAPPDATA")
            && !local_app_data.is_empty()
        {
            return Ok(PathBuf::from(local_app_data));
        }
        let profile = std::env::var("USERPROFILE")
            .map_err(|_| "neither LOCALAPPDATA nor USERPROFILE is set".to_string())?;
        return Ok(PathBuf::from(profile).join("AppData").join("Local"));
    }

    let home =
        std::env::var("HOME").map_err(|_| "HOME environment variable is not set".to_string())?;
    if cfg!(target_os = "macos") {
        Ok(PathBuf::from(home).join("Library").join("Caches"))
    } else {
        Ok(PathBuf::from(home).join(".cache"))
    }
}

/// Subdirectory inside the extracted archive that holds the shared library.
/// `pdfium-binaries` ships the Windows DLL under `bin/` and the Unix library
/// under `lib/`.
fn lib_subdir() -> &'static str {
    if cfg!(target_os = "windows") {
        "bin"
    } else {
        "lib"
    }
}

/// The `PDFium` shared library file name for this target.
fn dylib_file_name() -> &'static str {
    if cfg!(target_os = "windows") {
        "pdfium.dll"
    } else if cfg!(target_os = "macos") {
        "libpdfium.dylib"
    } else {
        "libpdfium.so"
    }
}

/// Map the current target to the `pdfium-binaries` asset stem (without `.tgz`).
///
/// Mirrors the target → asset mapping in `liteparse-pdfium-sys`'s `build.rs` for
/// every platform Spice builds and releases on.
fn pdfium_asset_stem() -> Result<&'static str, String> {
    let stem = if cfg!(target_os = "macos") {
        if cfg!(target_arch = "aarch64") {
            "pdfium-mac-arm64"
        } else if cfg!(target_arch = "x86_64") {
            "pdfium-mac-x64"
        } else {
            return Err(unsupported_platform());
        }
    } else if cfg!(target_os = "linux") {
        if cfg!(target_arch = "x86_64") {
            if cfg!(target_env = "musl") {
                "pdfium-linux-musl-x64"
            } else {
                "pdfium-linux-x64"
            }
        } else if cfg!(target_arch = "aarch64") {
            "pdfium-linux-arm64"
        } else if cfg!(target_arch = "arm") {
            "pdfium-linux-arm"
        } else {
            return Err(unsupported_platform());
        }
    } else if cfg!(target_os = "windows") {
        if cfg!(target_arch = "x86_64") {
            "pdfium-win-x64"
        } else if cfg!(target_arch = "aarch64") {
            "pdfium-win-arm64"
        } else if cfg!(target_arch = "x86") {
            "pdfium-win-x86"
        } else {
            return Err(unsupported_platform());
        }
    } else {
        return Err(unsupported_platform());
    };
    Ok(stem)
}

fn unsupported_platform() -> String {
    format!(
        "automatic PDFium download is not supported on this platform ({}/{})",
        std::env::consts::OS,
        std::env::consts::ARCH
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn asset_stem_resolves_for_current_platform() {
        // Every target Spice compiles on must map to a downloadable asset.
        let stem = pdfium_asset_stem().expect("current platform must map to a PDFium asset");
        assert!(stem.starts_with("pdfium-"), "unexpected asset stem: {stem}");
    }

    #[test]
    fn dylib_name_matches_platform() {
        let name = dylib_file_name();
        if cfg!(target_os = "windows") {
            assert_eq!(name, "pdfium.dll");
        } else if cfg!(target_os = "macos") {
            assert_eq!(name, "libpdfium.dylib");
        } else {
            assert_eq!(name, "libpdfium.so");
        }
    }

    #[test]
    fn cache_dir_is_namespaced_by_tag_and_asset() {
        // Only exercised where a cache root is resolvable; never touches disk.
        let Ok(dir) = pdfium_cache_dir("pdfium-linux-x64") else {
            return;
        };
        assert!(dir.ends_with("pdfium-linux-x64"), "{}", dir.display());
        assert!(
            dir.to_string_lossy().contains("pdfium"),
            "{}",
            dir.display()
        );
    }
}
