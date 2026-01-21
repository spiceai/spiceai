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

//! Hostname utilities.
//!
//! Provides functions to get the system hostname using POSIX APIs.

use std::ffi::OsString;
use std::io;

/// Returns the hostname of the current system.
///
/// On Unix systems, this calls the `gethostname` POSIX function.
/// On Windows, this returns an error (not supported).
///
/// # Errors
///
/// Returns an error if:
/// - The underlying OS call fails
/// - The hostname contains invalid UTF-8 (use [`get_raw`] for `OsString`)
///
/// # Examples
///
/// ```
/// use util::hostname::get;
///
/// match get() {
///     Ok(name) => println!("Hostname: {}", name),
///     Err(e) => eprintln!("Failed to get hostname: {}", e),
/// }
/// ```
#[cfg(unix)]
pub fn get() -> io::Result<String> {
    get_raw()?
        .into_string()
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "hostname is not valid UTF-8"))
}

/// Returns the hostname of the current system as an `OsString`.
///
/// This is useful when you need the raw hostname bytes without UTF-8 validation.
///
/// # Errors
///
/// Returns an error if the underlying OS call fails.
///
/// # Examples
///
/// ```
/// use util::hostname::get_raw;
///
/// match get_raw() {
///     Ok(name) => println!("Hostname: {:?}", name),
///     Err(e) => eprintln!("Failed to get hostname: {}", e),
/// }
/// ```
#[cfg(unix)]
pub fn get_raw() -> io::Result<OsString> {
    use std::ffi::CStr;
    use std::os::unix::ffi::OsStringExt;

    // HOST_NAME_MAX is typically 64 on Linux, 255 on macOS
    // Use 256 to be safe across platforms
    let mut buffer = vec![0u8; 256];

    // SAFETY: We're calling gethostname with a valid buffer and its length.
    // The function will write at most `len` bytes including the null terminator.
    let result = unsafe { libc::gethostname(buffer.as_mut_ptr().cast(), buffer.len()) };

    if result != 0 {
        return Err(io::Error::last_os_error());
    }

    // Find the null terminator and create CStr
    let nul_pos = buffer.iter().position(|&b| b == 0).unwrap_or(buffer.len());

    // SAFETY: We just found the null terminator position, so we know the slice
    // contains exactly one null byte at `nul_pos`.
    let hostname = if nul_pos < buffer.len() {
        let cstr = unsafe { CStr::from_bytes_with_nul_unchecked(&buffer[..=nul_pos]) }.to_bytes();
        OsString::from_vec(cstr.to_vec())
    } else {
        // No null terminator found, use the whole buffer (shouldn't happen normally)
        OsString::from_vec(buffer)
    };

    Ok(hostname)
}

/// Returns the hostname of the current system (Windows stub).
///
/// # Errors
///
/// Always returns an error on Windows as it's not implemented.
#[cfg(not(unix))]
pub fn get() -> io::Result<String> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "hostname retrieval not implemented for this platform",
    ))
}

/// Returns the hostname of the current system as an `OsString` (Windows stub).
///
/// # Errors
///
/// Always returns an error on Windows as it's not implemented.
#[cfg(not(unix))]
pub fn get_raw() -> io::Result<OsString> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "hostname retrieval not implemented for this platform",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[cfg(unix)]
    fn test_get_returns_non_empty_hostname() {
        let hostname = get().expect("should get hostname");
        assert!(!hostname.is_empty(), "hostname should not be empty");
    }

    #[test]
    #[cfg(unix)]
    fn test_get_raw_returns_non_empty_hostname() {
        let hostname = get_raw().expect("should get raw hostname");
        assert!(!hostname.is_empty(), "hostname should not be empty");
    }

    #[test]
    #[cfg(unix)]
    fn test_get_is_consistent() {
        let hostname1 = get().expect("should get hostname");
        let hostname2 = get().expect("should get hostname again");
        assert_eq!(hostname1, hostname2, "hostname should be consistent");
    }

    #[test]
    #[cfg(unix)]
    fn test_get_raw_matches_get() {
        let hostname_str = get().expect("should get hostname");
        let hostname_os = get_raw().expect("should get raw hostname");
        assert_eq!(
            hostname_os.to_str(),
            Some(hostname_str.as_str()),
            "get and get_raw should return the same value"
        );
    }

    #[test]
    #[cfg(unix)]
    fn test_hostname_has_reasonable_length() {
        let hostname = get().expect("should get hostname");
        // Hostnames are typically limited to 64-255 characters
        assert!(
            hostname.len() <= 255,
            "hostname should have reasonable length"
        );
    }

    #[test]
    #[cfg(unix)]
    fn test_hostname_is_valid_utf8() {
        // If get() succeeds, the hostname is valid UTF-8
        let result = get();
        assert!(result.is_ok(), "hostname should be valid UTF-8");
    }
}
