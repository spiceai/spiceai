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

//! Cross-platform home directory detection.

use std::path::PathBuf;

/// Returns the home directory for the current user.
///
/// - On Unix (Linux, macOS): Uses `$HOME` environment variable
/// - On Windows: Uses `USERPROFILE` environment variable
///
/// # Examples
///
/// ```
/// use util::home_dir::home_dir;
///
/// if let Some(home) = home_dir() {
///     println!("Home directory: {}", home.display());
/// }
/// ```
#[must_use]
pub fn home_dir() -> Option<PathBuf> {
    #[cfg(unix)]
    {
        std::env::var_os("HOME").map(PathBuf::from)
    }

    #[cfg(windows)]
    {
        std::env::var_os("USERPROFILE").map(PathBuf::from)
    }

    #[cfg(not(any(unix, windows)))]
    {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_home_dir_returns_some_on_standard_systems() {
        // On most systems running tests, home directory should be set
        let home = home_dir();
        assert!(
            home.is_some(),
            "home_dir() should return Some on standard systems"
        );
    }

    #[test]
    fn test_home_dir_path_not_empty() {
        if let Some(path) = home_dir() {
            assert!(
                !path.as_os_str().is_empty(),
                "home directory path should not be empty"
            );
        }
    }

    #[test]
    fn test_home_dir_is_absolute() {
        if let Some(path) = home_dir() {
            assert!(
                path.is_absolute(),
                "home directory should be an absolute path, got: {}",
                path.display()
            );
        }
    }

    #[test]
    #[cfg(unix)]
    fn test_home_dir_matches_env_var_on_unix() {
        if let Some(expected) = std::env::var_os("HOME") {
            let home = home_dir();
            assert_eq!(
                home,
                Some(PathBuf::from(expected)),
                "home_dir() should match $HOME on Unix"
            );
        }
    }

    #[test]
    #[cfg(windows)]
    fn test_home_dir_matches_env_var_on_windows() {
        if let Some(expected) = std::env::var_os("USERPROFILE") {
            let home = home_dir();
            assert_eq!(
                home,
                Some(PathBuf::from(expected)),
                "home_dir() should match USERPROFILE on Windows"
            );
        }
    }

    #[test]
    fn test_home_dir_deterministic() {
        // Calling multiple times should return the same result
        let home1 = home_dir();
        let home2 = home_dir();
        assert_eq!(home1, home2, "home_dir() should be deterministic");
    }

    #[test]
    fn test_home_dir_can_be_joined() {
        if let Some(home) = home_dir() {
            let subpath = home.join(".config").join("test");
            assert!(
                subpath.starts_with(&home),
                "Joined path should start with home directory"
            );
        }
    }
}
