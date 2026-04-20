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

/// Expand tilde in path to home directory.
///
/// Handles:
/// - `~/path/to/file` → `/home/user/path/to/file`
/// - `~` → `/home/user`
/// - `~user/path` → not supported, returned as-is
pub fn expand_tilde(path: &std::path::Path) -> std::path::PathBuf {
    let Some(path_str) = path.to_str() else {
        return path.to_path_buf();
    };

    let Some(home) = dirs::home_dir() else {
        return path.to_path_buf();
    };

    if path_str == "~" {
        home
    } else if let Some(rest) = path_str.strip_prefix("~/") {
        home.join(rest)
    } else {
        path.to_path_buf()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn test_expand_tilde_with_path() {
        let input = Path::new("~/test/path");
        let result = expand_tilde(input);
        let home = dirs::home_dir().expect("home directory should be available");
        assert_eq!(result, home.join("test/path"));
    }

    #[test]
    fn test_expand_tilde_bare() {
        let input = Path::new("~");
        let result = expand_tilde(input);
        assert_eq!(
            result,
            dirs::home_dir().expect("home directory should be available")
        );
    }

    #[test]
    fn test_no_tilde_expansion() {
        let input = Path::new("/absolute/path");
        let result = expand_tilde(input);
        assert_eq!(result, input);
    }

    #[test]
    fn test_relative_path_no_tilde() {
        let input = Path::new("relative/path");
        let result = expand_tilde(input);
        assert_eq!(result, input);
    }
}
