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

use std::path::PathBuf;

/// Expand tilde in path to home directory.
///
/// Handles:
/// - `~/path/to/file` → `/home/user/path/to/file`
/// - `~` → `/home/user`
/// - `~user/path` → not supported, returned as-is
pub fn expand_tilde(path: &PathBuf) -> PathBuf {
    let path_str = match path.to_str() {
        Some(s) => s,
        None => return path.clone(),
    };

    // Handle bare "~" as the home directory.
    if path_str == "~" {
        if let Some(home) = dirs::home_dir() {
            return home;
        }
        return path.clone();
    }

    // Handle paths starting with "~/...".
    if let Some(stripped) = path_str.strip_prefix("~/") {
        if let Some(home) = dirs::home_dir() {
            return home.join(stripped);
        }
    }

    path.clone()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_expand_tilde_with_path() {
        let input = PathBuf::from("~/test/path");
        let result = expand_tilde(&input);
        let home = dirs::home_dir().unwrap();
        assert_eq!(result, home.join("test/path"));
    }

    #[test]
    fn test_expand_tilde_bare() {
        let input = PathBuf::from("~");
        let result = expand_tilde(&input);
        assert_eq!(result, dirs::home_dir().unwrap());
    }

    #[test]
    fn test_no_tilde_expansion() {
        let input = PathBuf::from("/absolute/path");
        let result = expand_tilde(&input);
        assert_eq!(result, input);
    }

    #[test]
    fn test_relative_path_no_tilde() {
        let input = PathBuf::from("relative/path");
        let result = expand_tilde(&input);
        assert_eq!(result, input);
    }
}
