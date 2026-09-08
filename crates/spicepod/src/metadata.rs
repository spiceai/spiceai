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

use serde_json::Value;

pub(crate) fn metadata_value_to_string(value: &Value) -> String {
    value
        .as_str()
        .map_or_else(|| value.to_string(), ToString::to_string)
}

#[cfg(test)]
mod tests {
    use super::metadata_value_to_string;
    use serde_json::json;

    #[test]
    fn metadata_value_to_string_preserves_raw_strings() {
        assert_eq!(
            metadata_value_to_string(&json!("enabled")),
            "enabled".to_string()
        );
    }

    #[test]
    fn metadata_value_to_string_serializes_structured_values() {
        assert_eq!(
            metadata_value_to_string(&json!({"owner":"analytics"})),
            "{\"owner\":\"analytics\"}".to_string()
        );
    }
}
