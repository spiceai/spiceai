/*
Copyright 2024 The Spice.ai OSS Authors

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

use url::form_urlencoded;

use crate::parameters::Parameters;

mod connector;
mod infer;
pub use connector::ListingTableConnector;

#[must_use]
pub fn build_fragments(params: &Parameters, keys: Vec<&str>) -> String {
    let mut fragments = vec![];
    let mut fragment_builder = form_urlencoded::Serializer::new(String::new());

    for key in keys {
        if let Some(value) = params.get(key).expose().ok() {
            fragment_builder.append_pair(key, value);
        }
    }
    fragments.push(fragment_builder.finish());
    fragments.join("&")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parameters::ParameterSpec;
    use datafusion_table_providers::util::secrets::to_secret_map;
    use std::collections::HashMap;

    const TEST_PARAMETERS: &[ParameterSpec] = &[
        ParameterSpec::runtime("file_extension"),
        ParameterSpec::runtime("file_format"),
        ParameterSpec::runtime("schema_infer_max_records"),
        ParameterSpec::runtime("csv_has_header"),
        ParameterSpec::runtime("csv_quote"),
        ParameterSpec::runtime("csv_escape"),
        ParameterSpec::runtime("csv_schema_infer_max_records"),
        ParameterSpec::runtime("csv_delimiter"),
        ParameterSpec::runtime("file_compression_type"),
    ];

    #[test]
    fn test_build_fragments() {
        let mut params = HashMap::new();
        params.insert("file_format".to_string(), "csv".to_string());
        params.insert("csv_has_header".to_string(), "true".to_string());
        let params = Parameters::new(
            to_secret_map(params).into_iter().collect(),
            "test",
            TEST_PARAMETERS,
        );

        assert_eq!(
            build_fragments(&params, vec!["file_format", "csv_has_header"]),
            "file_format=csv&csv_has_header=true"
        );
    }
}
