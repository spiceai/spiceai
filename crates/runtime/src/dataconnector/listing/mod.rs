/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

use crate::parameters::{ParameterSpec, Parameters};

mod connector;
mod infer;
pub use connector::ListingTableConnector;

/// All [`super::DataConnectorFactory`] that create [`ListingTableConnector`]s should have at least these parameters returned from the associated [`super::DataConnectorFactory::parameters`].
pub const LISTING_TABLE_PARAMETERS: &[ParameterSpec] = &[
    ParameterSpec::runtime("file_format"),
    ParameterSpec::runtime("file_extension"),
    ParameterSpec::runtime("schema_infer_max_records")
        .description("Set a limit in terms of records to scan to infer the schema."),
    ParameterSpec::runtime("tsv_has_header")
        .description("Set true to indicate that the first line is a header."),
    ParameterSpec::runtime("tsv_quote").description("The quote character in a row."),
    ParameterSpec::runtime("tsv_escape").description("The escape character in a row."),
    ParameterSpec::runtime("tsv_schema_infer_max_records")
        .description("Set a limit in terms of records to scan to infer the schema.")
        .deprecated("use 'schema_infer_max_records' instead"),
    ParameterSpec::runtime("csv_has_header")
        .description("Set true to indicate that the first line is a header."),
    ParameterSpec::runtime("csv_quote").description("The quote character in a row."),
    ParameterSpec::runtime("csv_escape").description("The escape character in a row."),
    ParameterSpec::runtime("csv_schema_infer_max_records")
        .description("Set a limit in terms of records to scan to infer the schema.")
        .deprecated("use 'schema_infer_max_records' instead"),
    ParameterSpec::runtime("csv_delimiter")
        .description("The character separating values within a row."),
    ParameterSpec::runtime("file_compression_type")
        .description("The type of compression used on the file. Supported types are: GZIP, BZIP2, XZ, ZSTD, UNCOMPRESSED"),
    ParameterSpec::runtime("hive_partitioning_enabled")
        .description("Enable partitioning using hive-style partitioning from the folder structure. Defaults to false."),
];

pub enum DelimitedFormat {
    Tsv,
    Csv,
}

impl std::fmt::Display for DelimitedFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DelimitedFormat::Tsv => write!(f, "tsv"),
            DelimitedFormat::Csv => write!(f, "csv"),
        }
    }
}

impl DelimitedFormat {
    fn separator(&self) -> u8 {
        match self {
            DelimitedFormat::Tsv => b'\t',
            DelimitedFormat::Csv => b',',
        }
    }
}

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
