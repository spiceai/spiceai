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

use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
use url::{Url, form_urlencoded};

use crate::parameters::{ParameterSpec, Parameters};

mod connector;
mod infer;
pub use connector::{ListingTableConnector, build_table_parquet_options};

/// All [`super::DataConnectorFactory`] that create [`ListingTableConnector`]s should have at least these parameters returned from the associated [`super::DataConnectorFactory::parameters`].
pub const LISTING_TABLE_PARAMETERS: &[ParameterSpec] = &[
    ParameterSpec::runtime("file_format"),
    ParameterSpec::runtime("file_extension"),
    ParameterSpec::runtime("schema_infer_max_records")
        .description("Set a limit in terms of records to scan to infer the schema."),
    ParameterSpec::runtime("tsv_has_header")
        .description("Set true to indicate that the first line is a header.")
        .is_boolean(),
    ParameterSpec::runtime("tsv_quote").description("The quote character in a row."),
    ParameterSpec::runtime("tsv_escape").description("The escape character in a row."),
    ParameterSpec::runtime("tsv_schema_infer_max_records")
        .description("Set a limit in terms of records to scan to infer the schema.")
        .deprecated("use 'schema_infer_max_records' instead"),
    ParameterSpec::runtime("csv_has_header")
        .description("Set true to indicate that the first line is a header.")
        .is_boolean(),
    ParameterSpec::runtime("csv_quote").description("The quote character in a row."),
    ParameterSpec::runtime("csv_escape").description("The escape character in a row."),
    ParameterSpec::runtime("csv_schema_infer_max_records")
        .description("Set a limit in terms of records to scan to infer the schema.")
        .deprecated("use 'schema_infer_max_records' instead"),
    ParameterSpec::runtime("csv_delimiter")
        .description("The character separating values within a row."),
    ParameterSpec::runtime("file_compression_type")
        .description("The type of compression used on the file. If unset, Spice infers compression from file extensions such as .gz, .bz2, .xz, and .zst. Supported types are: GZIP, BZIP2, XZ, ZSTD, UNCOMPRESSED")
        .one_of(&["GZIP", "BZIP2", "XZ", "ZSTD", "UNCOMPRESSED"]),
    ParameterSpec::runtime("hive_partitioning_enabled")
        .description("Enable partitioning using hive-style partitioning from the folder structure. Defaults to false.")
        .is_boolean(),
    ParameterSpec::runtime("schema_source_path")
        .description("Specify a path to use for schema inference."),
    ParameterSpec::runtime("json_format")
        .description("json | jsonl | ndjson | ldjson | array | object | soda | socrata | auto. When 'file_format' is explicitly 'json', effective default is 'json' (no SODA auto-detection).")
        .default("auto")
        .one_of(&["json", "jsonl", "ndjson", "ldjson", "array", "object", "soda", "socrata", "auto"]),
    ParameterSpec::runtime("json_pointer")
        .description("An RFC 6901 JSON Pointer to extract data from within a JSON value. E.g. '/data' for {\"data\": [...]} or '/response/items' for nested objects. A leading '/' is added automatically if missing."),
    ParameterSpec::runtime("json_path")
        .description("Alias for 'json_pointer'. An RFC 6901 JSON Pointer to extract data from within a JSON value."),
    ParameterSpec::runtime("flatten_json")
        .description("Set true to flatten nested structs in JSON as separate columns.")
        .is_boolean(),
    ParameterSpec::runtime("soda_metadata")
        .description("Set to 'enabled' to include Socrata internal metadata columns (sid, id, position, etc.) in the schema for SODA format responses. Defaults to disabled.")
        .default("disabled")
        .one_of(&["enabled", "disabled"]),
    ParameterSpec::runtime("refresh_skip")
        .description("Control skipping refreshes for single-file S3 datasets when cached ETag/Version metadata matches. Set to 'enabled' (default) or 'disabled'.")
        .default("enabled")
        .one_of(&["enabled", "disabled"]),
];

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ParsedFileExtension {
    pub(crate) file_extension: String,
    pub(crate) format_extension: Option<String>,
    pub(crate) compression: Option<FileCompressionType>,
}

pub(crate) fn parse_file_extension_param(value: &str) -> Option<ParsedFileExtension> {
    let extension = value.trim().trim_start_matches('.');
    parse_extension_components(&extension.split('.').collect::<Vec<_>>(), false)
}

pub(crate) fn detect_file_extension_from_url_or_path(value: &str) -> Option<ParsedFileExtension> {
    Url::parse(value)
        .ok()
        .and_then(|url| detect_file_extension_from_path(url.path()))
        .or_else(|| detect_file_extension_from_path(value))
}

pub(crate) fn detect_file_extension_from_path(path: &str) -> Option<ParsedFileExtension> {
    let path = path
        .split(['?', '#'])
        .next()
        .unwrap_or(path)
        .trim_end_matches('/');
    let file_name = path.rsplit(['/', '\\']).next().unwrap_or(path);

    if file_name.is_empty() || !file_name.contains('.') {
        return None;
    }

    parse_extension_components(&file_name.split('.').collect::<Vec<_>>(), true)
}

fn parse_extension_components(
    components: &[&str],
    has_file_stem: bool,
) -> Option<ParsedFileExtension> {
    let last = components.last().copied()?.trim();
    if last.is_empty() {
        return None;
    }

    if let Some(compression) = compression_from_extension(last) {
        let format_index = if has_file_stem {
            components
                .len()
                .checked_sub(2)
                .filter(|_| components.len() >= 3)
        } else {
            components.len().checked_sub(2)
        };

        let Some(format_index) = format_index else {
            return Some(ParsedFileExtension {
                file_extension: format!(".{last}"),
                format_extension: None,
                compression: Some(compression),
            });
        };

        let format_extension = components[format_index].trim();
        if format_extension.is_empty() {
            return None;
        }

        return Some(ParsedFileExtension {
            file_extension: format!(".{format_extension}.{last}"),
            format_extension: Some(format_extension.to_ascii_lowercase()),
            compression: Some(compression),
        });
    }

    Some(ParsedFileExtension {
        file_extension: format!(".{last}"),
        format_extension: Some(last.to_ascii_lowercase()),
        compression: None,
    })
}

fn compression_from_extension(extension: &str) -> Option<FileCompressionType> {
    let compression = extension.parse::<FileCompressionType>().ok()?;
    compression.is_compressed().then_some(compression)
}

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
