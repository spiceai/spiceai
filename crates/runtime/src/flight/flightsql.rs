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

pub(crate) mod get_catalogs;
pub(crate) mod get_primary_keys;
pub(crate) mod get_schemas;
pub(crate) mod get_sql_info;
pub(crate) mod get_table_types;
pub(crate) mod get_tables;
pub(crate) mod get_xdbc_type_info;
pub(crate) mod prepared_statement_handle;
pub(crate) mod prepared_statement_query;
pub(crate) mod prepared_statement_update;
pub(crate) mod statement_query;
pub(crate) mod statement_substrait_plan;
pub(crate) mod statement_update;
mod substrait_consumer;

use std::sync::{Arc, OnceLock};

use arrow_flight::{FlightData, error::FlightError};
use arrow_schema::SchemaRef;
use arrow_tools::map_entries;
use tonic::Status;

/// Longest `LIKE` filter pattern, in bytes, that a metadata request may carry. A pattern with
/// wildcards compiles to a regex whose size grows with the pattern.
const MAX_FILTER_PATTERN_LEN: usize = 1024;

/// Rejects a `field` filter pattern longer than [`MAX_FILTER_PATTERN_LEN`] bytes.
pub(crate) fn check_filter_pattern(field: &str, pattern: Option<&str>) -> Result<(), Status> {
    match pattern {
        Some(pattern) if pattern.len() > MAX_FILTER_PATTERN_LEN => {
            Err(Status::invalid_argument(format!(
                "{field} is {} bytes long; the limit is {MAX_FILTER_PATTERN_LEN} bytes",
                pattern.len()
            )))
        }
        _ => Ok(()),
    }
}

/// Most entries that one list field of a metadata request may carry.
const MAX_LIST_ENTRIES: usize = 1024;

/// Returns `values` sorted and deduplicated, or rejects a `field` list longer than
/// [`MAX_LIST_ENTRIES`] entries.
pub(crate) fn bounded_list<T: Ord>(field: &str, mut values: Vec<T>) -> Result<Vec<T>, Status> {
    if values.len() > MAX_LIST_ENTRIES {
        return Err(Status::invalid_argument(format!(
            "{field} lists {} entries; the limit is {MAX_LIST_ENTRIES}",
            values.len()
        )));
    }
    values.sort_unstable();
    values.dedup();
    Ok(values)
}

/// Passes `message` through unless it is a compressed batch that declares too large a
/// decompressed size for its body (see `arrow_tools::ipc::check_declared_decompressed_size`).
pub(crate) fn within_decompression_bound(message: FlightData) -> Result<FlightData, FlightError> {
    arrow_tools::ipc::check_declared_decompressed_size(&message.data_header, &message.data_body)
        .map_err(|error| FlightError::Tonic(Box::new(Status::invalid_argument(error))))?;
    Ok(message)
}

/// Keeps a `DoPut` stream decodable when a client declares a `MAP`'s `entries` field nullable.
///
/// The Arrow map layout forbids that declaration, and the decode refuses it outright — over the
/// one part of the column that holds no data, reporting neither the column nor which of the two
/// map rules was broken. `FlightDataDecoder` reads its schema off the stream rather than taking
/// one from its caller, so the repair has to reach it as bytes: [`Self::repair`] replaces the
/// schema message on the way past with the form that decodes, where a map is labelled as the
/// list it is laid out as.
///
/// What the client declared is kept, because that — not the substitution the decoder goes on to
/// report — is what a `MapEntriesNormalizer` has to be built from for the batches to be put back
/// as the maps they describe. Writing them on under the substitution would bind a `LIST`
/// parameter where the client sent a `MAP`.
#[derive(Clone, Default)]
pub(crate) struct DecodableSchema(Arc<OnceLock<SchemaRef>>);

impl DecodableSchema {
    /// Returns `message` with its schema message replaced by the form a decoder can build
    /// against, recording what it declared. Any other message is passed through untouched.
    pub(crate) fn repair(&self, mut message: FlightData) -> FlightData {
        if let Some((declared, header)) =
            map_entries::decodable_schema_message(&message.data_header)
        {
            drop(self.0.set(declared));
            message.data_header = header.into();
        }
        message
    }

    /// What the client declared: the schema [`Self::repair`] substituted away, or `decoded` when
    /// no substitution was needed and the decoder's own report is the client's.
    pub(crate) fn declared(&self, decoded: SchemaRef) -> SchemaRef {
        self.0.get().map_or(decoded, Arc::clone)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_list_at_the_limit_is_accepted_and_deduplicated() {
        let ids = (0..MAX_LIST_ENTRIES)
            .map(|i| u32::try_from(i % 4).expect("small id"))
            .collect();
        let ids = bounded_list("GetSqlInfo info", ids).expect("a list at the limit was rejected");
        assert_eq!(ids, vec![0, 1, 2, 3]);

        let types = vec![
            "VIEW".to_string(),
            "BASE TABLE".to_string(),
            "VIEW".to_string(),
        ];
        let types =
            bounded_list("GetTables table_types", types).expect("a short list was rejected");
        assert_eq!(types, vec!["BASE TABLE".to_string(), "VIEW".to_string()]);
    }

    #[test]
    fn a_list_over_the_limit_is_rejected() {
        let status = bounded_list(
            "GetTables table_types",
            vec![String::new(); MAX_LIST_ENTRIES + 1],
        )
        .expect_err("a list over the limit was accepted");
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert_eq!(
            status.message(),
            "GetTables table_types lists 1025 entries; the limit is 1024"
        );
    }

    #[test]
    fn a_filter_pattern_at_the_limit_is_accepted() {
        let pattern = format!("{}%", "a".repeat(MAX_FILTER_PATTERN_LEN - 1));
        assert!(check_filter_pattern("table_name_filter_pattern", Some(&pattern)).is_ok());
        assert!(check_filter_pattern("table_name_filter_pattern", Some("orders_%")).is_ok());
        assert!(check_filter_pattern("table_name_filter_pattern", None).is_ok());
    }

    #[test]
    fn a_filter_pattern_over_the_limit_is_rejected() {
        let pattern = format!("{}%", "a".repeat(MAX_FILTER_PATTERN_LEN));
        let status = check_filter_pattern("db_schema_filter_pattern", Some(&pattern))
            .expect_err("a pattern over the limit was accepted");
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert_eq!(
            status.message(),
            "db_schema_filter_pattern is 1025 bytes long; the limit is 1024 bytes"
        );
    }
}
