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
pub(crate) mod prepared_statement_query;
pub(crate) mod prepared_statement_update;
pub(crate) mod statement_query;
pub(crate) mod statement_substrait_plan;
pub(crate) mod statement_update;

use std::sync::{Arc, OnceLock};

use arrow_flight::FlightData;
use arrow_schema::SchemaRef;
use arrow_tools::map_entries;

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
