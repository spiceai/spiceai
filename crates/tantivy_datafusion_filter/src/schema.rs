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

//! Tantivy text-field analysis helpers, shared by index schema validation and by the filter
//! classifier in [`crate::filter`], which must not push a term filter against a tokenized column.

use tantivy::schema::{FieldType, TextFieldIndexing};

/// The tokenizer a text field is analyzed with, or [`None`] for any other field type.
pub fn text_tokenizer(field_type: &FieldType) -> Option<&str> {
    match field_type {
        FieldType::Str(options) => options
            .get_indexing_options()
            .map(TextFieldIndexing::tokenizer),
        _ => None,
    }
}

/// Whether a text field is analyzed into multiple terms, rather than indexed as the single term
/// that [`tantivy::schema::STRING`] (and so a primary-key lookup) relies on.
#[must_use]
pub fn is_tokenized(field_type: &FieldType) -> bool {
    // Compare against tantivy's own untokenized text options rather than naming its tokenizer,
    // which tantivy does not export.
    let untokenized = FieldType::Str(tantivy::schema::STRING);
    match (text_tokenizer(field_type), text_tokenizer(&untokenized)) {
        (Some(tokenizer), Some(untokenized)) => tokenizer != untokenized,
        _ => false,
    }
}
