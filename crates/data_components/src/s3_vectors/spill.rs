/*
Copyright 2025 The Spice.ai OSS Authors

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

use snafu::prelude::*;

/// The separator used between the base index name and spill sequence number.
const SPILL_SEPARATOR: &str = "-";

/// Maximum sequence number for spill indexes (00-99).
pub const MAX_SPILL_SEQUENCE: u8 = 99;

/// Represents a spill index with its base name and sequence number.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpillIndex {
    /// The base index name (without spill suffix).
    pub base_name: String,
    /// The spill sequence number (0-99).
    pub sequence: u8,
}

#[derive(Debug, PartialEq, Snafu)]
pub enum Error {
    #[snafu(display(
        "Invalid spill index name format: '{name}'. Expected format: base_name{SPILL_SEPARATOR}sequence_number"
    ))]
    InvalidSpillIndexFormat { name: String },

    #[snafu(display(
        "Spill sequence number {sequence} exceeds maximum allowed value of {MAX_SPILL_SEQUENCE}"
    ))]
    SequenceNumberTooLarge { sequence: u8 },

    #[snafu(display(
        "Spill sequence number could not be parsed from '{sequence_str}' in index name '{name}'"
    ))]
    InvalidSequenceNumber { sequence_str: String, name: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

impl SpillIndex {
    fn format_name(base_name: &str, sequence: u8) -> String {
        format!("{base_name}{SPILL_SEPARATOR}{sequence:02}")
    }

    /// Parses a spill index name into its components.
    pub fn parse(index_name: &str) -> Result<Option<Self>> {
        let parts: Vec<&str> = index_name.split(SPILL_SEPARATOR).collect();

        if parts.len() < 2 {
            return Ok(None);
        }

        let Some(sequence_str) = parts.last() else {
            return Ok(None);
        };

        let base_name_parts = &parts[..parts.len() - 1];
        let base_name = base_name_parts.join(SPILL_SEPARATOR);

        if sequence_str.len() != 2 {
            return Ok(None);
        }

        let sequence = sequence_str
            .parse::<u8>()
            .map_err(|_| Error::InvalidSequenceNumber {
                sequence_str: (*sequence_str).to_string(),
                name: index_name.to_string(),
            })?;

        if sequence > MAX_SPILL_SEQUENCE {
            return Err(Error::SequenceNumberTooLarge { sequence });
        }

        Ok(Some(Self {
            base_name,
            sequence,
        }))
    }

    /// Gets all spill index names that belong to the same virtual index.
    #[must_use]
    fn get_spill_indexes_for_virtual_index(base_name: &str, all_indexes: &[String]) -> Vec<String> {
        let mut spill_indexes = Vec::new();

        for index_name in all_indexes {
            if let Ok(Some(spill)) = Self::parse(index_name)
                && spill.base_name == base_name
            {
                spill_indexes.push(spill);
            }
        }

        // Sort by sequence number for consistent ordering
        spill_indexes.sort_by(|a, b| a.sequence.cmp(&b.sequence));

        spill_indexes
            .into_iter()
            .map(|i| Self::format_name(&i.base_name, i.sequence))
            .collect()
    }

    /// Gets all index names (main + spills) that belong to a virtual index.
    #[must_use]
    pub fn get_all_indexes_for_virtual_index(
        virtual_index_name: &str,
        all_indexes: &[String],
    ) -> Vec<String> {
        let base_name = if let Ok(Some(spill)) = Self::parse(virtual_index_name) {
            spill.base_name
        } else {
            virtual_index_name.to_string()
        };

        let mut result = Vec::new();

        // Add main index if it exists
        if all_indexes.contains(&base_name) {
            result.push(base_name.clone());
        }

        // Add all spill indexes
        let spill_indexes = Self::get_spill_indexes_for_virtual_index(&base_name, all_indexes);
        result.extend(spill_indexes);

        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_name() {
        assert_eq!(SpillIndex::format_name("myindex", 0), "myindex-00");
        assert_eq!(SpillIndex::format_name("myindex", 1), "myindex-01");
        assert_eq!(SpillIndex::format_name("myindex", 42), "myindex-42");
        assert_eq!(SpillIndex::format_name("myindex", 99), "myindex-99");
    }

    #[test]
    fn test_parse_valid_spill_index() {
        let result = SpillIndex::parse("myindex-00")
            .expect("success")
            .expect("success");
        assert_eq!(result.base_name, "myindex");
        assert_eq!(result.sequence, 0);

        let result = SpillIndex::parse("myindex-42")
            .expect("success")
            .expect("success");
        assert_eq!(result.base_name, "myindex");
        assert_eq!(result.sequence, 42);

        let result = SpillIndex::parse("test_index-99")
            .expect("success")
            .expect("success");
        assert_eq!(result.base_name, "test_index");
        assert_eq!(result.sequence, 99);
    }

    #[test]
    fn test_parse_partitioned_spill_index() {
        let result = SpillIndex::parse("myindex.hash1.hash2.hash3-01")
            .expect("success")
            .expect("success");
        assert_eq!(result.base_name, "myindex.hash1.hash2.hash3");
        assert_eq!(result.sequence, 1);

        let result = SpillIndex::parse("dataset.col.expr.val-05")
            .expect("success")
            .expect("success");
        assert_eq!(result.base_name, "dataset.col.expr.val");
        assert_eq!(result.sequence, 5);
    }

    #[test]
    fn test_parse_invalid_sequence() {
        assert!(SpillIndex::parse("myindex").expect("success").is_none());
        assert!(SpillIndex::parse("myindex.1").expect("success").is_none());
        assert!(SpillIndex::parse("myindex.123").expect("success").is_none());
        assert!(SpillIndex::parse("myindex.abc").expect("success").is_none());
        let result = SpillIndex::parse("myindex-aa");
        assert!(result.is_err());
    }

    #[test]
    fn test_get_spill_indexes_for_virtual_index() {
        let all_indexes = vec![
            "myindex".to_string(),
            "myindex-01".to_string(),
            "myindex-02".to_string(),
            "other-01".to_string(),
            "myindex-10".to_string(),
        ];

        let result = SpillIndex::get_spill_indexes_for_virtual_index("myindex", &all_indexes);
        assert_eq!(
            result,
            vec![
                "myindex-01".to_string(),
                "myindex-02".to_string(),
                "myindex-10".to_string(),
            ]
        );
    }

    #[test]
    fn test_get_all_indexes_for_virtual_index() {
        let all_indexes = vec![
            "myindex".to_string(),
            "myindex-01".to_string(),
            "myindex-02".to_string(),
            "other-01".to_string(),
        ];

        // From main index
        let result = SpillIndex::get_all_indexes_for_virtual_index("myindex", &all_indexes);
        assert_eq!(
            result,
            vec![
                "myindex".to_string(),
                "myindex-01".to_string(),
                "myindex-02".to_string(),
            ]
        );

        // From spill index
        let result = SpillIndex::get_all_indexes_for_virtual_index("myindex-01", &all_indexes);
        assert_eq!(
            result,
            vec![
                "myindex".to_string(),
                "myindex-01".to_string(),
                "myindex-02".to_string(),
            ]
        );
    }
}
