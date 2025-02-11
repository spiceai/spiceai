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

use super::{Acceleration, IndexType};
use crate::component::dataset;
use arrow::datatypes::SchemaRef;
use datafusion::common::{Constraint, Constraints};
use std::{collections::HashMap, fmt::Display};

impl Acceleration {
    #[must_use]
    pub fn hashmap_to_option_string<K, V>(map: &HashMap<K, V>) -> String
    where
        K: Display,
        V: Display,
    {
        map.iter()
            .map(|(k, v)| format!("{k}:{v}"))
            .collect::<Vec<String>>()
            .join(";")
    }

    fn valid_columns(schema: &SchemaRef) -> String {
        schema
            .flattened_fields()
            .into_iter()
            .map(|f| f.name().to_string())
            .collect::<Vec<_>>()
            .join(", ")
    }

    pub fn validate_indexes(&self, schema: &SchemaRef) -> dataset::Result<()> {
        for column in self.indexes.keys() {
            for index_column in column.iter() {
                if schema.field_with_name(index_column).is_err() {
                    return dataset::IndexColumnNotFoundSnafu {
                        index: index_column.to_string(),
                        valid_columns: Self::valid_columns(schema),
                    }
                    .fail();
                }
            }
        }

        Ok(())
    }

    pub fn validate_primary_key(&self, schema: &SchemaRef) -> dataset::Result<()> {
        if let Some(columns) = &self.primary_key {
            for column in columns.iter() {
                if schema.field_with_name(column).is_err() {
                    return dataset::PrimaryKeyColumnNotFoundSnafu {
                        invalid_column: column.to_string(),
                        valid_columns: Self::valid_columns(schema),
                    }
                    .fail();
                }
            }
        }

        Ok(())
    }

    #[allow(clippy::needless_pass_by_value)]
    pub fn table_constraints(&self, schema: SchemaRef) -> dataset::Result<Option<Constraints>> {
        if self.indexes.is_empty() && self.primary_key.is_none() {
            return Ok(None);
        }

        let mut table_constraints: Vec<Constraint> = Vec::new();

        for (column, index_type) in &self.indexes {
            match index_type {
                IndexType::Enabled => {}
                IndexType::Unique => {
                    let index_indices: Vec<usize> = column
                        .iter()
                        .filter_map(|c| schema.index_of(c).ok())
                        .collect();
                    let tc = Constraint::Unique(index_indices);

                    table_constraints.push(tc);
                }
            };
        }

        if let Some(primary_key) = &self.primary_key {
            let pk_indices: Vec<usize> = primary_key
                .iter()
                .filter_map(|c| schema.index_of(c).ok())
                .collect();
            let tc = Constraint::PrimaryKey(pk_indices);

            table_constraints.push(tc);
        }

        Ok(Some(Constraints::new_unverified(table_constraints)))
    }
}
