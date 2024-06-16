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

use super::{Acceleration, IndexType};
use crate::component::dataset;
use arrow::datatypes::SchemaRef;
use datafusion::{
    common::{Constraints, DFSchema},
    sql::sqlparser::ast::TableConstraint,
};
use snafu::prelude::*;
use std::{collections::HashMap, fmt::Display, sync::Arc};

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
            .all_fields()
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

    pub fn table_constraints(&self, schema: SchemaRef) -> dataset::Result<Option<Constraints>> {
        if self.indexes.is_empty() && self.primary_key.is_none() {
            return Ok(None);
        }

        let mut table_constraints: Vec<TableConstraint> = Vec::new();

        for (column, index_type) in &self.indexes {
            match index_type {
                IndexType::Enabled => {}
                IndexType::Unique => {
                    let tc = TableConstraint::Unique {
                        columns: column.iter().map(Into::into).collect(),
                        name: None,
                        index_name: None,
                        index_type_display:
                            datafusion::sql::sqlparser::ast::KeyOrIndexDisplay::None,
                        index_options: vec![],
                        characteristics: None,
                        index_type: None,
                    };

                    table_constraints.push(tc);
                }
            };
        }

        if let Some(primary_key) = &self.primary_key {
            let tc = TableConstraint::PrimaryKey {
                columns: primary_key.iter().map(Into::into).collect(),
                name: None,
                index_name: None,
                index_options: vec![],
                characteristics: None,
                index_type: None,
            };

            table_constraints.push(tc);
        }

        Ok(Some(
            Constraints::new_from_table_constraints(
                &table_constraints,
                &Arc::new(
                    DFSchema::try_from(schema)
                        .context(dataset::UnableToConvertSchemaRefToDFSchemaSnafu)?,
                ),
            )
            .context(dataset::UnableToGetTableConstraintsSnafu)?,
        ))
    }
}
