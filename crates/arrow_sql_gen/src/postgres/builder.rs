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
#![allow(clippy::module_name_repetitions)]

use arrow::datatypes::Fields;
use sea_query::{Alias, ColumnDef, PostgresQueryBuilder, TableBuilder, Write};

use crate::statement::map_data_type_to_column_type;

pub struct TypeBuilder<'a> {
    name: &'a str,
    columns: Vec<ColumnDef>,
}

impl<'a> TypeBuilder<'a> {
    #[must_use]
    pub fn new(name: &'a str, fields: &Fields) -> Self {
        Self {
            name,
            columns: fields_to_simple_column_defs(fields),
        }
    }

    #[must_use]
    pub fn build(self) -> Vec<String> {
        let pg_builder = PostgresQueryBuilder;

        // To be idempotent, we need to drop the type if it already exists before creating it.
        let mut drop_sql = String::new();

        let mut sql = String::new();

        let _ = write!(sql, "CREATE TYPE {} AS (", self.name);

        let mut first = true;

        for column_def in &self.columns {
            if !first {
                let _ = write!(sql, ", ");
            }

            pg_builder.prepare_column_def(column_def, &mut sql);
            first = false;
        }

        let _ = write!(sql, " )");

        sql
    }
}

/// Convert a `Fields` struct into a vector of `ColumnDef` without any constraints or other column specs.
fn fields_to_simple_column_defs(fields: &Fields) -> Vec<ColumnDef> {
    let mut column_defs = Vec::new();
    for field in fields {
        let column_type = map_data_type_to_column_type(field.data_type());
        let column_def = ColumnDef::new_with_type(Alias::new(field.name()), column_type);

        column_defs.push(column_def);
    }

    column_defs
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;

    #[test]
    fn test_type_builder() {
        let fields = vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ];
        let schema = Schema::new(fields);

        let type_builder = TypeBuilder::new("person", schema.fields());
        let sql = type_builder.build();

        assert_eq!(sql, r#"CREATE TYPE person AS ("id" integer, "name" text )"#);
    }
}
