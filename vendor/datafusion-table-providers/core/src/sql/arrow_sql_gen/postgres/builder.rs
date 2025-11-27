use datafusion::arrow::datatypes::Fields;
use sea_query::{Alias, ColumnDef, PostgresQueryBuilder, TableBuilder};

use crate::sql::arrow_sql_gen::statement::map_data_type_to_column_type;

pub struct TypeBuilder {
    name: String,
    columns: Vec<ColumnDef>,
}

impl TypeBuilder {
    #[must_use]
    pub fn new(name: String, fields: &Fields) -> Self {
        Self {
            name,
            columns: fields_to_simple_column_defs(fields),
        }
    }

    #[must_use]
    pub fn build(self) -> String {
        let pg_builder = PostgresQueryBuilder;

        let mut sql = String::new();

        // Postgres doesn't natively support a CREATE TYPE IF NOT EXISTS statement,
        // so we'll wrap it in a DO block with a conditional check.

        sql.push_str(&format!(
            "
        DO $$ 
        BEGIN
            IF NOT EXISTS (
                SELECT 1
                FROM pg_type t
                WHERE t.typname = '{}'
            ) THEN
                ",
            self.name
        ));

        sql.push_str(&format!("CREATE TYPE {} AS (", self.name));

        let mut first = true;

        for column_def in &self.columns {
            if !first {
                sql.push_str(", ");
            }

            pg_builder.prepare_column_def(column_def, &mut sql);
            first = false;
        }

        sql.push_str(" );");

        sql.push_str(
            "
            END IF;
        END $$;
        ",
        );

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
    use datafusion::arrow::datatypes::{DataType, Field, Schema};

    use super::*;

    #[test]
    fn test_type_builder() {
        let fields = vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ];
        let schema = Schema::new(fields);

        let type_builder = TypeBuilder::new("person".to_string(), schema.fields());
        let sql = type_builder.build();

        assert_eq!(
            sql,
            r#"
        DO $$ 
        BEGIN
            IF NOT EXISTS (
                SELECT 1
                FROM pg_type t
                WHERE t.typname = 'person'
            ) THEN
                CREATE TYPE person AS ("id" integer, "name" text );
            END IF;
        END $$;
        "#
        );
    }
}
