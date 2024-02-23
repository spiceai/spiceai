use arrow::datatypes::{DataType, Schema};
use sea_query::{
    Alias, ColumnDef, ColumnType, Index, IntoIden, IntoIndexColumn, PostgresQueryBuilder, Table,
};

pub struct CreateTableBuilder {
    schema: Schema,
    table_name: String,
    primary_keys: Vec<String>,
}

impl CreateTableBuilder {
    #[must_use]
    pub fn new(schema: &Schema, table_name: &str) -> Self {
        Self {
            schema: schema.clone(),
            table_name: table_name.to_string(),
            primary_keys: Vec::new(),
        }
    }

    #[must_use]
    pub fn primary_keys(mut self, keys: Vec<&str>) -> Self {
        self.primary_keys = keys.into_iter().map(ToString::to_string).collect();
        self
    }

    #[must_use]
    pub fn build(self) -> String {
        let mut create_stmt = Table::create();
        create_stmt
            .table(Alias::new(self.table_name))
            .if_not_exists();

        for field in self.schema.fields() {
            let column_type = map_data_type_to_column_type(field.data_type());
            let mut column_def = ColumnDef::new_with_type(Alias::new(field.name()), column_type);
            if !field.is_nullable() {
                column_def.not_null();
            }

            create_stmt.col(&mut column_def);
        }

        if !self.primary_keys.is_empty() {
            let mut index = Index::create();
            index.primary();
            for key in self.primary_keys {
                index.col(Alias::new(key).into_iden().into_index_column());
            }
            create_stmt.primary_key(&mut index);
        }

        create_stmt.to_string(PostgresQueryBuilder)
    }
}

fn map_data_type_to_column_type(data_type: &DataType) -> ColumnType {
    match data_type {
        DataType::Int8 => ColumnType::TinyInteger,
        DataType::Int16 => ColumnType::SmallInteger,
        DataType::Int32 => ColumnType::Integer,
        DataType::Int64 => ColumnType::BigInteger,
        DataType::UInt8 => ColumnType::TinyUnsigned,
        DataType::UInt16 => ColumnType::SmallUnsigned,
        DataType::UInt32 => ColumnType::Unsigned,
        DataType::UInt64 => ColumnType::BigUnsigned,
        DataType::Float32 => ColumnType::Float,
        DataType::Float64 => ColumnType::Double,
        DataType::Utf8 => ColumnType::Text,
        DataType::Boolean => ColumnType::Boolean,
        #[allow(clippy::cast_sign_loss)]
        DataType::Decimal128(p, s) => ColumnType::Decimal(Some((u32::from(*p), *s as u32))),
        DataType::Timestamp(_unit, _time_zone) => ColumnType::Timestamp,

        // Add more mappings here as needed
        _ => unimplemented!("Data type mapping not implemented for {:?}", data_type),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};

    #[test]
    fn test_basic_table_creation() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("age", DataType::Int32, true),
        ]);
        let sql = CreateTableBuilder::new(&schema, "users").build();

        assert_eq!(sql, "CREATE TABLE IF NOT EXISTS \"users\" ( \"id\" integer NOT NULL, \"name\" text NOT NULL, \"age\" integer )");
    }

    #[test]
    fn test_table_creation_with_primary_keys() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("id2", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("age", DataType::Int32, true),
        ]);
        let sql = CreateTableBuilder::new(&schema, "users")
            .primary_keys(vec!["id", "id2"])
            .build();

        assert_eq!(sql, "CREATE TABLE IF NOT EXISTS \"users\" ( \"id\" integer NOT NULL, \"id2\" integer NOT NULL, \"name\" text NOT NULL, \"age\" integer, PRIMARY KEY (\"id\", \"id2\") )");
    }
}
