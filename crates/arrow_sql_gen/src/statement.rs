use arrow::{
    array::{array, Array, RecordBatch},
    datatypes::{DataType, SchemaRef},
};

use bigdecimal_0_3_0::BigDecimal;

use time::{OffsetDateTime, PrimitiveDateTime};

use sea_query::{
    Alias, ColumnDef, ColumnType, Index, InsertStatement, IntoIden, IntoIndexColumn,
    PostgresQueryBuilder, Query, SimpleExpr, Table,
};

pub struct CreateTableBuilder {
    schema: SchemaRef,
    table_name: String,
    primary_keys: Vec<String>,
}

impl CreateTableBuilder {
    #[must_use]
    pub fn new(schema: SchemaRef, table_name: &str) -> Self {
        Self {
            schema,
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

pub struct InsertBuilder {
    table_name: String,
    record_batches: Vec<RecordBatch>,
}

impl InsertBuilder {
    #[must_use]
    pub fn new(table_name: &str, record_batches: Vec<RecordBatch>) -> Self {
        Self {
            table_name: table_name.to_string(),
            record_batches,
        }
    }

    #[allow(clippy::too_many_lines)]
    pub fn construct_insert_stmt(
        &self,
        insert_stmt: &mut InsertStatement,
        record_batch: &RecordBatch,
    ) {
        for row in 0..record_batch.num_rows() {
            let mut row_values: Vec<SimpleExpr> = vec![];
            for col in 0..record_batch.num_columns() {
                let column = record_batch.column(col);
                match column.data_type() {
                    DataType::Int8 => {
                        let array = column.as_any().downcast_ref::<array::Int8Array>();
                        if let Some(valid_array) = array {
                            row_values.push(valid_array.value(row).into());
                        }
                    }
                    DataType::Int16 => {
                        let array = column.as_any().downcast_ref::<array::Int16Array>();
                        if let Some(valid_array) = array {
                            row_values.push(valid_array.value(row).into());
                        }
                    }
                    DataType::Int32 => {
                        let array = column.as_any().downcast_ref::<array::Int32Array>();
                        if let Some(valid_array) = array {
                            row_values.push(valid_array.value(row).into());
                        }
                    }
                    DataType::Int64 => {
                        let array = column.as_any().downcast_ref::<array::Int64Array>();
                        if let Some(valid_array) = array {
                            row_values.push(valid_array.value(row).into());
                        }
                    }
                    DataType::UInt8 => {
                        let array = column.as_any().downcast_ref::<array::UInt8Array>();
                        if let Some(valid_array) = array {
                            row_values.push(valid_array.value(row).into());
                        }
                    }
                    DataType::UInt16 => {
                        let array = column.as_any().downcast_ref::<array::UInt16Array>();
                        if let Some(valid_array) = array {
                            row_values.push(valid_array.value(row).into());
                        }
                    }
                    DataType::UInt32 => {
                        let array = column.as_any().downcast_ref::<array::UInt32Array>();
                        if let Some(valid_array) = array {
                            row_values.push(valid_array.value(row).into());
                        }
                    }
                    DataType::UInt64 => {
                        let array = column.as_any().downcast_ref::<array::UInt64Array>();
                        if let Some(valid_array) = array {
                            row_values.push(valid_array.value(row).into());
                        }
                    }
                    DataType::Float32 => {
                        let array = column.as_any().downcast_ref::<array::Float32Array>();
                        if let Some(valid_array) = array {
                            row_values.push(valid_array.value(row).into());
                        }
                    }
                    DataType::Float64 => {
                        let array = column.as_any().downcast_ref::<array::Float64Array>();
                        if let Some(valid_array) = array {
                            row_values.push(valid_array.value(row).into());
                        }
                    }
                    DataType::Utf8 => {
                        let array = column.as_any().downcast_ref::<array::StringArray>();
                        if let Some(valid_array) = array {
                            row_values.push(valid_array.value(row).into());
                        }
                    }
                    DataType::Boolean => {
                        let array = column.as_any().downcast_ref::<array::BooleanArray>();
                        if let Some(valid_array) = array {
                            row_values.push(valid_array.value(row).into());
                        }
                    }
                    DataType::Decimal128(_, scale) => {
                        let array = column.as_any().downcast_ref::<array::Decimal128Array>();
                        if let Some(valid_array) = array {
                            row_values.push(
                                BigDecimal::new(valid_array.value(row).into(), i64::from(*scale))
                                    .into(),
                            );
                        }
                    }
                    DataType::Timestamp(_, _) => {
                        let array = column
                            .as_any()
                            .downcast_ref::<array::TimestampMicrosecondArray>();
                        if let Some(valid_array) = array {
                            match OffsetDateTime::from_unix_timestamp(
                                valid_array.value(row) / 1_000_000,
                            ) {
                                Ok(offset_time) => {
                                    row_values.push(
                                        PrimitiveDateTime::new(
                                            offset_time.date(),
                                            offset_time.time(),
                                        )
                                        .into(),
                                    );
                                }
                                Err(_) => {
                                    return;
                                }
                            };
                        }
                    }
                    _ => unimplemented!(
                        "Data type mapping not implemented for {:?}",
                        column.data_type()
                    ),
                }
            }
            insert_stmt.values_panic(row_values);
        }
    }

    #[must_use]
    pub fn build(&self) -> String {
        let columns: Vec<Alias> = (self.record_batches[0])
            .schema()
            .fields()
            .iter()
            .map(|field| Alias::new(field.name()))
            .collect();

        let mut insert_stmt = Query::insert()
            .into_table(Alias::new(&self.table_name))
            .columns(columns)
            .to_owned();

        for record_batch in &self.record_batches {
            self.construct_insert_stmt(&mut insert_stmt, record_batch);
        }
        insert_stmt.to_string(PostgresQueryBuilder)
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
        #[allow(clippy::cast_sign_loss)] // This is safe because scale will never be negative
        DataType::Decimal128(p, s) => ColumnType::Decimal(Some((u32::from(*p), *s as u32))),
        DataType::Timestamp(_unit, _time_zone) => ColumnType::Timestamp,

        // Add more mappings here as needed
        _ => unimplemented!("Data type mapping not implemented for {:?}", data_type),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};

    #[test]
    fn test_basic_table_creation() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("age", DataType::Int32, true),
        ]);
        let sql = CreateTableBuilder::new(SchemaRef::new(schema), "users").build();

        assert_eq!(sql, "CREATE TABLE IF NOT EXISTS \"users\" ( \"id\" integer NOT NULL, \"name\" text NOT NULL, \"age\" integer )");
    }

    #[test]
    fn test_table_insertion() {
        let schema1 = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("age", DataType::Int32, true),
        ]);
        let id_array = array::Int32Array::from(vec![1, 2, 3]);
        let name_array = array::StringArray::from(vec!["a", "b", "c"]);
        let age_array = array::Int32Array::from(vec![10, 20, 30]);

        let batch1 = RecordBatch::try_new(
            Arc::new(schema1.clone()),
            vec![
                Arc::new(id_array.clone()),
                Arc::new(name_array.clone()),
                Arc::new(age_array.clone()),
            ],
        )
        .expect("Unable to build record batch");

        let schema2 = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("blah", DataType::Int32, true),
        ]);

        let batch2 = RecordBatch::try_new(
            Arc::new(schema2),
            vec![
                Arc::new(id_array),
                Arc::new(name_array),
                Arc::new(age_array),
            ],
        )
        .expect("Unable to build record batch");
        let record_batches = vec![batch1, batch2];

        let sql = InsertBuilder::new("users", record_batches).build();
        assert_eq!(sql, "INSERT INTO \"users\" (\"id\", \"name\", \"age\") VALUES (1, 'a', 10), (2, 'b', 20), (3, 'c', 30), (1, 'a', 10), (2, 'b', 20), (3, 'c', 30)");
    }

    #[test]
    fn test_table_creation_with_primary_keys() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("id2", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("age", DataType::Int32, true),
        ]);
        let sql = CreateTableBuilder::new(SchemaRef::new(schema), "users")
            .primary_keys(vec!["id", "id2"])
            .build();

        assert_eq!(sql, "CREATE TABLE IF NOT EXISTS \"users\" ( \"id\" integer NOT NULL, \"id2\" integer NOT NULL, \"name\" text NOT NULL, \"age\" integer, PRIMARY KEY (\"id\", \"id2\") )");
    }
}
