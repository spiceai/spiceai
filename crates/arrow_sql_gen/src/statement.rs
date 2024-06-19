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
use arrow::{
    array::{array, Array, RecordBatch, StructArray},
    datatypes::{DataType, Fields, SchemaRef, TimeUnit},
};

use bigdecimal_0_3_0::BigDecimal;

use serde_json::Value;
use snafu::Snafu;
use time::{OffsetDateTime, PrimitiveDateTime};

use sea_query::{
    Alias, BlobSize, ColumnDef, ColumnType, GenericBuilder, Index, InsertStatement, IntoIden,
    IntoIndexColumn, Keyword, MysqlQueryBuilder, OnConflict, PostgresQueryBuilder, Query,
    SimpleExpr, SqliteQueryBuilder, Table,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to build insert statement: {source}"))]
    FailedToCreateInsertStatement {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Unimplemented data type in insert statement: {data_type:?}"))]
    UnimplementedDataTypeInInsertStatement { data_type: DataType },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

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
    pub fn primary_keys<T>(mut self, keys: Vec<T>) -> Self
    where
        T: Into<String>,
    {
        self.primary_keys = keys.into_iter().map(Into::into).collect();
        self
    }

    #[must_use]
    pub fn build_postgres(self) -> String {
        self.build(PostgresQueryBuilder)
    }

    #[must_use]
    pub fn build_sqlite(self) -> String {
        self.build(SqliteQueryBuilder)
    }

    #[must_use]
    pub fn build_mysql(self) -> String {
        self.build(MysqlQueryBuilder)
    }

    #[must_use]
    pub fn build<T: GenericBuilder>(self, query_builder: T) -> String {
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

        create_stmt.to_string(query_builder)
    }
}

macro_rules! push_value {
    ($row_values:expr, $column:expr, $row:expr, $array_type:ident) => {{
        let array = $column.as_any().downcast_ref::<array::$array_type>();
        if let Some(valid_array) = array {
            if valid_array.is_null($row) {
                $row_values.push(Keyword::Null.into());
                continue;
            }
            $row_values.push(valid_array.value($row).into());
        }
    }};
}

macro_rules! push_list_values {
    ($data_type:expr, $list_array:expr, $row_values:expr, $array_type:ty, $vec_type:ty, $sql_type:expr) => {{
        let mut list_values: Vec<$vec_type> = Vec::new();
        for i in 0..$list_array.len() {
            let temp_array = $list_array.as_any().downcast_ref::<$array_type>();
            if let Some(valid_array) = temp_array {
                list_values.push(valid_array.value(i));
            }
        }
        let expr: SimpleExpr = list_values.into();
        // We must cast here in case the array is empty which SeaQuery does not handle.
        $row_values.push(expr.cast_as(Alias::new($sql_type)));
    }};
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

    /// Create an Insert statement from a `RecordBatch`.
    ///
    /// # Errors
    ///
    /// Returns an error if a column's data type is not supported, or its conversion failed.
    #[allow(clippy::too_many_lines)]
    pub fn construct_insert_stmt(
        &self,
        insert_stmt: &mut InsertStatement,
        record_batch: &RecordBatch,
    ) -> Result<()> {
        for row in 0..record_batch.num_rows() {
            let mut row_values: Vec<SimpleExpr> = vec![];
            for col in 0..record_batch.num_columns() {
                let column = record_batch.column(col);

                match column.data_type() {
                    DataType::Int8 => push_value!(row_values, column, row, Int8Array),
                    DataType::Int16 => push_value!(row_values, column, row, Int16Array),
                    DataType::Int32 => push_value!(row_values, column, row, Int32Array),
                    DataType::Int64 => push_value!(row_values, column, row, Int64Array),
                    DataType::UInt8 => push_value!(row_values, column, row, UInt8Array),
                    DataType::UInt16 => push_value!(row_values, column, row, UInt16Array),
                    DataType::UInt32 => push_value!(row_values, column, row, UInt32Array),
                    DataType::UInt64 => push_value!(row_values, column, row, UInt64Array),
                    DataType::Float32 => push_value!(row_values, column, row, Float32Array),
                    DataType::Float64 => push_value!(row_values, column, row, Float64Array),
                    DataType::Utf8 => push_value!(row_values, column, row, StringArray),
                    DataType::LargeUtf8 => push_value!(row_values, column, row, LargeStringArray),
                    DataType::Boolean => push_value!(row_values, column, row, BooleanArray),
                    DataType::Decimal128(_, scale) => {
                        let array = column.as_any().downcast_ref::<array::Decimal128Array>();
                        if let Some(valid_array) = array {
                            if valid_array.is_null(row) {
                                row_values.push(Keyword::Null.into());
                                continue;
                            }
                            row_values.push(
                                BigDecimal::new(valid_array.value(row).into(), i64::from(*scale))
                                    .into(),
                            );
                        }
                    }
                    DataType::Date32 => {
                        let array = column.as_any().downcast_ref::<array::Date32Array>();
                        if let Some(valid_array) = array {
                            if valid_array.is_null(row) {
                                row_values.push(Keyword::Null.into());
                                continue;
                            }
                            row_values.push(
                                match OffsetDateTime::from_unix_timestamp(
                                    i64::from(valid_array.value(row)) * 86_400,
                                ) {
                                    Ok(offset_time) => offset_time.date().into(),
                                    Err(e) => {
                                        return Result::Err(Error::FailedToCreateInsertStatement {
                                            source: Box::new(e),
                                        })
                                    }
                                },
                            );
                        }
                    }
                    DataType::Date64 => {
                        let array = column.as_any().downcast_ref::<array::Date64Array>();
                        if let Some(valid_array) = array {
                            if valid_array.is_null(row) {
                                row_values.push(Keyword::Null.into());
                                continue;
                            }
                            row_values.push(
                                match OffsetDateTime::from_unix_timestamp(
                                    valid_array.value(row) * 86_400,
                                ) {
                                    Ok(offset_time) => offset_time.date().into(),
                                    Err(e) => {
                                        return Result::Err(Error::FailedToCreateInsertStatement {
                                            source: Box::new(e),
                                        })
                                    }
                                },
                            );
                        }
                    }
                    DataType::Time64(TimeUnit::Nanosecond) => {
                        let array = column
                            .as_any()
                            .downcast_ref::<array::Time64NanosecondArray>();
                        if let Some(valid_array) = array {
                            if valid_array.is_null(row) {
                                row_values.push(Keyword::Null.into());
                                continue;
                            }
                            row_values.push(valid_array.value(row).into());
                        }
                    }
                    DataType::Timestamp(TimeUnit::Second, _) => {
                        let array = column
                            .as_any()
                            .downcast_ref::<array::TimestampSecondArray>();

                        if let Some(valid_array) = array {
                            if valid_array.is_null(row) {
                                row_values.push(Keyword::Null.into());
                                continue;
                            }
                            insert_timestamp_into_row_values(
                                OffsetDateTime::from_unix_timestamp(valid_array.value(row)),
                                &mut row_values,
                            )?;
                        }
                    }
                    DataType::Timestamp(TimeUnit::Millisecond, _) => {
                        let array = column
                            .as_any()
                            .downcast_ref::<array::TimestampMillisecondArray>();

                        if let Some(valid_array) = array {
                            if valid_array.is_null(row) {
                                row_values.push(Keyword::Null.into());
                                continue;
                            }
                            insert_timestamp_into_row_values(
                                OffsetDateTime::from_unix_timestamp_nanos(
                                    i128::from(valid_array.value(row)) * 1_000_000,
                                ),
                                &mut row_values,
                            )?;
                        }
                    }
                    DataType::Timestamp(TimeUnit::Microsecond, _) => {
                        let array = column
                            .as_any()
                            .downcast_ref::<array::TimestampMicrosecondArray>();

                        if let Some(valid_array) = array {
                            if valid_array.is_null(row) {
                                row_values.push(Keyword::Null.into());
                                continue;
                            }
                            insert_timestamp_into_row_values(
                                OffsetDateTime::from_unix_timestamp_nanos(
                                    i128::from(valid_array.value(row)) * 1_000,
                                ),
                                &mut row_values,
                            )?;
                        }
                    }
                    DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                        let array = column
                            .as_any()
                            .downcast_ref::<array::TimestampNanosecondArray>();

                        if let Some(valid_array) = array {
                            if valid_array.is_null(row) {
                                row_values.push(Keyword::Null.into());
                                continue;
                            }
                            insert_timestamp_into_row_values(
                                OffsetDateTime::from_unix_timestamp_nanos(i128::from(
                                    valid_array.value(row),
                                )),
                                &mut row_values,
                            )?;
                        }
                    }
                    DataType::List(list_type) => {
                        let array = column.as_any().downcast_ref::<array::ListArray>();
                        if let Some(valid_array) = array {
                            if valid_array.is_null(row) {
                                row_values.push(Keyword::Null.into());
                                continue;
                            }
                            let list_array = valid_array.value(row);
                            match list_type.data_type() {
                                DataType::Int8 => push_list_values!(
                                    list_type.data_type(),
                                    list_array,
                                    row_values,
                                    array::Int8Array,
                                    i8,
                                    "int2[]"
                                ),
                                DataType::Int16 => push_list_values!(
                                    list_type.data_type(),
                                    list_array,
                                    row_values,
                                    array::Int16Array,
                                    i16,
                                    "int2[]"
                                ),
                                DataType::Int32 => push_list_values!(
                                    list_type.data_type(),
                                    list_array,
                                    row_values,
                                    array::Int32Array,
                                    i32,
                                    "int4[]"
                                ),
                                DataType::Int64 => push_list_values!(
                                    list_type.data_type(),
                                    list_array,
                                    row_values,
                                    array::Int64Array,
                                    i64,
                                    "int8[]"
                                ),
                                DataType::Float32 => push_list_values!(
                                    list_type.data_type(),
                                    list_array,
                                    row_values,
                                    array::Float32Array,
                                    f32,
                                    "float4[]"
                                ),
                                DataType::Float64 => push_list_values!(
                                    list_type.data_type(),
                                    list_array,
                                    row_values,
                                    array::Float64Array,
                                    f64,
                                    "float8[]"
                                ),
                                DataType::Utf8 => {
                                    let mut list_values: Vec<String> = vec![];
                                    for i in 0..list_array.len() {
                                        let int_array = list_array
                                            .as_any()
                                            .downcast_ref::<array::StringArray>();
                                        if let Some(valid_int_array) = int_array {
                                            list_values.push(valid_int_array.value(i).to_string());
                                        }
                                    }
                                    let expr: SimpleExpr = list_values.into();
                                    // We must cast here in case the array is empty which SeaQuery does not handle.
                                    row_values.push(expr.cast_as(Alias::new("text[]")));
                                }
                                DataType::LargeUtf8 => {
                                    let mut list_values: Vec<String> = vec![];
                                    for i in 0..list_array.len() {
                                        let int_array = list_array
                                            .as_any()
                                            .downcast_ref::<array::LargeStringArray>();
                                        if let Some(valid_int_array) = int_array {
                                            list_values.push(valid_int_array.value(i).to_string());
                                        }
                                    }
                                    let expr: SimpleExpr = list_values.into();
                                    // We must cast here in case the array is empty which SeaQuery does not handle.
                                    row_values.push(expr.cast_as(Alias::new("text[]")));
                                }
                                DataType::Boolean => push_list_values!(
                                    list_type.data_type(),
                                    list_array,
                                    row_values,
                                    array::BooleanArray,
                                    bool,
                                    "boolean[]"
                                ),
                                _ => unimplemented!(
                                    "Data type mapping not implemented for {}",
                                    list_type.data_type()
                                ),
                            }
                        }
                    }
                    DataType::Binary => {
                        let array = column.as_any().downcast_ref::<array::BinaryArray>();

                        if let Some(valid_array) = array {
                            if valid_array.is_null(row) {
                                row_values.push(Keyword::Null.into());
                                continue;
                            }

                            row_values.push(valid_array.value(row).into());
                        }
                    }
                    DataType::Struct(fields) => {
                        let array = column.as_any().downcast_ref::<array::StructArray>();

                        if let Some(valid_array) = array {
                            if valid_array.is_null(row) {
                                row_values.push(Keyword::Null.into());
                                continue;
                            }
                            row_values.push(serialize_struct_to_json(fields, valid_array, row).into());
                        }
                    }
                    unimplemented_type => {
                        return Result::Err(Error::UnimplementedDataTypeInInsertStatement {
                            data_type: unimplemented_type.clone(),
                        })
                    }
                }
            }
            match insert_stmt.values(row_values) {
                Ok(_) => (),
                Err(e) => {
                    return Result::Err(Error::FailedToCreateInsertStatement {
                        source: Box::new(e),
                    })
                }
            }
        }
        Ok(())
    }

    ///
    /// # Errors
    ///
    /// Returns an error if any `RecordBatch` fails to convert into a valid postgres insert statement.
    pub fn build_postgres(self, on_conflict: Option<OnConflict>) -> Result<String> {
        self.build(PostgresQueryBuilder, on_conflict)
    }

    ///
    /// # Errors
    ///
    /// Returns an error if any `RecordBatch` fails to convert into a valid sqlite insert statement.
    pub fn build_sqlite(self, on_conflict: Option<OnConflict>) -> Result<String> {
        self.build(SqliteQueryBuilder, on_conflict)
    }

    ///
    /// # Errors
    ///
    /// Returns an error if any `RecordBatch` fails to convert into a valid `MySQL` insert statement.
    pub fn build_mysql(self, on_conflict: Option<OnConflict>) -> Result<String> {
        self.build(MysqlQueryBuilder, on_conflict)
    }

    /// # Errors
    ///
    /// Returns an error if any `RecordBatch` fails to convert into a valid insert statement. Upon
    /// error, no further `RecordBatch` is processed.
    pub fn build<T: GenericBuilder>(
        &self,
        query_builder: T,
        on_conflict: Option<OnConflict>,
    ) -> Result<String> {
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
            self.construct_insert_stmt(&mut insert_stmt, record_batch)?;
        }
        if let Some(on_conflict) = on_conflict {
            insert_stmt.on_conflict(on_conflict);
        }
        Ok(insert_stmt.to_string(query_builder))
    }
}

pub struct IndexBuilder {
    table_name: String,
    columns: Vec<String>,
    unique: bool,
}

impl IndexBuilder {
    #[must_use]
    pub fn new(table_name: &str, columns: Vec<&str>) -> Self {
        Self {
            table_name: table_name.to_string(),
            columns: columns.into_iter().map(ToString::to_string).collect(),
            unique: false,
        }
    }

    #[must_use]
    pub fn unique(mut self) -> Self {
        self.unique = true;
        self
    }

    #[must_use]
    pub fn index_name(&self) -> String {
        format!("i_{}_{}", self.table_name, self.columns.join("_"))
    }

    #[must_use]
    pub fn build_postgres(self) -> String {
        self.build(PostgresQueryBuilder)
    }

    #[must_use]
    pub fn build_sqlite(self) -> String {
        self.build(SqliteQueryBuilder)
    }

    #[must_use]
    pub fn build_mysql(self) -> String {
        self.build(MysqlQueryBuilder)
    }

    #[must_use]
    pub fn build<T: GenericBuilder>(self, query_builder: T) -> String {
        let mut index = Index::create();
        index.table(Alias::new(&self.table_name));
        index.name(self.index_name());
        if self.unique {
            index.unique();
        }
        for column in self.columns {
            index.col(Alias::new(column).into_iden().into_index_column());
        }
        index.if_not_exists();
        index.to_string(query_builder)
    }
}

fn insert_timestamp_into_row_values(
    timestamp: Result<OffsetDateTime, time::error::ComponentRange>,
    row_values: &mut Vec<SimpleExpr>,
) -> Result<()> {
    match timestamp {
        Ok(offset_time) => {
            row_values.push(PrimitiveDateTime::new(offset_time.date(), offset_time.time()).into());
            Ok(())
        }
        Err(e) => Err(Error::FailedToCreateInsertStatement {
            source: Box::new(e),
        }),
    }
}

fn serialize_struct_to_json(fields: &Fields, array: &StructArray, row: usize) -> String {
    // TODO: invetigate https://arrow.apache.org/rust/arrow_json/reader/struct.Decoder.html

    let mut json_object = serde_json::Map::new();

    for (idx, field) in array.fields().iter().enumerate() {
        let column = array.column(idx);

        let field_name = field.name();
        let field_value = match field.data_type() {
            DataType::Int8 => {
                let int_array = column.as_any().downcast_ref::<array::Int8Array>().unwrap();
                if int_array.is_null(row) {
                    serde_json::Value::Null
                } else {
                    serde_json::json!(int_array.value(row))
                }
            }
            DataType::Int16 => {
                let int_array = column.as_any().downcast_ref::<array::Int16Array>().unwrap();
                if int_array.is_null(row) {
                    serde_json::Value::Null
                } else {
                    serde_json::json!(int_array.value(row))
                }
            }
            DataType::Int32 => {
                let int_array = column.as_any().downcast_ref::<array::Int32Array>().unwrap();
                if int_array.is_null(row) {
                    serde_json::Value::Null
                } else {
                    serde_json::json!(int_array.value(row))
                }
            }
            DataType::Int64 => {
                let int_array = column.as_any().downcast_ref::<array::Int64Array>().unwrap();
                if int_array.is_null(row) {
                    serde_json::Value::Null
                } else {
                    serde_json::json!(int_array.value(row))
                }
            }
            DataType::Float32 => {
                let float_array = column.as_any().downcast_ref::<array::Float32Array>().unwrap();
                if float_array.is_null(row) {
                    serde_json::Value::Null
                } else {
                    serde_json::json!(float_array.value(row))
                }
            },
            DataType::Float64 => {
                let float_array = column.as_any().downcast_ref::<array::Float64Array>().unwrap();
                if float_array.is_null(row) {
                    serde_json::Value::Null
                } else {
                    serde_json::json!(float_array.value(row))
                }
            },
            DataType::Boolean => {
                let bool_array = column.as_any().downcast_ref::<array::BooleanArray>().unwrap();
                if bool_array.is_null(row) {
                    serde_json::Value::Null
                } else {
                    serde_json::json!(bool_array.value(row))
                }
            },
            DataType::Utf8 => {
                let str_array = column.as_any().downcast_ref::<array::StringArray>().unwrap();
                if str_array.is_null(row) {
                    serde_json::Value::Null
                } else {
                    serde_json::json!(str_array.value(row))
                }
            },
            DataType::Struct(_) => {
                let struct_array = column.as_any().downcast_ref::<array::StructArray>().unwrap();
                if struct_array.is_null(row) {
                    serde_json::Value::Null
                } else {
                    serde_json::json!(serialize_struct_to_json(fields, struct_array, row))
                }
            },
            _ => unimplemented!(
                "Struct does not implement data type mapping for {}",
                field.data_type()
            ),
        };

        json_object.insert(field_name.to_string(), field_value);

    }

    Value::Object(json_object).to_string()

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
        DataType::Utf8 | DataType::LargeUtf8 => ColumnType::Text,
        DataType::Boolean => ColumnType::Boolean,
        #[allow(clippy::cast_sign_loss)] // This is safe because scale will never be negative
        DataType::Decimal128(p, s) => ColumnType::Decimal(Some((u32::from(*p), *s as u32))),
        DataType::Timestamp(_unit, _time_zone) => ColumnType::Timestamp,
        DataType::Date32 | DataType::Date64 => ColumnType::Date,
        DataType::Time64(_unit) | DataType::Time32(_unit) => ColumnType::Time,
        DataType::List(list_type) => {
            ColumnType::Array(map_data_type_to_column_type(list_type.data_type()).into())
        }
        DataType::Struct(_) => ColumnType::Json,
        DataType::Binary => ColumnType::Binary(BlobSize::Blob(None)),

        // Add more mappings here as needed
        _ => unimplemented!("Data type mapping not implemented for {:?}", data_type),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use arrow::datatypes::{DataType, Field, Int32Type, Schema};

    #[test]
    fn test_basic_table_creation() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("age", DataType::Int32, true),
        ]);
        let sql = CreateTableBuilder::new(SchemaRef::new(schema), "users").build_postgres();

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

        let sql = InsertBuilder::new("users", record_batches)
            .build_postgres(None)
            .expect("Failed to build insert statement");
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
            .build_postgres();

        assert_eq!(sql, "CREATE TABLE IF NOT EXISTS \"users\" ( \"id\" integer NOT NULL, \"id2\" integer NOT NULL, \"name\" text NOT NULL, \"age\" integer, PRIMARY KEY (\"id\", \"id2\") )");
    }

    #[test]
    fn test_table_insertion_with_list() {
        let schema1 = Schema::new(vec![Field::new(
            "list",
            DataType::List(Field::new("item", DataType::Int32, true).into()),
            true,
        )]);
        let list_array = array::ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(1), Some(2), Some(3)]),
            Some(vec![Some(4), Some(5), Some(6)]),
            Some(vec![Some(7), Some(8), Some(9)]),
        ]);

        let batch = RecordBatch::try_new(Arc::new(schema1.clone()), vec![Arc::new(list_array)])
            .expect("Unable to build record batch");

        let sql = InsertBuilder::new("arrays", vec![batch])
            .build_postgres(None)
            .expect("Failed to build insert statement");
        assert_eq!(
            sql,
            "INSERT INTO \"arrays\" (\"list\") VALUES (CAST(ARRAY [1,2,3] AS int4[])), (CAST(ARRAY [4,5,6] AS int4[])), (CAST(ARRAY [7,8,9] AS int4[]))"
        );
    }

    #[test]
    fn test_create_index() {
        let sql = IndexBuilder::new("users", vec!["id", "name"]).build_postgres();
        assert_eq!(
            sql,
            r#"CREATE INDEX IF NOT EXISTS "i_users_id_name" ON "users" ("id", "name")"#
        );
    }

    #[test]
    fn test_create_unique_index() {
        let sql = IndexBuilder::new("users", vec!["id", "name"])
            .unique()
            .build_postgres();
        assert_eq!(
            sql,
            r#"CREATE UNIQUE INDEX IF NOT EXISTS "i_users_id_name" ON "users" ("id", "name")"#
        );
    }
}
