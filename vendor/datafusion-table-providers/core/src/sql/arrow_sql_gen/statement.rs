use bigdecimal::BigDecimal;
use chrono::{DateTime, Offset, TimeZone};
use datafusion::arrow::{
    array::{
        array, timezone::Tz, Array, ArrayRef, BooleanArray, Float32Array, Float64Array, Int16Array,
        Int32Array, Int64Array, Int8Array, LargeStringArray, RecordBatch, StringArray,
        StringViewArray, StructArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
    },
    datatypes::{DataType, Field, Fields, IntervalUnit, Schema, SchemaRef, TimeUnit},
    util::display::array_value_to_string,
};
use datafusion::sql::TableReference;
use num_bigint::BigInt;
use sea_query::{
    Alias, ColumnDef, ColumnType, Expr, GenericBuilder, Index, InsertStatement, IntoIden,
    IntoIndexColumn, Keyword, MysqlQueryBuilder, OnConflict, PostgresQueryBuilder, Query,
    QueryBuilder, SeaRc, SimpleExpr, SqliteQueryBuilder, Table, TableRef,
};
use snafu::Snafu;
use std::{str::FromStr, sync::Arc};
use time::{OffsetDateTime, PrimitiveDateTime};

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
    temporary: bool,
}

impl CreateTableBuilder {
    #[must_use]
    pub fn new(schema: SchemaRef, table_name: &str) -> Self {
        Self {
            schema,
            table_name: table_name.to_string(),
            primary_keys: Vec::new(),
            temporary: false,
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
    /// Set whether the table is temporary or not.
    pub fn temporary(mut self, temporary: bool) -> Self {
        self.temporary = temporary;
        self
    }

    #[must_use]
    #[cfg(feature = "postgres")]
    pub fn build_postgres(self) -> Vec<String> {
        use crate::sql::arrow_sql_gen::postgres::{
            builder::TypeBuilder, get_postgres_composite_type_name,
            map_data_type_to_column_type_postgres,
        };
        let schema = Arc::clone(&self.schema);
        let table_name = self.table_name.clone();
        let main_table_creation =
            self.build(PostgresQueryBuilder, &|f: &Arc<Field>| -> ColumnType {
                map_data_type_to_column_type_postgres(f.data_type(), &table_name, f.name())
            });

        // Postgres supports composite types (i.e. Structs) but needs to have the type defined first
        // https://www.postgresql.org/docs/current/rowtypes.html
        let mut creation_stmts = Vec::new();
        for field in schema.fields() {
            let DataType::Struct(struct_inner_fields) = field.data_type() else {
                continue;
            };
            let type_builder = TypeBuilder::new(
                get_postgres_composite_type_name(&table_name, field.name()),
                struct_inner_fields,
            );
            creation_stmts.push(type_builder.build());
        }

        creation_stmts.push(main_table_creation);
        creation_stmts
    }

    #[must_use]
    pub fn build_sqlite(self) -> String {
        self.build(SqliteQueryBuilder, &|f: &Arc<Field>| -> ColumnType {
            // Sqlite does not natively support Arrays, Structs, etc
            // so we use JSON column type for List, FixedSizeList, LargeList, Struct, etc
            if f.data_type().is_nested() {
                return ColumnType::JsonBinary;
            }

            // SQLite stores decimals as TEXT strings (via BigDecimal::to_string()).
            // Using TEXT column type avoids sea-query's precision limit of 16 for decimals.
            // The decimal.c extension (enabled via bundled-decimal feature) provides
            // exact arithmetic operations on these text-stored decimal values.
            // https://sqlite.org/floatingpoint.html
            match f.data_type() {
                DataType::Decimal32(_, _)
                | DataType::Decimal64(_, _)
                | DataType::Decimal128(_, _)
                | DataType::Decimal256(_, _) => ColumnType::Text,
                _ => map_data_type_to_column_type(f.data_type()),
            }
        })
    }

    #[must_use]
    pub fn build_mysql(self) -> String {
        self.build(MysqlQueryBuilder, &|f: &Arc<Field>| -> ColumnType {
            // MySQL does not natively support Arrays, Structs, etc
            // so we use JSON column type for List, FixedSizeList, LargeList, Struct, etc
            if f.data_type().is_nested() {
                return ColumnType::JsonBinary;
            }
            map_data_type_to_column_type(f.data_type())
        })
    }

    #[must_use]
    fn build<T: GenericBuilder>(
        self,
        query_builder: T,
        map_data_type_to_column_type_fn: &dyn Fn(&Arc<Field>) -> ColumnType,
    ) -> String {
        let mut create_stmt = Table::create();
        create_stmt
            .table(Alias::new(self.table_name.clone()))
            .if_not_exists();

        for field in self.schema.fields() {
            let column_type = map_data_type_to_column_type_fn(field);
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

        if self.temporary {
            create_stmt.temporary();
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
    table: TableReference,
    record_batches: Vec<RecordBatch>,
}

#[allow(unused_variables)]
pub fn use_json_insert_for_type<T: QueryBuilder + 'static>(
    data_type: &DataType,
    query_builder: &T,
) -> bool {
    #[cfg(feature = "sqlite")]
    {
        use std::any::Any;
        let any_builder = query_builder as &dyn Any;
        if any_builder.is::<SqliteQueryBuilder>() {
            return data_type.is_nested();
        }
    }
    #[cfg(feature = "mysql")]
    {
        use std::any::Any;
        let any_builder = query_builder as &dyn Any;
        if any_builder.is::<MysqlQueryBuilder>() {
            return data_type.is_nested();
        }
    }
    false
}

impl InsertBuilder {
    #[must_use]
    pub fn new(table: &TableReference, record_batches: Vec<RecordBatch>) -> Self {
        Self {
            table: table.clone(),
            record_batches,
        }
    }

    /// Create an Insert statement from a `RecordBatch`.
    ///
    /// # Errors
    ///
    /// Returns an error if a column's data type is not supported, or its conversion failed.
    #[allow(clippy::too_many_lines)]
    pub fn construct_insert_stmt<T: QueryBuilder + 'static>(
        &self,
        insert_stmt: &mut InsertStatement,
        record_batch: &RecordBatch,
        query_builder: &T,
    ) -> Result<()> {
        for row in 0..record_batch.num_rows() {
            let mut row_values: Vec<SimpleExpr> = vec![];
            for col in 0..record_batch.num_columns() {
                let column = record_batch.column(col);
                let column_data_type = column.data_type();

                match column_data_type {
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
                    DataType::Utf8View => push_value!(row_values, column, row, StringViewArray),
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
                    DataType::Decimal256(_, scale) => {
                        let array = column.as_any().downcast_ref::<array::Decimal256Array>();
                        if let Some(valid_array) = array {
                            if valid_array.is_null(row) {
                                row_values.push(Keyword::Null.into());
                                continue;
                            }

                            let bigint =
                                BigInt::from_signed_bytes_le(&valid_array.value(row).to_le_bytes());

                            row_values.push(BigDecimal::new(bigint, i64::from(*scale)).into());
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
                                    valid_array.value(row) / 1000,
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
                    DataType::Duration(time_unit) => match time_unit {
                        TimeUnit::Second => {
                            push_value!(row_values, column, row, DurationSecondArray);
                        }
                        TimeUnit::Microsecond => {
                            push_value!(row_values, column, row, DurationMicrosecondArray);
                        }
                        TimeUnit::Millisecond => {
                            push_value!(row_values, column, row, DurationMillisecondArray);
                        }
                        TimeUnit::Nanosecond => {
                            push_value!(row_values, column, row, DurationNanosecondArray);
                        }
                    },
                    DataType::Time32(time_unit) => match time_unit {
                        TimeUnit::Millisecond => {
                            let array = column
                                .as_any()
                                .downcast_ref::<array::Time32MillisecondArray>();
                            if let Some(valid_array) = array {
                                if valid_array.is_null(row) {
                                    row_values.push(Keyword::Null.into());
                                    continue;
                                }

                                let (h, m, s, micro) =
                                    match OffsetDateTime::from_unix_timestamp_nanos(
                                        i128::from(valid_array.value(row)) * 1_000_000,
                                    ) {
                                        Ok(timestamp) => timestamp.to_hms_micro(),
                                        Err(e) => {
                                            return Result::Err(
                                                Error::FailedToCreateInsertStatement {
                                                    source: Box::new(e),
                                                },
                                            )
                                        }
                                    };

                                let time = match time::Time::from_hms_micro(h, m, s, micro) {
                                    Ok(value) => value,
                                    Err(e) => {
                                        return Result::Err(Error::FailedToCreateInsertStatement {
                                            source: Box::new(e),
                                        })
                                    }
                                };

                                row_values.push(time.into());
                            }
                        }
                        TimeUnit::Second => {
                            let array = column.as_any().downcast_ref::<array::Time32SecondArray>();
                            if let Some(valid_array) = array {
                                if valid_array.is_null(row) {
                                    row_values.push(Keyword::Null.into());
                                    continue;
                                }

                                let (h, m, s) = match OffsetDateTime::from_unix_timestamp(
                                    i64::from(valid_array.value(row)),
                                ) {
                                    Ok(timestamp) => timestamp.to_hms(),
                                    Err(e) => {
                                        return Result::Err(Error::FailedToCreateInsertStatement {
                                            source: Box::new(e),
                                        })
                                    }
                                };

                                let time = match time::Time::from_hms(h, m, s) {
                                    Ok(value) => value,
                                    Err(e) => {
                                        return Result::Err(Error::FailedToCreateInsertStatement {
                                            source: Box::new(e),
                                        })
                                    }
                                };

                                row_values.push(time.into());
                            }
                        }
                        _ => unreachable!(),
                    },
                    DataType::Time64(time_unit) => match time_unit {
                        TimeUnit::Nanosecond => {
                            let array = column
                                .as_any()
                                .downcast_ref::<array::Time64NanosecondArray>();
                            if let Some(valid_array) = array {
                                if valid_array.is_null(row) {
                                    row_values.push(Keyword::Null.into());
                                    continue;
                                }
                                let (h, m, s, nano) =
                                    match OffsetDateTime::from_unix_timestamp_nanos(i128::from(
                                        valid_array.value(row),
                                    )) {
                                        Ok(timestamp) => timestamp.to_hms_nano(),
                                        Err(e) => {
                                            return Result::Err(
                                                Error::FailedToCreateInsertStatement {
                                                    source: Box::new(e),
                                                },
                                            )
                                        }
                                    };

                                let time = match time::Time::from_hms_nano(h, m, s, nano) {
                                    Ok(value) => value,
                                    Err(e) => {
                                        return Result::Err(Error::FailedToCreateInsertStatement {
                                            source: Box::new(e),
                                        })
                                    }
                                };

                                row_values.push(time.into());
                            }
                        }
                        TimeUnit::Microsecond => {
                            let array = column
                                .as_any()
                                .downcast_ref::<array::Time64MicrosecondArray>();
                            if let Some(valid_array) = array {
                                if valid_array.is_null(row) {
                                    row_values.push(Keyword::Null.into());
                                    continue;
                                }

                                let (h, m, s, micro) =
                                    match OffsetDateTime::from_unix_timestamp_nanos(
                                        i128::from(valid_array.value(row)) * 1_000,
                                    ) {
                                        Ok(timestamp) => timestamp.to_hms_micro(),
                                        Err(e) => {
                                            return Result::Err(
                                                Error::FailedToCreateInsertStatement {
                                                    source: Box::new(e),
                                                },
                                            )
                                        }
                                    };

                                let time = match time::Time::from_hms_micro(h, m, s, micro) {
                                    Ok(value) => value,
                                    Err(e) => {
                                        return Result::Err(Error::FailedToCreateInsertStatement {
                                            source: Box::new(e),
                                        })
                                    }
                                };

                                row_values.push(time.into());
                            }
                        }
                        _ => unreachable!(),
                    },
                    DataType::Timestamp(TimeUnit::Second, timezone) => {
                        let array = column
                            .as_any()
                            .downcast_ref::<array::TimestampSecondArray>();

                        if let Some(valid_array) = array {
                            if valid_array.is_null(row) {
                                row_values.push(Keyword::Null.into());
                                continue;
                            }
                            if let Some(timezone) = timezone {
                                let utc_time = DateTime::from_timestamp_nanos(
                                    valid_array.value(row) * 1_000_000_000,
                                )
                                .to_utc();
                                let timezone = Tz::from_str(timezone).map_err(|_| {
                                    Error::FailedToCreateInsertStatement {
                                        source: "Unable to parse arrow timezone information".into(),
                                    }
                                })?;
                                let offset = timezone
                                    .offset_from_utc_datetime(&utc_time.naive_utc())
                                    .fix();
                                let time_with_offset = utc_time.with_timezone(&offset);
                                row_values.push(time_with_offset.into());
                            } else {
                                insert_timestamp_into_row_values(
                                    OffsetDateTime::from_unix_timestamp(valid_array.value(row)),
                                    &mut row_values,
                                )?;
                            }
                        }
                    }
                    DataType::Timestamp(TimeUnit::Millisecond, timezone) => {
                        let array = column
                            .as_any()
                            .downcast_ref::<array::TimestampMillisecondArray>();

                        if let Some(valid_array) = array {
                            if valid_array.is_null(row) {
                                row_values.push(Keyword::Null.into());
                                continue;
                            }
                            if let Some(timezone) = timezone {
                                let utc_time = DateTime::from_timestamp_nanos(
                                    valid_array.value(row) * 1_000_000,
                                )
                                .to_utc();
                                let timezone = Tz::from_str(timezone).map_err(|_| {
                                    Error::FailedToCreateInsertStatement {
                                        source: "Unable to parse arrow timezone information".into(),
                                    }
                                })?;
                                let offset = timezone
                                    .offset_from_utc_datetime(&utc_time.naive_utc())
                                    .fix();
                                let time_with_offset = utc_time.with_timezone(&offset);
                                row_values.push(time_with_offset.into());
                            } else {
                                insert_timestamp_into_row_values(
                                    OffsetDateTime::from_unix_timestamp_nanos(
                                        i128::from(valid_array.value(row)) * 1_000_000,
                                    ),
                                    &mut row_values,
                                )?;
                            }
                        }
                    }
                    DataType::Timestamp(TimeUnit::Microsecond, timezone) => {
                        let array = column
                            .as_any()
                            .downcast_ref::<array::TimestampMicrosecondArray>();

                        if let Some(valid_array) = array {
                            if valid_array.is_null(row) {
                                row_values.push(Keyword::Null.into());
                                continue;
                            }
                            if let Some(timezone) = timezone {
                                let utc_time =
                                    DateTime::from_timestamp_nanos(valid_array.value(row) * 1_000)
                                        .to_utc();
                                let timezone = Tz::from_str(timezone).map_err(|_| {
                                    Error::FailedToCreateInsertStatement {
                                        source: "Unable to parse arrow timezone information".into(),
                                    }
                                })?;
                                let offset = timezone
                                    .offset_from_utc_datetime(&utc_time.naive_utc())
                                    .fix();
                                let time_with_offset = utc_time.with_timezone(&offset);
                                row_values.push(time_with_offset.into());
                            } else {
                                insert_timestamp_into_row_values(
                                    OffsetDateTime::from_unix_timestamp_nanos(
                                        i128::from(valid_array.value(row)) * 1_000,
                                    ),
                                    &mut row_values,
                                )?;
                            }
                        }
                    }
                    DataType::Timestamp(TimeUnit::Nanosecond, timezone) => {
                        let array = column
                            .as_any()
                            .downcast_ref::<array::TimestampNanosecondArray>();

                        if let Some(valid_array) = array {
                            if valid_array.is_null(row) {
                                row_values.push(Keyword::Null.into());
                                continue;
                            }
                            if let Some(timezone) = timezone {
                                let utc_time =
                                    DateTime::from_timestamp_nanos(valid_array.value(row)).to_utc();
                                let timezone = Tz::from_str(timezone).map_err(|_| {
                                    Error::FailedToCreateInsertStatement {
                                        source: "Unable to parse arrow timezone information".into(),
                                    }
                                })?;
                                let offset = timezone
                                    .offset_from_utc_datetime(&utc_time.naive_utc())
                                    .fix();
                                let time_with_offset = utc_time.with_timezone(&offset);
                                row_values.push(time_with_offset.into());
                            } else {
                                insert_timestamp_into_row_values(
                                    OffsetDateTime::from_unix_timestamp_nanos(i128::from(
                                        valid_array.value(row),
                                    )),
                                    &mut row_values,
                                )?;
                            }
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

                            if use_json_insert_for_type(column_data_type, query_builder) {
                                insert_list_into_row_values_json(
                                    list_array,
                                    list_type,
                                    &mut row_values,
                                )?;
                            } else {
                                insert_list_into_row_values(list_array, list_type, &mut row_values);
                            }
                        }
                    }
                    DataType::LargeList(list_type) => {
                        let array = column.as_any().downcast_ref::<array::LargeListArray>();
                        if let Some(valid_array) = array {
                            if valid_array.is_null(row) {
                                row_values.push(Keyword::Null.into());
                                continue;
                            }
                            let list_array = valid_array.value(row);

                            if use_json_insert_for_type(column_data_type, query_builder) {
                                insert_list_into_row_values_json(
                                    list_array,
                                    list_type,
                                    &mut row_values,
                                )?;
                            } else {
                                insert_list_into_row_values(list_array, list_type, &mut row_values);
                            }
                        }
                    }
                    DataType::FixedSizeList(list_type, _) => {
                        let array = column.as_any().downcast_ref::<array::FixedSizeListArray>();
                        if let Some(valid_array) = array {
                            if valid_array.is_null(row) {
                                row_values.push(Keyword::Null.into());
                                continue;
                            }
                            let list_array = valid_array.value(row);

                            if use_json_insert_for_type(column_data_type, query_builder) {
                                insert_list_into_row_values_json(
                                    list_array,
                                    list_type,
                                    &mut row_values,
                                )?;
                            } else {
                                insert_list_into_row_values(list_array, list_type, &mut row_values);
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
                    DataType::LargeBinary => {
                        let array = column.as_any().downcast_ref::<array::LargeBinaryArray>();

                        if let Some(valid_array) = array {
                            if valid_array.is_null(row) {
                                row_values.push(Keyword::Null.into());
                                continue;
                            }

                            row_values.push(valid_array.value(row).into());
                        }
                    }
                    DataType::FixedSizeBinary(_) => {
                        let array = column
                            .as_any()
                            .downcast_ref::<array::FixedSizeBinaryArray>();

                        if let Some(valid_array) = array {
                            if valid_array.is_null(row) {
                                row_values.push(Keyword::Null.into());
                                continue;
                            }

                            row_values.push(valid_array.value(row).into());
                        }
                    }
                    DataType::BinaryView => {
                        let array = column.as_any().downcast_ref::<array::BinaryViewArray>();

                        if let Some(valid_array) = array {
                            if valid_array.is_null(row) {
                                row_values.push(Keyword::Null.into());
                                continue;
                            }

                            row_values.push(valid_array.value(row).into());
                        }
                    }
                    DataType::Interval(interval_unit) => match interval_unit {
                        IntervalUnit::DayTime => {
                            let array = column
                                .as_any()
                                .downcast_ref::<array::IntervalDayTimeArray>();

                            if let Some(valid_array) = array {
                                if valid_array.is_null(row) {
                                    row_values.push(Keyword::Null.into());
                                    continue;
                                }

                                let interval_str =
                                    if let Ok(str) = array_value_to_string(valid_array, row) {
                                        str
                                    } else {
                                        let days = valid_array.value(row).days;
                                        let milliseconds = valid_array.value(row).milliseconds;
                                        format!("{days} days {milliseconds} milliseconds")
                                    };

                                row_values.push(interval_str.into());
                            }
                        }
                        IntervalUnit::YearMonth => {
                            let array = column
                                .as_any()
                                .downcast_ref::<array::IntervalYearMonthArray>();

                            if let Some(valid_array) = array {
                                if valid_array.is_null(row) {
                                    row_values.push(Keyword::Null.into());
                                    continue;
                                }

                                let interval_str =
                                    if let Ok(str) = array_value_to_string(valid_array, row) {
                                        str
                                    } else {
                                        let months = valid_array.value(row);
                                        format!("{months} months")
                                    };

                                row_values.push(interval_str.into());
                            }
                        }
                        // The smallest unit in Postgres for interval is microsecond
                        // Cast with loss of precision in nano second
                        IntervalUnit::MonthDayNano => {
                            let array = column
                                .as_any()
                                .downcast_ref::<array::IntervalMonthDayNanoArray>();

                            if let Some(valid_array) = array {
                                if valid_array.is_null(row) {
                                    row_values.push(Keyword::Null.into());
                                    continue;
                                }

                                let interval_str =
                                    if let Ok(str) = array_value_to_string(valid_array, row) {
                                        str
                                    } else {
                                        let months = valid_array.value(row).months;
                                        let days = valid_array.value(row).days;
                                        let nanoseconds = valid_array.value(row).nanoseconds;
                                        let micros = nanoseconds / 1_000;
                                        format!("{months} months {days} days {micros} microseconds")
                                    };

                                row_values.push(interval_str.into());
                            }
                        }
                    },
                    DataType::Struct(fields) => {
                        let array = column.as_any().downcast_ref::<array::StructArray>();

                        if let Some(valid_array) = array {
                            if valid_array.is_null(row) {
                                row_values.push(Keyword::Null.into());
                                continue;
                            }

                            if use_json_insert_for_type(column_data_type, query_builder) {
                                insert_struct_into_row_values_json(
                                    fields,
                                    valid_array,
                                    row,
                                    &mut row_values,
                                )?;
                                continue;
                            }

                            let mut param_values: Vec<SimpleExpr> = vec![];

                            for col in valid_array.columns() {
                                match col.data_type() {
                                    DataType::Int8 => {
                                        let int_array =
                                            col.as_any().downcast_ref::<array::Int8Array>();

                                        if let Some(valid_int_array) = int_array {
                                            param_values.push(valid_int_array.value(row).into());
                                        }
                                    }
                                    DataType::Int16 => {
                                        let int_array =
                                            col.as_any().downcast_ref::<array::Int16Array>();

                                        if let Some(valid_int_array) = int_array {
                                            param_values.push(valid_int_array.value(row).into());
                                        }
                                    }
                                    DataType::Int32 => {
                                        let int_array =
                                            col.as_any().downcast_ref::<array::Int32Array>();

                                        if let Some(valid_int_array) = int_array {
                                            param_values.push(valid_int_array.value(row).into());
                                        }
                                    }
                                    DataType::Int64 => {
                                        let int_array =
                                            col.as_any().downcast_ref::<array::Int64Array>();

                                        if let Some(valid_int_array) = int_array {
                                            param_values.push(valid_int_array.value(row).into());
                                        }
                                    }
                                    DataType::UInt8 => {
                                        let int_array =
                                            col.as_any().downcast_ref::<array::UInt8Array>();

                                        if let Some(valid_int_array) = int_array {
                                            param_values.push(valid_int_array.value(row).into());
                                        }
                                    }
                                    DataType::UInt16 => {
                                        let int_array =
                                            col.as_any().downcast_ref::<array::UInt16Array>();

                                        if let Some(valid_int_array) = int_array {
                                            param_values.push(valid_int_array.value(row).into());
                                        }
                                    }
                                    DataType::UInt32 => {
                                        let int_array =
                                            col.as_any().downcast_ref::<array::UInt32Array>();

                                        if let Some(valid_int_array) = int_array {
                                            param_values.push(valid_int_array.value(row).into());
                                        }
                                    }
                                    DataType::UInt64 => {
                                        let int_array =
                                            col.as_any().downcast_ref::<array::UInt64Array>();

                                        if let Some(valid_int_array) = int_array {
                                            param_values.push(valid_int_array.value(row).into());
                                        }
                                    }
                                    DataType::Float32 => {
                                        let float_array =
                                            col.as_any().downcast_ref::<array::Float32Array>();

                                        if let Some(valid_float_array) = float_array {
                                            param_values.push(valid_float_array.value(row).into());
                                        }
                                    }
                                    DataType::Float64 => {
                                        let float_array =
                                            col.as_any().downcast_ref::<array::Float64Array>();

                                        if let Some(valid_float_array) = float_array {
                                            param_values.push(valid_float_array.value(row).into());
                                        }
                                    }
                                    DataType::Utf8 => {
                                        let string_array =
                                            col.as_any().downcast_ref::<array::StringArray>();

                                        if let Some(valid_string_array) = string_array {
                                            param_values.push(valid_string_array.value(row).into());
                                        }
                                    }
                                    DataType::Null => {
                                        param_values.push(Keyword::Null.into());
                                    }
                                    DataType::Boolean => {
                                        let bool_array =
                                            col.as_any().downcast_ref::<array::BooleanArray>();

                                        if let Some(valid_bool_array) = bool_array {
                                            param_values.push(valid_bool_array.value(row).into());
                                        }
                                    }
                                    DataType::Binary => {
                                        let binary_array =
                                            col.as_any().downcast_ref::<array::BinaryArray>();

                                        if let Some(valid_binary_array) = binary_array {
                                            param_values.push(valid_binary_array.value(row).into());
                                        }
                                    }
                                    DataType::FixedSizeBinary(_) => {
                                        let binary_array = col
                                            .as_any()
                                            .downcast_ref::<array::FixedSizeBinaryArray>();

                                        if let Some(valid_binary_array) = binary_array {
                                            param_values.push(valid_binary_array.value(row).into());
                                        }
                                    }
                                    DataType::LargeBinary => {
                                        let binary_array =
                                            col.as_any().downcast_ref::<array::LargeBinaryArray>();

                                        if let Some(valid_binary_array) = binary_array {
                                            param_values.push(valid_binary_array.value(row).into());
                                        }
                                    }
                                    DataType::LargeUtf8 => {
                                        let string_array =
                                            col.as_any().downcast_ref::<array::LargeStringArray>();

                                        if let Some(valid_string_array) = string_array {
                                            param_values.push(valid_string_array.value(row).into());
                                        }
                                    }
                                    DataType::Utf8View => {
                                        let view_array =
                                            col.as_any().downcast_ref::<array::StringViewArray>();

                                        if let Some(valid_view_array) = view_array {
                                            param_values.push(valid_view_array.value(row).into());
                                        }
                                    }
                                    DataType::BinaryView => {
                                        let view_array =
                                            col.as_any().downcast_ref::<array::BinaryViewArray>();

                                        if let Some(valid_view_array) = view_array {
                                            param_values.push(valid_view_array.value(row).into());
                                        }
                                    }
                                    DataType::Float16
                                    | DataType::Timestamp(_, _)
                                    | DataType::Date32
                                    | DataType::Date64
                                    | DataType::Time32(_)
                                    | DataType::Time64(_)
                                    | DataType::Duration(_)
                                    | DataType::Interval(_)
                                    | DataType::List(_)
                                    | DataType::ListView(_)
                                    | DataType::FixedSizeList(_, _)
                                    | DataType::LargeList(_)
                                    | DataType::LargeListView(_)
                                    | DataType::Struct(_)
                                    | DataType::Union(_, _)
                                    | DataType::Dictionary(_, _)
                                    | DataType::Map(_, _)
                                    | DataType::RunEndEncoded(_, _)
                                    | DataType::Decimal32(_, _)
                                    | DataType::Decimal64(_, _)
                                    | DataType::Decimal128(_, _)
                                    | DataType::Decimal256(_, _) => {
                                        unimplemented!(
                                            "Data type mapping not implemented for Struct of {}",
                                            col.data_type()
                                        )
                                    }
                                }
                            }

                            let mut params_vec = Vec::new();
                            for param_value in &param_values {
                                let mut params_str = String::new();
                                query_builder.prepare_simple_expr(param_value, &mut params_str);
                                params_vec.push(params_str);
                            }

                            let params = params_vec.join(", ");
                            row_values.push(Expr::cust(format!("ROW({params})")));
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
    pub fn build<T: GenericBuilder + 'static>(
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
            .into_table(table_reference_to_sea_table_ref(&self.table))
            .columns(columns)
            .to_owned();

        for record_batch in &self.record_batches {
            self.construct_insert_stmt(&mut insert_stmt, record_batch, &query_builder)?;
        }
        if let Some(on_conflict) = on_conflict {
            insert_stmt.on_conflict(on_conflict);
        }
        Ok(insert_stmt.to_string(query_builder))
    }
}

fn table_reference_to_sea_table_ref(table: &TableReference) -> TableRef {
    match table {
        TableReference::Bare { table } => {
            TableRef::Table(SeaRc::new(Alias::new(table.to_string())))
        }
        TableReference::Partial { schema, table } => TableRef::SchemaTable(
            SeaRc::new(Alias::new(schema.to_string())),
            SeaRc::new(Alias::new(table.to_string())),
        ),
        TableReference::Full {
            catalog,
            schema,
            table,
        } => TableRef::DatabaseSchemaTable(
            SeaRc::new(Alias::new(catalog.to_string())),
            SeaRc::new(Alias::new(schema.to_string())),
            SeaRc::new(Alias::new(table.to_string())),
        ),
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

#[allow(clippy::needless_pass_by_value)]
fn insert_list_into_row_values(
    list_array: Arc<dyn Array>,
    list_type: &Arc<Field>,
    row_values: &mut Vec<SimpleExpr>,
) {
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
                let int_array = list_array.as_any().downcast_ref::<array::StringArray>();
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
        DataType::Utf8View => {
            let mut list_values: Vec<String> = vec![];
            for i in 0..list_array.len() {
                let view_array = list_array.as_any().downcast_ref::<array::StringViewArray>();
                if let Some(valid_view_array) = view_array {
                    list_values.push(valid_view_array.value(i).to_string());
                }
            }
            let expr: SimpleExpr = list_values.into();
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
        DataType::Binary => {
            let mut list_values: Vec<Vec<u8>> = Vec::new();
            for i in 0..list_array.len() {
                let temp_array = list_array.as_any().downcast_ref::<array::BinaryArray>();
                if let Some(valid_array) = temp_array {
                    list_values.push(valid_array.value(i).to_vec());
                }
            }
            let expr: SimpleExpr = list_values.into();
            // We must cast here in case the array is empty which SeaQuery does not handle.
            row_values.push(expr.cast_as(Alias::new("bytea[]")));
        }
        _ => unimplemented!(
            "Data type mapping not implemented for {}",
            list_type.data_type()
        ),
    }
}

#[allow(clippy::cast_sign_loss)]
pub(crate) fn map_data_type_to_column_type(data_type: &DataType) -> ColumnType {
    match data_type {
        DataType::Int8 => ColumnType::TinyInteger,
        DataType::Int16 => ColumnType::SmallInteger,
        DataType::Int32 => ColumnType::Integer,
        DataType::Int64 | DataType::Duration(_) => ColumnType::BigInteger,
        DataType::UInt8 => ColumnType::TinyUnsigned,
        DataType::UInt16 => ColumnType::SmallUnsigned,
        DataType::UInt32 => ColumnType::Unsigned,
        DataType::UInt64 => ColumnType::BigUnsigned,
        DataType::Float32 => ColumnType::Float,
        DataType::Float64 => ColumnType::Double,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => ColumnType::Text,
        DataType::Boolean => ColumnType::Boolean,
        #[allow(clippy::cast_sign_loss)] // This is safe because scale will never be negative
        DataType::Decimal128(p, s) | DataType::Decimal256(p, s) => {
            ColumnType::Decimal(Some((u32::from(*p), *s as u32)))
        }
        DataType::Timestamp(_unit, time_zone) => {
            if time_zone.is_some() {
                return ColumnType::TimestampWithTimeZone;
            }
            ColumnType::Timestamp
        }
        DataType::Date32 | DataType::Date64 => ColumnType::Date,
        DataType::Time64(_unit) | DataType::Time32(_unit) => ColumnType::Time,
        DataType::List(list_type)
        | DataType::LargeList(list_type)
        | DataType::FixedSizeList(list_type, _) => {
            ColumnType::Array(map_data_type_to_column_type(list_type.data_type()).into())
        }
        // Originally mapped to VarBinary type, corresponding to MySQL's varbinary, which has a maximum length of 65535.
        // This caused the error: "Row size too large. The maximum row size for the used table type, not counting BLOBs, is 65535.
        // This includes storage overhead, check the manual. You have to change some columns to TEXT or BLOBs."
        // Changing to Blob fixes this issue. This change does not affect Postgres, and for Sqlite, the mapping type changes from varbinary_blob to blob.
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => ColumnType::Blob,
        DataType::FixedSizeBinary(num_bytes) => ColumnType::Binary(num_bytes.to_owned() as u32),
        DataType::Interval(_) => ColumnType::Interval(None, None),
        // Add more mappings here as needed
        _ => unimplemented!("Data type mapping not implemented for {:?}", data_type),
    }
}

macro_rules! serialize_list_values {
    ($data_type:expr, $list_array:expr, $array_type:ty, $vec_type:ty) => {{
        let mut list_values: Vec<$vec_type> = vec![];
        if let Some(array) = $list_array.as_any().downcast_ref::<$array_type>() {
            for i in 0..array.len() {
                list_values.push(array.value(i).into());
            }
        }

        serde_json::to_string(&list_values).map_err(|e| Error::FailedToCreateInsertStatement {
            source: Box::new(e),
        })?
    }};
}

fn insert_list_into_row_values_json(
    list_array: Arc<dyn Array>,
    list_type: &Arc<Field>,
    row_values: &mut Vec<SimpleExpr>,
) -> Result<()> {
    let data_type = list_type.data_type();

    let json_string: String = match data_type {
        DataType::Int8 => serialize_list_values!(data_type, list_array, Int8Array, i8),
        DataType::Int16 => serialize_list_values!(data_type, list_array, Int16Array, i16),
        DataType::Int32 => serialize_list_values!(data_type, list_array, Int32Array, i32),
        DataType::Int64 => serialize_list_values!(data_type, list_array, Int64Array, i64),
        DataType::UInt8 => serialize_list_values!(data_type, list_array, UInt8Array, u8),
        DataType::UInt16 => serialize_list_values!(data_type, list_array, UInt16Array, u16),
        DataType::UInt32 => serialize_list_values!(data_type, list_array, UInt32Array, u32),
        DataType::UInt64 => serialize_list_values!(data_type, list_array, UInt64Array, u64),
        DataType::Float32 => serialize_list_values!(data_type, list_array, Float32Array, f32),
        DataType::Float64 => serialize_list_values!(data_type, list_array, Float64Array, f64),
        DataType::Utf8 => serialize_list_values!(data_type, list_array, StringArray, String),
        DataType::LargeUtf8 => {
            serialize_list_values!(data_type, list_array, LargeStringArray, String)
        }
        DataType::Utf8View => {
            serialize_list_values!(data_type, list_array, StringViewArray, String)
        }
        DataType::Boolean => serialize_list_values!(data_type, list_array, BooleanArray, bool),
        _ => unimplemented!(
            "List to json conversion is not implemented for {}",
            list_type.data_type()
        ),
    };

    let expr: SimpleExpr = Expr::value(json_string);
    row_values.push(expr);

    Ok(())
}

fn insert_struct_into_row_values_json(
    fields: &Fields,
    array: &StructArray,
    row_index: usize,
    row_values: &mut Vec<SimpleExpr>,
) -> Result<()> {
    // The length of each column in a StructArray is the same as the length of the StructArray itself.
    // The presence of null values does not change the length of the columns (affects the validity bitmap only).
    // Similar to Recordbatch slice: https://github.com/apache/arrow-rs/blob/855666d9e9283c1ef11648762fe92c7c188b68f1/arrow-array/src/record_batch.rs#L375
    let single_row_columns: Vec<ArrayRef> = (0..array.num_columns())
        .map(|i| array.column(i).slice(row_index, 1))
        .collect();

    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields.clone())), single_row_columns)
        .map_err(|e| Error::FailedToCreateInsertStatement {
            source: Box::new(e),
        })?;

    let mut writer = datafusion::arrow::json::LineDelimitedWriter::new(Vec::new());
    writer
        .write(&batch)
        .map_err(|e| Error::FailedToCreateInsertStatement {
            source: Box::new(e),
        })?;
    writer
        .finish()
        .map_err(|e| Error::FailedToCreateInsertStatement {
            source: Box::new(e),
        })?;
    let json_bytes = writer.into_inner();

    let json = String::from_utf8(json_bytes).map_err(|e| Error::FailedToCreateInsertStatement {
        source: Box::new(e),
    })?;

    let expr: SimpleExpr = Expr::value(json);
    row_values.push(expr);

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Int32Type, Schema};

    #[test]
    fn test_basic_table_creation() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("age", DataType::Int32, true),
        ]);
        let sql = CreateTableBuilder::new(SchemaRef::new(schema), "users").build_sqlite();

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

        let sql = InsertBuilder::new(&TableReference::from("users"), record_batches)
            .build_postgres(None)
            .expect("Failed to build insert statement");
        assert_eq!(sql, "INSERT INTO \"users\" (\"id\", \"name\", \"age\") VALUES (1, 'a', 10), (2, 'b', 20), (3, 'c', 30), (1, 'a', 10), (2, 'b', 20), (3, 'c', 30)");
    }

    #[test]
    fn test_table_insertion_with_schema() {
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

        let sql = InsertBuilder::new(&TableReference::from("schema.users"), record_batches)
            .build_postgres(None)
            .expect("Failed to build insert statement");
        assert_eq!(sql, "INSERT INTO \"schema\".\"users\" (\"id\", \"name\", \"age\") VALUES (1, 'a', 10), (2, 'b', 20), (3, 'c', 30), (1, 'a', 10), (2, 'b', 20), (3, 'c', 30)");
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
            .build_sqlite();

        assert_eq!(sql, "CREATE TABLE IF NOT EXISTS \"users\" ( \"id\" integer NOT NULL, \"id2\" integer NOT NULL, \"name\" text NOT NULL, \"age\" integer, PRIMARY KEY (\"id\", \"id2\") )");
    }

    #[test]
    fn test_temporary_table_creation() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]);
        let sql = CreateTableBuilder::new(SchemaRef::new(schema), "users")
            .primary_keys(vec!["id"])
            .temporary(true)
            .build_sqlite();

        assert_eq!(sql, "CREATE TEMPORARY TABLE IF NOT EXISTS \"users\" ( \"id\" integer NOT NULL, \"name\" text NOT NULL, PRIMARY KEY (\"id\") )");
    }

    #[test]
    fn test_sqlite_decimal_mapped_to_text() {
        // Test that all decimals are mapped to TEXT for SQLite to preserve full precision.
        // SQLite stores decimals as TEXT strings via BigDecimal, supporting all decimal precisions
        // including high-precision Decimal128 (up to precision 38) and Decimal256 (up to precision 76).
        let schema = Schema::new(vec![
            Field::new("high_precision", DataType::Decimal128(38, 10), true),
            Field::new("decimal256", DataType::Decimal256(76, 20), true),
            Field::new("normal_decimal", DataType::Decimal128(10, 2), true),
        ]);
        let sql = CreateTableBuilder::new(SchemaRef::new(schema), "decimals").build_sqlite();

        // All decimals should be mapped to "text" for SQLite to preserve full precision
        assert!(
            sql.contains("text"),
            "Decimals should be mapped to text for SQLite, got: {}",
            sql
        );
        assert!(
            !sql.to_lowercase().contains("decimal("),
            "SQL should not contain decimal type for SQLite, got: {}",
            sql
        );
        // Verify the expected output
        assert_eq!(
            sql,
            "CREATE TABLE IF NOT EXISTS \"decimals\" ( \"high_precision\" text, \"decimal256\" text, \"normal_decimal\" text )"
        );
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

        let sql = InsertBuilder::new(&TableReference::from("arrays"), vec![batch])
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
