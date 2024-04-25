use arrow_sql_gen::clickhouse::block_to_arrow;

#[cfg(feature = "clickhouse")]
#[test]
fn test_block_to_arrow() {
    use clickhouse_rs::{types::Decimal, Block};

    let block = Block::new()
        .add_column("int_8", vec![1_i8, 2, 4])
        .add_column("int_16", vec![1_i16, 2, 4])
        .add_column("int_32", vec![1_i32, 2, 4])
        .add_column("int_64", vec![1_i64, 2, 4])
        .add_column("uint_8", vec![1_u8, 2, 4])
        .add_column("uint_16", vec![1_u16, 2, 4])
        .add_column("uint_32", vec![1_u32, 2, 4])
        .add_column("uint_64", vec![1_u64, 2, 4])
        .add_column("float_32", vec![1.0_f32, 2.0, 4.0])
        .add_column("float_64", vec![1.0_f64, 2.0, 4.0])
        .add_column("string", vec!["a", "b", "c"])
        .add_column(
            "uuid",
            vec![
                uuid::Uuid::default(),
                uuid::Uuid::default(),
                uuid::Uuid::default(),
            ],
        )
        .add_column("nullable", vec![Some(1), None, Some(3)])
        .add_column(
            "date",
            vec![
                chrono::NaiveDate::from_ymd_opt(2021, 1, 1),
                chrono::NaiveDate::from_ymd_opt(2021, 1, 2),
                chrono::NaiveDate::from_ymd_opt(2021, 1, 3),
            ],
        )
        .add_column("bool", vec![true, false, true])
        .add_column(
            "decimal",
            vec![
                Decimal::new(123, 2),
                Decimal::new(543, 2),
                Decimal::new(451, 2),
            ],
        );

    let rec = block_to_arrow(&block).expect("Failed to convert block to arrow");

    assert_eq!(rec.num_rows(), 3, "Number of rows mismatch");
    assert_eq!(rec.num_columns(), 16, "Number of columns mismatch");

    let expected_datatypes = vec![
        arrow::datatypes::DataType::Int8,
        arrow::datatypes::DataType::Int16,
        arrow::datatypes::DataType::Int32,
        arrow::datatypes::DataType::Int64,
        arrow::datatypes::DataType::UInt8,
        arrow::datatypes::DataType::UInt16,
        arrow::datatypes::DataType::UInt32,
        arrow::datatypes::DataType::UInt64,
        arrow::datatypes::DataType::Float32,
        arrow::datatypes::DataType::Float64,
        arrow::datatypes::DataType::Utf8,
        arrow::datatypes::DataType::FixedSizeBinary(16),
        arrow::datatypes::DataType::Int32,
        arrow::datatypes::DataType::Date32,
        arrow::datatypes::DataType::Boolean,
        arrow::datatypes::DataType::Decimal128(18, 2),
    ];
    for (index, field) in rec.schema().fields.iter().enumerate() {
        assert_eq!(*field.data_type(), expected_datatypes[index]);
    }
}
