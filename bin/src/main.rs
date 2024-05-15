use std::{sync::Arc};
use arrow::array::{make_array, Array, PrimitiveArray, RecordBatch, TimestampMicrosecondArray, TimestampNanosecondArray};
use arrow::{datatypes::ToByteSlice};
use arrow_sql_gen::statement::{InsertBuilder, CreateTableBuilder};
use arrow::buffer::{Buffer, OffsetBuffer, ScalarBuffer};
use arrow::datatypes::{i256, BooleanType, DataType, Field, Fields, Int16Type, Int32Type, Int8Type};
use arrow::array::{ListArray, RunArray, ArrayData, ArrayRef, IntervalMonthDayNanoArray, Time64NanosecondArray, BinaryArray, BooleanArray, Date32Array, Date64Array, Decimal128Array, Decimal256Array, DictionaryArray, DurationMicrosecondArray, DurationMillisecondArray, DurationNanosecondArray, DurationSecondArray, FixedSizeBinaryArray, FixedSizeListArray, Float16Array, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, IntervalDayTimeArray, IntervalYearMonthArray, LargeBinaryArray, LargeListArray, LargeStringArray, MapArray, StringArray, StructArray, Time32MillisecondArray, Time32SecondArray, Time64MicrosecondArray, TimestampMillisecondArray, TimestampSecondArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array, UnionArray};
use half::f16;
use rusqlite::Connection;
/**
 * CREATE TABLE IF NOT EXISTS `sqllite` (
  `bool` bool NOT NULL, 
  `int8` tinyint NOT NULL, 
  `int16` smallint NOT NULL, 
  `int32` int NOT NULL, 
  `int64` bigint NOT NULL, 
  `uint8` tinyint NOT NULL, 
  `uint16` smallint NOT NULL, 
  `uint32` int NOT NULL, 
  `uint64` bigint NOT NULL, 
  `float32` float NOT NULL, 
  `float64` double NOT NULL, 
  `timestamp_us` timestamp NOT NULL, 
  `string` text NOT NULL, 
  `decimal128` decimal(38, 10) NOT NULL
)

CREATE TABLE IF NOT EXISTS "postgres" (
  "bool" bool NOT NULL, 
  "int8" smallint NOT NULL, 
  "int16" smallint NOT NULL, 
  "int32" integer NOT NULL, 
  "int64" bigint NOT NULL, 
  "uint8" smallint NOT NULL, 
  "uint16" smallint NOT NULL, 
  "uint32" integer NOT NULL, 
  "uint64" bigint NOT NULL, 
  "float32" real NOT NULL, 
  "float64" double precision NOT NULL, 
  "timestamp_us" timestamp NOT NULL, 
  "string" text NOT NULL, 
  "decimal128" decimal(38, 10) NOT NULL, 
  "list_array" integer[] NOT NULL
)

CREATE TABLE IF NOT EXISTS `mysql` (
  `bool` bool NOT NULL, 
  `int8` tinyint NOT NULL, 
  `int16` smallint NOT NULL, 
  `int32` int NOT NULL, 
  `int64` bigint NOT NULL, 
  `uint8` tinyint NOT NULL, 
  `uint16` smallint NOT NULL, 
  `uint32` int NOT NULL, 
  `uint64` bigint NOT NULL, 
  `float32` float NOT NULL, 
  `float64` double NOT NULL, 
  `timestamp_us` timestamp NOT NULL, 
  `string` text NOT NULL, 
  `decimal128` decimal(38, 10) NOT NULL
)


 * 
 */
fn create_map() -> MapArray {
    let keys_data = ArrayData::builder(DataType::Int32)
        .len(8)
        .add_buffer(Buffer::from(&[0, 1, 2, 3, 4, 5, 6, 7].to_byte_slice()))
        .build()
        .unwrap();
    let values_data = ArrayData::builder(DataType::UInt32)
        .len(8)
        .add_buffer(Buffer::from(
            &[0u32, 10, 20, 30, 40, 50, 60, 70].to_byte_slice(),
        ))
        .build()
        .unwrap();

    let entry_offsets = Buffer::from(&[0, 3, 6, 8].to_byte_slice());
    let keys = Arc::new(Field::new("keys", DataType::Int32, false));
    let values = Arc::new(Field::new("values", DataType::UInt32, false));
    let entry_struct = StructArray::from(vec![
        (keys, make_array(keys_data)),
        (values, make_array(values_data)),
    ]);

    // Construct a map array from the above two
    let map_data_type = DataType::Map(
        Arc::new(Field::new(
            "entries",
            entry_struct.data_type().clone(),
            false,
        )),
        false,
    );
    let map_data = ArrayData::builder(map_data_type)
        .len(3)
        .add_buffer(entry_offsets)
        .add_child_data(entry_struct.into_data())
        .build()
        .unwrap();
    MapArray::from(map_data)
}

fn create_union() -> UnionArray {
    let int_array = Int32Array::from(vec![5, 6, 9]);
    let int64_array = Int64Array::from(vec![5_i64, 7, 8]);
    let float_array = Float64Array::from(vec![10.0, 0.0, 3.4]);

    let type_ids = [1_i8, 0, 2];
    let offsets = [0_i32, 0, 1];

    let type_id_buffer = Buffer::from_slice_ref(type_ids);
    let value_offsets_buffer = Buffer::from_slice_ref(offsets);

    let children: Vec<(Field, ArrayRef)> = vec![
        (
            Field::new("A", DataType::Int64, false),
            Arc::new(int64_array),
        ),
        (
            Field::new("B", DataType::Int32, false),
            Arc::new(int_array)
        ),
        (
            Field::new("C", DataType::Float64, false),
            Arc::new(float_array),
        ),
    ];
    
    UnionArray::try_new(
        &[0, 1, 2],
        type_id_buffer,
        Some(value_offsets_buffer),
        children,
    ).unwrap()
}

fn create_record() -> RecordBatch {
    let bool: ArrayRef = Arc::new(BooleanArray::from(vec![true, true, false]));

    let int8: ArrayRef = Arc::new(Int8Array::from(vec![1, 2, 3]));
    let int16: ArrayRef = Arc::new(Int16Array::from(vec![1, 2, 3]));
    let int32: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
    let int64: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));

    let uint8: ArrayRef = Arc::new(Int8Array::from(vec![1, 2, 3]));
    let uint16: ArrayRef = Arc::new(Int16Array::from(vec![1, 2, 3]));
    let uint32: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
    let uint64: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));

    let float16: ArrayRef = Arc::new(Float16Array::from(vec![f16::from_f64(1.0), f16::from_f64(2.0), f16::from_f64(3.0)]));  
    let float32: ArrayRef = Arc::new(Float32Array::from(vec![1.0_f32, 2.0_f32, 3.0_f32]));
    let float64: ArrayRef = Arc::new(Float64Array::from(vec![1.0_f64, 2.0_f64, 3.0_f64]));
    let date32: ArrayRef = Arc::new(Date32Array::from(vec![1, 2, 3]));
    let date64: ArrayRef = Arc::new(Date64Array::from(vec![1, 2, 3]));
    let time32_ms: ArrayRef = Arc::new(Time32MillisecondArray::from(vec![1, 2, 3]));
    let time32_s: ArrayRef = Arc::new(Time32SecondArray::from(vec![1, 2, 3]));
    let timestamp_s: ArrayRef = Arc::new(TimestampSecondArray::from(vec![1, 2, 3]));
    let timestamp_ms: ArrayRef = Arc::new(TimestampMillisecondArray::from(vec![1, 2, 3]));
    let timestamp_us: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![1, 2, 3]));
    let timestamp_ns: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![1, 2, 3]));

    let string: ArrayRef = Arc::new(StringArray::from(vec![Some("foo"), Some("baz"), Some("bar")]));

    let binary: ArrayRef = Arc::new(BinaryArray::from_opt_vec(vec![Some(b"foo"), Some(b"baz"), Some(b"bar")]));
    
    let fixed_size_binary: ArrayRef = Arc::new(FixedSizeBinaryArray::try_from_iter(vec![vec![1, 2], vec![3, 4], vec![5, 6]].into_iter()).unwrap());
    let time64_us: ArrayRef = Arc::new(Time64MicrosecondArray::from(vec![1, 2, 3]));
    let time64_ns: ArrayRef = Arc::new(Time64NanosecondArray::from(vec![1, 2, 3]));
    let interval_ym: ArrayRef = Arc::new(IntervalYearMonthArray::from(vec![Some(1), Some(2), Some(-5)]));
    let interval_daytime: ArrayRef = Arc::new(IntervalDayTimeArray::from(vec![Some(1), None, Some(-5)]));
    let duration_s: ArrayRef = Arc::new(DurationSecondArray::from(vec![1, 2, 3]));
    let duration_ms: ArrayRef = Arc::new(DurationMillisecondArray::from(vec![1, 2, 3]));
    let duration_us: ArrayRef = Arc::new(DurationMicrosecondArray::from(vec![1, 2, 3]));
    let duration_ns: ArrayRef = Arc::new(DurationNanosecondArray::from(vec![1, 2, 3]));
    
    
    let decimal128: ArrayRef = Arc::new(Decimal128Array::from(vec![1, 2, 3]));
    let decimal256: ArrayRef = Arc::new(Decimal256Array::from(vec![i256::from(1), i256::from(2), i256::from(3)])); 

    //  [[0, 1, 2], [3, 4, 5], [6, 7]]
    let values = Int32Array::from(vec![0, 1, 2, 3, 4, 5, 6, 7]);
    let offsets = OffsetBuffer::new(ScalarBuffer::from(vec![0, 3, 6, 8]));
    let field = Arc::new(Field::new("item", DataType::Int32, true));
    let list_array: ArrayRef = Arc::new(ListArray::new(field, offsets, Arc::new(values), None));
    // let list_array: ArrayRef = Arc::new(ListArray::from(create_map()));

    let struct_array: ArrayRef = Arc::new(StructArray::from(vec![
        (
            Arc::new(Field::new("b", DataType::Boolean, false)),
            Arc::new(BooleanArray::from(vec![false, true, true])) as ArrayRef,
        ),
        (
            Arc::new(Field::new("c", DataType::Int32, false)),
            Arc::new(Int32Array::from(vec![42, 19, 31])).clone() as ArrayRef,
        ),
    ]));

    // TODO: dense vs sparse
    let union: ArrayRef = Arc::new(create_union());

    // DictionaryArray, MapArray, FixedSizeListArray, LargeStringArray, LargeBinaryArray, 
    let dict: ArrayRef = Arc::new(DictionaryArray::<Int32Type>::from_iter([Some("a"), Some("b"), None]));
    let map: ArrayRef = Arc::new(create_map()); 

    let data = vec![
        Some(vec![Some(0), Some(1), Some(2)]),
        None,
        Some(vec![Some(6), Some(7), Some(45)]),
    ];
    let fixed_size_list_array: ArrayRef = Arc::new(FixedSizeListArray::from_iter_primitive::<Int32Type, _, _>(data.clone(), 3));
    let large_binary_array: ArrayRef = Arc::new(LargeBinaryArray::from_vec(vec![b"one", b"two", b"three"]));
    let large_string_array: ArrayRef = Arc::new(LargeStringArray::from(vec!["a", "b", "c"]));
    let large_list_array: ArrayRef = Arc::new(LargeListArray::from_iter_primitive::<Int32Type, _, _>(data.clone()));

    let interval_monthday_nano: ArrayRef = Arc::new(IntervalMonthDayNanoArray::from(vec![
        Some(100000000000000000000),
        None,
        Some(-500000000000000000000),
    ]));

    let value_data = PrimitiveArray::<Int8Type>::from_iter_values([10_i8, 115]);

    let run_ends_values = [2_i16, 3];
    let run_ends_data = PrimitiveArray::<Int16Type>::from_iter_values(run_ends_values.iter().copied());
    let runend_encoded: ArrayRef = Arc::new(RunArray::<Int16Type>::try_new(&run_ends_data, &value_data).unwrap());


    // TODO: Handle these. 
    // | string_view              | String (UTF8) view type with 4-byte prefix and inline small string optimization. |        |        |          |
    // | binary_view              | Bytes view type with 4-byte prefix and inline small string optimization.         |        |        |          |
    // | list_view                | A list of some logical data type represented by offset and size.                 |        |        |          |
    // | large_list_view          | Like LIST_VIEW, but with 64-bit offsets and sizes.                               |        |        |          |
    RecordBatch::try_from_iter(vec![
        ("bool", bool),
        ("int8", int8),
        ("int16", int16),
        ("int32", int32),
        ("int64", int64),
        ("uint8", uint8),
        ("uint16", uint16),
        ("uint32", uint32),
        ("uint64", uint64),
        // ("float16", float16), // not implemented: Data type mapping not implemented for Float16
        ("float32", float32),
        ("float64", float64),
        // ("date32", date32), // not implemented: Data type mapping not implemented for Date32
        // ("date64", date64), // not implemented: Data type mapping not implemented for Date64
        // ("time32_ms", time32_ms), // not implemented: Data type mapping not implemented for Time32(Millisecond)
        // ("time32_s", time32_s), // not implemented: Data type mapping not implemented for Time32(Second)
        ("timestamp_s", timestamp_s),
        ("timestamp_ms", timestamp_ms),
        ("timestamp_us", timestamp_us),
        ("timestamp_ns", timestamp_ns),
        ("string", string),
        // ("binary", binary), // not implemented: Data type mapping not implemented for Binary
        
        
        // ("fixed_size_binary", fixed_size_binary), // not implemented: Data type mapping not implemented for FixedSizeBinary(2)
        // ("time64_us", time64_us), // not implemented: Data type mapping not implemented for Time64(Microsecond)
        // ("time64_ns", time64_ns), // not implemented: Data type mapping not implemented for Time64(Nanosecond)

        // ("interval_ym", interval_ym), // not implemented: Data type mapping not implemented for Interval(YearMonth)
        // ("interval_daytime", interval_daytime), // not implemented: Data type mapping not implemented for Interval(DayTime)
        // ("duration_s", duration_s), // not implemented: Data type mapping not implemented for Duration(Second)
        // ("duration_ms", duration_ms),  // not implemented: Data type mapping not implemented for Duration(Millisecond)
        // ("duration_us", duration_us),  // not implemented: Data type mapping not implemented for Duration(Microsecond)
        // ("duration_ns", duration_ns),  // not implemented: Data type mapping not implemented for Duration(Nanosecond)

        ("decimal128", decimal128),
        // ("decimal256", decimal256), // not implemented: Data type mapping not implemented for Decimal256(76, 10)
        ("list_array", list_array),
        // ("struct_array", struct_array), // not implemented: Data type mapping not implemented for Struct([Field { name: "keys", data_type: Int32, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "values", data_type: UInt32, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }])
        // ("union", union), // not implemented: Data type mapping not implemented for Union([(0, Field { name: "A", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), (1, Field { name: "B", data_type: Int32, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), (2, Field { name: "C", data_type: Float64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} })], Dense)
        // ("dict", dict), // not implemented: Data type mapping not implemented for Dictionary(Int32, Utf8)
        // ("map", map), // not implemented: Data type mapping not implemented for Map(Field { name: "entries", data_type: Struct([Field { name: "keys", data_type: Int32, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "values", data_type: UInt32, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, false)
        // ("fixed_size_list_array", fixed_size_list_array), // not implemented: Data type mapping not implemented for FixedSizeList(Field { name: "item", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, 3)
        // ("large_binary_array", large_binary_array), // not implemented: Data type mapping not implemented for LargeBinary
        // ("large_string_array", large_string_array), // not implemented: Data type mapping not implemented for LargeList(Field { name: "item", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} })
        // ("large_list_array", large_list_array), // not implemented: Data type mapping not implemented for LargeList(Field { name: "item", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} })
        // ("interval_monthday_nano", interval_monthday_nano), // not implemented: Data type mapping not implemented for Interval(MonthDayNano)
        // ("runend_encoded", runend_encoded),  // not implemented: Data type mapping not implemented for RunEndEncoded(Field { name: "run_ends", data_type: Int16, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "values", data_type: Int8, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} })
    ]).unwrap()
}

pub fn main() {
    let conn = Connection::open_in_memory().unwrap();
    let b = create_record();

    let create = CreateTableBuilder::new(b.schema(), "table_name");
    println!("{}", create.build_postgres());

    // conn.execute(create.build_mysql().as_str(), []).unwrap();
    
    // let desc = "describe table;";
    // let mut binding = conn.prepare(desc).unwrap();
    // let mut rows = binding.query([]).unwrap();
    // while let Some(row) = rows.next().unwrap() {
    //     let name: String = row.get(0).unwrap();
    //     let data_type: String = row.get(1).unwrap();
    //     let is_nullable: String = row.get(2).unwrap();
    //     let key: String = row.get(3).unwrap();
    //     println!("name: {}, data_type: {}, is_nullable: {}, key: {}", name, data_type, is_nullable, key);
    // }

    // let insert = InsertBuilder::new("table_name", vec![b]);
    // println!("{}", insert.build_mysql()); // Doesn't seem to support: ("timestamp_s", timestamp_s), ("timestamp_ms", timestamp_ms),
    

}
