use chrono::{DateTime, Utc};
use datafusion::{error::DataFusionError, execution::context::SessionContext};
use datafusion_table_providers::mongodb::table::MongoDBTable;
use mongodb::bson::{doc, Bson, DateTime as BsonDateTime, Decimal128, Document};
use rstest::rstest;
use std::str::FromStr;
use std::sync::Arc;
use std::time::SystemTime;

use arrow::{
    array::*,
    datatypes::{DataType, Field, Schema, TimeUnit},
};

use crate::docker::RunningContainer;

mod common;

async fn test_mongodb_datetime_types(port: usize) {
    let ts0 = DateTime::parse_from_rfc3339("2024-09-12T10:00:00Z")
        .unwrap()
        .with_timezone(&Utc);
    let ts1 = DateTime::parse_from_rfc3339("2024-09-12T10:00:00.1Z")
        .unwrap()
        .with_timezone(&Utc);
    let ts2 = DateTime::parse_from_rfc3339("2024-09-12T10:00:00.12Z")
        .unwrap()
        .with_timezone(&Utc);
    let ts3 = DateTime::parse_from_rfc3339("2024-09-12T10:00:00.123Z")
        .unwrap()
        .with_timezone(&Utc);
    let ts4 = DateTime::parse_from_rfc3339("2024-09-12T00:00:00Z")
        .unwrap()
        .with_timezone(&Utc);

    let test_docs = vec![doc! {
        "timestamp_field": Bson::DateTime(BsonDateTime::from(SystemTime::from(ts0))),
        "timestamp_one_fraction": Bson::DateTime(BsonDateTime::from(SystemTime::from(ts1))),
        "timestamp_two_fraction": Bson::DateTime(BsonDateTime::from(SystemTime::from(ts2))),
        "timestamp_three_fraction": Bson::DateTime(BsonDateTime::from(SystemTime::from(ts3))),
        "created_date": Bson::DateTime(BsonDateTime::from(SystemTime::from(ts4))),
    }];

    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "timestamp_field",
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            true,
        ),
        Field::new(
            "timestamp_one_fraction",
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            true,
        ),
        Field::new(
            "timestamp_two_fraction",
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            true,
        ),
        Field::new(
            "timestamp_three_fraction",
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            true,
        ),
        Field::new("created_date", DataType::Date32, true),
    ]));

    let expected_record = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(
                TimestampMillisecondArray::from(vec![1_726_135_200_000])
                    .with_timezone(Arc::from("UTC")),
            ),
            Arc::new(
                TimestampMillisecondArray::from(vec![1_726_135_200_100])
                    .with_timezone(Arc::from("UTC")),
            ),
            Arc::new(
                TimestampMillisecondArray::from(vec![1_726_135_200_120])
                    .with_timezone(Arc::from("UTC")),
            ),
            Arc::new(
                TimestampMillisecondArray::from(vec![1_726_135_200_123])
                    .with_timezone(Arc::from("UTC")),
            ),
            Arc::new(Date32Array::from(vec![19_978])),
        ],
    )
    .expect("Failed to create arrow record batch");

    arrow_mongodb_one_way(
        port,
        "timestamp_collection",
        test_docs,
        expected_record,
        None,
    )
    .await;
}

#[allow(clippy::approx_constant)]
async fn test_mongodb_numeric_types(port: usize) {
    let decimal = Decimal128::from_str("123.456").unwrap();

    let test_docs = vec![doc! {
        "int32_field": 2147483647i32,
        "int64_field": 9223372036854775807i64,
        "double_field": 3.14159265359,
        "decimal_field": Bson::Decimal128(decimal),
    }];

    let schema = Arc::new(Schema::new(vec![
        Field::new("int32_field", DataType::Int32, true),
        Field::new("int64_field", DataType::Int64, true),
        Field::new("double_field", DataType::Float64, true),
        Field::new("decimal_field", DataType::Decimal128(18, 6), true),
    ]));

    let expected_record = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int32Array::from(vec![2147483647i32])),
            Arc::new(Int64Array::from(vec![9223372036854775807i64])),
            Arc::new(Float64Array::from(vec![3.14159265359])),
            Arc::new(
                Decimal128Array::from(vec![Some(123456000i128)])
                    .with_precision_and_scale(18, 6)
                    .unwrap(),
            ),
        ],
    )
    .expect("Failed to create arrow record batch");

    let _array = expected_record
        .column(3)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();

    arrow_mongodb_one_way(port, "numeric_collection", test_docs, expected_record, None).await;
}

async fn test_mongodb_string_types(port: usize) {
    let test_docs = vec![
        doc! {
            "name": "Alice",
            "description": "Software Engineer",
            "notes": Bson::Null,
        },
        doc! {
            "name": "Bob",
            "description": "Data Scientist",
            "notes": "Likes MongoDB",
        },
    ];

    let schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, true),
        Field::new("description", DataType::Utf8, true),
        Field::new("notes", DataType::Utf8, true),
    ]));

    let expected_record = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec!["Alice", "Bob"])),
            Arc::new(StringArray::from(vec![
                "Software Engineer",
                "Data Scientist",
            ])),
            Arc::new(StringArray::from(vec![None, Some("Likes MongoDB")])),
        ],
    )
    .expect("Failed to create arrow record batch");

    arrow_mongodb_one_way(port, "string_collection", test_docs, expected_record, None).await;
}

async fn test_mongodb_boolean_types(port: usize) {
    let test_docs = vec![
        doc! {
            "is_active": true,
            "is_verified": false,
            "is_premium": Bson::Null,
        },
        doc! {
            "is_active": false,
            "is_verified": true,
            "is_premium": true,
        },
    ];

    let schema = Arc::new(Schema::new(vec![
        Field::new("is_active", DataType::Boolean, true),
        Field::new("is_verified", DataType::Boolean, true),
        Field::new("is_premium", DataType::Boolean, true),
    ]));

    let expected_record = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(BooleanArray::from(vec![true, false])),
            Arc::new(BooleanArray::from(vec![false, true])),
            Arc::new(BooleanArray::from(vec![None, Some(true)])),
        ],
    )
    .expect("Failed to create arrow record batch");

    arrow_mongodb_one_way(port, "boolean_collection", test_docs, expected_record, None).await;
}

async fn test_mongodb_binary_types(port: usize) {
    let test_docs = vec![doc! {
        "binary_data": Bson::Binary(mongodb::bson::Binary {
            subtype: mongodb::bson::spec::BinarySubtype::Generic,
            bytes: b"hello world".to_vec(),
        }),
        "file_content": Bson::Binary(mongodb::bson::Binary {
            subtype: mongodb::bson::spec::BinarySubtype::Generic,
            bytes: b"binary file content".to_vec(),
        }),
    }];

    let schema = Arc::new(Schema::new(vec![
        Field::new("binary_data", DataType::Binary, true),
        Field::new("file_content", DataType::Binary, true),
    ]));

    let expected_record = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(BinaryArray::from_vec(vec![b"hello world"])),
            Arc::new(BinaryArray::from_vec(vec![b"binary file content"])),
        ],
    )
    .expect("Failed to create arrow record batch");

    arrow_mongodb_one_way(port, "binary_collection", test_docs, expected_record, None).await;
}

async fn test_mongodb_object_id_types(port: usize) {
    let oid1 = mongodb::bson::oid::ObjectId::new();
    let oid2 = mongodb::bson::oid::ObjectId::new();

    let test_docs = vec![doc! {
        "_id": oid1,
        "ref_id": oid2,
    }];

    let schema = Arc::new(Schema::new(vec![
        Field::new("_id", DataType::Utf8, true), // ObjectId typically converted to string
        Field::new("ref_id", DataType::Utf8, true),
    ]));

    let expected_record = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec![oid1.to_hex()])),
            Arc::new(StringArray::from(vec![oid2.to_hex()])),
        ],
    )
    .expect("Failed to create arrow record batch");

    arrow_mongodb_one_way(
        port,
        "objectid_collection",
        test_docs,
        expected_record,
        None,
    )
    .await;
}

#[allow(clippy::approx_constant)]
async fn test_mongodb_array_types(port: usize) {
    let test_docs = vec![
        doc! {
            "string_tags": ["rust", "mongodb", "arrow"],
            "mixed_array": ["text", 42, true, 3.14],
            "empty_array": [],
            "numbers_as_strings": [1, 2, 3],
        },
        doc! {
            "string_tags": ["python", "sql"],
            "mixed_array": ["another", false, 99],
            "empty_array": [],
            "numbers_as_strings": [4, 5],
        },
    ];

    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "string_tags",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            true,
        ),
        Field::new(
            "mixed_array",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            true,
        ),
        Field::new(
            "empty_array",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            true,
        ),
        Field::new(
            "numbers_as_strings",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            true,
        ),
    ]));

    // Create the expected ListArrays manually
    let string_tags_builder = ListBuilder::new(StringBuilder::new());
    let mut string_tags_list = string_tags_builder;

    // First document string_tags: ["rust", "mongodb", "arrow"]
    string_tags_list.values().append_value("rust");
    string_tags_list.values().append_value("mongodb");
    string_tags_list.values().append_value("arrow");
    string_tags_list.append(true);

    // Second document string_tags: ["python", "sql"]
    string_tags_list.values().append_value("python");
    string_tags_list.values().append_value("sql");
    string_tags_list.append(true);

    let string_tags_array = Arc::new(string_tags_list.finish());

    // Mixed array (all converted to strings)
    let mixed_array_builder = ListBuilder::new(StringBuilder::new());
    let mut mixed_array_list = mixed_array_builder;

    // First document mixed_array: ["text", "42", "true", "3.14"]
    mixed_array_list.values().append_value("text");
    mixed_array_list.values().append_value("42");
    mixed_array_list.values().append_value("true");
    mixed_array_list.values().append_value("3.14");
    mixed_array_list.append(true);

    // Second document mixed_array: ["another", "false", "99"]
    mixed_array_list.values().append_value("another");
    mixed_array_list.values().append_value("false");
    mixed_array_list.values().append_value("99");
    mixed_array_list.append(true);

    let mixed_array_array = Arc::new(mixed_array_list.finish());

    // Empty arrays
    let empty_array_builder = ListBuilder::new(StringBuilder::new());
    let mut empty_array_list = empty_array_builder;

    // First document: empty array
    empty_array_list.append(true);
    // Second document: empty array
    empty_array_list.append(true);

    let empty_array_array = Arc::new(empty_array_list.finish());

    // Numbers as strings array
    let numbers_builder = ListBuilder::new(StringBuilder::new());
    let mut numbers_list = numbers_builder;

    // First document: [1, 2, 3] -> ["1", "2", "3"]
    numbers_list.values().append_value("1");
    numbers_list.values().append_value("2");
    numbers_list.values().append_value("3");
    numbers_list.append(true);

    // Second document: [4, 5] -> ["4", "5"]
    numbers_list.values().append_value("4");
    numbers_list.values().append_value("5");
    numbers_list.append(true);

    let numbers_array = Arc::new(numbers_list.finish());

    let expected_record = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            string_tags_array,
            mixed_array_array,
            empty_array_array,
            numbers_array,
        ],
    )
    .expect("Failed to create arrow record batch");

    arrow_mongodb_one_way(port, "array_collection", test_docs, expected_record, None).await;
}

async fn test_mongodb_nested_object_types(port: usize) {
    let test_docs = vec![
        doc! {
            "user": {
                "name": "Alice",
                "age": 30,
                "contact": {
                    "email": "alice@example.com",
                    "phone": "555-1234"
                }
            },
            "metadata": {
                "created_at": "2024-01-01",
                "tags": ["important", "user"],
                "settings": {
                    "theme": "dark",
                    "notifications": true
                }
            },
            "empty_object": {},
            "simple_string": "not an object"
        },
        doc! {
            "user": {
                "name": "Bob",
                "age": 25,
                "contact": {
                    "email": "bob@example.com"
                }
            },
            "metadata": {
                "created_at": "2024-01-02",
                "tags": ["user"],
                "settings": {
                    "theme": "light",
                    "notifications": false
                }
            },
            "empty_object": {},
            "simple_string": "also not an object"
        },
    ];

    // We'll test the content, not the exact JSON string format
    let ctx = SessionContext::new();
    let client = common::get_mongodb_client(port)
        .await
        .expect("MongoDB client should be created");

    // Insert test data into MongoDB collection
    let db = client.database("testdb");
    let collection = db.collection::<Document>("nested_object_collection");

    // Drop collection if it exists
    let _ = collection.drop().await;

    // Insert test documents
    collection
        .insert_many(test_docs)
        .await
        .expect("MongoDB documents should be inserted");

    let expected_user1 = serde_json::json!({
        "name": "Alice",
        "age": 30,
        "contact": {
            "email": "alice@example.com",
            "phone": "555-1234"
        }
    });

    let expected_user2 = serde_json::json!({
        "name": "Bob",
        "age": 25,
        "contact": {
            "email": "bob@example.com"
        }
    });

    let expected_metadata1 = serde_json::json!({
        "created_at": "2024-01-01",
        "tags": ["important", "user"],
        "settings": {
            "theme": "dark",
            "notifications": true
        }
    });

    let expected_metadata2 = serde_json::json!({
        "created_at": "2024-01-02",
        "tags": ["user"],
        "settings": {
            "theme": "light",
            "notifications": false
        }
    });

    let expected_empty = serde_json::json!({});

    // Register DataFusion table
    let mongo_conn_pool = common::get_mongodb_connection_pool(port, None)
        .await
        .expect("MongoDB connection pool should be created");

    let table = MongoDBTable::new(&Arc::new(mongo_conn_pool), "nested_object_collection")
        .await
        .expect("Table should be created");

    ctx.register_table("nested_object_collection", Arc::new(table))
        .expect("Table should be registered");

    // Query the data
    let sql = r#"SELECT "user", "metadata", "empty_object", "simple_string" FROM nested_object_collection"#;
    let df = ctx
        .sql(sql)
        .await
        .expect("DataFrame should be created from query");

    let record_batches = df.collect().await.expect("RecordBatch should be collected");
    assert_eq!(record_batches.len(), 1);

    let batch = &record_batches[0];
    assert_eq!(batch.num_rows(), 2);
    assert_eq!(batch.num_columns(), 4);

    // Verify the JSON content by parsing and comparing structure
    let user_array = batch
        .column_by_name("user")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let metadata_array = batch
        .column_by_name("metadata")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let empty_array = batch
        .column_by_name("empty_object")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let string_array = batch
        .column_by_name("simple_string")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    // Parse actual JSON strings and compare with expected JSON objects
    let actual_user1: serde_json::Value = serde_json::from_str(user_array.value(0)).unwrap();
    let actual_user2: serde_json::Value = serde_json::from_str(user_array.value(1)).unwrap();
    let actual_metadata1: serde_json::Value =
        serde_json::from_str(metadata_array.value(0)).unwrap();
    let actual_metadata2: serde_json::Value =
        serde_json::from_str(metadata_array.value(1)).unwrap();
    let actual_empty1: serde_json::Value = serde_json::from_str(empty_array.value(0)).unwrap();
    let actual_empty2: serde_json::Value = serde_json::from_str(empty_array.value(1)).unwrap();

    // Direct JSON comparison - order doesn't matter!
    assert_eq!(actual_user1, expected_user1);
    assert_eq!(actual_user2, expected_user2);
    assert_eq!(actual_metadata1, expected_metadata1);
    assert_eq!(actual_metadata2, expected_metadata2);
    assert_eq!(actual_empty1, expected_empty);
    assert_eq!(actual_empty2, expected_empty);

    // String values remain simple
    assert_eq!(string_array.value(0), "not an object");
    assert_eq!(string_array.value(1), "also not an object");
}

async fn test_mongodb_null_and_missing_fields(port: usize) {
    let test_docs = vec![
        doc! {
            "name": "Alice",
            "age": 30,
            "email": "alice@example.com",
        },
        doc! {
            "name": "Bob",
            "age": Bson::Null,
            "phone": "555-1234",
        },
        doc! {
            "name": "Charlie",
            "age": 25,
            // email and phone missing
        },
    ];

    let schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, true),
        Field::new("age", DataType::Int32, true),
        Field::new("email", DataType::Utf8, true),
        Field::new("phone", DataType::Utf8, true),
    ]));

    let expected_record = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
            Arc::new(Int32Array::from(vec![Some(30), None, Some(25)])),
            Arc::new(StringArray::from(vec![
                Some("alice@example.com"),
                None,
                None,
            ])),
            Arc::new(StringArray::from(vec![None, Some("555-1234"), None])),
        ],
    )
    .expect("Failed to create arrow record batch");

    arrow_mongodb_one_way(
        port,
        "null_fields_collection",
        test_docs,
        expected_record,
        None,
    )
    .await;
}

async fn arrow_mongodb_one_way(
    port: usize,
    collection_name: &str,
    test_docs: Vec<Document>,
    expected_record: RecordBatch,
    unnest_depth: Option<usize>,
) -> Vec<RecordBatch> {
    tracing::debug!("Running MongoDB tests on {collection_name}");

    let ctx = SessionContext::new();
    let client = common::get_mongodb_client(port)
        .await
        .expect("MongoDB client should be created");

    // Insert test data into MongoDB collection
    let db = client.database("testdb");
    let collection = db.collection::<Document>(collection_name);

    // Drop collection if it exists
    let _ = collection.drop().await;

    // Insert test documents
    if !test_docs.is_empty() {
        collection
            .insert_many(test_docs)
            .await
            .expect("MongoDB documents should be inserted");
    }

    // Register DataFusion table
    let mongo_conn_pool = common::get_mongodb_connection_pool(port, unnest_depth)
        .await
        .expect("MongoDB connection pool should be created");

    let table = MongoDBTable::new(&Arc::new(mongo_conn_pool), collection_name)
        .await
        .expect("Table should be created");

    ctx.register_table(collection_name, Arc::new(table))
        .expect("Table should be registered");

    // Extract expected columns (excluding _id)
    let schema_ref = expected_record.schema();
    let expected_fields: Vec<&str> = schema_ref
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .filter(|name| *name != "_id")
        .collect();

    // Build SELECT query with correct projection
    let projection = expected_fields
        .iter()
        .map(|c| format!("\"{c}\""))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!("SELECT {projection} FROM {collection_name}");

    let df = ctx
        .sql(&sql)
        .await
        .expect("DataFrame should be created from query");

    let record_batches = df.collect().await.expect("RecordBatch should be collected");
    assert_eq!(record_batches.len(), 1);

    // Normalize actual and expected
    let actual_projected =
        project_record_batch(&record_batches[0], &expected_fields).expect("Project actual");
    let expected_projected =
        project_record_batch(&expected_record, &expected_fields).expect("Project expected");

    assert_eq!(actual_projected, expected_projected);

    record_batches
}

async fn test_mongodb_unnesting_depth_1(port: usize) {
    let ts0 = DateTime::parse_from_rfc3339("2024-09-12T10:00:00Z")
        .unwrap()
        .with_timezone(&Utc);
    let ts4 = DateTime::parse_from_rfc3339("2024-09-12T00:00:00Z")
        .unwrap()
        .with_timezone(&Utc);

    let test_docs = vec![
        doc! {
            "id": 1,
           "user": {
                "name": "John",
                "age": 10,
            },
            "created_date": Bson::DateTime(BsonDateTime::from(SystemTime::from(ts4))),
        },
        doc! {
            "id": 2,
            "user": {
                "name": "Jane",
                "age": 20,
            },
            "timestamp_field": Bson::DateTime(BsonDateTime::from(SystemTime::from(ts0))),
        },
        doc! {
            "id": 3,
            "user": {
                "name": "Bob",
            },
            "active": true,
        },
    ];

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, true),
        Field::new("user.name", DataType::Utf8, true),
        Field::new("user.age", DataType::Int32, true),
        Field::new("created_date", DataType::Date32, true),
        Field::new(
            "timestamp_field",
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            true,
        ),
        Field::new("active", DataType::Boolean, true),
    ]));

    let expected_record = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int32Array::from(vec![Some(1), Some(2), Some(3)])),
            Arc::new(StringArray::from(vec![
                Some("John"),
                Some("Jane"),
                Some("Bob"),
            ])),
            Arc::new(Int32Array::from(vec![Some(10), Some(20), None])),
            Arc::new(Date32Array::from(vec![Some(19_978), None, None])),
            Arc::new(
                TimestampMillisecondArray::from(vec![None, Some(1_726_135_200_000), None])
                    .with_timezone(Arc::from("UTC")),
            ),
            Arc::new(BooleanArray::from(vec![None, None, Some(true)])),
        ],
    )
    .expect("Failed to create arrow record batch");

    arrow_mongodb_one_way(
        port,
        "unnesting_collection",
        test_docs,
        expected_record,
        Some(1),
    )
    .await;
}

use datafusion::common::Result as DFResult;
fn project_record_batch(batch: &RecordBatch, columns: &[&str]) -> DFResult<RecordBatch> {
    let schema = batch.schema();
    let indices: Vec<usize> = columns
        .iter()
        .map(|col| schema.index_of(col).expect("Column not found"))
        .collect();
    let arrays = indices.iter().map(|&i| batch.column(i).clone()).collect();
    let fields = indices
        .iter()
        .map(|&i| schema.field(i).clone())
        .collect::<Vec<_>>();
    let projected_schema = Arc::new(arrow::datatypes::Schema::new(fields));
    RecordBatch::try_new(projected_schema, arrays)
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
}

async fn start_mongodb_container(port: usize) -> RunningContainer {
    let running_container = common::start_mongodb_docker_container(port)
        .await
        .expect("MongoDB container to start");

    tracing::debug!("MongoDB Container started");

    running_container
}

#[rstest]
#[test_log::test(tokio::test)]
async fn test_mongodb_arrow_oneway() {
    let port = crate::get_random_port();
    let mongodb_container = start_mongodb_container(port).await;

    test_mongodb_datetime_types(port).await;
    test_mongodb_numeric_types(port).await;
    test_mongodb_string_types(port).await;
    test_mongodb_boolean_types(port).await;
    test_mongodb_binary_types(port).await;
    test_mongodb_object_id_types(port).await;
    test_mongodb_array_types(port).await;
    test_mongodb_nested_object_types(port).await;
    test_mongodb_null_and_missing_fields(port).await;
    test_mongodb_unnesting_depth_1(port).await;

    mongodb_container.remove().await.expect("container to stop");
}
