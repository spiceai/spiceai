// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::arrow_record_batch_gen::*;
use adbc_core::options::{AdbcVersion, OptionDatabase, OptionValue};
use adbc_core::{Driver, LOAD_FLAG_DEFAULT};
use adbc_driver_manager::{ManagedDatabase, ManagedDriver};
use arrow::array::RecordBatch;
use arrow_schema::SchemaRef;
use datafusion::physical_plan::collect;
use datafusion::{catalog::memory::MemorySourceConfig, prelude::SessionContext};

#[cfg(feature = "adbc-federation")]
use datafusion_federation::schema_cast::record_convert::try_cast_to;

use datafusion_table_providers::{
    adbc::AdbcTableFactory, sql::db_connection_pool::adbcpool::ADBCPool,
};
use rstest::rstest;
use std::sync::Arc;

fn get_db(driver_name: &str) -> ManagedDatabase {
    let mut driver = ManagedDriver::load_from_name(
        driver_name,
        None,
        AdbcVersion::V110,
        LOAD_FLAG_DEFAULT,
        None,
    )
    .unwrap();

    driver
        .new_database_with_opts([(
            OptionDatabase::Uri,
            OptionValue::String(":memory:".to_string()),
        )])
        .unwrap()
}

async fn arrow_adbc_round_trip(
    arrow_record: RecordBatch,
    _source_schema: SchemaRef,
    table_name: &str,
) {
    let adbc_pool =
        Arc::new(ADBCPool::new(get_db("sqlite"), None).expect("Failed to create ADBC pool"));

    let table_factory = AdbcTableFactory::new(adbc_pool.clone());
    let ctx = SessionContext::new();
    let mem_exec = MemorySourceConfig::try_new_exec(
        &[vec![arrow_record.clone()]],
        arrow_record.schema(),
        None,
    )
    .expect("Failed to create memory source execution plan");

    let create_plan = table_factory
        .create_from(&ctx.state(), mem_exec, table_name.into())
        .await
        .expect("Failed to create table");

    let _ = collect(create_plan, ctx.task_ctx())
        .await
        .expect("Table creation failed");

    let table_provider = table_factory
        .table_provider(table_name.into(), None)
        .await
        .expect("Failed to register table provider");
    ctx.register_table(table_name, Arc::clone(&table_provider))
        .expect("Failed to register table");

    let select_sql = format!("SELECT * FROM {table_name}");
    let df = ctx
        .sql(&select_sql)
        .await
        .expect("DataFrame should be created from query");

    let record_batch = df.collect().await.expect("RecordBatch should be collected");

    #[cfg(feature = "adbc-federation")]
    let casted_record = try_cast_to(record_batch[0].clone(), _source_schema).unwrap();

    tracing::debug!("Original Arrow Record Batch: {:?}", arrow_record.columns());
    tracing::debug!(
        "Adbc returned Record Batch: {:?}",
        record_batch[0].columns()
    );

    // Check results
    assert_eq!(record_batch.len(), 1);
    assert_eq!(record_batch[0].num_rows(), arrow_record.num_rows());
    assert_eq!(record_batch[0].num_columns(), arrow_record.num_columns());
    #[cfg(feature = "adbc-federation")]
    assert_eq!(casted_record, arrow_record);
}

#[rstest]
#[case::binary(get_arrow_binary_record_batch(), "binary")]
#[case::int(get_arrow_int_record_batch(), "int")]
#[case::float(get_arrow_float_record_batch(), "float")]
#[case::utf8(get_arrow_utf8_record_batch(), "utf8")]
#[ignore] // sqlite does not support Time32 / Time64
#[case::time(get_arrow_time_record_batch(), "time")]
#[case::timestamp(get_arrow_timestamp_record_batch(), "timestamp")]
#[ignore] // sqlite does not support Date32 / Date64
#[case::date(get_arrow_date_record_batch(), "date")]
#[ignore] // sqlite does not support Struct type
#[case::struct_type(get_arrow_struct_record_batch(), "struct")]
#[ignore] // sqlite does not support Decimal256
#[case::decimal(get_arrow_decimal_record_batch(), "decimal")]
#[ignore]
// Interval(DayTime) is not supported: / "Conversion Error: Could not convert Interval to Microsecond"
#[case::interval(get_arrow_interval_record_batch(), "interval")]
#[ignore] // TimeUnit::Nanosecond is not correctly supported; written values are zeros
#[case::duration(get_arrow_duration_record_batch(), "duration")]
#[ignore] // sqlite does not support List type
#[case::list(get_arrow_list_record_batch(), "list")]
#[case::null(get_arrow_null_record_batch(), "null")]
#[ignore] // sqlite does not support list type
#[case::list_of_structs(get_arrow_list_of_structs_record_batch(), "list_of_structs")]
#[ignore] // sqlite does not support list type
#[case::list_of_fixed_size_lists(
    get_arrow_list_of_fixed_size_lists_record_batch(),
    "list_of_fixed_size_lists"
)]
#[ignore] // sqlite does not support list type
#[case::list_of_lists(get_arrow_list_of_lists_record_batch(), "list_of_lists")]
#[ignore] // sqlite does not support map type
#[case::map(get_arrow_map_record_batch(), "map")]
#[case::dictionary(get_arrow_dictionary_array_record_batch(), "dictionary")]
#[test_log::test(tokio::test)]
async fn test_arrow_adbc_roundtrip(
    #[case] arrow_result: (RecordBatch, SchemaRef),
    #[case] table_name: &str,
) {
    arrow_adbc_round_trip(
        arrow_result.0,
        arrow_result.1,
        &format!("{table_name}_types"),
    )
    .await;
}
