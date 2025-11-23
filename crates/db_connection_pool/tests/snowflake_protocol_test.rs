/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

#![cfg(feature = "snowflake")]

use std::collections::HashMap;

use db_connection_pool::{
    snowflakepool::SnowflakeConnectionPool,
    dbconnection::snowflakeconn::SnowflakeConnection,
};
use secrecy::SecretString;

fn get_test_params() -> HashMap<String, SecretString> {
    let account = std::env::var("SPICE_SNOWFLAKE_ACCOUNT")
        .expect("SPICE_SNOWFLAKE_ACCOUNT environment variable not set");
    let username = std::env::var("SPICE_SNOWFLAKE_USERNAME")
        .expect("SPICE_SNOWFLAKE_USERNAME environment variable not set");
    let password = std::env::var("SPICE_SNOWFLAKE_PASSWORD")
        .expect("SPICE_SNOWFLAKE_PASSWORD environment variable not set");

    HashMap::from([
        ("account".to_string(), SecretString::from(account)),
        ("username".to_string(), SecretString::from(username)),
        ("password".to_string(), SecretString::from(password)),
    ])
}

async fn get_snowflake_connection(
    protocol: &str,
) -> SnowflakeConnection {
    use datafusion_table_providers::sql::db_connection_pool::DbConnectionPool;
    
    let mut params = get_test_params();
    params.insert("protocol".to_string(), SecretString::from(protocol));

    let pool = SnowflakeConnectionPool::new(&params)
        .await
        .expect("Failed to create pool");
    
    let conn_box = pool.connect().await.expect("Failed to connect");
    // SAFETY: We control the pool and know it returns SnowflakeConnection
    unsafe {
        let raw = Box::into_raw(conn_box);
        let cast = raw as *mut SnowflakeConnection;
        *Box::from_raw(cast)
    }
}

#[tokio::test]
async fn test_snowflake_adbc_protocol_connection() {
    let mut params = get_test_params();
    params.insert("protocol".to_string(), SecretString::from("adbc"));

    let result = SnowflakeConnectionPool::new(&params).await;
    assert!(
        result.is_ok(),
        "Failed to create ADBC connection pool: {:?}",
        result.err()
    );
}

#[tokio::test]
async fn test_snowflake_http_protocol_connection() {
    let mut params = get_test_params();
    params.insert("protocol".to_string(), SecretString::from("http"));

    let result = SnowflakeConnectionPool::new(&params).await;
    assert!(
        result.is_ok(),
        "Failed to create HTTP connection pool: {:?}",
        result.err()
    );
}

#[tokio::test]
async fn test_snowflake_default_protocol_is_adbc() {
    let params = get_test_params();
    // Don't specify protocol - should default to ADBC

    let result = SnowflakeConnectionPool::new(&params).await;
    assert!(
        result.is_ok(),
        "Failed to create connection pool with default protocol: {:?}",
        result.err()
    );
}

#[tokio::test]
async fn test_snowflake_invalid_protocol() {
    let mut params = get_test_params();
    params.insert("protocol".to_string(), SecretString::from("grpc"));

    // This should default to ADBC since the protocol string won't parse
    let result = SnowflakeConnectionPool::new(&params).await;
    assert!(
        result.is_ok(),
        "Should fall back to default protocol when invalid protocol specified"
    );
}

// TPCH Tests using Snowflake Sample Data

#[tokio::test]
async fn test_tpch_region_adbc() {
    use datafusion_table_providers::sql::db_connection_pool::dbconnection::AsyncDbConnection;
    use futures::StreamExt;

    let conn = get_snowflake_connection("adbc").await;

    let query = r#"
        SELECT R_REGIONKEY, R_NAME, R_COMMENT 
        FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.REGION 
        ORDER BY R_REGIONKEY
    "#;

    let mut stream = conn.query_arrow(query, &[], None).await.expect("Query failed");
    let mut total_rows = 0;

    while let Some(batch_result) = stream.next().await {
        let batch = batch_result.expect("Batch error");
        assert_eq!(batch.num_columns(), 3, "Expected 3 columns");
        total_rows += batch.num_rows();
    }

    assert_eq!(total_rows, 5, "Expected 5 regions in TPCH data");
}

#[tokio::test]
async fn test_tpch_region_http() {
    use datafusion_table_providers::sql::db_connection_pool::dbconnection::AsyncDbConnection;
    use futures::StreamExt;

    let conn = get_snowflake_connection("http").await;

    let query = r#"
        SELECT R_REGIONKEY, R_NAME, R_COMMENT 
        FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.REGION 
        ORDER BY R_REGIONKEY
    "#;

    let mut stream = conn.query_arrow(query, &[], None).await.expect("Query failed");
    let mut total_rows = 0;

    while let Some(batch_result) = stream.next().await {
        let batch = batch_result.expect("Batch error");
        assert_eq!(batch.num_columns(), 3, "Expected 3 columns");
        total_rows += batch.num_rows();
    }

    assert_eq!(total_rows, 5, "Expected 5 regions in TPCH data");
}

#[tokio::test]
async fn test_tpch_nation_adbc() {
    use datafusion_table_providers::sql::db_connection_pool::dbconnection::AsyncDbConnection;
    use futures::StreamExt;

    let conn = get_snowflake_connection("adbc").await;

    let query = r#"
        SELECT N_NATIONKEY, N_NAME, N_REGIONKEY, N_COMMENT 
        FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.NATION 
        ORDER BY N_NATIONKEY
    "#;

    let mut stream = conn.query_arrow(query, &[], None).await.expect("Query failed");
    let mut total_rows = 0;

    while let Some(batch_result) = stream.next().await {
        let batch = batch_result.expect("Batch error");
        assert_eq!(batch.num_columns(), 4, "Expected 4 columns");
        total_rows += batch.num_rows();
    }

    assert_eq!(total_rows, 25, "Expected 25 nations in TPCH data");
}

#[tokio::test]
async fn test_tpch_nation_http() {
    use datafusion_table_providers::sql::db_connection_pool::dbconnection::AsyncDbConnection;
    use futures::StreamExt;

    let conn = get_snowflake_connection("http").await;

    let query = r#"
        SELECT N_NATIONKEY, N_NAME, N_REGIONKEY, N_COMMENT 
        FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.NATION 
        ORDER BY N_NATIONKEY
    "#;

    let mut stream = conn.query_arrow(query, &[], None).await.expect("Query failed");
    let mut total_rows = 0;

    while let Some(batch_result) = stream.next().await {
        let batch = batch_result.expect("Batch error");
        assert_eq!(batch.num_columns(), 4, "Expected 4 columns");
        total_rows += batch.num_rows();
    }

    assert_eq!(total_rows, 25, "Expected 25 nations in TPCH data");
}

#[tokio::test]
async fn test_tpch_customer_sample_adbc() {
    use datafusion_table_providers::sql::db_connection_pool::dbconnection::AsyncDbConnection;
    use futures::StreamExt;

    let conn = get_snowflake_connection("adbc").await;

    let query = r#"
        SELECT C_CUSTKEY, C_NAME, C_ADDRESS, C_NATIONKEY, C_PHONE, C_ACCTBAL, C_MKTSEGMENT
        FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER 
        ORDER BY C_CUSTKEY 
        LIMIT 100
    "#;

    let mut stream = conn.query_arrow(query, &[], None).await.expect("Query failed");
    let mut total_rows = 0;

    while let Some(batch_result) = stream.next().await {
        let batch = batch_result.expect("Batch error");
        assert_eq!(batch.num_columns(), 7, "Expected 7 columns");
        total_rows += batch.num_rows();
    }

    assert_eq!(total_rows, 100, "Expected 100 customer rows");
}

#[tokio::test]
async fn test_tpch_customer_sample_http() {
    use datafusion_table_providers::sql::db_connection_pool::dbconnection::AsyncDbConnection;
    use futures::StreamExt;

    let conn = get_snowflake_connection("http").await;

    let query = r#"
        SELECT C_CUSTKEY, C_NAME, C_ADDRESS, C_NATIONKEY, C_PHONE, C_ACCTBAL, C_MKTSEGMENT
        FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER 
        ORDER BY C_CUSTKEY 
        LIMIT 100
    "#;

    let mut stream = conn.query_arrow(query, &[], None).await.expect("Query failed");
    let mut total_rows = 0;

    while let Some(batch_result) = stream.next().await {
        let batch = batch_result.expect("Batch error");
        assert_eq!(batch.num_columns(), 7, "Expected 7 columns");
        total_rows += batch.num_rows();
    }

    assert_eq!(total_rows, 100, "Expected 100 customer rows");
}

#[tokio::test]
async fn test_tpch_lineitem_aggregation_adbc() {
    use datafusion_table_providers::sql::db_connection_pool::dbconnection::AsyncDbConnection;
    use futures::StreamExt;

    let conn = get_snowflake_connection("adbc").await;

    // Test aggregation query
    let query = r#"
        SELECT L_RETURNFLAG, L_LINESTATUS, COUNT(*) as count, SUM(L_QUANTITY) as sum_qty
        FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.LINEITEM 
        WHERE L_SHIPDATE <= '1998-12-01'
        GROUP BY L_RETURNFLAG, L_LINESTATUS
        ORDER BY L_RETURNFLAG, L_LINESTATUS
    "#;

    let mut stream = conn.query_arrow(query, &[], None).await.expect("Query failed");
    let mut total_rows = 0;

    while let Some(batch_result) = stream.next().await {
        let batch = batch_result.expect("Batch error");
        assert_eq!(batch.num_columns(), 4, "Expected 4 columns");
        total_rows += batch.num_rows();
    }

    assert!(total_rows > 0, "Expected aggregation results");
}

#[tokio::test]
async fn test_tpch_lineitem_aggregation_http() {
    use datafusion_table_providers::sql::db_connection_pool::dbconnection::AsyncDbConnection;
    use futures::StreamExt;

    let conn = get_snowflake_connection("http").await;

    // Test aggregation query
    let query = r#"
        SELECT L_RETURNFLAG, L_LINESTATUS, COUNT(*) as count, SUM(L_QUANTITY) as sum_qty
        FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.LINEITEM 
        WHERE L_SHIPDATE <= '1998-12-01'
        GROUP BY L_RETURNFLAG, L_LINESTATUS
        ORDER BY L_RETURNFLAG, L_LINESTATUS
    "#;

    let mut stream = conn.query_arrow(query, &[], None).await.expect("Query failed");
    let mut total_rows = 0;

    while let Some(batch_result) = stream.next().await {
        let batch = batch_result.expect("Batch error");
        assert_eq!(batch.num_columns(), 4, "Expected 4 columns");
        total_rows += batch.num_rows();
    }

    assert!(total_rows > 0, "Expected aggregation results");
}

#[tokio::test]
async fn test_tpch_join_adbc() {
    use datafusion_table_providers::sql::db_connection_pool::dbconnection::AsyncDbConnection;
    use futures::StreamExt;

    let conn = get_snowflake_connection("adbc").await;

    // Test join query
    let query = r#"
        SELECT n.N_NAME, r.R_NAME 
        FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.NATION n
        JOIN SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.REGION r ON n.N_REGIONKEY = r.R_REGIONKEY
        ORDER BY n.N_NAME
    "#;

    let mut stream = conn.query_arrow(query, &[], None).await.expect("Query failed");
    let mut total_rows = 0;

    while let Some(batch_result) = stream.next().await {
        let batch = batch_result.expect("Batch error");
        assert_eq!(batch.num_columns(), 2, "Expected 2 columns");
        total_rows += batch.num_rows();
    }

    assert_eq!(total_rows, 25, "Expected 25 joined rows");
}

#[tokio::test]
async fn test_tpch_join_http() {
    use datafusion_table_providers::sql::db_connection_pool::dbconnection::AsyncDbConnection;
    use futures::StreamExt;

    let conn = get_snowflake_connection("http").await;

    // Test join query
    let query = r#"
        SELECT n.N_NAME, r.R_NAME 
        FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.NATION n
        JOIN SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.REGION r ON n.N_REGIONKEY = r.R_REGIONKEY
        ORDER BY n.N_NAME
    "#;

    let mut stream = conn.query_arrow(query, &[], None).await.expect("Query failed");
    let mut total_rows = 0;

    while let Some(batch_result) = stream.next().await {
        let batch = batch_result.expect("Batch error");
        assert_eq!(batch.num_columns(), 2, "Expected 2 columns");
        total_rows += batch.num_rows();
    }

    assert_eq!(total_rows, 25, "Expected 25 joined rows");
}
