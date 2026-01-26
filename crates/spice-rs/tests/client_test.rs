#[cfg(test)]
mod tests {
    use arrow::array::{ArrayRef, Float64Array, Int32Array};
    use arrow::compute::concat_batches;
    use arrow::datatypes::{
        DataType::{Float64, Int32},
        Field, Schema,
    };
    use arrow::record_batch::RecordBatch;
    use arrow::util::pretty::pretty_format_batches;
    use futures::stream::StreamExt;
    use spiceai::{Client, ClientBuilder};
    use std::env;
    use std::path::Path;
    use std::sync::Arc;

    async fn new_cloud_client() -> Client {
        dotenv::from_path(Path::new(".env.local")).ok();
        let api_key =
            env::var("SCP_SPICEAI_TPCH_API_KEY").expect("SCP_SPICEAI_TPCH_API_KEY not found");
        ClientBuilder::new()
            .api_key(&api_key)
            .use_spiceai_cloud()
            .build()
            .await
            .expect("Failed to create client")
    }

    #[tokio::test]
    async fn test_new_client_builder() {
        new_cloud_client().await;
    }

    async fn new_local_client() -> Client {
        ClientBuilder::new()
            .build()
            .await
            .expect("Failed to create client")
    }

    pub fn create_param_batch() -> RecordBatch {
        let fields = vec![
            Arc::new(Field::new("$1", Int32, true)),
            Arc::new(Field::new("$2", Float64, true)),
        ];
        let columns = vec![
            Arc::new(Int32Array::from(vec![1])) as ArrayRef,
            Arc::new(Float64Array::from(vec![1.0])) as ArrayRef,
        ];

        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
            .expect("Failed to create RecordBatch")
    }

    fn get_expected_result() -> String {
        String::from(
            "+----------+----------------------+-------------+\n| VendorID | tpep_pickup_datetime | fare_amount |\n+----------+----------------------+-------------+\n| 1        | 2024-01-03T13:34:41  | 1.5         |\n| 1        | 2024-01-06T14:49:10  | 2.0         |\n| 1        | 2024-01-16T07:28:44  | 2.0         |\n| 1        | 2024-01-18T02:11:51  | 2.0         |\n| 1        | 2024-01-18T17:47:40  | 2.0         |\n+----------+----------------------+-------------+",
        )
    }

    #[tokio::test]
    async fn test_local_query() {
        let _ = rustls::crypto::CryptoProvider::install_default(
            rustls::crypto::aws_lc_rs::default_provider(),
        );
        let spice_client = new_local_client().await;
        match spice_client
            .query("SELECT VendorID, tpep_pickup_datetime, fare_amount FROM taxi_trips WHERE VendorID == 1 and fare_amount > 1.0 ORDER BY fare_amount, tpep_pickup_datetime LIMIT 5;")
            .await
        {
            Ok(mut flight_data_stream) => {
                let mut batches = Vec::new();
                // Read back RecordBatches
                while let Some(batch) = flight_data_stream.next().await {
                    match batch {
                        Ok(batch) => {
                            batches.push(batch);
                        }
                        Err(e) => {
                            panic!("Error: {e}")
                        }
                    }
                }
                let batch_concat = concat_batches(&batches[0].schema(), &batches).expect("Failed to concat batches");
                let formatted = format!("{}", pretty_format_batches(&[batch_concat.clone()]).expect("Failed to format batches"));
                assert_eq!(batch_concat.num_columns(), 3);
                assert_eq!(batch_concat.num_rows(), 5);
                assert_eq!(formatted, get_expected_result());
            }
            Err(e) => {
                panic!("Error: {e}");
            }
        };
    }

    #[tokio::test]
    async fn test_local_query_with_params() {
        let _ = rustls::crypto::CryptoProvider::install_default(
            rustls::crypto::aws_lc_rs::default_provider(),
        );
        let spice_client = new_local_client().await;
        let params = create_param_batch();
        match spice_client
            .query_with_params(
                "SELECT VendorID, tpep_pickup_datetime, fare_amount FROM taxi_trips WHERE VendorID == $1 and fare_amount > $2 ORDER BY fare_amount, tpep_pickup_datetime LIMIT 5;",
                Some(params),
            )
            .await
        {
            Ok(mut flight_data_stream) => {
                let mut batches = Vec::new();
                // Read back RecordBatches
                while let Some(batch) = flight_data_stream.next().await {
                    match batch {
                        Ok(batch) => {
                            batches.push(batch);
                        }
                        Err(e) => {
                            panic!("Error: {e}")
                        }
                    }
                }
                let batch_concat = concat_batches(&batches[0].schema(), &batches).expect("Failed to concat batches");
                let formatted = format!("{}", pretty_format_batches(&[batch_concat.clone()]).expect("Failed to format batches"));
                assert_eq!(batch_concat.num_columns(), 3);
                assert_eq!(batch_concat.num_rows(), 5);
                assert_eq!(formatted, get_expected_result());
            }
            Err(e) => {
                panic!("Error: {e}");
            }
        };
    }

    #[tokio::test]
    async fn test_query() {
        let spice_client = new_cloud_client().await;
        match spice_client
            .query("select c_custkey, c_name, c_nationkey from tpch.customer limit 10;")
            .await
        {
            Ok(mut flight_data_stream) => {
                let mut batches = Vec::new();
                // Read back RecordBatches
                while let Some(batch) = flight_data_stream.next().await {
                    match batch {
                        Ok(batch) => {
                            batches.push(batch);
                        }
                        Err(e) => {
                            panic!("Error: {e}")
                        }
                    };
                }
                let batch_concat = concat_batches(&batches[0].schema(), &batches)
                    .expect("Failed to concat batches");
                assert_eq!(batch_concat.num_columns(), 3);
                assert_eq!(batch_concat.num_rows(), 10);
            }
            Err(e) => {
                panic!("Error: {e}");
            }
        };
    }

    #[tokio::test]
    async fn test_query_streaming() {
        let spice_client = new_cloud_client().await;
        match spice_client
            .query("select l_orderkey, l_partkey, l_quantity from tpch.lineitem limit 10000")
            .await
        {
            Ok(mut flight_data_stream) => {
                // Read back RecordBatches
                let mut num_batches = 0;
                let mut total_rows = 0;
                while let Some(batch) = flight_data_stream.next().await {
                    match batch {
                        Ok(batch) => {
                            num_batches += 1;
                            total_rows += batch.num_rows();
                        }
                        Err(e) => {
                            panic!("Error: {e}")
                        }
                    };
                }
                assert_eq!(total_rows, 10000);
                assert_ne!(num_batches, 1);
            }
            Err(e) => {
                panic!("Error: {e}");
            }
        };
    }

    /// Test querying integer and string types from the nation table
    #[tokio::test]
    async fn test_tpch_nation_types() {
        let spice_client = new_cloud_client().await;
        match spice_client
            .query("SELECT n_nationkey, n_name, n_regionkey, n_comment FROM tpch.nation ORDER BY n_nationkey LIMIT 5")
            .await
        {
            Ok(mut flight_data_stream) => {
                let mut batches = Vec::new();
                while let Some(batch) = flight_data_stream.next().await {
                    match batch {
                        Ok(batch) => batches.push(batch),
                        Err(e) => panic!("Error: {e}"),
                    }
                }
                let batch_concat = concat_batches(&batches[0].schema(), &batches)
                    .expect("Failed to concat batches");
                // nation table has 4 columns: n_nationkey (int), n_name (string), n_regionkey (int), n_comment (string)
                assert_eq!(batch_concat.num_columns(), 4);
                assert_eq!(batch_concat.num_rows(), 5);
            }
            Err(e) => panic!("Error: {e}"),
        };
    }

    /// Test querying decimal types from the orders table
    #[tokio::test]
    async fn test_tpch_orders_decimal_types() {
        let spice_client = new_cloud_client().await;
        match spice_client
            .query("SELECT o_orderkey, o_custkey, o_totalprice, o_orderstatus FROM tpch.orders ORDER BY o_orderkey LIMIT 10")
            .await
        {
            Ok(mut flight_data_stream) => {
                let mut batches = Vec::new();
                while let Some(batch) = flight_data_stream.next().await {
                    match batch {
                        Ok(batch) => batches.push(batch),
                        Err(e) => panic!("Error: {e}"),
                    }
                }
                let batch_concat = concat_batches(&batches[0].schema(), &batches)
                    .expect("Failed to concat batches");
                // o_totalprice is decimal type, o_orderstatus is string
                assert_eq!(batch_concat.num_columns(), 4);
                assert_eq!(batch_concat.num_rows(), 10);
            }
            Err(e) => panic!("Error: {e}"),
        };
    }

    /// Test querying date types from the orders table
    #[tokio::test]
    async fn test_tpch_orders_date_types() {
        let spice_client = new_cloud_client().await;
        match spice_client
            .query("SELECT o_orderkey, o_orderdate, o_orderpriority FROM tpch.orders ORDER BY o_orderdate LIMIT 10")
            .await
        {
            Ok(mut flight_data_stream) => {
                let mut batches = Vec::new();
                while let Some(batch) = flight_data_stream.next().await {
                    match batch {
                        Ok(batch) => batches.push(batch),
                        Err(e) => panic!("Error: {e}"),
                    }
                }
                let batch_concat = concat_batches(&batches[0].schema(), &batches)
                    .expect("Failed to concat batches");
                // o_orderdate is date type
                assert_eq!(batch_concat.num_columns(), 3);
                assert_eq!(batch_concat.num_rows(), 10);
            }
            Err(e) => panic!("Error: {e}"),
        };
    }

    /// Test querying multiple decimal columns from the lineitem table
    #[tokio::test]
    async fn test_tpch_lineitem_decimal_types() {
        let spice_client = new_cloud_client().await;
        match spice_client
            .query("SELECT l_orderkey, l_quantity, l_extendedprice, l_discount, l_tax FROM tpch.lineitem LIMIT 20")
            .await
        {
            Ok(mut flight_data_stream) => {
                let mut batches = Vec::new();
                while let Some(batch) = flight_data_stream.next().await {
                    match batch {
                        Ok(batch) => batches.push(batch),
                        Err(e) => panic!("Error: {e}"),
                    }
                }
                let batch_concat = concat_batches(&batches[0].schema(), &batches)
                    .expect("Failed to concat batches");
                // l_quantity, l_extendedprice, l_discount, l_tax are all decimal types
                assert_eq!(batch_concat.num_columns(), 5);
                assert_eq!(batch_concat.num_rows(), 20);
            }
            Err(e) => panic!("Error: {e}"),
        };
    }

    /// Test querying multiple date columns from the lineitem table
    #[tokio::test]
    async fn test_tpch_lineitem_date_types() {
        let spice_client = new_cloud_client().await;
        match spice_client
            .query(
                "SELECT l_orderkey, l_shipdate, l_commitdate, l_receiptdate FROM tpch.lineitem LIMIT 15",
            )
            .await
        {
            Ok(mut flight_data_stream) => {
                let mut batches = Vec::new();
                while let Some(batch) = flight_data_stream.next().await {
                    match batch {
                        Ok(batch) => batches.push(batch),
                        Err(e) => panic!("Error: {e}"),
                    }
                }
                let batch_concat = concat_batches(&batches[0].schema(), &batches)
                    .expect("Failed to concat batches");
                // l_shipdate, l_commitdate, l_receiptdate are all date types
                assert_eq!(batch_concat.num_columns(), 4);
                assert_eq!(batch_concat.num_rows(), 15);
            }
            Err(e) => panic!("Error: {e}"),
        };
    }

    /// Test aggregation query with GROUP BY
    #[tokio::test]
    async fn test_tpch_aggregation() {
        let spice_client = new_cloud_client().await;
        match spice_client
            .query("SELECT n_regionkey, COUNT(*) as nation_count FROM tpch.nation GROUP BY n_regionkey ORDER BY n_regionkey")
            .await
        {
            Ok(mut flight_data_stream) => {
                let mut batches = Vec::new();
                while let Some(batch) = flight_data_stream.next().await {
                    match batch {
                        Ok(batch) => batches.push(batch),
                        Err(e) => panic!("Error: {e}"),
                    }
                }
                let batch_concat = concat_batches(&batches[0].schema(), &batches)
                    .expect("Failed to concat batches");
                assert_eq!(batch_concat.num_columns(), 2);
                // 5 regions in TPCH
                assert_eq!(batch_concat.num_rows(), 5);
            }
            Err(e) => panic!("Error: {e}"),
        };
    }

    /// Test query with SUM aggregation on decimal columns
    #[tokio::test]
    async fn test_tpch_sum_aggregation() {
        let spice_client = new_cloud_client().await;
        match spice_client
            .query("SELECT o_orderstatus, COUNT(*) as order_count, SUM(o_totalprice) as total_value FROM tpch.orders GROUP BY o_orderstatus ORDER BY o_orderstatus")
            .await
        {
            Ok(mut flight_data_stream) => {
                let mut batches = Vec::new();
                while let Some(batch) = flight_data_stream.next().await {
                    match batch {
                        Ok(batch) => batches.push(batch),
                        Err(e) => panic!("Error: {e}"),
                    }
                }
                let batch_concat = concat_batches(&batches[0].schema(), &batches)
                    .expect("Failed to concat batches");
                assert_eq!(batch_concat.num_columns(), 3);
                // Order statuses: F (fulfilled), O (open), P (pending)
                assert!(batch_concat.num_rows() >= 1);
            }
            Err(e) => panic!("Error: {e}"),
        };
    }

    /// Test query with JOIN between tables
    #[tokio::test]
    async fn test_tpch_join_query() {
        let spice_client = new_cloud_client().await;
        match spice_client
            .query("SELECT n.n_name, r.r_name FROM tpch.nation n JOIN tpch.region r ON n.n_regionkey = r.r_regionkey ORDER BY n.n_name LIMIT 10")
            .await
        {
            Ok(mut flight_data_stream) => {
                let mut batches = Vec::new();
                while let Some(batch) = flight_data_stream.next().await {
                    match batch {
                        Ok(batch) => batches.push(batch),
                        Err(e) => panic!("Error: {e}"),
                    }
                }
                let batch_concat = concat_batches(&batches[0].schema(), &batches)
                    .expect("Failed to concat batches");
                assert_eq!(batch_concat.num_columns(), 2);
                assert_eq!(batch_concat.num_rows(), 10);
            }
            Err(e) => panic!("Error: {e}"),
        };
    }

    /// Test querying the region table (small reference table)
    #[tokio::test]
    async fn test_tpch_region_table() {
        let spice_client = new_cloud_client().await;
        match spice_client
            .query("SELECT r_regionkey, r_name, r_comment FROM tpch.region ORDER BY r_regionkey")
            .await
        {
            Ok(mut flight_data_stream) => {
                let mut batches = Vec::new();
                while let Some(batch) = flight_data_stream.next().await {
                    match batch {
                        Ok(batch) => batches.push(batch),
                        Err(e) => panic!("Error: {e}"),
                    }
                }
                let batch_concat = concat_batches(&batches[0].schema(), &batches)
                    .expect("Failed to concat batches");
                assert_eq!(batch_concat.num_columns(), 3);
                // TPCH has exactly 5 regions
                assert_eq!(batch_concat.num_rows(), 5);
            }
            Err(e) => panic!("Error: {e}"),
        };
    }

    /// Test querying the supplier table with decimal (acctbal) type
    #[tokio::test]
    async fn test_tpch_supplier_types() {
        let spice_client = new_cloud_client().await;
        match spice_client
            .query("SELECT s_suppkey, s_name, s_acctbal, s_nationkey FROM tpch.supplier ORDER BY s_suppkey LIMIT 10")
            .await
        {
            Ok(mut flight_data_stream) => {
                let mut batches = Vec::new();
                while let Some(batch) = flight_data_stream.next().await {
                    match batch {
                        Ok(batch) => batches.push(batch),
                        Err(e) => panic!("Error: {e}"),
                    }
                }
                let batch_concat = concat_batches(&batches[0].schema(), &batches)
                    .expect("Failed to concat batches");
                // s_acctbal is decimal type
                assert_eq!(batch_concat.num_columns(), 4);
                assert_eq!(batch_concat.num_rows(), 10);
            }
            Err(e) => panic!("Error: {e}"),
        };
    }

    /// Test querying the part table with various types
    #[tokio::test]
    async fn test_tpch_part_types() {
        let spice_client = new_cloud_client().await;
        match spice_client
            .query("SELECT p_partkey, p_name, p_brand, p_size, p_retailprice FROM tpch.part ORDER BY p_partkey LIMIT 10")
            .await
        {
            Ok(mut flight_data_stream) => {
                let mut batches = Vec::new();
                while let Some(batch) = flight_data_stream.next().await {
                    match batch {
                        Ok(batch) => batches.push(batch),
                        Err(e) => panic!("Error: {e}"),
                    }
                }
                let batch_concat = concat_batches(&batches[0].schema(), &batches)
                    .expect("Failed to concat batches");
                // p_size is int, p_retailprice is decimal
                assert_eq!(batch_concat.num_columns(), 5);
                assert_eq!(batch_concat.num_rows(), 10);
            }
            Err(e) => panic!("Error: {e}"),
        };
    }

    /// Test querying the partsupp table
    #[tokio::test]
    async fn test_tpch_partsupp_types() {
        let spice_client = new_cloud_client().await;
        match spice_client
            .query(
                "SELECT ps_partkey, ps_suppkey, ps_availqty, ps_supplycost FROM tpch.partsupp LIMIT 10",
            )
            .await
        {
            Ok(mut flight_data_stream) => {
                let mut batches = Vec::new();
                while let Some(batch) = flight_data_stream.next().await {
                    match batch {
                        Ok(batch) => batches.push(batch),
                        Err(e) => panic!("Error: {e}"),
                    }
                }
                let batch_concat = concat_batches(&batches[0].schema(), &batches)
                    .expect("Failed to concat batches");
                // ps_availqty is int, ps_supplycost is decimal
                assert_eq!(batch_concat.num_columns(), 4);
                assert_eq!(batch_concat.num_rows(), 10);
            }
            Err(e) => panic!("Error: {e}"),
        };
    }

    /// Test query with calculated/derived columns
    #[tokio::test]
    async fn test_tpch_calculated_columns() {
        let spice_client = new_cloud_client().await;
        match spice_client
            .query("SELECT l_orderkey, l_quantity, l_extendedprice, l_discount, l_extendedprice * (1 - l_discount) as net_price FROM tpch.lineitem LIMIT 10")
            .await
        {
            Ok(mut flight_data_stream) => {
                let mut batches = Vec::new();
                while let Some(batch) = flight_data_stream.next().await {
                    match batch {
                        Ok(batch) => batches.push(batch),
                        Err(e) => panic!("Error: {e}"),
                    }
                }
                let batch_concat = concat_batches(&batches[0].schema(), &batches)
                    .expect("Failed to concat batches");
                assert_eq!(batch_concat.num_columns(), 5);
                assert_eq!(batch_concat.num_rows(), 10);
            }
            Err(e) => panic!("Error: {e}"),
        };
    }

    /// Test query with WHERE clause filtering
    #[tokio::test]
    async fn test_tpch_filtered_query() {
        let spice_client = new_cloud_client().await;
        match spice_client
            .query("SELECT c_custkey, c_name, c_acctbal FROM tpch.customer WHERE c_acctbal > 0 ORDER BY c_acctbal DESC LIMIT 10")
            .await
        {
            Ok(mut flight_data_stream) => {
                let mut batches = Vec::new();
                while let Some(batch) = flight_data_stream.next().await {
                    match batch {
                        Ok(batch) => batches.push(batch),
                        Err(e) => panic!("Error: {e}"),
                    }
                }
                let batch_concat = concat_batches(&batches[0].schema(), &batches)
                    .expect("Failed to concat batches");
                // c_acctbal is decimal type
                assert_eq!(batch_concat.num_columns(), 3);
                assert_eq!(batch_concat.num_rows(), 10);
            }
            Err(e) => panic!("Error: {e}"),
        };
    }
}
