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
        let api_key = env::var("API_KEY").expect("API_KEY not found");
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
        String::from("+----------+----------------------+-------------+\n| VendorID | tpep_pickup_datetime | fare_amount |\n+----------+----------------------+-------------+\n| 1        | 2024-01-03T13:34:41  | 1.5         |\n| 1        | 2024-01-06T14:49:10  | 2.0         |\n| 1        | 2024-01-16T07:28:44  | 2.0         |\n| 1        | 2024-01-18T02:11:51  | 2.0         |\n| 1        | 2024-01-18T17:47:40  | 2.0         |\n+----------+----------------------+-------------+")
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
            .query(
                r#"select VendorID, trip_distance, tpep_pickup_datetime from taxi_trips limit 10;"#,
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
            .query(
                "select VendorID, trip_distance, tpep_pickup_datetime from taxi_trips limit 10000",
            )
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
}
