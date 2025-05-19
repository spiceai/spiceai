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

use arrow::array::RecordBatch;
use futures::TryStreamExt;
use spicepod::component::dataset::Dataset;

use crate::{
    flight::{create_flight_client, start_spice_test_app},
    init_tracing,
    utils::test_request_context,
};

#[tokio::test]
async fn test_flight_do_get_dict_types() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let ds = Dataset::new(
                "s3://spiceai-public-datasets/test_different_formats/dictionaries.parquet",
                "test_table_dict_types",
            );

            let (channel, _) = start_spice_test_app(None, None, Some(ds)).await?;
            let mut client = create_flight_client(channel, None)?;

            let ticket = arrow_flight::Ticket::new(
                "select * from test_table_dict_types".as_bytes().to_vec(),
            );
            let stream = client.do_get(ticket).await?;

            let data: Vec<RecordBatch> = stream.try_collect().await?;
            let result_str = arrow::util::pretty::pretty_format_batches(&data)?;

            insta::assert_snapshot!("dict_types", result_str);

            Ok(())
        })
        .await
}
