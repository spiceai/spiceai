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

use arrow::record_batch::RecordBatch;
use models::model::Model;
use std::result::Result;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::DataFusion;

pub async fn run(
    m: &Model,
    df: Arc<RwLock<DataFusion>>,
) -> Result<RecordBatch, models::model::Error> {
    match df
        .read()
        .await
        .ctx
        .sql(
            &(format!(
                "select * from datafusion.public.{} order by ts asc",
                m.model.datasets[0]
            )),
        )
        .await
    {
        Ok(data) => match data.collect().await {
            Ok(d) => m.run(d),
            Err(e) => Err(models::model::Error::UnableToRunModel {
                source: Box::new(e),
            }),
        },
        Err(e) => Err(models::model::Error::UnableToRunModel {
            source: Box::new(e),
        }),
    }
}
