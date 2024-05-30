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

use std::{sync::Arc};

use tokio::time::{sleep, Duration};

use app::AppBuilder;
use arrow::array::RecordBatch;
use bb8_postgres::tokio_postgres::{self, NoTls};
use futures::TryStreamExt;
use runtime::{config::Config, datafusion::DataFusion, Runtime};
use spicepod::component::{
    dataset::Dataset, params::Params, runtime::ResultsCache, secrets::SpiceSecretStore,
};
use tokio::{sync::RwLock};

use crate::{init_tracing, pg_server};

fn make_s3_tpch_dataset(name: &str) -> Dataset {
    let mut test_dataset = Dataset::new(
        format!("s3://spiceai-demo-datasets/tpch/{name}/").to_string(),
        name.to_string(),
    );
    test_dataset.params = Some(Params::from_string_map(
        vec![("file_format".to_string(), "parquet".to_string())]
            .into_iter()
            .collect(),
    ));

    test_dataset
}

#[tokio::test]
async fn pg_server_query_test() -> Result<(), String> {
    let _tracing = init_tracing(None);
    let app = AppBuilder::new("test")
        .with_secret_store(SpiceSecretStore::File)
        .with_dataset(make_s3_tpch_dataset("customer"))
        .build();

    let df = Arc::new(RwLock::new(DataFusion::new()));
    let df_copy = Arc::clone(&df);

    let mut rt = Runtime::new(Some(app), df, Arc::new(vec![])).await;

    rt.init_results_cache().await;
    rt.load_secrets().await;
    rt.load_datasets().await;

    let pg_server_future =  runtime::pg_server::start("127.0.0.1:5444".parse().unwrap(), df_copy);

    //let tokio_rt = tokio::runtime::Runtime::new().unwrap();

    // Spawn a task to run `start_servers` in the background
    let handle = tokio::spawn(async move {
        let _ = pg_server_future.await;
    });

    sleep(Duration::from_secs(1)).await;


    // psql -h 127.0.0.1 -p 5444
    let (mut pg, connection) = tokio_postgres::connect(
        format!("host=127.0.0.1 port=5444").as_str(),
        NoTls,
    ).await.unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {e}");
        }
    });

    println!("Query1: {:?}", pg.query("SELECT 1;", &[]).await.unwrap());
    println!("Query2: {:?}", pg.query("SELECT c_name from customer limit 1;", &[]).await.unwrap());



    let rows = {
        let tx = pg.transaction().await.unwrap();
        let rows = tx.query("SELECT c_name from customer limit 1;", &[]).await.unwrap();
        tx.commit().await.unwrap();
        rows
    };

    Ok(())
}
