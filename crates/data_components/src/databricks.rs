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

use async_trait::async_trait;
use datafusion::{common::OwnedTableReference, datasource::TableProvider};
use deltalake::aws::storage::s3_constants::AWS_S3_ALLOW_UNSAFE_RENAME;
use deltalake::open_table_with_storage_options;
use secrets::{ExposeSecret, Secret};
use serde::Deserialize;
use std::{collections::HashMap, error::Error, fmt, sync::Arc};
use spark_connect_rust::{RemoteSparkSession};

use crate::{spark_connect, Read, ReadWrite};

use self::write::DeltaTableWriter;

mod write;

#[derive(Clone)]
pub struct Databricks {
    pub secret: Arc<Option<Secret>>,
    pub params: Arc<Option<HashMap<String, String>>>,
}
impl Databricks {
    #[must_use]
    pub async fn new(secret: Arc<Option<Secret>>, params: Arc<Option<HashMap<String, String>>>) -> Result<Self, Box<dyn Error>> {
        Ok(Self { secret, params })
    }

    async fn create_session(&self) -> Result<SparkSession, Box<dyn Error + Send + Sync>> {
        let param_deref = match self.params.as_ref() {
            None => return Err("Dataset params not found".into()),
            Some(params) => params,
        };
    
        let Some(endpoint) = param_deref.get("endpoint") else {
            return Err("Endpoint not found in dataset params".into());
        };

        let mut token = "Token not found in auth provider";
        if let Some(secret) = self.secret.as_ref() {
            if let Some(token_secret_val) = secret.get("token") {
                token = token_secret_val;
            };
        };
        let connection =format!("sc://{endpoint}:15002/;user_id=rust_test;session_id=0d2af2a9-cc3c-4d4b-bf27-e2fefeaca233");
        let session = RemoteSparkSession::remote(connection.as_str())
        .build()
        .await?;
        Ok(session)
    }
}

#[async_trait]
impl ReadWrite for Databricks {
    async fn table_provider(
        &self,
        table_reference: OwnedTableReference,
    ) -> Result<Arc<dyn TableProvider + 'static>, Box<dyn Error + Send + Sync>> {
        let spark_session = self.create_session().await.map_err(
            |_e| Box::new(SimpleError("Failed to create spark session".to_string())) as Box<dyn std::error::Error + Send + Sync>
        ).unwrap();

        let provider = spark_connect::get_table_provider(
            spark_session,
            table_reference,
        )
        .await?;
        Ok(provider)
        // DeltaTableWriter::create(delta_table).map_err(Into::into)
    }
}
#[derive(Debug)]
struct SimpleError(String);

// Implement the Display trait for SimpleError
impl fmt::Display for SimpleError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

// Implement the Error trait for SimpleError
impl std::error::Error for SimpleError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        None
    }

    fn description(&self) -> &str {
        "description() is deprecated; use Display"
    }

    fn cause(&self) -> Option<&dyn Error> {
        self.source()
    }

    
}


// async fn get_delta_table(
//     secret: Arc<Option<Secret>>,
//     table_reference: OwnedTableReference,
//     params: Arc<Option<HashMap<String, String>>>,
// ) -> Result<Arc<dyn TableProvider>, Box<dyn Error + Send + Sync>> {
//     // Needed to be able to load the s3:// scheme
//     deltalake::aws::register_handlers(None);
//     deltalake::azure::register_handlers(None);
//     let table_uri = resolve_table_uri(table_reference, &secret, params).await?;

//     let mut storage_options = HashMap::new();
//     if let Some(secret) = secret.as_ref() {
//         for (key, value) in secret.iter() {
//             if key == "token" {
//                 continue;
//             }
//             storage_options.insert(key.to_string(), value.expose_secret().clone());
//         }
//     };
//     storage_options.insert(AWS_S3_ALLOW_UNSAFE_RENAME.to_string(), "true".to_string());

//     let delta_table = open_table_with_storage_options(table_uri, storage_options).await?;

//     Ok(Arc::new(delta_table) as Arc<dyn TableProvider>)
// }

// #[derive(Deserialize)]
// struct DatabricksTablesApiResponse {
//     storage_location: String,
// }

// #[allow(clippy::implicit_hasher)]
// pub async fn resolve_table_uri(
//     table_reference: OwnedTableReference,
//     secret: &Arc<Option<Secret>>,
//     params: Arc<Option<HashMap<String, String>>>,
// ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
//     let params = match params.as_ref() {
//         None => return Err("Dataset params not found".into()),
//         Some(params) => params,
//     };

//     let Some(endpoint) = params.get("endpoint") else {
//         return Err("Endpoint not found in dataset params".into());
//     };

//     let table_name = table_reference.table();

//     let mut token = "Token not found in auth provider";
//     if let Some(secret) = secret.as_ref() {
//         if let Some(token_secret_val) = secret.get("token") {
//             token = token_secret_val;
//         };
//     };

//     let url = format!(
//         "{}/api/2.1/unity-catalog/tables/{}",
//         endpoint.trim_end_matches('/'),
//         table_name
//     );

//     let client = reqwest::Client::new();
//     let response = client.get(&url).bearer_auth(token).send().await?;

//     if response.status().is_success() {
//         let api_response: DatabricksTablesApiResponse = response.json().await?;
//         Ok(api_response.storage_location)
//     } else {
//         Err(format!(
//             "Failed to retrieve databricks table URI. Status: {}",
//             response.status()
//         )
//         .into())
//     }
// }
