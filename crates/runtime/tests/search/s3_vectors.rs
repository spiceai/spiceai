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

use aws_config::Region;
use aws_credential_types::Credentials;
use aws_sdk_credential_bridge::default_aws_config;
use s3_vectors::{Client, DeleteIndexInput, S3Vectors};
use snafu::ResultExt;

use crate::utils::verify_env_secret_exists;

use spicepod::vector::VectorStore;

pub(super) async fn prepare_for_aws_tests(
    store: &VectorStore,
    predelete_index: bool,
) -> Result<(), anyhow::Error> {
    for env_var in ["AWS_S3_VECTORS_KEY", "AWS_S3_VECTORS_SECRET"] {
        verify_env_secret_exists(env_var)
            .await
            .map_err(anyhow::Error::msg)?;
    }

    if predelete_index {
        let bucket_name = store
            .params
            .as_ref()
            .and_then(|p| p.as_string_map().get("s3_vectors_bucket").cloned())
            .unwrap_or_default();
        let region = store
            .params
            .as_ref()
            .and_then(|p| p.as_string_map().get("s3_vectors_aws_region").cloned())
            .unwrap_or_default();
        let index_name = store
            .params
            .as_ref()
            .and_then(|p| p.as_string_map().get("s3_vectors_index").cloned())
            .unwrap_or_default();
        let config = default_aws_config()
            .region(Region::new(region))
            .credentials_provider(Credentials::new(
                std::env::var("AWS_S3_VECTORS_KEY").ok().unwrap_or_default(),
                std::env::var("AWS_S3_VECTORS_SECRET")
                    .ok()
                    .unwrap_or_default(),
                None,
                None,
                "S3Vectors",
            ))
            .load()
            .await;

        let s3_vector_client = Client::new(&config);

        let input = DeleteIndexInput::builder()
            .set_index_name(Some(index_name.to_string()))
            .set_vector_bucket_name(Some(bucket_name.to_string()))
            .build()?;

        let _ = s3_vector_client.delete_index(input).await.boxed().map_err(|e| {
            tracing::warn!("failed to delete index {index_name} before test. This may just be because index does not exist. Error: {e}. ");
            anyhow::anyhow!(e)
        })?;
    }
    Ok(())
}
