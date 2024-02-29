use async_trait::async_trait;
use datafusion::execution::context::SessionContext;
use deltalake::open_table_with_storage_options;
use snafu::prelude::*;
use std::pin::Pin;
use std::sync::Arc;
use std::{collections::HashMap, future::Future};

use spicepod::component::dataset::Dataset;

use crate::auth::AuthProvider;

use super::DataConnector;

pub struct Databricks {
    auth_provider: AuthProvider,
}

#[async_trait]
impl DataConnector for Databricks {
    fn new(
        auth_provider: AuthProvider,
        params: Arc<Option<HashMap<String, String>>>,
    ) -> Pin<Box<dyn Future<Output = super::Result<Self>> + Send>>
    where
        Self: Sized,
    {
        // Needed to be able to load the s3:// scheme
        deltalake::aws::register_handlers(None);
        Box::pin(async move { Ok(Self { auth_provider }) })
    }

    fn get_all_data(
        &self,
        dataset: &Dataset,
    ) -> Pin<Box<dyn Future<Output = Vec<arrow::record_batch::RecordBatch>> + Send>> {
        // SUPER HACK: Initialize a new DataFusion SessionContext, get the TableProvider and use that to query for the data. This needs to be reworked.
        let auth_provider = self.auth_provider.clone();
        let dataset = dataset.clone();
        Box::pin(async move {
            let ctx = SessionContext::new();

            let table_provider = match get_table_provider(auth_provider, &dataset).await {
                Ok(provider) => provider,
                Err(e) => {
                    tracing::error!("Failed to get table provider: {}", e);
                    return vec![];
                }
            };

            let _ = ctx.register_table("temp_table", table_provider);

            let sql = "SELECT * FROM temp_table;";

            let df = match ctx.sql(sql).await {
                Ok(df) => df,
                Err(e) => {
                    tracing::error!("Failed to execute query: {}", e);
                    return vec![];
                }
            };

            df.collect().await.unwrap_or_else(|e| {
                tracing::error!("Failed to collect results: {}", e);
                vec![]
            })
        })
    }

    fn has_table_provider(&self) -> bool {
        true
    }

    async fn get_table_provider(
        &self,
        dataset: &Dataset,
    ) -> std::result::Result<Arc<dyn datafusion::datasource::TableProvider>, super::Error> {
        get_table_provider(self.auth_provider.clone(), dataset).await
    }
}

async fn get_table_provider(
    auth_provider: AuthProvider,
    dataset: &Dataset,
) -> std::result::Result<Arc<dyn datafusion::datasource::TableProvider>, super::Error> {
    // TODO: Translate the databricks URI
    let table_uri = "s3://databricks-workspace-stack-f0780-bucket/unity-catalog/686830279408652/__unitystorage/catalogs/1c3649de-309b-4c73-805e-8cf93aa4ee25/tables/c704d041-f240-43eb-b648-88efb26fdefc";

    let mut storage_options = HashMap::new();
    for (key, value) in auth_provider.iter() {
        if !key.starts_with("AWS_") {
            continue;
        }
        storage_options.insert(key.to_string(), value.to_string());
    }

    let delta_table: deltalake::DeltaTable =
        open_table_with_storage_options(table_uri, storage_options)
            .await
            .boxed()
            .context(super::UnableToGetTableProviderSnafu)?;

    Ok(Arc::new(delta_table))
}
