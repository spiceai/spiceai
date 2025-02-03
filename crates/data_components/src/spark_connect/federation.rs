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

use std::sync::Arc;

use arrow::{array::RecordBatch, datatypes::SchemaRef};
use async_stream::stream;
use async_trait::async_trait;
use datafusion::{
    error::{DataFusionError, Result as DataFusionResult},
    execution::SendableRecordBatchStream,
    physical_plan::stream::RecordBatchStreamAdapter,
    sql::unparser::dialect::{CustomDialectBuilder, Dialect},
};
use datafusion_federation::{FederatedTableProviderAdaptor, FederatedTableSource};
use datafusion_federation_sql::{SQLExecutor, SQLFederationProvider, SQLTableSource};
use futures::Stream;

use crate::spark_connect::map_error_to_datafusion_err;

use super::SparkConnectTableProvider;

impl SparkConnectTableProvider {
    fn create_federated_table_source(
        self: Arc<Self>,
    ) -> DataFusionResult<Arc<dyn FederatedTableSource>> {
        let table_name = self.table_reference.to_quoted_string();
        tracing::trace!(
            table_name,
            %self.table_reference,
            "create_federated_table_source"
        );
        let schema = Arc::clone(&self.schema);
        let fed_provider = Arc::new(SQLFederationProvider::new(self));
        Ok(Arc::new(SQLTableSource::new_with_schema(
            fed_provider,
            table_name,
            schema,
        )?))
    }

    pub fn create_federated_table_provider(
        self: Arc<Self>,
    ) -> DataFusionResult<FederatedTableProviderAdaptor> {
        let table_source = Self::create_federated_table_source(Arc::clone(&self))?;
        Ok(FederatedTableProviderAdaptor::new_with_provider(
            table_source,
            self,
        ))
    }
}

#[async_trait]
impl SQLExecutor for SparkConnectTableProvider {
    fn name(&self) -> &'static str {
        "SparkConnect"
    }

    fn compute_context(&self) -> Option<String> {
        Some(self.join_push_down_context.clone())
    }

    fn dialect(&self) -> Arc<dyn Dialect> {
        Arc::new(
            CustomDialectBuilder::new()
                .with_interval_style(datafusion::sql::unparser::dialect::IntervalStyle::SQLStandard)
                .with_identifier_quote_style('`')
                .build(),
        )
    }

    fn execute(
        &self,
        query: &str,
        schema: SchemaRef,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            spark_query_to_stream(self.dataframe.clone().sparkSession(), query.to_string()),
        )))
    }

    async fn table_names(&self) -> DataFusionResult<Vec<String>> {
        Err(DataFusionError::NotImplemented(
            "table inference not implemented".to_string(),
        ))
    }

    async fn get_table_schema(&self, table_name: &str) -> DataFusionResult<SchemaRef> {
        Ok(self
            .dataframe
            .clone()
            .sparkSession()
            .table(table_name)
            .map_err(map_error_to_datafusion_err)?
            .limit(0)
            .collect()
            .await
            .map_err(map_error_to_datafusion_err)?
            .schema())
    }
}

#[allow(clippy::needless_pass_by_value)]
fn spark_query_to_stream(
    session: Arc<spark_connect_rs::SparkSession>,
    query: String,
) -> impl Stream<Item = DataFusionResult<RecordBatch>> {
    let session = Arc::clone(&session);

    stream! {
        let data = session
            .sql(&query)
            .await
            .map_err(map_error_to_datafusion_err)?
            .collect()
            .await
            .map_err(map_error_to_datafusion_err)?;
        yield (Ok(data))
    }
}
