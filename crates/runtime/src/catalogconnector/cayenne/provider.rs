/*
Copyright 2026 The Spice.ai OSS Authors

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
pub use cayenne::CayenneCatalogProvider;

use crate::catalogconnector::PartitionAwareCatalog;

#[async_trait]
impl PartitionAwareCatalog for CayenneCatalogProvider {
    async fn table_partition_expr(
        &self,
        schema_name: &str,
        table_name: &str,
    ) -> crate::catalogconnector::Result<Option<String>> {
        let metadata_table_name = format!("{schema_name}/{table_name}");
        let metadata = self
            .metadata_catalog()
            .get_table(&metadata_table_name)
            .await
            .map_err(
                |source| crate::catalogconnector::Error::PartitionMetadataRead {
                    schema_name: schema_name.to_string(),
                    table_name: table_name.to_string(),
                    source: Box::new(source),
                },
            )?;

        Ok(metadata.partition_column)
    }
}
