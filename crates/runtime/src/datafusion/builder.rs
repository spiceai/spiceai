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

use std::{
    collections::HashSet,
    sync::{Arc, RwLock},
};

use cache::QueryResultsCacheProvider;
use datafusion::{
    catalog_common::{CatalogProvider, MemoryCatalogProvider},
    execution::SessionStateBuilder,
    prelude::{SessionConfig, SessionContext},
};
use datafusion_federation::FederationAnalyzerRule;
use tokio::sync::RwLock as TokioRwLock;

use crate::{embeddings, object_store_registry::default_runtime_env, status};

use super::{
    extension::{bytes_processed::BytesProcessedOptimizerRule, SpiceQueryPlanner},
    schema::SpiceSchemaProvider,
    DataFusion, SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA, SPICE_METADATA_SCHEMA,
    SPICE_RUNTIME_SCHEMA,
};

pub struct DataFusionBuilder {
    config: SessionConfig,
    status: Arc<status::RuntimeStatus>,
    cache_provider: Option<Arc<QueryResultsCacheProvider>>,
}

impl DataFusionBuilder {
    #[must_use]
    pub fn new(status: Arc<status::RuntimeStatus>) -> Self {
        let mut df_config = SessionConfig::new()
            .with_information_schema(true)
            .with_create_default_catalog_and_schema(false);

        df_config.options_mut().sql_parser.dialect = "PostgreSQL".to_string();
        df_config.options_mut().catalog.default_catalog = SPICE_DEFAULT_CATALOG.to_string();
        df_config.options_mut().catalog.default_schema = SPICE_DEFAULT_SCHEMA.to_string();
        df_config
            .options_mut()
            .execution
            .listing_table_ignore_subdirectory = false;

        // There are some unidentified bugs in DataFusion that cause schema checks to fail for aggregate functions.
        // Spice is affected by this - skip the check until all bugs are fixed.
        // Tracking issue: https://github.com/apache/datafusion/issues/12733
        df_config
            .options_mut()
            .execution
            .skip_physical_aggregate_schema_check = true;

        Self {
            config: df_config,
            status,
            cache_provider: None,
        }
    }

    #[must_use]
    pub fn with_cache_provider(mut self, cache_provider: Arc<QueryResultsCacheProvider>) -> Self {
        self.cache_provider = Some(cache_provider);
        self
    }

    /// Builds the `DataFusion` instance.
    ///
    /// # Panics
    ///
    /// Panics if the `DataFusion` instance cannot be built due to errors in registering functions or schemas.
    #[must_use]
    pub fn build(self) -> DataFusion {
        let mut state = SessionStateBuilder::new()
            .with_config(self.config)
            .with_default_features()
            .with_query_planner(Arc::new(SpiceQueryPlanner::new()))
            .with_runtime_env(default_runtime_env())
            .build();

        if let Err(e) = datafusion_functions_json::register_all(&mut state) {
            panic!("Unable to register JSON functions: {e}");
        };

        let ctx = SessionContext::new_with_state(state);
        ctx.add_analyzer_rule(Arc::new(FederationAnalyzerRule::new()));
        ctx.add_optimizer_rule(Arc::new(BytesProcessedOptimizerRule::new()));
        ctx.register_udf(embeddings::cosine_distance::CosineDistance::new().into());
        ctx.register_udf(crate::datafusion::udf::Greatest::new().into());
        ctx.register_udf(crate::datafusion::udf::Least::new().into());
        let catalog = MemoryCatalogProvider::new();
        let default_schema = SpiceSchemaProvider::new();
        let runtime_schema = SpiceSchemaProvider::new();
        let metadata_schema = SpiceSchemaProvider::new();

        match catalog.register_schema(SPICE_DEFAULT_SCHEMA, Arc::new(default_schema)) {
            Ok(_) => {}
            Err(e) => {
                panic!("Unable to register default schema: {e}");
            }
        }

        match catalog.register_schema(SPICE_RUNTIME_SCHEMA, Arc::new(runtime_schema)) {
            Ok(_) => {}
            Err(e) => {
                panic!("Unable to register spice runtime schema: {e}");
            }
        }

        match catalog.register_schema(SPICE_METADATA_SCHEMA, Arc::new(metadata_schema)) {
            Ok(_) => {}
            Err(e) => {
                panic!("Unable to register spice runtime schema: {e}");
            }
        }

        ctx.register_catalog(SPICE_DEFAULT_CATALOG, Arc::new(catalog));

        DataFusion {
            runtime_status: self.status,
            ctx: Arc::new(ctx),
            data_writers: RwLock::new(HashSet::new()),
            cache_provider: RwLock::new(self.cache_provider),
            pending_sink_tables: TokioRwLock::new(Vec::new()),
            accelerated_tables: TokioRwLock::new(HashSet::new()),
        }
    }
}
