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

use std::{collections::HashSet, sync::Arc};

use tokio::time::Instant;
use uuid::Uuid;

use crate::datafusion::DataFusion;

use super::{tracker::QueryTracker, Query};

pub struct QueryBuilder<'a> {
    df: Arc<DataFusion>,
    sql: &'a str,
    query_id: Uuid,
}

impl<'a> QueryBuilder<'a> {
    pub fn new(sql: &'a str, df: Arc<DataFusion>) -> Self {
        Self {
            df,
            sql,
            query_id: Uuid::new_v4(),
        }
    }

    #[must_use]
    pub fn query_id(mut self, query_id: Uuid) -> Self {
        self.query_id = query_id;
        self
    }

    #[must_use]
    pub fn build(self) -> Query {
        let sql: Arc<str> = self.sql.into();
        Query {
            df: Arc::clone(&self.df),
            sql: Arc::clone(&sql),
            tracker: QueryTracker {
                schema: None,
                query_duration_secs: None,
                query_execution_duration_secs: None,
                rows_produced: 0,
                results_cache_hit: None,
                is_accelerated: None,
                error_message: None,
                error_code: None,
                query_duration_timer: Instant::now(),
                query_execution_duration_timer: Instant::now(),
                datasets: Arc::new(HashSet::default()),
            },
        }
    }
}
