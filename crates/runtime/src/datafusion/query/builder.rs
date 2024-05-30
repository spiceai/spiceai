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

use std::{sync::Arc, time::SystemTime};

use datafusion::execution::context::SQLOptions;
use uuid::Uuid;

use crate::datafusion::DataFusion;

use super::Query;

#[allow(clippy::module_name_repetitions)]
pub struct QueryBuilder {
    df: Arc<DataFusion>,
    sql: String,
    query_id: Uuid,
    nsql: Option<String>,
    restricted_sql_options: Option<SQLOptions>,
}

impl QueryBuilder {
    pub fn new(sql: String, df: Arc<DataFusion>) -> Self {
        Self {
            df,
            sql,
            query_id: Uuid::new_v4(),
            nsql: None,
            restricted_sql_options: None,
        }
    }

    #[must_use]
    pub fn nsql(mut self, nsql: Option<String>) -> Self {
        self.nsql = nsql;
        self
    }

    #[must_use]
    pub fn query_id(mut self, query_id: Uuid) -> Self {
        self.query_id = query_id;
        self
    }

    #[must_use]
    pub fn restricted_sql_options(mut self, restricted_sql_options: Option<SQLOptions>) -> Self {
        self.restricted_sql_options = restricted_sql_options;
        self
    }

    #[must_use]
    pub fn build(self) -> Query {
        Query {
            df: self.df,
            sql: self.sql,
            query_id: self.query_id,
            schema: None,
            nsql: self.nsql,
            start_time: SystemTime::now(),
            end_time: None,
            execution_time: None,
            rows_produced: None,
            results_cache_hit: None,
            restricted_sql_options: self.restricted_sql_options,
        }
    }
}
