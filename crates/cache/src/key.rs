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

use std::hash::Hash;
use std::hash::Hasher;
use std::sync::Arc;

use datafusion::common::ParamValues;
use datafusion::logical_expr::LogicalPlan;
use datafusion::scalar::ScalarValue;
use datafusion::sql::sqlparser::ast::Expr;

// To avoid a circular dependency, we define a placeholder for a SearchKey
// SearchRequest implements Into<SearchKey> in the `search` runtime module.
// TODO: Move SearchRequest into the `search` crate to prevent circular dependency, to reuse here?
// https://github.com/spiceai/spiceai/issues/6018
#[derive(Hash)]
pub struct SearchKey {
    text: Arc<str>,
    datasets: Option<Vec<Arc<str>>>,
    limit: usize,
    where_cond: Option<Expr>,
    additional_columns: Option<Vec<Arc<str>>>,
    keywords: Vec<Arc<str>>,
}

impl SearchKey {
    #[must_use]
    pub fn new(
        text: Arc<str>,
        datasets: Option<Vec<Arc<str>>>,
        limit: usize,
        where_cond: Option<Expr>,
        additional_columns: Option<Vec<Arc<str>>>,
        keywords: Vec<Arc<str>>,
    ) -> Self {
        Self {
            text,
            datasets,
            limit,
            where_cond,
            additional_columns,
            keywords,
        }
    }
}

#[derive(Clone, Copy)]
pub enum CacheKey<'a> {
    LogicalPlan(&'a LogicalPlan),
    Query(&'a str, Option<&'a ParamValues>),
    Search(&'a SearchKey),
    ClientSupplied(&'a str),
}

impl CacheKey<'_> {
    #[must_use]
    pub fn as_raw_key<T: Hasher>(&self, mut hasher: T) -> RawCacheKey {
        match self {
            Self::LogicalPlan(logical_plan) => logical_plan.hash(&mut hasher),
            Self::Search(search_key) => search_key.hash(&mut hasher),
            Self::Query(sql, param_values) => {
                sql.hash(&mut hasher);
                if let Some(params) = param_values {
                    match params {
                        ParamValues::List(vec) => vec.hash(&mut hasher),
                        ParamValues::Map(hash_map) => {
                            // implementing Hash for HashMap
                            let mut pairs: Vec<(&String, &ScalarValue)> = hash_map.iter().collect();
                            pairs.sort_by(|a, b| a.0.cmp(b.0)); // Sort by keys

                            for (key, value) in pairs {
                                key.hash(&mut hasher);
                                value.hash(&mut hasher);
                            }
                        }
                    }
                }
            }
            Self::ClientSupplied(user_key) => user_key.hash(&mut hasher),
        }
        RawCacheKey(hasher.finish())
    }
}

#[derive(Hash, Eq, PartialEq)]
pub struct RawCacheKey(u64);

impl RawCacheKey {
    #[must_use]
    pub fn as_u64(&self) -> u64 {
        self.0
    }
}
