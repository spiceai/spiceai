/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

use std::hash::BuildHasher;
use std::hash::Hash;
use std::hash::Hasher;
use std::sync::Arc;

use async_openai::types::embeddings::CreateEmbeddingRequest;
use async_openai::types::embeddings::EmbeddingInput;
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
    // Embedding keys could either be the full request (for distinguising between dimension count, encoding format, etc)
    // or just the individual input for less complex requests (e.g. via `.embed()` for some models instead of `.embed_request()`)
    EmbeddingRequest(&'a CreateEmbeddingRequest),
    EmbeddingInput(&'a str, &'a EmbeddingInput),
}

impl<'a> From<&'a CreateEmbeddingRequest> for CacheKey<'a> {
    fn from(embedding_request: &'a CreateEmbeddingRequest) -> Self {
        Self::EmbeddingRequest(embedding_request)
    }
}

impl<'a> From<(&'a str, &'a EmbeddingInput)> for CacheKey<'a> {
    fn from(input: (&'a str, &'a EmbeddingInput)) -> Self {
        let (model_name, input) = input;
        Self::EmbeddingInput(model_name, input)
    }
}

impl CacheKey<'_> {
    #[must_use]
    pub fn as_raw_key<T: Hasher>(&self, mut hasher: T) -> RawCacheKey {
        match self {
            Self::LogicalPlan(logical_plan) => logical_plan.hash(&mut hasher),
            Self::Search(search_key) => search_key.hash(&mut hasher),
            Self::EmbeddingRequest(embedding_request) => embedding_request.hash(&mut hasher),
            Self::EmbeddingInput(model_name, embedding_input) => {
                model_name.hash(&mut hasher);
                embedding_input.hash(&mut hasher);
            }
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

#[derive(Hash, Eq, PartialEq, Clone, Copy)]
pub struct RawCacheKey(u64);

impl RawCacheKey {
    #[must_use]
    pub fn new(key: u64) -> Self {
        Self(key)
    }

    #[must_use]
    pub fn as_u64(&self) -> u64 {
        self.0
    }
}

/// A hash builder that builds a hasher which simply passes through u64 values as-is.
/// This is useful to reduce hashing overhead when we already have a u64 hash key, as returned from `CacheKey::as_raw_key()`.
#[derive(Clone)]
pub(crate) struct PassthroughHashBuilder<T: BuildHasher + Clone + Send + Sync + 'static> {
    hasher: T,
}

impl<T: BuildHasher + Clone + Send + Sync + 'static> PassthroughHashBuilder<T> {
    pub(crate) fn new(hasher: T) -> Self {
        Self { hasher }
    }
}

impl<T: BuildHasher + Clone + Send + Sync + 'static> BuildHasher for PassthroughHashBuilder<T>
where
    <T as BuildHasher>::Hasher: Send + Sync + 'static,
{
    type Hasher = PassthroughHasher<T::Hasher>;

    fn build_hasher(&self) -> Self::Hasher {
        PassthroughHasher {
            hash: None,
            hasher: self.hasher.build_hasher(),
        }
    }
}

pub(crate) struct PassthroughHasher<T: Hasher + Send + Sync + 'static> {
    hash: Option<u64>,
    hasher: T,
}

impl<T: Hasher + Send + Sync + 'static> Hasher for PassthroughHasher<T> {
    fn finish(&self) -> u64 {
        self.hash.unwrap_or_else(|| self.hasher.finish())
    }

    // moka generates an internal UUID v4 for bucket IDs, which is a string
    // it re-uses the provided hash builder for hashing the value of the UUID, which is used to target a bucket segment
    // as a result, even though our keys are always u64, we also need to support hashing arbitrary byte slices (strings)
    //
    // to support this need, we fallback to the hash builder from the generic type for non-u64 inputs
    fn write(&mut self, bytes: &[u8]) {
        self.hasher.write(bytes);
    }

    fn write_u64(&mut self, i: u64) {
        self.hash = Some(i);
    }
}

#[cfg(test)]
mod tests {
    use std::hash::RandomState;

    use super::*;

    // explicitly allow this rule, because we're validating that the builtin u64 hash -> .write_u64() path works as expected
    #[expect(clippy::manual_hash_one)]
    #[test]
    fn test_passthrough_hasher() {
        // validate that `write_u64` and `write` produce the same hash result from a u64 input
        let mut hasher1 = PassthroughHashBuilder::new(RandomState::default()).build_hasher();
        hasher1.write_u64(42);
        let hash1 = hasher1.finish();
        assert_eq!(hash1, 42);

        let mut hasher2 = PassthroughHashBuilder::new(RandomState::default()).build_hasher();
        42u64.hash(&mut hasher2);
        let hash2 = hasher2.finish();

        assert_eq!(hash1, hash2);
    }
}
