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
    /// Hash this key's payload into `hasher`. Used by both [`Self::as_raw_key`]
    /// and [`Self::as_raw_key_in_namespace`] so the payload byte stream stays
    /// identical regardless of whether a namespace prefix was mixed in.
    fn hash_payload<T: Hasher>(&self, hasher: &mut T) {
        match self {
            Self::LogicalPlan(logical_plan) => logical_plan.hash(hasher),
            Self::Search(search_key) => search_key.hash(hasher),
            Self::EmbeddingRequest(embedding_request) => embedding_request.hash(hasher),
            Self::EmbeddingInput(model_name, embedding_input) => {
                model_name.hash(hasher);
                embedding_input.hash(hasher);
            }
            Self::Query(sql, param_values) => {
                sql.hash(hasher);
                if let Some(params) = param_values {
                    match params {
                        ParamValues::List(vec) => {
                            for item in vec {
                                item.value().hash(hasher);
                            }
                        }
                        ParamValues::Map(hash_map) => {
                            // implementing Hash for HashMap
                            let mut pairs: Vec<_> = hash_map.iter().collect();
                            pairs.sort_by(|a, b| a.0.cmp(b.0)); // Sort by keys

                            for (key, value) in pairs {
                                key.hash(hasher);
                                value.value().hash(hasher);
                            }
                        }
                    }
                }
            }
            Self::ClientSupplied(user_key) => user_key.hash(hasher),
        }
    }

    /// Compute the raw cache key with no namespace mixed in. Use this for
    /// surfaces whose result is a pure function of the inputs and is safe
    /// to share across all callers — most importantly the embeddings cache,
    /// where `(model, input) -> embedding` is permission-independent.
    #[must_use]
    pub fn as_raw_key<T: Hasher>(&self, mut hasher: T) -> RawCacheKey {
        self.hash_payload(&mut hasher);
        RawCacheKey(hasher.finish())
    }

    /// Compute the raw cache key with a namespace prefix folded into the
    /// hash. Two requests whose `(namespace_tag, namespace_id)` differ
    /// hash to distinct keys for otherwise-identical payloads, which is
    /// what makes cache hits safe under per-user authentication.
    ///
    /// `namespace_tag` is the discriminant of the namespace kind
    /// (`0` = public/shared, `1` = principal, `2` = system) and
    /// `namespace_id` is the principal's stable opaque id (empty for
    /// public/system).
    ///
    /// The byte stream hashed is:
    /// `[namespace_tag][namespace_id.len() as u64 LE][namespace_id...][payload...]`
    /// so that `(tag=1, id="abc")` and `(tag=1, id="a")` followed by a
    /// payload starting with `"bc"` cannot collide.
    #[must_use]
    pub fn as_raw_key_in_namespace<T: Hasher>(
        &self,
        mut hasher: T,
        namespace_tag: u8,
        namespace_id: &[u8],
    ) -> RawCacheKey {
        hasher.write_u8(namespace_tag);
        hasher.write_u64(namespace_id.len() as u64);
        hasher.write(namespace_id);
        self.hash_payload(&mut hasher);
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
    type Hasher = PassthroughHasher<T>;

    fn build_hasher(&self) -> Self::Hasher {
        PassthroughHasher {
            hash: None,
            hasher: None,
            builder: self.hasher.clone(),
        }
    }
}

pub(crate) struct PassthroughHasher<T: BuildHasher> {
    hash: Option<u64>,
    /// Built only once bytes are written. The `u64` keys every lookup and insert
    /// hash never need it, and building one can allocate — the streaming
    /// `XxHash3_64` does.
    hasher: Option<T::Hasher>,
    builder: T,
}

impl<T: BuildHasher> Hasher for PassthroughHasher<T> {
    fn finish(&self) -> u64 {
        match (self.hash, &self.hasher) {
            (Some(hash), _) => hash,
            (None, Some(hasher)) => hasher.finish(),
            (None, None) => self.builder.build_hasher().finish(),
        }
    }

    // moka generates an internal UUID v4 for bucket IDs, which is a string
    // it re-uses the provided hash builder for hashing the value of the UUID, which is used to target a bucket segment
    // as a result, even though our keys are always u64, we also need to support hashing arbitrary byte slices (strings)
    //
    // to support this need, we fallback to the hash builder from the generic type for non-u64 inputs
    fn write(&mut self, bytes: &[u8]) {
        self.hasher
            .get_or_insert_with(|| self.builder.build_hasher())
            .write(bytes);
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

    /// A plan's key is the value of hashing that plan write by write, for every
    /// configured algorithm — including `ahash`, which folds integer writes.
    #[test]
    fn a_plan_key_is_the_same_however_its_bytes_reach_the_hasher() {
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use datafusion::logical_expr::{col, table_scan};
        use spicepod::component::caching::HashingAlgorithm;

        let schema = Schema::new(
            (0..200)
                .map(|i| {
                    let data_type = if i % 2 == 0 {
                        DataType::Int64
                    } else {
                        DataType::Utf8
                    };
                    Field::new(format!("c{i}"), data_type, true)
                })
                .collect::<Vec<_>>(),
        );
        let plan = |columns: usize| {
            table_scan(Some("wide"), &schema, None)
                .expect("a scan of the wide schema")
                .project((0..columns).map(|i| col(format!("c{i}"))))
                .expect("a projection of its columns")
                .build()
                .expect("the plan")
        };
        let (wide, narrow) = (plan(200), plan(1));

        for algorithm in [
            HashingAlgorithm::Ahash,
            HashingAlgorithm::Siphash,
            HashingAlgorithm::Blake3,
            HashingAlgorithm::XXH3,
            HashingAlgorithm::XXH32,
            HashingAlgorithm::XXH64,
            HashingAlgorithm::XXH128,
        ] {
            let builder = crate::get_hash_builder(algorithm).expect("a supported algorithm");
            let mut write_by_write = builder.build_hasher();
            write_by_write.write_u8(1);
            write_by_write.write_u64(b"abc".len() as u64);
            write_by_write.write(b"abc");
            wide.hash(&mut write_by_write);

            let key = CacheKey::LogicalPlan(&wide).as_raw_key_in_namespace(
                builder.build_hasher(),
                1,
                b"abc",
            );
            assert_eq!(key.as_u64(), write_by_write.finish(), "{algorithm:?}");
        }

        let ahash = crate::get_hash_builder(HashingAlgorithm::Ahash).expect("ahash");
        let key = |plan: &LogicalPlan| {
            CacheKey::LogicalPlan(plan)
                .as_raw_key_in_namespace(ahash.build_hasher(), 0, &[])
                .as_u64()
        };
        assert_eq!(key(&wide), key(&wide));
        assert_ne!(key(&wide), key(&narrow));
    }
}
