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

//! Each dataset's object store, chosen by the object path.
//!
//! `DataFusion` keeps one object store per URL scheme and host in a runtime, so two
//! datasets in one bucket would share whichever store registered last, credentials
//! included. A [`PrefixRoutingStore`] is the one store registered for a scheme and
//! host; it sends each request to the store of the dataset whose prefix holds the
//! path, so every dataset reads and writes only with its own store. A path under no
//! dataset's prefix goes to the store something else registered for that bucket
//! before Cayenne did, and is an error when there is none.

use std::collections::HashMap;
use std::fmt::{self, Display, Formatter};
use std::ops::Range;
use std::sync::{Arc, LazyLock};

use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use futures::stream::{self, BoxStream};
use object_store::path::Path;
use object_store::{
    CopyOptions, Error as ObjectStoreError, GetOptions, GetResult, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    RenameOptions,
};
#[cfg(test)]
use object_store::ObjectStoreExt;
use parking_lot::RwLock;

const STORE_NAME: &str = "cayenne";

/// The routing store for each `scheme://host` a Cayenne dataset has registered.
static ROUTERS: LazyLock<RwLock<HashMap<String, Arc<PrefixRoutingStore>>>> =
    LazyLock::new(Default::default);

fn authority(url: &url::Url) -> String {
    format!("{}://{}", url.scheme(), url.host_str().unwrap_or_default())
}

/// The object path a URL addresses, without its leading `/`.
fn url_prefix(url: &url::Url) -> String {
    url.path().trim_matches('/').to_string()
}

/// Routes requests under `url`'s path to `store` and returns the routing store for
/// `url`'s scheme and host, creating it on first use.
pub(crate) fn register(url: &url::Url, store: Arc<dyn ObjectStore>) -> Arc<PrefixRoutingStore> {
    let router = {
        let mut routers = ROUTERS.write();
        Arc::clone(
            routers
                .entry(authority(url))
                .or_insert_with(|| Arc::new(PrefixRoutingStore::new(authority(url)))),
        )
    };
    router.insert(&url_prefix(url), store);
    router
}

/// The routing store for `url`'s scheme and host, when a dataset registered one.
pub(crate) fn router_for(url: &url::Url) -> Option<Arc<PrefixRoutingStore>> {
    ROUTERS.read().get(&authority(url)).cloned()
}

/// One object store per dataset prefix, under one URL scheme and host.
#[derive(Debug)]
pub struct PrefixRoutingStore {
    authority: String,
    /// `(prefix, store)`, longest prefix first, so the most specific prefix wins.
    routes: RwLock<Vec<(String, Arc<dyn ObjectStore>)>>,
    /// The store something other than Cayenne registered for this scheme and host,
    /// which serves paths under no dataset's prefix.
    fallback: RwLock<Option<Arc<dyn ObjectStore>>>,
}

impl PrefixRoutingStore {
    fn new(authority: String) -> Self {
        Self {
            authority,
            routes: RwLock::new(Vec::new()),
            fallback: RwLock::new(None),
        }
    }

    fn insert(&self, prefix: &str, store: Arc<dyn ObjectStore>) {
        let mut routes = self.routes.write();
        if let Some(existing) = routes.iter_mut().find(|(p, _)| p == prefix) {
            existing.1 = store;
            return;
        }
        routes.push((prefix.to_string(), store));
        routes.sort_by(|(a, _), (b, _)| b.len().cmp(&a.len()));
    }

    /// Keeps `store`, registered for this scheme and host by something other than
    /// Cayenne, as the store for paths under no dataset's prefix. The first one wins.
    pub(crate) fn keep_fallback(&self, store: Arc<dyn ObjectStore>) {
        let mut fallback = self.fallback.write();
        if fallback.is_none() {
            *fallback = Some(store);
        }
    }

    /// The number of dataset prefixes routed, for tests and diagnostics.
    #[must_use]
    pub fn route_count(&self) -> usize {
        self.routes.read().len()
    }

    fn under(prefix: &str, location: &str) -> bool {
        prefix.is_empty()
            || location == prefix
            || location
                .strip_prefix(prefix)
                .is_some_and(|rest| rest.starts_with('/'))
    }

    fn route(&self, location: &str) -> Result<Arc<dyn ObjectStore>, ObjectStoreError> {
        if let Some((_, store)) = self
            .routes
            .read()
            .iter()
            .find(|(prefix, _)| Self::under(prefix, location))
        {
            return Ok(Arc::clone(store));
        }
        self.fallback
            .read()
            .clone()
            .ok_or_else(|| ObjectStoreError::Generic {
                store: STORE_NAME,
                source: format!(
                    "No Cayenne dataset's storage contains '{}/{location}', so it was not accessed. A dataset reads and writes only under its own data path.",
                    self.authority
                )
                .into(),
            })
    }

    fn route_path(&self, location: &Path) -> Result<Arc<dyn ObjectStore>, ObjectStoreError> {
        self.route(location.as_ref())
    }

    fn route_prefix(&self, prefix: Option<&Path>) -> Result<Arc<dyn ObjectStore>, ObjectStoreError> {
        self.route(prefix.map_or("", Path::as_ref))
    }

    fn same_store(&self, from: &Path, to: &Path) -> Result<Arc<dyn ObjectStore>, ObjectStoreError> {
        let source = self.route_path(from)?;
        let target = self.route_path(to)?;
        if Arc::ptr_eq(&source, &target) {
            Ok(source)
        } else {
            Err(ObjectStoreError::Generic {
                store: STORE_NAME,
                source: format!(
                    "Cannot copy '{from}' to '{to}' in '{}': they belong to different datasets' storage.",
                    self.authority
                )
                .into(),
            })
        }
    }
}

impl Display for PrefixRoutingStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "PrefixRoutingStore({})", self.authority)
    }
}

/// Deletes in batches, each batch sent to the store of the dataset its paths are in.
const DELETE_BATCH: usize = 1_000;

#[async_trait]
impl ObjectStore for PrefixRoutingStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult, ObjectStoreError> {
        self.route_path(location)?
            .put_opts(location, payload, opts)
            .await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>, ObjectStoreError> {
        self.route_path(location)?
            .put_multipart_opts(location, opts)
            .await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> Result<GetResult, ObjectStoreError> {
        self.route_path(location)?.get_opts(location, options).await
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[Range<u64>],
    ) -> Result<Vec<Bytes>, ObjectStoreError> {
        self.route_path(location)?.get_ranges(location, ranges).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path, ObjectStoreError>>,
    ) -> BoxStream<'static, Result<Path, ObjectStoreError>> {
        let routes: Vec<(String, Arc<dyn ObjectStore>)> = self.routes.read().clone();
        let fallback = self.fallback.read().clone();
        let authority = self.authority.clone();
        locations
            .ready_chunks(DELETE_BATCH)
            .flat_map(move |batch| {
                // Group the batch by store, keeping each path's error in place.
                let mut groups: Vec<(Arc<dyn ObjectStore>, Vec<Path>)> = Vec::new();
                let mut failures: Vec<Result<Path, ObjectStoreError>> = Vec::new();
                for item in batch {
                    let path = match item {
                        Ok(path) => path,
                        Err(error) => {
                            failures.push(Err(error));
                            continue;
                        }
                    };
                    let store = routes
                        .iter()
                        .find(|(prefix, _)| Self::under(prefix, path.as_ref()))
                        .map(|(_, store)| Arc::clone(store))
                        .or_else(|| fallback.clone());
                    let Some(store) = store else {
                        failures.push(Err(ObjectStoreError::Generic {
                            store: STORE_NAME,
                            source: format!(
                                "No Cayenne dataset's storage contains '{authority}/{path}', so it was not deleted."
                            )
                            .into(),
                        }));
                        continue;
                    };
                    match groups.iter_mut().find(|(s, _)| Arc::ptr_eq(s, &store)) {
                        Some((_, paths)) => paths.push(path),
                        None => groups.push((store, vec![path])),
                    }
                }
                let deletes = groups.into_iter().map(|(store, paths)| {
                    store.delete_stream(stream::iter(paths.into_iter().map(Ok)).boxed())
                });
                stream::iter(failures).chain(stream::iter(deletes).flatten())
            })
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta, ObjectStoreError>> {
        match self.route_prefix(prefix) {
            Ok(store) => store.list(prefix),
            Err(error) => stream::once(async { Err(error) }).boxed(),
        }
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta, ObjectStoreError>> {
        match self.route_prefix(prefix) {
            Ok(store) => store.list_with_offset(prefix, offset),
            Err(error) => stream::once(async { Err(error) }).boxed(),
        }
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&Path>,
    ) -> Result<ListResult, ObjectStoreError> {
        self.route_prefix(prefix)?.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> Result<(), ObjectStoreError> {
        self.same_store(from, to)?.copy_opts(from, to, options).await
    }

    async fn rename_opts(
        &self,
        from: &Path,
        to: &Path,
        options: RenameOptions,
    ) -> Result<(), ObjectStoreError> {
        self.same_store(from, to)?.rename_opts(from, to, options).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;

    fn router_with(routes: &[(&str, &Arc<dyn ObjectStore>)]) -> PrefixRoutingStore {
        let router = PrefixRoutingStore::new("s3://bucket".to_string());
        for (prefix, store) in routes {
            router.insert(prefix, Arc::clone(store));
        }
        router
    }

    #[tokio::test]
    async fn each_dataset_prefix_reaches_only_its_own_store() {
        let a: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let b: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let router = router_with(&[("base/orders", &a), ("base/orders_2", &b)]);

        router
            .put(&Path::from("base/orders/t/1.vortex"), PutPayload::from_static(b"a"))
            .await
            .expect("put under orders");
        router
            .put(&Path::from("base/orders_2/t/1.vortex"), PutPayload::from_static(b"b"))
            .await
            .expect("put under orders_2");

        // `orders_2` shares a string prefix with `orders` but not a path segment.
        let in_a: Vec<_> = a.list(None).map(|m| m.expect("list a").location.to_string()).collect().await;
        let in_b: Vec<_> = b.list(None).map(|m| m.expect("list b").location.to_string()).collect().await;
        assert_eq!(in_a, vec!["base/orders/t/1.vortex".to_string()]);
        assert_eq!(in_b, vec!["base/orders_2/t/1.vortex".to_string()]);
    }

    #[tokio::test]
    async fn a_path_under_no_dataset_is_an_error_without_a_fallback() {
        let a: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let router = router_with(&[("base/orders", &a)]);
        let error = router
            .get(&Path::from("base/customers/t/1.vortex"))
            .await
            .expect_err("a path outside every dataset is refused");
        assert!(
            error
                .to_string()
                .contains("No Cayenne dataset's storage contains 's3://bucket/base/customers/t/1.vortex'"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn a_path_under_no_dataset_uses_the_fallback() {
        let a: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let other: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        other
            .put(&Path::from("raw/file.parquet"), PutPayload::from_static(b"x"))
            .await
            .expect("seed fallback");
        let router = router_with(&[("base/orders", &a)]);
        router.keep_fallback(Arc::clone(&other));
        let bytes = router
            .get(&Path::from("raw/file.parquet"))
            .await
            .expect("fallback serves the path")
            .bytes()
            .await
            .expect("bytes");
        assert_eq!(bytes.as_ref(), b"x");
    }

    #[tokio::test]
    async fn deletes_split_by_dataset_and_report_unrouted_paths() {
        let a: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let b: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        for (store, key) in [(&a, "p/a/1"), (&b, "p/b/1")] {
            store
                .put(&Path::from(key), PutPayload::from_static(b"x"))
                .await
                .expect("seed");
        }
        let router = router_with(&[("p/a", &a), ("p/b", &b)]);
        let results: Vec<_> = router
            .delete_stream(
                stream::iter(["p/a/1", "p/b/1", "p/c/1"].map(|k| Ok(Path::from(k)))).boxed(),
            )
            .collect()
            .await;
        let deleted: Vec<String> = results
            .iter()
            .filter_map(|r| r.as_ref().ok().map(ToString::to_string))
            .collect();
        let errors = results.iter().filter(|r| r.is_err()).count();
        assert_eq!(errors, 1, "the path under no dataset fails: {results:?}");
        assert!(deleted.contains(&"p/a/1".to_string()) && deleted.contains(&"p/b/1".to_string()));
        assert!(a.head(&Path::from("p/a/1")).await.is_err(), "deleted from a");
        assert!(b.head(&Path::from("p/b/1")).await.is_err(), "deleted from b");
    }

    #[tokio::test]
    async fn copy_across_datasets_is_refused() {
        let a: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let b: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        a.put(&Path::from("p/a/1"), PutPayload::from_static(b"x"))
            .await
            .expect("seed");
        let router = router_with(&[("p/a", &a), ("p/b", &b)]);
        let error = router
            .copy(&Path::from("p/a/1"), &Path::from("p/b/1"))
            .await
            .expect_err("cross-dataset copy is refused");
        assert!(error.to_string().contains("belong to different datasets' storage"), "{error}");
        assert!(b.head(&Path::from("p/b/1")).await.is_err(), "nothing copied");
    }
}
