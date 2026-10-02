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

//! Two SMB shares on one host, read through the registry the runtime uses.
//!
//! Runs only against a live SMB server, named by `SPICE_SMB_TEST_HOST`
//! (`host:port`), `SPICE_SMB_TEST_USER` and `SPICE_SMB_TEST_PASS`, which
//! serves a share `data` holding `sales/sales.csv` and a share `other`
//! holding `o.csv`. Without the host it reports that it was skipped.
//!
//! Regression test for #14550: the registry keys a store by `smb://host:port`
//! and `DataFusion` looks it up at query time by that path-less URL, so the
//! one store on a host has to serve every share named by the locations it
//! is handed.

#![cfg(feature = "smb")]

use std::sync::Arc;

use datafusion::execution::object_store::ObjectStoreRegistry;
use futures::TryStreamExt;
use object_store::ObjectStoreExt;
use object_store::path::Path;
use runtime_object_store::registry::SpiceObjectStoreRegistry;
use tokio::runtime::Handle;
use url::Url;

struct LiveServer {
    host: String,
    fragment: String,
}

fn live_server() -> Option<LiveServer> {
    let host = std::env::var("SPICE_SMB_TEST_HOST").ok()?;
    let user = std::env::var("SPICE_SMB_TEST_USER").ok()?;
    let pass = std::env::var("SPICE_SMB_TEST_PASS").ok()?;
    let port = host.rsplit_once(':').map_or("445", |(_, port)| port);
    // Encoded the way the connector encodes it, so a password holding `&`,
    // `%`, `=` or `#` survives the fragment.
    let fragment = url::form_urlencoded::Serializer::new(String::new())
        .append_pair("port", port)
        .append_pair("user", &user)
        .append_pair("pass", &pass)
        .finish();
    Some(LiveServer { host, fragment })
}

#[tokio::test]
async fn two_shares_on_one_host_are_each_served_from_their_own_share() {
    let Some(server) = live_server() else {
        eprintln!("skipped: set SPICE_SMB_TEST_HOST, SPICE_SMB_TEST_USER and SPICE_SMB_TEST_PASS");
        return;
    };
    let dataset_url = |path: &str| {
        Url::parse(&format!("smb://{}/{path}#{}", server.host, server.fragment))
            .expect("the dataset URL is well formed")
    };
    let registry = SpiceObjectStoreRegistry::new(Handle::current());

    // Registering the two datasets, in this order, builds the store from the
    // `data` URL and hands the same store back for the `other` one.
    let first = registry
        .get_store(&dataset_url("data/sales/sales.csv"))
        .expect("the first dataset's store is built from its URL");
    let second = registry
        .get_store(&dataset_url("other/o.csv"))
        .expect("the second dataset reuses the store registered for the host");
    assert!(
        Arc::ptr_eq(&first, &second),
        "both datasets on one host must resolve to one registered store"
    );

    // At query time `ListingTableUrl::object_store` looks the store up by the
    // URL without its path, so the share has to come from each location.
    let query_time_url = Url::parse(&format!("smb://{}/", server.host))
        .expect("the query-time lookup URL is well formed");
    let store = registry
        .get_store(&query_time_url)
        .expect("the query-time lookup finds the registered store");

    let other = store
        .head(&Path::from("other/o.csv"))
        .await
        .expect("o.csv is served from the `other` share, not looked up on `data`");
    assert!(other.size > 0, "o.csv on the `other` share is not empty");

    let data = store
        .head(&Path::from("data/sales/sales.csv"))
        .await
        .expect("sales.csv is still served from the `data` share");
    assert!(
        data.size > other.size,
        "sales.csv holds more rows than o.csv"
    );

    let listed: Vec<Path> = store
        .list(Some(&Path::from("other")))
        .map_ok(|meta| meta.location)
        .try_collect()
        .await
        .expect("listing the `other` share succeeds");
    assert_eq!(
        listed,
        vec![Path::from("other/o.csv")],
        "a listing names its objects under the share it was asked for"
    );
}
