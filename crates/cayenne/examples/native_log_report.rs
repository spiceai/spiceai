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

//! Reads an object-native metastore directory (`{metadata_dir}/object_native`) and
//! reports what its commits hold: how many there are, how many carry a source
//! position, and whether each position committed alone or with other changes,
//! plus the size of every checkpoint.
//!
//! ```sh
//! cargo run --release -p cayenne --example native_log_report -- <object_native dir>
//! ```

#![expect(clippy::expect_used, reason = "an experiment binary")]

use std::collections::BTreeMap;
use std::sync::Arc;

use futures::TryStreamExt;
use object_store::{ObjectStore, ObjectStoreExt, path::Path};
use object_store_occ::headlog::{Group, Reader, Record};
use prost::Message;

/// A sealed batch: the encoded log records it holds, oldest first.
#[derive(Clone, PartialEq, Message)]
struct Batch {
    #[prost(bytes = "vec", repeated, tag = "1")]
    records: Vec<Vec<u8>>,
}

#[derive(Default)]
struct Counts {
    commits: usize,
    spilled: usize,
    spilled_missing: usize,
    position_alone: usize,
    position_with_other_changes: usize,
    kinds: BTreeMap<&'static str, usize>,
}

impl Counts {
    /// `spilled` maps a spilled payload's digest, in hex, to its bytes.
    fn add(&mut self, encoded: &[u8], spilled: &BTreeMap<String, bytes::Bytes>) {
        let record = Record::decode(encoded).expect("log record");
        self.commits += 1;
        let payload = match &record.spilled {
            Some(reference) => {
                self.spilled += 1;
                let digest: String = reference
                    .digest
                    .iter()
                    .map(|b| format!("{b:02x}"))
                    .collect();
                let Some(bytes) = spilled.get(&digest) else {
                    self.spilled_missing += 1;
                    return;
                };
                bytes.clone()
            }
            None => record.payload,
        };
        let mut names = Vec::new();
        for payload in Group::decode(payload).expect("group").payloads {
            let record = cayenne_metastore_actions::Record::decode(payload).expect("record");
            names.extend(
                record
                    .actions
                    .iter()
                    .map(cayenne_metastore_actions::Action::name),
            );
        }
        if names.contains(&"AdvanceSourcePosition") {
            if names.iter().all(|name| *name == "AdvanceSourcePosition") {
                self.position_alone += 1;
            } else {
                self.position_with_other_changes += 1;
            }
        }
        for name in names {
            *self.kinds.entry(name).or_default() += 1;
        }
    }
}

async fn listed(store: &Arc<dyn ObjectStore>, prefix: &Path) -> Vec<object_store::ObjectMeta> {
    let mut objects: Vec<_> = store.list(Some(prefix)).try_collect().await.expect("list");
    objects.sort_by(|a, b| a.location.cmp(&b.location));
    objects
}

#[tokio::main]
async fn main() {
    let directory = std::env::args().nth(1).expect("object_native directory");
    let store: Arc<dyn ObjectStore> =
        Arc::new(object_store_occ::LocalConditionalPut::new(&directory).expect("store"));
    let prefix = Path::from("catalog");
    let (reader, _) = Reader::open(Arc::clone(&store), prefix.clone())
        .await
        .expect("open");
    let head = &reader.state().head;
    // Nothing is deleted yet, so every sealed batch is still present; the head's
    // window holds the commits not yet sealed.
    // Spilled payloads live at `objects/<generation>/records/<digest>`.
    let mut spilled = BTreeMap::new();
    for meta in listed(&store, &prefix.clone().join("objects")).await {
        let parts: Vec<_> = meta.location.parts().collect();
        if parts.len() >= 2 && parts[parts.len() - 2].as_ref() == "records" {
            let digest = parts[parts.len() - 1].as_ref().to_string();
            let bytes = store
                .get(&meta.location)
                .await
                .expect("spilled record")
                .bytes()
                .await
                .expect("bytes");
            spilled.insert(digest, bytes);
        }
    }
    let mut counts = Counts::default();
    for meta in listed(&store, &prefix.clone().join("wal")).await {
        let bytes = store
            .get(&meta.location)
            .await
            .expect("batch")
            .bytes()
            .await
            .expect("bytes");
        for encoded in Batch::decode(bytes).expect("batch").records {
            counts.add(&encoded, &spilled);
        }
    }
    for entry in &head.window {
        counts.add(&entry.encoded, &spilled);
    }
    println!(
        "head commit={} floor={} commits_read={} spilled={} spilled_missing={} position_alone={} position_with_other_changes={}",
        head.commit,
        head.floor,
        counts.commits,
        counts.spilled,
        counts.spilled_missing,
        counts.position_alone,
        counts.position_with_other_changes
    );
    println!("actions: {:?}", counts.kinds);
    for meta in listed(&store, &prefix.clone().join("checkpoints")).await {
        println!("checkpoint {} bytes={}", meta.location, meta.size);
    }
}
