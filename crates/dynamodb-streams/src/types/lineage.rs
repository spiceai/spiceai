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
use super::super::sdk_client::DynamodbSDKClient;
use super::Shard;
use std::pin::Pin;

use aws_sdk_dynamodbstreams::types::Record;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;
use tracing;

/// A representation of shard lineage(parent and children).
///
/// Because an application must always process a parent shard before it processes a child shard,
/// this struct ensures that the stream records are processed in the correct order.
#[derive(Debug, Clone)]
pub struct Lineage {
    /// The parent shard of this children.
    shard: Shard,

    /// The lineages from the child of this shard.
    children: Vec<Lineage>,
}

impl Lineage {
    fn new(shard: Shard) -> Self {
        Self {
            shard,
            children: vec![],
        }
    }

    fn shard_id(&self) -> &str {
        self.shard.id()
    }

    fn parent_shard_id(&self) -> Option<&str> {
        self.shard.parent_shard_id()
    }

    fn set_children(&mut self, children: Vec<Lineage>) {
        self.children = children;
    }

    /// Return true if the passed shard id is equal to the lineage's shard id
    /// or the lineage has the shard having passed shard id as its descendant.
    fn has(&self, shard_id: Option<&str>) -> bool {
        if let Some(id) = shard_id {
            if self.shard_id() == id {
                true
            } else {
                self.children.iter().any(|child| child.has(shard_id))
            }
        } else {
            false
        }
    }

    /// Return true if the passed shard id is equal to the lineage' shard parent's.
    fn is_child(&self, shard_id: &str) -> bool {
        if let Some(parent_shard_id) = self.parent_shard_id() {
            parent_shard_id == shard_id
        } else {
            false
        }
    }

    /// Set lineages as the shard's descendant.
    fn set_descendant(&mut self, desc: &Lineage) {
        if let Some(parent_shard_id) = desc.parent_shard_id() {
            if self.shard_id() == parent_shard_id {
                self.children.push(desc.clone());
            } else {
                for lineage in &mut self.children {
                    lineage.set_descendant(desc);
                }
            }
        }
    }

    /// Get records and next shard iterator, then send them. This method ensures that
    /// processing shards in correct order (processing parent shard before children).
    fn get_records<Client>(
        self,
        client: Arc<Client>,
        tx: Sender<(Option<Shard>, Vec<Record>)>,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>>
    where
        Client: DynamodbSDKClient + Send + Sync + 'static,
    {
        Box::pin(async move {
            let Lineage { shard, children } = self;

            let (shard, records) = client.get_records(shard).await.map_or_else(
                |err| {
                    tracing::error!("Unexpected error during getting records: {err}");
                    (None, vec![])
                },
                |output| (output.shard, output.records),
            );

            if let Err(err) = tx.send((shard, records)).await {
                tracing::error!("Unexpected error during sending shard and records: {err}");
            }

            for child in children {
                let tx = tx.clone();
                let client = Arc::clone(&client);

                tokio::spawn(async move {
                    child.get_records(client, tx).await;
                });
            }
        })
    }
}

/// A representation of group of shard lineage(parent and children).
#[derive(Debug, Clone)]
pub struct Lineages(Vec<Lineage>);

impl Lineages {
    /// Create a new lineage group
    fn new() -> Self {
        Self(vec![])
    }

    /// Set shard as a lineage to the lineage group.
    ///
    /// - If there are children to the shard, set children and includes it as a new lineage.
    /// - If the shard is a descendant of any lineage in the group, includes it as a new lineage.
    fn init(lineages: Self, shard: &Shard) -> Self {
        let lineages = lineages.0;
        // Search children of the given shard.
        let (children, mut lineages): (Vec<Lineage>, Vec<Lineage>) = lineages
            .into_iter()
            .partition(|lineage| lineage.is_child(shard.id()));

        // Set children to the new lineage.
        let mut lineage = Lineage::new(shard.clone());
        lineage.set_children(children);

        // If there are an ancestor lineage in the current lineages, set the new lineage
        // as its descendant otherwise push the new lineage as the current lineages.
        if let Some(ancestor) = lineages.iter_mut().find(|l| l.has(shard.parent_shard_id())) {
            ancestor.set_descendant(&lineage);
        } else {
            lineages.push(lineage);
        }

        Self(lineages)
    }

    pub fn get_records<Client>(
        self,
        client: &Arc<Client>,
        tx: &Sender<(Option<Shard>, Vec<Record>)>,
    ) where
        Client: DynamodbSDKClient + 'static,
    {
        for lineage in self.0 {
            let client = Arc::clone(client);
            let tx = tx.clone();

            tokio::spawn(async move {
                lineage.get_records(client, tx).await;
            });
        }
    }
}

impl From<Vec<Shard>> for Lineages {
    fn from(shards: Vec<Shard>) -> Self {
        shards
            .into_iter()
            .fold(Self::new(), |acc, shard| Self::init(acc, &shard))
    }
}

#[cfg(test)]
mod tests {
    use super::super::{super::error::Error, GetRecordsOutput, GetShardsOutput};
    use super::*;
    use async_trait::async_trait;
    use aws_sdk_dynamodbstreams::types::{ShardIteratorType, StreamRecord};
    use itertools::Itertools;
    use std::sync::{Arc, Mutex};

    #[derive(Clone)]
    pub struct TestClient {
        outputs: Arc<Mutex<dyn Iterator<Item = GetRecordsOutput> + Send + Sync>>,
    }

    impl TestClient {
        pub fn new(outputs: Vec<GetRecordsOutput>) -> Self {
            Self {
                outputs: Arc::new(Mutex::new(outputs.into_iter())),
            }
        }
    }

    #[async_trait]
    impl DynamodbSDKClient for TestClient {
        async fn get_stream_arn(&self, _table_name: String) -> Result<String, Error> {
            unimplemented!()
        }

        async fn get_shards(
            &self,
            _stream_arn: &str,
            _exclusive_start_shard_id: Option<String>,
        ) -> Result<GetShardsOutput, Error> {
            unimplemented!()
        }

        async fn get_shard_with_iterator(
            &self,
            _stream_arn: String,
            _shard_id: &str,
            _parent_shard_id: Option<&str>,
            _shard_iterator_type: &ShardIteratorType,
            _sequence_number: Option<String>,
        ) -> Result<Shard, Error> {
            unimplemented!()
        }

        async fn get_records(&self, _shard: Shard) -> Result<GetRecordsOutput, Error> {
            let mut outputs = self.outputs.lock().unwrap();
            Ok(outputs.next().unwrap())
        }
    }

    fn get_child(lineage: &Lineage, shard_id: &str) -> Option<Lineage> {
        lineage
            .children
            .iter()
            .find(|l| l.shard_id() == shard_id)
            .cloned()
    }

    fn create_shard(shard_id: &str, parent_shard_id: Option<&str>) -> Shard {
        Shard::new(
            shard_id.to_string(),
            parent_shard_id.map(std::string::ToString::to_string),
            None,
        )
    }

    fn create_records(seqs: &[&str]) -> Vec<Record> {
        seqs.iter().map(|&seq| create_record(seq)).collect()
    }

    fn create_record(seq: &str) -> Record {
        let dynamodb = StreamRecord::builder().sequence_number(seq).build();
        Record::builder().dynamodb(dynamodb).build()
    }

    fn get_records_output(
        shard_id: &str,
        parent_shard_id: Option<&str>,
        seqs: &[&str],
    ) -> GetRecordsOutput {
        let shard = create_shard(shard_id, parent_shard_id);
        let records = create_records(seqs);
        GetRecordsOutput {
            shard: Some(shard),
            records,
        }
    }

    fn assert_include(shards: &[Shard], shard_id: &str) {
        assert!(shards.iter().any(|s| s.id() == shard_id));
    }

    //     0
    //  / \  \
    //  1  2  3
    // / \  \
    // 4  5  6
    //     /  \
    //    7    8
    #[test]
    fn a_lineage_has_tree_structure() {
        let s0 = create_shard("0", None);
        let s1 = create_shard("1", Some("0"));
        let s2 = create_shard("2", Some("0"));
        let s3 = create_shard("3", Some("0"));
        let s4 = create_shard("4", Some("1"));
        let s5 = create_shard("5", Some("1"));
        let s6 = create_shard("6", Some("2"));
        let s7 = create_shard("7", Some("6"));
        let s8 = create_shard("8", Some("6"));

        for shards in [s0, s1, s2, s3, s4, s5, s6, s7, s8]
            .into_iter()
            .permutations(9)
        {
            assert_eq!(shards.len(), 9);

            let mut lineages = Lineages::from(shards).0;
            assert_eq!(lineages.len(), 1);

            let l0 = lineages.pop().unwrap();
            assert_eq!(l0.shard_id(), "0");
            assert_eq!(l0.children.len(), 3);

            let l1 = get_child(&l0, "1");
            let l2 = get_child(&l0, "2");
            let l3 = get_child(&l0, "3");
            assert!(l1.is_some());
            assert!(l2.is_some());
            assert!(l3.is_some());

            let l1 = l1.unwrap();
            let l2 = l2.unwrap();
            let l3 = l3.unwrap();
            assert_eq!(l1.children.len(), 2);
            assert_eq!(l2.children.len(), 1);
            assert_eq!(l3.children.len(), 0);

            let l4 = get_child(&l1, "4");
            let l5 = get_child(&l1, "5");
            let l6 = get_child(&l2, "6");
            assert!(l4.is_some());
            assert!(l5.is_some());
            assert!(l6.is_some());

            let l4 = l4.unwrap();
            let l5 = l5.unwrap();
            let l6 = l6.unwrap();
            assert_eq!(l4.children.len(), 0);
            assert_eq!(l5.children.len(), 0);
            assert_eq!(l6.children.len(), 2);

            let l7 = get_child(&l6, "7");
            let l8 = get_child(&l6, "8");
            assert!(l7.is_some());
            assert!(l8.is_some());

            let l7 = l7.unwrap();
            let l8 = l8.unwrap();
            assert_eq!(l7.children.len(), 0);
            assert_eq!(l8.children.len(), 0);
        }
    }

    //     0       3
    //   /  \    /  \
    //  1   2   4    5
    #[test]
    fn shards_are_transformed_into_multiple_lineages() {
        let s0 = create_shard("0", None);
        let s1 = create_shard("1", Some("0"));
        let s2 = create_shard("2", Some("0"));
        let s3 = create_shard("3", None);
        let s4 = create_shard("4", Some("3"));
        let s5 = create_shard("5", Some("3"));

        for shards in [s0, s1, s2, s3, s4, s5].into_iter().permutations(6) {
            assert_eq!(shards.len(), 6);

            let lineages = Lineages::from(shards).0;
            assert_eq!(lineages.len(), 2);

            let l0 = lineages.iter().find(|l| l.shard_id() == "0").unwrap();
            let l3 = lineages.iter().find(|l| l.shard_id() == "3").unwrap();

            let l1 = get_child(l0, "1");
            let l2 = get_child(l0, "2");
            let l4 = get_child(l3, "4");
            let l5 = get_child(l3, "5");
            assert!(l1.is_some());
            assert!(l2.is_some());
            assert!(l4.is_some());
            assert!(l5.is_some());
        }
    }

    #[tokio::test]
    async fn lineages_get_records_returns_shards_and_records() {
        let s0 = create_shard("0", None);
        let s1 = create_shard("1", Some("0"));
        let s2 = create_shard("2", Some("0"));

        let shards = vec![s0, s1, s2];
        let lineages = Lineages::from(shards);

        let out3 = get_records_output("3", None, &["0012", "0004", "0008", "0001"]);
        let out4 = get_records_output("4", Some("3"), &["0003", "0010", "0011", "0009", "0006"]);
        let out5 = get_records_output("5", Some("3"), &["0002", "0005", "0013", "0007"]);

        let client = Arc::new(TestClient::new(vec![out3, out4, out5]));

        let (tx, mut rx) = tokio::sync::mpsc::channel(100);

        lineages.get_records(&client, &tx);

        drop(tx);

        // Collect results from the receiver
        let mut shards = Vec::new();
        let mut records = Vec::new();

        while let Some((shard_opt, recs)) = rx.recv().await {
            if let Some(shard) = shard_opt {
                shards.push(shard);
            }
            records.extend(recs);
        }

        assert_include(&shards, "3");
        assert_include(&shards, "4");
        assert_include(&shards, "5");

        assert_eq!(
            records
                .iter()
                .map(|r| r
                    .dynamodb
                    .as_ref()
                    .expect("dynamodb")
                    .sequence_number
                    .as_ref()
                    .expect("sequence_number")
                    .to_string())
                .collect::<Vec<String>>(),
            [
                "0012", "0004", "0008", "0001", "0003", "0010", "0011", "0009", "0006", "0002",
                "0005", "0013", "0007"
            ]
        );
    }
}
