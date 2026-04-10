/*
Copyright 2026 The Spice.ai OSS Authors

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

//! Dataset readiness evaluation based on partition assignments and executor-reported statuses.

use std::collections::HashMap;

use datafusion::sql::TableReference;

use crate::cluster::partition::PartitionManager;
use crate::status::{ComponentStatus, RuntimeStatus};

/// Evaluate readiness for a single dataset and promote it to [`ComponentStatus::Ready`]
/// if all partitions are assigned and all assigned executors report Ready.
///
/// `executor_statuses` is the pre-fetched snapshot from [`ExecutorRegistry::get_executor_dataset_statuses`].
pub(crate) fn evaluate_dataset_readiness(
    table_name: &str,
    partition_manager: &PartitionManager,
    status: &RuntimeStatus,
    executor_statuses: &HashMap<String, HashMap<String, ComponentStatus>>,
) {
    let table_ref = TableReference::parse_str(table_name);
    let Some(metadata) = partition_manager.get_cached_table_metadata(&table_ref) else {
        return;
    };

    // A table with zero partitions has nothing to assign, so it's ready.
    // Otherwise, every partition must be assigned to at least one executor.
    let all_assigned = metadata
        .partitions
        .iter()
        .all(super::metadata::PartitionMetadata::is_assigned);
    if !all_assigned {
        return;
    }

    // Each partition must have at least one assigned executor that reports
    // this dataset as Ready — i.e. the partition is queryable somewhere.
    let all_partitions_queryable = metadata.partitions.iter().all(|p| {
        p.assigned_executors.iter().any(|exec_id| {
            executor_statuses
                .get(exec_id)
                .and_then(|ds_map| ds_map.get(table_name))
                .is_some_and(|s| *s == ComponentStatus::Ready)
        })
    });

    if all_partitions_queryable {
        status.update_dataset(&table_ref, ComponentStatus::Ready);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use object_store::memory::InMemory;

    use super::*;
    use crate::cluster::partition::{PartitionMetadata, PartitionValue, TablePartitionMetadata};

    fn make_partition(key: &str, value: &str) -> PartitionValue {
        let mut p = HashMap::new();
        p.insert(key.to_string(), value.to_string());
        p
    }

    fn make_assigned_partition(key: &str, value: &str, executor: &str) -> PartitionMetadata {
        let mut pm = PartitionMetadata::new(make_partition(key, value));
        pm.assign_to(executor.to_string(), 1000);
        pm
    }

    fn make_unassigned_partition(key: &str, value: &str) -> PartitionMetadata {
        PartitionMetadata::new(make_partition(key, value))
    }

    /// Helper to set up a partition manager with pre-populated metadata.
    async fn setup_partition_manager(
        tables: Vec<(&str, Vec<PartitionMetadata>)>,
    ) -> Arc<PartitionManager> {
        let store = Arc::new(InMemory::new());
        let manager = Arc::new(PartitionManager::new(store));

        for (table_name, partitions) in tables {
            let table_ref = TableReference::parse_str(table_name);
            manager
                .initialize_metadata(&table_ref, vec!["date".to_string()])
                .await
                .expect("should initialize metadata");

            let metadata = TablePartitionMetadata {
                table_name: table_name.to_string(),
                partitions,
                schema_version: 1,
                updated_at: 1000,
                partition_expressions: vec!["date".to_string()],
            };
            manager
                .write_metadata(table_name, metadata)
                .await
                .expect("should write metadata");
        }

        manager.refresh().await.expect("should refresh");
        manager
    }

    /// Helper: build executor statuses map from a list of (`executor_id`, [(dataset, status)]).
    fn executor_statuses(
        entries: Vec<(&str, Vec<(&str, ComponentStatus)>)>,
    ) -> HashMap<String, HashMap<String, ComponentStatus>> {
        entries
            .into_iter()
            .map(|(exec_id, ds_statuses)| {
                let ds_map: HashMap<String, ComponentStatus> = ds_statuses
                    .into_iter()
                    .map(|(ds, s)| (ds.to_string(), s))
                    .collect();
                (exec_id.to_string(), ds_map)
            })
            .collect()
    }

    #[tokio::test]
    async fn test_assigned_and_executors_ready_marks_dataset_ready() {
        let pm = setup_partition_manager(vec![(
            "test_table",
            vec![
                make_assigned_partition("date", "2024-01-01", "executor1"),
                make_assigned_partition("date", "2024-01-02", "executor2"),
            ],
        )])
        .await;

        let es = executor_statuses(vec![
            ("executor1", vec![("test_table", ComponentStatus::Ready)]),
            ("executor2", vec![("test_table", ComponentStatus::Ready)]),
        ]);

        let status = RuntimeStatus::new();
        let table_ref = TableReference::parse_str("test_table");
        status.update_dataset(&table_ref, ComponentStatus::Initializing);

        evaluate_dataset_readiness("test_table", &pm, &status, &es);

        assert_eq!(
            status.get_component_status("dataset:test_table"),
            Some(ComponentStatus::Ready),
            "Dataset should be Ready when all partitions assigned and all executors report Ready"
        );
    }

    #[tokio::test]
    async fn test_unassigned_partitions_keeps_initializing() {
        let pm = setup_partition_manager(vec![(
            "test_table",
            vec![
                make_assigned_partition("date", "2024-01-01", "executor1"),
                make_unassigned_partition("date", "2024-01-02"),
            ],
        )])
        .await;

        let es = executor_statuses(vec![(
            "executor1",
            vec![("test_table", ComponentStatus::Ready)],
        )]);

        let status = RuntimeStatus::new();
        status.update_dataset(
            &TableReference::parse_str("test_table"),
            ComponentStatus::Initializing,
        );

        evaluate_dataset_readiness("test_table", &pm, &status, &es);

        assert_eq!(
            status.get_component_status("dataset:test_table"),
            Some(ComponentStatus::Initializing),
            "Dataset should remain Initializing when some partitions are unassigned"
        );
    }

    #[tokio::test]
    async fn test_empty_partitions_marks_dataset_ready() {
        let pm = setup_partition_manager(vec![("test_table", vec![])]).await;
        let es = HashMap::new();

        let status = RuntimeStatus::new();
        status.update_dataset(
            &TableReference::parse_str("test_table"),
            ComponentStatus::Initializing,
        );

        evaluate_dataset_readiness("test_table", &pm, &status, &es);

        assert_eq!(
            status.get_component_status("dataset:test_table"),
            Some(ComponentStatus::Ready),
            "Dataset with no partitions should be marked Ready"
        );
    }

    #[tokio::test]
    async fn test_all_unassigned_partitions_keeps_initializing() {
        let pm = setup_partition_manager(vec![(
            "test_table",
            vec![
                make_unassigned_partition("date", "2024-01-01"),
                make_unassigned_partition("date", "2024-01-02"),
            ],
        )])
        .await;

        let es = HashMap::new();
        let status = RuntimeStatus::new();
        status.update_dataset(
            &TableReference::parse_str("test_table"),
            ComponentStatus::Initializing,
        );

        evaluate_dataset_readiness("test_table", &pm, &status, &es);

        assert_eq!(
            status.get_component_status("dataset:test_table"),
            Some(ComponentStatus::Initializing),
            "Dataset should remain Initializing when all partitions are unassigned"
        );
    }

    #[tokio::test]
    async fn test_table_not_in_partition_manager() {
        let store = Arc::new(InMemory::new());
        let pm = PartitionManager::new(store);
        let es = HashMap::new();

        let status = RuntimeStatus::new();
        status.update_dataset(
            &TableReference::parse_str("unknown_table"),
            ComponentStatus::Initializing,
        );

        evaluate_dataset_readiness("unknown_table", &pm, &status, &es);

        assert_eq!(
            status.get_component_status("dataset:unknown_table"),
            Some(ComponentStatus::Initializing),
            "Dataset should be unchanged when table has no partition metadata"
        );
    }

    #[tokio::test]
    async fn test_executor_not_ready_stays_initializing() {
        let pm = setup_partition_manager(vec![(
            "test_table",
            vec![
                make_assigned_partition("date", "2024-01-01", "executor1"),
                make_assigned_partition("date", "2024-01-02", "executor2"),
            ],
        )])
        .await;

        // executor1 ready, executor2 still refreshing
        let es = executor_statuses(vec![
            ("executor1", vec![("test_table", ComponentStatus::Ready)]),
            (
                "executor2",
                vec![("test_table", ComponentStatus::Refreshing)],
            ),
        ]);

        let status = RuntimeStatus::new();
        status.update_dataset(
            &TableReference::parse_str("test_table"),
            ComponentStatus::Initializing,
        );

        evaluate_dataset_readiness("test_table", &pm, &status, &es);

        assert_eq!(
            status.get_component_status("dataset:test_table"),
            Some(ComponentStatus::Initializing),
            "Dataset should stay Initializing when one executor has not finished accelerating"
        );
    }

    #[tokio::test]
    async fn test_executor_reports_error_stays_initializing() {
        let pm = setup_partition_manager(vec![(
            "test_table",
            vec![make_assigned_partition("date", "2024-01-01", "executor1")],
        )])
        .await;

        let es = executor_statuses(vec![(
            "executor1",
            vec![("test_table", ComponentStatus::error())],
        )]);

        let status = RuntimeStatus::new();
        status.update_dataset(
            &TableReference::parse_str("test_table"),
            ComponentStatus::Initializing,
        );

        evaluate_dataset_readiness("test_table", &pm, &status, &es);

        assert_eq!(
            status.get_component_status("dataset:test_table"),
            Some(ComponentStatus::Initializing),
            "Dataset should stay Initializing when executor reports Error"
        );
    }

    #[tokio::test]
    async fn test_no_executor_status_reported_stays_initializing() {
        let pm = setup_partition_manager(vec![(
            "test_table",
            vec![make_assigned_partition("date", "2024-01-01", "executor1")],
        )])
        .await;

        // Empty — no executor has reported any status
        let es = HashMap::new();

        let status = RuntimeStatus::new();
        status.update_dataset(
            &TableReference::parse_str("test_table"),
            ComponentStatus::Initializing,
        );

        evaluate_dataset_readiness("test_table", &pm, &status, &es);

        assert_eq!(
            status.get_component_status("dataset:test_table"),
            Some(ComponentStatus::Initializing),
            "Dataset should stay Initializing when executor has not reported any status"
        );
    }

    #[tokio::test]
    async fn test_partition_with_multiple_executors_one_ready_is_queryable() {
        // Partition assigned to two executors, only one reports Ready
        let pm = setup_partition_manager(vec![(
            "test_table",
            vec![{
                let mut p = PartitionMetadata::new(make_partition("date", "2024-01-01"));
                p.assign_to("executor1".to_string(), 1000);
                p.assign_to("executor2".to_string(), 1000);
                p
            }],
        )])
        .await;

        let es = executor_statuses(vec![
            ("executor1", vec![("test_table", ComponentStatus::Ready)]),
            (
                "executor2",
                vec![("test_table", ComponentStatus::Refreshing)],
            ),
        ]);

        let status = RuntimeStatus::new();
        status.update_dataset(
            &TableReference::parse_str("test_table"),
            ComponentStatus::Initializing,
        );

        evaluate_dataset_readiness("test_table", &pm, &status, &es);

        assert_eq!(
            status.get_component_status("dataset:test_table"),
            Some(ComponentStatus::Ready),
            "Partition is queryable if at least one assigned executor reports Ready"
        );
    }

    #[tokio::test]
    async fn test_end_to_end_runtime_readiness() {
        // End-to-end: dataset Initializing → partitions assigned → executors report Ready → runtime ready
        let pm = setup_partition_manager(vec![(
            "test_table",
            vec![
                make_unassigned_partition("date", "2024-01-01"),
                make_unassigned_partition("date", "2024-01-02"),
            ],
        )])
        .await;

        let status = RuntimeStatus::new();
        let table_ref = TableReference::parse_str("test_table");
        status.update_dataset(&table_ref, ComponentStatus::Initializing);
        status.update_component_status("partition_metadata", ComponentStatus::Initializing);
        assert!(!status.is_ready());

        // Simulate: partitions get assigned
        let metadata = TablePartitionMetadata {
            table_name: "test_table".to_string(),
            partitions: vec![
                make_assigned_partition("date", "2024-01-01", "executor1"),
                make_assigned_partition("date", "2024-01-02", "executor2"),
            ],
            schema_version: 1,
            updated_at: 2000,
            partition_expressions: vec!["date".to_string()],
        };
        pm.write_metadata("test_table", metadata)
            .await
            .expect("should write");
        pm.refresh().await.expect("should refresh");
        status.update_component_status("partition_metadata", ComponentStatus::Ready);

        // No executor statuses yet → still not ready
        let empty_es = HashMap::new();
        evaluate_dataset_readiness("test_table", &pm, &status, &empty_es);
        assert_eq!(
            status.get_component_status("dataset:test_table"),
            Some(ComponentStatus::Initializing),
        );
        assert!(!status.is_ready());

        // Executors report Ready → dataset ready → runtime ready
        let es = executor_statuses(vec![
            ("executor1", vec![("test_table", ComponentStatus::Ready)]),
            ("executor2", vec![("test_table", ComponentStatus::Ready)]),
        ]);
        evaluate_dataset_readiness("test_table", &pm, &status, &es);

        assert_eq!(
            status.get_component_status("dataset:test_table"),
            Some(ComponentStatus::Ready),
        );
        assert!(
            status.is_ready(),
            "Runtime should be ready when all datasets are queryable"
        );
    }
}
