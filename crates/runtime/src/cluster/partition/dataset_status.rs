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

use crate::cluster::executor_registry::ExecutorRegistry;
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

/// Check **all** tables in the partition manager and update each dataset's
/// [`ComponentStatus`] on the given runtime status tracker.
///
/// This is the bulk entry point used after heartbeat reconciliation.
/// For a targeted single-dataset check (e.g. after receiving a
/// `DatasetStatusChange`), call [`evaluate_dataset_readiness`] directly.
pub(crate) async fn update_dataset_statuses_from_partitions(
    partition_manager: &PartitionManager,
    status: &RuntimeStatus,
    executor_registry: &ExecutorRegistry,
) {
    let tables = match partition_manager.list_tables().await {
        Ok(t) => t,
        Err(e) => {
            tracing::warn!(error = %e, "Failed to list tables for dataset status update");
            return;
        }
    };

    let executor_statuses = executor_registry.get_executor_dataset_statuses().await;

    for table_name in &tables {
        evaluate_dataset_readiness(table_name, partition_manager, status, &executor_statuses);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use object_store::memory::InMemory;

    use super::*;
    use crate::cluster::partition::{PartitionMetadata, PartitionValue, TablePartitionMetadata};

    /// Creates an empty `ExecutorRegistry` with no dataset statuses.
    fn empty_executor_registry() -> Arc<ExecutorRegistry> {
        let store = Arc::new(InMemory::new());
        let pm = Arc::new(PartitionManager::new(store.clone()));
        let pm2 = Arc::new(PartitionManager::new(store));
        Arc::new(ExecutorRegistry::new(pm, pm2))
    }

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

    /// Helper to create an `ExecutorRegistry` with pre-populated dataset statuses.
    async fn make_executor_registry_with_statuses(
        statuses: Vec<(&str, Vec<(&str, ComponentStatus)>)>,
    ) -> Arc<ExecutorRegistry> {
        let store = Arc::new(InMemory::new());
        let pm = Arc::new(PartitionManager::new(store.clone()));
        let pm2 = Arc::new(PartitionManager::new(store));
        let registry = Arc::new(ExecutorRegistry::new(pm, pm2));

        for (executor_id, ds_statuses) in statuses {
            let map: HashMap<String, ComponentStatus> = ds_statuses
                .into_iter()
                .map(|(ds, s)| (ds.to_string(), s))
                .collect();
            registry
                .replace_executor_dataset_statuses(executor_id, map)
                .await;
        }

        registry
    }

    #[tokio::test]
    async fn test_all_partitions_assigned_and_ready_marks_dataset_ready() {
        let partition_manager = setup_partition_manager(vec![(
            "test_table",
            vec![
                make_assigned_partition("date", "2024-01-01", "executor1"),
                make_assigned_partition("date", "2024-01-02", "executor2"),
            ],
        )])
        .await;

        let registry = make_executor_registry_with_statuses(vec![
            ("executor1", vec![("test_table", ComponentStatus::Ready)]),
            ("executor2", vec![("test_table", ComponentStatus::Ready)]),
        ])
        .await;

        let status = RuntimeStatus::new();
        let table_ref = TableReference::parse_str("test_table");
        status.update_dataset(&table_ref, ComponentStatus::Initializing);
        assert_eq!(
            status.get_component_status("dataset:test_table"),
            Some(ComponentStatus::Initializing)
        );

        update_dataset_statuses_from_partitions(&partition_manager, &status, &registry).await;

        assert_eq!(
            status.get_component_status("dataset:test_table"),
            Some(ComponentStatus::Ready),
            "Dataset should be Ready when all partitions are assigned and executors report Ready"
        );
    }

    #[tokio::test]
    async fn test_unassigned_partitions_keeps_dataset_initializing() {
        let partition_manager = setup_partition_manager(vec![(
            "test_table",
            vec![
                make_assigned_partition("date", "2024-01-01", "executor1"),
                make_unassigned_partition("date", "2024-01-02"),
            ],
        )])
        .await;

        let status = RuntimeStatus::new();
        let table_ref = TableReference::parse_str("test_table");
        status.update_dataset(&table_ref, ComponentStatus::Initializing);

        let empty_reg = empty_executor_registry();
        update_dataset_statuses_from_partitions(&partition_manager, &status, &empty_reg).await;

        assert_eq!(
            status.get_component_status("dataset:test_table"),
            Some(ComponentStatus::Initializing),
            "Dataset should remain Initializing when some partitions are unassigned"
        );
    }

    #[tokio::test]
    async fn test_empty_partitions_marks_dataset_ready() {
        let partition_manager = setup_partition_manager(vec![("test_table", vec![])]).await;

        let status = RuntimeStatus::new();
        let table_ref = TableReference::parse_str("test_table");
        status.update_dataset(&table_ref, ComponentStatus::Initializing);

        let empty_reg = empty_executor_registry();
        update_dataset_statuses_from_partitions(&partition_manager, &status, &empty_reg).await;

        assert_eq!(
            status.get_component_status("dataset:test_table"),
            Some(ComponentStatus::Ready),
            "Dataset with no partitions should be marked Ready"
        );
    }

    #[tokio::test]
    async fn test_multiple_tables_independent_status() {
        let partition_manager = setup_partition_manager(vec![
            (
                "table_a",
                vec![
                    make_assigned_partition("date", "2024-01-01", "executor1"),
                    make_assigned_partition("date", "2024-01-02", "executor1"),
                ],
            ),
            (
                "table_b",
                vec![
                    make_assigned_partition("date", "2024-01-01", "executor1"),
                    make_unassigned_partition("date", "2024-01-02"),
                ],
            ),
        ])
        .await;

        // executor1 reports Ready for both tables
        let registry = make_executor_registry_with_statuses(vec![(
            "executor1",
            vec![
                ("table_a", ComponentStatus::Ready),
                ("table_b", ComponentStatus::Ready),
            ],
        )])
        .await;

        let status = RuntimeStatus::new();
        let table_a = TableReference::parse_str("table_a");
        let table_b = TableReference::parse_str("table_b");
        status.update_dataset(&table_a, ComponentStatus::Initializing);
        status.update_dataset(&table_b, ComponentStatus::Initializing);

        update_dataset_statuses_from_partitions(&partition_manager, &status, &registry).await;

        assert_eq!(
            status.get_component_status("dataset:table_a"),
            Some(ComponentStatus::Ready),
            "table_a should be Ready (all partitions assigned and executor reports Ready)"
        );
        assert_eq!(
            status.get_component_status("dataset:table_b"),
            Some(ComponentStatus::Initializing),
            "table_b should remain Initializing (has unassigned partition)"
        );
    }

    #[tokio::test]
    async fn test_no_tables_in_partition_manager() {
        let store = Arc::new(InMemory::new());
        let partition_manager = PartitionManager::new(store);

        let status = RuntimeStatus::new();
        let table_ref = TableReference::parse_str("some_table");
        status.update_dataset(&table_ref, ComponentStatus::Initializing);

        let empty_reg = empty_executor_registry();
        update_dataset_statuses_from_partitions(&partition_manager, &status, &empty_reg).await;

        assert_eq!(
            status.get_component_status("dataset:some_table"),
            Some(ComponentStatus::Initializing),
            "Dataset status should be unchanged when no tables exist in partition manager"
        );
    }

    #[tokio::test]
    async fn test_all_unassigned_partitions_keeps_initializing() {
        let partition_manager = setup_partition_manager(vec![(
            "test_table",
            vec![
                make_unassigned_partition("date", "2024-01-01"),
                make_unassigned_partition("date", "2024-01-02"),
                make_unassigned_partition("date", "2024-01-03"),
            ],
        )])
        .await;

        let status = RuntimeStatus::new();
        let table_ref = TableReference::parse_str("test_table");
        status.update_dataset(&table_ref, ComponentStatus::Initializing);

        let empty_reg = empty_executor_registry();
        update_dataset_statuses_from_partitions(&partition_manager, &status, &empty_reg).await;

        assert_eq!(
            status.get_component_status("dataset:test_table"),
            Some(ComponentStatus::Initializing),
            "Dataset should remain Initializing when all partitions are unassigned"
        );
    }

    #[tokio::test]
    async fn test_runtime_not_ready_until_all_datasets_queryable() {
        let partition_manager = setup_partition_manager(vec![(
            "test_table",
            vec![
                make_unassigned_partition("date", "2024-01-01"),
                make_unassigned_partition("date", "2024-01-02"),
            ],
        )])
        .await;

        let status = RuntimeStatus::new();
        let table_ref = TableReference::parse_str("test_table");

        // Step 1: Dataset registered as Initializing (simulates accelerated_table scheduler path)
        status.update_dataset(&table_ref, ComponentStatus::Initializing);
        status.update_component_status("partition_metadata", ComponentStatus::Initializing);

        assert!(
            !status.is_ready(),
            "Runtime should not be ready with Initializing components"
        );

        // Simulate: management cycle assigns partitions to executors
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
        partition_manager
            .write_metadata("test_table", metadata)
            .await
            .expect("should write");
        partition_manager.refresh().await.expect("should refresh");

        status.update_component_status("partition_metadata", ComponentStatus::Ready);

        // Partitions assigned but executors haven't reported Ready yet → still not ready
        let empty_reg = empty_executor_registry();
        update_dataset_statuses_from_partitions(&partition_manager, &status, &empty_reg).await;

        assert_eq!(
            status.get_component_status("dataset:test_table"),
            Some(ComponentStatus::Initializing),
            "Dataset should stay Initializing until executors report Ready"
        );
        assert!(!status.is_ready());

        // Executors report Ready → dataset becomes Ready → runtime is ready
        let registry = make_executor_registry_with_statuses(vec![
            ("executor1", vec![("test_table", ComponentStatus::Ready)]),
            ("executor2", vec![("test_table", ComponentStatus::Ready)]),
        ])
        .await;

        update_dataset_statuses_from_partitions(&partition_manager, &status, &registry).await;

        assert_eq!(
            status.get_component_status("dataset:test_table"),
            Some(ComponentStatus::Ready)
        );
        assert!(
            status.is_ready(),
            "Runtime should be ready when all datasets are queryable"
        );
    }

    #[tokio::test]
    async fn test_assigned_and_executors_ready_marks_dataset_ready() {
        let partition_manager = setup_partition_manager(vec![(
            "test_table",
            vec![
                make_assigned_partition("date", "2024-01-01", "executor1"),
                make_assigned_partition("date", "2024-01-02", "executor2"),
            ],
        )])
        .await;

        let registry = make_executor_registry_with_statuses(vec![
            ("executor1", vec![("test_table", ComponentStatus::Ready)]),
            ("executor2", vec![("test_table", ComponentStatus::Ready)]),
        ])
        .await;

        let status = RuntimeStatus::new();
        let table_ref = TableReference::parse_str("test_table");
        status.update_dataset(&table_ref, ComponentStatus::Initializing);

        update_dataset_statuses_from_partitions(&partition_manager, &status, &registry).await;

        assert_eq!(
            status.get_component_status("dataset:test_table"),
            Some(ComponentStatus::Ready),
            "Dataset should be Ready when all partitions assigned AND all executors report Ready"
        );
    }

    #[tokio::test]
    async fn test_assigned_but_executor_not_ready_stays_initializing() {
        let partition_manager = setup_partition_manager(vec![(
            "test_table",
            vec![
                make_assigned_partition("date", "2024-01-01", "executor1"),
                make_assigned_partition("date", "2024-01-02", "executor2"),
            ],
        )])
        .await;

        let registry = make_executor_registry_with_statuses(vec![
            ("executor1", vec![("test_table", ComponentStatus::Ready)]),
            (
                "executor2",
                vec![("test_table", ComponentStatus::Refreshing)],
            ),
        ])
        .await;

        let status = RuntimeStatus::new();
        let table_ref = TableReference::parse_str("test_table");
        status.update_dataset(&table_ref, ComponentStatus::Initializing);

        update_dataset_statuses_from_partitions(&partition_manager, &status, &registry).await;

        assert_eq!(
            status.get_component_status("dataset:test_table"),
            Some(ComponentStatus::Initializing),
            "Dataset should stay Initializing when one executor has not finished accelerating"
        );
    }

    #[tokio::test]
    async fn test_assigned_but_executor_reports_error_stays_initializing() {
        let partition_manager = setup_partition_manager(vec![(
            "test_table",
            vec![make_assigned_partition("date", "2024-01-01", "executor1")],
        )])
        .await;

        let registry = make_executor_registry_with_statuses(vec![(
            "executor1",
            vec![("test_table", ComponentStatus::error())],
        )])
        .await;

        let status = RuntimeStatus::new();
        let table_ref = TableReference::parse_str("test_table");
        status.update_dataset(&table_ref, ComponentStatus::Initializing);

        update_dataset_statuses_from_partitions(&partition_manager, &status, &registry).await;

        assert_eq!(
            status.get_component_status("dataset:test_table"),
            Some(ComponentStatus::Initializing),
            "Dataset should stay Initializing when executor reports Error"
        );
    }

    #[tokio::test]
    async fn test_assigned_but_no_executor_status_reported_stays_initializing() {
        let partition_manager = setup_partition_manager(vec![(
            "test_table",
            vec![make_assigned_partition("date", "2024-01-01", "executor1")],
        )])
        .await;

        let registry = make_executor_registry_with_statuses(vec![]).await;

        let status = RuntimeStatus::new();
        let table_ref = TableReference::parse_str("test_table");
        status.update_dataset(&table_ref, ComponentStatus::Initializing);

        update_dataset_statuses_from_partitions(&partition_manager, &status, &registry).await;

        assert_eq!(
            status.get_component_status("dataset:test_table"),
            Some(ComponentStatus::Initializing),
            "Dataset should stay Initializing when executor has not reported any status"
        );
    }

    #[tokio::test]
    async fn test_executor_disconnect_removes_statuses() {
        let store = Arc::new(InMemory::new());
        let pm = Arc::new(PartitionManager::new(store.clone()));
        let pm2 = Arc::new(PartitionManager::new(store));
        let registry = ExecutorRegistry::new(pm, pm2);

        registry
            .update_executor_dataset_status("executor1", "test_table", ComponentStatus::Ready)
            .await;

        let statuses = registry.get_executor_dataset_statuses().await;
        assert!(statuses.contains_key("executor1"));

        registry.unregister("executor1").await;

        let statuses = registry.get_executor_dataset_statuses().await;
        assert!(
            !statuses.contains_key("executor1"),
            "Executor statuses should be cleaned up on disconnect"
        );
    }
}
