use super::*;
use crate::metadata::DeletionType;
use std::sync::Arc;

#[tokio::test]
async fn test_catalog_creation() {
    let _catalog = CayenneCatalog::new("sqlite://./test.db").expect("Failed to create catalog");
    // Tests will be added once implementation is complete
}

#[tokio::test]
async fn test_concurrent_table_creation() {
    // Create a unique test database to avoid conflicts with other tests
    let test_db = format!("sqlite://./.test_concurrent_{}.db", uuid::Uuid::now_v7());
    let catalog = Arc::new(CayenneCatalog::new(&test_db).expect("Failed to create catalog"));

    // Initialize the catalog
    catalog.init().await.expect("Failed to initialize catalog");

    // Create test schema
    let schema = Arc::new(arrow_schema::Schema::new(vec![
        arrow_schema::Field::new("id", arrow_schema::DataType::Int64, false),
        arrow_schema::Field::new("name", arrow_schema::DataType::Utf8, true),
    ]));

    let table_name = "test_concurrent_table";
    let base_path = "/tmp/cayenne_test";

    // Spawn multiple tasks that all try to create the same table concurrently
    let mut handles = vec![];
    for _ in 0..10 {
        let catalog_clone = Arc::clone(&catalog);
        let schema_clone = Arc::clone(&schema);
        let table_name = table_name.to_string();
        let base_path = base_path.to_string();

        let handle = tokio::spawn(async move {
            let options = CreateTableOptions {
                table_name: table_name.clone(),
                schema: schema_clone,
                primary_key: vec![],
                on_conflict: None,
                base_path,
                partition_column: None,
                vortex_config: crate::metadata::VortexConfig::default(),
            };

            catalog_clone.create_table(options).await
        });

        handles.push(handle);
    }

    // Wait for all tasks to complete
    let results: Vec<_> = futures::future::join_all(handles).await;

    // All tasks should succeed (either creating or finding the table)
    let mut table_ids = vec![];
    for result in results {
        let table_id = result.expect("Task panicked").expect("create_table failed");
        table_ids.push(table_id);
    }

    // All tasks should have gotten the same table_id
    assert!(
        table_ids.windows(2).all(|w| w[0] == w[1]),
        "All concurrent create_table calls should return the same table_id"
    );

    // Verify the table exists and can be queried
    let table_metadata = catalog
        .get_table(table_name)
        .await
        .expect("Failed to get table metadata");

    assert_eq!(table_metadata.table_name, table_name);
    assert_eq!(table_metadata.table_id, table_ids[0]);

    // Cleanup test database
    let db_path = test_db.strip_prefix("sqlite://").unwrap_or(&test_db);
    let _ = std::fs::remove_file(db_path);
    let _ = std::fs::remove_file(format!("{db_path}-shm"));
    let _ = std::fs::remove_file(format!("{db_path}-wal"));
}

#[tokio::test]
async fn test_concurrent_partition_creation() {
    // Create a unique test database to avoid conflicts with other tests
    let test_db = format!(
        "sqlite://./.test_concurrent_partition_{}.db",
        uuid::Uuid::now_v7()
    );
    let catalog = Arc::new(CayenneCatalog::new(&test_db).expect("Failed to create catalog"));

    // Initialize the catalog
    catalog.init().await.expect("Failed to initialize catalog");

    // Create a test table first
    let schema = Arc::new(arrow_schema::Schema::new(vec![
        arrow_schema::Field::new("id", arrow_schema::DataType::Int64, false),
        arrow_schema::Field::new("date", arrow_schema::DataType::Utf8, true),
    ]));

    let table_options = CreateTableOptions {
        table_name: "test_table".to_string(),
        schema,
        primary_key: vec![],
        on_conflict: None,
        base_path: "/tmp/cayenne_test_partition".to_string(),
        partition_column: Some("date".to_string()),
        vortex_config: crate::metadata::VortexConfig::default(),
    };

    let table_id = catalog
        .create_table(table_options)
        .await
        .expect("Failed to create table");

    // Spawn multiple tasks that all try to create the same partition concurrently
    let mut handles = vec![];
    for _ in 0..10 {
        let catalog_clone = Arc::clone(&catalog);
        let table_id = table_id.clone();

        let handle = tokio::spawn(async move {
            let partition = PartitionMetadata {
                partition_id: String::new(), // Will be assigned by catalog
                table_id,
                partition_columns: vec!["date".to_string()],
                partition_values: vec!["2024-01-01".to_string()],
                path: "/tmp/cayenne_test_partition/partition_20240101".to_string(),
                path_is_relative: false,
                record_count: 100,
                file_size_bytes: 1024,
            };

            catalog_clone.add_partition(partition).await
        });

        handles.push(handle);
    }

    // Wait for all tasks to complete
    let results: Vec<_> = futures::future::join_all(handles).await;

    // All tasks should succeed (either creating or finding the partition)
    let mut partition_ids = vec![];
    for result in results {
        let partition_id = result
            .expect("Task panicked")
            .expect("add_partition failed");
        partition_ids.push(partition_id);
    }

    // All tasks should have gotten the same partition_id
    assert!(
        partition_ids.windows(2).all(|w| w[0] == w[1]),
        "All concurrent add_partition calls should return the same partition_id"
    );

    // Verify the partition exists and can be queried
    let partitions = catalog
        .get_partitions(&table_id)
        .await
        .expect("Failed to get partitions");

    assert_eq!(partitions.len(), 1);
    assert_eq!(partitions[0].partition_id, partition_ids[0]);
    assert_eq!(partitions[0].partition_values, vec!["2024-01-01"]);

    // Cleanup test database
    let db_path = test_db.strip_prefix("sqlite://").unwrap_or(&test_db);
    let _ = std::fs::remove_file(db_path);
    let _ = std::fs::remove_file(format!("{db_path}-shm"));
    let _ = std::fs::remove_file(format!("{db_path}-wal"));
}

#[tokio::test]
async fn test_concurrent_delete_file_creation() {
    // Create a unique test database to avoid conflicts with other tests
    let test_db = format!(
        "sqlite://./.test_concurrent_delete_file_{}.db",
        uuid::Uuid::now_v7()
    );
    let catalog = Arc::new(CayenneCatalog::new(&test_db).expect("Failed to create catalog"));

    // Initialize the catalog
    catalog.init().await.expect("Failed to initialize catalog");

    // Create a table via the catalog API to get a valid table_id
    let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
        "id",
        arrow_schema::DataType::Int64,
        false,
    )]));
    let table_options = CreateTableOptions {
        table_name: "test_table".to_string(),
        schema,
        primary_key: vec![],
        on_conflict: None,
        base_path: "/tmp/cayenne_test".to_string(),
        partition_column: None,
        vortex_config: crate::metadata::VortexConfig::default(),
    };
    let table_id = catalog
        .create_table(table_options)
        .await
        .expect("Failed to create table");

    // Spawn multiple tasks that all try to create delete files concurrently
    let mut handles = vec![];
    for i in 0..10 {
        let catalog_clone = Arc::clone(&catalog);
        let table_id = table_id.clone();

        let handle = tokio::spawn(async move {
            let delete_file = DeleteFile {
                delete_file_id: String::new(), // Will be assigned by catalog
                table_id,
                source_data_file_path: None,
                path: format!("/tmp/delete_file_{i}.parquet"),
                path_is_relative: false,
                format: "parquet".to_string(),
                delete_count: 10,
                file_size_bytes: 512,
                deletion_type: DeletionType::default(),
                sequence_number: 1, // Test sequence number
            };

            catalog_clone.add_delete_file(delete_file).await
        });

        handles.push(handle);
    }

    // Wait for all tasks to complete
    let results: Vec<_> = futures::future::join_all(handles).await;

    // All tasks should succeed with unique delete_file_ids
    let mut delete_file_ids = vec![];
    for result in results {
        let delete_file_id = result
            .expect("Task panicked")
            .expect("add_delete_file failed");
        delete_file_ids.push(delete_file_id);
    }

    // All delete_file_ids should be unique (unlike tables/partitions which are idempotent)
    let unique_ids: std::collections::HashSet<_> = delete_file_ids.iter().collect();
    assert_eq!(
        unique_ids.len(),
        delete_file_ids.len(),
        "All concurrent add_delete_file calls should return unique delete_file_ids"
    );

    // Verify all delete files were created
    let delete_files = catalog
        .get_table_delete_files(&table_id)
        .await
        .expect("Failed to get delete files");

    assert_eq!(delete_files.len(), 10);

    // Cleanup test database
    let db_path = test_db.strip_prefix("sqlite://").unwrap_or(&test_db);
    let _ = std::fs::remove_file(db_path);
    let _ = std::fs::remove_file(format!("{db_path}-shm"));
    let _ = std::fs::remove_file(format!("{db_path}-wal"));
}

#[tokio::test]
async fn test_concurrent_delete_file_creation_is_idempotent_for_same_path() {
    let test_db = format!(
        "sqlite://./.test_concurrent_delete_file_same_path_{}.db",
        uuid::Uuid::now_v7()
    );
    let catalog = Arc::new(CayenneCatalog::new(&test_db).expect("Failed to create catalog"));

    catalog.init().await.expect("Failed to initialize catalog");

    let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
        "id",
        arrow_schema::DataType::Int64,
        false,
    )]));
    let table_options = CreateTableOptions {
        table_name: "test_table_same_path".to_string(),
        schema,
        primary_key: vec![],
        on_conflict: None,
        base_path: "/tmp/cayenne_test".to_string(),
        partition_column: None,
        vortex_config: crate::metadata::VortexConfig::default(),
    };
    let table_id = catalog
        .create_table(table_options)
        .await
        .expect("Failed to create table");

    let mut handles = vec![];
    for _ in 0..10 {
        let catalog_clone = Arc::clone(&catalog);
        let table_id = table_id.clone();

        let handle = tokio::spawn(async move {
            let delete_file = DeleteFile {
                delete_file_id: String::new(),
                table_id,
                source_data_file_path: None,
                path: "/tmp/delete_file_same_path.parquet".to_string(),
                path_is_relative: false,
                format: "parquet".to_string(),
                delete_count: 10,
                file_size_bytes: 512,
                deletion_type: DeletionType::default(),
                sequence_number: 1,
            };

            catalog_clone.add_delete_file(delete_file).await
        });

        handles.push(handle);
    }

    let results: Vec<_> = futures::future::join_all(handles).await;

    let mut delete_file_ids = vec![];
    for result in results {
        let delete_file_id = result
            .expect("Task panicked")
            .expect("add_delete_file failed");
        delete_file_ids.push(delete_file_id);
    }

    let unique_ids: std::collections::HashSet<_> = delete_file_ids.iter().collect();
    assert_eq!(
        unique_ids.len(),
        1,
        "All concurrent add_delete_file calls for the same path should return the same delete_file_id"
    );

    let delete_files = catalog
        .get_table_delete_files(&table_id)
        .await
        .expect("Failed to get delete files");

    assert_eq!(delete_files.len(), 1);
    assert_eq!(delete_files[0].path, "/tmp/delete_file_same_path.parquet");

    let db_path = test_db.strip_prefix("sqlite://").unwrap_or(&test_db);
    let _ = std::fs::remove_file(db_path);
    let _ = std::fs::remove_file(format!("{db_path}-shm"));
    let _ = std::fs::remove_file(format!("{db_path}-wal"));
}

#[tokio::test]
async fn test_same_delete_file_path_rejects_conflicting_metadata() {
    let test_db = format!(
        "sqlite://./.test_conflicting_delete_file_same_path_{}.db",
        uuid::Uuid::now_v7()
    );
    let catalog = Arc::new(CayenneCatalog::new(&test_db).expect("Failed to create catalog"));

    catalog.init().await.expect("Failed to initialize catalog");

    let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
        "id",
        arrow_schema::DataType::Int64,
        false,
    )]));
    let table_options = CreateTableOptions {
        table_name: "test_table_conflicting_same_path".to_string(),
        schema,
        primary_key: vec![],
        on_conflict: None,
        base_path: "/tmp/cayenne_test".to_string(),
        partition_column: None,
        vortex_config: crate::metadata::VortexConfig::default(),
    };
    let table_id = catalog
        .create_table(table_options)
        .await
        .expect("Failed to create table");

    let delete_file = DeleteFile {
        delete_file_id: String::new(),
        table_id: table_id.clone(),
        source_data_file_path: Some("/tmp/source.parquet".to_string()),
        path: "/tmp/delete_file_same_path_conflict.parquet".to_string(),
        path_is_relative: false,
        format: "parquet".to_string(),
        delete_count: 10,
        file_size_bytes: 512,
        deletion_type: DeletionType::default(),
        sequence_number: 1,
    };

    let first_id = catalog
        .add_delete_file(delete_file.clone())
        .await
        .expect("initial add_delete_file should succeed");

    let mut conflicting_delete_file = delete_file;
    conflicting_delete_file.file_size_bytes = 1024;

    let err = catalog
        .add_delete_file(conflicting_delete_file)
        .await
        .expect_err("conflicting same-path metadata should be rejected");

    match err {
        CatalogError::FailedToAddDeleteFile { source } => match *source {
            CatalogError::ConstraintViolation { message } => {
                assert!(
                    message.contains("file_size_bytes"),
                    "expected file_size_bytes mismatch in error, got: {message}"
                );
            }
            other => panic!("expected nested ConstraintViolation, got: {other}"),
        },
        other => panic!("expected FailedToAddDeleteFile, got: {other}"),
    }

    let delete_files = catalog
        .get_table_delete_files(&table_id)
        .await
        .expect("Failed to get delete files");

    assert_eq!(delete_files.len(), 1);
    assert_eq!(delete_files[0].delete_file_id, first_id);
    assert_eq!(delete_files[0].file_size_bytes, 512);

    let db_path = test_db.strip_prefix("sqlite://").unwrap_or(&test_db);
    let _ = std::fs::remove_file(db_path);
    let _ = std::fs::remove_file(format!("{db_path}-shm"));
    let _ = std::fs::remove_file(format!("{db_path}-wal"));
}

#[tokio::test]
async fn test_commit_on_conflict_deletions_is_idempotent_for_same_delete_file() {
    let test_db = format!(
        "sqlite://./.test_on_conflict_delete_file_idempotent_{}.db",
        uuid::Uuid::now_v7()
    );
    let catalog = CayenneCatalog::new(&test_db).expect("Failed to create catalog");

    catalog.init().await.expect("Failed to initialize catalog");

    let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
        "id",
        arrow_schema::DataType::Int64,
        false,
    )]));
    let table_options = CreateTableOptions {
        table_name: "test_table_on_conflict_same_path".to_string(),
        schema,
        primary_key: vec![],
        on_conflict: None,
        base_path: "/tmp/cayenne_test".to_string(),
        partition_column: None,
        vortex_config: crate::metadata::VortexConfig::default(),
    };
    let table_id = catalog
        .create_table(table_options)
        .await
        .expect("Failed to create table");

    let delete_file = DeleteFile {
        delete_file_id: String::new(),
        table_id: table_id.clone(),
        source_data_file_path: Some("/tmp/source.parquet".to_string()),
        path: "/tmp/on_conflict_delete_file_same_path.parquet".to_string(),
        path_is_relative: false,
        format: "parquet".to_string(),
        delete_count: 10,
        file_size_bytes: 512,
        deletion_type: DeletionType::default(),
        sequence_number: 1,
    };

    catalog
        .commit_on_conflict_deletions(
            vec![delete_file.clone()],
            &table_id,
            vec![vec![1_u8]],
            2,
            None,
        )
        .await
        .expect("initial on-conflict deletion commit should succeed");
    catalog
        .commit_on_conflict_deletions(vec![delete_file], &table_id, vec![vec![1_u8]], 2, None)
        .await
        .expect("replayed on-conflict deletion commit should be idempotent");

    let delete_files = catalog
        .get_table_delete_files(&table_id)
        .await
        .expect("Failed to get delete files");
    assert_eq!(delete_files.len(), 1);
    assert_eq!(delete_files[0].file_size_bytes, 512);

    let insert_records = catalog
        .get_insert_records(&table_id)
        .await
        .expect("Failed to get insert records");
    assert_eq!(insert_records.get([1_u8].as_slice()), Some(&2));

    let db_path = test_db.strip_prefix("sqlite://").unwrap_or(&test_db);
    let _ = std::fs::remove_file(db_path);
    let _ = std::fs::remove_file(format!("{db_path}-shm"));
    let _ = std::fs::remove_file(format!("{db_path}-wal"));
}

#[tokio::test]
async fn test_commit_on_conflict_deletions_rejects_conflicting_delete_file_metadata() {
    let test_db = format!(
        "sqlite://./.test_on_conflict_delete_file_conflict_{}.db",
        uuid::Uuid::now_v7()
    );
    let catalog = CayenneCatalog::new(&test_db).expect("Failed to create catalog");

    catalog.init().await.expect("Failed to initialize catalog");

    let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
        "id",
        arrow_schema::DataType::Int64,
        false,
    )]));
    let table_options = CreateTableOptions {
        table_name: "test_table_on_conflict_conflict".to_string(),
        schema,
        primary_key: vec![],
        on_conflict: None,
        base_path: "/tmp/cayenne_test".to_string(),
        partition_column: None,
        vortex_config: crate::metadata::VortexConfig::default(),
    };
    let table_id = catalog
        .create_table(table_options)
        .await
        .expect("Failed to create table");

    let delete_file = DeleteFile {
        delete_file_id: String::new(),
        table_id: table_id.clone(),
        source_data_file_path: Some("/tmp/source.parquet".to_string()),
        path: "/tmp/on_conflict_delete_file_conflict.parquet".to_string(),
        path_is_relative: false,
        format: "parquet".to_string(),
        delete_count: 10,
        file_size_bytes: 512,
        deletion_type: DeletionType::default(),
        sequence_number: 1,
    };

    catalog
        .commit_on_conflict_deletions(
            vec![delete_file.clone()],
            &table_id,
            vec![vec![1_u8]],
            2,
            None,
        )
        .await
        .expect("initial on-conflict deletion commit should succeed");

    let mut conflicting_delete_file = delete_file;
    conflicting_delete_file.file_size_bytes = 1024;

    let err = catalog
        .commit_on_conflict_deletions(
            vec![conflicting_delete_file],
            &table_id,
            vec![vec![2_u8]],
            3,
            None,
        )
        .await
        .expect_err("conflicting delete-file metadata should be rejected");

    match err {
        CatalogError::InvalidOperation { message, source } => {
            assert!(
                message.contains("Delete-file metadata conflicts"),
                "expected descriptive on-conflict conflict message, got: {message}"
            );
            match source.downcast_ref::<CatalogError>() {
                Some(CatalogError::ConstraintViolation { message }) => {
                    assert!(
                        message.contains("file_size_bytes"),
                        "expected file_size_bytes mismatch in error, got: {message}"
                    );
                }
                Some(other) => panic!("expected nested ConstraintViolation, got: {other}"),
                None => panic!("expected nested CatalogError, got: {source}"),
            }
        }
        other => panic!("expected InvalidOperation, got: {other}"),
    }

    let delete_files = catalog
        .get_table_delete_files(&table_id)
        .await
        .expect("Failed to get delete files");
    assert_eq!(delete_files.len(), 1);
    assert_eq!(delete_files[0].file_size_bytes, 512);

    let insert_records = catalog
        .get_insert_records(&table_id)
        .await
        .expect("Failed to get insert records");
    assert_eq!(insert_records.get([1_u8].as_slice()), Some(&2));
    assert!(!insert_records.contains_key([2_u8].as_slice()));

    let db_path = test_db.strip_prefix("sqlite://").unwrap_or(&test_db);
    let _ = std::fs::remove_file(db_path);
    let _ = std::fs::remove_file(format!("{db_path}-shm"));
    let _ = std::fs::remove_file(format!("{db_path}-wal"));
}

#[tokio::test]
async fn test_commit_on_conflict_deletions_batches_multiple_delete_files() {
    // Exercises the batched multi-VALUES INSERT path: multiple distinct
    // delete files committed in a single transaction must all be visible
    // afterward and produce a single row per (table_id, path).
    let test_db = format!(
        "sqlite://./.test_on_conflict_delete_file_batched_{}.db",
        uuid::Uuid::now_v7()
    );
    let catalog = CayenneCatalog::new(&test_db).expect("Failed to create catalog");

    catalog.init().await.expect("Failed to initialize catalog");

    let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
        "id",
        arrow_schema::DataType::Int64,
        false,
    )]));
    let table_options = CreateTableOptions {
        table_name: "test_table_on_conflict_batched".to_string(),
        schema,
        primary_key: vec![],
        on_conflict: None,
        base_path: "/tmp/cayenne_test".to_string(),
        partition_column: None,
        vortex_config: crate::metadata::VortexConfig::default(),
    };
    let table_id = catalog
        .create_table(table_options)
        .await
        .expect("Failed to create table");

    let make_delete_file = |idx: usize| DeleteFile {
        delete_file_id: String::new(),
        table_id: table_id.clone(),
        source_data_file_path: Some(format!("/tmp/source_{idx}.parquet")),
        path: format!("/tmp/on_conflict_delete_file_batched_{idx}.parquet"),
        path_is_relative: false,
        format: "parquet".to_string(),
        delete_count: 10,
        file_size_bytes: 512,
        deletion_type: DeletionType::default(),
        sequence_number: 1,
    };

    let delete_files: Vec<DeleteFile> = (0..5).map(make_delete_file).collect();
    let insert_pks: Vec<Vec<u8>> = (0..5_u8).map(|i| vec![i]).collect();

    catalog
        .commit_on_conflict_deletions(delete_files.clone(), &table_id, insert_pks, 2, None)
        .await
        .expect("batched on-conflict deletion commit should succeed");

    let stored = catalog
        .get_table_delete_files(&table_id)
        .await
        .expect("Failed to get delete files");
    assert_eq!(stored.len(), 5);
    let stored_paths: std::collections::HashSet<&str> =
        stored.iter().map(|d| d.path.as_str()).collect();
    for expected in &delete_files {
        assert!(
            stored_paths.contains(expected.path.as_str()),
            "missing delete file path: {}",
            expected.path
        );
    }

    // Replay should be idempotent across the whole batch.
    catalog
        .commit_on_conflict_deletions(delete_files, &table_id, vec![vec![0_u8]], 2, None)
        .await
        .expect("replayed batched on-conflict deletion commit should be idempotent");
    let stored = catalog
        .get_table_delete_files(&table_id)
        .await
        .expect("Failed to get delete files after replay");
    assert_eq!(stored.len(), 5);

    let db_path = test_db.strip_prefix("sqlite://").unwrap_or(&test_db);
    let _ = std::fs::remove_file(db_path);
    let _ = std::fs::remove_file(format!("{db_path}-shm"));
    let _ = std::fs::remove_file(format!("{db_path}-wal"));
}

#[tokio::test]
async fn test_commit_on_conflict_deletions_with_tombstone_folds_one_txn() {
    // The cycle-3 Stage-A fold: delete files + insert records + the
    // protected-snapshot sequence + the Option-D inline tombstone must all be
    // committed by a single call, and the tombstone must land
    // `published = false` (inert) with the returned id.
    let test_db = format!(
        "sqlite://./.test_on_conflict_with_tombstone_fold_{}.db",
        uuid::Uuid::now_v7()
    );
    let catalog = CayenneCatalog::new(&test_db).expect("Failed to create catalog");
    catalog.init().await.expect("Failed to initialize catalog");

    let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
        "id",
        arrow_schema::DataType::Int64,
        false,
    )]));
    let table_options = CreateTableOptions {
        table_name: "test_table_with_tombstone_fold".to_string(),
        schema,
        primary_key: vec![],
        on_conflict: None,
        base_path: "/tmp/cayenne_test".to_string(),
        partition_column: None,
        vortex_config: crate::metadata::VortexConfig::default(),
    };
    let table_id = catalog
        .create_table(table_options)
        .await
        .expect("Failed to create table");

    let delete_file = DeleteFile {
        delete_file_id: String::new(),
        table_id: table_id.clone(),
        source_data_file_path: Some("/tmp/source_fold.parquet".to_string()),
        path: "/tmp/on_conflict_with_tombstone_fold.parquet".to_string(),
        path_is_relative: false,
        format: "parquet".to_string(),
        delete_count: 4,
        file_size_bytes: 256,
        deletion_type: DeletionType::default(),
        sequence_number: 1,
    };
    let snapshot_id = uuid::Uuid::now_v7().to_string();
    let tombstone = InlinedDelete {
        inlined_id: String::new(),
        table_id: table_id.clone(),
        delete_ipc: vec![7_u8, 8, 9],
        delete_count: 2,
        sequence_number: 1,
        created_at: String::new(),
        published: false,
    };

    let returned_id = catalog
        .commit_on_conflict_deletions_with_tombstone(
            vec![delete_file],
            &table_id,
            vec![vec![0_u8]],
            2,
            Some(SnapshotSequenceCommit {
                snapshot_id: snapshot_id.clone(),
                sequence_number: 3,
            }),
            Some(tombstone),
            &[],
        )
        .await
        .expect("folded on-conflict commit should succeed")
        .expect("a tombstone was supplied, so an inlined_id must be returned");

    // Deletion metadata landed.
    let stored = catalog
        .get_table_delete_files(&table_id)
        .await
        .expect("get delete files");
    assert_eq!(
        stored.len(),
        1,
        "delete file should be committed by the fold"
    );

    // Protected-snapshot sequence landed in the SAME call.
    let seq = catalog
        .get_snapshot_sequence(&table_id, &snapshot_id)
        .await
        .expect("get snapshot sequence");
    assert_eq!(
        seq,
        Some(3),
        "snapshot sequence should be committed by the fold"
    );

    // The tombstone landed `published = false` (inert): visible to the
    // unfiltered read, hidden from the published-only hot read path.
    let all = catalog
        .get_inlined_deletes(&table_id)
        .await
        .expect("get inlined deletes");
    assert_eq!(all.len(), 1, "the tombstone must be persisted by the fold");
    assert_eq!(all[0].inlined_id, returned_id, "returned id must match row");
    assert!(
        !all[0].published,
        "the staged tombstone must be inert (published = false)"
    );
    let published = catalog
        .get_published_inlined_deletes(&table_id)
        .await
        .expect("get published inlined deletes");
    assert!(
        published.is_empty(),
        "an unpublished tombstone must not appear on the published-only read path"
    );

    // The returned id is exactly the row a Stage-B flip would target.
    catalog
        .mark_inlined_delete_published(&table_id, &returned_id)
        .await
        .expect("flip should target the returned id");
    let published = catalog
        .get_published_inlined_deletes(&table_id)
        .await
        .expect("get published inlined deletes after flip");
    assert_eq!(
        published.len(),
        1,
        "after the flip the tombstone becomes visible on the published path"
    );

    let db_path = test_db.strip_prefix("sqlite://").unwrap_or(&test_db);
    let _ = std::fs::remove_file(db_path);
    let _ = std::fs::remove_file(format!("{db_path}-shm"));
    let _ = std::fs::remove_file(format!("{db_path}-wal"));
}

#[tokio::test]
async fn test_commit_on_conflict_deletions_with_tombstone_batches_deferred_flips() {
    // cycle-8 TASK D4: a batch carrying several deferred `published = 1` flips
    // must publish ALL of them in the one folded txn — the batched
    // `… inlined_id IN (…)` UPDATE is exactly equivalent to the prior per-row
    // loop. Stage several inert tombstones, then ride their ids as
    // `pending_durable_flips` on a later commit and assert every one flips.
    let test_db = format!(
        "sqlite://./.test_on_conflict_tombstone_flip_batch_{}.db",
        uuid::Uuid::now_v7()
    );
    let catalog = CayenneCatalog::new(&test_db).expect("Failed to create catalog");
    catalog.init().await.expect("Failed to initialize catalog");

    let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
        "id",
        arrow_schema::DataType::Int64,
        false,
    )]));
    let table_id = catalog
        .create_table(CreateTableOptions {
            table_name: "test_table_flip_batch".to_string(),
            schema,
            primary_key: vec![],
            on_conflict: None,
            base_path: "/tmp/cayenne_test".to_string(),
            partition_column: None,
            vortex_config: crate::metadata::VortexConfig::default(),
        })
        .await
        .expect("Failed to create table");

    // Stage 5 inert (published = false) tombstones, capturing their ids.
    let mut staged_ids = Vec::new();
    for i in 0..5_i64 {
        let id = catalog
            .commit_on_conflict_deletions_with_tombstone(
                vec![],
                &table_id,
                vec![],
                0,
                None,
                Some(InlinedDelete {
                    inlined_id: String::new(),
                    table_id: table_id.clone(),
                    delete_ipc: vec![u8::try_from(i).unwrap_or(0)],
                    delete_count: 1,
                    sequence_number: i + 1,
                    created_at: String::new(),
                    published: false,
                }),
                &[],
            )
            .await
            .expect("stage inert tombstone")
            .expect("tombstone id");
        staged_ids.push(id);
    }

    // None are published yet.
    assert!(
        catalog
            .get_published_inlined_deletes(&table_id)
            .await
            .expect("get published")
            .is_empty(),
        "freshly-staged tombstones must be inert"
    );

    // Commit a batch that carries ALL 5 ids as deferred flips (no other work).
    let ret = catalog
        .commit_on_conflict_deletions_with_tombstone(
            vec![],
            &table_id,
            vec![],
            0,
            None,
            None,
            &staged_ids,
        )
        .await
        .expect("flip-only commit should succeed");
    assert!(ret.is_none(), "no tombstone supplied ⇒ no returned id");

    // Every staged tombstone is now published — the batched IN-list UPDATE
    // flipped them all in the one folded transaction.
    let published = catalog
        .get_published_inlined_deletes(&table_id)
        .await
        .expect("get published after batched flips");
    assert_eq!(
        published.len(),
        5,
        "all deferred flips must be applied by the batched UPDATE"
    );
    let mut published_ids: Vec<String> = published.into_iter().map(|d| d.inlined_id).collect();
    published_ids.sort();
    let mut expected = staged_ids.clone();
    expected.sort();
    assert_eq!(
        published_ids, expected,
        "exactly the staged ids must be flipped (no more, no fewer)"
    );

    // Idempotent: re-flipping the same ids is a harmless no-op (set-to-1).
    catalog
        .commit_on_conflict_deletions_with_tombstone(
            vec![],
            &table_id,
            vec![],
            0,
            None,
            None,
            &staged_ids,
        )
        .await
        .expect("re-flip should be an idempotent no-op");
    assert_eq!(
        catalog
            .get_published_inlined_deletes(&table_id)
            .await
            .expect("get published after re-flip")
            .len(),
        5,
        "re-applying the flips must not change the published set"
    );

    let db_path = test_db.strip_prefix("sqlite://").unwrap_or(&test_db);
    let _ = std::fs::remove_file(db_path);
    let _ = std::fs::remove_file(format!("{db_path}-shm"));
    let _ = std::fs::remove_file(format!("{db_path}-wal"));
}

#[tokio::test]
async fn test_commit_on_conflict_deletions_with_tombstone_none_returns_none() {
    // With no tombstone, the fold behaves exactly like
    // `commit_on_conflict_deletions` and returns `Ok(None)`.
    let test_db = format!(
        "sqlite://./.test_on_conflict_with_tombstone_none_{}.db",
        uuid::Uuid::now_v7()
    );
    let catalog = CayenneCatalog::new(&test_db).expect("Failed to create catalog");
    catalog.init().await.expect("Failed to initialize catalog");

    let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
        "id",
        arrow_schema::DataType::Int64,
        false,
    )]));
    let table_options = CreateTableOptions {
        table_name: "test_table_with_tombstone_none".to_string(),
        schema,
        primary_key: vec![],
        on_conflict: None,
        base_path: "/tmp/cayenne_test".to_string(),
        partition_column: None,
        vortex_config: crate::metadata::VortexConfig::default(),
    };
    let table_id = catalog
        .create_table(table_options)
        .await
        .expect("Failed to create table");

    let delete_file = DeleteFile {
        delete_file_id: String::new(),
        table_id: table_id.clone(),
        source_data_file_path: Some("/tmp/source_none.parquet".to_string()),
        path: "/tmp/on_conflict_with_tombstone_none.parquet".to_string(),
        path_is_relative: false,
        format: "parquet".to_string(),
        delete_count: 1,
        file_size_bytes: 128,
        deletion_type: DeletionType::default(),
        sequence_number: 1,
    };

    let returned = catalog
        .commit_on_conflict_deletions_with_tombstone(
            vec![delete_file],
            &table_id,
            vec![vec![0_u8]],
            2,
            None,
            None,
            &[],
        )
        .await
        .expect("folded commit without a tombstone should succeed");
    assert!(
        returned.is_none(),
        "no tombstone supplied => no inlined_id returned"
    );
    let stored = catalog
        .get_table_delete_files(&table_id)
        .await
        .expect("get delete files");
    assert_eq!(stored.len(), 1, "delete file should still be committed");
    let all = catalog
        .get_inlined_deletes(&table_id)
        .await
        .expect("get inlined deletes");
    assert!(all.is_empty(), "no tombstone should have been written");

    let db_path = test_db.strip_prefix("sqlite://").unwrap_or(&test_db);
    let _ = std::fs::remove_file(db_path);
    let _ = std::fs::remove_file(format!("{db_path}-shm"));
    let _ = std::fs::remove_file(format!("{db_path}-wal"));
}

#[tokio::test]
async fn test_concurrent_sequence_reservations_do_not_overlap() {
    const TASK_COUNT: usize = 16;
    const BLOCK_SIZE: u32 = 2;

    let test_db = format!(
        "sqlite://./.test_sequence_reservation_concurrency_{}.db",
        uuid::Uuid::now_v7()
    );
    let catalog = Arc::new(CayenneCatalog::new(&test_db).expect("Failed to create catalog"));

    catalog.init().await.expect("Failed to initialize catalog");

    let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
        "id",
        arrow_schema::DataType::Int64,
        false,
    )]));
    let table_options = CreateTableOptions {
        table_name: "test_sequence_reservation_concurrency".to_string(),
        schema,
        primary_key: vec![],
        on_conflict: None,
        base_path: "/tmp/cayenne_test".to_string(),
        partition_column: None,
        vortex_config: crate::metadata::VortexConfig::default(),
    };
    let table_id = catalog
        .create_table(table_options)
        .await
        .expect("Failed to create table");

    let mut tasks = Vec::with_capacity(TASK_COUNT);
    for _ in 0..TASK_COUNT {
        let catalog = Arc::clone(&catalog);
        let table_id = table_id.clone();
        tasks.push(tokio::spawn(async move {
            catalog
                .reserve_sequence_numbers(&table_id, BLOCK_SIZE)
                .await
                .expect("sequence reservation should succeed")
        }));
    }

    let block_size_usize = usize::try_from(BLOCK_SIZE).expect("BLOCK_SIZE fits in usize");
    let mut reserved_sequences = Vec::with_capacity(TASK_COUNT * block_size_usize);
    for task in tasks {
        let block_start = task.await.expect("reservation task should join");
        for offset in 0..BLOCK_SIZE {
            reserved_sequences.push(block_start + i64::from(offset));
        }
    }

    reserved_sequences.sort_unstable();
    assert_eq!(reserved_sequences.first().copied(), Some(1));
    assert_eq!(
        reserved_sequences.last().copied(),
        Some(i64::try_from(TASK_COUNT).expect("TASK_COUNT fits in i64") * i64::from(BLOCK_SIZE))
    );
    for (expected, actual) in (1_i64..).zip(&reserved_sequences) {
        assert_eq!(*actual, expected);
    }

    let final_sequence = catalog
        .get_sequence_number(&table_id)
        .await
        .expect("Failed to get final sequence number");
    assert_eq!(
        final_sequence,
        i64::try_from(TASK_COUNT).expect("TASK_COUNT fits in i64") * i64::from(BLOCK_SIZE)
    );

    let db_path = test_db.strip_prefix("sqlite://").unwrap_or(&test_db);
    let _ = std::fs::remove_file(db_path);
    let _ = std::fs::remove_file(format!("{db_path}-shm"));
    let _ = std::fs::remove_file(format!("{db_path}-wal"));
}

#[tokio::test]
async fn test_reserve_sequence_numbers_missing_table_errors() {
    let test_db = format!(
        "sqlite://./.test_sequence_reservation_missing_table_{}.db",
        uuid::Uuid::now_v7()
    );
    let catalog = CayenneCatalog::new(&test_db).expect("Failed to create catalog");

    catalog.init().await.expect("Failed to initialize catalog");

    let err = catalog
        .reserve_sequence_numbers("missing_table", 2)
        .await
        .expect_err("missing table sequence reservation should fail");

    match err {
        CatalogError::InvalidOperationNoSource { message } => assert!(
            message.contains("table row does not exist"),
            "expected missing-table error, got: {message}"
        ),
        other => panic!("expected InvalidOperationNoSource, got: {other}"),
    }

    let db_path = test_db.strip_prefix("sqlite://").unwrap_or(&test_db);
    let _ = std::fs::remove_file(db_path);
    let _ = std::fs::remove_file(format!("{db_path}-shm"));
    let _ = std::fs::remove_file(format!("{db_path}-wal"));
}

/// Test that shutdown properly flushes WAL and data persists across catalog restarts.
#[tokio::test]
async fn test_shutdown_wal_checkpoint_and_reload() {
    // Create a unique test database
    let test_db = format!(
        "sqlite://./.test_shutdown_reload_{}.db",
        uuid::Uuid::now_v7()
    );
    let db_path = test_db.strip_prefix("sqlite://").expect("test db path");

    // Create test schema
    let schema = Arc::new(arrow_schema::Schema::new(vec![
        arrow_schema::Field::new("id", arrow_schema::DataType::Int64, false),
        arrow_schema::Field::new("name", arrow_schema::DataType::Utf8, true),
    ]));

    let table_name = "test_shutdown_table";
    let base_path = "/tmp/cayenne_shutdown_test";

    // Phase 1: Create catalog, add data, shutdown
    let table_id;
    {
        let catalog = CayenneCatalog::new(&test_db).expect("Failed to create catalog");
        catalog.init().await.expect("Failed to initialize catalog");

        // Create a table
        let options = CreateTableOptions {
            table_name: table_name.to_string(),
            schema: Arc::clone(&schema),
            primary_key: vec!["id".to_string()],
            on_conflict: None,
            base_path: base_path.to_string(),
            partition_column: Some("name".to_string()),
            vortex_config: crate::metadata::VortexConfig::default(),
        };

        table_id = catalog
            .create_table(options)
            .await
            .expect("Failed to create table");

        // Add a partition
        let partition = PartitionMetadata {
            partition_id: String::new(),
            table_id: table_id.clone(),
            partition_columns: vec!["name".to_string()],
            partition_values: vec!["test_value".to_string()],
            path: format!("{base_path}/partition_test"),
            path_is_relative: false,
            record_count: 100,
            file_size_bytes: 2048,
        };
        catalog
            .add_partition(partition)
            .await
            .expect("Failed to add partition");

        // Add a delete file
        let delete_file = DeleteFile {
            delete_file_id: String::new(),
            table_id: table_id.clone(),
            source_data_file_path: None,
            path: format!("{base_path}/delete_file.parquet"),
            path_is_relative: false,
            format: "parquet".to_string(),
            delete_count: 5,
            file_size_bytes: 256,
            deletion_type: DeletionType::default(),
            sequence_number: 1,
        };
        catalog
            .add_delete_file(delete_file)
            .await
            .expect("Failed to add delete file");

        // Increment sequence number
        let seq = catalog
            .increment_sequence_number(&table_id)
            .await
            .expect("Failed to increment sequence");
        assert_eq!(seq, 1);

        // Perform graceful shutdown - this checkpoints the WAL
        catalog
            .shutdown()
            .await
            .expect("Failed to shutdown catalog");

        // Catalog goes out of scope here, connection is dropped
    }

    // Phase 2: Reopen catalog and verify all data persisted correctly
    {
        let catalog = CayenneCatalog::new(&test_db).expect("Failed to reopen catalog");
        catalog
            .init()
            .await
            .expect("Failed to reinitialize catalog");

        // Verify table exists with correct metadata
        let table = catalog
            .get_table(table_name)
            .await
            .expect("Table should exist after restart");

        assert_eq!(table.table_id, table_id);
        assert_eq!(table.table_name, table_name);
        assert_eq!(table.primary_key, vec!["id".to_string()]);
        assert_eq!(table.partition_column, Some("name".to_string()));
        assert_eq!(table.current_sequence_number, 1);

        // Verify partition persisted
        let partitions = catalog
            .get_partitions(&table_id)
            .await
            .expect("Failed to get partitions");
        assert_eq!(partitions.len(), 1);
        assert_eq!(partitions[0].partition_values, vec!["test_value"]);
        assert_eq!(partitions[0].record_count, 100);

        // Verify delete file persisted
        let delete_files = catalog
            .get_table_delete_files(&table_id)
            .await
            .expect("Failed to get delete files");
        assert_eq!(delete_files.len(), 1);
        assert_eq!(delete_files[0].delete_count, 5);
        assert_eq!(delete_files[0].sequence_number, 1);

        // Verify sequence number persisted
        let seq = catalog
            .get_sequence_number(&table_id)
            .await
            .expect("Failed to get sequence number");
        assert_eq!(seq, 1);
    }

    // Cleanup
    let _ = std::fs::remove_file(db_path);
    let _ = std::fs::remove_file(format!("{db_path}-shm"));
    let _ = std::fs::remove_file(format!("{db_path}-wal"));
}

/// Test multiple shutdown/reload cycles to ensure repeated restarts maintain integrity.
#[tokio::test]
async fn test_multiple_shutdown_reload_cycles() {
    let test_db = format!(
        "sqlite://./.test_multi_shutdown_{}.db",
        uuid::Uuid::now_v7()
    );
    let db_path = test_db.strip_prefix("sqlite://").expect("test db path");

    let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
        "id",
        arrow_schema::DataType::Int64,
        false,
    )]));

    let table_name = "cycle_test_table";
    let base_path = "/tmp/cayenne_cycle_test";

    // Cycle 1: Create table
    let table_id;
    {
        let catalog = CayenneCatalog::new(&test_db).expect("Failed to create catalog");
        catalog.init().await.expect("Failed to init");

        let options = CreateTableOptions {
            table_name: table_name.to_string(),
            schema: Arc::clone(&schema),
            primary_key: vec![],
            on_conflict: None,
            base_path: base_path.to_string(),
            partition_column: None,
            vortex_config: crate::metadata::VortexConfig::default(),
        };

        table_id = catalog
            .create_table(options)
            .await
            .expect("Failed to create table");
        catalog.shutdown().await.expect("Shutdown failed");
    }

    // Cycle 2: Add delete files
    {
        let catalog = CayenneCatalog::new(&test_db).expect("Failed to reopen");
        catalog.init().await.expect("Failed to init");

        for i in 0..5 {
            let delete_file = DeleteFile {
                delete_file_id: String::new(),
                table_id: table_id.clone(),
                source_data_file_path: None,
                path: format!("{base_path}/delete_{i}.parquet"),
                path_is_relative: false,
                format: "parquet".to_string(),
                delete_count: i + 1,
                file_size_bytes: 100,
                deletion_type: DeletionType::default(),
                sequence_number: i + 1,
            };
            catalog
                .add_delete_file(delete_file)
                .await
                .expect("Failed to add delete file");
        }

        catalog.shutdown().await.expect("Shutdown failed");
    }

    // Cycle 3: Verify and modify
    {
        let catalog = CayenneCatalog::new(&test_db).expect("Failed to reopen");
        catalog.init().await.expect("Failed to init");

        let delete_files = catalog
            .get_table_delete_files(&table_id)
            .await
            .expect("Failed to get delete files");
        assert_eq!(delete_files.len(), 5, "All 5 delete files should persist");

        // Increment sequence number multiple times
        for _ in 0..3 {
            catalog
                .increment_sequence_number(&table_id)
                .await
                .expect("Failed to increment");
        }

        catalog.shutdown().await.expect("Shutdown failed");
    }

    // Cycle 4: Final verification
    {
        let catalog = CayenneCatalog::new(&test_db).expect("Failed to reopen");
        catalog.init().await.expect("Failed to init");

        let table = catalog
            .get_table(table_name)
            .await
            .expect("Table should exist");
        assert_eq!(
            table.current_sequence_number, 3,
            "Sequence number should be 3 after 3 increments"
        );

        let delete_files = catalog
            .get_table_delete_files(&table_id)
            .await
            .expect("Failed to get delete files");
        assert_eq!(delete_files.len(), 5);

        // Verify delete file sequence numbers
        let mut seq_nums: Vec<i64> = delete_files.iter().map(|f| f.sequence_number).collect();
        seq_nums.sort_unstable();
        assert_eq!(seq_nums, vec![1, 2, 3, 4, 5]);

        catalog.shutdown().await.expect("Shutdown failed");
    }

    // Cleanup
    let _ = std::fs::remove_file(db_path);
    let _ = std::fs::remove_file(format!("{db_path}-shm"));
    let _ = std::fs::remove_file(format!("{db_path}-wal"));
}

/// Test that data persists even without explicit shutdown (WAL should still be readable).
#[tokio::test]
async fn test_data_persists_without_explicit_shutdown() {
    let test_db = format!("sqlite://./.test_no_shutdown_{}.db", uuid::Uuid::now_v7());
    let db_path = test_db.strip_prefix("sqlite://").expect("test db path");

    let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
        "id",
        arrow_schema::DataType::Int64,
        false,
    )]));

    let table_name = "no_shutdown_table";

    // Create and populate without explicit shutdown
    let table_id;
    {
        let catalog = CayenneCatalog::new(&test_db).expect("Failed to create catalog");
        catalog.init().await.expect("Failed to init");

        let options = CreateTableOptions {
            table_name: table_name.to_string(),
            schema,
            primary_key: vec![],
            on_conflict: None,
            base_path: "/tmp/no_shutdown_test".to_string(),
            partition_column: None,
            vortex_config: crate::metadata::VortexConfig::default(),
        };

        table_id = catalog
            .create_table(options)
            .await
            .expect("Failed to create table");

        // Add some data
        catalog
            .increment_sequence_number(&table_id)
            .await
            .expect("Failed to increment");
        catalog
            .increment_sequence_number(&table_id)
            .await
            .expect("Failed to increment");

        // NO explicit shutdown - catalog just drops
    }

    // Reopen and verify data is still accessible (SQLite WAL recovery)
    {
        let catalog = CayenneCatalog::new(&test_db).expect("Failed to reopen");
        catalog.init().await.expect("Failed to init");

        let table = catalog
            .get_table(table_name)
            .await
            .expect("Table should exist");
        assert_eq!(table.table_id, table_id);
        assert_eq!(table.current_sequence_number, 2, "Sequence should be 2");

        // Now do proper shutdown
        catalog.shutdown().await.expect("Shutdown failed");
    }

    // Cleanup
    let _ = std::fs::remove_file(db_path);
    let _ = std::fs::remove_file(format!("{db_path}-shm"));
    let _ = std::fs::remove_file(format!("{db_path}-wal"));
}

/// Test insert records persist across shutdown/reload.
#[tokio::test]
async fn test_insert_records_persist_across_restart() {
    let test_db = format!(
        "sqlite://./.test_insert_records_{}.db",
        uuid::Uuid::now_v7()
    );
    let db_path = test_db.strip_prefix("sqlite://").expect("test db path");

    let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
        "id",
        arrow_schema::DataType::Int64,
        false,
    )]));

    // Create and add insert records
    let table_id;
    {
        let catalog = CayenneCatalog::new(&test_db).expect("Failed to create catalog");
        catalog.init().await.expect("Failed to init");

        let options = CreateTableOptions {
            table_name: "insert_record_test".to_string(),
            schema,
            primary_key: vec!["id".to_string()],
            on_conflict: None,
            base_path: "/tmp/insert_record_test".to_string(),
            partition_column: None,
            vortex_config: crate::metadata::VortexConfig::default(),
        };

        table_id = catalog
            .create_table(options)
            .await
            .expect("Failed to create table");

        // Add individual insert records
        catalog
            .add_insert_record(&table_id, vec![1, 2, 3, 4], 1)
            .await
            .expect("Failed to add insert record");
        catalog
            .add_insert_record(&table_id, vec![5, 6, 7, 8], 2)
            .await
            .expect("Failed to add insert record");

        // Add batch insert records
        catalog
            .add_insert_records_batch(&table_id, vec![vec![9, 10], vec![11, 12], vec![13, 14]], 3)
            .await
            .expect("Failed to add batch insert records");

        catalog.shutdown().await.expect("Shutdown failed");
    }

    // Reopen and verify
    {
        let catalog = CayenneCatalog::new(&test_db).expect("Failed to reopen");
        catalog.init().await.expect("Failed to init");

        let records = catalog
            .get_insert_records(&table_id)
            .await
            .expect("Failed to get insert records");

        assert_eq!(records.len(), 5, "Should have 5 insert records");

        // Verify specific records by converting to Box<[u8]> for lookup
        let key1: Box<[u8]> = vec![1u8, 2, 3, 4].into_boxed_slice();
        let key2: Box<[u8]> = vec![5u8, 6, 7, 8].into_boxed_slice();
        let key3: Box<[u8]> = vec![9u8, 10].into_boxed_slice();
        let key4: Box<[u8]> = vec![11u8, 12].into_boxed_slice();
        let key5: Box<[u8]> = vec![13u8, 14].into_boxed_slice();

        assert_eq!(records.get(&key1), Some(&1i64));
        assert_eq!(records.get(&key2), Some(&2i64));
        assert_eq!(records.get(&key3), Some(&3i64));
        assert_eq!(records.get(&key4), Some(&3i64));
        assert_eq!(records.get(&key5), Some(&3i64));

        catalog.shutdown().await.expect("Shutdown failed");
    }

    // Cleanup
    let _ = std::fs::remove_file(db_path);
    let _ = std::fs::remove_file(format!("{db_path}-shm"));
    let _ = std::fs::remove_file(format!("{db_path}-wal"));
}

/// Delete-then-reinsert visibility round-trips through the raw-UUID-bytes
/// `table_id` BLOB encoding: an insert-record written for a re-inserted PK
/// is read back verbatim (`pk_bytes` + `sequence_number`), a checkpoint
/// clear (`clear_insert_records`) empties it, and a fresh re-insert lands
/// again. This exercises the full catalog write→BLOB→read→clear cycle the
/// deletion-visibility ordering depends on (`insert_seq` > `delete_seq` ⇒ the
/// PK is resurrected). Uses a real `now_v7()` `table_id` from `create_table`
/// so the 16-byte encoding path (not the non-UUID fallback) is taken.
#[tokio::test]
async fn test_insert_record_delete_reinsert_visibility_roundtrip_blob_key() {
    let test_db = format!(
        "sqlite://./.test_insert_record_vis_{}.db",
        uuid::Uuid::now_v7()
    );
    let db_path = test_db.strip_prefix("sqlite://").expect("test db path");

    let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
        "id",
        arrow_schema::DataType::Int64,
        false,
    )]));

    let catalog = CayenneCatalog::new(&test_db).expect("create catalog");
    catalog.init().await.expect("init");

    let table_id = catalog
        .create_table(CreateTableOptions {
            table_name: "vis_roundtrip".to_string(),
            schema,
            primary_key: vec!["id".to_string()],
            on_conflict: None,
            base_path: "/tmp/vis_roundtrip".to_string(),
            partition_column: None,
            vortex_config: crate::metadata::VortexConfig::default(),
        })
        .await
        .expect("create table");

    // The table_id minted by create_table is a well-formed UUID, so the
    // compact 16-byte encoding (not the fallback) is what gets stored.
    assert!(
        uuid::Uuid::parse_str(&table_id).is_ok(),
        "create_table must mint a UUID table_id"
    );

    // A PK that was deleted (at some delete_seq) is re-inserted at seq=5.
    let pk: Vec<u8> = 7_i64.to_be_bytes().to_vec();
    catalog
        .add_insert_record(&table_id, pk.clone(), 5)
        .await
        .expect("add insert record");

    // Read back: the reader returns pk_bytes + sequence_number, keyed by
    // the BLOB table_id WHERE filter.
    let records = catalog
        .get_insert_records(&table_id)
        .await
        .expect("get insert records");
    let key: Box<[u8]> = pk.clone().into_boxed_slice();
    assert_eq!(
        records.get(&key),
        Some(&5_i64),
        "the re-inserted PK's insert sequence (5) must round-trip through the BLOB key"
    );

    // Re-insert the SAME PK with a newer sequence → INSERT OR REPLACE
    // collapses to one row with the latest seq (the conflict target is
    // still (table_id, pk_bytes) with the BLOB table_id).
    catalog
        .add_insert_record(&table_id, pk.clone(), 9)
        .await
        .expect("re-add insert record");
    let records = catalog
        .get_insert_records(&table_id)
        .await
        .expect("get insert records 2");
    assert_eq!(records.len(), 1, "same PK must not duplicate");
    assert_eq!(
        records.get(&key),
        Some(&9_i64),
        "the later insert sequence must win the upsert"
    );

    // Checkpoint clear empties the insert-records for this table_id.
    catalog
        .clear_insert_records(&table_id)
        .await
        .expect("clear insert records");
    let records = catalog
        .get_insert_records(&table_id)
        .await
        .expect("get insert records after clear");
    assert!(
        records.is_empty(),
        "clear_insert_records must empty the BLOB-keyed rows"
    );

    // A fresh re-insert after the clear lands again (post-clear rebuild).
    catalog
        .add_insert_record(&table_id, pk.clone(), 12)
        .await
        .expect("add after clear");
    let records = catalog
        .get_insert_records(&table_id)
        .await
        .expect("get after re-add");
    assert_eq!(
        records.get(&key),
        Some(&12_i64),
        "re-insert after a checkpoint clear must be visible again"
    );

    catalog.shutdown().await.expect("shutdown");
    let _ = std::fs::remove_file(db_path);
    let _ = std::fs::remove_file(format!("{db_path}-shm"));
    let _ = std::fs::remove_file(format!("{db_path}-wal"));
}

/// Test snapshot sequences persist across restart.
#[tokio::test]
async fn test_snapshot_sequences_persist_across_restart() {
    let test_db = format!("sqlite://./.test_snapshot_seq_{}.db", uuid::Uuid::now_v7());
    let db_path = test_db.strip_prefix("sqlite://").expect("test db path");

    let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
        "id",
        arrow_schema::DataType::Int64,
        false,
    )]));

    let snapshot_1 = uuid::Uuid::now_v7().to_string();
    let snapshot_2 = uuid::Uuid::now_v7().to_string();
    let snapshot_3 = uuid::Uuid::now_v7().to_string();

    // Create and set snapshot sequences
    let table_id;
    {
        let catalog = CayenneCatalog::new(&test_db).expect("Failed to create catalog");
        catalog.init().await.expect("Failed to init");

        let options = CreateTableOptions {
            table_name: "snapshot_seq_test".to_string(),
            schema,
            primary_key: vec![],
            on_conflict: None,
            base_path: "/tmp/snapshot_seq_test".to_string(),
            partition_column: None,
            vortex_config: crate::metadata::VortexConfig::default(),
        };

        table_id = catalog
            .create_table(options)
            .await
            .expect("Failed to create table");

        catalog
            .set_snapshot_sequence(&table_id, &snapshot_1, 10)
            .await
            .expect("Failed to set snapshot seq");
        catalog
            .set_snapshot_sequence(&table_id, &snapshot_2, 20)
            .await
            .expect("Failed to set snapshot seq");
        catalog
            .set_snapshot_sequence(&table_id, &snapshot_3, 30)
            .await
            .expect("Failed to set snapshot seq");

        catalog.shutdown().await.expect("Shutdown failed");
    }

    // Reopen and verify
    {
        let catalog = CayenneCatalog::new(&test_db).expect("Failed to reopen");
        catalog.init().await.expect("Failed to init");

        let seq_1 = catalog
            .get_snapshot_sequence(&table_id, &snapshot_1)
            .await
            .expect("Failed to get seq");
        let seq_2 = catalog
            .get_snapshot_sequence(&table_id, &snapshot_2)
            .await
            .expect("Failed to get seq");
        let seq_3 = catalog
            .get_snapshot_sequence(&table_id, &snapshot_3)
            .await
            .expect("Failed to get seq");

        assert_eq!(seq_1, Some(10));
        assert_eq!(seq_2, Some(20));
        assert_eq!(seq_3, Some(30));

        let all_seqs = catalog
            .get_all_snapshot_sequences(&table_id)
            .await
            .expect("Failed to get all seqs");
        assert_eq!(all_seqs.len(), 3);

        catalog.shutdown().await.expect("Shutdown failed");
    }

    // Cleanup
    let _ = std::fs::remove_file(db_path);
    let _ = std::fs::remove_file(format!("{db_path}-shm"));
    let _ = std::fs::remove_file(format!("{db_path}-wal"));
}

#[tokio::test]
async fn test_create_table_falls_back_on_config_change() {
    let test_db = format!("sqlite://./.test_config_change_{}.db", uuid::Uuid::now_v7());
    let catalog = CayenneCatalog::new(&test_db).expect("Failed to create catalog");
    catalog.init().await.expect("Failed to initialize catalog");

    let schema = Arc::new(arrow_schema::Schema::new(vec![
        arrow_schema::Field::new("id", arrow_schema::DataType::Int64, false),
        arrow_schema::Field::new("name", arrow_schema::DataType::Utf8, true),
    ]));

    // Create initial table with no primary key
    let options = CreateTableOptions {
        table_name: "test_table".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec![],
        on_conflict: None,
        base_path: "/tmp/cayenne_config_test".to_string(),
        partition_column: None,
        vortex_config: crate::metadata::VortexConfig::default(),
    };
    let table_id_1 = catalog
        .create_table(options)
        .await
        .expect("Failed to create table");

    // Verify table was created
    let metadata = catalog
        .get_table("test_table")
        .await
        .expect("Failed to get table");
    assert!(metadata.primary_key.is_empty());
    assert_eq!(metadata.table_id, table_id_1);

    // Now try to create with a primary key change — should fall back to stored config
    // (a warning is logged, but create_table succeeds with the original table_id)
    let options_changed = CreateTableOptions {
        table_name: "test_table".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec!["id".to_string()],
        on_conflict: None,
        base_path: "/tmp/cayenne_config_test".to_string(),
        partition_column: None,
        vortex_config: crate::metadata::VortexConfig::default(),
    };
    let table_id_2 = catalog
        .create_table(options_changed)
        .await
        .expect("Config change should fall back gracefully");
    assert_eq!(
        table_id_1, table_id_2,
        "Should return the original table_id when config changes"
    );

    // Original table should still be intact with original config
    let metadata = catalog
        .get_table("test_table")
        .await
        .expect("Failed to get table");
    assert!(metadata.primary_key.is_empty());
    assert_eq!(metadata.table_id, table_id_1);

    // Recreate with the SAME config — should return the same table_id
    let options_same = CreateTableOptions {
        table_name: "test_table".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec![],
        on_conflict: None,
        base_path: "/tmp/cayenne_config_test".to_string(),
        partition_column: None,
        vortex_config: crate::metadata::VortexConfig::default(),
    };
    let table_id_2 = catalog
        .create_table(options_same)
        .await
        .expect("Failed to create table with same config");

    // Should reuse the existing table
    assert_eq!(table_id_1, table_id_2);

    // Cleanup
    let db_path = test_db.strip_prefix("sqlite://").unwrap_or(&test_db);
    let _ = std::fs::remove_file(db_path);
    let _ = std::fs::remove_file(format!("{db_path}-shm"));
    let _ = std::fs::remove_file(format!("{db_path}-wal"));
}

#[tokio::test]
async fn test_create_table_falls_back_on_sort_columns_change() {
    let test_db = format!("sqlite://./.test_sort_change_{}.db", uuid::Uuid::now_v7());
    let catalog = CayenneCatalog::new(&test_db).expect("Failed to create catalog");
    catalog.init().await.expect("Failed to initialize catalog");

    let schema = Arc::new(arrow_schema::Schema::new(vec![
        arrow_schema::Field::new("id", arrow_schema::DataType::Int64, false),
        arrow_schema::Field::new("ts", arrow_schema::DataType::Int64, false),
    ]));

    // Create table with no sort columns
    let options = CreateTableOptions {
        table_name: "sorted_table".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec![],
        on_conflict: None,
        base_path: "/tmp/cayenne_sort_test".to_string(),
        partition_column: None,
        vortex_config: crate::metadata::VortexConfig::default(),
    };
    let table_id_1 = catalog
        .create_table(options)
        .await
        .expect("Failed to create table");

    // Add sort columns — should fall back to stored config with a warning
    let vortex_config = crate::metadata::VortexConfig {
        sort_columns: vec!["ts".to_string()],
        ..Default::default()
    };
    let options_sorted = CreateTableOptions {
        table_name: "sorted_table".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec![],
        on_conflict: None,
        base_path: "/tmp/cayenne_sort_test".to_string(),
        partition_column: None,
        vortex_config,
    };
    let table_id_2 = catalog
        .create_table(options_sorted)
        .await
        .expect("Sort column change should fall back gracefully");
    assert_eq!(
        table_id_1, table_id_2,
        "Should return the original table_id when sort columns change"
    );

    // Cleanup
    let db_path = test_db.strip_prefix("sqlite://").unwrap_or(&test_db);
    let _ = std::fs::remove_file(db_path);
    let _ = std::fs::remove_file(format!("{db_path}-shm"));
    let _ = std::fs::remove_file(format!("{db_path}-wal"));
}

#[tokio::test]
async fn test_create_table_cache_change_does_not_recreate() {
    let test_db = format!("sqlite://./.test_cache_change_{}.db", uuid::Uuid::now_v7());
    let catalog = CayenneCatalog::new(&test_db).expect("Failed to create catalog");
    catalog.init().await.expect("Failed to initialize catalog");

    let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
        "id",
        arrow_schema::DataType::Int64,
        false,
    )]));

    // Create table with default cache settings
    let options = CreateTableOptions {
        table_name: "cache_table".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec![],
        on_conflict: None,
        base_path: "/tmp/cayenne_cache_test".to_string(),
        partition_column: None,
        vortex_config: crate::metadata::VortexConfig::default(),
    };
    let table_id_1 = catalog
        .create_table(options)
        .await
        .expect("Failed to create table");

    // Change only cache sizes (non-data-affecting) — should NOT trigger recreation
    let vortex_config = crate::metadata::VortexConfig {
        footer_cache_mb: Some(512),
        segment_cache_mb: 1024,
        upload_concurrency: 8,
        write_concurrency: Some(16),
        target_vortex_file_size_mb: 512,
        ..Default::default()
    };
    let options_cache_changed = CreateTableOptions {
        table_name: "cache_table".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec![],
        on_conflict: None,
        base_path: "/tmp/cayenne_cache_test".to_string(),
        partition_column: None,
        vortex_config,
    };
    let table_id_2 = catalog
        .create_table(options_cache_changed)
        .await
        .expect("Failed to create table with cache change");

    // Should reuse the same table (cache changes don't affect data)
    assert_eq!(table_id_1, table_id_2);

    // Cleanup
    let db_path = test_db.strip_prefix("sqlite://").unwrap_or(&test_db);
    let _ = std::fs::remove_file(db_path);
    let _ = std::fs::remove_file(format!("{db_path}-shm"));
    let _ = std::fs::remove_file(format!("{db_path}-wal"));
}

/// Test that `commit_compaction` clears delete files, insert records, and
/// snapshot sequences, and updates the active snapshot pointer.
#[tokio::test]
async fn test_commit_compaction_clears_metadata() {
    let test_db = format!(
        "sqlite://./.test_commit_compaction_{}.db",
        uuid::Uuid::now_v7()
    );
    let catalog = CayenneCatalog::new(&test_db).expect("Failed to create catalog");
    catalog.init().await.expect("Failed to initialize catalog");

    // Create a table.
    let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
        "id",
        arrow_schema::DataType::Int64,
        false,
    )]));
    let table_id = catalog
        .create_table(CreateTableOptions {
            table_name: "compaction_test".to_string(),
            schema,
            primary_key: vec![],
            on_conflict: None,
            base_path: "/tmp/cayenne_compaction_test".to_string(),
            partition_column: None,
            vortex_config: crate::metadata::VortexConfig::default(),
        })
        .await
        .expect("Failed to create table");

    // Add a delete file so there is something to clear.
    let delete_file = DeleteFile {
        delete_file_id: String::new(),
        table_id: table_id.clone(),
        source_data_file_path: None,
        path: "/tmp/delete.parquet".to_string(),
        path_is_relative: false,
        format: "parquet".to_string(),
        delete_count: 5,
        file_size_bytes: 256,
        deletion_type: DeletionType::default(),
        sequence_number: 1,
    };
    catalog
        .add_delete_file(delete_file)
        .await
        .expect("Failed to add delete file");

    // Verify delete file exists before compaction.
    let before = catalog
        .get_table_delete_files(&table_id)
        .await
        .expect("Failed to get delete files");
    assert_eq!(before.len(), 1, "Expected 1 delete file before compaction");

    // Commit compaction with a new snapshot ID.
    let new_snapshot_id = uuid::Uuid::now_v7().to_string();
    catalog
        .commit_compaction(&table_id, &new_snapshot_id)
        .await
        .expect("commit_compaction failed");

    // Verify delete files were cleared.
    let after = catalog
        .get_table_delete_files(&table_id)
        .await
        .expect("Failed to get delete files after compaction");
    assert!(
        after.is_empty(),
        "Delete files should be cleared after compaction"
    );

    // Verify the snapshot pointer was updated.
    let table = catalog
        .get_table("compaction_test")
        .await
        .expect("Failed to get table after compaction");
    assert_eq!(
        table.current_snapshot_id, new_snapshot_id,
        "Snapshot pointer should be updated after compaction"
    );

    // Cleanup.
    let db_path = test_db.strip_prefix("sqlite://").unwrap_or(&test_db);
    let _ = std::fs::remove_file(db_path);
    let _ = std::fs::remove_file(format!("{db_path}-shm"));
    let _ = std::fs::remove_file(format!("{db_path}-wal"));
}

/// Test that `commit_compaction` rejects non-UUID identifiers.
#[tokio::test]
async fn test_commit_compaction_rejects_invalid_uuid() {
    let test_db = format!(
        "sqlite://./.test_compaction_invalid_{}.db",
        uuid::Uuid::now_v7()
    );
    let catalog = CayenneCatalog::new(&test_db).expect("Failed to create catalog");
    catalog.init().await.expect("Failed to initialize catalog");

    let valid_uuid = uuid::Uuid::now_v7().to_string();

    // Invalid table_id should fail.
    let result = catalog
        .commit_compaction("'; DROP TABLE cayenne_table;--", &valid_uuid)
        .await;
    assert!(result.is_err(), "Should reject non-UUID table_id");

    // Invalid new_snapshot_id should fail.
    let result = catalog.commit_compaction(&valid_uuid, "not-a-uuid").await;
    assert!(result.is_err(), "Should reject non-UUID new_snapshot_id");

    // Cleanup.
    let db_path = test_db.strip_prefix("sqlite://").unwrap_or(&test_db);
    let _ = std::fs::remove_file(db_path);
    let _ = std::fs::remove_file(format!("{db_path}-shm"));
    let _ = std::fs::remove_file(format!("{db_path}-wal"));
}

/// Helper used by the `commit_compaction_in_txn` tests: create a table and
/// attach a delete file to it so the `in_txn` variant has metadata to clear
/// and a snapshot pointer to advance.
async fn setup_table_with_delete_file(
    catalog: &CayenneCatalog,
    table_name: &str,
    base_path: &str,
) -> String {
    let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
        "id",
        arrow_schema::DataType::Int64,
        false,
    )]));
    let table_id = catalog
        .create_table(CreateTableOptions {
            table_name: table_name.to_string(),
            schema,
            primary_key: vec![],
            on_conflict: None,
            base_path: base_path.to_string(),
            partition_column: None,
            vortex_config: crate::metadata::VortexConfig::default(),
        })
        .await
        .expect("Failed to create table");

    let delete_file = DeleteFile {
        delete_file_id: String::new(),
        table_id: table_id.clone(),
        source_data_file_path: None,
        path: format!("/tmp/delete_{table_name}.parquet"),
        path_is_relative: false,
        format: "parquet".to_string(),
        delete_count: 5,
        file_size_bytes: 256,
        deletion_type: DeletionType::default(),
        sequence_number: 1,
    };
    catalog
        .add_delete_file(delete_file)
        .await
        .expect("Failed to add delete file");

    table_id
}

#[tokio::test]
async fn test_clear_inlined_data_and_deletes_clears_both_tables() {
    let test_db = format!(
        "sqlite://./.test_clear_inline_metadata_{}.db",
        uuid::Uuid::now_v7()
    );
    let catalog = CayenneCatalog::new(&test_db).expect("Failed to create catalog");
    catalog.init().await.expect("Failed to initialize catalog");

    let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
        "id",
        arrow_schema::DataType::Int64,
        false,
    )]));
    let table_id = catalog
        .create_table(CreateTableOptions {
            table_name: "clear_inline_metadata".to_string(),
            schema,
            primary_key: vec![],
            on_conflict: None,
            base_path: "/tmp/clear_inline_metadata".to_string(),
            partition_column: None,
            vortex_config: crate::metadata::VortexConfig::default(),
        })
        .await
        .expect("Failed to create table");

    catalog
        .add_inlined_data(InlinedData {
            inlined_id: String::new(),
            table_id: table_id.clone(),
            partition_key: None,
            data_ipc: vec![1, 2, 3],
            record_count: 3,
            sequence_number: 1,
            created_at: String::new(),
        })
        .await
        .expect("Failed to add inlined data");
    catalog
        .add_inlined_delete(InlinedDelete {
            inlined_id: String::new(),
            table_id: table_id.clone(),
            delete_ipc: vec![4, 5, 6],
            delete_count: 2,
            sequence_number: 2,
            created_at: String::new(),
            published: false,
        })
        .await
        .expect("Failed to add inlined delete");

    assert_eq!(
        catalog
            .get_inlined_data_count(&table_id)
            .await
            .expect("Failed to get inlined data count"),
        3
    );
    assert_eq!(
        catalog
            .get_inlined_deletes(&table_id)
            .await
            .expect("Failed to get inlined deletes")
            .len(),
        1
    );

    catalog
        .clear_inlined_data_and_deletes(&table_id)
        .await
        .expect("Failed to clear inline metadata");

    assert_eq!(
        catalog
            .get_inlined_data_count(&table_id)
            .await
            .expect("Failed to get inlined data count after clear"),
        0
    );
    assert!(
        catalog
            .get_inlined_deletes(&table_id)
            .await
            .expect("Failed to get inlined deletes after clear")
            .is_empty()
    );

    let db_path = test_db.strip_prefix("sqlite://").unwrap_or(&test_db);
    let _ = std::fs::remove_file(db_path);
    let _ = std::fs::remove_file(format!("{db_path}-shm"));
    let _ = std::fs::remove_file(format!("{db_path}-wal"));
}

/// Issue #10125 — `commit_compaction_in_txn` applied to a single partition
/// inside an explicit transaction is observably equivalent to the legacy
/// `commit_compaction`: snapshot pointer advances, delete files cleared.
#[tokio::test]
async fn test_commit_compaction_in_txn_single_partition_parity() {
    let test_db = format!("sqlite://./.test_in_txn_parity_{}.db", uuid::Uuid::now_v7());
    let catalog = CayenneCatalog::new(&test_db).expect("Failed to create catalog");
    catalog.init().await.expect("Failed to initialize catalog");

    let table_id =
        setup_table_with_delete_file(&catalog, "in_txn_parity", "/tmp/in_txn_parity").await;

    // Sanity: delete file exists before the in_txn call.
    let before = catalog
        .get_table_delete_files(&table_id)
        .await
        .expect("Failed to get delete files");
    assert_eq!(before.len(), 1, "Expected 1 delete file before commit");

    let new_snapshot_id = uuid::Uuid::now_v7().to_string();

    // Caller-owned transaction: open, apply in_txn variant, commit.
    let mut tx = catalog
        .begin_transaction()
        .await
        .expect("Failed to begin transaction");
    catalog
        .commit_compaction_in_txn(&mut *tx, &table_id, &new_snapshot_id)
        .await
        .expect("commit_compaction_in_txn failed");
    tx.commit()
        .await
        .expect("Failed to commit caller transaction");

    // Delete files cleared.
    let after = catalog
        .get_table_delete_files(&table_id)
        .await
        .expect("Failed to get delete files after commit");
    assert!(
        after.is_empty(),
        "Delete files should be cleared after commit_compaction_in_txn"
    );

    // Snapshot pointer advanced.
    let table = catalog
        .get_table("in_txn_parity")
        .await
        .expect("Failed to get table after commit");
    assert_eq!(table.current_snapshot_id, new_snapshot_id);

    // Cleanup.
    let db_path = test_db.strip_prefix("sqlite://").unwrap_or(&test_db);
    let _ = std::fs::remove_file(db_path);
    let _ = std::fs::remove_file(format!("{db_path}-shm"));
    let _ = std::fs::remove_file(format!("{db_path}-wal"));
}

/// Issue #10125 — two `commit_compaction_in_txn` calls inside one
/// transaction commit atomically: after `tx.commit()`, both partitions'
/// pointers have advanced together.
#[tokio::test]
async fn test_commit_compaction_in_txn_cross_partition_atomicity() {
    let test_db = format!(
        "sqlite://./.test_in_txn_cross_atomic_{}.db",
        uuid::Uuid::now_v7()
    );
    let catalog = CayenneCatalog::new(&test_db).expect("Failed to create catalog");
    catalog.init().await.expect("Failed to initialize catalog");

    // Two "partitions": independent tables, treated as a single atomic
    // commit unit by the (future) cross-partition coordinator.
    let table_a = setup_table_with_delete_file(&catalog, "partition_a", "/tmp/p_a").await;
    let table_b = setup_table_with_delete_file(&catalog, "partition_b", "/tmp/p_b").await;

    let snap_a = uuid::Uuid::now_v7().to_string();
    let snap_b = uuid::Uuid::now_v7().to_string();

    let mut tx = catalog
        .begin_transaction()
        .await
        .expect("Failed to begin transaction");
    catalog
        .commit_compaction_in_txn(&mut *tx, &table_a, &snap_a)
        .await
        .expect("partition A in_txn failed");
    catalog
        .commit_compaction_in_txn(&mut *tx, &table_b, &snap_b)
        .await
        .expect("partition B in_txn failed");
    tx.commit().await.expect("Failed to commit transaction");

    // Both partitions advanced after the single tx.commit().
    let a = catalog.get_table("partition_a").await.expect("get a");
    let b = catalog.get_table("partition_b").await.expect("get b");
    assert_eq!(a.current_snapshot_id, snap_a);
    assert_eq!(b.current_snapshot_id, snap_b);

    // Cleanup.
    let db_path = test_db.strip_prefix("sqlite://").unwrap_or(&test_db);
    let _ = std::fs::remove_file(db_path);
    let _ = std::fs::remove_file(format!("{db_path}-shm"));
    let _ = std::fs::remove_file(format!("{db_path}-wal"));
}

/// Issue #10125 — dropping the transaction without committing rolls back
/// every `commit_compaction_in_txn` call applied inside it. The catalog
/// is left exactly as it was before the transaction opened.
#[tokio::test]
async fn test_commit_compaction_in_txn_rolls_back_on_drop() {
    let test_db = format!(
        "sqlite://./.test_in_txn_rollback_{}.db",
        uuid::Uuid::now_v7()
    );
    let catalog = CayenneCatalog::new(&test_db).expect("Failed to create catalog");
    catalog.init().await.expect("Failed to initialize catalog");

    let table_id =
        setup_table_with_delete_file(&catalog, "in_txn_rollback", "/tmp/in_txn_rb").await;

    // Capture pre-commit state.
    let before = catalog.get_table("in_txn_rollback").await.expect("get");
    let original_snapshot_id = before.current_snapshot_id.clone();

    let attempted_snapshot_id = uuid::Uuid::now_v7().to_string();

    {
        let mut tx = catalog
            .begin_transaction()
            .await
            .expect("Failed to begin transaction");
        catalog
            .commit_compaction_in_txn(&mut *tx, &table_id, &attempted_snapshot_id)
            .await
            .expect("in_txn variant succeeded inside tx");
        // Drop tx without committing — auto-rollback.
    }

    // The pointer must NOT have advanced.
    let after = catalog.get_table("in_txn_rollback").await.expect("get");
    assert_eq!(
        after.current_snapshot_id, original_snapshot_id,
        "Dropping the transaction must roll back commit_compaction_in_txn"
    );

    // The delete file must STILL exist.
    let delete_files = catalog
        .get_table_delete_files(&table_id)
        .await
        .expect("get delete files");
    assert_eq!(
        delete_files.len(),
        1,
        "Delete files must still exist after a rolled-back commit_compaction_in_txn"
    );

    // Cleanup.
    let db_path = test_db.strip_prefix("sqlite://").unwrap_or(&test_db);
    let _ = std::fs::remove_file(db_path);
    let _ = std::fs::remove_file(format!("{db_path}-shm"));
    let _ = std::fs::remove_file(format!("{db_path}-wal"));
}

/// Issue #10125 — `commit_compaction_in_txn` rejects non-UUID identifiers
/// before touching the borrowed transaction. The error path leaves the
/// catalog and the transaction untouched.
#[tokio::test]
async fn test_commit_compaction_in_txn_rejects_invalid_uuid() {
    let test_db = format!(
        "sqlite://./.test_in_txn_invalid_uuid_{}.db",
        uuid::Uuid::now_v7()
    );
    let catalog = CayenneCatalog::new(&test_db).expect("Failed to create catalog");
    catalog.init().await.expect("Failed to initialize catalog");

    let valid_uuid = uuid::Uuid::now_v7().to_string();

    let mut tx = catalog
        .begin_transaction()
        .await
        .expect("Failed to begin transaction");

    // Invalid table_id should fail.
    let result = catalog
        .commit_compaction_in_txn(&mut *tx, "'; DROP TABLE cayenne_table;--", &valid_uuid)
        .await;
    assert!(result.is_err(), "Should reject non-UUID table_id");

    // Invalid new_snapshot_id should fail.
    let result = catalog
        .commit_compaction_in_txn(&mut *tx, &valid_uuid, "not-a-uuid")
        .await;
    assert!(result.is_err(), "Should reject non-UUID new_snapshot_id");

    // The borrowed transaction is still usable for a subsequent valid call
    // (we never rolled back; the error path is purely validation, no SQL
    // was sent).
    drop(tx);

    // Cleanup.
    let db_path = test_db.strip_prefix("sqlite://").unwrap_or(&test_db);
    let _ = std::fs::remove_file(db_path);
    let _ = std::fs::remove_file(format!("{db_path}-shm"));
    let _ = std::fs::remove_file(format!("{db_path}-wal"));
}

#[test]
fn test_sql_text_literal_escapes_single_quotes() {
    assert_eq!(sql_text_literal("abc'def"), "'abc''def'");
}

#[test]
fn test_delete_file_unique_constraint_violation_message_matches_expected_conflicts() {
    let messages = [
        "UNIQUE constraint failed: cayenne_delete_file.table_id, cayenne_delete_file.path",
        "constraint failed: idx_cayenne_delete_file_table_path",
    ];

    for message in messages {
        assert!(is_delete_file_unique_constraint_violation_message(message));
    }
}

#[test]
fn test_delete_file_unique_constraint_violation_message_rejects_unrelated_constraints() {
    let messages = [
        "FOREIGN KEY constraint failed",
        "UNIQUE constraint failed: cayenne_table.table_name",
    ];

    for message in messages {
        assert!(!is_delete_file_unique_constraint_violation_message(message));
    }
}

#[test]
fn test_partition_unique_constraint_violation_message_matches_expected_conflicts() {
    let message =
        "UNIQUE constraint failed: cayenne_partition.table_id, cayenne_partition.partition_key";
    assert!(is_partition_unique_constraint_violation_message(message));
}

#[test]
fn test_partition_unique_constraint_violation_message_rejects_unrelated_constraints() {
    let messages = [
        "FOREIGN KEY constraint failed",
        "UNIQUE constraint failed: cayenne_delete_file.table_id, cayenne_delete_file.path",
    ];

    for message in messages {
        assert!(!is_partition_unique_constraint_violation_message(message));
    }
}

/// Helper to create a [`TableMetadata`] for unit tests.
fn make_test_metadata(
    primary_key: Vec<String>,
    on_conflict: Option<datafusion_table_providers::util::on_conflict::OnConflict>,
    partition_column: Option<String>,
    path: &str,
    vortex_config: crate::metadata::VortexConfig,
    schema: arrow_schema::SchemaRef,
) -> TableMetadata {
    TableMetadata {
        table_id: "test-id".to_string(),
        table_name: "test_table".to_string(),
        path: path.to_string(),
        path_is_relative: false,
        schema,
        primary_key,
        on_conflict,
        current_snapshot_id: "snap-1".to_string(),
        partition_column,
        vortex_config,
        current_sequence_number: 0,
    }
}

#[test]
fn test_configuration_matches_identical() {
    let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
        "id",
        arrow_schema::DataType::Int64,
        false,
    )]));
    let stored = make_test_metadata(
        vec!["id".to_string()],
        None,
        None,
        "/tmp/test",
        crate::metadata::VortexConfig::default(),
        Arc::clone(&schema),
    );
    let options = CreateTableOptions {
        table_name: "test_table".to_string(),
        schema,
        primary_key: vec!["id".to_string()],
        on_conflict: None,
        base_path: "/tmp/test".to_string(),
        partition_column: None,
        vortex_config: crate::metadata::VortexConfig::default(),
    };
    assert!(
        configuration_matches(&stored, &options),
        "Identical configurations should match"
    );
}

#[test]
fn test_configuration_matches_primary_key_differs() {
    let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
        "id",
        arrow_schema::DataType::Int64,
        false,
    )]));
    let stored = make_test_metadata(
        vec![],
        None,
        None,
        "/tmp/test",
        crate::metadata::VortexConfig::default(),
        Arc::clone(&schema),
    );
    let options = CreateTableOptions {
        table_name: "test_table".to_string(),
        schema,
        primary_key: vec!["id".to_string()],
        on_conflict: None,
        base_path: "/tmp/test".to_string(),
        partition_column: None,
        vortex_config: crate::metadata::VortexConfig::default(),
    };
    assert!(
        !configuration_matches(&stored, &options),
        "Different primary_key should not match"
    );
}

#[test]
fn test_configuration_matches_on_conflict_differs() {
    use datafusion_table_providers::util::on_conflict::OnConflict;
    let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
        "id",
        arrow_schema::DataType::Int64,
        false,
    )]));
    let stored = make_test_metadata(
        vec!["id".to_string()],
        None,
        None,
        "/tmp/test",
        crate::metadata::VortexConfig::default(),
        Arc::clone(&schema),
    );
    let options = CreateTableOptions {
        table_name: "test_table".to_string(),
        schema,
        primary_key: vec!["id".to_string()],
        on_conflict: Some(OnConflict::DoNothingAll),
        base_path: "/tmp/test".to_string(),
        partition_column: None,
        vortex_config: crate::metadata::VortexConfig::default(),
    };
    assert!(
        !configuration_matches(&stored, &options),
        "Different on_conflict should not match"
    );
}

#[test]
fn test_configuration_matches_sort_columns_differ() {
    let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
        "id",
        arrow_schema::DataType::Int64,
        false,
    )]));
    let stored = make_test_metadata(
        vec![],
        None,
        None,
        "/tmp/test",
        crate::metadata::VortexConfig::default(),
        Arc::clone(&schema),
    );
    let changed_vortex = crate::metadata::VortexConfig {
        sort_columns: vec!["id".to_string()],
        ..Default::default()
    };
    let options = CreateTableOptions {
        table_name: "test_table".to_string(),
        schema,
        primary_key: vec![],
        on_conflict: None,
        base_path: "/tmp/test".to_string(),
        partition_column: None,
        vortex_config: changed_vortex,
    };
    assert!(
        !configuration_matches(&stored, &options),
        "Different sort_columns should not match"
    );
}

#[test]
fn test_configuration_matches_base_path_differs() {
    let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
        "id",
        arrow_schema::DataType::Int64,
        false,
    )]));
    let stored = make_test_metadata(
        vec![],
        None,
        None,
        "/tmp/old_path",
        crate::metadata::VortexConfig::default(),
        Arc::clone(&schema),
    );
    let options = CreateTableOptions {
        table_name: "test_table".to_string(),
        schema,
        primary_key: vec![],
        on_conflict: None,
        base_path: "/tmp/new_path".to_string(),
        partition_column: None,
        vortex_config: crate::metadata::VortexConfig::default(),
    };
    assert!(
        !configuration_matches(&stored, &options),
        "Different base_path should not match"
    );
}

#[test]
fn test_log_configuration_differences_primary_key_change() {
    let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
        "id",
        arrow_schema::DataType::Int64,
        false,
    )]));
    let stored = make_test_metadata(
        vec![],
        None,
        None,
        "/tmp/test",
        crate::metadata::VortexConfig::default(),
        Arc::clone(&schema),
    );
    let options = CreateTableOptions {
        table_name: "test_table".to_string(),
        schema,
        primary_key: vec!["id".to_string()],
        on_conflict: None,
        base_path: "/tmp/test".to_string(),
        partition_column: None,
        vortex_config: crate::metadata::VortexConfig::default(),
    };
    // Should not panic; exercises the logging path for primary_key change.
    log_configuration_differences("test_table", &stored, &options);
}

#[test]
fn test_log_configuration_differences_on_conflict_change() {
    use datafusion_table_providers::util::on_conflict::OnConflict;
    let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
        "id",
        arrow_schema::DataType::Int64,
        false,
    )]));
    let stored = make_test_metadata(
        vec!["id".to_string()],
        None,
        None,
        "/tmp/test",
        crate::metadata::VortexConfig::default(),
        Arc::clone(&schema),
    );
    let options = CreateTableOptions {
        table_name: "test_table".to_string(),
        schema,
        primary_key: vec!["id".to_string()],
        on_conflict: Some(OnConflict::DoNothingAll),
        base_path: "/tmp/test".to_string(),
        partition_column: None,
        vortex_config: crate::metadata::VortexConfig::default(),
    };
    // Should not panic; exercises the logging path for on_conflict change.
    log_configuration_differences("test_table", &stored, &options);
}

#[test]
fn test_log_configuration_differences_multiple_fields() {
    use datafusion_table_providers::util::on_conflict::OnConflict;
    let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
        "id",
        arrow_schema::DataType::Int64,
        false,
    )]));
    let stored = make_test_metadata(
        vec![],
        None,
        None,
        "/tmp/old",
        crate::metadata::VortexConfig::default(),
        Arc::clone(&schema),
    );
    let changed_vortex = crate::metadata::VortexConfig {
        sort_columns: vec!["id".to_string()],
        ..Default::default()
    };
    let options = CreateTableOptions {
        table_name: "test_table".to_string(),
        schema,
        primary_key: vec!["id".to_string()],
        on_conflict: Some(OnConflict::DoNothingAll),
        base_path: "/tmp/new".to_string(),
        partition_column: Some("region".to_string()),
        vortex_config: changed_vortex,
    };
    // Should not panic; exercises the logging path when many fields change at once.
    log_configuration_differences("test_table", &stored, &options);
}

#[tokio::test]
async fn test_create_table_on_conflict_change_falls_back() {
    use datafusion_table_providers::util::on_conflict::OnConflict;
    let test_db = format!(
        "sqlite://./.test_on_conflict_change_{}.db",
        uuid::Uuid::now_v7()
    );
    let catalog = CayenneCatalog::new(&test_db).expect("Failed to create catalog");
    catalog.init().await.expect("Failed to initialize catalog");

    let schema = Arc::new(arrow_schema::Schema::new(vec![
        arrow_schema::Field::new("id", arrow_schema::DataType::Int64, false),
        arrow_schema::Field::new("name", arrow_schema::DataType::Utf8, true),
    ]));

    // Create table without on_conflict
    let options = CreateTableOptions {
        table_name: "oc_table".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec!["id".to_string()],
        on_conflict: None,
        base_path: "/tmp/cayenne_oc_test".to_string(),
        partition_column: None,
        vortex_config: crate::metadata::VortexConfig::default(),
    };
    let table_id_1 = catalog
        .create_table(options)
        .await
        .expect("Failed to create table");

    // Now try to add on_conflict — should fall back gracefully
    let options_changed = CreateTableOptions {
        table_name: "oc_table".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec!["id".to_string()],
        on_conflict: Some(OnConflict::DoNothingAll),
        base_path: "/tmp/cayenne_oc_test".to_string(),
        partition_column: None,
        vortex_config: crate::metadata::VortexConfig::default(),
    };
    let table_id_2 = catalog
        .create_table(options_changed)
        .await
        .expect("on_conflict change should fall back gracefully");
    assert_eq!(
        table_id_1, table_id_2,
        "Should return the original table_id when on_conflict changes"
    );

    // Stored metadata should still have original config (no on_conflict)
    let metadata = catalog
        .get_table("oc_table")
        .await
        .expect("Failed to get table");
    assert!(
        metadata.on_conflict.is_none(),
        "Stored on_conflict should remain None"
    );

    // Cleanup
    let db_path = test_db.strip_prefix("sqlite://").unwrap_or(&test_db);
    let _ = std::fs::remove_file(db_path);
    let _ = std::fs::remove_file(format!("{db_path}-shm"));
    let _ = std::fs::remove_file(format!("{db_path}-wal"));
}

#[tokio::test]
async fn test_validate_existing_table_configuration_returns_error_on_mismatch() {
    let test_db = format!(
        "sqlite://./.test_validate_mismatch_{}.db",
        uuid::Uuid::now_v7()
    );
    let catalog = CayenneCatalog::new(&test_db).expect("Failed to create catalog");
    catalog.init().await.expect("Failed to initialize catalog");

    let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
        "id",
        arrow_schema::DataType::Int64,
        false,
    )]));

    // Create table with no primary key
    let options = CreateTableOptions {
        table_name: "validate_table".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec![],
        on_conflict: None,
        base_path: "/tmp/cayenne_validate_test".to_string(),
        partition_column: None,
        vortex_config: crate::metadata::VortexConfig::default(),
    };
    catalog
        .create_table(options)
        .await
        .expect("Failed to create table");

    // validate_existing_table_configuration should return ChangedConfiguration
    let changed_options = CreateTableOptions {
        table_name: "validate_table".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec!["id".to_string()],
        on_conflict: None,
        base_path: "/tmp/cayenne_validate_test".to_string(),
        partition_column: None,
        vortex_config: crate::metadata::VortexConfig::default(),
    };
    let result = catalog
        .validate_existing_table_configuration("validate_table", &changed_options)
        .await;
    assert!(
        matches!(&result, Err(CatalogError::ChangedConfiguration { .. })),
        "Expected ChangedConfiguration error from validate, got: {result:?}"
    );

    // validate_existing_table_configuration should return Ok when config matches
    let same_options = CreateTableOptions {
        table_name: "validate_table".to_string(),
        schema,
        primary_key: vec![],
        on_conflict: None,
        base_path: "/tmp/cayenne_validate_test".to_string(),
        partition_column: None,
        vortex_config: crate::metadata::VortexConfig::default(),
    };
    let result = catalog
        .validate_existing_table_configuration("validate_table", &same_options)
        .await;
    assert!(
        result.is_ok(),
        "Expected Ok when config matches, got: {result:?}"
    );

    // Cleanup
    let db_path = test_db.strip_prefix("sqlite://").unwrap_or(&test_db);
    let _ = std::fs::remove_file(db_path);
    let _ = std::fs::remove_file(format!("{db_path}-shm"));
    let _ = std::fs::remove_file(format!("{db_path}-wal"));
}

#[tokio::test]
async fn test_validate_existing_table_configuration_allows_configured_footer_cache_drift() {
    let test_db = format!(
        "sqlite://./.test_footer_cache_validate_{}.db",
        uuid::Uuid::now_v7()
    );
    let catalog = CayenneCatalog::new(&test_db).expect("Failed to create catalog");
    catalog.init().await.expect("Failed to initialize catalog");

    let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
        "id",
        arrow_schema::DataType::Int64,
        false,
    )]));

    let options = CreateTableOptions {
        table_name: "footer_cache_validate_table".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec![],
        on_conflict: None,
        base_path: "/tmp/cayenne_footer_cache_validate_test".to_string(),
        partition_column: None,
        vortex_config: crate::metadata::VortexConfig {
            footer_cache_mb: Some(128),
            ..Default::default()
        },
    };
    catalog
        .create_table(options)
        .await
        .expect("Failed to create table");

    let changed_options = CreateTableOptions {
        table_name: "footer_cache_validate_table".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec![],
        on_conflict: None,
        base_path: "/tmp/cayenne_footer_cache_validate_test".to_string(),
        partition_column: None,
        vortex_config: crate::metadata::VortexConfig {
            footer_cache_mb: Some(256),
            ..Default::default()
        },
    };
    let result = catalog
        .validate_existing_table_configuration("footer_cache_validate_table", &changed_options)
        .await;
    assert!(
        result.is_ok(),
        "Expected Ok for footer cache runtime tuning drift, got: {result:?}"
    );

    let db_path = test_db.strip_prefix("sqlite://").unwrap_or(&test_db);
    let _ = std::fs::remove_file(db_path);
    let _ = std::fs::remove_file(format!("{db_path}-shm"));
    let _ = std::fs::remove_file(format!("{db_path}-wal"));
}
