/*
Copyright 2025 The Spice.ai OSS Authors

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

use datafusion::arrow::datatypes::SchemaRef;
use std::{
    path::Path,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use super::{DatasetCheckpoint, Error, Result};

// These functions will be used for Cayenne directory-based checkpoint operations
// when full Cayenne snapshot support is integrated.
#[expect(
    dead_code,
    reason = "Functions will be used when Cayenne snapshot support is fully integrated"
)]
impl DatasetCheckpoint {
    /// Helper function to recursively find the most recent file modification time
    fn visit_dirs(dir: &Path, latest: &mut Option<SystemTime>) -> std::io::Result<()> {
        if dir.is_dir() {
            for entry in std::fs::read_dir(dir)? {
                let entry = entry?;
                let path = entry.path();
                if path.is_dir() {
                    Self::visit_dirs(&path, latest)?;
                } else if let Ok(metadata) = std::fs::metadata(&path)
                    && let Ok(modified) = metadata.modified()
                    && (latest.is_none() || modified > latest.unwrap_or(UNIX_EPOCH))
                {
                    *latest = Some(modified);
                }
            }
        }
        Ok(())
    }

    /// Initialize Cayenne-specific checkpoint tracking in the catalog's metadata database.
    ///
    /// Cayenne stores its metadata in a `SQLite` or Turso database within the metadata directory.
    /// We leverage this existing database to track checkpoint timestamps and schemas.
    pub(super) fn init_cayenne(metadata_path: &Path, data_path: &Path) -> Result<()> {
        // For Cayenne, the metadata database is managed by the Cayenne catalog itself.
        // We just need to ensure the directories exist.
        if !metadata_path.exists() {
            std::fs::create_dir_all(metadata_path).map_err(Error::external)?;
        }
        if !data_path.exists() {
            std::fs::create_dir_all(data_path).map_err(Error::external)?;
        }
        Ok(())
    }

    /// Check if a checkpoint exists for this Cayenne dataset.
    ///
    /// For Cayenne, we consider a checkpoint to exist if the data directory
    /// contains any data files (i.e., is not empty).
    pub(super) fn exists_cayenne(data_path: &Path) -> Result<bool> {
        if !data_path.exists() {
            return Ok(false);
        }

        // Check if the data directory contains any files/subdirectories
        let has_content = std::fs::read_dir(data_path)
            .map_err(Error::external)?
            .next()
            .is_some();

        Ok(has_content)
    }

    /// Get the last checkpoint time for Cayenne.
    ///
    /// We use the most recent modification time of files in the data directory
    /// as a proxy for the last checkpoint time.
    pub(super) fn last_checkpoint_time_cayenne(data_path: &Path) -> Result<Option<SystemTime>> {
        if !data_path.exists() {
            return Ok(None);
        }

        let mut latest_time: Option<SystemTime> = None;

        Self::visit_dirs(data_path, &mut latest_time).map_err(Error::external)?;
        Ok(latest_time)
    }

    /// Perform a checkpoint for Cayenne.
    ///
    /// For Cayenne, checkpointing means ensuring all data is flushed to disk.
    /// The Cayenne catalog handles its own WAL checkpointing through its `shutdown()` method.
    /// We store the schema in the metadata database for snapshot validation.
    pub(super) fn checkpoint_cayenne(
        metadata_path: &Path,
        data_path: &Path,
        schema: &SchemaRef,
    ) -> Result<()> {
        // Ensure directories exist
        if !metadata_path.exists() {
            std::fs::create_dir_all(metadata_path).map_err(Error::external)?;
        }
        if !data_path.exists() {
            std::fs::create_dir_all(data_path).map_err(Error::external)?;
        }

        // Store schema in a simple JSON file in the metadata directory
        // This allows us to validate schema compatibility during snapshot restore
        let schema_file = metadata_path.join("schema.json");
        let schema_json = Self::serialize_schema(schema)?;
        std::fs::write(&schema_file, schema_json).map_err(Error::external)?;

        // Store checkpoint timestamp
        let timestamp_file = metadata_path.join("checkpoint_timestamp");
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_micros();
        std::fs::write(&timestamp_file, now.to_string()).map_err(Error::external)?;

        // Force WAL checkpoint for Cayenne's SQLite/Turso database will be handled
        // by the catalog's shutdown() method when snapshots are created

        Ok(())
    }

    /// Get the schema for this Cayenne dataset from the metadata directory.
    pub(super) fn get_schema_cayenne(metadata_path: &Path) -> Result<Option<SchemaRef>> {
        let schema_file = metadata_path.join("schema.json");
        if !schema_file.exists() {
            return Ok(None);
        }

        let schema_json = std::fs::read_to_string(&schema_file).map_err(Error::external)?;
        Ok(Some(Self::deserialize_schema(&schema_json)?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;
    use tempfile::TempDir;

    /// Helper to create temp directories for Cayenne tests.
    /// Returns (`metadata_dir`, `data_dir`) `TempDir` instances.
    fn create_test_dirs() -> (TempDir, TempDir) {
        let metadata_dir = TempDir::new().expect("Failed to create temp metadata dir");
        let data_dir = TempDir::new().expect("Failed to create temp data dir");
        (metadata_dir, data_dir)
    }

    #[test]
    fn test_cayenne_checkpoint_init() {
        let metadata_dir = TempDir::new().expect("Failed to create temp metadata dir");
        let data_dir = TempDir::new().expect("Failed to create temp data dir");

        let result = DatasetCheckpoint::init_cayenne(metadata_dir.path(), data_dir.path());
        result.expect("Cayenne dataset checkpoint should succeed");
        assert!(metadata_dir.path().exists());
        assert!(data_dir.path().exists());
    }

    #[test]
    fn test_cayenne_exists() {
        let (_metadata_dir, data_dir) = create_test_dirs();

        // Initially empty
        assert!(
            !DatasetCheckpoint::exists_cayenne(data_dir.path())
                .expect("Failed to check if checkpoint exists")
        );

        // Create a file in the data directory
        std::fs::write(data_dir.path().join("test.vortex"), b"test data")
            .expect("Failed to write test data");

        // Now it should exist
        assert!(
            DatasetCheckpoint::exists_cayenne(data_dir.path())
                .expect("Failed to check if checkpoint exists")
        );
    }

    #[test]
    fn test_cayenne_schema_roundtrip() {
        let (metadata_dir, data_dir) = create_test_dirs();

        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]);
        let schema_ref = Arc::new(schema.clone());

        // Save the schema via checkpoint
        DatasetCheckpoint::checkpoint_cayenne(metadata_dir.path(), data_dir.path(), &schema_ref)
            .expect("Failed to checkpoint");

        // Retrieve the schema
        let retrieved_schema = DatasetCheckpoint::get_schema_cayenne(metadata_dir.path())
            .expect("Failed to get schema")
            .expect("Schema should exist");

        assert_eq!(&schema, retrieved_schema.as_ref());
    }

    #[test]
    fn test_cayenne_last_checkpoint_time() {
        let (_metadata_dir, data_dir) = create_test_dirs();

        // Initially no checkpoint time
        assert!(
            DatasetCheckpoint::last_checkpoint_time_cayenne(data_dir.path())
                .expect("Failed to get last checkpoint time")
                .is_none()
        );

        // Create a file
        std::fs::write(data_dir.path().join("test.vortex"), b"test data")
            .expect("Failed to write test data");

        // Should now have a checkpoint time
        let checkpoint_time = DatasetCheckpoint::last_checkpoint_time_cayenne(data_dir.path())
            .expect("Failed to get last checkpoint time");
        assert!(checkpoint_time.is_some());
    }

    #[test]
    fn test_cayenne_init_creates_directories() {
        let temp = TempDir::new().expect("Failed to create temp dir");
        let metadata_path = temp.path().join("new_metadata");
        let data_path = temp.path().join("new_data");

        // Directories don't exist yet
        assert!(!metadata_path.exists());
        assert!(!data_path.exists());

        // Init should create them
        DatasetCheckpoint::init_cayenne(&metadata_path, &data_path)
            .expect("Failed to init cayenne");

        assert!(metadata_path.exists());
        assert!(data_path.exists());
    }

    #[test]
    fn test_cayenne_exists_nonexistent_path() {
        let temp = TempDir::new().expect("Failed to create temp dir");
        let nonexistent_path = temp.path().join("does_not_exist");

        // Should return false for non-existent path
        let result = DatasetCheckpoint::exists_cayenne(&nonexistent_path)
            .expect("Failed to check existence");
        assert!(!result);
    }

    #[test]
    fn test_cayenne_get_schema_no_file() {
        let temp = TempDir::new().expect("Failed to create temp dir");

        // No schema file exists
        let result =
            DatasetCheckpoint::get_schema_cayenne(temp.path()).expect("Failed to get schema");
        assert!(result.is_none());
    }

    #[test]
    fn test_cayenne_last_checkpoint_time_nonexistent_path() {
        let temp = TempDir::new().expect("Failed to create temp dir");
        let nonexistent_path = temp.path().join("does_not_exist");

        // Should return None for non-existent path
        let result = DatasetCheckpoint::last_checkpoint_time_cayenne(&nonexistent_path)
            .expect("Failed to get checkpoint time");
        assert!(result.is_none());
    }

    #[test]
    fn test_cayenne_checkpoint_creates_timestamp_file() {
        let (metadata_dir, data_dir) = create_test_dirs();

        let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
        let schema_ref = Arc::new(schema);

        DatasetCheckpoint::checkpoint_cayenne(metadata_dir.path(), data_dir.path(), &schema_ref)
            .expect("Failed to checkpoint");

        // Verify timestamp file was created
        let timestamp_file = metadata_dir.path().join("checkpoint_timestamp");
        assert!(timestamp_file.exists());

        // Verify timestamp is a valid number
        let timestamp_content =
            std::fs::read_to_string(&timestamp_file).expect("Failed to read timestamp file");
        let _timestamp: u128 = timestamp_content
            .parse()
            .expect("Timestamp should be a valid number");
    }

    #[test]
    fn test_cayenne_schema_complex_types() {
        let (metadata_dir, data_dir) = create_test_dirs();

        // Create a schema with various complex types
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("score", DataType::Float64, true),
            Field::new("is_active", DataType::Boolean, false),
            Field::new(
                "created_at",
                DataType::Timestamp(
                    datafusion::arrow::datatypes::TimeUnit::Microsecond,
                    Some("UTC".into()),
                ),
                true,
            ),
            Field::new("data", DataType::Binary, true),
            Field::new(
                "tags",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                true,
            ),
        ]);
        let schema_ref = Arc::new(schema.clone());

        // Save the schema
        DatasetCheckpoint::checkpoint_cayenne(metadata_dir.path(), data_dir.path(), &schema_ref)
            .expect("Failed to checkpoint");

        // Retrieve and verify
        let retrieved_schema = DatasetCheckpoint::get_schema_cayenne(metadata_dir.path())
            .expect("Failed to get schema")
            .expect("Schema should exist");

        assert_eq!(&schema, retrieved_schema.as_ref());
    }

    #[test]
    fn test_cayenne_checkpoint_nested_directories() {
        let (_metadata_dir, data_dir) = create_test_dirs();

        // Create nested directory structure
        let nested_dir = data_dir.path().join("subdir1/subdir2/subdir3");
        std::fs::create_dir_all(&nested_dir).expect("Failed to create nested dirs");
        std::fs::write(nested_dir.join("deep_file.vortex"), b"deep data")
            .expect("Failed to write deep file");

        // Should still detect as existing
        assert!(
            DatasetCheckpoint::exists_cayenne(data_dir.path())
                .expect("Failed to check if checkpoint exists")
        );

        // Last checkpoint time should find the nested file
        let checkpoint_time = DatasetCheckpoint::last_checkpoint_time_cayenne(data_dir.path())
            .expect("Failed to get last checkpoint time");
        assert!(checkpoint_time.is_some());
    }

    #[test]
    fn test_cayenne_last_checkpoint_time_finds_most_recent() {
        let (_metadata_dir, data_dir) = create_test_dirs();

        // Create multiple files at different times
        std::fs::write(data_dir.path().join("file1.vortex"), b"data1")
            .expect("Failed to write file1");
        std::thread::sleep(std::time::Duration::from_millis(50));
        std::fs::write(data_dir.path().join("file2.vortex"), b"data2")
            .expect("Failed to write file2");
        std::thread::sleep(std::time::Duration::from_millis(50));
        std::fs::write(data_dir.path().join("file3.vortex"), b"data3")
            .expect("Failed to write file3");

        let checkpoint_time = DatasetCheckpoint::last_checkpoint_time_cayenne(data_dir.path())
            .expect("Failed to get last checkpoint time")
            .expect("Should have checkpoint time");

        // Get the modification time of the most recent file
        let file3_modified = std::fs::metadata(data_dir.path().join("file3.vortex"))
            .expect("Failed to get metadata")
            .modified()
            .expect("Failed to get modified time");

        // The checkpoint time should be the modification time of file3
        assert_eq!(checkpoint_time, file3_modified);
    }
}
