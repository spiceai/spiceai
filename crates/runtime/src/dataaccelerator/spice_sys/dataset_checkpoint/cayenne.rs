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

    fn create_test_cayenne_checkpoint() -> (DatasetCheckpoint, TempDir, TempDir) {
        use super::super::super::AccelerationConnection;
        use runtime_acceleration::snapshot::SnapshotBehavior;

        let metadata_dir = TempDir::new().expect("Failed to create temp metadata dir");
        let data_dir = TempDir::new().expect("Failed to create temp data dir");

        // Create a mock Cayenne checkpoint (we don't actually need a real connection for these tests)
        let checkpoint = DatasetCheckpoint {
            dataset_name: "test_cayenne_dataset".to_string(),
            acceleration_connection: AccelerationConnection::Cayenne(
                metadata_dir.path().to_path_buf(),
                data_dir.path().to_path_buf(),
            ),
            snapshot_behavior: SnapshotBehavior::Disabled,
        };

        (checkpoint, metadata_dir, data_dir)
    }

    #[test]
    fn test_cayenne_checkpoint_init() {
        let metadata_dir = TempDir::new().expect("Failed to create temp metadata dir");
        let data_dir = TempDir::new().expect("Failed to create temp data dir");

        let result = DatasetCheckpoint::init_cayenne(metadata_dir.path(), data_dir.path());
        assert!(result.is_ok());
        assert!(metadata_dir.path().exists());
        assert!(data_dir.path().exists());
    }

    #[test]
    fn test_cayenne_exists() {
        let (_checkpoint, _metadata_dir, data_dir) = create_test_cayenne_checkpoint();

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
        let (_checkpoint, metadata_dir, data_dir) = create_test_cayenne_checkpoint();

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
        let (_checkpoint, _metadata_dir, data_dir) = create_test_cayenne_checkpoint();

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
}
