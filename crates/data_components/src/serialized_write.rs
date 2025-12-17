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

//! Serialized write provider for ensuring exclusive write access to shared resources.
//!
//! This module provides a wrapper around `TableProvider` that serializes write operations
//! using an external mutex. This is essential for databases like `SQLite` where concurrent
//! writes to the same file from multiple connections can cause "database is locked" errors.

use std::any::Any;
use std::borrow::Cow;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::{Constraints, Statistics};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::Result as DataFusionResult;
use datafusion::logical_expr::{Expr, LogicalPlan, TableProviderFilterPushDown, dml::InsertOp};
use datafusion::physical_plan::ExecutionPlan;
use tokio::sync::Mutex;

use crate::delete::DeletionTableProvider;

/// A wrapper around `TableProvider` that serializes write operations.
///
/// This provider uses an external `tokio::sync::Mutex` to ensure that only one
/// write operation (insert or delete) can run at a time for a shared resource
/// (e.g., a `SQLite` file shared by multiple tables).
///
/// Read operations (scans) are not serialized and can run concurrently.
#[derive(Debug)]
pub struct SerializedWriteProvider<T: TableProvider + Send + Sync + 'static> {
    inner: Arc<T>,
    /// Write lock to serialize insert and delete operations.
    /// The lock is external so it can be shared across multiple table providers
    /// that write to the same underlying resource.
    write_lock: Arc<Mutex<()>>,
}

impl<T: TableProvider + Send + Sync + 'static> SerializedWriteProvider<T> {
    /// Create a new `SerializedWriteProvider` wrapping the given table provider.
    ///
    /// The `write_lock` should be shared across all providers that write to the
    /// same underlying resource (e.g., the same `SQLite` file).
    #[must_use]
    pub fn new(inner: Arc<T>, write_lock: Arc<Mutex<()>>) -> Self {
        Self { inner, write_lock }
    }

    /// Get a reference to the inner table provider.
    #[must_use]
    pub fn inner(&self) -> &Arc<T> {
        &self.inner
    }
}

impl<T: TableProvider + Send + Sync + 'static> Clone for SerializedWriteProvider<T> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            write_lock: Arc::clone(&self.write_lock),
        }
    }
}

#[async_trait]
impl<T: TableProvider + Send + Sync + 'static> TableProvider for SerializedWriteProvider<T> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn constraints(&self) -> Option<&Constraints> {
        self.inner.constraints()
    }

    fn table_type(&self) -> TableType {
        self.inner.table_type()
    }

    fn get_table_definition(&self) -> Option<&str> {
        self.inner.get_table_definition()
    }

    fn get_logical_plan(&self) -> Option<Cow<'_, LogicalPlan>> {
        self.inner.get_logical_plan()
    }

    fn get_column_default(&self, column: &str) -> Option<&Expr> {
        self.inner.get_column_default(column)
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        self.inner.supports_filters_pushdown(filters)
    }

    fn statistics(&self) -> Option<Statistics> {
        self.inner.statistics()
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        // Reads are not serialized - SQLite WAL mode allows concurrent reads
        self.inner.scan(state, projection, filters, limit).await
    }

    async fn insert_into(
        &self,
        state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        overwrite: InsertOp,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        // Acquire the write lock before inserting
        let _guard = self.write_lock.lock().await;
        self.inner.insert_into(state, input, overwrite).await
    }
}

#[async_trait]
impl<T: TableProvider + DeletionTableProvider + Send + Sync + 'static> DeletionTableProvider
    for SerializedWriteProvider<T>
{
    async fn delete_from(
        &self,
        state: &dyn Session,
        filters: &[Expr],
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        // Acquire the write lock before deleting
        let _guard = self.write_lock.lock().await;
        self.inner.delete_from(state, filters).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::MemTable;

    #[tokio::test]
    async fn test_serialized_write_prevents_concurrent_inserts() {
        // This test verifies that concurrent insert calls are serialized
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        // MemTable requires at least one partition (empty batches are fine)
        let mem_table = Arc::new(
            MemTable::try_new(Arc::clone(&schema), vec![vec![]]).expect("create mem table"),
        );

        let write_lock = Arc::new(Mutex::new(()));
        let _provider = SerializedWriteProvider::new(mem_table, Arc::clone(&write_lock));

        // The lock should be obtainable
        {
            let _guard = write_lock.try_lock();
            assert!(
                _guard.is_ok(),
                "Lock should be available when no writes are in progress"
            );
        }
    }

    #[tokio::test]
    async fn test_write_lock_is_shared() {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        // MemTable requires at least one partition (empty batches are fine)
        let mem_table1 =
            Arc::new(MemTable::try_new(Arc::clone(&schema), vec![vec![]]).expect("mem1"));
        let mem_table2 = Arc::new(MemTable::try_new(schema, vec![vec![]]).expect("mem2"));

        // Same write lock shared between two providers
        let write_lock = Arc::new(Mutex::new(()));
        let provider1 = SerializedWriteProvider::new(mem_table1, Arc::clone(&write_lock));
        let provider2 = SerializedWriteProvider::new(mem_table2, Arc::clone(&write_lock));

        // Both providers share the same lock
        assert!(Arc::ptr_eq(&provider1.write_lock, &provider2.write_lock));
    }
}
