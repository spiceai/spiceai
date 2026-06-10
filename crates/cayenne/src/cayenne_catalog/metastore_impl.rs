//! Backend-dispatch layer for [`CayenneCatalog`]: the [`MetastoreImpl`] enum
//! statically dispatches every query/execute/transaction call to the
//! configured backend — [`SqliteMetastore`] (`sqlite://`) or, behind the
//! `turso` feature, `TursoMetastore` (`libsql://`). Also hosts the
//! delete-file row decoding shared by `mod.rs` and `metadata_catalog_impl`.

use super::{
    CatalogError, CatalogResult, ExecuteParams, MetastoreBackend, MetastoreGetValue, MetastoreRow,
    MetastoreValue, QueryParams, QueryRowParams, SqliteMetastore, TursoMetastore,
};

pub(super) struct ExistingDeleteFileRecord {
    pub(super) delete_file_id: String,
    pub(super) path_is_relative: bool,
    pub(super) format: String,
    pub(super) delete_count: i64,
    pub(super) file_size_bytes: i64,
    pub(super) source_data_file_path: Option<String>,
    pub(super) sequence_number: i64,
}

pub(super) fn metastore_value_at(
    values: &[MetastoreValue],
    index: usize,
) -> CatalogResult<&MetastoreValue> {
    values.get(index).ok_or_else(|| CatalogError::Database {
        message: format!("Expected metastore value at index {index}"),
    })
}

pub(super) fn existing_delete_file_record_from_values(
    values: &[MetastoreValue],
) -> CatalogResult<ExistingDeleteFileRecord> {
    Ok(ExistingDeleteFileRecord {
        delete_file_id: String::from_value(metastore_value_at(values, 0)?)?,
        path_is_relative: bool::from_value(metastore_value_at(values, 1)?)?,
        format: String::from_value(metastore_value_at(values, 2)?)?,
        delete_count: i64::from_value(metastore_value_at(values, 3)?)?,
        file_size_bytes: i64::from_value(metastore_value_at(values, 4)?)?,
        source_data_file_path: Option::<String>::from_value(metastore_value_at(values, 5)?)?,
        sequence_number: Option::<i64>::from_value(metastore_value_at(values, 6)?)?.unwrap_or(0),
    })
}

/// Metastore backend enum to support different implementations.
#[derive(Debug)]
pub(crate) enum MetastoreImpl {
    Sqlite(SqliteMetastore),
    #[cfg(feature = "turso")]
    Turso(TursoMetastore),
}

impl MetastoreImpl {
    /// Helper to query a single row from metastore, working with both `SQLite` and Turso
    pub(crate) async fn query_row_helper<F, T>(
        &self,
        params: QueryRowParams<'_>,
        f: F,
    ) -> CatalogResult<T>
    where
        F: FnOnce(&dyn MetastoreRow) -> CatalogResult<T> + Send + 'static,
        T: Send + 'static,
    {
        match self {
            MetastoreImpl::Sqlite(m) => m.query_row(params, f).await,
            #[cfg(feature = "turso")]
            MetastoreImpl::Turso(m) => m.query_row(params, f).await,
        }
    }

    /// Helper to execute a statement on metastore, working with both `SQLite` and Turso
    pub(crate) async fn execute_helper(&self, params: ExecuteParams<'_>) -> CatalogResult<()> {
        match self {
            MetastoreImpl::Sqlite(m) => m.execute(params).await,
            #[cfg(feature = "turso")]
            MetastoreImpl::Turso(m) => m.execute(params).await,
        }
    }

    /// Helper to execute a transactional batch on metastore, working with both `SQLite` and Turso
    pub(crate) async fn execute_transaction_batch_helper(&self, sql: &str) -> CatalogResult<()> {
        match self {
            MetastoreImpl::Sqlite(m) => m.execute_transaction_batch(sql).await,
            #[cfg(feature = "turso")]
            MetastoreImpl::Turso(m) => m.execute_transaction_batch(sql).await,
        }
    }

    /// Helper to query multiple rows from metastore, working with both `SQLite` and Turso
    pub(crate) async fn query_helper<F, T>(
        &self,
        params: QueryParams<'_>,
        f: F,
    ) -> CatalogResult<Vec<T>>
    where
        F: Fn(&dyn MetastoreRow) -> CatalogResult<T> + Send + 'static,
        T: Send + 'static,
    {
        match self {
            MetastoreImpl::Sqlite(m) => m.query(params, f).await,
            #[cfg(feature = "turso")]
            MetastoreImpl::Turso(m) => m.query(params, f).await,
        }
    }

    /// Shutdown the metastore, performing any necessary cleanup.
    pub(crate) async fn shutdown(&self) -> CatalogResult<()> {
        match self {
            MetastoreImpl::Sqlite(m) => m.shutdown().await,
            #[cfg(feature = "turso")]
            MetastoreImpl::Turso(m) => m.shutdown().await,
        }
    }

    /// Run a non-blocking WAL checkpoint off the hot path (cycle-5 TASK 2b).
    pub(crate) async fn checkpoint_wal(&self) -> CatalogResult<()> {
        match self {
            MetastoreImpl::Sqlite(m) => m.checkpoint_wal().await,
            #[cfg(feature = "turso")]
            MetastoreImpl::Turso(m) => m.checkpoint_wal().await,
        }
    }

    /// Begin a transaction on the underlying metastore.
    ///
    /// Each backend sends the appropriate BEGIN statement (`BEGIN IMMEDIATE`
    /// for `SQLite` — acquiring the reserved write lock up front so the busy
    /// timeout serializes contending writers — `BEGIN CONCURRENT` for Turso).
    /// The returned transaction object holds exclusive access to the
    /// connection until commit/rollback/drop (drop auto-rolls-back).
    pub(crate) async fn begin_transaction(
        &self,
    ) -> CatalogResult<Box<dyn crate::metastore::MetastoreTransaction>> {
        match self {
            MetastoreImpl::Sqlite(m) => m.begin_transaction().await,
            #[cfg(feature = "turso")]
            MetastoreImpl::Turso(m) => m.begin_transaction().await,
        }
    }
}
