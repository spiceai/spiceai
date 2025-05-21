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
#![allow(clippy::implicit_hasher)]
use std::{collections::HashMap, sync::Arc};

use app::App;
use arrow::array::RecordBatch;
use datafusion::common::Constraint;
use datafusion::error::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::{datasource::TableProvider, sql::TableReference};
use datafusion_federation::FederatedTableProviderAdaptor;
use snafu::ResultExt;
use tokio::sync::RwLock;
use tokio_stream::StreamExt;

use crate::accelerated_table::AcceleratedTable;
use crate::datafusion::{DataFusion, SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA};

use crate::embeddings::table::EmbeddingTable;

pub(super) async fn collect_batches(
    mut stream: SendableRecordBatchStream,
) -> std::result::Result<Vec<RecordBatch>, DataFusionError> {
    let mut batches = Vec::new();
    while let Some(batch) = stream.next().await {
        batches.push(batch?);
    }

    Ok(batches)
}

/// If a [`TableProvider`] is an [`EmbeddingTable`], return the [`EmbeddingTable`].
/// This includes if the [`TableProvider`] is an [`AcceleratedTable`] with a [`EmbeddingTable`] underneath.
pub(super) async fn get_embedding_table(
    tbl: &Arc<dyn TableProvider>,
) -> Option<Arc<EmbeddingTable>> {
    if let Some(embedding_table) = tbl.as_any().downcast_ref::<EmbeddingTable>() {
        return Some(Arc::new(embedding_table.clone()));
    }

    let tbl = if let Some(adaptor) = tbl.as_any().downcast_ref::<FederatedTableProviderAdaptor>() {
        adaptor.table_provider.clone()?
    } else {
        Arc::clone(tbl)
    };

    if let Some(accelerated_table) = tbl.as_any().downcast_ref::<AcceleratedTable>() {
        let federated_table = accelerated_table
            .get_federated_table()
            .table_provider()
            .await;
        if let Some(embedding_table) = federated_table.as_any().downcast_ref::<EmbeddingTable>() {
            return Some(Arc::new(embedding_table.clone()));
        }
    }
    None
}

/// Compute the primary keys for each table in the app. Primary Keys can be explicitly defined in the Spicepod.yaml
pub async fn parse_explicit_primary_keys(
    app: Arc<RwLock<Option<Arc<App>>>>,
) -> HashMap<TableReference, Vec<String>> {
    app.read().await.as_ref().map_or(HashMap::new(), |app| {
        app.datasets
            .iter()
            .filter_map(|d| {
                let primary_keys_from_embeddings: Option<Vec<String>> =
                    d.embeddings.iter().find_map(|e| e.primary_keys.clone());

                let primary_keys_from_columns: Option<Vec<String>> = d
                    .columns
                    .iter()
                    .find_map(|c| c.embeddings.iter().find_map(|e| e.row_ids.clone()));

                let primary_keys = match (primary_keys_from_columns, primary_keys_from_embeddings) {
                    (Some(pks), None) | (None, Some(pks)) => pks,
                    (Some(pks), Some(_)) => {
                        tracing::warn!("Dataset '{}' provided primary keys in both `.columns[].embeddings[].row_id` and `.embeddings[].primary_keys`. Using the former.", d.name);
                        pks
                    }
                    (None, None) => return None,
                };

                Some((
                    TableReference::parse_str(&d.name)
                        .resolve(SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA)
                        .into(),
                    primary_keys,
                ))
            })
            .collect::<HashMap<TableReference, Vec<_>>>()
    })
}

async fn get_primary_keys(
    df: &Arc<DataFusion>,
    table: &TableReference,
) -> super::Result<Vec<String>> {
    let tbl_ref = df
        .get_table(table)
        .await
        .ok_or_else(|| super::Error::DataSourcesNotFound {
            data_source: vec![table.clone()],
        })?;

    let constraint_idx = tbl_ref
        .constraints()
        .map(|c| c.iter())
        .unwrap_or_default()
        .find_map(|c| match c {
            Constraint::PrimaryKey(columns) => Some(columns),
            Constraint::Unique(_) => None,
        })
        .cloned()
        .unwrap_or(Vec::new());

    tbl_ref
        .schema()
        .project(&constraint_idx)
        .map(|schema_projection| {
            schema_projection
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect::<Vec<_>>()
        })
        .boxed()
        .map_err(|e| super::Error::DataFusionError { source: e })
}

/// For a set of tables, get their primary keys. Attempt to determine the primary key(s) of the
/// table from the [`TableProvider`] constraints, and if not provided, use the explicit primary
/// keys defined in the spicepod configuration.
pub async fn get_primary_keys_with_overrides(
    df: &Arc<DataFusion>,
    tables: &[TableReference],
    explicit_primary_keys: &HashMap<TableReference, Vec<String>>,
) -> super::Result<HashMap<TableReference, Vec<String>>> {
    let mut tbl_to_pks: HashMap<TableReference, Vec<String>> = HashMap::new();

    for tbl in tables {
        // `explicit_primary_keys` are [`ResolvedTableReference`], must resolve with spice defaults first.
        // Equivalent to using [`TableReference::resolve_eq`] on `explicit_primary_keys` keys.
        let resolved_tbl: TableReference = tbl
            .clone()
            .resolve(SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA)
            .into();
        let pks = get_primary_keys(df, &resolved_tbl).await?;
        if !pks.is_empty() {
            tbl_to_pks.insert(tbl.clone(), pks);
        } else if let Some(explicit_pks) = explicit_primary_keys.get(&resolved_tbl) {
            tbl_to_pks.insert(tbl.clone(), explicit_pks.clone());
        }
    }
    Ok(tbl_to_pks)
}

pub async fn user_tables_with_embeddings(
    df: &Arc<DataFusion>,
) -> super::Result<Vec<TableReference>> {
    let tables = df.get_user_table_names();
    let mut tables_with_embeddings = Vec::new();

    for t in tables {
        let table_provider = df
            .get_table(&t)
            .await
            // we should not fail here, as we are iterating over the tables that we know exist
            .ok_or_else(|| super::Error::DataSourceNotFound { table: t.clone() })?;
        if get_embedding_table(&table_provider).await.is_some() {
            tables_with_embeddings.push(t);
        }
    }
    Ok(tables_with_embeddings)
}
