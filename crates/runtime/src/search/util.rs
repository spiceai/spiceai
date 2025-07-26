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

use std::collections::HashSet;
use std::{collections::HashMap, sync::Arc};

use app::App;
use datafusion::common::Column;
use datafusion::{datasource::TableProvider, sql::TableReference};
use datafusion_federation::FederatedTableProviderAdaptor;
use runtime_datafusion_index::IndexedTableProvider;
use search::generation::CandidateGeneration;
use search::generation::text_search::index::FullTextDatabaseIndex;
use search::generation::util::get_primary_keys;
use snafu::ResultExt;
use tokio::sync::RwLock;

use crate::accelerated_table::AcceleratedTable;
use crate::datafusion::{DataFusion, SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA};

use crate::embeddings::table::EmbeddingTable;
use crate::search::SearchGenerationSnafu;
use crate::search::full_text::as_candidate_generations;

use super::{Error, Result};

/// Attempt to return a concrete [`TableProvider`] type from a given [`impl TableProvider`]. This includes if the [`TableProvider`] is a base table for an [`AcceleratedTable`] or [`FederatedTableProviderAdaptor`] or other known [`TableProvider`] that wrap a table.
pub(crate) fn find_concrete_table_provider<T: TableProvider + Clone + 'static>(
    tbl: &Arc<dyn TableProvider>,
) -> Option<Arc<T>> {
    let mut current_tbl = Arc::clone(tbl);

    // For the many possible wrapping [`TableProvider`], attempt to find the concrete `impl TableProvider`.
    // Also avoids having to [`Box::pin`] for recursive `async fn`.
    loop {
        // Attempt to downcast the current table to the desired type.
        if let Some(found_table) = current_tbl.as_any().downcast_ref::<T>() {
            return Some(Arc::new(found_table.clone()));
        }

        // Handle specific table wrapping logic.
        if let Some(index_table) = current_tbl.as_any().downcast_ref::<IndexedTableProvider>() {
            current_tbl = index_table.get_underlying();
            continue;
        }

        if let Some(adaptor) = current_tbl
            .as_any()
            .downcast_ref::<FederatedTableProviderAdaptor>()
        {
            if let Some(adapted_tbl) = adaptor.table_provider.clone() {
                current_tbl = adapted_tbl;
                continue;
            }
        }

        if let Some(accelerated_table) = current_tbl.as_any().downcast_ref::<AcceleratedTable>() {
            current_tbl = accelerated_table
                .get_federated_table()
                .try_table_provider_sync()?;
            continue;
        }

        // Exit if no further wrapping is found.
        return None;
    }
}

/// Compute the primary keys for each table in the app. Primary Keys can be explicitly defined in the Spicepod.yaml
pub async fn parse_explicit_primary_keys(
    app: Arc<RwLock<Option<Arc<App>>>>,
) -> HashMap<TableReference, Vec<String>> {
    app.read().await.as_ref().map_or(HashMap::new(), |app| {
        app.datasets
            .iter()
            .filter_map(|d| {
                d.primary_key_override().map(|pks| {
                    (
                        TableReference::parse_str(&d.name)
                            .resolve(SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA)
                            .into(),
                        pks,
                    )
                })
            })
            .collect::<HashMap<TableReference, Vec<_>>>()
    })
}

pub(crate) async fn get_primary_keys_from_table(
    df: &Arc<DataFusion>,
    table: &TableReference,
) -> Result<Vec<String>> {
    let tbl_ref = df
        .get_table(table)
        .await
        .ok_or_else(|| Error::DataSourcesNotFound {
            data_source: vec![table.clone()],
        })?;

    get_primary_keys(&tbl_ref)
        .await
        .boxed()
        .map_err(|e| Error::DataFusionError { source: e })
}

/// For a set of tables, get their primary keys. Attempt to determine the primary key(s) of the
/// table from the [`TableProvider`] constraints, and if not provided, use the explicit primary
/// keys defined in the spicepod configuration.
pub async fn get_primary_keys_with_overrides(
    df: &Arc<DataFusion>,
    tables: &[TableReference],
    explicit_primary_keys: &HashMap<TableReference, Vec<String>>,
) -> Result<HashMap<TableReference, Vec<String>>> {
    let mut tbl_to_pks: HashMap<TableReference, Vec<String>> = HashMap::new();

    for tbl in tables {
        // `explicit_primary_keys` are [`ResolvedTableReference`], must resolve with spice defaults first.
        // Equivalent to using [`TableReference::resolve_eq`] on `explicit_primary_keys` keys.
        let resolved_tbl: TableReference = tbl
            .clone()
            .resolve(SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA)
            .into();
        let pks = get_primary_keys_from_table(df, &resolved_tbl).await?;
        if !pks.is_empty() {
            tbl_to_pks.insert(tbl.clone(), pks);
        } else if let Some(explicit_pks) = explicit_primary_keys.get(&resolved_tbl) {
            tbl_to_pks.insert(tbl.clone(), explicit_pks.clone());
        }
    }
    Ok(tbl_to_pks)
}

pub async fn user_tables_with_embeddings(df: &Arc<DataFusion>) -> Result<Vec<TableReference>> {
    let mut tables_with_embeddings = Vec::new();

    for t in df.get_user_table_names() {
        if embedding_columns_from_table(df, &t)
            .await
            .is_some_and(|cols| !cols.is_empty())
        {
            tables_with_embeddings.push(t);
        }
    }
    Ok(tables_with_embeddings)
}

/// Returns the column names of a [`TableReference`] that have associated embedding column(s)
///
/// This includes per-row embeddings and chunked embeddings.
pub async fn embedding_columns_from_table(
    df: &Arc<DataFusion>,
    tbl: &TableReference,
) -> Option<Vec<String>> {
    let table_provider = df.get_table(tbl).await?;

    let mut embedding_columns: HashSet<String> = HashSet::default();

    // embedding columns from [`EmbeddingTable`].
    if let Some(embedding_table) = find_concrete_table_provider::<EmbeddingTable>(&table_provider) {
        for c in embedding_table.get_embedding_columns() {
            embedding_columns.insert(c);
        }
    }

    // embedding columns from [`IndexedTableProvider`].
    #[cfg(feature = "s3_vectors")]
    {
        if let Some(indexed) = find_concrete_table_provider::<IndexedTableProvider>(&table_provider)
        {
            use crate::embeddings::index::{S3Vector, VectorIndex};
            if let Some(s3_vector) = indexed.get_index::<S3Vector>() {
                embedding_columns.insert(s3_vector.embedded_column());
            }
        }
    }

    Some(embedding_columns.into_iter().collect())
}

/// Returns a full text search [`CandidateGeneration`] if the [`TableReference`] has the appropriate index(es) defined in [`DataFusion`].
///
/// Returns:
///   None:
///     - `tbl` does not exist
///     - `tbl` does not have relevant full text search support.
pub async fn full_text_search_candidates(
    df: &Arc<DataFusion>,
    tbl: &TableReference,
) -> Option<Result<Vec<Arc<dyn CandidateGeneration>>>> {
    let table_provider = df.get_table(tbl).await?;

    // If the table exists, but does not have full text search support, return no candidates.
    let Some(indexed_table) = find_concrete_table_provider::<IndexedTableProvider>(&table_provider)
    else {
        return Some(Ok(vec![]));
    };

    let Some(fts) = indexed_table.get_index::<FullTextDatabaseIndex>() else {
        return Some(Ok(vec![]));
    };

    Some(
        as_candidate_generations(&fts.with_new_base(table_provider))
            .await
            .context(SearchGenerationSnafu),
    )
}

/// There is no [`Expr`] that can parse a fully qualified table name. For UDTFs that require
/// tables as an input [`Expr`], it will be parsed as a [`Column`]. This function converts a
///  [`Column`] to the [`TableReference`] intended.
#[must_use]
pub fn table_ref_from_column_expr(c: &Column) -> TableReference {
    let table: Arc<str> = c.name.clone().into();
    let schema: Option<&str> = c.relation.as_ref().map(TableReference::table);
    let catalog: Option<&str> = c.relation.as_ref().and_then(TableReference::schema);
    match (catalog, schema) {
        // Catalog without schema is impossible.
        (None | Some(_), None) => TableReference::Bare { table },
        (None, Some(s)) => TableReference::Partial {
            schema: s.into(),
            table,
        },
        (Some(c), Some(s)) => TableReference::Full {
            catalog: c.into(),
            schema: s.into(),
            table,
        },
    }
}

// Constructs the associated [`Column`] derived from [`table_ref_from_column_expr`].
#[must_use]
pub fn to_column_expr(tbl: &TableReference) -> Column {
    match tbl {
        TableReference::Bare { table } => Column::new_unqualified(table.to_string()),
        TableReference::Partial { schema, table } => Column::new(
            Some(TableReference::Bare {
                table: Arc::clone(schema),
            }),
            table.to_string(),
        ),
        TableReference::Full {
            catalog,
            schema,
            table,
        } => Column::new(
            Some(TableReference::Partial {
                schema: Arc::clone(catalog),
                table: Arc::clone(schema),
            }),
            table.to_string(),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::FullTextDatabaseIndex;
    use super::*;
    use arrow_schema::{DataType, Field, Schema};
    use data_components::arrow::write::MemTable;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_find_concrete_table_provider_direct_match() {
        let base: Arc<dyn TableProvider> = Arc::new(
            MemTable::try_new(Arc::new(Schema::empty()), vec![]).expect("failed to make table"),
        );

        assert!(find_concrete_table_provider::<EmbeddingTable>(&base).is_none());
    }

    #[tokio::test]
    async fn test_find_concrete_table_provider_wrapped_in_full_text() {
        let base_table: Arc<dyn TableProvider> = Arc::new(
            MemTable::try_new(
                Arc::new(Schema::new(vec![Field::new(
                    "search_field",
                    DataType::Utf8,
                    false,
                )])),
                vec![],
            )
            .expect("failed to make table"),
        );

        let index = Arc::new(
            FullTextDatabaseIndex::try_new(
                Arc::clone(&base_table),
                vec!["search_field".to_string()],
                vec![].into(),
            )
            .await
            .expect("cannot make full text table"),
        );

        let wrapped_table = Arc::new(IndexedTableProvider::new(base_table).add_index(index))
            as Arc<dyn TableProvider>;

        assert!(find_concrete_table_provider::<IndexedTableProvider>(&wrapped_table).is_some());

        assert!(find_concrete_table_provider::<EmbeddingTable>(&wrapped_table).is_none());
    }
}
