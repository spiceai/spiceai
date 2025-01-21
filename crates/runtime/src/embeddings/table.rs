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

use std::collections::{HashMap, HashSet};
use std::{any::Any, sync::Arc};

use arrow::datatypes::{DataType, Schema, SchemaRef};
use arrow_schema::Field;
use arrow_tools::schema;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::{project_schema, Constraints, Statistics};
use datafusion::error::Result as DataFusionResult;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::TableProviderFilterPushDown;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::{
    datasource::{TableProvider, TableType},
    logical_expr::Expr,
};
use itertools::Itertools;
use llms::{
    chunking::{Chunker, ChunkingConfig},
    embeddings::Error as EmbedError,
};
use snafu::prelude::*;

use tokio::sync::RwLock;

use crate::embeddings::common::base_col;
use crate::embeddings::execution_plan::EmbeddingTableExec;
use crate::model::EmbeddingModelStore;
use crate::{embedding_col, offset_col};

use super::common::{is_valid_embedding_type, is_valid_offset_type, vector_length};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Column '{column}' has an unsupported data type for embedding. Only string types are allowed.\nFor details, visit: https://spiceai.org/docs/components/embeddings",
    ))]
    InvalidColumnType { column: String, data_type: DataType },
}

/// An [`EmbeddingTable`] is a [`TableProvider`] where some columns are augmented with associated embedding columns
#[derive(Clone)]
pub struct EmbeddingTable {
    base_table: Arc<dyn TableProvider>,

    embedded_columns: HashMap<String, EmbeddingColumnConfig>,

    embedding_models: Arc<RwLock<EmbeddingModelStore>>,
}

impl std::fmt::Debug for EmbeddingTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EmbeddingTable")
            .field("base_table", &self.base_table)
            .field("embedded_columns", &self.embedded_columns)
            .finish_non_exhaustive()
    }
}

#[derive(Clone)]
pub struct EmbeddingColumnConfig {
    /// The name of the embedding model to use for this column.
    /// Can be used as a key into [`EmbeddingModelStore`] for [`EmbeddingTable`].
    pub model_name: String,

    /// Expected size of its embedding. precompute to avoid async lock waits from `embedding_models` data structure.
    pub vector_size: i32,

    /// If `true`, assume embedding column is in the base table and does not need to be generated at query time.
    pub in_base_table: bool,

    // If None, either no chunking is needed, or [`in_base_table`] is true.
    pub chunker: Option<Arc<dyn Chunker>>,
}

impl std::fmt::Debug for EmbeddingColumnConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EmbeddingColumnConfig")
            .field("model_name", &self.model_name)
            .field("vector_size", &self.vector_size)
            .field("in_base_table", &self.in_base_table)
            .finish_non_exhaustive()
    }
}

impl EmbeddingTable {
    /// When creating a new [`EmbeddingTable`], the provided columns (in `embedded_column_to_model`) must be checked to see if they are already in the base table.
    /// Constructing the [`EmbeddingColumnConfig`] for each column is different depending on whether the column is in the base table or not.
    pub async fn try_new(
        base_table: Arc<dyn TableProvider>,
        embedded_column_to_model: HashMap<String, String>,
        embedding_models: Arc<RwLock<EmbeddingModelStore>>,
        embed_chunker_config: HashMap<String, ChunkingConfig<'_>>,
    ) -> Result<Self, Error> {
        let base_schema = base_table.schema();
        let mut embedded_columns: HashMap<String, EmbeddingColumnConfig> = HashMap::new();

        for (column, model) in embedded_column_to_model {
            let chunking_config_opt = embed_chunker_config.get(&column);

            if Self::base_table_has_embedding_column(&base_schema, &column) {
                tracing::debug!(
                    "Column '{column}' has needed embeddings in base table. Will not augment."
                );

                if chunking_config_opt.is_some() {
                    tracing::warn!("Column '{}' is an embedding from the base table, but chunking config was provided. It will not be used. Chunking will be determined by base table config.", column);
                }

                let Some(vector_length) =
                    Self::embedding_size_from_base_table(&column, &base_schema)
                else {
                    tracing::warn!("Column '{}' has embeddings in base table, but the vector length could not be determined. Ignoring column.", column);
                    continue;
                };

                embedded_columns.insert(
                    column,
                    EmbeddingColumnConfig {
                        model_name: model,
                        vector_size: vector_length,
                        in_base_table: true,
                        chunker: None, // Don't need chunking since it is done in base table.
                    },
                );
            } else {
                tracing::debug!("Column '{column}' does not have needed embeddings in base table. Will augment with model {model}.");

                Self::verify_column_type_supported(&column, &base_schema)?;

                let Some(vector_length) =
                    Self::embedding_size_from_models(&model, &embedding_models).await
                else {
                    tracing::warn!("For column '{column}', cannot precompute vector length from model '{model}'. Ignoring column.");
                    continue;
                };

                let mut chunker = None;
                if let Some(chunking_config) = chunking_config_opt {
                    match Self::construct_chunker(&model, chunking_config, &embedding_models).await
                    {
                        Ok(c) => chunker = Some(c),
                        Err(e) => {
                            tracing::warn!("Column '{column}' expects to be chunked, but the model '{model}' does not support chunking. Ignoring chunking config. Error: {e}");
                        }
                    }
                }

                embedded_columns.insert(
                    column,
                    EmbeddingColumnConfig {
                        model_name: model,
                        vector_size: vector_length,
                        in_base_table: false,
                        chunker,
                    },
                );
            }
        }

        Ok(Self {
            base_table,
            embedded_columns,
            embedding_models,
        })
    }

    /// Check if the base table has a column that is augmented with an embedding.
    /// For a base table with column, c, we expect:
    ///  - `c` to be in the base schema.
    ///  - `c_embedding` to be in the base schema. It needs to have a type compatible with [`Self::embedding_fields`].
    ///  - If `c_embedding` has a doubly-nested list type, `c_offsets` should also be in the base schema. It should be a `List[FixedSizeList[Int32, 2]]`.
    fn base_table_has_embedding_column(base_schema: &SchemaRef, column: &str) -> bool {
        // Check if the base column exists
        if base_schema.column_with_name(column).is_none() {
            tracing::warn!(
                "Column '{column}' does not exist in the base table. Cannot use it create an embeddings"
            );
            return false;
        }

        // Check if the embedding column exists and has a valid data type
        let Some((_, embedding_field)) =
            base_schema.column_with_name(embedding_col!(column).as_str())
        else {
            return false;
        };

        if !is_valid_embedding_type(embedding_field.data_type()) {
            return false;
        }

        // If embedding is doubly nested, also check for the offsets column
        if let DataType::List(inner)
        | DataType::LargeList(inner)
        | DataType::FixedSizeList(inner, _) = embedding_field.data_type()
        {
            if let DataType::FixedSizeList(_, _) = inner.data_type() {
                let Some((_, offsets_field)) =
                    base_schema.column_with_name(offset_col!(column).as_str())
                else {
                    return false;
                };

                if !is_valid_offset_type(offsets_field.data_type()) {
                    return false;
                }
            }
        }
        true
    }

    /// Get the names of the embedding models used by this table across its columns.
    #[must_use]
    pub fn get_embedding_models_used(&self) -> Vec<String> {
        self.embedded_columns
            .values()
            .map(|cfg| cfg.model_name.clone())
            .collect()
    }

    /// Get the names of the embedding columns that must be augmented (i.e. not in the base table).
    ///
    /// These are the underlying columns, not the embedding columns (e.g. `content`, not `content_embedding` or `content_offset`).
    ///
    /// The columns are sorted alphabetically.
    #[must_use]
    fn get_additional_embedding_columns_sorted(&self) -> Vec<String> {
        self.embedded_columns
            .iter()
            .filter_map(|(c, cfg)| {
                if cfg.in_base_table {
                    None
                } else {
                    Some(c.clone())
                }
            })
            .sorted()
            .collect()
    }

    /// Get the names of the additional fields that should be added to the schema for the embedding.
    ///
    /// These are the embedding columns, not the underlying columns (e.g. `content_embedding` or `content_offset`, not `content`).
    ///
    /// The columns are sorted alphabetically.
    fn get_additional_embedding_field_names(&self) -> Vec<String> {
        self.get_additional_embedding_columns_sorted()
            .iter()
            .flat_map(|col| {
                let Some(cfg) = self.embedded_columns.get(col) else {
                    return vec![];
                };

                if cfg.chunker.is_some() {
                    vec![embedding_col!(col), offset_col!(col)]
                } else {
                    vec![embedding_col!(col)]
                }
            })
            .collect()
    }

    /// Checks if a column has an embedding column associated with it, and should be chunked.
    /// If the column is not in the table, returns false.
    #[must_use]
    pub fn is_chunked(&self, column: &str) -> bool {
        self.embedded_columns.get(column).is_some_and(|cfg| {
            if cfg.in_base_table {
                self.base_table
                    .schema()
                    .column_with_name(offset_col!(column).as_str())
                    .is_some()
            } else {
                // Cheaper to check then looking at schema (which is created dynamically).
                cfg.chunker.is_some()
            }
        })
    }

    /// Get the names of the columns that are augmented with embeddings.
    #[must_use]
    pub fn get_embedding_columns(&self) -> Vec<String> {
        self.embedded_columns.keys().cloned().collect()
    }

    // Get the schema of the base table.
    #[must_use]
    pub fn get_base_table_schema(&self) -> SchemaRef {
        self.base_table.schema()
    }

    /// Makes a [`Chunker`] from [`ChunkingConfig`] and a column's embedding model in [`EmbeddingModelStore`].
    async fn construct_chunker(
        model_name: &str,
        chunk_config: &ChunkingConfig<'_>,
        embedding_models: &Arc<RwLock<EmbeddingModelStore>>,
    ) -> Result<Arc<dyn Chunker>, EmbedError> {
        let embedding_models_guard = embedding_models.read().await;
        let Some(embed_model) = embedding_models_guard.get(model_name) else {
            // Don't need warn, as we should have already checked/logged this.
            return Err(EmbedError::ModelDoesNotExist {
                model_name: model_name.to_string(),
            });
        };
        embed_model.chunker(chunk_config)
    }

    fn embedding_size_from_base_table(column: &str, base_schema: &SchemaRef) -> Option<i32> {
        let (_, embedding_field) = base_schema.column_with_name(embedding_col!(column).as_str())?;
        vector_length(embedding_field.data_type())
    }

    fn verify_column_type_supported(column: &str, base_schema: &SchemaRef) -> Result<(), Error> {
        if let Some((_, field)) = base_schema.column_with_name(column) {
            let data_type = field.data_type();
            if !matches!(
                data_type,
                DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8
            ) {
                return InvalidColumnTypeSnafu {
                    column: column.to_string(),
                    data_type: data_type.clone(),
                }
                .fail();
            }
        }
        Ok(())
    }

    async fn embedding_size_from_models(
        model_name: &str,
        embedding_models: &Arc<RwLock<EmbeddingModelStore>>,
    ) -> Option<i32> {
        let embedding_models_guard = embedding_models.read().await;
        embedding_models_guard
            .get(model_name)
            .map(|model| model.size())
    }

    /// For a given projection on the entire [`Schema`], find which [`Self::embedded_columns`] need to be computed.
    /// If `projection.is_none()`, all embedding columns are in projection, and therefore needed.
    ///
    /// Any embedding column that is in the base table does not need to be computed.
    ///
    /// Any project index (in `projection`) that is greater than the number of columns in the base
    /// table is an embedding column. The relation of underlying column to embedding column is, for example, as follows:
    ///
    /// | projection idx | 0 | 1 | 2 | 3 | 4 | 5 |      6        |      7     |       8       |
    /// |  column name   | A | B | C | D | E | F | `B_embedding` | `B_offset` | `E_embedding` |
    ///
    ///     - 6 Base columns A, B, C, D, E, F
    ///     - 2 Embedding columns B_embedding, E_embedding
    ///     - 1 Offset column B_offset
    ///     - Any projection index >=6 is an embedding column.
    ///
    /// The order of the additionally-generated embedding columns in [`Self::Schema`] is alphabetical.
    fn columns_to_embed(&self, projection: Option<&Vec<usize>>) -> Vec<String> {
        // Order of embedding columns in [`Self::Schema`] is alphabetical.
        match projection {
            None => self.get_additional_embedding_columns_sorted(),
            Some(column_idx) => {
                let additional_fields = self.get_additional_embedding_field_names();
                let base_cols = self.base_table.schema().fields.len();

                column_idx
                    .iter()
                    .filter_map(|&c| {
                        if c >= base_cols {
                            additional_fields
                                .get(c - base_cols)
                                .and_then(|col| base_col(col))
                        } else {
                            None
                        }
                    })
                    .unique()
                    .collect()
            }
        }
    }

    /// For a given field in the base table, return the additional field(s) that should be added to the schema for the embedding.
    /// For fields that shouldn't be embedded, or embeddings already exist in the base table, an empty vector is returned.
    ///
    /// These fields should match produces in [`super::execution_plan::get_embedding_columns`].
    fn embedding_fields(&self, field: &Field) -> Vec<Arc<Field>> {
        // [`Field`] not an embedding column
        let Some(cfg) = self.embedded_columns.get(field.name()) else {
            return vec![];
        };

        // No new fields needed
        if cfg.in_base_table {
            return vec![];
        }

        if cfg.chunker.is_some() {
            vec![
                Arc::new(Field::new_list(
                    embedding_col!(field.name()),
                    Field::new_fixed_size_list(
                        "item",
                        Field::new("item", DataType::Float32, false),
                        cfg.vector_size,
                        false,
                    ),
                    false,
                )),
                Arc::new(Field::new_list(
                    offset_col!(field.name()),
                    Field::new_fixed_size_list(
                        "item",
                        Field::new("item", DataType::Int32, false),
                        2,
                        false,
                    ),
                    false,
                )),
            ]
        } else {
            vec![Arc::new(Field::new_fixed_size_list(
                embedding_col!(field.name()),
                Field::new("item", DataType::Float32, false),
                cfg.vector_size,
                true,
            ))]
        }
    }
}

#[async_trait]
impl TableProvider for EmbeddingTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn constraints(&self) -> Option<&Constraints> {
        self.base_table.constraints()
    }

    fn table_type(&self) -> TableType {
        self.base_table.table_type()
    }

    fn get_column_default(&self, column: &str) -> Option<&Expr> {
        self.base_table.get_column_default(column)
    }

    fn schema(&self) -> SchemaRef {
        let base_schema = self.base_table.schema();
        let mut base_fields: Vec<_> = (0..base_schema.fields.len())
            .filter_map(|i| base_schema.fields.get(i).cloned())
            .collect();

        let mut computed_columns_meta: HashMap<String, Vec<String>> = HashMap::new();

        // Important to be kept alphabetical for fast lookup in [`EmbeddingTable::columns_to_embed`]
        let mut embedding_fields: Vec<_> = self
            .get_additional_embedding_columns_sorted()
            .iter()
            .filter_map(|base_column_name| {
                base_schema
                    .column_with_name(base_column_name)
                    .map(|(_, field)| {
                        let embedding_fields = self.embedding_fields(field);
                        computed_columns_meta.insert(
                            base_column_name.clone(),
                            embedding_fields
                                .iter()
                                .map(|f| f.name().to_string())
                                .collect(),
                        );
                        embedding_fields
                    })
            })
            .flatten()
            .collect();

        base_fields.append(&mut embedding_fields);

        let mut schema = Schema::new(base_fields);

        schema::set_computed_columns_meta(&mut schema, &computed_columns_meta);

        Arc::new(schema)
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let columns_to_embed = self.columns_to_embed(projection);
        let num_base_cols = self.base_table.schema().fields.len();

        // No embedding work is needed.
        if columns_to_embed.is_empty() {
            tracing::trace!("For `EmbeddingTable`, no additional embedding columns to compute. Forwarding entirely to base table.");
            return self
                .base_table
                .scan(
                    state,
                    projection
                        .as_ref()
                        .map(|p| {
                            p.iter()
                                .filter(|&&idx| idx < num_base_cols)
                                .copied()
                                .collect()
                        })
                        .as_ref(),
                    filters,
                    limit,
                )
                .await;
        }
        tracing::trace!(
            "For `EmbeddingTable`, additional embedding columns to compute: {columns_to_embed:?}"
        );
        let schema = &self.schema();

        let scan_embed_columns: HashMap<String, EmbeddingColumnConfig> = self
            .embedded_columns
            .iter()
            .filter(|(c, _m)| columns_to_embed.contains(c))
            .map(|(c, m)| (c.clone(), m.clone()))
            .collect();

        // Need to ensure base table gets the underlying column for each embedding column specified (as well as everything in the original [`projection`]).
        let projection_for_base_table: Option<Vec<usize>> = match projection.cloned() {
            None => None,
            Some(mut proj) => {
                let mut base_cols = scan_embed_columns
                    .keys()
                    .filter_map(|c| schema.column_with_name(c).map(|(idx, _field)| idx))
                    .collect_vec();
                proj.append(&mut base_cols);
                Some(
                    proj.iter()
                        .unique()
                        .filter(|&&c| c < num_base_cols) // Don't include embedding columns for `base_table`
                        .copied()
                        .collect_vec(),
                )
            }
        };

        let projected_schema = project_schema(&self.schema(), projection)?;
        let base_plan = self
            .base_table
            .scan(state, projection_for_base_table.as_ref(), filters, limit)
            .await?;

        Ok(Arc::new(EmbeddingTableExec::new(
            &projected_schema,
            filters,
            limit,
            base_plan,
            scan_embed_columns,
            Arc::clone(&self.embedding_models),
        )) as Arc<dyn ExecutionPlan>)
    }

    /// Any filter in [`filters`] can still be exact
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        let base_field_names: HashSet<String> = self
            .base_table
            .schema()
            .fields
            .iter()
            .map(|f| f.name().to_string())
            .collect();

        let push_downs = filters
            .iter()
            .map(|&f| {
                // If all columns in the filter are in the base table, we can push down the filter
                // dependent on the [`EmbeddingTable::base_table`]'s [`supports_filters_pushdown`].
                let additional_fields_count = f
                    .column_refs()
                    .iter()
                    .filter(|c| !base_field_names.contains(c.name()))
                    .count();

                if additional_fields_count == 0 {
                    self.base_table.supports_filters_pushdown(&[f]).map(|v| {
                        v.first()
                            .cloned()
                            .unwrap_or(TableProviderFilterPushDown::Unsupported)
                    })
                } else {
                    Ok(TableProviderFilterPushDown::Unsupported)
                }
            })
            .collect::<DataFusionResult<Vec<_>>>()?;
        Ok(push_downs)
    }

    fn statistics(&self) -> Option<Statistics> {
        None
    }

    async fn insert_into(
        &self,
        state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        overwrite: InsertOp,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        self.base_table.insert_into(state, input, overwrite).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow_schema::FieldRef;
    use std::sync::Arc;

    fn field(name: &str, data_type: DataType) -> FieldRef {
        Arc::new(Field::new(name, data_type, false))
    }

    #[test]
    fn test_base_column_missing() {
        assert!(!EmbeddingTable::base_table_has_embedding_column(
            &Arc::new(Schema::empty()),
            "c"
        ));
    }

    #[test]
    fn test_embedding_column_missing() {
        assert!(!EmbeddingTable::base_table_has_embedding_column(
            &Arc::new(Schema::new(vec![field("c", DataType::Utf8)])),
            "c"
        ));
    }

    #[test]
    fn test_embedding_column_invalid_type() {
        assert!(!EmbeddingTable::base_table_has_embedding_column(
            &Arc::new(Schema::new(vec![
                field("c", DataType::Utf8),
                field("c_embedding", DataType::Int32),
            ])),
            "c"
        ));
    }

    #[test]
    fn test_single_nested_embedding() {
        assert!(EmbeddingTable::base_table_has_embedding_column(
            &Arc::new(Schema::new(vec![
                field("c", DataType::Utf8),
                field(
                    "c_embedding",
                    DataType::List(field("item", DataType::Float32)),
                ),
            ])),
            "c"
        ));
    }

    #[test]
    fn test_doubly_nested_embedding_without_offsets() {
        assert!(!EmbeddingTable::base_table_has_embedding_column(
            &Arc::new(Schema::new(vec![
                field("c", DataType::Utf8),
                field(
                    "c_embedding",
                    DataType::List(field(
                        "item",
                        DataType::FixedSizeList(field("item", DataType::Float32), 4),
                    )),
                ),
            ])),
            "c"
        ));
    }

    #[test]
    fn test_doubly_nested_embedding_with_offsets() {
        assert!(EmbeddingTable::base_table_has_embedding_column(
            &Arc::new(Schema::new(vec![
                field("c", DataType::Utf8),
                field(
                    "c_embedding",
                    DataType::List(field(
                        "item",
                        DataType::FixedSizeList(field("item", DataType::Float32), 4),
                    )),
                ),
                field(
                    "c_offset",
                    DataType::List(field(
                        "item",
                        DataType::FixedSizeList(field("item", DataType::Int32), 2)
                    ),),
                ),
            ])),
            "c"
        ));
    }

    #[test]
    fn test_doubly_nested_embedding_with_invalid_offsets() {
        assert!(!EmbeddingTable::base_table_has_embedding_column(
            &Arc::new(Schema::new(vec![
                field("c", DataType::Utf8),
                field(
                    "c_embedding",
                    DataType::List(field(
                        "item",
                        DataType::FixedSizeList(field("item", DataType::Float32), 4),
                    )),
                ),
                // Offsets have invalid type (Utf8 instead of Int32)
                field(
                    "c_offset",
                    DataType::List(field(
                        "item",
                        DataType::FixedSizeList(field("item", DataType::Utf8), 2),
                    )),
                ),
            ])),
            "c"
        ));
    }
}
