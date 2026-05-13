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
use chunking::{Chunker, ChunkingConfig};
use datafusion::catalog::Session;
use datafusion::common::{Constraints, Statistics, project_schema};
use datafusion::error::Result as DataFusionResult;
use datafusion::logical_expr::TableProviderFilterPushDown;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::{
    datasource::{TableProvider, TableType},
    logical_expr::Expr,
};
use itertools::Itertools;
use snafu::prelude::*;

use crate::embeddings::common::base_col;
use crate::embeddings::construct_chunker;
use crate::embeddings::execution_plan::EmbeddingTableExec;
use crate::model::EmbeddingModelStore;
use crate::{embedding_col, offset_col};
use spicepod::component::embeddings::{
    ColumnEmbeddingConfig, EmbeddingAggregation, MULTI_VECTOR_MAX_ELEMENTS_DEFAULT,
    MULTI_VECTOR_MAX_ELEMENTS_HARD_CAP,
};
use tokio::sync::RwLock;

use super::common::{is_valid_embedding_type, is_valid_offset_type, vector_length};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Column '{column}' has an unsupported data type for embedding. Supported types are string (`Utf8`, `Utf8View`, `LargeUtf8`) and list-of-string (`List<Utf8>`, `LargeList<Utf8>`). For details, visit: https://spiceai.org/docs/components/embeddings",
    ))]
    InvalidColumnType { column: String, data_type: DataType },

    #[snafu(display(
        "Column '{column}' is configured for multi-vector embedding (list-typed) but also has chunking enabled. Chunking only applies to scalar string columns. Remove the chunking configuration for multi-vector columns."
    ))]
    MultiVectorChunkingNotSupported { column: String },

    #[snafu(display(
        "Column '{column}' was configured with `aggregation` or `max_elements_per_row`, but its type '{data_type}' is not a list-typed column. These options only apply to multi-vector (list-typed) columns."
    ))]
    MultiVectorOptionsOnScalar { column: String, data_type: DataType },

    #[snafu(display(
        "Column '{column}': `max_elements_per_row` must be between 1 and {cap}, got {value}."
    ))]
    MaxElementsPerRowOutOfRange {
        column: String,
        value: usize,
        cap: usize,
    },

    #[snafu(display(
        "The dataset is configured with an embedding model '{model}' to embed column '{column}', but the model '{model}' is not defined in Spicepod (as an 'embeddings') or failed to load.\nFor details, visit: https://spiceai.org/docs/components/embeddings"
    ))]
    EmbeddingModelNotFound { column: String, model: String },

    #[snafu(display(
        "Embedding row_id column '{row_id_column}' for column '{column}' was not found in the dataset schema. Valid columns: {valid_columns}. Verify the row_id configuration and try again.\nFor details, visit: https://spiceai.org/docs/components/embeddings"
    ))]
    RowIdColumnNotFound {
        column: String,
        row_id_column: String,
        valid_columns: String,
    },

    #[snafu(display(
        "The dataset is configured with an embedding for column '{column}', but '{column}' is not present in the dataset schema. Verify the column configuration and try again.\nFor details, visit: https://spiceai.org/docs/components/embeddings"
    ))]
    EmbeddingColumnNotInSchema { column: String },
}

/// An [`EmbeddingTable`] is a [`TableProvider`] where some columns are augmented with associated embedding columns
#[derive(Clone)]
pub struct EmbeddingTable {
    pub base_table: Arc<dyn TableProvider>,

    pub embedded_columns: HashMap<String, EmbeddingColumnConfig>,

    pub embedding_models: Arc<RwLock<EmbeddingModelStore>>,
}

impl std::fmt::Debug for EmbeddingTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EmbeddingTable")
            .field("base_table", &self.base_table)
            .field("embedded_columns", &self.embedded_columns)
            .finish_non_exhaustive()
    }
}

/// Internal classifier for the source column's Arrow type.
#[derive(Clone, Debug, PartialEq, Eq)]
enum SourceShape {
    /// `Utf8` / `Utf8View` / `LargeUtf8` — carries the concrete type for error messages.
    Scalar(DataType),
    /// `List<Utf8>` / `LargeList<Utf8>` (and `Utf8View`/`LargeUtf8` element variants).
    ListOfString,
}

/// Compatibility matrix for the multi-vector output type
/// (`List<FixedSizeList<F32, D>>`) across the accelerator engines Spice
/// supports. This shape is identical to what the chunked-scalar path has
/// produced since its introduction; multi-vector columns inherit that
/// behavior.
///
/// | Accelerator | Storage                                     | Notes                        |
/// |-------------|---------------------------------------------|------------------------------|
/// | Arrow       | Native Arrow in-memory                      | Transparent.                 |
/// | Cayenne     | Native Arrow persistence                    | Transparent.                 |
/// | `DuckDB`      | Native `FLOAT[D][]`                         | Transparent.                 |
/// | `SQLite`      | JSON-serialized `TEXT` (via table-providers)| Functional; JSON overhead.   |
/// | Turso       | JSON-serialized `TEXT`                      | See `turso.rs:581-583`.      |
/// | `PostgreSQL`  | Not yet supported                           | Out of scope this milestone. |
///
/// `SQLite` / Turso JSON serialization is lossy in type fidelity (everything
/// round-trips as TEXT) but functionally correct. A proper side-table
/// strategy (`<base>__<col>_mv(pk, elem_idx, vector)`) is a future
/// optimization for those accelerators; the current behavior is the same
/// the chunked-scalar path has shipped with.
///
/// Shape of the source column being embedded.
///
/// `Scalar` — the source column is a single string per row (`Utf8` /
/// `Utf8View` / `LargeUtf8`). One embedding vector is produced per row,
/// optionally doubly-nested if chunking is enabled.
///
/// `ListMulti` — the source column is a list of strings per row
/// (`List<Utf8>` / `LargeList<Utf8>` and their `Utf8View` / `LargeUtf8`
/// variants). One embedding vector is produced per list element; at
/// query time per-element similarities are aggregated into a single
/// per-row score via `aggregation`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EmbeddingInputMode {
    Scalar,
    ListMulti {
        aggregation: EmbeddingAggregation,
        max_elements_per_row: usize,
    },
}

impl EmbeddingInputMode {
    #[must_use]
    pub fn is_list_multi(&self) -> bool {
        matches!(self, Self::ListMulti { .. })
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

    /// Shape of the source column. Determines the output Arrow type and
    /// whether the search path uses MaxSim-over-elements.
    pub input_mode: EmbeddingInputMode,
}

impl std::fmt::Debug for EmbeddingColumnConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EmbeddingColumnConfig")
            .field("model_name", &self.model_name)
            .field("vector_size", &self.vector_size)
            .field("in_base_table", &self.in_base_table)
            .field("input_mode", &self.input_mode)
            .finish_non_exhaustive()
    }
}

impl EmbeddingTable {
    pub async fn from_spicepod_columns(
        base_table: Arc<dyn TableProvider>,
        embeddings: Vec<ColumnEmbeddingConfig>,
        embedding_models: &Arc<RwLock<EmbeddingModelStore>>,
        file_format: Option<&str>,
    ) -> Result<Arc<dyn TableProvider>, Error> {
        if embeddings.is_empty() {
            return Ok(base_table);
        }

        let embed_columns: HashMap<String, ColumnEmbeddingConfig, _> = embeddings
            .iter()
            .map(|e| (e.column.clone(), e.clone()))
            .collect::<HashMap<_, _>>();

        // Validate that all row_id columns exist in the dataset schema.
        let base_schema = base_table.schema();
        for (column, config) in &embed_columns {
            if let Some(primary_keys) = &config.primary_keys {
                for pk in primary_keys {
                    if base_schema.column_with_name(pk).is_none() {
                        let valid_columns: String = base_schema
                            .fields()
                            .iter()
                            .map(|f| f.name().as_str())
                            .collect::<Vec<_>>()
                            .join(", ");
                        return Err(Error::RowIdColumnNotFound {
                            column: column.clone(),
                            row_id_column: pk.clone(),
                            valid_columns,
                        });
                    }
                }
            }
        }

        // Early check if embedding models are available.
        for (column, config) in &embed_columns {
            let model = &config.model;
            if !embedding_models.read().await.contains_key(model) {
                return Err(Error::EmbeddingModelNotFound {
                    column: column.clone(),
                    model: model.clone(),
                });
            }
        }

        let embed_chunker_config: HashMap<String, ChunkingConfig> = embeddings
            .iter()
            .filter(|e| e.chunking.as_ref().is_some_and(|s| s.enabled))
            .filter_map(|e| {
                e.chunking.as_ref().map(|chunk_cfg| {
                    (
                        e.column.clone(),
                        ChunkingConfig {
                            target_chunk_size: chunk_cfg.target_chunk_size,
                            overlap_size: chunk_cfg.overlap_size,
                            trim_whitespace: chunk_cfg.trim_whitespace,
                            file_format,
                        },
                    )
                })
            })
            .collect::<HashMap<_, _>>();

        let embedding_table = EmbeddingTable::try_new(
            base_table,
            embed_columns,
            Arc::clone(embedding_models),
            embed_chunker_config,
        )
        .await?;

        Ok(Arc::new(embedding_table) as Arc<dyn TableProvider>)
    }

    /// When creating a new [`EmbeddingTable`], the provided columns (in `embed_columns`) must be checked to see if they are already in the base table.
    /// Constructing the [`EmbeddingColumnConfig`] for each column is different depending on whether the column is in the base table or not.
    pub async fn try_new(
        base_table: Arc<dyn TableProvider>,
        embed_columns: HashMap<String, ColumnEmbeddingConfig>,
        embedding_models: Arc<RwLock<EmbeddingModelStore>>,
        embed_chunker_config: HashMap<String, ChunkingConfig<'_>>,
    ) -> Result<Self, Error> {
        let base_schema = base_table.schema();
        let mut embedded_columns: HashMap<String, EmbeddingColumnConfig> = HashMap::new();

        for (column, config) in embed_columns {
            let model = config.model.clone();
            let chunking_config_opt = embed_chunker_config.get(&column);

            let source_shape = Self::detect_source_shape(&column, &base_schema)?;

            if Self::base_table_has_embedding_column(&base_schema, &column) {
                tracing::debug!(
                    "Column '{column}' has needed embeddings in base table. Will not augment."
                );

                if chunking_config_opt.is_some() {
                    tracing::warn!(
                        "Column '{}' is an embedding from the base table, but chunking config was provided. It will not be used. Chunking will be determined by base table config.",
                        column
                    );
                }

                let Some(vector_length) =
                    Self::embedding_size_from_base_table(&column, &base_schema)
                        .or(config.vector_size.and_then(|sz| i32::try_from(sz).ok()))
                else {
                    tracing::warn!(
                        "Column '{column}' has embeddings in base table, but the vector length could not be determined from schema. Ignoring column. Provide a value for the vector_size key in the column's embedding configuration.",
                    );
                    continue;
                };

                // For precomputed embeddings, resolve the mode based on
                // the source column's shape. If the source column isn't
                // present (unusual — we got here via the embedding
                // column existing), default to Scalar.
                let input_mode = match source_shape {
                    Some(shape) => Self::resolve_input_mode(&column, shape, &config)?,
                    None => EmbeddingInputMode::Scalar,
                };

                embedded_columns.insert(
                    column,
                    EmbeddingColumnConfig {
                        model_name: model,
                        vector_size: vector_length,
                        in_base_table: true,
                        chunker: None, // Don't need chunking since it is done in base table.
                        input_mode,
                    },
                );
            } else {
                tracing::debug!(
                    "Column '{column}' does not have needed embeddings in base table. Will augment with model {model}."
                );

                // Source shape is required when we're computing
                // embeddings — we can't embed a column we can't read.
                let Some(shape) = source_shape else {
                    return EmbeddingColumnNotInSchemaSnafu { column }.fail();
                };
                let input_mode = Self::resolve_input_mode(&column, shape, &config)?;

                let Some(vector_length) =
                    Self::embedding_size_from_models(&model, &embedding_models).await
                else {
                    tracing::warn!(
                        "For column '{column}', cannot precompute vector length from model '{model}'. Ignoring column."
                    );
                    continue;
                };

                let mut chunker = None;
                if let Some(chunking_config) = chunking_config_opt {
                    match construct_chunker(&model, chunking_config, &embedding_models).await {
                        Ok(c) => chunker = Some(c),
                        Err(e) => {
                            tracing::warn!(
                                "Column '{column}' expects to be chunked, but the model '{model}' does not support chunking. Ignoring chunking config. Error: {e}"
                            );
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
                        input_mode,
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
    ///  - If `c_embedding` has a doubly-nested list type AND the source column `c` is scalar-typed
    ///    (a chunked scalar embedding), `c_offsets` must also be in the base schema as
    ///    `List[FixedSizeList[Int32, 2]]`. For multi-vector embeddings (`c` is list-typed), no
    ///    offsets column is required — element index is the implicit offset.
    fn base_table_has_embedding_column(base_schema: &SchemaRef, column: &str) -> bool {
        // Check if the base column exists
        let Some((_, source_field)) = base_schema.column_with_name(column) else {
            tracing::warn!(
                "Column '{column}' does not exist in the base table. Cannot use it to create embeddings"
            );
            return false;
        };

        // Check if the embedding column exists and has a valid data type
        let Some((_, embedding_field)) =
            base_schema.column_with_name(embedding_col!(column).as_str())
        else {
            return false;
        };

        if !is_valid_embedding_type(embedding_field.data_type()) {
            return false;
        }

        // If the source column is list-of-string, this is multi-vector
        // mode: no sibling offsets column is required.
        let source_is_list_of_string = matches!(
            source_field.data_type(),
            DataType::List(inner) | DataType::LargeList(inner)
                if matches!(
                    inner.data_type(),
                    DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8
                )
        );

        // Multi-vector mode must have a doubly-nested embedding column
        // (`List<FixedSizeList<...>>` or similar). Otherwise treating a
        // scalar embedding as precomputed leads to UNNEST planning errors
        // downstream.
        let embedding_is_doubly_nested = matches!(
            embedding_field.data_type(),
            DataType::List(inner)
            | DataType::LargeList(inner)
            | DataType::FixedSizeList(inner, _)
                if matches!(inner.data_type(), DataType::FixedSizeList(_, _))
        );

        if source_is_list_of_string && !embedding_is_doubly_nested {
            tracing::warn!(
                "Column '{column}' is list-typed (multi-vector) but the precomputed embedding column '{}' is not doubly-nested (`List<FixedSizeList<...>>`). Will recompute embeddings.",
                embedding_col!(column).as_str()
            );
            return false;
        }

        // Otherwise, if the embedding is doubly nested (chunked scalar),
        // require the offsets column too.
        if !source_is_list_of_string && embedding_is_doubly_nested {
            let Some((_, offsets_field)) =
                base_schema.column_with_name(offset_col!(column).as_str())
            else {
                return false;
            };

            if !is_valid_offset_type(offsets_field.data_type()) {
                return false;
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

    #[must_use]
    pub fn get_embedding_model_used_by(&self, column: &str) -> Option<String> {
        self.embedded_columns
            .get(column)
            .map(|cfg| cfg.model_name.clone())
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

    /// Returns true if the column's embedding is produced in multi-vector
    /// mode (source column is list-typed, one embedding per list
    /// element). Multi-vector and chunked outputs share the same
    /// doubly-nested Arrow shape, but the search path aggregates them
    /// differently (multi-vector: max over list elements; chunked: max
    /// over chunks of one scalar string).
    #[must_use]
    pub fn is_multi_vector(&self, column: &str) -> bool {
        self.embedded_columns
            .get(column)
            .is_some_and(|cfg| cfg.input_mode.is_list_multi())
    }

    /// Returns the aggregation strategy configured for a multi-vector
    /// column, or `None` if the column is scalar.
    #[must_use]
    pub fn multi_vector_aggregation(&self, column: &str) -> Option<EmbeddingAggregation> {
        self.embedded_columns
            .get(column)
            .and_then(|cfg| match cfg.input_mode {
                EmbeddingInputMode::ListMulti { aggregation, .. } => Some(aggregation),
                EmbeddingInputMode::Scalar => None,
            })
    }

    /// Returns true when the column's output Arrow type is
    /// doubly-nested (`List<FixedSizeList<...>>`): either because the
    /// scalar source is chunked, or because the source is list-typed
    /// (multi-vector). Both use the same UNNEST-based search path.
    #[must_use]
    pub fn has_nested_embedding_output(&self, column: &str) -> bool {
        self.is_chunked(column) || self.is_multi_vector(column)
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

    #[must_use]
    pub fn get_underlying_ref(&self) -> &Arc<dyn TableProvider> {
        &self.base_table
    }

    fn embedding_size_from_base_table(column: &str, base_schema: &SchemaRef) -> Option<i32> {
        let (_, embedding_field) = base_schema.column_with_name(embedding_col!(column).as_str())?;
        vector_length(embedding_field.data_type())
    }

    /// Shape of the source column, if it exists and has a supported
    /// type. Returns `None` when the column isn't in the base schema
    /// (caller handles that case); errors when the column exists but has
    /// an unsupported type.
    fn detect_source_shape(
        column: &str,
        base_schema: &SchemaRef,
    ) -> Result<Option<SourceShape>, Error> {
        let Some((_, field)) = base_schema.column_with_name(column) else {
            return Ok(None);
        };
        let data_type = field.data_type();
        match data_type {
            DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8 => {
                Ok(Some(SourceShape::Scalar(data_type.clone())))
            }
            DataType::List(inner) | DataType::LargeList(inner)
                if matches!(
                    inner.data_type(),
                    DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8
                ) =>
            {
                Ok(Some(SourceShape::ListOfString))
            }
            _ => InvalidColumnTypeSnafu {
                column: column.to_string(),
                data_type: data_type.clone(),
            }
            .fail(),
        }
    }

    /// Resolve the effective [`EmbeddingInputMode`] for a column given
    /// its detected source shape and the user-provided configuration.
    /// Enforces validation rules: list-typed multi-vector options only
    /// apply to list columns; chunking is incompatible with multi-vector;
    /// `max_elements_per_row` is bounds-checked.
    fn resolve_input_mode(
        column: &str,
        shape: SourceShape,
        config: &ColumnEmbeddingConfig,
    ) -> Result<EmbeddingInputMode, Error> {
        match shape {
            SourceShape::Scalar(data_type) => {
                if config.aggregation.is_some() || config.max_elements_per_row.is_some() {
                    return MultiVectorOptionsOnScalarSnafu {
                        column: column.to_string(),
                        data_type,
                    }
                    .fail();
                }
                Ok(EmbeddingInputMode::Scalar)
            }
            SourceShape::ListOfString => {
                if config.chunking.as_ref().is_some_and(|c| c.enabled) {
                    return MultiVectorChunkingNotSupportedSnafu {
                        column: column.to_string(),
                    }
                    .fail();
                }
                let aggregation = config.aggregation.unwrap_or_default();
                let cap = config
                    .max_elements_per_row
                    .unwrap_or(MULTI_VECTOR_MAX_ELEMENTS_DEFAULT);
                if cap == 0 || cap > MULTI_VECTOR_MAX_ELEMENTS_HARD_CAP {
                    return MaxElementsPerRowOutOfRangeSnafu {
                        column: column.to_string(),
                        value: cap,
                        cap: MULTI_VECTOR_MAX_ELEMENTS_HARD_CAP,
                    }
                    .fail();
                }
                Ok(EmbeddingInputMode::ListMulti {
                    aggregation,
                    max_elements_per_row: cap,
                })
            }
        }
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

        match (cfg.input_mode, cfg.chunker.is_some()) {
            // Scalar + chunked: doubly nested embedding + offsets
            // (character offsets of each chunk into the source string).
            (EmbeddingInputMode::Scalar, true) => vec![
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
            ],
            // Scalar + unchunked: one vector per row.
            (EmbeddingInputMode::Scalar, false) => vec![Arc::new(Field::new_fixed_size_list(
                embedding_col!(field.name()),
                Field::new("item", DataType::Float32, true),
                cfg.vector_size,
                true,
            ))],
            // Multi-vector: one vector per list element. No offsets —
            // element index serves as the implicit offset into the
            // source list at query time.
            //
            // The inner FixedSizeList is nullable so that null strings
            // inside the source list produce null vectors in the output,
            // preserving index correspondence with the source column.
            // The outer list is non-null: a null source row maps to an
            // empty output list.
            (EmbeddingInputMode::ListMulti { .. }, _) => vec![Arc::new(Field::new_list(
                embedding_col!(field.name()),
                Field::new_fixed_size_list(
                    "item",
                    Field::new("item", DataType::Float32, false),
                    cfg.vector_size,
                    true,
                ),
                false,
            ))],
        }
    }
}

#[deny(clippy::missing_trait_methods)]
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
                            embedding_fields.iter().map(|f| f.name().clone()).collect(),
                        );
                        embedding_fields
                    })
            })
            .flatten()
            .collect();

        // Deduplicate: if the base table already stores the embedding column (e.g. after a
        // refresh writes it to DuckDB while in_base_table was set to false at startup), skip
        // re-appending it to avoid a duplicate field that corrupts column-index resolution.
        let base_field_names: std::collections::HashSet<_> =
            base_fields.iter().map(|f| f.name().clone()).collect();
        embedding_fields.retain(|f| !base_field_names.contains(f.name()));
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
            tracing::trace!(
                "For `EmbeddingTable`, no additional embedding columns to compute. Forwarding entirely to base table."
            );
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

        // If we have an engine, Don't do this. Engine has two modes:
        //   1. List records, essentially another table provider we can JOIN ON
        //   2. Query records, Same as above BUT, not a plain scan. Instead they've been ordered by some query payload.  BUTTTT we output a score from this???

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
            .map(|f| f.name().clone())
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

    async fn delete_from(
        &self,
        state: &dyn Session,
        filters: Vec<Expr>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        self.base_table.delete_from(state, filters).await
    }

    async fn update(
        &self,
        state: &dyn Session,
        assignments: Vec<(String, Expr)>,
        filters: Vec<Expr>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        self.base_table.update(state, assignments, filters).await
    }

    async fn scan_with_args<'a>(
        &self,
        state: &dyn Session,
        args: datafusion::catalog::ScanArgs<'a>,
    ) -> DataFusionResult<datafusion::catalog::ScanResult> {
        self.base_table.scan_with_args(state, args).await
    }

    async fn truncate(&self, state: &dyn Session) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        self.base_table.truncate(state).await
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

    #[test]
    fn test_list_source_with_scalar_embedding_rejected() {
        // Multi-vector source (`List<Utf8>`) paired with a singly-nested
        // (scalar) embedding column is a shape mismatch: the runtime
        // cannot UNNEST stored vectors per-element from a
        // `FixedSizeList<Float32>` alone.
        assert!(!EmbeddingTable::base_table_has_embedding_column(
            &Arc::new(Schema::new(vec![
                field("c", DataType::List(field("item", DataType::Utf8))),
                field(
                    "c_embedding",
                    DataType::FixedSizeList(field("item", DataType::Float32), 4),
                ),
            ])),
            "c"
        ));
    }

    #[tokio::test]
    async fn test_invalid_row_id_column_rejected() {
        let schema = Arc::new(Schema::new(vec![
            field("id", DataType::Int64),
            field("r_name", DataType::Utf8),
            field("r_comment", DataType::Utf8),
        ]));
        let base_table: Arc<dyn TableProvider> = Arc::new(
            datafusion::catalog::MemTable::try_new(schema, vec![vec![]]).expect("create MemTable"),
        );
        let embedding_models = Arc::new(RwLock::new(HashMap::new()));

        let embeddings = vec![ColumnEmbeddingConfig {
            column: "r_name".to_string(),
            model: "test_model".to_string(),
            primary_keys: Some(vec!["n_regionkey".to_string()]),
            chunking: None,
            vector_size: None,
            aggregation: None,
            max_elements_per_row: None,
        }];

        let result =
            EmbeddingTable::from_spicepod_columns(base_table, embeddings, &embedding_models, None)
                .await;

        assert!(result.is_err());
        let err = result.expect_err("expected row_id validation error");
        let err_msg = err.to_string();
        assert!(
            err_msg.contains("n_regionkey"),
            "Error should mention the invalid column name, got: {err_msg}"
        );
        assert!(
            err_msg.contains("r_name"),
            "Error should mention the embedding column, got: {err_msg}"
        );
    }

    #[tokio::test]
    async fn test_valid_row_id_column_accepted() {
        let schema = Arc::new(Schema::new(vec![
            field("id", DataType::Int64),
            field("r_name", DataType::Utf8),
        ]));
        let base_table: Arc<dyn TableProvider> = Arc::new(
            datafusion::catalog::MemTable::try_new(schema, vec![vec![]]).expect("create MemTable"),
        );
        let embedding_models = Arc::new(RwLock::new(HashMap::new()));

        // Valid row_id but model doesn't exist — should fail on model check, NOT row_id check
        let embeddings = vec![ColumnEmbeddingConfig {
            column: "r_name".to_string(),
            model: "test_model".to_string(),
            primary_keys: Some(vec!["id".to_string()]),
            chunking: None,
            vector_size: None,
            aggregation: None,
            max_elements_per_row: None,
        }];

        let result =
            EmbeddingTable::from_spicepod_columns(base_table, embeddings, &embedding_models, None)
                .await;

        assert!(result.is_err());
        let err = result.expect_err("expected model-not-found error");
        // Should fail on model not found, not row_id validation
        assert!(
            matches!(err, Error::EmbeddingModelNotFound { .. }),
            "Expected EmbeddingModelNotFound error, got: {err}"
        );
    }

    #[tokio::test]
    async fn test_no_row_id_columns_accepted() {
        let schema = Arc::new(Schema::new(vec![
            field("id", DataType::Int64),
            field("r_name", DataType::Utf8),
        ]));
        let base_table: Arc<dyn TableProvider> = Arc::new(
            datafusion::catalog::MemTable::try_new(schema, vec![vec![]]).expect("create MemTable"),
        );
        let embedding_models = Arc::new(RwLock::new(HashMap::new()));

        // No primary_keys specified — should pass row_id validation and fail on model check
        let embeddings = vec![ColumnEmbeddingConfig {
            column: "r_name".to_string(),
            model: "test_model".to_string(),
            primary_keys: None,
            chunking: None,
            vector_size: None,
            aggregation: None,
            max_elements_per_row: None,
        }];

        let result =
            EmbeddingTable::from_spicepod_columns(base_table, embeddings, &embedding_models, None)
                .await;

        assert!(result.is_err());
        let err = result.expect_err("expected model-not-found error");
        assert!(
            matches!(err, Error::EmbeddingModelNotFound { .. }),
            "Expected EmbeddingModelNotFound error, got: {err}"
        );
    }

    #[tokio::test]
    async fn test_multiple_row_id_columns_one_invalid() {
        let schema = Arc::new(Schema::new(vec![
            field("id", DataType::Int64),
            field("r_name", DataType::Utf8),
        ]));
        let base_table: Arc<dyn TableProvider> = Arc::new(
            datafusion::catalog::MemTable::try_new(schema, vec![vec![]]).expect("create MemTable"),
        );
        let embedding_models = Arc::new(RwLock::new(HashMap::new()));

        let embeddings = vec![ColumnEmbeddingConfig {
            column: "r_name".to_string(),
            model: "test_model".to_string(),
            primary_keys: Some(vec!["id".to_string(), "nonexistent".to_string()]),
            chunking: None,
            vector_size: None,
            aggregation: None,
            max_elements_per_row: None,
        }];

        let result =
            EmbeddingTable::from_spicepod_columns(base_table, embeddings, &embedding_models, None)
                .await;

        assert!(result.is_err());
        let err = result.expect_err("expected row_id validation error");
        let err_msg = err.to_string();
        assert!(
            err_msg.contains("nonexistent"),
            "Error should mention the invalid column, got: {err_msg}"
        );
    }

    // ===== M1: multi-vector configuration =====

    fn list_of_utf8(name: &str) -> FieldRef {
        Arc::new(Field::new_list(
            name,
            Field::new("item", DataType::Utf8, true),
            true,
        ))
    }

    #[test]
    fn test_detect_source_shape_scalar_utf8() {
        let schema = Arc::new(Schema::new(vec![field("c", DataType::Utf8)]));
        let shape = EmbeddingTable::detect_source_shape("c", &schema).expect("ok");
        assert_eq!(shape, Some(SourceShape::Scalar(DataType::Utf8)));
    }

    #[test]
    fn test_detect_source_shape_list_of_utf8() {
        let schema = Arc::new(Schema::new(vec![list_of_utf8("tags")]));
        let shape = EmbeddingTable::detect_source_shape("tags", &schema).expect("ok");
        assert_eq!(shape, Some(SourceShape::ListOfString));
    }

    #[test]
    fn test_detect_source_shape_unsupported() {
        let schema = Arc::new(Schema::new(vec![field("c", DataType::Int32)]));
        let err = EmbeddingTable::detect_source_shape("c", &schema)
            .expect_err("expected unsupported type");
        assert!(matches!(err, Error::InvalidColumnType { .. }));
    }

    #[test]
    fn test_detect_source_shape_missing_column() {
        let schema = Arc::new(Schema::new(vec![field("other", DataType::Utf8)]));
        let shape = EmbeddingTable::detect_source_shape("c", &schema).expect("ok");
        assert_eq!(shape, None);
    }

    #[test]
    fn test_resolve_input_mode_scalar_default() {
        let cfg = ColumnEmbeddingConfig {
            column: "c".to_string(),
            model: "m".to_string(),
            primary_keys: None,
            chunking: None,
            vector_size: None,
            aggregation: None,
            max_elements_per_row: None,
        };
        let mode =
            EmbeddingTable::resolve_input_mode("c", SourceShape::Scalar(DataType::Utf8), &cfg)
                .expect("ok");
        assert_eq!(mode, EmbeddingInputMode::Scalar);
    }

    #[test]
    fn test_resolve_input_mode_scalar_rejects_multi_vector_options() {
        let cfg = ColumnEmbeddingConfig {
            column: "c".to_string(),
            model: "m".to_string(),
            primary_keys: None,
            chunking: None,
            vector_size: None,
            aggregation: Some(EmbeddingAggregation::Max),
            max_elements_per_row: None,
        };
        let err =
            EmbeddingTable::resolve_input_mode("c", SourceShape::Scalar(DataType::Utf8), &cfg)
                .expect_err("expected rejection");
        assert!(matches!(err, Error::MultiVectorOptionsOnScalar { .. }));
    }

    #[test]
    fn test_resolve_input_mode_list_defaults_max_and_cap_32() {
        let cfg = ColumnEmbeddingConfig {
            column: "tags".to_string(),
            model: "m".to_string(),
            primary_keys: None,
            chunking: None,
            vector_size: None,
            aggregation: None,
            max_elements_per_row: None,
        };
        let mode = EmbeddingTable::resolve_input_mode("tags", SourceShape::ListOfString, &cfg)
            .expect("ok");
        match mode {
            EmbeddingInputMode::ListMulti {
                aggregation,
                max_elements_per_row,
            } => {
                assert_eq!(aggregation, EmbeddingAggregation::Max);
                assert_eq!(max_elements_per_row, MULTI_VECTOR_MAX_ELEMENTS_DEFAULT);
            }
            EmbeddingInputMode::Scalar => panic!("expected ListMulti"),
        }
    }

    #[test]
    fn test_resolve_input_mode_list_honors_aggregation_override() {
        let cfg = ColumnEmbeddingConfig {
            column: "tags".to_string(),
            model: "m".to_string(),
            primary_keys: None,
            chunking: None,
            vector_size: None,
            aggregation: Some(EmbeddingAggregation::Mean),
            max_elements_per_row: Some(64),
        };
        let mode = EmbeddingTable::resolve_input_mode("tags", SourceShape::ListOfString, &cfg)
            .expect("ok");
        assert_eq!(
            mode,
            EmbeddingInputMode::ListMulti {
                aggregation: EmbeddingAggregation::Mean,
                max_elements_per_row: 64,
            }
        );
    }

    #[test]
    fn test_resolve_input_mode_list_rejects_chunking() {
        let cfg = ColumnEmbeddingConfig {
            column: "tags".to_string(),
            model: "m".to_string(),
            primary_keys: None,
            chunking: Some(spicepod::component::embeddings::EmbeddingChunkConfig {
                enabled: true,
                target_chunk_size: 256,
                overlap_size: 0,
                trim_whitespace: false,
            }),
            vector_size: None,
            aggregation: None,
            max_elements_per_row: None,
        };
        let err = EmbeddingTable::resolve_input_mode("tags", SourceShape::ListOfString, &cfg)
            .expect_err("expected chunking rejection");
        assert!(matches!(err, Error::MultiVectorChunkingNotSupported { .. }));
    }

    #[test]
    fn test_resolve_input_mode_list_rejects_zero_cap() {
        let cfg = ColumnEmbeddingConfig {
            column: "tags".to_string(),
            model: "m".to_string(),
            primary_keys: None,
            chunking: None,
            vector_size: None,
            aggregation: None,
            max_elements_per_row: Some(0),
        };
        let err = EmbeddingTable::resolve_input_mode("tags", SourceShape::ListOfString, &cfg)
            .expect_err("expected cap rejection");
        assert!(matches!(err, Error::MaxElementsPerRowOutOfRange { .. }));
    }

    #[test]
    fn test_resolve_input_mode_list_rejects_cap_above_hard_cap() {
        let cfg = ColumnEmbeddingConfig {
            column: "tags".to_string(),
            model: "m".to_string(),
            primary_keys: None,
            chunking: None,
            vector_size: None,
            aggregation: None,
            max_elements_per_row: Some(MULTI_VECTOR_MAX_ELEMENTS_HARD_CAP + 1),
        };
        let err = EmbeddingTable::resolve_input_mode("tags", SourceShape::ListOfString, &cfg)
            .expect_err("expected cap rejection");
        assert!(matches!(err, Error::MaxElementsPerRowOutOfRange { .. }));
    }

    // ===== M4: accelerator schema compatibility =====
    //
    // Multi-vector output is `List<FixedSizeList<F32, D>>` — the
    // identical Arrow shape the chunked-scalar path has always emitted
    // (just without the sibling `_offset` column). Arrow, Cayenne, and
    // DuckDB accelerators already round-trip this shape via the chunked
    // code path, so multi-vector inherits that compatibility with no
    // additional accelerator changes. SQLite / Turso nested-
    // FixedSizeList support remains fragile and is addressed by the
    // M7 side-table strategy.

    #[test]
    fn test_embedding_fields_multi_vector_schema_matches_chunked_minus_offset() {
        // A multi-vector column should produce exactly one output field:
        // `<col>_embedding: List<FixedSizeList<F32, D>>`. The chunked path
        // adds an `<col>_offset` sibling; multi-vector does not.
        let tags_field = Arc::new(Field::new_list(
            "tags",
            Field::new("item", DataType::Utf8, true),
            true,
        ));
        let base_schema = Arc::new(Schema::new(vec![Arc::clone(&tags_field)]));

        let embedded_columns = HashMap::from([(
            "tags".to_string(),
            EmbeddingColumnConfig {
                model_name: "m".to_string(),
                vector_size: 4,
                in_base_table: false,
                chunker: None,
                input_mode: EmbeddingInputMode::ListMulti {
                    aggregation: EmbeddingAggregation::Max,
                    max_elements_per_row: 32,
                },
            },
        )]);

        let base_table: Arc<dyn TableProvider> = Arc::new(
            datafusion::catalog::MemTable::try_new(base_schema, vec![vec![]])
                .expect("valid schema"),
        );

        let table = EmbeddingTable {
            base_table,
            embedded_columns,
            embedding_models: Arc::new(RwLock::new(HashMap::new())),
        };

        let fields = table.embedding_fields(&tags_field);
        assert_eq!(fields.len(), 1, "multi-vector produces no offset column");
        let emb = &fields[0];
        assert_eq!(emb.name(), "tags_embedding");
        // Expect List<FixedSizeList<Float32, 4>>
        let DataType::List(inner) = emb.data_type() else {
            panic!("expected List, got {:?}", emb.data_type());
        };
        let DataType::FixedSizeList(leaf, size) = inner.data_type() else {
            panic!("expected inner FixedSizeList, got {:?}", inner.data_type());
        };
        assert_eq!(*size, 4);
        assert_eq!(leaf.data_type(), &DataType::Float32);
    }

    #[test]
    fn test_has_nested_embedding_output_list_multi() {
        let tags_field = Arc::new(Field::new_list(
            "tags",
            Field::new("item", DataType::Utf8, true),
            true,
        ));
        let base_schema = Arc::new(Schema::new(vec![tags_field]));
        let embedded_columns = HashMap::from([(
            "tags".to_string(),
            EmbeddingColumnConfig {
                model_name: "m".to_string(),
                vector_size: 4,
                in_base_table: false,
                chunker: None,
                input_mode: EmbeddingInputMode::ListMulti {
                    aggregation: EmbeddingAggregation::Max,
                    max_elements_per_row: 32,
                },
            },
        )]);
        let base_table: Arc<dyn TableProvider> = Arc::new(
            datafusion::catalog::MemTable::try_new(base_schema, vec![vec![]])
                .expect("valid schema"),
        );
        let table = EmbeddingTable {
            base_table,
            embedded_columns,
            embedding_models: Arc::new(RwLock::new(HashMap::new())),
        };

        assert!(table.is_multi_vector("tags"));
        assert!(!table.is_chunked("tags"));
        // has_nested_embedding_output covers either mode — this is what
        // the search dispatcher keys off of to pick the UNNEST path.
        assert!(table.has_nested_embedding_output("tags"));
        assert_eq!(
            table.multi_vector_aggregation("tags"),
            Some(EmbeddingAggregation::Max)
        );
    }

    #[test]
    fn test_base_table_has_embedding_list_multi_no_offset_required() {
        // Source is List<Utf8>; no offsets column should be required.
        let schema = Arc::new(Schema::new(vec![
            list_of_utf8("tags"),
            Arc::new(Field::new_list(
                "tags_embedding",
                Field::new_fixed_size_list(
                    "item",
                    Field::new("item", DataType::Float32, false),
                    4,
                    false,
                ),
                false,
            )),
        ]));
        assert!(EmbeddingTable::base_table_has_embedding_column(
            &schema, "tags"
        ));
    }

    // ===== schema() dedup =====

    #[test]
    fn test_schema_no_duplicate_when_base_already_has_embedding_column() {
        // Simulate the state after a DuckDB refresh writes the embedding column back to the
        // accelerated base table while `in_base_table` was set to `false` at startup.
        // `schema()` must not append the embedding field a second time.
        let body_field = Arc::new(Field::new("body", DataType::Utf8, true));
        let embedding_field = Arc::new(Field::new_fixed_size_list(
            "body_embedding",
            Field::new("item", DataType::Float32, false),
            4,
            true,
        ));
        // Base table already contains the embedding column.
        let base_schema = Arc::new(Schema::new(vec![
            Arc::clone(&body_field),
            Arc::clone(&embedding_field),
        ]));
        let base_table: Arc<dyn TableProvider> = Arc::new(
            datafusion::catalog::MemTable::try_new(base_schema, vec![vec![]])
                .expect("valid schema"),
        );

        let embedded_columns = HashMap::from([(
            "body".to_string(),
            EmbeddingColumnConfig {
                model_name: "m".to_string(),
                vector_size: 4,
                // in_base_table was false at startup — the refresh later wrote it to DuckDB.
                in_base_table: false,
                chunker: None,
                input_mode: EmbeddingInputMode::Scalar,
            },
        )]);

        let table = EmbeddingTable {
            base_table,
            embedded_columns,
            embedding_models: Arc::new(RwLock::new(HashMap::new())),
        };

        let schema = table.schema();
        let field_names: Vec<_> = schema.fields().iter().map(|f| f.name().as_str()).collect();

        // body_embedding must appear exactly once.
        assert_eq!(
            field_names
                .iter()
                .filter(|&&n| n == "body_embedding")
                .count(),
            1,
            "body_embedding appeared more than once in schema: {field_names:?}"
        );
        assert_eq!(
            field_names,
            vec!["body", "body_embedding"],
            "unexpected field order: {field_names:?}"
        );
    }

    #[test]
    fn test_schema_appends_embedding_column_when_not_in_base() {
        // When the base table does not yet have the embedding column, `schema()` must append it.
        let body_field = Arc::new(Field::new("body", DataType::Utf8, true));
        let base_schema = Arc::new(Schema::new(vec![Arc::clone(&body_field)]));
        let base_table: Arc<dyn TableProvider> = Arc::new(
            datafusion::catalog::MemTable::try_new(base_schema, vec![vec![]])
                .expect("valid schema"),
        );

        let embedded_columns = HashMap::from([(
            "body".to_string(),
            EmbeddingColumnConfig {
                model_name: "m".to_string(),
                vector_size: 4,
                in_base_table: false,
                chunker: None,
                input_mode: EmbeddingInputMode::Scalar,
            },
        )]);

        let table = EmbeddingTable {
            base_table,
            embedded_columns,
            embedding_models: Arc::new(RwLock::new(HashMap::new())),
        };

        let schema = table.schema();
        let field_names: Vec<_> = schema.fields().iter().map(|f| f.name().as_str()).collect();

        assert!(
            field_names.contains(&"body_embedding"),
            "body_embedding missing from schema: {field_names:?}"
        );
        assert_eq!(
            field_names
                .iter()
                .filter(|&&n| n == "body_embedding")
                .count(),
            1,
            "body_embedding appeared more than once: {field_names:?}"
        );
    }
}
