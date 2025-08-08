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

use std::{any::Any, collections::HashMap, sync::Arc};

use arrow::datatypes::SchemaRef;
use arrow_schema::{DataType, Field};
use async_trait::async_trait;

use data_components::s3_vectors::{S3_VECTOR_EMBEDDING_NAME, S3_VECTOR_PRIMARY_KEY_NAME};

use datafusion::{
    catalog::Session,
    common::{Column, Constraints, DFSchema, DFSchemaRef, JoinConstraint, JoinType},
    datasource::{DefaultTableSource, TableProvider, TableType},
    error::{DataFusionError, Result as DataFusionResult},
    logical_expr::{
        Cast, Expr, Join, Limit, LogicalPlan, Projection, TableProviderFilterPushDown, TableScan,
        expr::Alias,
    },
    physical_plan::ExecutionPlan,
    scalar::ScalarValue,
    sql::TableReference,
};

use crate::{embedding_col, embeddings::index::VectorIndex};
use search::generation::util::append_fields;

/// A [`TableProvider`] that adds an embedding column to an underlying [`TableProvider`].
#[derive(Debug, Clone)]
pub struct VectorScanTableProvider {
    pub table_provider: Arc<dyn TableProvider>,
    pub index: Arc<dyn VectorIndex>,
}

impl VectorScanTableProvider {
    pub fn new(table_provider: Arc<dyn TableProvider>, index: Arc<dyn VectorIndex>) -> Self {
        Self {
            table_provider,
            index,
        }
    }

    /// Construct [`TableScan`] for underlying table for `projection` & `filters` relative to [`VectorScanTableProvider`].
    fn underlying_table_scan(
        &self,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
    ) -> DataFusionResult<TableScan> {
        let num_underlying_columns = self.table_provider.schema().fields().len();
        let underlying_projection = projection.map(|proj| {
            proj.iter()
                .filter(|&idx| *idx < num_underlying_columns)
                .copied()
                .collect()
        });

        let filter_refs: Vec<&Expr> = filters.iter().collect();
        let underlying_filters = self
            .table_provider
            .supports_filters_pushdown(filter_refs.as_slice())?
            .into_iter()
            .zip(filters.iter())
            .filter_map(|(supported, filter)| {
                if matches!(supported, TableProviderFilterPushDown::Unsupported) {
                    None
                } else {
                    Some(filter.clone())
                }
            })
            .collect::<Vec<_>>();

        TableScan::try_new(
            TableReference::parse_str("base_table"),
            Arc::new(DefaultTableSource::new(Arc::clone(&self.table_provider))),
            underlying_projection,
            underlying_filters,
            None,
        )
    }

    /// Construct [`TableScan`] for associated vector search index table for `projection` & `filters` relative to [`VectorScanTableProvider`].
    ///
    /// Ok(None), if no columns from table scan are required and no filters are needed.
    fn vector_table_scan(
        &self,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
    ) -> DataFusionResult<Option<LogicalPlan>> {
        // Filter pushdown not supported for S3 vector listVectors. If vector is not needed in projection, do not need this table.
        let need_vector_column = self.need_vector_column(projection);
        if !need_vector_column {
            return Ok(None);
        }

        let list_scan = self.index.list_table_provider();
        let list_scan_schema = list_scan.schema();
        let proj = [
            index_of_column(&list_scan_schema, S3_VECTOR_EMBEDDING_NAME),
            index_of_column(&list_scan_schema, S3_VECTOR_PRIMARY_KEY_NAME),
        ]
        .iter()
        .filter_map(|p| *p)
        .collect();

        let scan = TableScan::try_new(
            TableReference::parse_str("vector_index"),
            Arc::new(DefaultTableSource::new(list_scan)),
            Some(proj),
            vec![],
            None,
        )?;

        // Add expected column aliases.
        let primary_key = self
            .index
            .primary_fields()
            .first()
            .map_or(S3_VECTOR_PRIMARY_KEY_NAME.to_string(), |f| f.name().clone());

        let primary_key_datatype = self
            .index
            .primary_fields()
            .iter()
            .find_map(|f| {
                if *f.name() == primary_key {
                    Some(f.data_type().clone())
                } else {
                    None
                }
            })
            .unwrap_or(DataType::Utf8);

        let aliased = LogicalPlan::Projection(Projection::try_new(
            vec![
                Expr::Alias(Alias::new(
                    Expr::Column(Column::new_unqualified(S3_VECTOR_EMBEDDING_NAME)),
                    Some(TableReference::parse_str("vector_index")),
                    embedding_col!(self.index.embedded_column()),
                )),
                Expr::Alias(Alias::new(
                    Expr::Cast(Cast::new(
                        Box::new(Expr::Column(Column::new_unqualified(
                            S3_VECTOR_PRIMARY_KEY_NAME,
                        ))),
                        primary_key_datatype,
                    )),
                    Some(TableReference::parse_str("vector_index")),
                    primary_key,
                )),
            ],
            Arc::new(LogicalPlan::TableScan(scan)),
        )?);

        Ok(Some(aliased))
    }

    /// For a projection relative to [`VectorScanTableProvider`], check if the embedding column is being requested.
    fn need_vector_column(&self, projection: Option<&Vec<usize>>) -> bool {
        let Some(proj) = projection else {
            return true; // None projection -> "SELECT *".
        };

        let Some(idx) = index_of_column(
            &self.schema(),
            embedding_col!(self.index.embedded_column()).as_str(),
        ) else {
            return false; // Technically unreachable, but by definition not needed.
        };

        proj.contains(&idx)
    }

    /// Construct the required join on expressions as per the primary key.
    fn join_on_expr(&self) -> DataFusionResult<Vec<(Expr, Expr)>> {
        let primary_key_columns = self.index.primary_fields();
        let Some(pk) = primary_key_columns.first() else {
            return Err(DataFusionError::Execution("Vector search index was successfully created without a primary key available during physical planning.\nReport a bug on GitHub: https://github.com/spiceai/spiceai/issues".to_string()));
        };
        Ok(vec![(
            Expr::Column(Column::new_unqualified(pk.name().clone())),
            Expr::Column(Column::new_unqualified(pk.name().clone())),
        )])
    }

    fn qualified_schema(&self, projection: Option<&Vec<usize>>) -> DFSchemaRef {
        let base = self.table_provider.schema();
        let mut qualified_fields: Vec<_> = base
            .fields()
            .iter()
            .map(|f| (Some(TableReference::parse_str("base_table")), Arc::clone(f)))
            .collect();
        qualified_fields.push((
            Some(TableReference::parse_str("vector_index")),
            Arc::new(Field::new(
                embedding_col!(self.index.embedded_column()),
                DataType::new_list(DataType::Float32, false),
                true,
            )),
        ));

        let projected_qualified_fields = match projection {
            None => qualified_fields,
            Some(proj) => qualified_fields
                .into_iter()
                .enumerate()
                .filter_map(|(i, f)| if proj.contains(&i) { Some(f) } else { None })
                .collect(),
        };

        let Ok(df_schema) =
            DFSchema::new_with_metadata(projected_qualified_fields, HashMap::default())
        else {
            unreachable!("DFSchema::try_from is infallible as of DataFusion 38")
        };

        Arc::new(df_schema)
    }
}

fn index_of_column(s: &SchemaRef, col: &str) -> Option<usize> {
    Some(s.column_with_name(col)?.0)
}

#[async_trait]
impl TableProvider for VectorScanTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        append_fields(
            &self.table_provider.schema(),
            vec![Arc::new(Field::new(
                embedding_col!(self.index.embedded_column()),
                DataType::new_list(DataType::Float32, false),
                true,
            ))],
        )
    }

    fn constraints(&self) -> Option<&Constraints> {
        self.table_provider.constraints()
    }

    fn table_type(&self) -> TableType {
        self.table_provider.table_type()
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        // If vector table isn't needed (in either filters or projection)
        let Some(vector_table_scan) = self.vector_table_scan(projection, filters)? else {
            return self
                .table_provider
                .scan(state, projection, filters, limit)
                .await;
        };

        let underlying_table_scan = self.underlying_table_scan(projection, filters)?;

        // Right Join so that all rows in the underlying table are returned.
        // Rows may not have associated vectors periodically due to indexing delays.
        let join = LogicalPlan::Join(Join {
            left: Arc::new(vector_table_scan),
            right: Arc::new(LogicalPlan::TableScan(underlying_table_scan)),
            join_type: JoinType::Right,
            join_constraint: JoinConstraint::On,
            on: self.join_on_expr()?,
            filter: filters.iter().cloned().reduce(Expr::and),
            schema: self.qualified_schema(projection),
            null_equals_null: false,
        });

        let output_proj = LogicalPlan::Projection(Projection::new_from_schema(
            Arc::new(join),
            self.qualified_schema(projection),
        ));

        let limit = LogicalPlan::Limit(Limit {
            input: Arc::new(output_proj),
            fetch: Some(Box::new(Expr::Literal(ScalarValue::UInt64(
                limit.map(|l| l as u64),
            )))),
            skip: None,
        });

        state.create_physical_plan(&limit).await
    }
}

#[cfg(test)]
mod tests {

    use std::{any::Any, collections::HashMap, sync::Arc};

    use arrow::{
        array::{
            ArrayData, ArrayRef, BooleanArray, FixedSizeListArray, Float32Array, Float64Array,
            Int8Array, Int16Array, Int32Array, Int64Array, RecordBatch, StringArray, UInt8Array,
            UInt16Array, UInt32Array, UInt64Array, new_null_array,
        },
        buffer::Buffer,
        util::pretty,
    };
    use arrow_schema::{DataType, Field, Schema, SchemaRef};
    use data_components::s3_vectors::{
        MetadataColumn, MetadataColumns, S3_VECTOR_EMBEDDING_NAME, S3_VECTOR_PRIMARY_KEY_NAME,
        query_provider::S3_VECTOR_DISTANCE_NAME,
    };
    use datafusion::{
        catalog::{MemTable, Session, TableProvider},
        datasource::TableType,
        error::DataFusionError,
        logical_expr::TableProviderFilterPushDown,
        physical_plan::{DisplayAs, ExecutionPlan},
        prelude::{Expr, SessionConfig, SessionContext},
        sql::TableReference,
    };
    use search::generation::util::append_fields;
    use snafu::ResultExt;

    use crate::embeddings::index::{VectorIndex, VectorScanTableProvider};

    /// This is just a [`MemTable`] that pretends it can support all filter pushdowns.
    /// This is useful for testing explain plans.
    #[derive(Debug)]
    pub struct ExplainMemTable(MemTable);

    /// Wraps a [`ExecutionPlan`] with a new [`DisplayAs`] to show what filters have been pushed down.
    /// This is useful for testing explain plans.
    #[derive(Debug)]
    pub struct ExplainExecutionPlan(
        Arc<dyn ExecutionPlan>,
        Vec<Expr>,
        Option<usize>,
        Option<Vec<usize>>,
    );

    impl ExecutionPlan for ExplainExecutionPlan {
        fn name(&self) -> &'static str {
            "ExplainExecutionPlan"
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn properties(&self) -> &datafusion::physical_plan::PlanProperties {
            self.0.properties()
        }

        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            self.0.children()
        }

        fn with_new_children(
            self: Arc<Self>,
            children: Vec<Arc<dyn ExecutionPlan>>,
        ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
            Ok(Arc::new(ExplainExecutionPlan(
                Arc::clone(&self.0).with_new_children(children)?,
                self.1.clone(),
                self.2,
                self.3.clone(),
            )))
        }

        fn execute(
            &self,
            partition: usize,
            context: Arc<datafusion::execution::TaskContext>,
        ) -> datafusion::error::Result<datafusion::execution::SendableRecordBatchStream> {
            self.0.execute(partition, context)
        }
    }

    impl DisplayAs for ExplainExecutionPlan {
        fn fmt_as(
            &self,
            _t: datafusion::physical_plan::DisplayFormatType,
            f: &mut std::fmt::Formatter,
        ) -> std::fmt::Result {
            let columns: Vec<String> = self
                .schema()
                .fields()
                .iter()
                .map(|f| f.name())
                .cloned()
                .collect();

            write!(
                f,
                "ExplainExecutionPlan: projection={columns:?} filter={:?} limit={:?}",
                self.1, self.2,
            )?;
            Ok(())
        }
    }

    #[async_trait::async_trait]
    impl TableProvider for ExplainMemTable {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn schema(&self) -> SchemaRef {
            self.0.schema()
        }

        fn table_type(&self) -> TableType {
            self.0.table_type()
        }

        async fn scan(
            &self,
            state: &dyn Session,
            projection: Option<&Vec<usize>>,
            filters: &[Expr],
            limit: Option<usize>,
        ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
            Ok(Arc::new(ExplainExecutionPlan(
                self.0.scan(state, projection, filters, limit).await?,
                filters.to_vec(),
                limit,
                projection.cloned(),
            )) as Arc<dyn ExecutionPlan>)
        }

        fn supports_filters_pushdown(
            &self,
            filters: &[&Expr],
        ) -> Result<Vec<TableProviderFilterPushDown>, DataFusionError> {
            Ok(vec![TableProviderFilterPushDown::Exact; filters.len()])
        }
    }

    /// An implementation of [`VectorIndex`] that has one row. Useful for testing explain plans.
    #[derive(Debug)]
    pub struct PretendVectorIndex {
        embedded_column: String,
        primary_columns: Vec<Field>,
        schema: Schema,
        metadata: MetadataColumns,
    }
    impl PretendVectorIndex {
        pub fn new(embedded_column: String, primary_columns: Vec<Field>, schema: Schema) -> Self {
            let cols = schema
                .fields()
                .iter()
                .filter_map(|f| {
                    if f.name() == S3_VECTOR_PRIMARY_KEY_NAME
                        || f.name() == S3_VECTOR_EMBEDDING_NAME
                    {
                        return None;
                    }
                    if f.metadata().get("filterable") == Some(&"true".to_string()) {
                        Some(MetadataColumn::Filterable(Arc::clone(f)))
                    } else {
                        Some(MetadataColumn::NonFilterable(Arc::clone(f)))
                    }
                })
                .collect::<Vec<_>>();

            Self {
                embedded_column,
                primary_columns,
                schema,
                metadata: MetadataColumns::from(cols),
            }
        }
    }

    #[async_trait::async_trait]
    impl VectorIndex for PretendVectorIndex {
        fn embedded_column(&self) -> String {
            self.embedded_column.clone()
        }

        fn primary_fields(&self) -> Vec<Field> {
            self.primary_columns.clone()
        }

        fn list_table_provider(&self) -> Arc<dyn TableProvider> {
            Arc::new(ExplainMemTable(
                MemTable::try_new(
                    Arc::new(self.schema.clone()),
                    vec![vec![one_row_default_record_batch_for_schema(&Arc::new(
                        self.schema.clone(),
                    ))]],
                )
                .expect("Could not build PretendVectorIndex::list_table_provider"),
            ))
        }

        fn metadata_columns(&self) -> &MetadataColumns {
            &self.metadata
        }

        fn augment_table(self: Arc<Self>, table: Arc<dyn TableProvider>) -> Arc<dyn TableProvider> {
            table
        }

        async fn write(&self, _record: &RecordBatch) {}
        async fn query_table_provider(
            &self,
            _query: &str,
        ) -> Result<Arc<dyn TableProvider>, Box<dyn std::error::Error + Send + Sync>> {
            let schema = append_fields(
                &Arc::new(self.schema.clone()),
                vec![Arc::new(Field::new(
                    S3_VECTOR_DISTANCE_NAME,
                    DataType::Float64,
                    false,
                ))],
            );
            println!("In query_table_provider schema={:?}", schema);
            Ok(Arc::new(ExplainMemTable(
                MemTable::try_new(
                    Arc::clone(&schema),
                    vec![vec![one_row_default_record_batch_for_schema(&schema)]],
                )
                .boxed()?,
            )) as Arc<dyn TableProvider>)
        }
    }

    pub async fn test_explain(
        provider: Arc<dyn TableProvider>,
        tbl: TableReference,
        sql: &str,
        snapshot_name: &str,
    ) -> Result<(), String> {
        let session =
            SessionContext::new_with_config(SessionConfig::new().with_target_partitions(3));
        session
            .register_table(tbl, provider)
            .map_err(|e| e.to_string())?;

        let df = session
            .sql(format!("EXPLAIN {sql}").as_str())
            .await
            .map_err(|e| e.to_string())?;

        let col = df.collect().await.map_err(|e| e.to_string())?;
        insta::assert_snapshot!(
            snapshot_name,
            format!(
                "{}",
                pretty::pretty_format_batches(&col).map_err(|e| e.to_string())?
            )
        );
        Ok(())
    }

    #[allow(clippy::cast_sign_loss, clippy::cast_precision_loss)]
    fn default_value_array(dt: &DataType) -> ArrayRef {
        match dt {
            DataType::Int8 => Arc::new(Int8Array::from(vec![0])) as ArrayRef,
            DataType::Int16 => Arc::new(Int16Array::from(vec![0])) as ArrayRef,
            DataType::Int32 => Arc::new(Int32Array::from(vec![0])) as ArrayRef,
            DataType::Int64 => Arc::new(Int64Array::from(vec![0])) as ArrayRef,
            DataType::UInt8 => Arc::new(UInt8Array::from(vec![0])) as ArrayRef,
            DataType::UInt16 => Arc::new(UInt16Array::from(vec![0])) as ArrayRef,
            DataType::UInt32 => Arc::new(UInt32Array::from(vec![0])) as ArrayRef,
            DataType::UInt64 => Arc::new(UInt64Array::from(vec![0])) as ArrayRef,
            DataType::Float32 => Arc::new(Float32Array::from(vec![0.0])) as ArrayRef,
            DataType::Float64 => Arc::new(Float64Array::from(vec![0.0])) as ArrayRef,
            DataType::Boolean => Arc::new(BooleanArray::from(vec![false])) as ArrayRef,
            DataType::Utf8 => Arc::new(StringArray::from(vec![""])) as ArrayRef,
            DataType::FixedSizeList(_, length) => {
                let list_data_type = DataType::FixedSizeList(
                    Arc::new(Field::new_list_field(DataType::Float32, false)),
                    *length,
                );
                Arc::new(FixedSizeListArray::from(
                    ArrayData::builder(list_data_type.clone())
                        .len(1)
                        .add_child_data(
                            ArrayData::builder(DataType::Float32)
                                .len(*length as usize)
                                .add_buffer(Buffer::from_slice_ref(
                                    (0..(*length as usize))
                                        .map(|s| s as f32)
                                        .collect::<Vec<_>>(),
                                ))
                                .build()
                                .expect("unable to build FixedSizeListArray's ArrayData"),
                        )
                        .build()
                        .expect("unable to build FixedSizeListArray"),
                ))
            }
            _ => new_null_array(dt, 1),
        }
    }

    /// Creates a [`RecordBatch`] with a single row that has default value of types, as per the [`Schema`].
    fn one_row_default_record_batch_for_schema(schema: &Arc<Schema>) -> RecordBatch {
        let arrays: Vec<ArrayRef> = schema
            .fields()
            .iter()
            .map(|field| default_value_array(field.data_type()))
            .collect();

        RecordBatch::try_new(Arc::clone(schema), arrays)
            .expect("could not build RecordBatch with one row")
    }

    #[tokio::test]
    pub async fn test_vector_scan_basic() -> Result<(), String> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("pk", DataType::Int64, false),
            Field::new("body", DataType::Utf8, false),
            Field::new("another_column", DataType::Utf8, false),
        ]));

        let p = VectorScanTableProvider {
            table_provider: Arc::new(
                MemTable::try_new(
                    Arc::clone(&schema),
                    vec![vec![one_row_default_record_batch_for_schema(&schema)]],
                )
                .expect("could not make MemTable"),
            ),
            index: Arc::new(PretendVectorIndex::new(
                "body".to_string(),
                vec![Field::new("pk", DataType::Int64, false)],
                Schema::new(vec![
                    Field::new(S3_VECTOR_PRIMARY_KEY_NAME, DataType::Utf8, false),
                    Field::new(
                        S3_VECTOR_EMBEDDING_NAME,
                        DataType::new_fixed_size_list(DataType::Float32, 10, false),
                        false,
                    ),
                ]),
            )),
        };

        let provider: Arc<dyn TableProvider> = Arc::new(p);

        test_explain(
            Arc::clone(&provider),
            TableReference::parse_str("my_vectored_table"),
            "SELECT pk, body_embedding from my_vectored_table ORDER BY pk desc LIMIT 5",
            "basic",
        )
        .await?;

        test_explain(
            Arc::clone(&provider),
            TableReference::parse_str("my_vectored_table"),
            "SELECT pk, another_column, body_embedding from my_vectored_table ORDER BY pk desc LIMIT 5",
            "join_for_projection",
        )
        .await?;

        test_explain(
            Arc::clone(&provider),
            TableReference::parse_str("my_vectored_table"),
            "SELECT pk, body_embedding from my_vectored_table WHERE another_column != 'something' ORDER BY pk desc LIMIT 5",
            "join_for_filter",
        )
        .await?;

        Ok(())
    }

    #[tokio::test]
    pub async fn test_vector_scan_index_metadata() -> Result<(), String> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("pk", DataType::Int64, false),
            Field::new("body", DataType::Utf8, false),
            Field::new("another_column", DataType::Utf8, false),
            Field::new("a_number", DataType::Int64, false),
            Field::new("not_where", DataType::Utf8, false),
        ]));
        let p = VectorScanTableProvider {
            table_provider: Arc::new(
                MemTable::try_new(
                    Arc::clone(&schema),
                    vec![vec![one_row_default_record_batch_for_schema(&schema)]],
                )
                .expect("could not make MemTable"),
            ),
            index: Arc::new(PretendVectorIndex::new(
                "body".to_string(),
                vec![Field::new("pk", DataType::Int64, false)],
                Schema::new(vec![
                    Field::new(S3_VECTOR_PRIMARY_KEY_NAME, DataType::Utf8, false),
                    Field::new(
                        S3_VECTOR_EMBEDDING_NAME,
                        DataType::new_fixed_size_list(DataType::Float32, 10, false),
                        false,
                    ),
                    Field::new("a_number", DataType::Int64, false).with_metadata(HashMap::from([
                        ("filterable".to_string(), "true".to_string()),
                    ])),
                    Field::new("not_where", DataType::Utf8, false).with_metadata(HashMap::from([
                        ("filterable".to_string(), "false".to_string()),
                    ])),
                ]),
            )),
        };
        let provider: Arc<dyn TableProvider> = Arc::new(p);

        test_explain(
            Arc::clone(&provider),
            TableReference::parse_str("my_vectored_table"),
            "SELECT pk, body_embedding from my_vectored_table ORDER BY pk desc LIMIT 5",
            "basic",
        )
        .await?;

        test_explain(
            Arc::clone(&provider),
            TableReference::parse_str("my_vectored_table"),
            "SELECT pk, another_column, body_embedding from my_vectored_table ORDER BY pk desc LIMIT 5",
            "join_for_projection",
        )
        .await?;

        test_explain(
            Arc::clone(&provider),
            TableReference::parse_str("my_vectored_table"),
            "SELECT pk, another_column, not_where, body_embedding from my_vectored_table ORDER BY pk desc LIMIT 5",
            "join_for_projection_use_metadata",
        )
        .await?;

        test_explain(
            Arc::clone(&provider),
            TableReference::parse_str("my_vectored_table"),
            "SELECT pk, body_embedding from my_vectored_table WHERE another_column != 'something' AND a_number > 0 ORDER BY pk desc LIMIT 5",
            "join_for_filter_use_metadata",
        )
        .await?;

        test_explain(
            Arc::clone(&provider),
            TableReference::parse_str("my_vectored_table"),
            "SELECT pk, not_where, body_embedding from my_vectored_table ORDER BY pk desc LIMIT 5",
            "no_join_for_metadata_projection",
        )
        .await?;

        test_explain(
            Arc::clone(&provider),
            TableReference::parse_str("my_vectored_table"),
            "SELECT pk, body_embedding from my_vectored_table WHERE a_number > 0 ORDER BY pk desc LIMIT 5",
            "no_join_for_metadata_filter",
        )
        .await?;

        Ok(())
    }
}
