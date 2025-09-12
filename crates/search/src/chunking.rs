use std::sync::Arc;

use crate::{
    SEARCH_SCORE_COLUMN_NAME,
    index::SearchIndex,
    metadata::{MetadataColumn, MetadataColumns},
};

use arrow::array::{LargeStringArray, RecordBatch, StringArray, StringViewArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use chunking::Chunker;
use datafusion::{
    catalog::TableProvider,
    datasource::{DefaultTableSource, ViewTable},
    functions_aggregate::expr_fn::first_value,
    logical_expr::{Aggregate, LogicalPlan, Projection, Sort, SortExpr, TableScan},
    prelude::col,
    sql::TableReference,
};
use itertools::Itertools;
use snafu::{ResultExt, Snafu};
use util::convert_string_arrow_to_iterator;

/// A [`SearchIndex`] that chunks the [`SearchIndex::search_column`] before each [`SearchIndex::write`].
///
/// Two new [`FieldRef`]s augment the table:
///   1. An index of the chunks position in the underlying search column. This is an additional element in [`SearchIndex::primary_fields`].
///   2. The start and end index of the chunk into the underlying search column. This is an additional [`MetadataColumn::NonFilterable`] in  [`SearchIndex::metadata_columns`].
pub struct ChunkedSearchIndex {
    inner: Arc<dyn SearchIndex>,
    chunker: Arc<dyn Chunker>,

    /// inner.metadata_columns() + chunk_offsets. Must store in struct for ref.
    metadata: MetadataColumns,
}

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Could not write to search index. Provided data does not have search column '{search_column}'. Columns present: {}.", schema
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .join(", ")
    ))]
    WriteFailedNoSearchColumn {
        search_column: String,
        schema: SchemaRef,
    },

    #[snafu(display(
        "Cannot write search column '{search_column}' into search index. Expecting string-like type, found {data_type}"
    ))]
    WriteFailedSearchColumnNoString {
        search_column: String,
        data_type: DataType,
    },
}

pub fn is_chunked(idx: &Arc<dyn SearchIndex>) -> bool {
    idx.primary_fields()
        .iter()
        .any(|f| *f == Field::new("_spice.chunk_id", DataType::UInt64, false))
}

impl std::fmt::Debug for ChunkedSearchIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChunkedSearchIndex")
            .field("inner", &self.inner)
            .finish_non_exhaustive()
    }
}

impl ChunkedSearchIndex {
    pub fn new(inner: Arc<dyn SearchIndex>, chunker: Arc<dyn Chunker>) -> Self {
        let mut metadata: Vec<MetadataColumn> =
            inner.metadata_columns().clone().into_iter().collect();
        metadata.push(MetadataColumn::NonFilterable(
            Field::new(
                "_spice.chunk_offset",
                DataType::FixedSizeList(Field::new("item", DataType::Int32, false).into(), 2),
                false,
            )
            .into(),
        ));

        Self {
            inner,
            chunker,
            metadata: metadata.into(),
        }
    }
}

#[async_trait]
impl SearchIndex for ChunkedSearchIndex {
    fn search_column(&self) -> String {
        // TODO: this might need a separate name?
        self.inner.search_column()
    }

    fn primary_fields(&self) -> Vec<Field> {
        // we might not need '_spice.chunk_id'.
        [
            self.inner.primary_fields(),
            vec![], // vec![Field::new("_spice.chunk_id", DataType::UInt64, false)],
        ]
        .concat()
    }

    fn metadata_columns(&self) -> &MetadataColumns {
        &self.metadata
    }

    async fn write(
        &self,
        record: RecordBatch,
    ) -> Result<RecordBatch, Box<dyn std::error::Error + Send + Sync>> {
        let Some(arr) = record.column_by_name(self.search_column().as_str()) else {
            return WriteFailedNoSearchColumnSnafu {
                search_column: self.search_column(),
                schema: record.schema(),
            }
            .fail()
            .boxed();
        };

        let Some(arr_str) = convert_string_arrow_to_iterator!(arr) else {
            return WriteFailedSearchColumnNoStringSnafu {
                search_column: self.search_column(),
                data_type: arr.data_type().clone(),
            }
            .fail()
            .boxed();
        };

        // For each element of the search column, chunk and keep offsets
        let (offsets, chunks): (Vec<Vec<(usize, usize)>>, Vec<Vec<_>>) = arr_str
            .map(|s_opt| {
                if let Some(s) = s_opt {
                    self.chunker
                        .chunk_with_offsets(s)
                        .collect::<Vec<_>>()
                        .into_iter()
                        .unzip::<_, _, Vec<(usize, usize)>, Vec<&str>>()
                } else {
                    (vec![], vec![])
                }
            })
            .collect::<Vec<_>>()
            .into_iter()
            .unzip();

        // Now I need to expand out all the other ArrayRefs in RecordBatch. Somehow.

        Ok(record)
    }

    async fn query_table_provider(
        &self,
        query: &str,
    ) -> Result<Arc<dyn TableProvider>, Box<dyn std::error::Error + Send + Sync>> {
        let pk_expr: Vec<_> = self
            .inner
            .primary_fields()
            .iter()
            .map(|f| col(f.name().clone()))
            .collect();

        let tbl_prov = self.inner.query_table_provider(query).await?;
        let schema = tbl_prov.schema();

        let tbl = Arc::new(LogicalPlan::TableScan(TableScan::try_new(
            TableReference::parse_str("tbl"),
            Arc::new(DefaultTableSource::new(tbl_prov)),
            None,
            vec![],
            None,
        )?));

        let pk_order_by: Vec<SortExpr> = pk_expr
            .iter()
            .map(|e| SortExpr::new(e.clone(), false, false))
            .collect();
        let mut order_by = pk_order_by.clone();
        order_by.push(SortExpr::new(col(SEARCH_SCORE_COLUMN_NAME), false, false));

        let aggr_expr: Vec<_> = schema
            .fields()
            .iter()
            .map(|f| first_value(col(f.name()), Some(order_by.clone())))
            .collect();

        let agg =
            LogicalPlan::Aggregate(Aggregate::try_new(tbl, pk_expr.clone(), aggr_expr.clone())?);
        let sort = LogicalPlan::Sort(Sort {
            expr: pk_order_by,
            input: agg.into(),
            fetch: Some(1), // Only return most relevant chunk from each ID
        });

        let proj = LogicalPlan::Projection(Projection::try_new(aggr_expr, sort.into())?);
        let sort = LogicalPlan::Sort(Sort {
            expr: vec![SortExpr::new(col(SEARCH_SCORE_COLUMN_NAME), false, false)],
            input: proj.into(),
            fetch: None,
        });

        Ok(Arc::new(ViewTable::new(sort, None)))
    }
}
