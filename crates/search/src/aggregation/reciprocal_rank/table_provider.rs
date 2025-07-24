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

use std::{any::Any, sync::Arc};

use arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion::{
    catalog::{Session, TableProvider},
    datasource::TableType,
    error::DataFusionError,
    physical_plan::{DisplayAs, ExecutionPlan},
    prelude::Expr,
};

use crate::SEARCH_SCORE_COLUMN_NAME;

/// [`ReciprocalRankFusionProvider`] is a [`TableProvider`] equivalent of [`super::ReciprocalRankFusion`].
///
/// Each [`TableProvider`] of `ranked_retrieved` is expected to have
///   1. All columns of `primary_key`
///   2. An additional column `SEARCH_SCORE_COLUMN_NAME` of type [`arrow::array::Float64Array`].
#[derive(Debug)]
pub struct ReciprocalRankFusionProvider {
    primary_key: Vec<Field>,
    ranked_retrieved: Vec<Arc<dyn TableProvider>>,
    offset: usize,
}

impl ReciprocalRankFusionProvider {
    pub fn try_new(
        primary_key: Vec<Field>,
        ranked_retrieved: Vec<Arc<dyn TableProvider>>,
        offset: usize,
    ) -> Result<Self, DataFusionError> {
        let primary_key_schema = Schema::new(primary_key.clone());
        for (i, input) in ranked_retrieved.iter().enumerate() {
            let schema = input.schema();
            if !schema.contains(&primary_key_schema) {
                return Err(DataFusionError::Plan(format!(
                    "{i}th input to reciprocal rank fusion does not have required primary key fields. Primary key fields: {primary_key:?}. Input schema: {schema}",
                )));
            }
            let Some((_, score_field)) = schema.column_with_name(SEARCH_SCORE_COLUMN_NAME) else {
                return Err(DataFusionError::Plan(format!(
                    "{i}th input to reciprocal rank fusion does not have a numeric {SEARCH_SCORE_COLUMN_NAME} column. Input schema: {schema}"
                )));
            };
            // Doesn't have to be Float64, but any numeric can be cast to it.
            if !score_field.data_type().is_numeric() {
                return Err(DataFusionError::Plan(format!(
                    "{i}th input to reciprocal rank fusion has a non-numeric {SEARCH_SCORE_COLUMN_NAME} column. Data type {}",
                    score_field.data_type()
                )));
            }
        }
        Ok(Self {
            primary_key,
            ranked_retrieved,
            offset,
        })
    }
}

#[async_trait::async_trait]
impl TableProvider for ReciprocalRankFusionProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Schema::empty().into()
    }
    fn table_type(&self) -> TableType {
        TableType::View
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        Err(DataFusionError::NotImplemented(format!("blame jack")))
    }
}
