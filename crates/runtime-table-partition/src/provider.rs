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

use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion::{
    catalog::{Session, TableProvider},
    common::{Constraints, DFSchema},
    datasource::TableType,
    error::DataFusionError,
    physical_plan::ExecutionPlan,
    prelude::Expr,
};
use snafu::prelude::*;

use crate::{creator::PartitionCreator, expression::validate_scalar_compatibility};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Only a single 'partition_by' expression is supported, but {num_partition_by} were given."
    ))]
    PartitionByViolation { num_partition_by: usize },
    #[snafu(display("Creating partition failed: {source}"))]
    CreatingPartition { source: super::creator::Error },
    #[snafu(display("Validating expressions failed: {source}"))]
    ValidatingExpressions { source: super::expression::Error },
    #[snafu(display("{source}"))]
    DataFusion { source: DataFusionError },
}

type ScalarValueString = String;

#[derive(Debug)]
pub struct PartitionTableProvider {
    _creator: Arc<dyn PartitionCreator>,
    _partition_by: Vec<Expr>,
    _partitions: HashMap<ScalarValueString, Arc<dyn TableProvider>>,
    schema: SchemaRef,
}

impl PartitionTableProvider {
    /// Create a new [`PartitionTableProvider`] and attempt to infer existing
    /// `partitions` using the specified `creator`.
    ///
    /// # Errors
    /// Returns an error if partition inferencing fails.
    pub async fn new(
        creator: Arc<dyn PartitionCreator>,
        partition_by: Vec<Expr>,
        schema: SchemaRef,
    ) -> Result<Self, Error> {
        let num_partition_by = partition_by.len();
        let expr = partition_by
            .first()
            .context(PartitionByViolationSnafu { num_partition_by })?;

        let df_schema = DFSchema::try_from(Arc::clone(&schema)).context(DataFusionSnafu)?;

        let partitions = creator
            .infer_existing_partitions()
            .await
            .context(CreatingPartitionSnafu)?
            .into_iter()
            .map(|p| {
                validate_scalar_compatibility(expr, &p.partition_value, &df_schema)?;
                Ok((p.partition_value.to_string(), p.table_provider))
            })
            .collect::<Result<HashMap<_, _>, _>>()
            .context(ValidatingExpressionsSnafu)?;

        Ok(Self {
            _creator: creator,
            _partition_by: partition_by,
            _partitions: partitions,
            schema,
        })
    }
}

#[async_trait]
impl TableProvider for PartitionTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn constraints(&self) -> Option<&Constraints> {
        None
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        todo!()
    }
}
