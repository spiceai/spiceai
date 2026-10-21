/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! Keeps a source schema wider than [`MAX_SCHEMA_COLUMNS`] out of the runtime.

use std::sync::Arc;

pub use arrow_tools::column_limit::{MAX_SCHEMA_COLUMNS, TooManyColumns};
use datafusion::datasource::TableProvider;
use runtime_component::dataset::DatasetSpec;

use crate::{ConnectorComponent, ConnectorContext, DataConnector, DataConnectorError};

/// The load failure for `dataset` when its source reports too many columns.
#[must_use]
pub fn too_many_columns_error(dataset: &DatasetSpec, err: TooManyColumns) -> DataConnectorError {
    DataConnectorError::TooManyColumns {
        dataconnector: dataset.source().to_string(),
        connector_component: ConnectorComponent::from(dataset),
        source: Box::new(err),
    }
}

/// Fails when a source schema of `columns` top-level columns is too wide for `dataset`.
///
/// # Errors
///
/// Returns [`DataConnectorError::TooManyColumns`] when `columns` exceeds
/// [`MAX_SCHEMA_COLUMNS`].
pub fn ensure_within_column_limit(
    dataset: &DatasetSpec,
    columns: usize,
) -> Result<(), DataConnectorError> {
    arrow_tools::column_limit::ensure_within_column_limit(columns)
        .map_err(|err| too_many_columns_error(dataset, err))
}

/// Resolves `dataset`'s source table through `connector`, refusing one wider than
/// [`MAX_SCHEMA_COLUMNS`].
///
/// Every path that registers or re-resolves a dataset's source table goes through
/// this rather than calling [`DataConnector::read_provider`] directly, so a table too
/// wide to plan in bounded time is never registered, whichever connector reported it.
///
/// # Errors
///
/// Returns the connector's own error, or [`DataConnectorError::TooManyColumns`] when
/// the resolved table is too wide.
pub async fn read_provider_within_column_limit(
    connector: &dyn DataConnector,
    context: &dyn ConnectorContext,
    dataset: &DatasetSpec,
) -> Result<Arc<dyn TableProvider>, DataConnectorError> {
    let provider = connector.read_provider(context, dataset).await?;
    ensure_within_column_limit(dataset, provider.schema().fields().len())?;
    Ok(provider)
}
