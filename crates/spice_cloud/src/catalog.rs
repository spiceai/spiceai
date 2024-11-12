/*
Copyright 2024 The Spice.ai OSS Authors

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

use datafusion::{
    arrow::{self, datatypes::TimeUnit},
    catalog::{CatalogProvider, SchemaProvider},
};
use globset::GlobSet;
use runtime::{
    component::dataset::Dataset,
    dataconnector::{DataConnector, DataConnectorError},
};
use serde::Deserialize;
use snafu::prelude::*;
use std::{any::Any, collections::HashMap, sync::Arc};

use crate::{schema::SpiceAISchemaProvider, SpiceExtension};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to retrieve available datasets from Spice AI: {source}"))]
    UnableToRetrieveDatasets { source: super::Error },

    #[snafu(display("{source}"))]
    UnableToCreateSpiceDataset { source: runtime::Error },

    #[snafu(display("{source}"))]
    UnableToCreateSchema { source: DataConnectorError },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug)]
pub struct SpiceAICatalogProvider {
    schemas: HashMap<String, Arc<dyn SchemaProvider>>,
}

#[derive(Debug, Deserialize)]
struct DatasetSchemaResponse {
    name: String,
    fields: Vec<Field>,
}

#[derive(Debug, Deserialize)]
struct Field {
    name: String,
    r#type: String,
}

/// Converts a Spice AI field to an Arrow field.
///
/// The schema returned by the /v1/schemas API doesn't contain the full type information,
/// so we make some guesses (i.e. decimal/list types).
///
/// Once we support `FlightSQL` on the Spice.ai side, this can be removed.
fn field_to_arrow(field: &Field) -> arrow::datatypes::Field {
    let data_type = match field.r#type.to_ascii_lowercase().as_str() {
        "bigint" => arrow::datatypes::DataType::Int64,
        "varchar" => arrow::datatypes::DataType::Utf8,
        "list" => arrow::datatypes::DataType::List(Arc::new(arrow::datatypes::Field::new(
            "item",
            arrow::datatypes::DataType::Utf8,
            false,
        ))),
        "decimal" => arrow::datatypes::DataType::Decimal128(38, 9),
        "boolean" => arrow::datatypes::DataType::Boolean,
        "integer" => arrow::datatypes::DataType::Int32,
        "double" => arrow::datatypes::DataType::Float64,
        "date" => arrow::datatypes::DataType::Date32,
        "timestamp" => arrow::datatypes::DataType::Timestamp(TimeUnit::Millisecond, None),
        _ => panic!("Unsupported data type: {}", field.r#type),
    };

    arrow::datatypes::Field::new(field.name.clone(), data_type, true)
}

impl SpiceAICatalogProvider {
    /// Creates a new instance of the [`SpiceAICatalogProvider`].
    ///
    /// # Errors
    ///
    /// Returns an error if the list of available datasets cannot be retrieved from the Spice AI platform.
    pub async fn try_new(
        extension: &SpiceExtension,
        data_connector: Arc<dyn DataConnector>,
        filters: Option<GlobSet>,
    ) -> Result<Self> {
        let datasets: Vec<DatasetSchemaResponse> = extension
            .get_json("/v1/schemas")
            .await
            .context(UnableToRetrieveDatasetsSnafu)?;

        tracing::debug!("Found {} Spice AI datasets", datasets.len());

        let parsed_schemas: HashMap<String, HashMap<String, Dataset>> =
            datasets.iter().fold(HashMap::new(), |mut acc, ds| {
                let Some(schema_name) = get_schema_name(&ds.name) else {
                    return acc;
                };
                let Some(table_name) = get_normalized_table_name(&ds.name) else {
                    return acc;
                };

                let dataset_name = format!("{schema_name}.{table_name}");
                if let Some(filters) = &filters {
                    if !filters.is_match(&dataset_name) {
                        return acc;
                    }
                }

                let Ok(dataset) = Dataset::try_new(format!("spice.ai:{}", &ds.name), &dataset_name)
                else {
                    return acc;
                };

                let arrow_fields: Vec<_> = ds.fields.iter().map(field_to_arrow).collect();
                let arrow_schema = arrow::datatypes::Schema::new(arrow_fields);
                let dataset = dataset.with_schema(Arc::new(arrow_schema));

                let schema = acc.entry(schema_name.to_string()).or_default();
                schema.insert(table_name.to_string(), dataset);

                acc
            });

        let mut schemas = HashMap::new();
        for (schema_name, schema_contents) in parsed_schemas {
            let schema_provider = SpiceAISchemaProvider::try_new(
                &schema_name,
                Arc::clone(&data_connector),
                schema_contents,
            )
            .await
            .context(UnableToCreateSchemaSnafu)?;
            let schema_provider = Arc::new(schema_provider) as Arc<dyn SchemaProvider>;
            schemas.insert(schema_name, schema_provider);
        }

        Ok(Self { schemas })
    }
}

impl CatalogProvider for SpiceAICatalogProvider {
    /// Returns the catalog provider as [`Any`]
    /// so that it can be downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Retrieves the list of available schema names in this catalog.
    fn schema_names(&self) -> Vec<String> {
        self.schemas.keys().cloned().collect()
    }

    /// Retrieves a specific schema from the catalog by name, provided it exists.
    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        self.schemas.get(name).cloned()
    }
}

/// Returns the schema name from a Spice AI dataset path.
fn get_schema_name(name: &str) -> Option<&str> {
    name.split('.').next()
}

/// Returns the normalized table name from a Spice AI dataset path.
///
/// The normalization will take all characters after the first dot and replace any further dots with underscores
fn get_normalized_table_name(name: &str) -> Option<String> {
    if let Some(pos) = name.find('.') {
        let suffix = &name[pos + 1..];
        let normalized: String = suffix
            .chars()
            .map(|c| if c == '.' { '_' } else { c })
            .collect();
        Some(normalized)
    } else {
        None
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_normalized_table_name() {
        struct TestCase<'a> {
            input: &'a str,
            expected: Option<&'a str>,
        }

        let test_cases = [
            TestCase {
                input: "nodots",
                expected: None,
            },
            TestCase {
                input: "single.dot",
                expected: Some("dot"),
            },
            TestCase {
                input: "multiple.dots.in.path",
                expected: Some("dots_in_path"),
            },
            TestCase {
                input: "trailing.dot.",
                expected: Some("dot_"),
            },
            TestCase {
                input: ".dot.at.start",
                expected: Some("dot_at_start"),
            },
            TestCase {
                input: ".",
                expected: Some(""),
            },
        ];

        for case in test_cases {
            let result = get_normalized_table_name(case.input);
            assert_eq!(result.as_deref(), case.expected, "input: {}", case.input);
        }
    }
}
