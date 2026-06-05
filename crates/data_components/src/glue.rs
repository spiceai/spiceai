/*
Copyright 2026 The Spice.ai OSS Authors

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

//! Shared AWS Glue types used by both `connector-glue` and the runtime's Glue catalog connector.

use aws_sdk_glue::types::Table;
use snafu::prelude::*;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "The input format {input_format} for table '{table}' is not supported. For help, visit: https://docs.spiceai.org/components/data-connectors/glue"
    ))]
    InvalidInputFormat { input_format: String, table: String },

    #[snafu(display(
        "No storage descriptor found for table '{table}'. Ensure the table is correctly configured in AWS Glue. For help, visit: https://docs.spiceai.org/components/data-connectors/glue"
    ))]
    MissingStorageDescriptor { table: String },

    #[snafu(display(
        "No input format specified for table '{table}'. Ensure the table is correctly configured in AWS Glue. For help, visit: https://docs.spiceai.org/components/data-connectors/glue"
    ))]
    MissingInputFormat { table: String },
}

/// The storage format of an AWS Glue table, as detected from the table's input format string.
#[derive(Debug, PartialEq, Clone, Copy)]
pub enum InputFormat {
    Csv,
    Parquet,
    Iceberg,
}

impl InputFormat {
    /// Returns the file format name for use as a parameter value.
    #[must_use]
    pub fn file_format(self) -> &'static str {
        match self {
            InputFormat::Csv => "csv",
            InputFormat::Parquet => "parquet",
            InputFormat::Iceberg => "iceberg",
        }
    }
}

impl TryFrom<&Table> for InputFormat {
    type Error = Error;

    fn try_from(table: &Table) -> Result<Self, Self::Error> {
        if table
            .parameters
            .as_ref()
            .and_then(|params| params.get("table_type"))
            .is_some_and(|value| value.to_lowercase() == "iceberg")
        {
            return Ok(Self::Iceberg);
        }

        let Some(storage_descriptor) = table.storage_descriptor() else {
            return Err(Error::MissingStorageDescriptor {
                table: table.name().to_string(),
            });
        };

        let Some(input_format) = storage_descriptor.input_format() else {
            return Err(Error::MissingInputFormat {
                table: table.name().to_string(),
            });
        };

        Ok(match input_format {
            "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat" => Self::Parquet,
            "org.apache.hadoop.mapred.TextInputFormat" => Self::Csv,
            input_format => {
                return Err(Error::InvalidInputFormat {
                    input_format: input_format.to_string(),
                    table: table.name().to_string(),
                });
            }
        })
    }
}
