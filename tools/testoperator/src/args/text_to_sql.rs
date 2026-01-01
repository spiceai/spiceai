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

use super::CommonArgs;
use clap::{Parser, ValueEnum};
use itertools::Itertools;
use serde::{Deserialize, Serialize};

use std::{fs, path::PathBuf};
use test_framework::{
    anyhow,
    spicetest::text_to_sql::{TextToSqlConfig, TextToSqlRequest},
};

#[derive(Parser, Debug, Clone)]
pub struct TextToSqlArgs {
    #[clap(flatten)]
    pub(crate) common: CommonArgs,

    /// The language model (named in spicepod) to perform text-to-sql
    #[arg(long)]
    pub(crate) model: String,

    /// File path to a JSONL of test questions and expected SQL
    #[arg(long, conflicts_with = "queryset")]
    pub(crate) queryset_file: Option<PathBuf>,

    /// Inline JSON array of test questions and expected SQL
    #[arg(long, conflicts_with = "queryset_file")]
    pub(crate) queryset: Option<String>,

    /// Whether to use the `sample_data_enabled` HTTP parameter in the v1/nsql endpoint
    #[arg(long, default_value = "both")]
    pub(crate) sample_data_enabled: SampleDataOption,

    /// Whether to use the Accept: application/sql HTTP header in the v1/nsql endpoint
    #[arg(long, default_value = "both")]
    pub(crate) return_sql: ReturnSqlOption,
}

impl TextToSqlArgs {
    pub(crate) fn load_queries(&self) -> Result<Vec<TextToSqlQuery>, anyhow::Error> {
        if let Some(file_path) = &self.queryset_file {
            fs::read_to_string(file_path)?
                .lines()
                .filter(|line| !line.trim().is_empty())
                .map(|s| serde_json::from_str(s).map_err(anyhow::Error::new))
                .collect::<Result<Vec<TextToSqlQuery>, anyhow::Error>>()
        } else if let Some(queryset) = &self.queryset {
            serde_json::from_str(queryset).map_err(anyhow::Error::new)
        } else {
            Ok(vec![])
        }
    }

    /// Loads queries based on flags and generates independent [`TextToSqlRequests`].
    pub fn construct_requests(&self) -> Result<TextToSqlConfig, anyhow::Error> {
        Ok(TextToSqlConfig::new(
            self.load_queries()?
                .into_iter()
                .cartesian_product(self.sample_data_enabled.values())
                .cartesian_product(self.return_sql.values())
                .map(
                    |(
                        (
                            TextToSqlQuery {
                                question,
                                expected_sql,
                            },
                            sample_data,
                        ),
                        return_sql,
                    )| {
                        TextToSqlRequest::new(
                        format!(
                            "sample_data={sample_data},return_sql={return_sql},question={question}",
                        ),
                        question,
                        expected_sql,
                        self.model.clone(),
                    )
                    .with_sample_data_enabled(sample_data)
                    .with_return_sql(return_sql)
                    },
                )
                .collect(),
        ))
    }
}

#[derive(Debug, Clone, Copy, ValueEnum, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum SampleDataOption {
    True,
    False,

    #[default]
    Both,
}

impl SampleDataOption {
    pub fn values(self) -> Vec<bool> {
        match self {
            SampleDataOption::True => vec![true],
            SampleDataOption::False => vec![false],
            SampleDataOption::Both => vec![true, false],
        }
    }
}

#[derive(Debug, Clone, Copy, ValueEnum, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum ReturnSqlOption {
    True,
    False,

    #[default]
    Both,
}

impl ReturnSqlOption {
    pub fn values(self) -> Vec<bool> {
        match self {
            ReturnSqlOption::True => vec![true],
            ReturnSqlOption::False => vec![false],
            ReturnSqlOption::Both => vec![true, false],
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TextToSqlQuery {
    pub question: String,

    #[serde(rename = "sql")]
    pub expected_sql: String,
}
