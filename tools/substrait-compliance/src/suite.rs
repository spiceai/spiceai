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

use std::path::{Path, PathBuf};

use serde::Deserialize;
use snafu::ResultExt;

use crate::compare::{TableData, load_expected_csv};
use crate::error::{self, Result};

#[derive(Debug, Deserialize)]
struct SuiteDefinition {
    name: String,
    version: String,
    #[serde(default)]
    description: String,
    #[serde(rename = "testCases", default)]
    test_cases: Vec<TestCaseDefinition>,
}

#[derive(Debug, Deserialize)]
struct TestCaseDefinition {
    id: String,
    #[serde(default)]
    description: String,
    #[serde(rename = "planBinary")]
    plan_binary: String,
    #[serde(rename = "expectedOutput")]
    expected_output: Option<String>,
}

#[derive(Debug, Clone)]
pub struct TestCase {
    pub id: String,
    pub description: String,
    pub plan_bytes: Vec<u8>,
    pub expected_output: Option<TableData>,
}

#[derive(Debug, Clone)]
pub struct Suite {
    pub name: String,
    pub version: String,
    pub description: String,
    pub data_dir: PathBuf,
    pub cases: Vec<TestCase>,
}

impl Suite {
    pub fn load(suite_dir: &Path) -> Result<Self> {
        let metadata_path = suite_dir.join("metadata.yaml");
        if !metadata_path.exists() {
            return error::MissingSuitePathSnafu {
                path: metadata_path.display().to_string(),
            }
            .fail();
        }
        let text = std::fs::read_to_string(&metadata_path).context(error::IoSnafu {
            path: metadata_path.display().to_string(),
        })?;
        let def: SuiteDefinition = serde_yaml::from_str(&text).context(error::YamlSnafu {
            path: metadata_path.display().to_string(),
        })?;

        let mut cases = Vec::with_capacity(def.test_cases.len());
        for tc in def.test_cases {
            let plan_path = suite_dir.join(&tc.plan_binary);
            let plan_bytes = std::fs::read(&plan_path).context(error::IoSnafu {
                path: plan_path.display().to_string(),
            })?;

            let expected_path = match &tc.expected_output {
                Some(rel) => suite_dir.join(rel),
                None => suite_dir.join("expected").join(format!("{}.csv", tc.id)),
            };
            let expected_output = if expected_path.exists() {
                let csv = std::fs::read_to_string(&expected_path).context(error::IoSnafu {
                    path: expected_path.display().to_string(),
                })?;
                Some(load_expected_csv(&csv).map_err(|msg| error::Error::Io {
                    path: expected_path.display().to_string(),
                    source: std::io::Error::new(std::io::ErrorKind::InvalidData, msg),
                })?)
            } else {
                None
            };

            cases.push(TestCase {
                id: tc.id,
                description: tc.description,
                plan_bytes,
                expected_output,
            });
        }

        Ok(Self {
            name: def.name,
            version: def.version,
            description: def.description,
            data_dir: suite_dir.join("data"),
            cases,
        })
    }
}
