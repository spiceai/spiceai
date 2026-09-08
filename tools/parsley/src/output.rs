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
use serde::{Deserialize, Serialize};

#[derive(Serialize)]
pub struct StructuredOutput<'a> {
    path: String,
    content: OutputContent<'a>,
}

impl<'a> StructuredOutput<'a> {
    pub fn from_chunks(path: impl Into<String>, chunks: Vec<&'a str>) -> Self {
        StructuredOutput {
            path: path.into(),
            content: OutputContent::Chunks(chunks),
        }
    }

    pub fn from_content(path: impl Into<String>, content: &'a str) -> Self {
        StructuredOutput {
            path: path.into(),
            content: OutputContent::Full(content),
        }
    }
}

impl std::fmt::Display for StructuredOutput<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("---\nfile: {}\n---\n", self.path))?;
        match &self.content {
            OutputContent::Chunks(chnks) => {
                for c in chnks {
                    f.write_fmt(format_args!("{c}---\n"))?;
                }
            }
            OutputContent::Full(content) => f.write_fmt(format_args!("{content}---\n"))?,
        }
        Ok(())
    }
}

#[derive(Deserialize, Serialize)]
#[serde(untagged)]
pub enum OutputContent<'a> {
    Full(&'a str),
    Chunks(Vec<&'a str>),
}
