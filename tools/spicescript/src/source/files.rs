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

//! A directory of data files. Each instance has its own directory, exposed
//! as `${sources.<name>.path}`; a generated table is the directory
//! `<path>/<table>/` of part files. It cannot answer queries itself: a script
//! mirrors the generated rows into a database for that.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use serde_json::json;

use super::{Source, TextResult};
use crate::{
    docker::Container,
    generate::{Dialect, FileFormat, TableSpec, WriteMode},
};

/// Rows per written batch.
const BATCH_ROWS: usize = 65_536;

pub struct Files {
    root: PathBuf,
}

impl Files {
    pub fn new(root: PathBuf) -> Result<Self> {
        std::fs::create_dir_all(&root)?;
        Ok(Self { root })
    }

    fn dir(&self, namespace: &str) -> PathBuf {
        self.root.join(namespace)
    }
}

#[async_trait]
impl Source for Files {
    fn container(&self) -> Option<&Container> {
        None
    }

    async fn create_namespace(&self, namespace: &str) -> Result<super::Namespace> {
        let dir = self.dir(namespace);
        std::fs::create_dir_all(&dir)?;
        let mut extra = serde_json::Map::new();
        extra.insert("path".into(), json!(dir.display().to_string()));
        Ok(super::Namespace {
            params: json!({}),
            extra,
        })
    }

    async fn exec(&self, _namespace: &str, sql: &str) -> Result<()> {
        bail!("a files source runs no SQL (`{sql}`); generate or remove files instead")
    }

    async fn query(&self, _namespace: &str, sql: &str) -> Result<TextResult> {
        bail!("a files source answers no queries (`{sql}`); mirror its rows into a database")
    }

    fn dialect(&self) -> Dialect {
        Dialect::Postgres
    }

    async fn load(
        &self,
        _namespace: &str,
        _table: &str,
        _columns: &[String],
        _rows: &mut (dyn Iterator<Item = Vec<Option<String>>> + Send),
    ) -> Result<u64> {
        bail!("a files source is written with `generate`")
    }

    async fn list(&self, namespace: &str, prefix: &str) -> Result<Vec<String>> {
        let root = self.dir(namespace).join(prefix);
        let mut found = Vec::new();
        let mut pending = vec![root];
        while let Some(dir) = pending.pop() {
            let Ok(entries) = std::fs::read_dir(&dir) else {
                continue;
            };
            for entry in entries {
                let path = entry?.path();
                if path.is_dir() {
                    pending.push(path);
                } else {
                    found.push(path.display().to_string());
                }
            }
        }
        Ok(found)
    }

    /// Writes the rows as `files` part files, numbered after any already
    /// there when appending.
    async fn generate(&self, namespace: &str, spec: &TableSpec) -> Result<u64> {
        let dir = self.dir(namespace).join(&spec.table);
        if spec.mode == WriteMode::Replace && dir.exists() {
            std::fs::remove_dir_all(&dir)?;
        }
        std::fs::create_dir_all(&dir)?;
        let first = std::fs::read_dir(&dir)?.count();
        let spec = spec.clone();
        // Encoding is CPU-bound; keep it off the async workers.
        tokio::task::spawn_blocking(move || write_parts(&dir, first, &spec))
            .await
            .context("file writer panicked")?
    }
}

pub(crate) fn write_parts(dir: &Path, first: usize, spec: &TableSpec) -> Result<u64> {
    let files = u64::from(spec.files.max(1));
    let per_file = spec.rows.div_ceil(files);
    let mut rows = crate::generate::rows(spec)?;
    let mut written = 0;
    for part in 0..files {
        let extension = match spec.format {
            FileFormat::Parquet => "parquet",
            FileFormat::Csv => "csv",
        };
        #[expect(clippy::cast_possible_truncation, reason = "part counts are small")]
        let path = dir.join(format!("part-{:05}.{extension}", first + part as usize));
        let file = std::fs::File::create(&path)
            .with_context(|| format!("Failed to create {}", path.display()))?;
        let mut writer = Writer::new(spec.format, file, spec)?;
        let mut left = per_file.min(spec.rows - written);
        while left > 0 {
            #[expect(clippy::cast_possible_truncation, reason = "bounded by BATCH_ROWS")]
            let take = left.min(BATCH_ROWS as u64) as usize;
            let chunk: Vec<_> = rows.by_ref().take(take).collect();
            writer.write(&crate::generate::to_batch(&spec.columns, &chunk)?)?;
            left -= take as u64;
            written += take as u64;
        }
        writer.close()?;
    }
    Ok(written)
}

enum Writer {
    Parquet(parquet::arrow::ArrowWriter<std::fs::File>),
    Csv(arrow::csv::Writer<std::fs::File>),
}

impl Writer {
    fn new(format: FileFormat, file: std::fs::File, spec: &TableSpec) -> Result<Self> {
        Ok(match format {
            FileFormat::Parquet => {
                let schema = crate::generate::to_batch(&spec.columns, &[])?.schema();
                Self::Parquet(parquet::arrow::ArrowWriter::try_new(file, schema, None)?)
            }
            FileFormat::Csv => Self::Csv(
                arrow::csv::WriterBuilder::new()
                    .with_header(true)
                    .build(file),
            ),
        })
    }

    fn write(&mut self, batch: &arrow::array::RecordBatch) -> Result<()> {
        match self {
            Self::Parquet(w) => w.write(batch)?,
            Self::Csv(w) => w.write(batch)?,
        }
        Ok(())
    }

    fn close(self) -> Result<()> {
        match self {
            Self::Parquet(w) => {
                w.close()?;
            }
            Self::Csv(_) => {}
        }
        Ok(())
    }
}
