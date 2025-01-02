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

#![allow(clippy::missing_errors_doc)]

use bytes::Bytes;
use snafu::prelude::*;
use std::{
    any::Any,
    collections::HashMap,
    fmt::Display,
    sync::{Arc, LazyLock},
};
use tokio::sync::Mutex;

mod docx;
mod pdf;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Internal parsing {format} document. Error: {source:?}"))]
    InternalParsingError {
        format: DocumentType,
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

static DOCUMENT_PARSER_FACTORY_REGISTRY: LazyLock<
    Mutex<HashMap<String, Arc<dyn DocumentParserFactory>>>,
> = LazyLock::new(|| Mutex::new(HashMap::new()));

pub async fn register_all() {
    register_parser_factory("docx", Arc::new(docx::DocxParserFactory {})).await;
    register_parser_factory("pdf", Arc::new(pdf::PdfParserFactory {})).await;
}

pub async fn get_parser_factory(ext: &str) -> Option<Arc<dyn DocumentParserFactory>> {
    let registry = DOCUMENT_PARSER_FACTORY_REGISTRY.lock().await;
    registry.get(ext.strip_prefix('.').unwrap_or(ext)).cloned()
}

pub async fn register_parser_factory(
    name: &str,
    connector_factory: Arc<dyn DocumentParserFactory>,
) {
    let mut registry = DOCUMENT_PARSER_FACTORY_REGISTRY.lock().await;
    registry.insert(name.to_string(), connector_factory);
}

pub trait DocumentParserFactory: Send + Sync {
    fn create(&self, parser_options: &HashMap<String, String>) -> Result<Arc<dyn DocumentParser>>;

    /// Initialize a [`DocumentParser`] with all options set to default values
    fn default(&self) -> Arc<dyn DocumentParser>;

    /// Returns the table source as [`Any`] so that it can be
    /// downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any;
}

pub trait DocumentParser: Send + Sync {
    fn parse(&self, raw: &Bytes) -> Result<Arc<dyn Document>>;
}

pub trait Document {
    fn as_flat_utf8(&self) -> Result<String>;
    fn type_(&self) -> DocumentType;
}

#[derive(Debug, PartialEq, Eq)]
pub enum DocumentType {
    Pdf,
    Docx,
}

impl Display for DocumentType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DocumentType::Pdf => write!(f, "PDF"),
            DocumentType::Docx => write!(f, "DOCX"),
        }
    }
}
