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

use std::fmt::{Debug, Display, Formatter, Result};

pub mod onnx;

/// A `ModelFormat` specifies the supported format of a model artifacts.
///
/// Currently, only `onnx` is supported.
#[derive(Clone)]
pub enum ModelFormat {
    Onnx(onnx::Onnx),
}
impl Debug for ModelFormat {
    fn fmt(&self, f: &mut Formatter) -> Result {
        match self {
            ModelFormat::Onnx(_) => write!(f, "onnx"),
        }
    }
}

impl Display for ModelFormat {
    fn fmt(&self, f: &mut Formatter) -> Result {
        match self {
            ModelFormat::Onnx(_) => write!(f, "onnx"),
        }
    }
}

impl PartialEq for ModelFormat {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (ModelFormat::Onnx(_), ModelFormat::Onnx(_)) => true,
        }
    }
}

/// Determines the most appropriate `ModelFormat` given a local file path.
#[must_use]
pub fn from_path(path: &str) -> Option<ModelFormat> {
    if std::path::Path::new(path)
        .extension()
        .is_some_and(|ext| ext.eq_ignore_ascii_case("onnx"))
    {
        return Some(ModelFormat::Onnx(onnx::Onnx {}));
    }
    None
}
