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

use std::{
    collections::HashMap,
    fmt::{self, Display},
};

use super::WithDependsOn;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Llm {
    pub from: String,
    pub name: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub params: Option<HashMap<String, String>>,

    #[serde(skip_serializing_if = "Vec::is_empty")]
    #[serde(rename = "dependsOn", default)]
    pub depends_on: Vec<String>,
}

impl WithDependsOn<Llm> for Llm {
    fn depends_on(&self, depends_on: &[String]) -> Llm {
        Llm {
            from: self.from.clone(),
            name: self.name.clone(),
            depends_on: depends_on.to_vec(),
            params: self.params.clone(),
        }
    }
}

impl Llm {
    #[must_use]
    pub fn get_prefix(&self) -> Option<LlmPrefix> {
        LlmPrefix::try_from(self.from.as_str()).ok()
    }

    #[must_use]
    pub fn get_model_id(&self) -> Option<String> {
        match self.get_prefix() {
            Some(p) => self
                .from
                .strip_prefix(&format!("{p}/"))
                .map(ToString::to_string),
            None => None,
        }
    }
}

pub enum LlmPrefix {
    HuggingFace,
    SpiceAi,
    File,
    OpenAi,
}

impl TryFrom<&str> for LlmPrefix {
    type Error = &'static str;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        if value.starts_with("huggingface:huggingface.co") {
            Ok(LlmPrefix::HuggingFace)
        } else if value.starts_with("spice.ai") {
            Ok(LlmPrefix::SpiceAi)
        } else if value.starts_with("file:") {
            Ok(LlmPrefix::File)
        } else if value.starts_with("openai") {
            Ok(LlmPrefix::OpenAi)
        } else {
            Err("Unknown prefix")
        }
    }
}

impl Display for LlmPrefix {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LlmPrefix::HuggingFace => write!(f, "huggingface:huggingface.co"),
            LlmPrefix::SpiceAi => write!(f, "spice.ai"),
            LlmPrefix::File => write!(f, "file:"),
            LlmPrefix::OpenAi => write!(f, "openai"),
        }
    }
}

#[derive(Debug, Serialize, PartialEq, Clone)]
#[serde(untagged)]
pub enum LlmParams {
    HuggingfaceParams {
        model_type: Option<Architecture>,
        weights_path: Option<String>,
        tokenizer_path: Option<String>,
        tokenizer_config_path: Option<String>,
    },

    SpiceAiParams {},

    LocalModelParams {
        weights_path: String,
        tokenizer_path: Option<String>,
        tokenizer_config_path: String,
    },
    OpenAiParams {
        api_base: Option<String>,
        api_key: Option<String>,
        org_id: Option<String>,
        project_id: Option<String>,
    },
    None,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Architecture {
    #[serde(rename = "mistral")]
    Mistral,
    #[serde(rename = "gemma")]
    Gemma,
    #[serde(rename = "mixtral")]
    Mixtral,
    #[serde(rename = "llama")]
    Llama,
    #[serde(rename = "phi2")]
    Phi2,
    #[serde(rename = "phi3")]
    Phi3,
    #[serde(rename = "qwen2")]
    Qwen2,
}
impl TryFrom<&str> for Architecture {
    type Error = &'static str;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "mistral" => Ok(Architecture::Mistral),
            "gemma" => Ok(Architecture::Gemma),
            "mixtral" => Ok(Architecture::Mixtral),
            "llama" => Ok(Architecture::Llama),
            "phi2" => Ok(Architecture::Phi2),
            "phi3" => Ok(Architecture::Phi3),
            "qwen2" => Ok(Architecture::Qwen2),
            _ => Err("Unknown architecture"),
        }
    }
}

impl fmt::Display for Architecture {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Architecture::Mistral => write!(f, "mistral"),
            Architecture::Gemma => write!(f, "gemma"),
            Architecture::Mixtral => write!(f, "mixtral"),
            Architecture::Llama => write!(f, "llama"),
            Architecture::Phi2 => write!(f, "phi2"),
            Architecture::Phi3 => write!(f, "phi3"),
            Architecture::Qwen2 => write!(f, "qwen2"),
        }
    }
}
