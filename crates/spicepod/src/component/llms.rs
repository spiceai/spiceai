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

use std::fmt::{self, Display, Formatter};

use super::WithDependsOn;
use serde::{Deserialize, Deserializer, Serialize, de::Error as serde_error};
use serde_yaml;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Llm {
    pub from: String,
    pub name: String,

    #[serde(skip_serializing_if = "Vec::is_empty")]
    #[serde(rename = "dependsOn", default)]
    pub depends_on: Vec<String>,

    // #[serde(deserialize_with = "deserialize_params")]
    pub params: LlmParams,
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
        if value.starts_with("huggingface:huggingface.co/") {
            Ok(LlmPrefix::HuggingFace)
        } else if value.starts_with("spice.ai/") {
            Ok(LlmPrefix::SpiceAi)
        } else if value.starts_with("file:/") {
            Ok(LlmPrefix::File)
        } else if value.starts_with("openai/") {
            Ok(LlmPrefix::OpenAi)
        } else {
            Err("Unknown prefix")
        }
    }
}

impl Display for LlmPrefix {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            LlmPrefix::HuggingFace => write!(f, "huggingface"),
            LlmPrefix::SpiceAi => write!(f, "spice.ai"),
            LlmPrefix::File => write!(f, "file"),
            LlmPrefix::OpenAi => write!(f, "openai"),
        }
    }
}

#[derive(Debug, Serialize, PartialEq, Clone)]
#[serde(untagged)]
enum LlmParams {
    HuggingfaceParams{
        weights: Option<String>,
        tokenizer: Option<String>,
        chat_template: Option<String>,
    },

    SpiceAiParams{
        chat_template: Option<String>,
    },

    LocalModelParams{
        weights: Option<String>,
        tokenizer: Option<String>,
        chat_template: Option<String>,
    },
    OpenAiParams{
        model: Option<String>,
    },
    None
}


impl<'de> Deserialize<'de> for LlmParams {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
    let helper: serde_yaml::Value = Deserialize::deserialize(deserializer)?;
    
    let params = helper.get("params").cloned().unwrap_or(serde_yaml::Value::Null);
    
    let from = helper.get("from").and_then(serde_yaml::Value::as_str);
    
    let p: Result<LlmParams, _> = serde_yaml::from_value(params).map_err(serde_error::custom);
    return p;

    // match from.map(|f| LlmPrefix::try_from(f)) {
    //     Some(Ok(LlmPrefix::HuggingFace)) => {
    //         Ok(LlmParams::HuggingfaceParams(params))
    //     }
    //     Some(Ok(LlmPrefix::SpiceAi)) => {
    //         Ok(LlmParams::SpiceAiParams(params))
    //     }
    //     Some(Ok(LlmPrefix::File)) => {
    //         Ok(LlmParams::LocalModelParams(params))
    //     }
    //     Some(Ok(LlmPrefix::OpenAi)) => {
    //         Ok(LlmParams::OpenAiParams(params))
    //     }
    //     None => Err(serde_error::custom(format!("Unknown `from` value: {}", from.unwrap_or("no `from` parameter found")))),
    //     Some(Err(e)) => Err(serde_error::custom(e)),
    // }
    }
}
