use serde_json::Value;
use std::{collections::HashMap, fmt::Display};

use serde::Serialize;

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "lowercase")]
enum ProgressType {
    #[serde(rename = "err")]
    Error,
    #[serde(rename = "warn")]
    Warning,
    Log,
}

impl Display for ProgressType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl ProgressType {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Error => "err",
            Self::Warning => "warn",
            Self::Log => "log",
        }
    }
}

#[derive(Clone, Serialize)]
pub struct Progress {
    #[serde(rename = "type")]
    r#type: ProgressType,
    #[serde(skip_serializing_if = "Option::is_none")]
    id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    parent_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    content: Option<Value>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    tags: HashMap<String, String>,
}

impl Progress {
    #[must_use]
    pub fn log() -> Self {
        Self::new(ProgressType::Log)
    }
    #[must_use]
    pub fn error() -> Self {
        Self::new(ProgressType::Error)
    }
    #[must_use]
    pub fn warning() -> Self {
        Self::new(ProgressType::Warning)
    }
    fn new(progress_type: ProgressType) -> Self {
        Self {
            r#type: progress_type,
            id: None,
            parent_id: None,
            title: None,
            content: None,
            tags: HashMap::new(),
        }
    }

    #[must_use]
    pub fn id(mut self, id: String) -> Self {
        self.id = Some(id);
        self
    }

    #[must_use]
    pub fn parent_id(mut self, parent_id: String) -> Self {
        self.parent_id = Some(parent_id);
        self
    }

    #[must_use]
    pub fn title(mut self, title: String) -> Self {
        self.title = Some(title);
        self
    }

    #[must_use]
    pub fn json_content(mut self, content: Value) -> Self {
        self.content = Some(content);
        self
    }
    #[must_use]
    pub fn content(mut self, content: String) -> Self {
        self.content = Some(Value::String(content));
        self
    }

    #[must_use]
    pub fn tag(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.tags.insert(key.into(), value.into());
        self
    }

    #[must_use]
    pub fn to_jsonl(&self) -> String {
        let raw_json = match serde_json::to_string(self) {
            Ok(json) => json,
            Err(_) => {
                r#"{ "type": "err", "content": "Unexpected error converting progress to JSONL" }"#
                    .to_string()
            }
        };
        format!("!---jsonl{raw_json}")
    }
}
