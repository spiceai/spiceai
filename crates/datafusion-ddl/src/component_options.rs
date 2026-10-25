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

//! Spicepod datasets and catalogs as SQL `WITH` options.
//!
//! A dataset or catalog defined with SQL is the Spicepod component a
//! `datasets:` or `catalogs:` entry declares, and its `WITH` options follow
//! one rule:
//!
//! - A key names a field of the component. Nested fields join with `.`
//!   (`acceleration.engine`, `metadata.owner`), and a dataset's `columns`
//!   entries are addressed by column name (`columns.body.description`).
//! - A key that names no field is a connector parameter, the key written under
//!   the component's `params` (`pg_host`). `params.<key>` says so explicitly.
//! - A value is a string, number, or boolean literal, `ARRAY[...]` for a list,
//!   or `JSON '<json>'` for any structured value.
//!
//! The options are assembled into a YAML value and deserialized by the
//! Spicepod's own types, so every field, alias, default, and validation is the
//! one a Spicepod gets, including fields added after this module.
//!
//! The reverse direction renders a component as the options, and the
//! statement, that recreate it (`SHOW CREATE TABLE`).

use std::borrow::Cow;
use std::collections::HashSet;
use std::fmt::Write as _;
use std::sync::LazyLock;

use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::sql::sqlparser::ast::{
    ColumnOption, CreateTable, CreateTableOptions, DataType, Expr, ObjectName, SqlOption,
    TableConstraint, UnaryOperator, Value as SqlValue, WrappedCollection,
};
use datafusion::sql::sqlparser::dialect::Dialect;
use datafusion::sql::sqlparser::keywords::Keyword;
use datafusion::sql::sqlparser::tokenizer::{Location, Token, TokenWithSpan, Tokenizer};
use serde::Serialize;
use serde::de::{self, DeserializeOwned, Visitor};
use spicepod::component::catalog::Catalog;
use spicepod::component::dataset::Dataset;
use yaml::{Mapping, Number, Value};

/// Which Spicepod component a statement defines.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ComponentKind {
    Dataset,
    Catalog,
}

impl ComponentKind {
    /// The component's name in messages.
    #[must_use]
    pub fn noun(self) -> &'static str {
        match self {
            Self::Dataset => "dataset",
            Self::Catalog => "catalog",
        }
    }

    /// The SQL object a statement names for this component.
    #[must_use]
    pub fn sql_object(self) -> &'static str {
        match self {
            Self::Dataset => "TABLE",
            Self::Catalog => "CATALOG",
        }
    }

    /// The field names, aliases included, the component deserializes.
    fn fields(self) -> &'static [&'static str] {
        static DATASET: LazyLock<&'static [&'static str]> =
            LazyLock::new(struct_field_names::<Dataset>);
        static CATALOG: LazyLock<&'static [&'static str]> =
            LazyLock::new(struct_field_names::<Catalog>);
        match self {
            Self::Dataset => *DATASET,
            Self::Catalog => *CATALOG,
        }
    }

    fn is_field(self, name: &str) -> bool {
        self.fields().contains(&name)
    }
}

/// A Spicepod component that SQL can define.
pub trait SqlComponent: Serialize + DeserializeOwned {
    const KIND: ComponentKind;

    fn component_name(&self) -> &str;
}

impl SqlComponent for Dataset {
    const KIND: ComponentKind = ComponentKind::Dataset;

    fn component_name(&self) -> &str {
        &self.name
    }
}

impl SqlComponent for Catalog {
    const KIND: ComponentKind = ComponentKind::Catalog;

    fn component_name(&self) -> &str {
        &self.name
    }
}

/// A `WITH` option value, as the statement wrote it.
#[derive(Debug, Clone, PartialEq)]
pub enum OptionValue {
    /// A string literal, or an unquoted word (`engine = cayenne`).
    String(String),
    /// A number literal, in its spelling.
    Number(String),
    Bool(bool),
    /// `NULL`: the field is unset.
    Null,
    /// `ARRAY[...]` or `[...]`.
    List(Vec<OptionValue>),
    /// `JSON '<json>'`.
    Json(serde_json::Value),
}

/// One `WITH` option.
#[derive(Debug, Clone, PartialEq)]
pub struct ComponentOption {
    pub key: String,
    pub value: OptionValue,
}

impl ComponentOption {
    #[must_use]
    pub fn new(key: impl Into<String>, value: OptionValue) -> Self {
        Self {
            key: key.into(),
            value,
        }
    }
}

/// Read a statement's `WITH (...)` options.
///
/// # Errors
///
/// Returns an error for an option that is not `key = value`, a value that is
/// not a literal, `ARRAY[...]`, or `JSON '...'`, and a key given twice.
pub fn options_from_sql(options: &[SqlOption]) -> DFResult<Vec<ComponentOption>> {
    let mut seen = HashSet::new();
    let mut parsed = Vec::with_capacity(options.len());
    for option in options {
        let SqlOption::KeyValue { key, value } = option else {
            return Err(DataFusionError::Plan(format!(
                "Option '{option}' is not supported. Write each option as key = value, for example \"acceleration.engine\" = 'cayenne'."
            )));
        };
        let key = key.value.clone();
        if !seen.insert(key.clone()) {
            return Err(DataFusionError::Plan(format!(
                "Option '{key}' is set more than once. Set it once."
            )));
        }
        let value = OptionValue::from_expr(&key, value)?;
        if value == OptionValue::String(REDACTED.to_string()) {
            return Err(DataFusionError::Plan(format!(
                "Option '{key}' holds {REDACTED_SQL}, which SHOW CREATE prints in place of a secret it does not show. Set the value, or a reference to it such as '${{secrets:{name}}}'.",
                name = key.strip_prefix("params.").unwrap_or(&key),
            )));
        }
        parsed.push(ComponentOption { key, value });
    }
    Ok(parsed)
}

impl OptionValue {
    /// Read an option's value from its SQL expression.
    ///
    /// # Errors
    ///
    /// Returns an error for an expression that is not a literal, `ARRAY[...]`,
    /// or `JSON '...'`, or for `JSON` text that is not valid JSON.
    pub fn from_expr(key: &str, expr: &Expr) -> DFResult<Self> {
        match expr {
            Expr::Value(value) => match &value.value {
                SqlValue::SingleQuotedString(s)
                | SqlValue::DoubleQuotedString(s)
                | SqlValue::EscapedStringLiteral(s)
                | SqlValue::UnicodeStringLiteral(s)
                | SqlValue::TripleSingleQuotedString(s)
                | SqlValue::TripleDoubleQuotedString(s) => Ok(Self::String(s.clone())),
                SqlValue::DollarQuotedString(s) => Ok(Self::String(s.value.clone())),
                SqlValue::Number(n, _) => Ok(Self::Number(n.clone())),
                SqlValue::Boolean(b) => Ok(Self::Bool(*b)),
                SqlValue::Null => Ok(Self::Null),
                other => Err(unsupported_value(key, &other.to_string())),
            },
            // An unquoted word, `engine = cayenne`, or a double-quoted one, which
            // PostgreSQL reads as an identifier.
            Expr::Identifier(ident) => Ok(Self::String(ident.value.clone())),
            Expr::UnaryOp {
                op: UnaryOperator::Minus,
                expr: inner,
            } => match inner.as_ref() {
                Expr::Value(value) => match &value.value {
                    SqlValue::Number(n, _) => Ok(Self::Number(format!("-{n}"))),
                    _ => Err(unsupported_value(key, &expr.to_string())),
                },
                _ => Err(unsupported_value(key, &expr.to_string())),
            },
            Expr::Array(array) => array
                .elem
                .iter()
                .map(|elem| Self::from_expr(key, elem))
                .collect::<DFResult<Vec<_>>>()
                .map(Self::List),
            Expr::TypedString(typed) if matches!(typed.data_type, DataType::JSON) => {
                let text = typed.value.clone().into_string().ok_or_else(|| {
                    DataFusionError::Plan(format!(
                        "Option '{key}' must be written JSON '<json text>'."
                    ))
                })?;
                serde_json::from_str(&text).map(Self::Json).map_err(|e| {
                    DataFusionError::Plan(format!(
                        "Option '{key}' is not valid JSON: {e}. Write it as JSON '<json text>', for example JSON '[{{\"from\": \"openai\"}}]'."
                    ))
                })
            }
            other => Err(unsupported_value(key, &other.to_string())),
        }
    }

    fn into_yaml(self, key: &str) -> DFResult<Value> {
        Ok(match self {
            Self::String(s) => Value::String(s),
            Self::Number(spelling) => Value::Number(parse_number(key, &spelling)?),
            Self::Bool(b) => Value::Bool(b),
            Self::Null => Value::Null,
            Self::List(items) => Value::Sequence(
                items
                    .into_iter()
                    .map(|item| item.into_yaml(key))
                    .collect::<DFResult<_>>()?,
            ),
            Self::Json(json) => json_to_yaml(json),
        })
    }

    fn from_yaml(value: Value) -> Self {
        match value {
            Value::Null => Self::Null,
            Value::Bool(b) => Self::Bool(b),
            Value::Number(n) => Self::Number(number_spelling(&n)),
            Value::String(s) => Self::String(s),
            Value::Sequence(items) if items.iter().all(is_scalar) => {
                Self::List(items.into_iter().map(Self::from_yaml).collect())
            }
            structured @ (Value::Sequence(_) | Value::Mapping(_)) => {
                Self::Json(yaml_to_json(structured))
            }
        }
    }

    /// The value as SQL text that reads back as the same value.
    #[must_use]
    pub fn to_sql(&self) -> String {
        match self {
            Self::String(s) => quote_string(s),
            Self::Number(spelling) => spelling.clone(),
            Self::Bool(b) => b.to_string(),
            Self::Null => "NULL".to_string(),
            Self::List(items) => {
                let items: Vec<String> = items.iter().map(Self::to_sql).collect();
                format!("ARRAY[{}]", items.join(", "))
            }
            Self::Json(json) => format!("JSON {}", quote_string(&json.to_string())),
        }
    }
}

fn unsupported_value(key: &str, value: &str) -> DataFusionError {
    DataFusionError::Plan(format!(
        "Option '{key}' has the value {value}, which is not supported. Use a string, number, or boolean literal, ARRAY['a', 'b'] for a list, or JSON '<json>' for a structured value."
    ))
}

fn parse_number(key: &str, spelling: &str) -> DFResult<Number> {
    if let Ok(n) = spelling.parse::<u64>() {
        return Ok(Number::PosInt(n));
    }
    if let Ok(n) = spelling.parse::<i64>() {
        return Ok(Number::NegInt(n));
    }
    match spelling.parse::<f64>() {
        Ok(n) if n.is_finite() => Ok(Number::Float(n)),
        _ => Err(DataFusionError::Plan(format!(
            "Option '{key}' has the number {spelling}, which is out of range."
        ))),
    }
}

/// A number's spelling that parses back as the same number: a float always
/// carries a decimal point or an exponent, so `1.0` does not read back as the
/// integer `1`.
fn number_spelling(n: &Number) -> String {
    match n {
        Number::PosInt(n) => n.to_string(),
        Number::NegInt(n) => n.to_string(),
        Number::Float(n) => format!("{n:?}"),
    }
}

fn is_scalar(value: &Value) -> bool {
    matches!(
        value,
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_)
    )
}

fn json_to_yaml(json: serde_json::Value) -> Value {
    match json {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(b) => Value::Bool(b),
        serde_json::Value::Number(n) => Value::Number(if let Some(n) = n.as_u64() {
            Number::PosInt(n)
        } else if let Some(n) = n.as_i64() {
            Number::NegInt(n)
        } else {
            Number::Float(n.as_f64().unwrap_or(f64::NAN))
        }),
        serde_json::Value::String(s) => Value::String(s),
        serde_json::Value::Array(items) => {
            Value::Sequence(items.into_iter().map(json_to_yaml).collect())
        }
        serde_json::Value::Object(map) => Value::Mapping(
            map.into_iter()
                .map(|(k, v)| (Value::String(k), json_to_yaml(v)))
                .collect(),
        ),
    }
}

fn yaml_to_json(value: Value) -> serde_json::Value {
    match value {
        Value::Null => serde_json::Value::Null,
        Value::Bool(b) => serde_json::Value::Bool(b),
        Value::Number(Number::PosInt(n)) => serde_json::Value::from(n),
        Value::Number(Number::NegInt(n)) => serde_json::Value::from(n),
        Value::Number(Number::Float(n)) => serde_json::Number::from_f64(n)
            .map_or(serde_json::Value::Null, serde_json::Value::Number),
        Value::String(s) => serde_json::Value::String(s),
        Value::Sequence(items) => {
            serde_json::Value::Array(items.into_iter().map(yaml_to_json).collect())
        }
        Value::Mapping(map) => serde_json::Value::Object(
            map.into_iter()
                .map(|(k, v)| (yaml_key_text(&k), yaml_to_json(v)))
                .collect(),
        ),
    }
}

fn yaml_key_text(key: &Value) -> String {
    match key {
        Value::String(s) => s.clone(),
        Value::Number(n) => number_spelling(n),
        Value::Bool(b) => b.to_string(),
        Value::Null => "null".to_string(),
        structured => yaml_to_json(structured.clone()).to_string(),
    }
}

/// A SQL string literal for `s`.
#[must_use]
pub fn quote_string(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

/// A SQL identifier for `name`: bare when it is a lowercase word, otherwise
/// double-quoted.
#[must_use]
pub fn quote_identifier(name: &str) -> String {
    let mut chars = name.chars();
    let bare = chars
        .next()
        .is_some_and(|c| c.is_ascii_lowercase() || c == '_')
        && chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');
    if bare {
        name.to_string()
    } else {
        format!("\"{}\"", name.replace('"', "\"\""))
    }
}

// ── Field names, read from the Spicepod types ─────────────────────────────────

/// The field names, aliases included, that `T` deserializes: the `fields` its
/// derived `Deserialize` hands to `deserialize_struct`.
fn struct_field_names<T: DeserializeOwned>() -> &'static [&'static str] {
    let mut fields = None;
    // The capture fails the deserialization once it has the names.
    let _ = T::deserialize(FieldNameCapture {
        fields: &mut fields,
    });
    fields.unwrap_or(&[])
}

struct FieldNameCapture<'a> {
    fields: &'a mut Option<&'static [&'static str]>,
}

impl<'de> de::Deserializer<'de> for FieldNameCapture<'_> {
    type Error = de::value::Error;

    fn deserialize_any<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value, Self::Error> {
        Err(de::Error::custom("not a struct"))
    }

    fn deserialize_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        fields: &'static [&'static str],
        _visitor: V,
    ) -> Result<V::Value, Self::Error> {
        *self.fields = Some(fields);
        Err(de::Error::custom("field names captured"))
    }

    serde::forward_to_deserialize_any! {
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string bytes
        byte_buf option unit unit_struct newtype_struct seq tuple tuple_struct map
        enum identifier ignored_any
    }
}

// ── Options → component ───────────────────────────────────────────────────────

/// The value `SHOW CREATE` prints in place of a secret parameter's value. A
/// statement that sets an option to it is refused, so replaying the output
/// never stores the placeholder as the secret.
pub const REDACTED: &str = "<redacted>";
const REDACTED_SQL: &str = "'<redacted>'";

/// The parameters a component's connector reads: how a bare option key is told
/// apart as a parameter rather than a field.
pub trait ConnectorParameters {
    /// The connector's name, for messages.
    fn connector(&self) -> &str;

    /// Whether the connector reads the parameter `key`.
    fn reads(&self, key: &str) -> bool;
}

/// Where an option's key puts its value.
#[derive(Debug, Clone, PartialEq)]
enum KeyPath {
    /// A field of the component, by path.
    Field(Vec<String>),
    /// A connector parameter, under the component's `params`.
    Param(String),
}

impl KeyPath {
    fn segments(&self) -> Vec<String> {
        match self {
            Self::Field(segments) => segments.clone(),
            Self::Param(name) => vec!["params".to_string(), name.clone()],
        }
    }
}

fn resolve_key(
    kind: ComponentKind,
    key: &str,
    parameters: Option<&dyn ConnectorParameters>,
) -> DFResult<KeyPath> {
    // `dataset.<field>` is how `CREATE TABLE` options on catalog tables name a
    // dataset field (`"dataset.time_column"`); it stays an alias.
    let key = match kind {
        ComponentKind::Dataset => key.strip_prefix("dataset.").unwrap_or(key),
        ComponentKind::Catalog => key,
    };
    if key == "name" {
        return Err(DataFusionError::Plan(format!(
            "Option 'name' cannot be set: a {noun}'s name is the one the statement gives it, as in CREATE {object} <name>.",
            noun = kind.noun(),
            object = kind.sql_object(),
        )));
    }
    if let Some(param) = key.strip_prefix("params.") {
        if param.is_empty() {
            return Err(empty_key_segment(key));
        }
        return Ok(KeyPath::Param(param.to_string()));
    }
    let segments: Vec<String> = key.split('.').map(str::to_string).collect();
    if segments.iter().any(String::is_empty) {
        return Err(empty_key_segment(key));
    }
    let is_field = segments.first().is_some_and(|first| kind.is_field(first));
    match parameters {
        Some(parameters) if is_field && parameters.reads(key) => {
            Err(DataFusionError::Plan(format!(
                "Option '{key}' is both a {noun} field and a parameter of the '{connector}' connector. Write \"params.{key}\" to set the parameter.",
                noun = kind.noun(),
                connector = parameters.connector(),
            )))
        }
        _ if is_field => Ok(KeyPath::Field(segments)),
        _ => Ok(KeyPath::Param(key.to_string())),
    }
}

fn empty_key_segment(key: &str) -> DataFusionError {
    DataFusionError::Plan(format!(
        "Option '{key}' has an empty name part. Separate nested field names with a single '.', for example \"acceleration.engine\"."
    ))
}

/// The keys set on a definition so far, to refuse a key set twice or a key
/// inside a value another key set whole.
#[derive(Default)]
struct SetKeys {
    /// Each set path, with the option or clause that set it.
    set: Vec<(Vec<String>, String)>,
}

impl SetKeys {
    fn record(&mut self, path: &[String], source: &str) -> DFResult<()> {
        for (existing, existing_source) in &self.set {
            let shared = existing.len().min(path.len());
            if existing[..shared] == path[..shared] {
                return Err(DataFusionError::Plan(format!(
                    "{existing_source} and {source} set the same field. Set it once."
                )));
            }
        }
        self.set.push((path.to_vec(), source.to_string()));
        Ok(())
    }
}

/// Put `value` at `path` in a component's YAML mapping. A dataset's
/// `columns` entry is found, or added, by the name that follows `columns`.
fn set_path(
    mapping: &mut Mapping,
    kind: ComponentKind,
    path: &[String],
    value: Value,
    top_level: bool,
    key: &str,
) -> DFResult<()> {
    let Some((first, rest)) = path.split_first() else {
        return Err(empty_key_segment(key));
    };
    if top_level && kind == ComponentKind::Dataset && first == "columns" && !rest.is_empty() {
        return set_column_path(mapping, kind, rest, value, key);
    }
    if rest.is_empty() {
        mapping.insert(Value::String(first.clone()), value);
        return Ok(());
    }
    let entry = mapping
        .entry(Value::String(first.clone()))
        .or_insert_with(|| Value::Mapping(Mapping::new()));
    if entry.is_null() {
        *entry = Value::Mapping(Mapping::new());
    }
    let Value::Mapping(child) = entry else {
        return Err(DataFusionError::Plan(format!(
            "Option '{key}' sets a field inside '{first}', which is not a group of fields."
        )));
    };
    set_path(child, kind, rest, value, false, key)
}

fn set_column_path(
    mapping: &mut Mapping,
    kind: ComponentKind,
    path: &[String],
    value: Value,
    key: &str,
) -> DFResult<()> {
    let Some((column, rest)) = path.split_first() else {
        return Err(empty_key_segment(key));
    };
    let columns = mapping
        .entry(Value::String("columns".to_string()))
        .or_insert_with(|| Value::Sequence(Vec::new()));
    if columns.is_null() {
        *columns = Value::Sequence(Vec::new());
    }
    let Value::Sequence(columns) = columns else {
        return Err(DataFusionError::Plan(format!(
            "Option '{key}' sets a column field, but 'columns' is not a list of columns."
        )));
    };
    let position = columns
        .iter()
        .position(|c| c.get("name").and_then(Value::as_str) == Some(column.as_str()));
    let index = if let Some(index) = position {
        index
    } else {
        let mut entry = Mapping::new();
        entry.insert(
            Value::String("name".to_string()),
            Value::String(column.clone()),
        );
        columns.push(Value::Mapping(entry));
        columns.len() - 1
    };
    let Some(Value::Mapping(entry)) = columns.get_mut(index) else {
        return Err(DataFusionError::Plan(format!(
            "Option '{key}' sets a field of column '{column}', which is not a group of fields."
        )));
    };
    if rest.is_empty() {
        // `"columns.body" = JSON '{...}'` sets the column's fields whole.
        let Value::Mapping(fields) = value else {
            return Err(DataFusionError::Plan(format!(
                "Option '{key}' must be a JSON object of the column's fields, for example JSON '{{\"description\": \"...\"}}'."
            )));
        };
        entry.retain(|k, _| k.as_str() == Some("name"));
        for (k, v) in fields {
            if k.as_str() != Some("name") {
                entry.insert(k, v);
            }
        }
        return Ok(());
    }
    set_path(entry, kind, rest, value, false, key)
}

/// Remove the value at `path`. Returns whether it was set.
fn remove_path(
    mapping: &mut Mapping,
    kind: ComponentKind,
    path: &[String],
    top_level: bool,
) -> bool {
    let Some((first, rest)) = path.split_first() else {
        return false;
    };
    let key = Value::String(first.clone());
    if rest.is_empty() {
        return mapping.shift_remove(&key).is_some();
    }
    if top_level && kind == ComponentKind::Dataset && first == "columns" {
        let Some(Value::Sequence(columns)) = mapping.get_mut(&key) else {
            return false;
        };
        let Some((column, column_rest)) = rest.split_first() else {
            return false;
        };
        let Some(index) = columns
            .iter()
            .position(|c| c.get("name").and_then(Value::as_str) == Some(column.as_str()))
        else {
            return false;
        };
        let removed = if column_rest.is_empty() {
            columns.remove(index);
            true
        } else if let Some(Value::Mapping(entry)) = columns.get_mut(index) {
            let removed = remove_path(entry, kind, column_rest, false);
            // A column left with only its name declares nothing.
            if entry.len() == 1 && entry.contains_key(&Value::String("name".to_string())) {
                columns.remove(index);
            }
            removed
        } else {
            false
        };
        if columns.is_empty() {
            mapping.shift_remove(&key);
        }
        return removed;
    }
    let Some(Value::Mapping(child)) = mapping.get_mut(&key) else {
        return false;
    };
    let removed = remove_path(child, kind, rest, false);
    if child.is_empty() {
        mapping.shift_remove(&key);
    }
    removed
}

/// A component's fields that SQL clauses set, kept apart from its `WITH`
/// options so a field set both ways is refused.
#[derive(Debug, Default, Clone)]
pub struct ClauseFields {
    /// Each field path, its value, and the clause that set it.
    fields: Vec<(Vec<String>, Value, String)>,
}

impl ClauseFields {
    /// Record `value` at the field `path`, set by `clause`.
    pub fn set(&mut self, path: &[&str], value: Value, clause: impl Into<String>) {
        self.fields.push((
            path.iter().map(|s| (*s).to_string()).collect(),
            value,
            clause.into(),
        ));
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }
}

/// Build a component from a statement's clauses and `WITH` options.
///
/// # Errors
///
/// Returns an error when an option's key or value is not valid, two options
/// or clauses set the same field, or the Spicepod rejects the definition.
pub fn component_from_options<T: SqlComponent>(
    name: &str,
    clauses: &ClauseFields,
    options: &[ComponentOption],
    parameters: Option<&dyn ConnectorParameters>,
) -> DFResult<T> {
    let kind = T::KIND;
    let mut mapping = Mapping::new();
    mapping.insert(
        Value::String("name".to_string()),
        Value::String(name.to_string()),
    );
    let mut set = SetKeys::default();
    for (path, value, clause) in &clauses.fields {
        set.record(path, clause)?;
        set_path(&mut mapping, kind, path, value.clone(), true, clause)?;
    }
    for option in options {
        let path = resolve_key(kind, &option.key, parameters)?.segments();
        set.record(&path, &format!("Option '{}'", option.key))?;
        let value = option.value.clone().into_yaml(&option.key)?;
        if value.is_null() {
            continue;
        }
        set_path(&mut mapping, kind, &path, value, true, &option.key)?;
    }
    deserialize_component(kind, name, mapping, options)
}

/// `component` with `options` set on it (`ALTER ... SET`).
///
/// # Errors
///
/// Returns an error when an option's key or value is not valid, an option is
/// given twice, or the Spicepod rejects the changed definition.
pub fn component_with_options<T: SqlComponent>(
    component: &T,
    options: &[ComponentOption],
    parameters: Option<&dyn ConnectorParameters>,
) -> DFResult<T> {
    let kind = T::KIND;
    let mut mapping = component_mapping(component)?;
    let mut set = SetKeys::default();
    for option in options {
        let path = resolve_key(kind, &option.key, parameters)?.segments();
        set.record(&path, &format!("Option '{}'", option.key))?;
        let value = option.value.clone().into_yaml(&option.key)?;
        if value.is_null() {
            remove_path(&mut mapping, kind, &path, true);
        } else {
            set_path(&mut mapping, kind, &path, value, true, &option.key)?;
        }
    }
    deserialize_component(kind, component.component_name(), mapping, options)
}

/// `component` with the options `keys` unset (`ALTER ... RESET`).
///
/// # Errors
///
/// Returns an error when a key is not set on the component, or the Spicepod
/// rejects the changed definition.
pub fn component_without_options<T: SqlComponent>(
    component: &T,
    keys: &[String],
    parameters: Option<&dyn ConnectorParameters>,
) -> DFResult<T> {
    let kind = T::KIND;
    let mut mapping = component_mapping(component)?;
    for key in keys {
        let path = resolve_key(kind, key, parameters)?.segments();
        if path.first().map(String::as_str) == Some("from") {
            return Err(DataFusionError::Plan(format!(
                "Option 'from' cannot be reset: every {noun} reads from a source. Set a new one with SET (from = '...').",
                noun = kind.noun()
            )));
        }
        if !remove_path(&mut mapping, kind, &path, true) {
            let set_keys: Vec<String> = component_options(component)?
                .into_iter()
                .map(|option| option.key)
                .collect();
            let hint = util::levenshtein::closest_match(key, &set_keys)
                .map(|m| format!(" Did you mean '{m}'?"))
                .unwrap_or_default();
            return Err(DataFusionError::Plan(format!(
                "Option '{key}' is not set on {noun} '{name}', so it cannot be reset.{hint}",
                noun = kind.noun(),
                name = component.component_name(),
            )));
        }
    }
    deserialize_component(kind, component.component_name(), mapping, &[])
}

fn component_mapping<T: SqlComponent>(component: &T) -> DFResult<Mapping> {
    match yaml::to_value(component) {
        Ok(Value::Mapping(mapping)) => Ok(mapping),
        Ok(_) => Err(DataFusionError::Internal(format!(
            "The definition of {} '{}' is not a mapping",
            T::KIND.noun(),
            component.component_name()
        ))),
        Err(e) => Err(DataFusionError::Internal(format!(
            "Failed to read the definition of {} '{}': {e}",
            T::KIND.noun(),
            component.component_name()
        ))),
    }
}

fn deserialize_component<T: SqlComponent>(
    kind: ComponentKind,
    name: &str,
    mapping: Mapping,
    options: &[ComponentOption],
) -> DFResult<T> {
    yaml::from_value::<T>(Value::Mapping(mapping))
        .map_err(|e| definition_error(kind, name, options, &e.to_string()))
}

/// The error for a definition the Spicepod types reject, naming the option to
/// fix: serde names an unknown field and the fields it expected, and the
/// option carrying that field gets the closest one suggested.
fn definition_error(
    kind: ComponentKind,
    name: &str,
    options: &[ComponentOption],
    message: &str,
) -> DataFusionError {
    if let Some((field, expected)) = unknown_field(message) {
        let option = options
            .iter()
            .map(|o| o.key.as_str())
            .find(|key| key.split('.').any(|segment| segment == field));
        let suggestion = util::levenshtein::closest_match(field, &expected);
        if let Some(option) = option {
            let fix = match suggestion {
                Some(suggestion) => {
                    let fixed: Vec<&str> = option
                        .split('.')
                        .map(|segment| {
                            if segment == field {
                                suggestion.as_str()
                            } else {
                                segment
                            }
                        })
                        .collect();
                    format!("Did you mean '{}'?", fixed.join("."))
                }
                None => format!("Valid fields at that level: {}.", expected.join(", ")),
            };
            return DataFusionError::Plan(format!(
                "Option '{option}' is not a {noun} field. {fix} See https://spiceai.org/docs/reference/spicepod/{section}",
                noun = kind.noun(),
                section = reference_section(kind),
            ));
        }
    }
    DataFusionError::Plan(format!(
        "The definition of {noun} '{name}' is not valid: {message}. See https://spiceai.org/docs/reference/spicepod/{section}",
        noun = kind.noun(),
        section = reference_section(kind),
    ))
}

fn reference_section(kind: ComponentKind) -> &'static str {
    match kind {
        ComponentKind::Dataset => "datasets",
        ComponentKind::Catalog => "catalogs",
    }
}

/// The field serde reports as unknown, and the fields it expected, from its
/// ``unknown field `x`, expected one of `a`, `b` `` message.
fn unknown_field(message: &str) -> Option<(&str, Vec<String>)> {
    let rest = message.split("unknown field `").nth(1)?;
    let (field, rest) = rest.split_once('`')?;
    let expected = rest
        .split('`')
        .skip(1)
        .step_by(2)
        .map(str::to_string)
        .collect();
    Some((field, expected))
}

// ── Component → options ───────────────────────────────────────────────────────

/// The options that recreate `component`, in a stable order: `from`, the
/// connector parameters by name, then the other fields as the Spicepod orders
/// them.
///
/// # Errors
///
/// Returns an error when the component cannot be read as a Spicepod value.
pub fn component_options<T: SqlComponent>(component: &T) -> DFResult<Vec<ComponentOption>> {
    let kind = T::KIND;
    let mut mapping = component_mapping(component)?;
    prune_defaults::<T>(&mut mapping);
    mapping.shift_remove(&Value::String("name".to_string()));

    let mut options = Vec::new();
    if let Some(from) = mapping.shift_remove(&Value::String("from".to_string()))
        && !from.is_null()
    {
        options.push(ComponentOption::new("from", OptionValue::from_yaml(from)));
    }
    if let Some(Value::Mapping(params)) = mapping.shift_remove(&Value::String("params".to_string()))
    {
        let mut params: Vec<(String, Value)> = params
            .into_iter()
            .map(|(k, v)| (yaml_key_text(&k), v))
            .collect();
        params.sort_by(|(a, _), (b, _)| a.cmp(b));
        for (key, value) in params {
            // A parameter whose name reads as a field, or as an alias prefix,
            // keeps its `params.` prefix to read back as a parameter.
            let first = key.split('.').next().unwrap_or_default();
            let explicit = kind.is_field(first)
                || key.starts_with("params.")
                || key.starts_with("dataset.")
                || key == "name";
            let key = if explicit {
                format!("params.{key}")
            } else {
                key
            };
            options.push(ComponentOption::new(key, OptionValue::from_yaml(value)));
        }
    }
    for (key, value) in mapping {
        let key = yaml_key_text(&key);
        if kind == ComponentKind::Dataset && key == "columns" {
            flatten_columns(value, &mut options);
        } else {
            flatten(key, value, &mut options);
        }
    }
    Ok(options)
}

/// Drop the fields of a component's groups (`acceleration`, `replication`, …)
/// that hold the value the group takes when the field is left out, so the
/// options name only what the definition sets.
///
/// A group's defaults are read from the Spicepod types: the group deserialized
/// from an empty mapping and serialized back. A group left with no field keeps
/// `enabled` when it had it, so the options still declare the group.
fn prune_defaults<T: SqlComponent>(mapping: &mut Mapping) {
    let mut base = Mapping::new();
    for key in ["name", "from"] {
        let key = Value::String(key.to_string());
        if let Some(value) = mapping.get(&key) {
            base.insert(key, value.clone());
        }
    }
    let groups: Vec<Value> = mapping
        .iter()
        .filter(|(_, value)| value.is_mapping())
        .map(|(key, _)| key.clone())
        .collect();
    for group in groups {
        let mut probe = base.clone();
        probe.insert(group.clone(), Value::Mapping(Mapping::new()));
        let Ok(Value::Mapping(serialized)) =
            yaml::from_value::<T>(Value::Mapping(probe)).and_then(|c| yaml::to_value(&c))
        else {
            continue;
        };
        let Some(Value::Mapping(defaults)) = serialized.get(&group) else {
            continue;
        };
        let Some(Value::Mapping(fields)) = mapping.get_mut(&group) else {
            continue;
        };
        let enabled = Value::String("enabled".to_string());
        let kept_enabled = fields.get(&enabled).cloned();
        fields.retain(|field, value| defaults.get(field) != Some(value));
        if fields.is_empty()
            && let Some(value) = kept_enabled
        {
            fields.insert(enabled, value);
        }
    }
}

/// Whether a mapping's keys can each be one segment of a dotted key.
fn keys_are_segments(mapping: &Mapping) -> bool {
    mapping.keys().all(|k| match k {
        Value::String(s) => !s.is_empty() && !s.contains('.'),
        _ => false,
    })
}

fn flatten(key: String, value: Value, options: &mut Vec<ComponentOption>) {
    match value {
        // An unset optional field.
        Value::Null => {}
        Value::Mapping(mapping) if !mapping.is_empty() && keys_are_segments(&mapping) => {
            for (k, v) in mapping {
                flatten(format!("{key}.{}", yaml_key_text(&k)), v, options);
            }
        }
        other => options.push(ComponentOption::new(key, OptionValue::from_yaml(other))),
    }
}

/// A dataset's `columns`, addressed by column name when each one has a name
/// that is one key segment and declares something; otherwise the list whole.
fn flatten_columns(value: Value, options: &mut Vec<ComponentOption>) {
    let Value::Sequence(columns) = value else {
        flatten("columns".to_string(), value, options);
        return;
    };
    let mut names = HashSet::new();
    let addressable = columns.iter().all(|column| {
        let Value::Mapping(fields) = column else {
            return false;
        };
        let Some(name) = fields
            .get(&Value::String("name".to_string()))
            .and_then(Value::as_str)
        else {
            return false;
        };
        !name.is_empty()
            && !name.contains('.')
            && fields.len() > 1
            && names.insert(name.to_string())
    });
    if !addressable {
        options.push(ComponentOption::new(
            "columns",
            OptionValue::Json(yaml_to_json(Value::Sequence(columns))),
        ));
        return;
    }
    for column in columns {
        let Value::Mapping(mut fields) = column else {
            continue;
        };
        let Some(Value::String(name)) = fields.shift_remove(&Value::String("name".to_string()))
        else {
            continue;
        };
        for (k, v) in fields {
            flatten(format!("columns.{name}.{}", yaml_key_text(&k)), v, options);
        }
    }
}

/// The statement that recreates `component`. `redact` says whether the value
/// of a connector parameter is shown as `'***'`.
///
/// # Errors
///
/// Returns an error when the component cannot be read as a Spicepod value.
pub fn create_statement<T: SqlComponent>(
    component: &T,
    redact: &dyn Fn(&str, &OptionValue) -> bool,
) -> DFResult<String> {
    let kind = T::KIND;
    let options = component_options(component)?;
    let mut sql = format!(
        "CREATE {} {}",
        kind.sql_object(),
        object_name_sql(component.component_name())
    );
    if options.is_empty() {
        return Ok(sql);
    }
    sql.push_str(" WITH (");
    for (i, option) in options.iter().enumerate() {
        if i > 0 {
            sql.push(',');
        }
        let value = if is_param_key(kind, &option.key) && redact(&option.key, &option.value) {
            REDACTED_SQL.to_string()
        } else {
            option.value.to_sql()
        };
        let _ = write!(sql, "\n  {} = {value}", quote_identifier(&option.key));
    }
    sql.push_str("\n)");
    Ok(sql)
}

fn is_param_key(kind: ComponentKind, key: &str) -> bool {
    matches!(resolve_key(kind, key, None), Ok(KeyPath::Param(_)))
}

/// A component name as SQL: each part of a dotted name quoted as it needs.
///
/// A Spicepod name is read the way SQL reads a table reference, so `sales.orders`
/// is the table `orders` in the schema `sales`.
#[must_use]
pub fn object_name_sql(name: &str) -> String {
    let reference = datafusion::common::TableReference::parse_str(name);
    let mut parts = Vec::with_capacity(3);
    if let Some(catalog) = reference.catalog() {
        parts.push(quote_identifier(catalog));
    }
    if let Some(schema) = reference.schema() {
        parts.push(quote_identifier(schema));
    }
    parts.push(quote_identifier(reference.table()));
    parts.join(".")
}

/// The name a statement gives a component, in the Spicepod's spelling: the
/// object name as SQL wrote it, so quoted parts keep their case.
#[must_use]
pub fn component_name_from_sql(name: &ObjectName) -> String {
    name.to_string()
}

// ── CREATE TABLE clauses ──────────────────────────────────────────────────────

/// The dataset fields a `CREATE TABLE` statement's clauses set: its column
/// list, `PRIMARY KEY`, `PARTITION BY`, `CLUSTER BY`, and `COMMENT`. Every other
/// clause has no Spicepod field and is refused.
///
/// # Errors
///
/// Returns an error for a clause or column option a dataset cannot express.
pub fn dataset_clause_fields(create: &CreateTable) -> DFResult<ClauseFields> {
    refuse_unsupported_clauses(create)?;
    let mut clauses = ClauseFields::default();
    let mut primary_key: Vec<String> = Vec::new();

    for column in &create.columns {
        let name = column.name.value.clone();
        let mut entry = Mapping::new();
        entry.insert(
            Value::String("name".to_string()),
            Value::String(name.clone()),
        );
        entry.insert(
            Value::String("type".to_string()),
            Value::String(column.data_type.to_string()),
        );
        for option in &column.options {
            match &option.option {
                ColumnOption::NotNull => {
                    entry.insert(Value::String("nullable".to_string()), Value::Bool(false));
                }
                ColumnOption::Null => {
                    entry.insert(Value::String("nullable".to_string()), Value::Bool(true));
                }
                ColumnOption::Comment(comment) => {
                    entry.insert(
                        Value::String("description".to_string()),
                        Value::String(comment.clone()),
                    );
                }
                ColumnOption::PrimaryKey(_) => primary_key.push(name.clone()),
                other => {
                    return Err(DataFusionError::Plan(format!(
                        "Column option '{other}' on column '{name}' is not supported for a dataset. A column declares its type, NOT NULL, and COMMENT."
                    )));
                }
            }
        }
        clauses.fields.push((
            vec!["columns".to_string(), name.clone()],
            Value::Mapping(entry),
            format!("The column list's '{name}'"),
        ));
    }

    for constraint in &create.constraints {
        match constraint {
            TableConstraint::PrimaryKey(key) => {
                for column in &key.columns {
                    match &column.column.expr {
                        Expr::Identifier(ident) => primary_key.push(ident.value.clone()),
                        other => {
                            return Err(DataFusionError::Plan(format!(
                                "PRIMARY KEY ({other}) is not supported. List the key's column names."
                            )));
                        }
                    }
                }
            }
            other => {
                return Err(DataFusionError::Plan(format!(
                    "The constraint '{other}' is not supported for a dataset. A dataset declares a PRIMARY KEY."
                )));
            }
        }
    }
    if !primary_key.is_empty() {
        let key = if primary_key.len() == 1 {
            primary_key.remove(0)
        } else {
            format!("({})", primary_key.join(", "))
        };
        clauses.set(
            &["acceleration", "primary_key"],
            Value::String(key),
            "PRIMARY KEY",
        );
    }

    if let Some(partition_by) = &create.partition_by {
        clauses.set(
            &["acceleration", "partition_by"],
            Value::Sequence(vec![Value::String(partition_by.to_string())]),
            "PARTITION BY",
        );
    }

    if let Some(cluster_by) = &create.cluster_by {
        let exprs = match cluster_by {
            WrappedCollection::Parentheses(exprs) => exprs.clone(),
            WrappedCollection::NoWrapping(exprs) => match exprs.as_slice() {
                [Expr::Tuple(items)] => items.clone(),
                _ => exprs.clone(),
            },
        };
        let columns = exprs
            .iter()
            .map(|expr| match expr {
                Expr::Identifier(ident) => Ok(ident.value.clone()),
                Expr::Nested(inner) => match inner.as_ref() {
                    Expr::Identifier(ident) => Ok(ident.value.clone()),
                    other => Err(other.to_string()),
                },
                other => Err(other.to_string()),
            })
            .collect::<Result<Vec<_>, _>>()
            .map_err(|expr| {
                DataFusionError::Plan(format!(
                    "CLUSTER BY ({expr}) is not supported. CLUSTER BY lists column names."
                ))
            })?;
        clauses.set(
            &["acceleration", "params", "cayenne_cluster_by"],
            Value::String(columns.join(",")),
            "CLUSTER BY",
        );
    }

    if let Some(comment) = &create.comment {
        let text = match comment {
            datafusion::sql::sqlparser::ast::CommentDef::WithEq(text)
            | datafusion::sql::sqlparser::ast::CommentDef::WithoutEq(text) => text.clone(),
        };
        clauses.set(&["description"], Value::String(text), "COMMENT");
    }

    Ok(clauses)
}

fn refuse_unsupported_clauses(create: &CreateTable) -> DFResult<()> {
    let unsupported = [
        (create.temporary, "TEMPORARY"),
        (create.external, "EXTERNAL"),
        (create.global.is_some(), "GLOBAL or LOCAL"),
        (create.transient, "TRANSIENT"),
        (create.volatile, "VOLATILE"),
        (create.iceberg, "ICEBERG"),
        (create.like.is_some(), "LIKE"),
        (create.clone.is_some(), "CLONE"),
        (create.location.is_some(), "LOCATION"),
        (create.file_format.is_some(), "STORED AS"),
        (create.order_by.is_some(), "ORDER BY"),
        (create.primary_key.is_some(), "PRIMARY KEY expression"),
        (create.on_commit.is_some(), "ON COMMIT"),
        (create.on_cluster.is_some(), "ON CLUSTER"),
        (create.clustered_by.is_some(), "CLUSTERED BY"),
    ];
    if let Some((_, clause)) = unsupported.iter().find(|(set, _)| *set) {
        return Err(DataFusionError::Plan(format!(
            "CREATE TABLE ... {clause} is not supported for a dataset. Define the dataset with WITH (from = '...', ...)."
        )));
    }
    match &create.table_options {
        CreateTableOptions::None | CreateTableOptions::With(_) => Ok(()),
        other => Err(DataFusionError::Plan(format!(
            "Table options '{other}' are not supported for a dataset. Write them as WITH (key = value, ...)."
        ))),
    }
}

/// A statement's `WITH (...)` options, or none.
#[must_use]
pub fn create_table_with_options(create: &CreateTable) -> &[SqlOption] {
    match &create.table_options {
        CreateTableOptions::With(options) => options,
        _ => &[],
    }
}

// ── Option lists in statement text ───────────────────────────────────────────

/// One `key = value` of a `WITH (...)` or `SET (...)` list, or one key of a
/// `RESET (...)` list, where the statement text has it.
#[derive(Debug, Clone, PartialEq)]
struct OptionInText {
    /// The key's dot-separated parts, unquoted.
    parts: Vec<String>,
    /// Whether the key is unquoted parts joined by `.`.
    dotted: bool,
    /// The byte range of the key.
    key: std::ops::Range<usize>,
    /// The value, when the key has one.
    value: Option<ValueInText>,
}

/// An option's value where the statement text has it: every token from the
/// `=` to the `,` or `)` that ends the option, in any spelling the option
/// reader takes.
#[derive(Debug, Clone, PartialEq)]
struct ValueInText {
    /// The byte range of the whole value.
    range: std::ops::Range<usize>,
    /// The text of the value, when it is one string literal of any quoting.
    string: Option<String>,
}

/// The text of a string literal token, in any quoting.
fn string_literal(token: &Token) -> Option<&str> {
    match token {
        Token::SingleQuotedString(s)
        | Token::DoubleQuotedString(s)
        | Token::TripleSingleQuotedString(s)
        | Token::TripleDoubleQuotedString(s)
        | Token::EscapedStringLiteral(s)
        | Token::UnicodeStringLiteral(s)
        | Token::NationalStringLiteral(s) => Some(s),
        Token::DollarQuotedString(s) => Some(&s.value),
        Token::Word(word) if word.quote_style.is_some() => Some(&word.value),
        _ => None,
    }
}

/// Whether `sql` starts with `CREATE` or `ALTER`, the statements with option
/// lists: a check cheap enough to run on every query.
fn starts_a_definition(sql: &str) -> bool {
    let first_word = first_word(sql);
    ["create", "alter", "explain"]
        .iter()
        .any(|word| first_word.eq_ignore_ascii_case(word))
}

/// The first word of `sql`, after any whitespace and comments: what a cheap
/// check of the kind of statement reads, before the statement is tokenized.
#[must_use]
pub fn first_word(sql: &str) -> &str {
    let mut rest = sql;
    loop {
        rest = rest.trim_start();
        if let Some(comment) = rest.strip_prefix("--") {
            rest = comment.split_once('\n').map_or("", |(_, after)| after);
        } else if let Some(comment) = rest.strip_prefix("/*") {
            rest = comment.split_once("*/").map_or("", |(_, after)| after);
        } else {
            break;
        }
    }
    let end = rest
        .find(|c: char| !c.is_ascii_alphabetic())
        .unwrap_or(rest.len());
    &rest[..end]
}

/// The byte offset of each line start in `sql`, to turn token locations into
/// byte offsets.
fn line_starts(sql: &str) -> Vec<usize> {
    std::iter::once(0)
        .chain(sql.match_indices('\n').map(|(i, _)| i + 1))
        .collect()
}

fn byte_offset(sql: &str, line_starts: &[usize], location: Location) -> Option<usize> {
    let line = usize::try_from(location.line).ok()?.checked_sub(1)?;
    let column = usize::try_from(location.column).ok()?.checked_sub(1)?;
    let start = *line_starts.get(line)?;
    let rest = sql.get(start..)?;
    Some(
        start
            + rest
                .char_indices()
                .nth(column)
                .map_or(rest.len(), |(offset, _)| offset),
    )
}

/// The options of every `WITH (...)`, `SET (...)`, and `RESET (...)` list in
/// `sql`, a `CREATE` or `ALTER` statement. `None` when the text does not
/// tokenize.
fn options_in_text(sql: &str, dialect: &dyn Dialect) -> Option<Vec<OptionInText>> {
    let tokens = Tokenizer::new(dialect, sql).tokenize_with_location().ok()?;
    let starts = line_starts(sql);
    let significant: Vec<&TokenWithSpan> = tokens
        .iter()
        .filter(|t| !matches!(t.token, Token::Whitespace(_)))
        .collect();
    let range = |first: &TokenWithSpan, last: &TokenWithSpan| -> Option<std::ops::Range<usize>> {
        Some(
            byte_offset(sql, &starts, first.span.start)?..byte_offset(sql, &starts, last.span.end)?,
        )
    };

    let mut options = Vec::new();
    let mut depth = 0_usize;
    let mut i = 0;
    while i < significant.len() {
        let token = &significant[i].token;
        match token {
            Token::LParen => depth += 1,
            Token::RParen => depth = depth.saturating_sub(1),
            Token::Word(word)
                if depth == 0
                    && matches!(word.keyword, Keyword::WITH | Keyword::SET | Keyword::RESET)
                    && matches!(
                        significant.get(i + 1).map(|t| &t.token),
                        Some(Token::LParen)
                    ) =>
            {
                // An option list: `key [= value]` items at depth 1.
                let mut j = i + 2;
                let mut list_depth = 1_usize;
                let mut at_key = true;
                while j < significant.len() && list_depth > 0 {
                    match &significant[j].token {
                        Token::LParen => {
                            list_depth += 1;
                            j += 1;
                        }
                        Token::RParen => {
                            list_depth -= 1;
                            j += 1;
                        }
                        Token::Comma if list_depth == 1 => {
                            at_key = true;
                            j += 1;
                        }
                        Token::Word(_)
                        | Token::SingleQuotedString(_)
                        | Token::DoubleQuotedString(_)
                            if list_depth == 1 && at_key =>
                        {
                            // The key: a word, or words joined by `.`.
                            let first = j;
                            let mut parts = Vec::new();
                            let mut dotted = true;
                            loop {
                                match &significant[j].token {
                                    Token::Word(word) => {
                                        dotted &= word.quote_style.is_none();
                                        parts.push(word.value.clone());
                                    }
                                    Token::SingleQuotedString(s) | Token::DoubleQuotedString(s) => {
                                        dotted = false;
                                        parts.push(s.clone());
                                    }
                                    _ => break,
                                }
                                if matches!(
                                    significant.get(j + 1).map(|t| &t.token),
                                    Some(Token::Period)
                                ) && matches!(
                                    significant.get(j + 2).map(|t| &t.token),
                                    Some(Token::Word(_))
                                ) {
                                    j += 2;
                                } else {
                                    break;
                                }
                            }
                            let last = j;
                            j += 1;
                            let mut value = None;
                            if matches!(significant.get(j).map(|t| &t.token), Some(Token::Eq)) {
                                // The value runs to the `,` or `)` that ends the option.
                                let start = j + 1;
                                let mut end = start;
                                let mut nested = 0_usize;
                                while let Some(token) = significant.get(end) {
                                    match &token.token {
                                        Token::LParen | Token::LBracket => nested += 1,
                                        Token::RParen | Token::RBracket if nested > 0 => {
                                            nested -= 1;
                                        }
                                        Token::RParen | Token::Comma => break,
                                        _ => {}
                                    }
                                    end += 1;
                                }
                                if end > start
                                    && let Some(value_range) =
                                        range(significant[start], significant[end - 1])
                                {
                                    value = Some(ValueInText {
                                        range: value_range,
                                        string: (end == start + 1)
                                            .then(|| string_literal(&significant[start].token))
                                            .flatten()
                                            .map(str::to_string),
                                    });
                                }
                                j = end;
                            }
                            if let Some(key) = range(significant[first], significant[last]) {
                                options.push(OptionInText {
                                    dotted: dotted && parts.len() > 1,
                                    parts,
                                    key,
                                    value,
                                });
                            }
                            at_key = false;
                        }
                        _ => {
                            at_key = false;
                            j += 1;
                        }
                    }
                }
                i = j;
                continue;
            }
            _ => {}
        }
        i += 1;
    }
    Some(options)
}

/// `sql` with each replacement `(range, text)` applied; the ranges do not
/// overlap.
fn splice(sql: &str, mut replacements: Vec<(std::ops::Range<usize>, String)>) -> String {
    replacements.sort_by_key(|(range, _)| std::cmp::Reverse(range.start));
    let mut text = sql.to_string();
    for (range, replacement) in replacements {
        text.replace_range(range, &replacement);
    }
    text
}

/// `sql` with each unquoted dotted option key quoted, so
/// `WITH (acceleration.engine = 'cayenne')` reads as
/// `WITH ("acceleration.engine" = 'cayenne')`: the SQL parser takes an option
/// key as one identifier. Everything else in the text is kept as written.
#[must_use]
pub fn quote_dotted_option_keys<'a>(sql: &'a str, dialect: &dyn Dialect) -> Cow<'a, str> {
    if !starts_a_definition(sql) {
        return Cow::Borrowed(sql);
    }
    let Some(options) = options_in_text(sql, dialect) else {
        return Cow::Borrowed(sql);
    };
    let replacements: Vec<_> = options
        .into_iter()
        .filter(|option| option.dotted)
        .map(|option| {
            let key = option.parts.join(".").replace('"', "\"\"");
            (option.key, format!("\"{key}\""))
        })
        .collect();
    if replacements.is_empty() {
        Cow::Borrowed(sql)
    } else {
        Cow::Owned(splice(sql, replacements))
    }
}

/// `sql` with the value of each option whose key `secret` names replaced by
/// the redaction placeholder, whatever its spelling, unless the value is a
/// `${ ... }` reference: the text to record of a statement that may carry a
/// credential. Everything else in the text is kept as written.
#[must_use]
pub fn redact_secret_option_values<'a>(
    sql: &'a str,
    dialect: &dyn Dialect,
    secret: &dyn Fn(&str) -> bool,
) -> Cow<'a, str> {
    if !starts_a_definition(sql) {
        return Cow::Borrowed(sql);
    }
    let Some(options) = options_in_text(sql, dialect) else {
        return Cow::Borrowed(sql);
    };
    let replacements: Vec<_> = options
        .into_iter()
        .filter_map(|option| {
            let key = option.parts.join(".");
            let key = key.strip_prefix("params.").unwrap_or(&key).to_string();
            let value = option.value?;
            let reference = value
                .string
                .as_deref()
                .is_some_and(|s| s.trim_start().starts_with("${"));
            (secret(&key) && !reference).then(|| (value.range, REDACTED_SQL.to_string()))
        })
        .collect();
    if replacements.is_empty() {
        Cow::Borrowed(sql)
    } else {
        Cow::Owned(splice(sql, replacements))
    }
}

#[cfg(test)]
mod tests;
