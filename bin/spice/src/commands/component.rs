/*
Copyright 2026 The Spice.ai OSS Authors

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

//! Manifest editing commands for Spicepod sections.

use crate::error::{ConfigIoSnafu, InvalidArgumentSnafu, Result};
use crate::manifest;
use clap::{Args, Subcommand, ValueHint};
use snafu::{ResultExt, ensure};
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use yaml::{Mapping, Value};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ComponentSection {
    Catalog,
    Dataset,
    View,
    Model,
    Embedding,
    Reranker,
    Tool,
    Worker,
    Function,
    Secret,
}

impl ComponentSection {
    const fn field(self) -> &'static str {
        match self {
            Self::Catalog => "catalogs",
            Self::Dataset => "datasets",
            Self::View => "views",
            Self::Model => "models",
            Self::Embedding => "embeddings",
            Self::Reranker => "rerankers",
            Self::Tool => "tools",
            Self::Worker => "workers",
            Self::Function => "functions",
            Self::Secret => "secrets",
        }
    }

    const fn label(self) -> &'static str {
        match self {
            Self::Catalog => "catalog",
            Self::Dataset => "dataset",
            Self::View => "view",
            Self::Model => "model",
            Self::Embedding => "embedding",
            Self::Reranker => "reranker",
            Self::Tool => "tool",
            Self::Worker => "worker",
            Self::Function => "function",
            Self::Secret => "secret",
        }
    }

    const fn supports_references(self) -> bool {
        !matches!(self, Self::Secret)
    }

    const fn requires_from_for_add(self) -> bool {
        matches!(
            self,
            Self::Catalog
                | Self::Dataset
                | Self::Model
                | Self::Embedding
                | Self::Reranker
                | Self::Tool
                | Self::Function
                | Self::Secret
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SingletonSection {
    Runtime,
    Management,
    Snapshots,
}

impl SingletonSection {
    const fn field(self) -> &'static str {
        match self {
            Self::Runtime => "runtime",
            Self::Management => "management",
            Self::Snapshots => "snapshots",
        }
    }

    const fn label(self) -> &'static str {
        match self {
            Self::Runtime => "runtime",
            Self::Management => "management",
            Self::Snapshots => "snapshots",
        }
    }
}

/// Long help text shared by `spice <component>` commands (model, catalog, view, ...).
pub const COMPONENT_LONG_ABOUT: &str = r#"Add or configure a component entry in `spicepod.yaml`.

This command is used for the model, catalog, view, embedding, reranker, tool,
worker, function, and secret sections of a Spicepod. The component section is
determined by the parent command (e.g. `spice model ...` edits `models:`).

USAGE
  spice <section> add <name>          [body flags]   # add a new entry; fails if it exists
  spice <section> configure <name>    [body flags]   # add or update an entry in place
  spice <section> add --ref <path>                   # add a Spicepod reference instead of inline

BODY FLAGS
  --from <SOURCE>           Provider/URI for an inline component (e.g. openai:gpt-4o-mini, s3://bucket/data)
  --description <TEXT>      Human-readable description
  --sql <SQL> | --sql-ref <PATH>     Inline or referenced SQL (views, workers, functions)
  --cron <CRON>             Cron expression (workers)
  --body <BODY> | --body-ref <PATH>  Inline or referenced function body
  --param KEY=VALUE         Add a `params:` entry. Stored as a YAML string by default;
                            prefix the value with `yaml:` to parse it as a typed YAML value.
  --env KEY=VALUE           Add an `env:` entry (same string-vs-yaml rules as --param)
  --depends-on NAME         Append to `dependsOn:`
  --set PATH=VALUE          Set any schema field by dotted path; VALUE is parsed as YAML
  --enable | --disable      Set `enabled: true` / `enabled: false`
  --file <PATH> | --stdin   Read the inline body from a YAML/JSON file or stdin
  --manifest <PATH>         Edit a non-default Spicepod file

EXAMPLES
  # Add an OpenAI chat model
  spice model add llm --from openai:gpt-4o-mini \
      --param openai_api_key='${ secrets:OPENAI_API_KEY }'

  # Update a model's allowed datasets
  spice model configure llm --set datasets='[documents, orders]'

  # Add a Databricks Unity catalog
  spice catalog add tpch --from databricks \
      --param databricks_endpoint=https://example.cloud.databricks.com

  # Add a SQL view
  spice view add recent_orders --sql "select * from orders limit 100"

  # Add an MCP-backed tool with a secret-bound token
  spice tool add lookup --from mcp:server \
      --env TOKEN='${ secrets:TOKEN }'

  # Reference an external component definition file
  spice model add --ref models/llm.yaml

Docs: https://spiceai.org/docs"#;

/// Long help text shared by `spice <singleton>` commands (runtime, management, snapshots).
pub const SINGLETON_LONG_ABOUT: &str = r#"Configure a singleton (one-of) section of `spicepod.yaml` such as
`runtime:`, `management:`, or `snapshots:`.

USAGE
  spice <section> configure [body flags]

BODY FLAGS
  --set PATH=VALUE          Set any schema field by dotted path; VALUE is parsed as YAML
  --param KEY=VALUE         Add a `params:` entry (string by default; prefix `yaml:` for typed)
  --enable | --disable      Set `enabled: true` / `enabled: false`
  --api-key <KEY>           Convenience for `management.api_key`
  --location <URI>          Convenience for `snapshots.location`
  --file <PATH> | --stdin   Replace the section body from a YAML/JSON file or stdin
  --manifest <PATH>         Edit a non-default Spicepod file

EXAMPLES
  # Set the management API key
  spice management configure --api-key '${ secrets:MGMT_KEY }'

  # Point snapshots at an S3 location
  spice snapshots configure --location s3://my-bucket/snapshots

  # Tweak runtime parameters
  spice runtime configure --set telemetry.enabled=true

Docs: https://spiceai.org/docs"#;

/// Long help text for `spice extension`.
pub const EXTENSION_LONG_ABOUT: &str = r#"Add or configure entries under the `extensions:` section of `spicepod.yaml`.

USAGE
  spice extension add <name>       [body flags]   # add a new extension entry; fails if it exists
  spice extension configure <name> [body flags]   # add or update an extension in place

BODY FLAGS
  --set PATH=VALUE          Set any schema field by dotted path; VALUE is parsed as YAML
  --param KEY=VALUE         Add to the extension's `params:` map
  --enable | --disable      Set `enabled: true` / `enabled: false`
  --file <PATH> | --stdin   Replace the extension body from a YAML/JSON file or stdin
  --manifest <PATH>         Edit a non-default Spicepod file

EXAMPLES
  spice extension add memory --param store=redis --param ttl=3600
  spice extension configure memory --enable

Docs: https://spiceai.org/docs"#;

/// Long help text for `spice metadata`.
pub const METADATA_LONG_ABOUT: &str = r#"Add, update, or set entries under the `metadata:` section of `spicepod.yaml`.

USAGE
  spice metadata add KEY=VALUE [KEY=VALUE ...]    # add entries; fails if a key already exists
  spice metadata configure KEY=VALUE [...]        # add or overwrite entries
  spice metadata set <KEY> <VALUE>                # set exactly one entry

Values are stored as YAML strings by default. Prefix the value with `yaml:` to
parse it as a typed YAML value (numbers, booleans, lists, mappings).

EXAMPLES
  spice metadata add owner=data-team env=prod
  spice metadata set replicas yaml:3
  spice metadata configure tags='yaml:[ai, search]'

Docs: https://spiceai.org/docs"#;

#[derive(Args, Debug)]
#[command(
    about = "Add or configure a component entry in spicepod.yaml",
    long_about = COMPONENT_LONG_ABOUT,
)]
pub struct ComponentArgs {
    #[command(subcommand)]
    pub command: ComponentCommand,
}

#[derive(Subcommand, Debug)]
pub enum ComponentCommand {
    /// Add a new component or component reference
    Add(ComponentAddArgs),

    /// Create or update a component by name
    Configure(ComponentConfigureArgs),
}

#[derive(Args, Debug, Default)]
pub struct ComponentAddArgs {
    /// Component name for inline definitions
    pub name: Option<String>,

    #[command(flatten)]
    pub options: CommonComponentOptions,
}

#[derive(Args, Debug, Default)]
pub struct ComponentConfigureArgs {
    /// Component name to create or update
    pub name: Option<String>,

    #[command(flatten)]
    pub options: CommonComponentOptions,
}

impl ComponentConfigureArgs {
    #[must_use]
    pub fn has_manifest_edits(&self) -> bool {
        self.name.is_some() || self.options.has_component_body()
    }
}

#[derive(Args, Debug, Default)]
pub struct CommonComponentOptions {
    /// Path to the Spicepod manifest to edit
    #[arg(long, value_hint = ValueHint::FilePath)]
    pub manifest: Option<PathBuf>,

    /// Source URI/provider for inline components
    #[arg(long = "from", value_name = "SOURCE")]
    pub from: Option<String>,

    /// Add a component reference instead of an inline definition
    #[arg(long = "ref", value_name = "PATH")]
    pub reference: Option<String>,

    /// Read the inline component body from a YAML or JSON file
    #[arg(long, value_name = "PATH", value_hint = ValueHint::FilePath)]
    pub file: Option<PathBuf>,

    /// Read the inline component body from stdin
    #[arg(long)]
    pub stdin: bool,

    /// Component description
    #[arg(long, value_name = "TEXT")]
    pub description: Option<String>,

    /// Inline SQL for views, workers, or SQL functions
    #[arg(long, value_name = "SQL")]
    pub sql: Option<String>,

    /// SQL file reference for views
    #[arg(long = "sql-ref", value_name = "PATH")]
    pub sql_ref: Option<String>,

    /// Cron expression for workers
    #[arg(long, value_name = "CRON")]
    pub cron: Option<String>,

    /// Inline function body
    #[arg(long, value_name = "BODY")]
    pub body: Option<String>,

    /// Function body file reference
    #[arg(long = "body-ref", value_name = "PATH")]
    pub body_ref: Option<String>,

    /// Set a schema field using a dotted path and YAML value
    #[arg(long = "set", value_name = "PATH=VALUE")]
    pub set: Vec<String>,

    /// Set a params entry
    #[arg(long = "param", value_name = "KEY=VALUE")]
    pub params: Vec<String>,

    /// Set an env entry
    #[arg(long = "env", value_name = "KEY=VALUE")]
    pub env: Vec<String>,

    /// Add a dependsOn entry
    #[arg(long = "depends-on", value_name = "NAME")]
    pub depends_on: Vec<String>,

    /// Set enabled: true
    #[arg(long, conflicts_with = "disable")]
    pub enable: bool,

    /// Set enabled: false
    #[arg(long)]
    pub disable: bool,
}

impl CommonComponentOptions {
    fn has_component_body(&self) -> bool {
        self.from.is_some()
            || self.reference.is_some()
            || self.file.is_some()
            || self.stdin
            || self.description.is_some()
            || self.sql.is_some()
            || self.sql_ref.is_some()
            || self.cron.is_some()
            || self.body.is_some()
            || self.body_ref.is_some()
            || !self.set.is_empty()
            || !self.params.is_empty()
            || !self.env.is_empty()
            || !self.depends_on.is_empty()
            || self.enable
            || self.disable
    }

    fn has_reference_conflicts(&self, name: Option<&str>) -> bool {
        name.is_some()
            || self.from.is_some()
            || self.file.is_some()
            || self.stdin
            || self.description.is_some()
            || self.sql.is_some()
            || self.sql_ref.is_some()
            || self.cron.is_some()
            || self.body.is_some()
            || self.body_ref.is_some()
            || !self.set.is_empty()
            || !self.params.is_empty()
            || !self.env.is_empty()
            || self.enable
            || self.disable
    }
}

#[derive(Args, Debug)]
#[command(
    about = "Configure a singleton Spicepod section",
    long_about = SINGLETON_LONG_ABOUT,
)]
pub struct SingletonArgs {
    #[command(subcommand)]
    pub command: SingletonCommand,
}

#[derive(Subcommand, Debug)]
pub enum SingletonCommand {
    /// Create or update this section
    Configure(SingletonConfigureArgs),
}

#[derive(Args, Debug, Default)]
pub struct SingletonConfigureArgs {
    /// Path to the Spicepod manifest to edit
    #[arg(long, value_hint = ValueHint::FilePath)]
    pub manifest: Option<PathBuf>,

    /// Read the section body from a YAML or JSON file
    #[arg(long, value_name = "PATH", value_hint = ValueHint::FilePath)]
    pub file: Option<PathBuf>,

    /// Read the section body from stdin
    #[arg(long)]
    pub stdin: bool,

    /// Set a schema field using a dotted path and YAML value
    #[arg(long = "set", value_name = "PATH=VALUE")]
    pub set: Vec<String>,

    /// Set a params entry
    #[arg(long = "param", value_name = "KEY=VALUE")]
    pub params: Vec<String>,

    /// Set enabled: true
    #[arg(long, conflicts_with = "disable")]
    pub enable: bool,

    /// Set enabled: false
    #[arg(long)]
    pub disable: bool,

    /// Management API key
    #[arg(long = "api-key", value_name = "KEY")]
    pub api_key: Option<String>,

    /// Snapshot object store location
    #[arg(long, value_name = "URI")]
    pub location: Option<String>,
}

impl SingletonConfigureArgs {
    fn has_updates(&self) -> bool {
        self.file.is_some()
            || self.stdin
            || !self.set.is_empty()
            || !self.params.is_empty()
            || self.enable
            || self.disable
            || self.api_key.is_some()
            || self.location.is_some()
    }
}

#[derive(Args, Debug)]
#[command(
    about = "Add or configure entries under `extensions:` in spicepod.yaml",
    long_about = EXTENSION_LONG_ABOUT,
)]
pub struct ExtensionArgs {
    #[command(subcommand)]
    pub command: ExtensionCommand,
}

#[derive(Subcommand, Debug)]
pub enum ExtensionCommand {
    /// Add a new extension entry
    Add(ExtensionEditArgs),

    /// Create or update an extension entry
    Configure(ExtensionEditArgs),
}

#[derive(Args, Debug, Default)]
pub struct ExtensionEditArgs {
    /// Extension name
    pub name: String,

    /// Path to the Spicepod manifest to edit
    #[arg(long, value_hint = ValueHint::FilePath)]
    pub manifest: Option<PathBuf>,

    /// Read the extension body from a YAML or JSON file
    #[arg(long, value_name = "PATH", value_hint = ValueHint::FilePath)]
    pub file: Option<PathBuf>,

    /// Read the extension body from stdin
    #[arg(long)]
    pub stdin: bool,

    /// Set a schema field using a dotted path and YAML value
    #[arg(long = "set", value_name = "PATH=VALUE")]
    pub set: Vec<String>,

    /// Set an extension params entry
    #[arg(long = "param", value_name = "KEY=VALUE")]
    pub params: Vec<String>,

    /// Set enabled: true
    #[arg(long, conflicts_with = "disable")]
    pub enable: bool,

    /// Set enabled: false
    #[arg(long)]
    pub disable: bool,
}

#[derive(Args, Debug)]
#[command(
    about = "Add or configure key/value entries under `metadata:` in spicepod.yaml",
    long_about = METADATA_LONG_ABOUT,
)]
pub struct MetadataArgs {
    #[command(subcommand)]
    pub command: MetadataCommand,
}

#[derive(Subcommand, Debug)]
pub enum MetadataCommand {
    /// Add metadata entries, failing if a key already exists
    Add(MetadataEditArgs),

    /// Create or update metadata entries
    Configure(MetadataEditArgs),

    /// Create or update one metadata entry
    Set(MetadataSetArgs),
}

#[derive(Args, Debug, Default)]
pub struct MetadataEditArgs {
    /// Metadata entries as KEY=VALUE. Values are stored as strings unless prefixed with yaml:.
    #[arg(value_name = "KEY=VALUE")]
    pub entries: Vec<String>,

    /// Path to the Spicepod manifest to edit
    #[arg(long, value_hint = ValueHint::FilePath)]
    pub manifest: Option<PathBuf>,

    /// Set metadata entries as KEY=VALUE. Values are stored as strings unless prefixed with yaml:.
    #[arg(long = "set", value_name = "KEY=VALUE")]
    pub set: Vec<String>,
}

#[derive(Args, Debug)]
pub struct MetadataSetArgs {
    /// Metadata key
    pub key: String,

    /// Metadata value. Stored as a string unless prefixed with yaml:.
    pub value: String,

    /// Path to the Spicepod manifest to edit
    #[arg(long, value_hint = ValueHint::FilePath)]
    pub manifest: Option<PathBuf>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MutationMode {
    Add,
    Configure,
}

pub fn execute_component(section: ComponentSection, args: &ComponentArgs) -> Result<()> {
    match &args.command {
        ComponentCommand::Add(add_args) => add_component(section, add_args),
        ComponentCommand::Configure(configure_args) => configure_component(section, configure_args),
    }
}

pub fn add_component(section: ComponentSection, args: &ComponentAddArgs) -> Result<()> {
    let (manifest_path, mut spicepod, created) = load_manifest(args.options.manifest.as_deref())?;
    let before = spicepod.clone();
    mutate_component(
        &mut spicepod,
        section,
        args.name.as_deref(),
        &args.options,
        MutationMode::Add,
    )?;
    write_if_changed(&manifest_path, &spicepod, &before, created)
}

pub fn configure_component(section: ComponentSection, args: &ComponentConfigureArgs) -> Result<()> {
    let (manifest_path, mut spicepod, created) = load_manifest(args.options.manifest.as_deref())?;
    let before = spicepod.clone();
    mutate_component(
        &mut spicepod,
        section,
        args.name.as_deref(),
        &args.options,
        MutationMode::Configure,
    )?;
    write_if_changed(&manifest_path, &spicepod, &before, created)
}

pub fn execute_singleton(section: SingletonSection, args: &SingletonArgs) -> Result<()> {
    match &args.command {
        SingletonCommand::Configure(configure_args) => configure_singleton(section, configure_args),
    }
}

pub fn execute_extension(args: &ExtensionArgs) -> Result<()> {
    match &args.command {
        ExtensionCommand::Add(edit_args) => mutate_extension(edit_args, MutationMode::Add),
        ExtensionCommand::Configure(edit_args) => {
            mutate_extension(edit_args, MutationMode::Configure)
        }
    }
}

pub fn execute_metadata(args: &MetadataArgs) -> Result<()> {
    match &args.command {
        MetadataCommand::Add(edit_args) => mutate_metadata(edit_args, MutationMode::Add),
        MetadataCommand::Configure(edit_args) => {
            mutate_metadata(edit_args, MutationMode::Configure)
        }
        MetadataCommand::Set(set_args) => {
            let edit_args = MetadataEditArgs {
                entries: vec![format!("{}={}", set_args.key, set_args.value)],
                manifest: set_args.manifest.clone(),
                set: Vec::new(),
            };
            mutate_metadata(&edit_args, MutationMode::Configure)
        }
    }
}

fn mutate_component(
    spicepod: &mut Value,
    section: ComponentSection,
    name: Option<&str>,
    options: &CommonComponentOptions,
    mode: MutationMode,
) -> Result<()> {
    if let Some(reference) = &options.reference {
        ensure!(
            section.supports_references(),
            InvalidArgumentSnafu {
                message: format!("{} entries do not support --ref", section.label()),
            }
        );
        ensure!(
            !options.has_reference_conflicts(name),
            InvalidArgumentSnafu {
                message: "--ref can only be combined with --manifest and --depends-on".to_string(),
            }
        );
        return mutate_component_reference(spicepod, section, reference, &options.depends_on, mode);
    }

    let component = build_component_value(section, name, options, mode)?;
    match mode {
        MutationMode::Add => add_component_value(spicepod, section, component),
        MutationMode::Configure => upsert_component_value(spicepod, section, component),
    }
}

fn build_component_value(
    section: ComponentSection,
    name: Option<&str>,
    options: &CommonComponentOptions,
    mode: MutationMode,
) -> Result<Value> {
    ensure_file_or_stdin(options.file.as_deref(), options.stdin)?;

    let mut value = read_optional_value(options.file.as_deref(), options.stdin)?
        .unwrap_or_else(|| Value::Mapping(Mapping::new()));
    ensure_mapping_value(&mut value, section.label())?;

    if let Some(component_name) = name {
        set_path(
            &mut value,
            "name",
            Value::String(component_name.to_string()),
        )?;
    }
    if let Some(from) = &options.from {
        set_path(&mut value, "from", Value::String(from.clone()))?;
    }
    if let Some(description) = &options.description {
        set_path(
            &mut value,
            "description",
            Value::String(description.clone()),
        )?;
    }
    if let Some(sql) = &options.sql {
        set_path(&mut value, "sql", Value::String(sql.clone()))?;
    }
    if let Some(sql_ref) = &options.sql_ref {
        set_path(&mut value, "sql_ref", Value::String(sql_ref.clone()))?;
    }
    if let Some(cron) = &options.cron {
        set_path(&mut value, "cron", Value::String(cron.clone()))?;
    }
    if let Some(body) = &options.body {
        set_path(&mut value, "body", Value::String(body.clone()))?;
    }
    if let Some(body_ref) = &options.body_ref {
        set_path(&mut value, "body_ref", Value::String(body_ref.clone()))?;
    }
    if options.enable || options.disable {
        set_path(&mut value, "enabled", Value::Bool(options.enable))?;
    }
    if !options.depends_on.is_empty() {
        set_path(
            &mut value,
            "dependsOn",
            string_sequence(&options.depends_on),
        )?;
    }
    for pair in &options.params {
        let (key, param_value) = parse_string_or_yaml_prefixed_pair(pair)?;
        set_path(&mut value, &format!("params.{key}"), param_value)?;
    }
    for pair in &options.env {
        let (key, env_value) = parse_string_pair(pair)?;
        set_path(&mut value, &format!("env.{key}"), Value::String(env_value))?;
    }
    for pair in &options.set {
        let (path, field_value) = parse_key_value(pair)?;
        set_path(&mut value, &path, field_value)?;
    }

    if mode == MutationMode::Add
        && section.requires_from_for_add()
        && value.get("from").and_then(Value::as_str).is_none()
    {
        return InvalidArgumentSnafu {
            message: format!(
                "{} add requires --from, --file, --stdin, or --ref",
                section.label()
            ),
        }
        .fail();
    }

    Ok(value)
}

fn add_component_value(
    spicepod: &mut Value,
    section: ComponentSection,
    component: Value,
) -> Result<()> {
    let new_component_name = component_name(&component).map(ToString::to_string);
    let sequence = ensure_sequence_field(spicepod, section.field())?;

    if let Some(name) = &new_component_name {
        ensure!(
            !sequence
                .iter()
                .any(|entry| component_name(entry).is_some_and(|existing| existing == name)),
            InvalidArgumentSnafu {
                message: format!(
                    "{} '{name}' already exists. Use `spice {} configure {name}` to update it.",
                    section.label(),
                    section.label()
                ),
            }
        );
    }

    sequence.push(component);
    Ok(())
}

fn upsert_component_value(
    spicepod: &mut Value,
    section: ComponentSection,
    component: Value,
) -> Result<()> {
    let name = component_name(&component)
        .ok_or_else(|| {
            InvalidArgumentSnafu {
                message: format!("{} configure requires a component name", section.label()),
            }
            .build()
        })?
        .to_string();

    let sequence = ensure_sequence_field(spicepod, section.field())?;
    for entry in sequence.iter_mut() {
        if component_name(entry).is_some_and(|existing| existing == name) {
            let target =
                entry
                    .as_mapping_mut()
                    .ok_or_else(|| crate::error::Error::ConfigParse {
                        message: format!(
                            "{} entry '{name}' must be a YAML mapping",
                            section.label()
                        ),
                    })?;
            let Value::Mapping(source) = component else {
                return Err(crate::error::Error::ConfigParse {
                    message: format!("{} entry '{name}' must be a YAML mapping", section.label()),
                });
            };
            merge_mapping(target, source);
            return Ok(());
        }
    }

    sequence.push(component);
    Ok(())
}

fn mutate_component_reference(
    spicepod: &mut Value,
    section: ComponentSection,
    reference: &str,
    depends_on: &[String],
    mode: MutationMode,
) -> Result<()> {
    let normalized_reference = manifest::path_to_spicepod_ref(Path::new(reference));
    let reference_value = build_reference_value(&normalized_reference, depends_on);
    let sequence = ensure_sequence_field(spicepod, section.field())?;

    for entry in sequence.iter_mut() {
        if entry
            .get("ref")
            .and_then(Value::as_str)
            .is_some_and(|existing_ref| existing_ref == normalized_reference)
        {
            if mode == MutationMode::Add {
                ensure!(
                    entry == &reference_value,
                    InvalidArgumentSnafu {
                        message: format!(
                            "{} reference '{normalized_reference}' already exists. Use configure to update it.",
                            section.label()
                        ),
                    }
                );
            } else {
                *entry = reference_value;
            }
            return Ok(());
        }
    }

    sequence.push(reference_value);
    Ok(())
}

fn configure_singleton(section: SingletonSection, args: &SingletonConfigureArgs) -> Result<()> {
    ensure!(
        args.has_updates(),
        InvalidArgumentSnafu {
            message: format!(
                "{} configure requires at least one value to set",
                section.label()
            ),
        }
    );
    ensure_file_or_stdin(args.file.as_deref(), args.stdin)?;

    let (manifest_path, mut spicepod, created) = load_manifest(args.manifest.as_deref())?;
    let before = spicepod.clone();
    let mut value = read_optional_value(args.file.as_deref(), args.stdin)?
        .unwrap_or_else(|| Value::Mapping(Mapping::new()));
    ensure_mapping_value(&mut value, section.label())?;

    if args.enable || args.disable {
        ensure!(
            section != SingletonSection::Runtime,
            InvalidArgumentSnafu {
                message: "runtime does not have an enabled field".to_string(),
            }
        );
        set_path(&mut value, "enabled", Value::Bool(args.enable))?;
    }
    if let Some(api_key) = &args.api_key {
        ensure!(
            section == SingletonSection::Management,
            InvalidArgumentSnafu {
                message: "--api-key is only valid for management configure".to_string(),
            }
        );
        set_path(&mut value, "api_key", Value::String(api_key.clone()))?;
    }
    if let Some(location) = &args.location {
        ensure!(
            section == SingletonSection::Snapshots,
            InvalidArgumentSnafu {
                message: "--location is only valid for snapshots configure".to_string(),
            }
        );
        set_path(&mut value, "location", Value::String(location.clone()))?;
    }
    for pair in &args.params {
        let (key, param_value) = parse_string_or_yaml_prefixed_pair(pair)?;
        set_path(&mut value, &format!("params.{key}"), param_value)?;
    }
    for pair in &args.set {
        let (path, field_value) = parse_key_value(pair)?;
        set_path(&mut value, &path, field_value)?;
    }

    merge_root_mapping_field(&mut spicepod, section.field(), value)?;
    write_if_changed(&manifest_path, &spicepod, &before, created)
}

fn mutate_extension(args: &ExtensionEditArgs, mode: MutationMode) -> Result<()> {
    ensure_file_or_stdin(args.file.as_deref(), args.stdin)?;

    let (manifest_path, mut spicepod, created) = load_manifest(args.manifest.as_deref())?;
    let before = spicepod.clone();
    let mut value = read_optional_value(args.file.as_deref(), args.stdin)?
        .unwrap_or_else(|| Value::Mapping(Mapping::new()));
    ensure_mapping_value(&mut value, "extension")?;

    if args.enable || args.disable {
        set_path(&mut value, "enabled", Value::Bool(args.enable))?;
    }
    for pair in &args.params {
        let (key, param_value) = parse_string_pair(pair)?;
        set_path(
            &mut value,
            &format!("params.{key}"),
            Value::String(param_value),
        )?;
    }
    for pair in &args.set {
        let (path, field_value) = parse_key_value(pair)?;
        set_path(&mut value, &path, field_value)?;
    }

    let extensions = ensure_mapping_field(&mut spicepod, "extensions")?;
    let key = Value::String(args.name.clone());
    if mode == MutationMode::Add {
        ensure!(
            !extensions.contains_key(&key),
            InvalidArgumentSnafu {
                message: format!(
                    "extension '{}' already exists. Use `spice extension configure {}` to update it.",
                    args.name, args.name
                ),
            }
        );
        extensions.insert(key, value);
    } else if let Some(existing) = extensions.get_mut(&key) {
        let target = existing
            .as_mapping_mut()
            .ok_or_else(|| crate::error::Error::ConfigParse {
                message: format!("extension '{}' must be a YAML mapping", args.name),
            })?;
        let Value::Mapping(source) = value else {
            return Err(crate::error::Error::ConfigParse {
                message: format!("extension '{}' must be a YAML mapping", args.name),
            });
        };
        merge_mapping(target, source);
    } else {
        extensions.insert(key, value);
    }

    write_if_changed(&manifest_path, &spicepod, &before, created)
}

fn mutate_metadata(args: &MetadataEditArgs, mode: MutationMode) -> Result<()> {
    ensure!(
        !args.entries.is_empty() || !args.set.is_empty(),
        InvalidArgumentSnafu {
            message: "metadata requires at least one KEY=VALUE entry".to_string(),
        }
    );

    let (manifest_path, mut spicepod, created) = load_manifest(args.manifest.as_deref())?;
    let before = spicepod.clone();
    let metadata = ensure_mapping_field(&mut spicepod, "metadata")?;

    for pair in args.entries.iter().chain(args.set.iter()) {
        let (key, metadata_value) = parse_string_or_yaml_prefixed_pair(pair)?;
        let metadata_key = Value::String(key.clone());
        if mode == MutationMode::Add {
            ensure!(
                !metadata.contains_key(&metadata_key),
                InvalidArgumentSnafu {
                    message: format!(
                        "metadata key '{key}' already exists. Use `spice metadata configure {key}=...` to update it."
                    ),
                }
            );
        }
        metadata.insert(metadata_key, metadata_value);
    }

    write_if_changed(&manifest_path, &spicepod, &before, created)
}

fn load_manifest(manifest_path: Option<&Path>) -> Result<(PathBuf, Value, bool)> {
    if let Some(path) = manifest_path {
        if path.exists() {
            return Ok((
                path.to_path_buf(),
                manifest::read_spicepod_value(path)?,
                false,
            ));
        }

        let value: Value = yaml::from_str(&manifest::create_spicepod_yaml(&default_app_name(path)))
            .map_err(|source| crate::error::Error::ConfigParse {
                message: format!("Failed to create default Spicepod manifest: {source}"),
            })?;
        return Ok((path.to_path_buf(), value, true));
    }

    let current_dir = std::env::current_dir().context(ConfigIoSnafu {
        operation: "read",
        path: PathBuf::from("."),
    })?;
    let name = current_dir
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("app");
    manifest::load_or_create_spicepod_value(Path::new("."), name)
}

fn default_app_name(manifest_path: &Path) -> String {
    manifest_path
        .parent()
        .and_then(Path::file_name)
        .and_then(|name| name.to_str())
        .map(ToString::to_string)
        .or_else(|| {
            std::env::current_dir().ok().and_then(|path| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .map(ToString::to_string)
            })
        })
        .unwrap_or_else(|| "app".to_string())
}

fn write_if_changed(path: &Path, value: &Value, before: &Value, created: bool) -> Result<()> {
    if created {
        println!("Initialized {}", path.display());
    }

    if value == before && !created {
        println!("No changes to {}", path.display());
        return Ok(());
    }

    manifest::write_spicepod_value(path, value)?;
    println!("Updated {}", path.display());
    Ok(())
}

fn ensure_file_or_stdin(file: Option<&Path>, stdin: bool) -> Result<()> {
    ensure!(
        file.is_none() || !stdin,
        InvalidArgumentSnafu {
            message: "Use either --file or --stdin, not both".to_string(),
        }
    );
    Ok(())
}

fn read_optional_value(file: Option<&Path>, stdin: bool) -> Result<Option<Value>> {
    if let Some(path) = file {
        let content = std::fs::read_to_string(path).context(ConfigIoSnafu {
            operation: "read",
            path: path.to_path_buf(),
        })?;
        return parse_document_value(&content).map(Some);
    }

    if stdin {
        let mut content = String::new();
        io::stdin()
            .read_to_string(&mut content)
            .context(ConfigIoSnafu {
                operation: "read",
                path: PathBuf::from("stdin"),
            })?;
        return parse_document_value(&content).map(Some);
    }

    Ok(None)
}

fn parse_document_value(content: &str) -> Result<Value> {
    yaml::from_str(content).map_err(|source| crate::error::Error::ConfigParse {
        message: format!("Failed to parse manifest edit input: {source}"),
    })
}

fn parse_key_value(pair: &str) -> Result<(String, Value)> {
    let (key, raw_value) = split_pair(pair)?;
    let value = if raw_value.is_empty() {
        Value::String(String::new())
    } else {
        yaml::from_str(raw_value).unwrap_or_else(|_| Value::String(raw_value.to_string()))
    };
    Ok((key.to_string(), value))
}

fn parse_string_or_yaml_prefixed_pair(pair: &str) -> Result<(String, Value)> {
    let (key, raw_value) = split_pair(pair)?;
    if let Some(yaml_value) = raw_value.strip_prefix("yaml:") {
        let value =
            yaml::from_str(yaml_value).map_err(|source| crate::error::Error::ConfigParse {
                message: format!("Failed to parse YAML value for '{key}': {source}"),
            })?;
        return Ok((key.to_string(), value));
    }

    Ok((key.to_string(), Value::String(raw_value.to_string())))
}

fn parse_string_pair(pair: &str) -> Result<(String, String)> {
    let (key, value) = split_pair(pair)?;
    Ok((key.to_string(), value.to_string()))
}

fn split_pair(pair: &str) -> Result<(&str, &str)> {
    let Some((key, value)) = pair.split_once('=') else {
        return InvalidArgumentSnafu {
            message: format!("Expected KEY=VALUE but got '{pair}'"),
        }
        .fail();
    };

    ensure!(
        !key.is_empty(),
        InvalidArgumentSnafu {
            message: format!("Expected non-empty key in '{pair}'"),
        }
    );

    Ok((key, value))
}

fn string_sequence(items: &[String]) -> Value {
    Value::Sequence(
        items
            .iter()
            .map(|item| Value::String(item.clone()))
            .collect(),
    )
}

fn build_reference_value(reference: &str, depends_on: &[String]) -> Value {
    let mut mapping = Mapping::new();
    mapping.insert(
        Value::String("ref".to_string()),
        Value::String(reference.to_string()),
    );
    if !depends_on.is_empty() {
        mapping.insert(
            Value::String("dependsOn".to_string()),
            string_sequence(depends_on),
        );
    }
    Value::Mapping(mapping)
}

fn component_name(value: &Value) -> Option<&str> {
    value.get("name").and_then(Value::as_str)
}

fn ensure_sequence_field<'value>(
    value: &'value mut Value,
    field: &str,
) -> Result<&'value mut Vec<Value>> {
    let root = root_mapping_mut(value)?;
    let field_key = Value::String(field.to_string());
    if !root.contains_key(&field_key) {
        root.insert(field_key.clone(), Value::Sequence(Vec::new()));
    }

    root.get_mut(&field_key)
        .and_then(Value::as_sequence_mut)
        .ok_or_else(|| crate::error::Error::ConfigParse {
            message: format!("Spicepod field '{field}' must be a sequence"),
        })
}

fn ensure_mapping_field<'value>(
    value: &'value mut Value,
    field: &str,
) -> Result<&'value mut Mapping> {
    let root = root_mapping_mut(value)?;
    let field_key = Value::String(field.to_string());
    if !root.contains_key(&field_key) {
        root.insert(field_key.clone(), Value::Mapping(Mapping::new()));
    }

    root.get_mut(&field_key)
        .and_then(Value::as_mapping_mut)
        .ok_or_else(|| crate::error::Error::ConfigParse {
            message: format!("Spicepod field '{field}' must be a mapping"),
        })
}

fn merge_root_mapping_field(value: &mut Value, field: &str, source: Value) -> Result<()> {
    let Value::Mapping(source_mapping) = source else {
        return Err(crate::error::Error::ConfigParse {
            message: format!("Spicepod field '{field}' must be a mapping"),
        });
    };
    let target = ensure_mapping_field(value, field)?;
    merge_mapping(target, source_mapping);
    Ok(())
}

fn root_mapping_mut(value: &mut Value) -> Result<&mut Mapping> {
    value
        .as_mapping_mut()
        .ok_or_else(|| crate::error::Error::ConfigParse {
            message: "Spicepod manifest must be a YAML mapping".to_string(),
        })
}

fn ensure_mapping_value(value: &mut Value, label: &str) -> Result<()> {
    if value.as_mapping_mut().is_none() {
        return Err(crate::error::Error::ConfigParse {
            message: format!("{label} input must be a YAML mapping"),
        });
    }
    Ok(())
}

fn set_path(value: &mut Value, path: &str, new_value: Value) -> Result<()> {
    let segments: Vec<&str> = path.split('.').collect();
    ensure!(
        !segments.is_empty() && segments.iter().all(|segment| !segment.is_empty()),
        InvalidArgumentSnafu {
            message: format!("Invalid field path '{path}'"),
        }
    );
    set_path_segments(value, &segments, new_value)
}

fn set_path_segments(value: &mut Value, segments: &[&str], new_value: Value) -> Result<()> {
    let Some((segment, remaining)) = segments.split_first() else {
        return Ok(());
    };

    let mapping = value
        .as_mapping_mut()
        .ok_or_else(|| crate::error::Error::ConfigParse {
            message: format!("Cannot set '{}' on a non-mapping value", segments.join(".")),
        })?;
    let key = Value::String((*segment).to_string());

    if remaining.is_empty() {
        mapping.insert(key, new_value);
        return Ok(());
    }

    if !mapping.contains_key(&key) {
        mapping.insert(key.clone(), Value::Mapping(Mapping::new()));
    }

    let child = mapping
        .get_mut(&key)
        .ok_or_else(|| crate::error::Error::ConfigParse {
            message: format!("Failed to create field path '{}'.", segments.join(".")),
        })?;
    if !child.is_mapping() {
        *child = Value::Mapping(Mapping::new());
    }

    set_path_segments(child, remaining, new_value)
}

fn merge_mapping(target: &mut Mapping, source: Mapping) {
    for (key, source_value) in source {
        match (target.get_mut(&key), source_value) {
            (Some(Value::Mapping(target_mapping)), Value::Mapping(source_mapping)) => {
                merge_mapping(target_mapping, source_mapping);
            }
            (Some(target_value), value) => {
                *target_value = value;
            }
            (None, value) => {
                target.insert(key, value);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_spicepod() -> Value {
        yaml::from_str("version: v2\nkind: Spicepod\nname: test\n")
            .expect("base spicepod should parse")
    }

    #[test]
    fn add_model_component_from_flags() {
        let mut spicepod = base_spicepod();
        let args = ComponentAddArgs {
            name: Some("llm".to_string()),
            options: CommonComponentOptions {
                from: Some("openai:gpt-4o-mini".to_string()),
                params: vec!["api_version=1.10".to_string()],
                ..CommonComponentOptions::default()
            },
        };

        mutate_component(
            &mut spicepod,
            ComponentSection::Model,
            args.name.as_deref(),
            &args.options,
            MutationMode::Add,
        )
        .expect("model should be added");

        let models = spicepod
            .get("models")
            .and_then(Value::as_sequence)
            .expect("models should be a sequence");
        let model = models.first().expect("model should exist");
        assert_eq!(model.get("name").and_then(Value::as_str), Some("llm"));
        assert_eq!(
            model.get("from").and_then(Value::as_str),
            Some("openai:gpt-4o-mini")
        );
        assert_eq!(
            model
                .get("params")
                .and_then(|params| params.get("api_version")),
            Some(&Value::String("1.10".to_string()))
        );
    }

    #[test]
    fn param_values_accept_explicit_yaml_prefix() {
        let mut spicepod = base_spicepod();
        let args = ComponentAddArgs {
            name: Some("llm".to_string()),
            options: CommonComponentOptions {
                from: Some("openai:gpt-4o-mini".to_string()),
                params: vec!["temperature=yaml:0.2".to_string()],
                ..CommonComponentOptions::default()
            },
        };

        mutate_component(
            &mut spicepod,
            ComponentSection::Model,
            args.name.as_deref(),
            &args.options,
            MutationMode::Add,
        )
        .expect("model should be added");

        let model = spicepod
            .get("models")
            .and_then(Value::as_sequence)
            .and_then(|models| models.first())
            .expect("model should exist");
        assert_eq!(
            model
                .get("params")
                .and_then(|params| params.get("temperature")),
            Some(&Value::Number(yaml::Number::Float(0.2)))
        );
    }

    #[test]
    fn configure_component_merges_existing_fields() {
        let mut spicepod: Value = yaml::from_str(
            "version: v2\nkind: Spicepod\nname: test\nmodels:\n  - name: llm\n    from: openai:gpt-4o-mini\n    params:\n      temperature: 0.2\n",
        )
        .expect("spicepod should parse");
        let args = ComponentConfigureArgs {
            name: Some("llm".to_string()),
            options: CommonComponentOptions {
                set: vec!["params.top_p=0.9".to_string()],
                ..CommonComponentOptions::default()
            },
        };

        mutate_component(
            &mut spicepod,
            ComponentSection::Model,
            args.name.as_deref(),
            &args.options,
            MutationMode::Configure,
        )
        .expect("model should be configured");

        let model = spicepod
            .get("models")
            .and_then(Value::as_sequence)
            .and_then(|models| models.first())
            .expect("model should exist");
        let params = model
            .get("params")
            .and_then(Value::as_mapping)
            .expect("params should be a mapping");
        assert!(params.contains_key(&Value::String("temperature".to_string())));
        assert!(params.contains_key(&Value::String("top_p".to_string())));
    }

    #[test]
    fn add_component_rejects_duplicate_name() {
        let mut spicepod: Value = yaml::from_str(
            "version: v2\nkind: Spicepod\nname: test\nmodels:\n  - name: llm\n    from: openai:gpt-4o-mini\n",
        )
        .expect("spicepod should parse");
        let args = ComponentAddArgs {
            name: Some("llm".to_string()),
            options: CommonComponentOptions {
                from: Some("openai:gpt-4.1-mini".to_string()),
                ..CommonComponentOptions::default()
            },
        };

        let error = mutate_component(
            &mut spicepod,
            ComponentSection::Model,
            args.name.as_deref(),
            &args.options,
            MutationMode::Add,
        )
        .expect_err("duplicate model should fail");

        assert!(error.to_string().contains("already exists"));
    }

    #[test]
    fn configure_runtime_sets_nested_fields() {
        let mut spicepod = base_spicepod();
        let mut value = Value::Mapping(Mapping::new());
        set_path(&mut value, "functions.enabled", Value::Bool(true)).expect("field should be set");

        merge_root_mapping_field(&mut spicepod, "runtime", value)
            .expect("runtime should be merged");
        assert_eq!(
            spicepod
                .get("runtime")
                .and_then(|runtime| runtime.get("functions"))
                .and_then(|functions| functions.get("enabled"))
                .and_then(Value::as_bool),
            Some(true)
        );
    }
}
