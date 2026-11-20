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

//! Which components start a program, run a build command, or load native code
//! on the host.
//!
//! A Spicepod dependency may declare such a component only when the root
//! `spicepod.yaml` trusts it (`trust: full`). This module is the one place that
//! decides what counts, for the runtime and the `spice` CLI alike.

use crate::component::{
    catalog::Catalog, dataset::Dataset, embeddings::Embeddings, function::Function, model::Model,
    rerankers::Reranker, tool::Tool, view::View, worker::Worker,
};

/// A reference to one component of any kind.
///
/// Every component kind a Spicepod can declare has a variant, and
/// [`host_program`] matches them exhaustively, so a new kind cannot be added
/// without deciding whether it runs anything on the host.
#[derive(Clone, Copy, Debug)]
pub enum ComponentRef<'a> {
    Catalog(&'a Catalog),
    Dataset(&'a Dataset),
    View(&'a View),
    Model(&'a Model),
    Embeddings(&'a Embeddings),
    Reranker(&'a Reranker),
    Tool(&'a Tool),
    Worker(&'a Worker),
    Function(&'a Function),
}

impl ComponentRef<'_> {
    /// The component's kind, as a user reads it: `tool`, `dataset`, …
    #[must_use]
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Catalog(_) => "catalog",
            Self::Dataset(_) => "dataset",
            Self::View(_) => "view",
            Self::Model(_) => "model",
            Self::Embeddings(_) => "embedding",
            Self::Reranker(_) => "reranker",
            Self::Tool(_) => "tool",
            Self::Worker(_) => "worker",
            Self::Function(_) => "function",
        }
    }

    /// The component's name.
    #[must_use]
    pub fn name(&self) -> &str {
        match self {
            Self::Catalog(component) => &component.name,
            Self::Dataset(component) => &component.name,
            Self::View(component) => &component.name,
            Self::Model(component) => &component.name,
            Self::Embeddings(component) => &component.name,
            Self::Reranker(component) => &component.name,
            Self::Tool(component) => &component.name,
            Self::Worker(component) => &component.name,
            Self::Function(component) => &component.name,
        }
    }
}

/// What a component runs on the host.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum HostProgram {
    /// Starts a program: a stdio MCP server (`tools` with `from: mcp:<command>`).
    Program(String),
    /// Runs a build command: a function's `build:` block, run by `spice build`
    /// and `spice run`. Never run by the runtime.
    BuildCommand(String),
    /// Loads a native library: an ADBC driver given as a path.
    NativeLibrary(String),
}

impl HostProgram {
    /// What the component does, as the reason in a refusal: "the tool starts
    /// the program 'docker' on this host".
    #[must_use]
    pub fn reason(&self, kind: &str) -> String {
        match self {
            Self::Program(program) => {
                format!("the {kind} starts the program '{program}' on this host")
            }
            Self::BuildCommand(_) => "its `build:` command runs a program on this host".to_string(),
            Self::NativeLibrary(library) => {
                format!("the {kind} loads the native library '{library}' on this host")
            }
        }
    }
}

/// What `component` runs on the host, if anything.
///
/// | Component | Runs on the host |
/// | --- | --- |
/// | `tools` with `from: mcp:<command>` (stdio MCP server) | Yes: [`HostProgram::Program`] |
/// | `tools` with `from: mcp:https://…` (remote MCP server) | No: see [`mcp_target_starts_no_program`] |
/// | `functions` with a `build:` block | Yes: [`HostProgram::BuildCommand`] (CLI only) |
/// | `functions` with `from: wasm` and `params.module` | No: runs in the WebAssembly sandbox |
/// | `datasets`/`catalogs` from `adbc` with a driver given as a path | Yes: [`HostProgram::NativeLibrary`] |
/// | Connectors that run a fixed tool Spice chooses (the `git` connector) | No |
///
/// An ODBC driver given as a path, and a function's `params.source`, are
/// refused for every Spicepod (by the ODBC connector and the WASM loader), so
/// trust cannot enable them and they are not classified here.
#[must_use]
pub fn host_program(component: ComponentRef<'_>) -> Option<HostProgram> {
    match component {
        ComponentRef::Tool(tool) => stdio_mcp_program(&tool.from).map(HostProgram::Program),
        ComponentRef::Function(function) => function
            .build
            .as_ref()
            .map(|build| HostProgram::BuildCommand(build.command.clone())),
        ComponentRef::Dataset(dataset) => adbc_driver_library(
            &dataset.from,
            dataset.params.as_ref().map(|params| &params.data),
        )
        .map(HostProgram::NativeLibrary),
        ComponentRef::Catalog(catalog) => adbc_driver_library(
            &catalog.from,
            catalog.params.as_ref().map(|params| &params.data),
        )
        .map(HostProgram::NativeLibrary),
        ComponentRef::View(_)
        | ComponentRef::Model(_)
        | ComponentRef::Embeddings(_)
        | ComponentRef::Reranker(_)
        | ComponentRef::Worker(_) => None,
    }
}

/// The program a `from: mcp:<target>` tool starts, or `None` for a remote
/// (HTTP) MCP server or a tool that is not MCP.
fn stdio_mcp_program(from: &str) -> Option<String> {
    let target = from.strip_prefix("mcp:")?;
    if mcp_target_starts_no_program(target) {
        return None;
    }
    Some(target.trim().to_string())
}

/// Whether the `<target>` of a `from: mcp:<target>` tool starts no program on
/// the host: it is empty, or it names a remote MCP server (`http://` or
/// `https://` in any case, ignoring surrounding whitespace).
///
/// A dependency's tool with such a target is loaded without trust, so the
/// runtime must connect to it over HTTP or refuse it, and never start it as a
/// program, even when it is not a valid URL. The runtime decides with this same
/// function, so the two cannot disagree.
#[must_use]
pub fn mcp_target_starts_no_program(target: &str) -> bool {
    let target = target.trim();
    target.is_empty()
        || ["http://", "https://"].iter().any(|scheme| {
            target
                .get(..scheme.len())
                .is_some_and(|prefix| prefix.eq_ignore_ascii_case(scheme))
        })
}

/// The library an `adbc` dataset or catalog loads by path, or `None` when it
/// names an installed driver.
fn adbc_driver_library(
    from: &str,
    params: Option<&std::collections::HashMap<String, crate::param::ParamValue>>,
) -> Option<String> {
    let connector = from
        .split_once(':')
        .map_or(from, |(connector, _)| connector);
    if !connector.eq_ignore_ascii_case("adbc") {
        return None;
    }
    let params = params?;
    let value = |key: &str| {
        params
            .get(key)
            .or_else(|| params.get(key.trim_start_matches("adbc_")))
            .map(crate::param::ParamValue::as_string)
    };
    if let Some(path) = value("adbc_driver_path").filter(|path| !path.trim().is_empty()) {
        return Some(path);
    }
    value("adbc_driver").filter(|driver| names_a_library_file(driver))
}

/// Whether a driver value names a library file rather than an installed
/// driver: it has a directory separator or a shared-library extension.
#[must_use]
pub fn names_a_library_file(driver: &str) -> bool {
    let driver = driver.trim();
    let lowercase = driver.to_ascii_lowercase();
    driver.contains('/')
        || driver.contains('\\')
        || [".so", ".dylib", ".dll"]
            .iter()
            .any(|extension| lowercase.ends_with(extension))
        || lowercase.contains(".so.")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn from_yaml<T: serde::de::DeserializeOwned>(text: &str) -> T {
        yaml::from_str(text).expect("the fixture parses")
    }

    /// Every component kind, classified. The match below is exhaustive, so a
    /// new kind fails to compile here until it is given a fixture and an
    /// expected classification.
    #[test]
    fn every_component_kind_is_classified() {
        let catalog: Catalog = from_yaml("from: unity_catalog:https://example.com\nname: c\n");
        let adbc_catalog: Catalog =
            from_yaml("from: adbc\nname: c\nparams:\n  adbc_driver_path: /opt/drivers/libx.so\n");
        let dataset: Dataset = from_yaml("from: postgres:public.orders\nname: orders\n");
        let adbc_installed: Dataset =
            from_yaml("from: adbc:orders\nname: orders\nparams:\n  adbc_driver: duckdb\n");
        let adbc_path: Dataset = from_yaml(
            "from: adbc:orders\nname: orders\nparams:\n  adbc_driver: /opt/drivers/libduckdb.dylib\n",
        );
        let git: Dataset = from_yaml("from: git:https://github.com/spiceai/spiceai\nname: repo\n");
        let view: View = from_yaml("name: v\nsql: SELECT 1\n");
        let model: Model = from_yaml("from: openai:gpt-4o\nname: m\n");
        let embeddings: Embeddings = from_yaml("from: openai:text-embedding-3-small\nname: e\n");
        let reranker: Reranker = from_yaml("from: cohere:rerank-v3.5\nname: r\n");
        let stdio_tool: Tool = from_yaml("from: mcp:docker\nname: fetch\n");
        let remote_tool: Tool = from_yaml("from: mcp:https://mcp.example.com/mcp\nname: remote\n");
        let builtin_tool: Tool = from_yaml("from: sql\nname: sql\n");
        let worker: Worker = from_yaml("name: w\n");
        let built_function: Function = from_yaml(
            "name: score\nfrom: wasm\nsignature:\n  args: []\n  returns: int64\nparams:\n  module: ./score.wasm\nbuild:\n  command: cargo build --release\n",
        );
        let module_function: Function = from_yaml(
            "name: score\nfrom: wasm\nsignature:\n  args: []\n  returns: int64\nparams:\n  module: ./score.wasm\n",
        );

        let cases: Vec<(ComponentRef<'_>, Option<HostProgram>)> = vec![
            (ComponentRef::Catalog(&catalog), None),
            (
                ComponentRef::Catalog(&adbc_catalog),
                Some(HostProgram::NativeLibrary("/opt/drivers/libx.so".into())),
            ),
            (ComponentRef::Dataset(&dataset), None),
            (ComponentRef::Dataset(&adbc_installed), None),
            (
                ComponentRef::Dataset(&adbc_path),
                Some(HostProgram::NativeLibrary(
                    "/opt/drivers/libduckdb.dylib".into(),
                )),
            ),
            (ComponentRef::Dataset(&git), None),
            (ComponentRef::View(&view), None),
            (ComponentRef::Model(&model), None),
            (ComponentRef::Embeddings(&embeddings), None),
            (ComponentRef::Reranker(&reranker), None),
            (
                ComponentRef::Tool(&stdio_tool),
                Some(HostProgram::Program("docker".into())),
            ),
            (ComponentRef::Tool(&remote_tool), None),
            (ComponentRef::Tool(&builtin_tool), None),
            (ComponentRef::Worker(&worker), None),
            (
                ComponentRef::Function(&built_function),
                Some(HostProgram::BuildCommand("cargo build --release".into())),
            ),
            (ComponentRef::Function(&module_function), None),
        ];

        for (component, expected) in &cases {
            // Exhaustive on purpose: see the doc comment.
            match component {
                ComponentRef::Catalog(_)
                | ComponentRef::Dataset(_)
                | ComponentRef::View(_)
                | ComponentRef::Model(_)
                | ComponentRef::Embeddings(_)
                | ComponentRef::Reranker(_)
                | ComponentRef::Tool(_)
                | ComponentRef::Worker(_)
                | ComponentRef::Function(_) => {}
            }
            assert_eq!(
                &host_program(*component),
                expected,
                "{} '{}'",
                component.kind(),
                component.name()
            );
        }
    }

    #[test]
    fn the_reason_names_what_runs() {
        assert_eq!(
            HostProgram::Program("docker".into()).reason("tool"),
            "the tool starts the program 'docker' on this host"
        );
        assert_eq!(
            HostProgram::BuildCommand("cargo build".into()).reason("function"),
            "its `build:` command runs a program on this host"
        );
    }
}
