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

//! Startup secret preflight.
//!
//! Walks every `${ store:key }` reference in the loaded app's component
//! configuration and resolves each against the configured secret stores
//! *before* components start loading, so a missing secret or a misconfigured
//! store surfaces as one consolidated, root-caused report up front instead of
//! as scattered per-component "missing required parameter" errors later.
//!
//! This is diagnostics only: it never changes whether or how components load,
//! and it never logs secret values (it consumes [`RefStatus`], which carries
//! store/key names and error text only).

use std::collections::HashMap;

use app::App;
use runtime_secrets::{RefStatus, Secrets, iter_secret_references};

const DOCS_URL: &str = "https://spiceai.org/docs/components/secret-stores";

/// A single secret reference together with where it was found.
struct LocatedRef {
    /// Component kind, e.g. `dataset`, `model`.
    kind: &'static str,
    /// Component name (the `name:` field).
    component: String,
    /// Parameter name the reference appeared in.
    param: String,
    /// Store segment of `${ store:key }`.
    store: String,
    /// Key segment of `${ store:key }`.
    key: String,
}

impl LocatedRef {
    /// `${ store:key }` as written, for the report.
    fn reference(&self) -> String {
        format!("${{ {}:{} }}", self.store, self.key)
    }

    /// `kind 'name' (param)` provenance, for the report.
    fn provenance(&self) -> String {
        format!("{} '{}' ({})", self.kind, self.component, self.param)
    }
}

/// Runs the preflight against the app's components and logs a single summary.
///
/// On success: one `INFO` line (or nothing when there are no references). On
/// problems: one `WARN` block listing each unresolved reference with its
/// provenance and root cause. Never aborts; never logs secret values.
pub(crate) async fn run(app: &App, secrets: &Secrets) {
    let refs = collect_references(app);
    if refs.is_empty() {
        return;
    }

    // Resolve each distinct (store, key) once — components loading right after
    // re-hit the same lookups, which the store-level TTL caches absorb, but
    // within the preflight itself we dedupe so a key reused across components
    // costs a single check.
    let mut cache: HashMap<(String, String), RefStatus> = HashMap::new();
    let mut problems: Vec<(&LocatedRef, RefStatus)> = Vec::new();
    for located in &refs {
        let cache_key = (located.store.clone(), located.key.clone());
        let status = match cache.get(&cache_key) {
            Some(status) => status.clone(),
            None => {
                let status = secrets.check_reference(&located.store, &located.key).await;
                cache.insert(cache_key, status.clone());
                status
            }
        };
        if !matches!(status, RefStatus::Found { .. }) {
            problems.push((located, status));
        }
    }

    if problems.is_empty() {
        tracing::info!(
            "Secret preflight: {} secret reference(s) resolved.",
            refs.len()
        );
        return;
    }

    let mut report = format!(
        "Secret preflight: {} of {} secret reference(s) have problems:",
        problems.len(),
        refs.len()
    );
    for (located, status) in &problems {
        report.push_str(&format!(
            "\n  ✗ {}  {} — {}",
            located.reference(),
            located.provenance(),
            describe(status)
        ));
    }
    report.push_str(&format!("\nDocs: {DOCS_URL}"));
    tracing::warn!("{report}");
}

/// Human-readable explanation of a non-`Found` status. Carries only store/key
/// names and store error text — never secret values.
fn describe(status: &RefStatus) -> String {
    match status {
        RefStatus::Found { .. } => "resolved".to_string(),
        RefStatus::NotFound { searched } => {
            format!("not found in [{}]", searched.join(", "))
        }
        RefStatus::StoreError { store, error } => {
            format!("store '{store}' error: {error}")
        }
        RefStatus::UnknownStore { configured } => {
            format!(
                "references undefined store; configured stores: [{}]",
                configured.join(", ")
            )
        }
    }
}

/// Collects every `${ store:key }` reference across the app's component
/// parameter maps, tagged with provenance.
fn collect_references(app: &App) -> Vec<LocatedRef> {
    let mut refs = Vec::new();

    for dataset in &app.datasets {
        if let Some(params) = &dataset.params {
            scan_map("dataset", &dataset.name, params.as_string_map(), &mut refs);
        }
    }
    for catalog in &app.catalogs {
        if let Some(params) = &catalog.params {
            scan_map("catalog", &catalog.name, params.as_string_map(), &mut refs);
        }
    }
    for view in &app.views {
        if let Some(params) = &view.params {
            scan_map("view", &view.name, params.as_string_map(), &mut refs);
        }
    }
    for model in &app.models {
        scan_map("model", &model.name, value_map(&model.params), &mut refs);
    }
    for embedding in &app.embeddings {
        scan_map(
            "embedding",
            &embedding.name,
            value_map(&embedding.params),
            &mut refs,
        );
    }
    for tool in &app.tools {
        scan_map("tool", &tool.name, tool.params.clone(), &mut refs);
        scan_map("tool", &tool.name, tool.env.clone(), &mut refs);
    }

    refs
}

/// Converts a `HashMap<String, serde_json::Value>` (model/embedding params)
/// into scannable strings. String scalars are scanned as-is; other shapes are
/// serialized so a reference nested in an array/object is still surfaced,
/// attributed to the top-level parameter name.
fn value_map(params: &HashMap<String, serde_json::Value>) -> HashMap<String, String> {
    params
        .iter()
        .map(|(k, v)| {
            let s = match v {
                serde_json::Value::String(s) => s.clone(),
                other => other.to_string(),
            };
            (k.clone(), s)
        })
        .collect()
}

/// Scans one component's `param -> value` map and appends every reference found.
fn scan_map(
    kind: &'static str,
    component: &str,
    params: HashMap<String, String>,
    refs: &mut Vec<LocatedRef>,
) {
    for (param, value) in params {
        for reference in iter_secret_references(&value) {
            refs.push(LocatedRef {
                kind,
                component: component.to_string(),
                param: param.clone(),
                store: reference.store,
                key: reference.key,
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use app::AppBuilder;
    use spicepod::component::dataset::Dataset;
    use spicepod::component::embeddings::Embeddings;
    use spicepod::component::model::Model;
    use spicepod::component::tool::Tool;
    use spicepod::param::Params;
    use std::collections::HashMap;

    fn dataset_with_params(name: &str, params: &[(&str, &str)]) -> Dataset {
        let mut ds = Dataset::new("memory:data", name);
        let map: HashMap<String, String> = params
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect();
        ds.params = Some(Params::from_string_map(map));
        ds
    }

    fn find<'a>(refs: &'a [LocatedRef], param: &str) -> Option<&'a LocatedRef> {
        refs.iter().find(|r| r.param == param)
    }

    #[test]
    fn collects_dataset_param_references_with_provenance() {
        let app = AppBuilder::new("test")
            .with_dataset(dataset_with_params(
                "reports",
                &[
                    ("pg_pass", "${ secrets:PG_PASS }"),
                    ("pg_host", "localhost"), // no reference
                ],
            ))
            .build();

        let refs = collect_references(&app);
        assert_eq!(refs.len(), 1);
        let r = &refs[0];
        assert_eq!(r.kind, "dataset");
        assert_eq!(r.component, "reports");
        assert_eq!(r.param, "pg_pass");
        assert_eq!(r.store, "secrets");
        assert_eq!(r.key, "PG_PASS");
        assert_eq!(r.reference(), "${ secrets:PG_PASS }");
        assert_eq!(r.provenance(), "dataset 'reports' (pg_pass)");
    }

    #[test]
    fn collects_references_across_component_kinds_including_tool_env() {
        let mut model_params = HashMap::new();
        model_params.insert(
            "openai_api_key".to_string(),
            serde_json::Value::String("${ secrets:OPENAI_KEY }".to_string()),
        );
        let model = Model {
            params: model_params,
            ..Model::new("openai:gpt-4o", "gpt")
        };

        let mut embed_params = HashMap::new();
        embed_params.insert(
            "hf_token".to_string(),
            serde_json::Value::String("${ env:HF_TOKEN }".to_string()),
        );
        let embedding = Embeddings {
            params: embed_params,
            ..Embeddings::new("huggingface:foo", "embedder")
        };

        // Tool has no constructor and several optional fields; build it from
        // JSON so the test stays robust to field additions.
        let tool: Tool = serde_json::from_value(serde_json::json!({
            "from": "mcp:thing",
            "name": "mytool",
            "params": { "api_key": "${ secrets:TOOL_KEY }" },
            "env": { "TOKEN": "${ env:TOOL_ENV }" },
        }))
        .expect("valid tool");

        let app = AppBuilder::new("test")
            .with_model(model)
            .with_embedding(embedding)
            .with_tool(tool)
            .build();

        let refs = collect_references(&app);
        // model + embedding + tool.params + tool.env
        assert_eq!(refs.len(), 4);

        let model_ref = find(&refs, "openai_api_key").expect("model ref");
        assert_eq!(model_ref.kind, "model");
        assert_eq!((model_ref.store.as_str(), model_ref.key.as_str()), ("secrets", "OPENAI_KEY"));

        let embed_ref = find(&refs, "hf_token").expect("embedding ref");
        assert_eq!(embed_ref.kind, "embedding");

        let tool_param = find(&refs, "api_key").expect("tool param ref");
        assert_eq!(tool_param.kind, "tool");
        assert_eq!(tool_param.key, "TOOL_KEY");

        let tool_env = find(&refs, "TOKEN").expect("tool env ref");
        assert_eq!(tool_env.kind, "tool");
        assert_eq!((tool_env.store.as_str(), tool_env.key.as_str()), ("env", "TOOL_ENV"));
    }

    #[test]
    fn no_references_yields_empty() {
        let app = AppBuilder::new("test")
            .with_dataset(dataset_with_params("plain", &[("host", "localhost")]))
            .build();
        assert!(collect_references(&app).is_empty());
    }

    #[test]
    fn describe_renders_each_status_without_values() {
        assert_eq!(
            describe(&RefStatus::NotFound {
                searched: vec!["aws".to_string(), "env".to_string()],
            }),
            "not found in [aws, env]"
        );
        assert_eq!(
            describe(&RefStatus::StoreError {
                store: "aws".to_string(),
                error: "bad creds".to_string(),
            }),
            "store 'aws' error: bad creds"
        );
        assert_eq!(
            describe(&RefStatus::UnknownStore {
                configured: vec!["env".to_string()],
            }),
            "references undefined store; configured stores: [env]"
        );
    }
}
