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

//! Startup secret preflight.
//!
//! Walks every `${ store:key }` reference in the loaded app's component
//! configuration and resolves each against the configured secret stores
//! *before* components start loading, so a missing secret or a misconfigured
//! store surfaces as one consolidated, root-caused report up front instead of
//! as scattered per-component "missing required parameter" errors later.
//!
//! Driven from the start of [`Runtime::load_components`](crate::Runtime::load_components)
//! so it sees the same stores a component will: the built-in stores the runtime
//! registers on itself after it is built — Cloud Connect's delivered secrets,
//! restored from their local cache — are in place by then, and a check that ran
//! any earlier would report them all as missing.
//!
//! This is diagnostics only: it never changes whether or how components load,
//! and it never logs secret values (it consumes [`RefStatus`], which carries
//! store/key names and error text only).

use std::collections::HashMap;
use std::fmt::Write as _;

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

/// What the preflight found, as the summary it will be logged as.
#[derive(Debug, PartialEq, Eq)]
enum Outcome {
    /// The app declares no secret references, so nothing is logged.
    NoReferences,
    /// Every reference resolved.
    AllResolved { total: usize },
    /// At least one reference did not resolve; `report` is the `WARN` block,
    /// carrying store/key names and error text only.
    Unresolved { report: String },
}

/// Runs the preflight against the app's components and logs a single summary.
///
/// On success: one `INFO` line (or nothing when there are no references). On
/// problems: one `WARN` block listing each unresolved reference with its
/// provenance and root cause. Never aborts; never logs secret values.
pub(crate) async fn run(app: &App, secrets: &Secrets) {
    match evaluate(app, secrets).await {
        Outcome::NoReferences => {}
        Outcome::AllResolved { total } => {
            tracing::info!("Secrets: all {total} secret reference(s) resolved.");
        }
        Outcome::Unresolved { report } => tracing::warn!("{report}"),
    }
}

/// Resolves every reference and builds the summary, so what the operator reads
/// can be asserted without a subscriber.
async fn evaluate(app: &App, secrets: &Secrets) -> Outcome {
    let refs = collect_references(app);
    if refs.is_empty() {
        return Outcome::NoReferences;
    }

    // Resolve each distinct (store, key) once — components loading right after
    // re-hit the same lookups, which the store-level TTL caches absorb, but
    // within the preflight itself we dedupe so a key reused across components
    // costs a single check.
    let mut cache: HashMap<(String, String), RefStatus> = HashMap::new();
    let mut problems: Vec<(&LocatedRef, RefStatus)> = Vec::new();
    for located in &refs {
        let cache_key = (located.store.clone(), located.key.clone());
        let status = if let Some(status) = cache.get(&cache_key) {
            status.clone()
        } else {
            let status = secrets.check_reference(&located.store, &located.key).await;
            cache.insert(cache_key, status.clone());
            status
        };
        if !matches!(status, RefStatus::Found { .. }) {
            problems.push((located, status));
        }
    }

    if problems.is_empty() {
        return Outcome::AllResolved { total: refs.len() };
    }

    let mut report = format!(
        "Secrets: {} of {} secret reference(s) could not be resolved:",
        problems.len(),
        refs.len()
    );
    for (located, status) in &problems {
        // Infallible: writing to a String never errors.
        let _ = write!(
            report,
            "\n  ✗ {}  {} — {}",
            located.reference(),
            located.provenance(),
            describe(status)
        );
    }
    let _ = write!(report, "\nDocs: {DOCS_URL}");
    Outcome::Unresolved { report }
}

/// Human-readable explanation of a [`RefStatus`]. Carries only store/key
/// names and store error text — never secret values. Callers only pass the
/// non-`Found` statuses (the unresolved references); the `Found` arm is
/// handled for exhaustiveness.
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
            scan_map("dataset", &dataset.name, &params.as_string_map(), &mut refs);
        }
    }
    for catalog in &app.catalogs {
        if let Some(params) = &catalog.params {
            scan_map("catalog", &catalog.name, &params.as_string_map(), &mut refs);
        }
    }
    for view in &app.views {
        if let Some(params) = &view.params {
            scan_map("view", &view.name, &params.as_string_map(), &mut refs);
        }
    }
    for model in &app.models {
        scan_map("model", &model.name, &value_map(&model.params), &mut refs);
    }
    for embedding in &app.embeddings {
        scan_map(
            "embedding",
            &embedding.name,
            &value_map(&embedding.params),
            &mut refs,
        );
    }
    for tool in &app.tools {
        scan_map("tool", &tool.name, &tool.params, &mut refs);
        scan_map("tool", &tool.name, &tool.env, &mut refs);
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
/// Borrows the map so call sites (notably `tool.params` / `tool.env`) don't
/// clone during startup.
fn scan_map(
    kind: &'static str,
    component: &str,
    params: &HashMap<String, String>,
    refs: &mut Vec<LocatedRef>,
) {
    for (param, value) in params {
        for reference in iter_secret_references(value) {
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
        assert_eq!(
            (model_ref.store.as_str(), model_ref.key.as_str()),
            ("secrets", "OPENAI_KEY")
        );

        let embed_ref = find(&refs, "hf_token").expect("embedding ref");
        assert_eq!(embed_ref.kind, "embedding");

        let tool_param = find(&refs, "api_key").expect("tool param ref");
        assert_eq!(tool_param.kind, "tool");
        assert_eq!(tool_param.key, "TOOL_KEY");

        let tool_env = find(&refs, "TOKEN").expect("tool env ref");
        assert_eq!(tool_env.kind, "tool");
        assert_eq!(
            (tool_env.store.as_str(), tool_env.key.as_str()),
            ("env", "TOOL_ENV")
        );
    }

    #[test]
    fn no_references_yields_empty() {
        let app = AppBuilder::new("test")
            .with_dataset(dataset_with_params("plain", &[("host", "localhost")]))
            .build();
        assert!(collect_references(&app).is_empty());
    }

    /// A key no environment (and no `.env`) is expected to hold, so `env`
    /// cannot answer for the store under test. Unprefixed on purpose: the
    /// `env` store also looks a key up as `SPICE_<key>`.
    const DELIVERED_KEY: &str = "PREFLIGHT_TEST_UNSET_SECRET";

    /// Stands in for Cloud Connect's delivered-secrets store: registered on the
    /// runtime itself, after the spicepod's `secrets:` section was loaded.
    struct FixedStore {
        key: &'static str,
    }

    #[async_trait::async_trait]
    impl runtime_secrets::SecretStore for FixedStore {
        async fn get_secret(
            &self,
            key: &str,
        ) -> runtime_secrets::AnyErrorResult<Option<secrecy::SecretString>> {
            Ok((key == self.key).then(|| secrecy::SecretString::from("delivered")))
        }
    }

    fn app_referencing(key: &str) -> app::App {
        let reference = format!("${{ secrets:{key} }}");
        AppBuilder::new("test")
            .with_dataset(dataset_with_params(
                "orders",
                &[("pg_pass", reference.as_str())],
            ))
            .build()
    }

    /// A spicepod with only the default `env` store: the reference is named,
    /// located, and root-caused in one block.
    #[tokio::test]
    async fn unresolved_reference_is_reported_with_provenance_and_cause() {
        let mut secrets = Secrets::new();
        secrets.load_from(&[]).await.expect("env store loads");

        let Outcome::Unresolved { report } =
            evaluate(&app_referencing(DELIVERED_KEY), &secrets).await
        else {
            panic!("expected the reference to be reported as unresolved");
        };
        assert!(
            report.contains("Secrets: 1 of 1 secret reference(s) could not be resolved:"),
            "{report}"
        );
        assert!(
            report.contains(&format!(
                "✗ ${{ secrets:{DELIVERED_KEY} }}  dataset 'orders' (pg_pass) — not found in [env]"
            )),
            "{report}"
        );
        assert!(report.contains(DOCS_URL), "{report}");
    }

    /// The preflight runs at the start of the component load, so the built-in
    /// stores the runtime registers on itself after the spicepod's `secrets:`
    /// section — Cloud Connect's delivered secrets, restored from their local
    /// cache — answer for it, the same as they do for the components that load
    /// next. Checking any earlier reported every delivered secret as missing
    /// while the dataset referencing it went on to register.
    #[tokio::test]
    async fn reference_served_by_a_builtin_store_resolves() {
        let mut secrets = Secrets::new();
        secrets.load_from(&[]).await.expect("env store loads");
        secrets.register_builtin_store(
            "cloud",
            std::sync::Arc::new(FixedStore { key: DELIVERED_KEY }),
        );

        assert_eq!(
            evaluate(&app_referencing(DELIVERED_KEY), &secrets).await,
            Outcome::AllResolved { total: 1 }
        );
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
