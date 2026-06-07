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

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, LazyLock};

use crate::datafusion::pg_catalog::{
    COL_DESCRIPTION_UDF_NAME, OBJ_DESCRIPTION_UDF_NAME, register_postgres_comment_udfs,
};
use crate::datafusion::udtf::flatten_json::{
    FLATTEN_JSON_UDTF_NAME, FlattenJsonScalar, FlattenJsonTableFunc,
};
use crate::datafusion::udtf::json_properties::{
    FLATTEN_JSON_PROPERTIES_UDTF_NAME, FlattenJsonPropertiesScalar, FlattenJsonPropertiesTableFunc,
};
use crate::datafusion::udtf::json_tree::{JSON_TREE_UDTF_NAME, JsonTreeScalar, JsonTreeTableFunc};
use crate::embeddings::udtf::{VECTOR_SEARCH_UDTF_NAME, VectorSearchTableFunc};
use crate::search::full_text::udtf::{TEXT_SEARCH_UDTF_NAME, TextSearchTableFunc};
use crate::search::rerank::{RERANK_UDTF_NAME, RerankTableFunc};
use crate::search::rrf;
use crate::search::rrf::RRF_UDF_NAME;
use crate::search::util::parse_explicit_primary_keys;
use data_components::function_support::{FunctionRestriction, FunctionSupport};
use datafusion_table_providers::util::supported_functions::{
    FunctionRestriction as TpFunctionRestriction, FunctionSupport as TpFunctionSupport,
};
use datafusion::execution::FunctionRegistry;
use datafusion::functions::math::random::RandomFunc;
use datafusion::logical_expr::ScalarUDF;
use datafusion::prelude::SessionContext;
use parking_lot::RwLock;
use runtime_datafusion::query_engine::QueryEngine;
#[cfg(feature = "models")]
use runtime_datafusion_udfs::{
    ai::{AI_UDF_NAME, Ai},
    embed::{self, EMBED_UDF_NAME},
};
use runtime_secrets::{ExposeSecret, get_params_with_secrets};
#[cfg(not(feature = "models"))]
const EMBED_UDF_NAME: &str = "embed";
use runtime_datafusion_udfs::{
    alias::ScalarUDFAlias,
    bucket::{BUCKET_SCALAR_UDF_NAME, Bucket},
    cosine_distance::{COSINE_DISTANCE_UDF_NAME, CosineDistance},
    digest_many::{DIGEST_UDF_NAME, INSTANCE},
    inner_product::{INNER_PRODUCT_UDF_NAME, InnerProduct},
    l2_distance::{
        L2_DISTANCE_UDF_NAME, L2_SQUARED_DISTANCE_UDF_NAME, L2Distance, L2SquaredDistance,
    },
    l2_norm::{L2_NORM_UDF_NAME, L2Norm},
    truncate::{TRUNCATE_SCALAR_UDF_NAME, Truncate},
};
use serde_json::Value;
use spicepod::component::function::{Function, FunctionKind, Volatility};
use util::in_tracing_context_async;

/// Register core scalar UDFs that have no runtime dependencies.
///
/// These UDFs only need a [`SessionContext`] and can be registered on any
/// context, including isolated ones like the refresh-task context.
pub fn register_core_scalar_udfs(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDFAlias::new(Arc::new(RandomFunc::default()), "rand").into());
    ctx.register_udf(Bucket::new().into());
    ctx.register_udf(CosineDistance::new().into());
    ctx.register_udf(InnerProduct::new().into());
    ctx.register_udf(L2Distance::new().into());
    ctx.register_udf(L2SquaredDistance::new().into());
    ctx.register_udf(L2Norm::new().into());
    ctx.register_udf(Truncate::new().into());
    ctx.register_udf(INSTANCE.clone());
    register_postgres_comment_udfs(ctx);
}

pub async fn register_udfs(runtime: &crate::Runtime) {
    let ctx = &runtime.df.ctx;
    register_core_scalar_udfs(ctx);

    ctx.register_udf(TextSearchTableFunc::new(Arc::downgrade(&runtime.df)).into());
    ctx.register_udtf(
        TEXT_SEARCH_UDTF_NAME,
        Arc::new(TextSearchTableFunc::new(Arc::downgrade(&runtime.df))),
    );

    let explicit_pks = parse_explicit_primary_keys(runtime.app()).await;
    ctx.register_udf(
        VectorSearchTableFunc::new(Arc::downgrade(&runtime.df), explicit_pks.clone()).into(),
    );
    ctx.register_udtf(
        VECTOR_SEARCH_UDTF_NAME,
        Arc::new(VectorSearchTableFunc::new(
            Arc::downgrade(&runtime.df),
            explicit_pks,
        )),
    );

    ctx.register_udf(rrf::ReciprocalRankFusion::from_ctx(ctx).into());
    ctx.register_udtf(
        RRF_UDF_NAME,
        Arc::new(rrf::ReciprocalRankFusion::from_ctx(ctx)),
    );

    // `rerank(input, model => ..., document => ..., ...)` — reorders a
    // scored result set using a reranker model. Registered as both a scalar
    // UDF stub (so `rerank(...)` can appear nested inside another UDTF's arg
    // list, same trick vector_search/text_search/rrf use) and a UDTF (the
    // actual `FROM rerank(...)` implementation).
    let session_ctx: Arc<SessionContext> = Arc::clone(ctx);
    ctx.register_udf(
        RerankTableFunc::new(
            Arc::downgrade(&runtime.df),
            Arc::clone(&session_ctx),
            runtime.rerankers(),
            runtime.completion_llms(),
        )
        .into(),
    );
    ctx.register_udtf(
        RERANK_UDTF_NAME,
        Arc::new(RerankTableFunc::new(
            Arc::downgrade(&runtime.df),
            session_ctx,
            runtime.rerankers(),
            runtime.completion_llms(),
        )),
    );

    // `flatten_json_properties` / `flatten_json` / `json_tree` — JSON-Schema
    // and generic JSON shredders. Registered as both UDTF (FROM-clause,
    // literal input) and ScalarUDF returning `List<Struct<...>>` (per-row /
    // LATERAL via UNNEST).
    ctx.register_udtf(
        FLATTEN_JSON_PROPERTIES_UDTF_NAME,
        Arc::new(FlattenJsonPropertiesTableFunc::new()),
    );
    ctx.register_udf(FlattenJsonPropertiesScalar::new().into());
    ctx.register_udtf(
        FLATTEN_JSON_UDTF_NAME,
        Arc::new(FlattenJsonTableFunc::new()),
    );
    ctx.register_udf(FlattenJsonScalar::new().into());
    ctx.register_udtf(JSON_TREE_UDTF_NAME, Arc::new(JsonTreeTableFunc::new()));
    ctx.register_udf(JsonTreeScalar::new().into());

    #[cfg(feature = "models")]
    {
        ctx.register_udf(embed::Embed::new(runtime.embeds()).into());
        ctx.register_udf(
            Ai::new(runtime.completion_llms(), runtime.model_rate_controllers())
                .into_async_udf()
                .into_scalar_udf(),
        );
    }

    in_tracing_context_async(register_user_functions(runtime, ctx)).await;
}

/// Emits the user-defined functions BETA warning at most once per
/// process. Called from both startup registration and hot-reload so the
/// user sees it whenever a `functions:` entry becomes active for the
/// first time.
fn warn_beta_once() {
    static BETA_WARNING: std::sync::Once = std::sync::Once::new();
    BETA_WARNING.call_once(|| {
        let supported_sources = runtime_datafusion_udfs::user_functions::supported_schemes();
        tracing::warn!(
            "User-defined functions (spicepod `functions:` section) are in BETA. Supported sources in this build are {supported_sources}; APIs and on-disk \
             format may change before general availability. See: \
             https://spiceai.org/docs/reference/spicepod/functions"
        );
    });
}

async fn register_user_functions(runtime: &crate::Runtime, ctx: &SessionContext) {
    let Some(app) = runtime.read_app().await else {
        return;
    };
    if app.functions.is_empty() {
        return;
    }
    if !app.runtime.functions.enabled {
        tracing::error!(
            "User-defined functions are declared but disabled. Set `runtime.functions.enabled: true` to register spicepod `functions:` entries."
        );
        return;
    }

    warn_beta_once();

    let enabled_functions = enabled_function_declarations(&app.functions);
    if enabled_functions.is_empty() {
        return;
    }

    let enabled_functions = resolve_function_params(runtime.secrets(), &enabled_functions).await;
    let (built, errors) =
        runtime_datafusion_udfs::user_functions::build_all(&enabled_functions).await;
    for err in &errors {
        tracing::error!("{err}");
    }

    let mut registered_function_names = Vec::new();
    let mut functions_to_expose_as_tools = Vec::new();
    for (decl, built) in built {
        match built {
            runtime_datafusion_udfs::user_functions::BuiltFunction::Scalar(udf) => {
                if let Some(existing_name) = registered_scalar_udf_name(ctx, &decl.name) {
                    tracing::error!(name = %decl.name, existing_name = %existing_name, "Failed to register user function because a scalar UDF with this name is already registered; rename the function to avoid changing query semantics");
                    continue;
                }
                if let Some(existing_name) = registered_table_udf_name(ctx, &decl.name) {
                    tracing::error!(name = %decl.name, existing_name = %existing_name, "Failed to register user function because a table function with this name is already registered; rename the function to avoid changing query semantics");
                    continue;
                }
                ctx.register_udf(udf.as_ref().clone());
                registered_function_names.push(decl.name.clone());
                if function_executes_code(&decl) {
                    add_code_executing_function(&decl.name);
                }
                upsert_user_function_info(info_from_decl(&decl));
                tracing::info!(
                    name = %decl.name,
                    from = %decl.from,
                    "Registered user function"
                );
                functions_to_expose_as_tools.push(decl);
            }
            runtime_datafusion_udfs::user_functions::BuiltFunction::Table(udtf) => {
                if let Some(existing_name) = registered_scalar_udf_name(ctx, &decl.name) {
                    tracing::error!(name = %decl.name, existing_name = %existing_name, "Failed to register user table function because a scalar UDF with this name is already registered; rename the function to avoid changing query semantics");
                    continue;
                }
                if let Some(existing_name) = registered_table_udf_name(ctx, &decl.name) {
                    tracing::error!(name = %decl.name, existing_name = %existing_name, "Failed to register user table function because a table function with this name is already registered; rename the function to avoid changing query semantics");
                    continue;
                }
                ctx.register_udtf(&decl.name, udtf);
                registered_function_names.push(decl.name.clone());
                if function_executes_code(&decl) {
                    add_code_executing_function(&decl.name);
                }
                upsert_user_function_info(info_from_decl(&decl));
                tracing::info!(
                    name = %decl.name,
                    from = %decl.from,
                    "Registered user table function"
                );
                if decl.as_tool {
                    tracing::warn!(name = %decl.name, "User table functions cannot be exposed as tools; skipping tool exposure");
                }
            }
        }
    }
    add_user_functions_to_deny_list(registered_function_names);
    for decl in functions_to_expose_as_tools {
        maybe_register_function_as_tool(runtime, &decl).await;
    }
}

/// If `decl.as_tool` is true, construct a [`FunctionAsTool`] adapter and
/// insert it into the runtime's tool registry. Failures to build the
/// adapter (unsupported types) are logged at WARN and do not fail
/// function registration — the function remains callable via SQL.
///
/// When a tool is already registered under the same name (either a built-in
/// or a spicepod `tools:` entry loaded earlier) we log at WARN and skip —
/// silent overwrites were the pre-review behaviour and would mask
/// misconfiguration.
async fn maybe_register_function_as_tool(runtime: &crate::Runtime, decl: &Function) {
    if !decl.as_tool {
        return;
    }
    let df_dyn = Arc::clone(&runtime.df) as Arc<dyn QueryEngine>;
    let df_weak = Arc::downgrade(&df_dyn);
    match crate::tools::builtin::function_tool::build(decl, df_weak) {
        Ok(adapter) => {
            let tool: Arc<dyn tools::SpiceModelTool> = Arc::new(adapter);
            let name = decl.name.clone();
            let mut tools_map = runtime.tools.write().await;
            if tools_map.contains_key(&name) {
                tracing::warn!(
                    name = %name,
                    "Name collision — a tool with this name is already registered; \
                     not exposing the function as a tool. Rename one, or set `as_tool: false` on the function."
                );
                return;
            }
            tools_map.insert(name.clone(), crate::tools::Tooling::FunctionTool(tool));
            tracing::info!(name = %name, "Exposed user function as tool");
        }
        Err(e) => {
            tracing::warn!(
                name = %decl.name,
                "Skipping tool exposure for user function: {e}"
            );
        }
    }
}

/// Register an async-backed [`ScalarUDF`] into the session context and
/// add its name to the federation deny-list in one call. Used by the
/// tool→SQL bridge so the tool-registration path doesn't need to know
/// about the deny-list as an implementation detail.
pub fn register_async_user_udf(ctx: &SessionContext, udf: &ScalarUDF, name: &str) -> bool {
    if let Some(existing_name) = registered_scalar_udf_name(ctx, name) {
        tracing::warn!(name = %name, existing_name = %existing_name, "Skipping async user UDF registration because a scalar UDF with this name is already registered; rename the function to avoid changing query semantics");
        return false;
    }
    if let Some(existing_name) = registered_table_udf_name(ctx, name) {
        tracing::warn!(name = %name, existing_name = %existing_name, "Skipping async user UDF registration because a table function with this name is already registered; rename the function to avoid changing query semantics");
        return false;
    }
    ctx.register_udf(udf.clone());
    add_user_function_to_deny_list(name);
    add_code_executing_function(name);
    true
}

/// Return the exact registered scalar UDF name that collides with `name`, if any.
#[must_use]
pub fn registered_scalar_udf_name(ctx: &SessionContext, name: &str) -> Option<String> {
    ctx.udfs()
        .into_iter()
        .find(|registered_name| registered_name.eq_ignore_ascii_case(name))
}

/// Return the exact registered table-function name that collides with `name`, if any.
#[must_use]
pub fn registered_table_udf_name(ctx: &SessionContext, name: &str) -> Option<String> {
    ctx.state()
        .table_functions()
        .keys()
        .find(|registered_name| registered_name.eq_ignore_ascii_case(name))
        .cloned()
}

fn function_executes_code(decl: &Function) -> bool {
    let scheme = decl
        .from
        .split_once(':')
        .map_or(decl.from.as_str(), |(scheme, _)| scheme)
        .to_ascii_lowercase();
    matches!(scheme.as_str(), "http" | "https")
}

fn enabled_function_declarations(functions: &[Function]) -> Vec<Function> {
    functions
        .iter()
        .filter(|decl| decl.enabled)
        .cloned()
        .collect()
}

async fn resolve_function_params(
    secrets: Arc<tokio::sync::RwLock<runtime_secrets::Secrets>>,
    functions: &[Function],
) -> Vec<Function> {
    let mut resolved = Vec::with_capacity(functions.len());
    for decl in functions {
        let mut decl = decl.clone();
        let string_params = decl
            .params
            .iter()
            .filter_map(|(key, value)| value.as_str().map(|s| (key.clone(), s.to_string())))
            .collect::<HashMap<_, _>>();
        if !string_params.is_empty() {
            let params_with_secrets =
                get_params_with_secrets(Arc::clone(&secrets), &string_params).await;
            for (key, value) in params_with_secrets {
                decl.params
                    .insert(key, Value::String(value.expose_secret().to_string()));
            }
        }
        resolved.push(decl);
    }
    resolved
}

pub(crate) fn effective_user_function_volatility(decl: &Function) -> &'static str {
    match decl.volatility {
        Volatility::Immutable if function_executes_code(decl) => "stable",
        Volatility::Immutable => "immutable",
        Volatility::Stable => "stable",
        Volatility::Volatile => "volatile",
    }
}

fn info_from_decl(decl: &Function) -> UserFunctionInfo {
    UserFunctionInfo {
        name: decl.name.clone(),
        kind: decl.kind.as_str().to_string(),
        volatility: effective_user_function_volatility(decl).to_string(),
        from: decl.from.clone(),
        description: decl.description.clone(),
    }
}

/// Rebuild + re-register user functions against `new_app`, removing any
/// that are no longer declared. Called on spicepod hot-reload.
pub async fn apply_function_diff(
    runtime: &crate::Runtime,
    current_app: &Arc<app::App>,
    new_app: &Arc<app::App>,
) {
    let ctx = &runtime.df.ctx;
    let current_enabled = current_app.runtime.functions.enabled;
    let new_enabled = new_app.runtime.functions.enabled;

    // First pass: collect every function that needs to go away (removed or
    // changed). Do all the lock-free work (DF deregister, deny-list, info
    // registry) first so the tools-map write lock is held only once for the
    // batch of tool drops.
    let mut functions_to_drop: Vec<String> = Vec::new();
    let mut tools_to_drop: Vec<String> = Vec::new();
    for current in &current_app.functions {
        let current_registered = current_enabled && current.enabled;
        let needs_drop = current_registered
            && match new_app.functions.iter().find(|f| f.name == current.name) {
                Some(next) => !new_enabled || !next.enabled || next != current,
                None => true,
            };
        if needs_drop {
            match current.kind {
                FunctionKind::Scalar => {
                    ctx.deregister_udf(&current.name);
                }
                FunctionKind::Table => {
                    ctx.deregister_udtf(&current.name);
                }
            }
            functions_to_drop.push(current.name.clone());
            remove_code_executing_function(&current.name);
            remove_user_function_info(&current.name);
            if current.kind == FunctionKind::Scalar && current.as_tool {
                tools_to_drop.push(current.name.clone());
            }
            tracing::info!(name = %current.name, "Deregistered user function");
        }
    }
    remove_user_functions_from_deny_list(&functions_to_drop);
    if !tools_to_drop.is_empty() {
        let mut tools_map = runtime.tools.write().await;
        for name in &tools_to_drop {
            if matches!(
                tools_map.get(name),
                Some(crate::tools::Tooling::FunctionTool(_))
            ) {
                tools_map.remove(name);
            }
        }
    }

    if new_app.functions.is_empty() {
        return;
    }
    if !new_enabled {
        tracing::error!(
            "User-defined functions are declared but disabled. Set `runtime.functions.enabled: true` to register spicepod `functions:` entries."
        );
        return;
    }

    let functions_to_register = new_app
        .functions
        .iter()
        .filter(|next| next.enabled)
        .filter(|next| {
            !current_enabled
                || match current_app.functions.iter().find(|f| f.name == next.name) {
                    Some(prev) => !prev.enabled || prev != *next,
                    None => true,
                }
        })
        .cloned()
        .collect::<Vec<_>>();
    let functions_to_register =
        resolve_function_params(runtime.secrets(), &functions_to_register).await;
    let (built, errors) =
        runtime_datafusion_udfs::user_functions::build_all(&functions_to_register).await;
    for err in &errors {
        tracing::error!("{err}");
    }

    let mut registered_function_names = Vec::new();
    let mut functions_to_expose_as_tools = Vec::new();
    for (next, built) in built {
        match built {
            runtime_datafusion_udfs::user_functions::BuiltFunction::Scalar(udf) => {
                warn_beta_once();
                if let Some(existing_name) = registered_scalar_udf_name(ctx, &next.name) {
                    tracing::error!(name = %next.name, existing_name = %existing_name, "Failed to register user function because a scalar UDF with this name is already registered; rename the function to avoid changing query semantics");
                    continue;
                }
                if let Some(existing_name) = registered_table_udf_name(ctx, &next.name) {
                    tracing::error!(name = %next.name, existing_name = %existing_name, "Failed to register user function because a table function with this name is already registered; rename the function to avoid changing query semantics");
                    continue;
                }
                ctx.register_udf(udf.as_ref().clone());
                registered_function_names.push(next.name.clone());
                if function_executes_code(&next) {
                    add_code_executing_function(&next.name);
                }
                upsert_user_function_info(info_from_decl(&next));
                tracing::info!(name = %next.name, from = %next.from, "Registered user function");
                functions_to_expose_as_tools.push(next);
            }
            runtime_datafusion_udfs::user_functions::BuiltFunction::Table(udtf) => {
                warn_beta_once();
                if let Some(existing_name) = registered_scalar_udf_name(ctx, &next.name) {
                    tracing::error!(name = %next.name, existing_name = %existing_name, "Failed to register user table function because a scalar UDF with this name is already registered; rename the function to avoid changing query semantics");
                    continue;
                }
                if let Some(existing_name) = registered_table_udf_name(ctx, &next.name) {
                    tracing::error!(name = %next.name, existing_name = %existing_name, "Failed to register user table function because a table function with this name is already registered; rename the function to avoid changing query semantics");
                    continue;
                }
                ctx.register_udtf(&next.name, udtf);
                registered_function_names.push(next.name.clone());
                if function_executes_code(&next) {
                    add_code_executing_function(&next.name);
                }
                upsert_user_function_info(info_from_decl(&next));
                tracing::info!(name = %next.name, from = %next.from, "Registered user table function");
                if next.as_tool {
                    tracing::warn!(name = %next.name, "User table functions cannot be exposed as tools; skipping tool exposure");
                }
            }
        }
    }
    add_user_functions_to_deny_list(registered_function_names);
    for next in functions_to_expose_as_tools {
        maybe_register_function_as_tool(runtime, &next).await;
    }
}

/// Returns the full list of Spice-specific scalar function names denied for
/// federation by default.
fn denied_spice_function_names() -> Vec<String> {
    let mut names: Vec<String> = [
        "rand",
        BUCKET_SCALAR_UDF_NAME,
        COSINE_DISTANCE_UDF_NAME,
        INNER_PRODUCT_UDF_NAME,
        L2_DISTANCE_UDF_NAME,
        L2_SQUARED_DISTANCE_UDF_NAME,
        L2_NORM_UDF_NAME,
        TRUNCATE_SCALAR_UDF_NAME,
        OBJ_DESCRIPTION_UDF_NAME,
        COL_DESCRIPTION_UDF_NAME,
        EMBED_UDF_NAME,
        #[cfg(feature = "models")]
        AI_UDF_NAME,
        DIGEST_UDF_NAME,
        FLATTEN_JSON_PROPERTIES_UDTF_NAME,
        FLATTEN_JSON_UDTF_NAME,
        JSON_TREE_UDTF_NAME,
        RERANK_UDTF_NAME,
    ]
    .iter()
    .map(ToString::to_string)
    .collect();
    names.extend(json_functions());
    names
}

static BUILTIN_DENIED_SPICE_FUNCTION_NAMES: LazyLock<Vec<String>> =
    LazyLock::new(denied_spice_function_names);

/// Dynamic deny-list: built-ins plus any user-registered functions. Held
/// in a [`RwLock`] so that hot-reload can update the user slice without
/// requiring callers to refactor.
static DENY_LIST: LazyLock<RwLock<Arc<FunctionSupport>>> = LazyLock::new(|| {
    RwLock::new(Arc::new(build_function_support(
        BUILTIN_DENIED_SPICE_FUNCTION_NAMES.as_slice(),
        &[],
    )))
});

/// Names of user-defined functions currently in the deny-list. Kept
/// separate so we can rebuild the combined [`FunctionSupport`] when
/// either slice changes.
static USER_FUNCTION_NAMES: LazyLock<RwLock<Vec<String>>> = LazyLock::new(|| RwLock::new(vec![]));

/// Names of UDFs whose invocation can execute external code or make RPC/API
/// calls. Read-only API keys are not allowed to plan or execute these functions.
static CODE_EXECUTING_FUNCTION_NAMES: LazyLock<RwLock<Vec<String>>> =
    LazyLock::new(|| RwLock::new(vec![]));

/// Metadata for a currently-registered user-defined function. Surfaced
/// through the `list_udfs()` UDTF and the `/v1/functions` HTTP endpoint.
#[derive(Clone, Debug)]
pub struct UserFunctionInfo {
    pub name: String,
    pub kind: String,
    pub volatility: String,
    pub from: String,
    pub description: Option<String>,
}

/// Registry of user-function metadata keyed by name. Kept in sync with
/// the `DataFusion` session context and the federation deny-list.
static USER_FUNCTION_INFO: LazyLock<RwLock<Vec<UserFunctionInfo>>> =
    LazyLock::new(|| RwLock::new(vec![]));

/// Snapshot the current user-function metadata, ordered by registration.
#[must_use]
pub fn user_function_infos() -> Vec<UserFunctionInfo> {
    USER_FUNCTION_INFO.read().clone()
}

fn upsert_user_function_info(info: UserFunctionInfo) {
    let mut guard = USER_FUNCTION_INFO.write();
    if let Some(existing) = guard.iter_mut().find(|i| i.name == info.name) {
        *existing = info;
    } else {
        guard.push(info);
    }
}

fn remove_user_function_info(name: &str) {
    USER_FUNCTION_INFO.write().retain(|i| i.name != name);
}

fn build_function_support(builtins: &[String], user: &[String]) -> FunctionSupport {
    let mut denied: Vec<String> = Vec::with_capacity(builtins.len() + user.len());
    denied.extend(builtins.iter().cloned());
    denied.extend(user.iter().cloned());
    FunctionSupport::new(Some(FunctionRestriction::Deny(denied)), None, None)
}

/// Drop from a deny-list any function a backend can execute natively (the
/// `native` names, typically derived from that backend's unparser dialect).
///
/// A Spice-specific function that the backend's dialect can rewrite into a real
/// remote function — e.g. `cosine_distance` → `array_cosine_distance`, or `rand`
/// → `random()` for `DuckDB` — should be federated, not denied. Names in
/// `native` that aren't in the deny-list are ignored.
fn deny_list_excluding_native(deny: &[String], native: &[&str]) -> Vec<String> {
    let native: HashSet<&str> = native.iter().copied().collect();
    deny.iter()
        .filter(|name| !native.contains(name.as_str()))
        .cloned()
        .collect()
}

fn rebuild_deny_lists(user: &[String]) {
    let combined = build_function_support(BUILTIN_DENIED_SPICE_FUNCTION_NAMES.as_slice(), user);
    *DENY_LIST.write() = Arc::new(combined);
}

/// Add a user function name to the federation deny-list. Idempotent.
pub fn add_user_function_to_deny_list(name: &str) {
    add_user_functions_to_deny_list(std::iter::once(name.to_string()));
}

fn add_user_functions_to_deny_list(names: impl IntoIterator<Item = String>) {
    let user = {
        let mut guard = USER_FUNCTION_NAMES.write();
        let mut changed = false;
        for name in names {
            if !guard.iter().any(|n| n == &name) {
                guard.push(name);
                changed = true;
            }
        }
        changed.then(|| guard.clone())
    };
    if let Some(user) = user {
        rebuild_deny_lists(&user);
    }
}

/// Remove a user function name from the federation deny-list. No-op if
/// not present.
pub fn remove_user_function_from_deny_list(name: &str) {
    remove_user_functions_from_deny_list(&[name.to_string()]);
}

fn remove_user_functions_from_deny_list(names: &[String]) {
    if names.is_empty() {
        return;
    }
    let user = {
        let mut guard = USER_FUNCTION_NAMES.write();
        let before = guard.len();
        guard.retain(|n| !names.iter().any(|name| name == n));
        if guard.len() == before {
            return;
        }
        guard.clone()
    };
    rebuild_deny_lists(&user);
}

/// Add a function name to the read-write API key requirement set. Idempotent.
pub fn add_code_executing_function(name: &str) {
    let mut guard = CODE_EXECUTING_FUNCTION_NAMES.write();
    if !guard.iter().any(|n| n == name) {
        guard.push(name.to_string());
    }
}

/// Remove a function name from the read-write API key requirement set.
pub fn remove_code_executing_function(name: &str) {
    CODE_EXECUTING_FUNCTION_NAMES
        .write()
        .retain(|function_name| function_name != name);
}

/// Returns true when a function requires a read-write API key to execute.
#[must_use]
pub fn is_code_executing_function(name: &str) -> bool {
    CODE_EXECUTING_FUNCTION_NAMES
        .read()
        .iter()
        .any(|function_name| function_name.eq_ignore_ascii_case(name))
}

/// Return the current combined deny-list: built-ins plus every
/// user-registered function. The returned [`Arc`] is cheap to clone and is
/// safe to use from per-query filter pushdown paths.
#[must_use]
pub fn deny_spice_specific_functions() -> Arc<FunctionSupport> {
    Arc::clone(&*DENY_LIST.read())
}

/// Return a backend-specific federation deny-list: the default deny-list with
/// the `native` functions removed so they federate (push down) instead.
///
/// `native` is the set of functions the target backend can execute itself,
/// typically obtained from that backend's unparser dialect (e.g.
/// [`crate::datafusion::dialect::duckdb_native_function_names`]). This is how the
/// deny-list becomes connector/accelerator-aware: each backend only denies the
/// Spice functions it genuinely can't run, and pushes down the ones its dialect
/// can rewrite into a real remote function. Names in `native` that aren't in the
/// deny-list are ignored. User-registered functions are always denied — no
/// remote backend has a built-in equivalent for them — so only the built-in
/// Spice functions are eligible for the carve-out.
///
/// Returns the cached default list when `native` is empty.
#[must_use]
pub fn deny_spice_specific_functions_excluding(native: &[&str]) -> Arc<FunctionSupport> {
    if native.is_empty() {
        return deny_spice_specific_functions();
    }
    let builtins =
        deny_list_excluding_native(BUILTIN_DENIED_SPICE_FUNCTION_NAMES.as_slice(), native);
    let user = USER_FUNCTION_NAMES.read().clone();
    Arc::new(build_function_support(&builtins, &user))
}

/// Return the [`FunctionSupport`] for `DuckDB` connectors and accelerators.
///
/// Identical to [`deny_spice_specific_functions`] except it allows every
/// function the `DuckDB` unparser dialect can rewrite into native `DuckDB` SQL
/// (e.g. `cosine_distance` → `array_cosine_distance`, `rand` → `random()`). The
/// allowed set is derived from
/// [`crate::datafusion::dialect::duckdb_native_function_names`] so it tracks the
/// dialect automatically.
#[must_use]
pub fn deny_spice_functions_for_duckdb() -> Arc<FunctionSupport> {
    deny_spice_specific_functions_excluding(
        &crate::datafusion::dialect::duckdb_native_function_names(),
    )
}

/// The list of Spice function names denied to `DuckDB`: every built-in Spice UDF
/// the `DuckDB` dialect can't rewrite into native SQL, plus all user-registered
/// functions. Shared source of truth for both the local
/// [`deny_spice_functions_for_duckdb`] ([`FunctionSupport`]) and the
/// table-providers-typed [`deny_spice_functions_for_duckdb_table_providers`].
fn denied_function_names_for_duckdb() -> Vec<String> {
    let builtins = deny_list_excluding_native(
        BUILTIN_DENIED_SPICE_FUNCTION_NAMES.as_slice(),
        &crate::datafusion::dialect::duckdb_native_function_names(),
    );
    let user = USER_FUNCTION_NAMES.read().clone();
    let mut denied = Vec::with_capacity(builtins.len() + user.len());
    denied.extend(builtins);
    denied.extend(user);
    denied
}

/// Same deny-list as [`deny_spice_functions_for_duckdb`], but expressed in the
/// `datafusion-table-providers` [`TpFunctionSupport`] type that the fork's
/// `DuckDBTableFactory::with_function_support` seam expects.
///
/// The runtime maintains its deny-list in the local
/// [`data_components::function_support::FunctionSupport`] type, which is a
/// distinct (private-field) reimplementation of the table-providers type. The
/// `DuckDB` factory lives in the fork and takes the fork's type, so we build it
/// here from the shared [`denied_function_names_for_duckdb`] name list rather
/// than trying to convert between the two opaque structs. See issue #10703.
#[must_use]
pub fn deny_spice_functions_for_duckdb_table_providers() -> TpFunctionSupport {
    TpFunctionSupport::new(
        Some(TpFunctionRestriction::Deny(
            denied_function_names_for_duckdb(),
        )),
        None,
        None,
    )
}

fn json_functions() -> Vec<String> {
    let mut ctx = SessionContext::new();
    let existing: HashSet<_> = ctx.state().scalar_functions().keys().cloned().collect();
    let _ = datafusion_functions_json::register_all(&mut ctx);
    ctx.state()
        .scalar_functions()
        .keys()
        .filter(|&k| !existing.contains(k))
        .cloned()
        .collect()
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::DataType;
    use datafusion::logical_expr::expr::ScalarFunction;
    use datafusion::logical_expr::{ColumnarValue, Volatility as DataFusionVolatility, create_udf};
    use datafusion::prelude::{Expr, lit};
    use datafusion::scalar::ScalarValue;
    use datafusion_functions_json::udfs::{
        json_as_text_udf, json_contains_udf, json_get_bool_udf, json_get_float_udf,
        json_get_int_udf, json_get_json_udf, json_get_str_udf, json_get_udf, json_length_udf,
    };

    use super::*;

    /// Helper to create a scalar function expression for testing function support.
    fn make_json_expr(udf: Arc<datafusion::logical_expr::ScalarUDF>) -> Expr {
        Expr::ScalarFunction(ScalarFunction::new_udf(udf, vec![lit("{}"), lit("key")]))
    }

    fn spice_udf(
        impl_: impl Into<datafusion::logical_expr::ScalarUDF>,
    ) -> Arc<datafusion::logical_expr::ScalarUDF> {
        Arc::new(impl_.into())
    }

    fn stub_scalar_udf(name: &str) -> ScalarUDF {
        create_udf(
            name,
            vec![],
            DataType::Int64,
            DataFusionVolatility::Immutable,
            Arc::new(|_| Ok(ColumnarValue::Scalar(ScalarValue::Int64(Some(1))))),
        )
    }

    #[test]
    fn registered_scalar_udf_name_detects_case_insensitive_collision() {
        let ctx = SessionContext::new();
        ctx.register_udf(stub_scalar_udf("custom_fn"));

        assert_eq!(
            registered_scalar_udf_name(&ctx, "CUSTOM_FN").as_deref(),
            Some("custom_fn")
        );
    }

    #[test]
    fn register_async_user_udf_skips_existing_scalar_udf() {
        let ctx = SessionContext::new();
        ctx.register_udf(stub_scalar_udf("existing_fn"));

        assert!(!register_async_user_udf(
            &ctx,
            &stub_scalar_udf("Existing_Fn"),
            "Existing_Fn"
        ));
        assert!(!ctx.udfs().contains("Existing_Fn"));
    }

    fn test_user_function(name: &str, enabled: bool) -> Function {
        use spicepod::component::function::{
            FunctionArg, FunctionKind, FunctionReturns, Signature, Volatility,
        };

        Function {
            name: name.to_string(),
            from: "sql".to_string(),
            enabled,
            description: None,
            kind: FunctionKind::Scalar,
            volatility: Volatility::Immutable,
            signature: Signature {
                tables: vec![],
                args: vec![FunctionArg {
                    name: "x".to_string(),
                    arrow_type: "int64".to_string(),
                }],
                returns: Some(FunctionReturns::Scalar("int64".to_string())),
            },
            body: Some("x".to_string()),
            body_ref: None,
            metadata: std::collections::HashMap::new(),
            params: std::collections::HashMap::new(),
            depends_on: vec![],
            metrics: None,
            as_tool: true,
        }
    }

    #[test]
    fn enabled_function_declarations_filters_disabled_functions() {
        let functions = vec![
            test_user_function("enabled_fn", true),
            test_user_function("disabled_fn", false),
        ];

        let enabled = enabled_function_declarations(&functions);

        assert_eq!(enabled.len(), 1);
        assert_eq!(enabled[0].name, "enabled_fn");
    }

    #[test]
    fn effective_volatility_caps_remote_immutable_to_stable() {
        let mut function = test_user_function("remote_fn", true);
        function.from = "https://example.com/udf".to_string();

        assert_eq!(effective_user_function_volatility(&function), "stable");

        function.volatility = spicepod::component::function::Volatility::Volatile;
        assert_eq!(effective_user_function_volatility(&function), "volatile");
    }

    #[tokio::test]
    async fn resolve_function_params_expands_string_secret_refs() {
        use spicepod::component::secret::Secret;

        let env_file = std::env::temp_dir().join(format!(
            "spice_udf_params_{}_{}.env",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system time should be after UNIX epoch")
                .as_nanos()
        ));
        std::fs::write(&env_file, "UDF_TOKEN=resolved-token\n")
            .expect("test env file should be written");

        let mut secret_params = HashMap::new();
        secret_params.insert(
            "file_path".to_string(),
            env_file.to_string_lossy().into_owned(),
        );
        let mut secrets = runtime_secrets::Secrets::new();
        secrets
            .load_from(&[Secret {
                from: "env".to_string(),
                name: "env".to_string(),
                description: None,
                params: Some(spicepod::param::Params::from_string_map(secret_params)),
            }])
            .await
            .expect("env secret store should load");

        let mut function = test_user_function("remote_fn", true);
        function.params.insert(
            "auth_bearer".to_string(),
            Value::String("${ secrets:UDF_TOKEN }".to_string()),
        );
        function
            .params
            .insert("batch_size".to_string(), Value::Number(16_u64.into()));

        let resolved =
            resolve_function_params(Arc::new(tokio::sync::RwLock::new(secrets)), &[function]).await;

        assert_eq!(
            resolved[0].params.get("auth_bearer"),
            Some(&Value::String("resolved-token".to_string()))
        );
        assert_eq!(
            resolved[0].params.get("batch_size"),
            Some(&Value::Number(16_u64.into()))
        );

        std::fs::remove_file(env_file).ok();
    }

    #[test]
    fn deny_list_blocks_json_functions() {
        let support = deny_spice_specific_functions();

        let json_udfs = vec![
            json_get_udf(),
            json_get_str_udf(),
            json_get_int_udf(),
            json_get_float_udf(),
            json_get_bool_udf(),
            json_get_json_udf(),
            json_as_text_udf(),
            json_contains_udf(),
            json_length_udf(),
        ];

        for udf in json_udfs {
            let name = udf.name().to_string();
            let expr = make_json_expr(udf);
            assert!(
                !support.supports(&expr),
                "{name} should be denied by deny_spice_specific_functions"
            );
        }
    }

    #[test]
    fn deny_list_blocks_spice_builtins() {
        let support = deny_spice_specific_functions();

        let spice_udfs: Vec<Arc<datafusion::logical_expr::ScalarUDF>> = vec![
            spice_udf(CosineDistance::new()),
            spice_udf(Bucket::new()),
            spice_udf(Truncate::new()),
            Arc::new(INSTANCE.clone()),
            spice_udf(FlattenJsonPropertiesScalar::new()),
            spice_udf(FlattenJsonScalar::new()),
            spice_udf(JsonTreeScalar::new()),
        ];

        for udf in spice_udfs {
            let name = udf.name().to_string();
            let expr = make_json_expr(udf);
            assert!(
                !support.supports(&expr),
                "{name} should be denied by deny_spice_specific_functions"
            );
        }
    }

    /// Build a no-arg scalar-function expression with the given name so we can
    /// probe a [`FunctionSupport`] by name regardless of the real UDF impl.
    fn make_named_expr(name: &str) -> Expr {
        Expr::ScalarFunction(ScalarFunction::new_udf(
            Arc::new(stub_scalar_udf(name)),
            vec![],
        ))
    }

    #[test]
    fn duckdb_deny_list_allows_dialect_native_functions() {
        // The DuckDB unparser dialect rewrites these into native DuckDB SQL
        // (cosine_distance -> array_cosine_distance, inner_product ->
        // array_inner_product, rand -> random()), so they must federate rather
        // than be denied. Note `rand` is allowed purely by virtue of being in the
        // dialect — no manual carve-out.
        let support = deny_spice_functions_for_duckdb();
        for name in [COSINE_DISTANCE_UDF_NAME, INNER_PRODUCT_UDF_NAME, "rand"] {
            assert!(
                support.supports(&make_named_expr(name)),
                "{name} has a native DuckDB equivalent and should be pushed down"
            );
        }
        // No native DuckDB equivalent — must stay denied.
        let json_name = json_get_str_udf().name().to_string();
        for name in [EMBED_UDF_NAME, json_name.as_str()] {
            assert!(
                !support.supports(&make_named_expr(name)),
                "{name} has no native DuckDB equivalent and must stay denied"
            );
        }
    }

    #[test]
    fn default_deny_list_denies_every_builtin() {
        // Exhaustive "cannot push down" coverage: with no backend carve-out, the
        // generic deny-list must block EVERY Spice-specific built-in (including
        // functions a specific dialect could otherwise push down, e.g.
        // cosine_distance / rand).
        let support = deny_spice_specific_functions();
        for name in BUILTIN_DENIED_SPICE_FUNCTION_NAMES.iter() {
            assert!(
                !support.supports(&make_named_expr(name)),
                "{name} must be denied by the default (backend-agnostic) deny-list"
            );
        }
    }

    #[test]
    fn excluding_subtracts_only_listed_native_functions() {
        let support = deny_spice_specific_functions_excluding(&[COSINE_DISTANCE_UDF_NAME]);
        assert!(
            support.supports(&make_named_expr(COSINE_DISTANCE_UDF_NAME)),
            "explicitly-excluded cosine_distance should be allowed"
        );
        assert!(
            !support.supports(&make_named_expr("rand")),
            "rand was not excluded and must stay denied"
        );
        // An empty exclusion behaves exactly like the default deny-list.
        let default_support = deny_spice_specific_functions_excluding(&[]);
        assert!(!default_support.supports(&make_named_expr(COSINE_DISTANCE_UDF_NAME)));
    }

    #[test]
    fn duckdb_carveout_tracks_the_dialect() {
        // Regression guard: the DuckDB allow-state of every built-in deny-listed
        // function must match exactly what the DuckDB dialect can unparse, so the
        // dialect and the deny-list can never drift.
        let native: HashSet<&str> = crate::datafusion::dialect::duckdb_native_function_names()
            .into_iter()
            .collect();
        let support = deny_spice_functions_for_duckdb();
        for denied in BUILTIN_DENIED_SPICE_FUNCTION_NAMES.iter() {
            let expected_allowed = native.contains(denied.as_str());
            assert_eq!(
                support.supports(&make_named_expr(denied)),
                expected_allowed,
                "{denied}: DuckDB allow-state must match dialect native support"
            );
        }
    }

    #[test]
    fn duckdb_pushable_set_is_exactly_the_dialect_natives() {
        // Pin the exact set of deny-listed Spice functions that CAN be pushed
        // down to DuckDB. Everything else in the deny-list CANNOT. This makes the
        // can/can't partition explicit: if a dialect change makes another Spice
        // function pushable (or stops one), this test must be updated on purpose.
        //
        // `cosine_distance` -> array_cosine_distance, `inner_product` ->
        // array_inner_product, `rand` -> random(). (Note: l2 distance federates
        // via the non-deny-listed `array_distance` UDF, so it isn't part of this
        // deny-list carve-out.)
        use std::collections::BTreeSet;
        let support = deny_spice_functions_for_duckdb();
        let pushable: BTreeSet<&str> = BUILTIN_DENIED_SPICE_FUNCTION_NAMES
            .iter()
            .filter(|name| support.supports(&make_named_expr(name)))
            .map(String::as_str)
            .collect();
        assert_eq!(
            pushable,
            BTreeSet::from([COSINE_DISTANCE_UDF_NAME, INNER_PRODUCT_UDF_NAME, "rand"]),
            "unexpected change to the set of Spice functions pushable to DuckDB"
        );
    }
}
