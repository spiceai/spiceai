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

use std::collections::HashSet;
use std::sync::{Arc, LazyLock};

use crate::datafusion::udtf::json_properties::{
    FLATTEN_JSON_PROPERTIES_UDTF_NAME, FlattenJsonPropertiesScalar, FlattenJsonPropertiesTableFunc,
};
use crate::datafusion::udtf::json_tree::{JSON_TREE_UDTF_NAME, JsonTreeScalar, JsonTreeTableFunc};
use crate::embeddings::udtf::{VECTOR_SEARCH_UDTF_NAME, VectorSearchTableFunc};
use crate::search::full_text::udtf::{TEXT_SEARCH_UDTF_NAME, TextSearchTableFunc};
use crate::search::rrf;
use crate::search::rrf::RRF_UDF_NAME;
use crate::search::util::parse_explicit_primary_keys;
use datafusion::functions::math::random::RandomFunc;
use datafusion::logical_expr::ScalarUDF;
use datafusion::prelude::SessionContext;
use datafusion_table_providers::util::supported_functions::{FunctionRestriction, FunctionSupport};
use parking_lot::RwLock;
#[cfg(feature = "models")]
use runtime_datafusion_udfs::{
    ai::{AI_UDF_NAME, Ai},
    embed::{self, EMBED_UDF_NAME},
};
#[cfg(not(feature = "models"))]
const EMBED_UDF_NAME: &str = "embed";
use runtime_datafusion_udfs::{
    alias::ScalarUDFAlias,
    bucket::{BUCKET_SCALAR_UDF_NAME, Bucket},
    cosine_distance::{COSINE_DISTANCE_UDF_NAME, CosineDistance},
    digest_many::{DIGEST_UDF_NAME, INSTANCE},
    org::{ORG_UDF_NAME, OrgUdf},
    role::{ROLE_UDF_NAME, RoleUdf},
    session_property::{SESSION_PROPERTY_UDF_NAME, SessionPropertyUdf},
    truncate::{TRUNCATE_SCALAR_UDF_NAME, Truncate},
    user::{USER_UDF_NAME, UserUdf},
};

/// Register core scalar UDFs that have no runtime dependencies.
///
/// These UDFs only need a [`SessionContext`] and can be registered on any
/// context, including isolated ones like the refresh-task context.
pub fn register_core_scalar_udfs(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDFAlias::new(Arc::new(RandomFunc::default()), "rand").into());
    ctx.register_udf(Bucket::new().into());
    ctx.register_udf(CosineDistance::new().into());
    ctx.register_udf(Truncate::new().into());
    ctx.register_udf(INSTANCE.clone());
    ctx.register_udf(UserUdf::new().into());
    ctx.register_udf(OrgUdf::new().into());
    ctx.register_udf(RoleUdf::new().into());
    ctx.register_udf(SessionPropertyUdf::new().into());
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

    // `flatten_json_properties` / `json_tree` — JSON-Schema and generic JSON
    // shredders. Registered as both UDTF (FROM-clause, literal input) and
    // ScalarUDF returning `List<Struct<...>>` (per-row / LATERAL via UNNEST).
    ctx.register_udtf(
        FLATTEN_JSON_PROPERTIES_UDTF_NAME,
        Arc::new(FlattenJsonPropertiesTableFunc::new()),
    );
    ctx.register_udf(FlattenJsonPropertiesScalar::new().into());
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

    register_user_functions(runtime, ctx).await;
}

/// Emits the user-defined functions ALPHA warning at most once per
/// process. Called from both startup registration and hot-reload so the
/// user sees it whenever a `functions:` entry becomes active for the
/// first time.
fn warn_alpha_once() {
    static ALPHA_WARNING: std::sync::Once = std::sync::Once::new();
    ALPHA_WARNING.call_once(|| {
        tracing::warn!(
            "User-defined functions (spicepod `functions:` section) are in ALPHA. \
             They are not yet supported for production use; behavior, APIs, and on-disk \
             format may change without notice. See: \
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

    warn_alpha_once();

    let (built, errors) = runtime_datafusion_udfs::user_functions::build_all(&app.functions);
    for err in &errors {
        tracing::error!("{err}");
    }

    for (decl, built) in built {
        match built {
            runtime_datafusion_udfs::user_functions::BuiltFunction::Scalar(udf) => {
                ctx.register_udf(udf.as_ref().clone());
                add_user_function_to_deny_list(&decl.name);
                upsert_user_function_info(info_from_decl(&decl));
                tracing::info!(
                    name = %decl.name,
                    from = %decl.from,
                    "Registered user function"
                );
                maybe_register_function_as_tool(runtime, &decl).await;
            }
        }
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
async fn maybe_register_function_as_tool(
    runtime: &crate::Runtime,
    decl: &spicepod::component::function::Function,
) {
    if !decl.as_tool {
        return;
    }
    let df_weak = Arc::downgrade(&runtime.df);
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
            tools_map.insert(name.clone(), crate::tools::Tooling::Tool(tool));
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
pub fn register_async_user_udf(ctx: &SessionContext, udf: &ScalarUDF, name: &str) {
    ctx.register_udf(udf.clone());
    add_user_function_to_deny_list(name);
}

fn info_from_decl(decl: &spicepod::component::function::Function) -> UserFunctionInfo {
    use spicepod::component::function::{FunctionKind, Volatility};
    let kind = match decl.kind {
        FunctionKind::Scalar => "scalar",
        FunctionKind::Aggregate => "aggregate",
        FunctionKind::Window => "window",
        FunctionKind::Table => "table",
    };
    let volatility = match decl.volatility {
        Volatility::Immutable => "immutable",
        Volatility::Stable => "stable",
        Volatility::Volatile => "volatile",
    };
    UserFunctionInfo {
        name: decl.name.clone(),
        kind: kind.to_string(),
        volatility: volatility.to_string(),
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

    // First pass: collect every function that needs to go away (removed or
    // changed). Do all the lock-free work (DF deregister, deny-list, info
    // registry) first so the tools-map write lock is held only once for the
    // batch of tool drops.
    let mut tools_to_drop: Vec<String> = Vec::new();
    for current in &current_app.functions {
        let needs_drop = match new_app.functions.iter().find(|f| f.name == current.name) {
            Some(next) => next != current,
            None => true,
        };
        if needs_drop {
            ctx.deregister_udf(&current.name);
            remove_user_function_from_deny_list(&current.name);
            remove_user_function_info(&current.name);
            if current.as_tool {
                tools_to_drop.push(current.name.clone());
            }
            tracing::info!(name = %current.name, "Deregistered user function");
        }
    }
    if !tools_to_drop.is_empty() {
        let mut tools_map = runtime.tools.write().await;
        for name in &tools_to_drop {
            tools_map.remove(name);
        }
    }

    // Build + register any new or changed declarations.
    for next in &new_app.functions {
        let needs_register = match current_app.functions.iter().find(|f| f.name == next.name) {
            Some(prev) => prev != next,
            None => true,
        };
        if !needs_register {
            continue;
        }
        match runtime_datafusion_udfs::user_functions::build_function(next) {
            Ok(runtime_datafusion_udfs::user_functions::BuiltFunction::Scalar(udf)) => {
                warn_alpha_once();
                ctx.register_udf(udf.as_ref().clone());
                add_user_function_to_deny_list(&next.name);
                upsert_user_function_info(info_from_decl(next));
                tracing::info!(name = %next.name, from = %next.from, "Registered user function");
                maybe_register_function_as_tool(runtime, next).await;
            }
            Err(e) => tracing::error!("{e}"),
        }
    }
}

/// Names of built-in Spice UDFs that must never be pushed down to remote
/// databases. These are resolved at process start and don't change.
fn builtin_deny_list() -> Vec<String> {
    let mut names: Vec<String> = [
        "rand",
        BUCKET_SCALAR_UDF_NAME,
        COSINE_DISTANCE_UDF_NAME,
        TRUNCATE_SCALAR_UDF_NAME,
        EMBED_UDF_NAME,
        #[cfg(feature = "models")]
        AI_UDF_NAME,
        DIGEST_UDF_NAME,
        USER_UDF_NAME,
        ORG_UDF_NAME,
        ROLE_UDF_NAME,
        SESSION_PROPERTY_UDF_NAME,
        FLATTEN_JSON_PROPERTIES_UDTF_NAME,
        JSON_TREE_UDTF_NAME,
    ]
    .iter()
    .map(ToString::to_string)
    .collect();
    names.extend(json_functions());
    names
}

/// Dynamic deny-list: built-ins plus any user-registered functions. Held
/// in a [`RwLock`] so that hot-reload can update the user slice without
/// requiring callers to refactor.
static DENY_LIST: LazyLock<RwLock<Arc<FunctionSupport>>> =
    LazyLock::new(|| RwLock::new(Arc::new(build_function_support(&builtin_deny_list(), &[]))));

/// Names of user-defined functions currently in the deny-list. Kept
/// separate so we can rebuild the combined [`FunctionSupport`] when
/// either slice changes.
static USER_FUNCTION_NAMES: LazyLock<RwLock<Vec<String>>> = LazyLock::new(|| RwLock::new(vec![]));

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

fn rebuild_deny_list() {
    let user = USER_FUNCTION_NAMES.read().clone();
    let combined = build_function_support(&builtin_deny_list(), &user);
    *DENY_LIST.write() = Arc::new(combined);
}

/// Add a user function name to the federation deny-list. Idempotent.
pub fn add_user_function_to_deny_list(name: &str) {
    {
        let mut guard = USER_FUNCTION_NAMES.write();
        if guard.iter().any(|n| n == name) {
            return;
        }
        guard.push(name.to_string());
    }
    rebuild_deny_list();
}

/// Remove a user function name from the federation deny-list. No-op if
/// not present.
pub fn remove_user_function_from_deny_list(name: &str) {
    {
        let mut guard = USER_FUNCTION_NAMES.write();
        let before = guard.len();
        guard.retain(|n| n != name);
        if guard.len() == before {
            return;
        }
    }
    rebuild_deny_list();
}

/// Return the current combined deny-list: built-ins plus every
/// user-registered function.
///
/// Returns an owned [`FunctionSupport`] by cloning the internal cached
/// value. This is called at accelerator/table setup time (infrequent) so
/// the clone cost is not meaningful.
#[must_use]
pub fn deny_spice_specific_functions() -> FunctionSupport {
    (**DENY_LIST.read()).clone()
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
    use datafusion::logical_expr::expr::ScalarFunction;
    use datafusion::prelude::{Expr, lit};
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
}
