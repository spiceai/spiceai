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

//! Which functions a remote backend must not be asked to evaluate.
//!
//! Federating a filter to a data source is only safe if that source can evaluate
//! every function in it. Four sets of names are unsafe to push down:
//!
//! 1. **Spice functions** — the UDFs Spice defines (`bucket`, `cosine_distance`,
//!    `rerank`, …) plus any the user registers. No remote source knows them, so
//!    they are denied by default; a backend whose unparser dialect rewrites one
//!    into a real remote function (`cosine_distance` → `array_cosine_distance`)
//!    carves it back out by declaring it [`FunctionSupportBuilder::native`].
//! 2. **`DataFusion` built-ins a specific backend cannot evaluate** — e.g. the
//!    nested array/list/map functions relative to `PostgreSQL`. These are
//!    allowed by default; only the backend knows which subset it lacks, so it
//!    supplies them via [`FunctionSupportBuilder::deny_also`].
//! 3. **The `DataFusion` cast built-ins** — [`DATAFUSION_CAST_BUILTINS`]
//!    (`arrow_cast`, `cast_to_type`, …). The exception to set 2's default: they
//!    are denied for every backend, because no source can be assumed to answer
//!    them as `DataFusion` does. A source that is itself `DataFusion` (a
//!    `FlightSQL` server, say) could, but nothing tells the connector that it is
//!    talking to one, so these are kept local there too.
//! 4. **`DataFusion` built-ins that describe the plan** — the
//!    [`PLAN_INTROSPECTION_BUILTINS`] (`arrow_typeof`, …). No backend can
//!    answer them, so every policy denies them and no carve-out re-admits them.
//!
//! Set 1 lives here because Spice owns it: every Spice function registers its
//! name at its definition site with [`register_spice_function!`], collected into
//! [`SPICE_FUNCTION_REGISTRATIONS`] at link time. That is what keeps the
//! deny-list from drifting as UDFs are added — there is no separate list to
//! maintain, and no window during startup where the set is incomplete.
//!
//! Besides names, every policy refuses a call whose *clauses* the unparser
//! cannot carry to the backend: `IGNORE NULLS`, and an aggregate `ORDER BY` the
//! answer depends on. See [`aggregate_clauses_survive_unparsing`] and
//! [`window_clauses_survive_unparsing`].

use std::collections::HashSet;
use std::sync::LazyLock;

use datafusion::logical_expr::expr::{
    AggregateFunction, NullTreatment, ScalarFunction, WindowFunction,
};
use datafusion::logical_expr::utils::AggregateOrderSensitivity;
use datafusion_table_providers::util::supported_functions::{
    AggregateCallSupport, FunctionRestriction, FunctionSupport, ScalarCallSupport,
    WindowCallSupport,
};
use linkme::distributed_slice;

/// Re-exported so a crate invoking [`register_spice_function!`] can bring
/// `linkme` into scope with `use runtime_udfs_api::linkme;` instead of taking its
/// own dependency. `$crate` does not resolve inside an attribute-macro path, so
/// the expansion has to name `linkme` unqualified.
pub use linkme;
use parking_lot::RwLock;

/// A Spice-defined function name that a remote backend cannot be assumed to
/// evaluate. Created by [`register_spice_function!`].
pub struct SpiceFunctionRegistration {
    /// The function's name, read through a fn pointer because the name
    /// constants are `static`s and a `static` initializer cannot read another
    /// `static`. Mirrors `DataConnectorRegistration::constructor`.
    pub name: fn() -> &'static str,
}

impl SpiceFunctionRegistration {
    #[must_use]
    pub const fn new(name: fn() -> &'static str) -> Self {
        Self { name }
    }
}

/// Every Spice function name, collected at link time from the
/// [`register_spice_function!`] invocations in each defining crate.
#[distributed_slice]
pub static SPICE_FUNCTION_REGISTRATIONS: [SpiceFunctionRegistration] = [..];

/// Registers a Spice function name so it is never federated to a source that
/// has not declared it native.
///
/// Invoke it beside the function's name constant, so adding a UDF adds its
/// deny-list entry in the same edit:
///
/// ```ignore
/// pub const BUCKET_SCALAR_UDF_NAME: &str = "bucket";
/// register_spice_function!(BUCKET_DENY_REGISTRATION, BUCKET_SCALAR_UDF_NAME);
/// ```
///
/// # Linking
///
/// The registration is a `#[linkme::distributed_slice]` static, so it is present
/// only when its crate is actually linked — being a Cargo dependency is not
/// enough if nothing references the crate. Every crate registering a function
/// must therefore be reachable from the binary, exactly as with
/// `register_data_connector!`. A missed registration does not fail loudly: the
/// name silently becomes federatable, so `runtime` carries a test asserting the
/// full expected set is present.
#[macro_export]
macro_rules! register_spice_function {
    ($static_name:ident, $name:expr) => {
        #[linkme::distributed_slice($crate::SPICE_FUNCTION_REGISTRATIONS)]
        pub static $static_name: $crate::SpiceFunctionRegistration =
            $crate::SpiceFunctionRegistration::new(|| $name);
    };
}

/// Names of user-registered functions currently in the deny-list. Kept separate
/// from the link-time set because it changes as functions are registered and
/// dropped.
static USER_FUNCTION_NAMES: LazyLock<RwLock<Vec<String>>> = LazyLock::new(|| RwLock::new(vec![]));

/// Adds a user function name to the deny-list. Idempotent.
pub fn add_user_function(name: &str) {
    add_user_functions(std::iter::once(name.to_string()));
}

/// Adds several user function names to the deny-list. Idempotent.
pub fn add_user_functions(names: impl IntoIterator<Item = String>) {
    let mut guard = USER_FUNCTION_NAMES.write();
    for name in names {
        if !guard.iter().any(|n| n == &name) {
            guard.push(name);
        }
    }
}

/// Removes a user function name from the deny-list. No-op if not present.
pub fn remove_user_function(name: &str) {
    remove_user_functions(&[name.to_string()]);
}

/// Removes several user function names from the deny-list.
pub fn remove_user_functions(names: &[String]) {
    if names.is_empty() {
        return;
    }
    let mut guard = USER_FUNCTION_NAMES.write();
    guard.retain(|n| !names.iter().any(|name| name == n));
}

/// The user function names currently denied.
#[must_use]
pub fn user_function_names() -> Vec<String> {
    USER_FUNCTION_NAMES.read().clone()
}

/// Whether `name` is a user-registered function **right now**.
///
/// Read at the moment a plan is checked rather than when a provider was built,
/// which is what [`FunctionSupportBuilder::build`] needs: the name list it
/// freezes into a [`FunctionRestriction::Deny`] is a snapshot, and a function
/// registered after that provider exists is absent from it.
#[must_use]
pub fn is_user_function(name: &str) -> bool {
    USER_FUNCTION_NAMES.read().iter().any(|n| n == name)
}

/// The link-time set of Spice function names, plus the JSON functions
/// `datafusion-functions-json` contributes.
#[must_use]
pub fn spice_function_names() -> Vec<String> {
    SPICE_FUNCTION_REGISTRATIONS
        .iter()
        .map(|registration| (registration.name)().to_string())
        .chain(json_function_names().iter().cloned())
        .collect()
}

/// The scalar functions `datafusion-functions-json` registers, found by diffing
/// a session's function registry before and after registering the crate. They
/// have no name constants to register, so they are derived once here.
#[must_use]
pub fn json_function_names() -> &'static [String] {
    static NAMES: LazyLock<Vec<String>> = LazyLock::new(|| {
        let mut ctx = util::session_state::session_context();
        let existing: HashSet<_> = ctx.state().scalar_functions().keys().cloned().collect();
        // A failure here would yield an incomplete list, and this list is a
        // *deny*-list: a missing name federates instead of being blocked, so the
        // source is asked to evaluate a function it does not have. Registration
        // into a context created a line above cannot actually fail, so make that
        // assumption loud rather than silent.
        if let Err(error) = datafusion_functions_json::register_all(&mut ctx) {
            debug_assert!(false, "registering the JSON functions failed: {error}");
            tracing::error!(
                "Failed to enumerate the JSON functions for the federation deny-list ({error}). JSON functions may be pushed down to sources that cannot evaluate them."
            );
        }
        ctx.state()
            .scalar_functions()
            .keys()
            .filter(|&name| !existing.contains(name))
            .cloned()
            .collect()
    });
    &NAMES
}

/// `DataFusion`'s built-in nested (array/list/map) scalar functions, by
/// canonical name and every alias.
///
/// A backend that cannot evaluate these passes the subset it lacks to
/// [`FunctionSupportBuilder::deny_also`]. The set is fixed for the lifetime of
/// the process, so it is computed once.
#[must_use]
pub fn datafusion_nested_function_names() -> &'static [String] {
    static NAMES: LazyLock<Vec<String>> = LazyLock::new(|| {
        datafusion::functions_nested::all_default_nested_functions()
            .iter()
            .flat_map(|udf| {
                std::iter::once(udf.name().to_string()).chain(udf.aliases().iter().cloned())
            })
            .collect()
    });
    &NAMES
}

/// `DataFusion`'s own cast built-ins: `arrow_cast(expr, 'LargeUtf8')` and
/// `arrow_try_cast` cast to an Arrow type named by a string, `cast_to_type` and
/// `try_cast_to_type` to the type of their second argument, and the `try_`
/// forms answer NULL where the cast fails. Every [`FunctionSupportBuilder`]
/// denies them, and no backend's native carve-out re-admits them, so they are
/// evaluated above the federated scan by `DataFusion`'s cast kernel.
///
/// Most SQL engines define none of these names, so a federated call failed
/// remotely as an unknown function (issue #14444). `DuckDB` does define a
/// `cast_to_type`, but it casts by `DuckDB`'s rules rather than Arrow's —
/// `cast_to_type(1.5, 1)` is `2` there and `1` locally — so federating it is not
/// faithful either.
pub const DATAFUSION_CAST_BUILTINS: &[&str] = &[
    "arrow_cast",
    "arrow_try_cast",
    "cast_to_type",
    "try_cast_to_type",
];

/// `DataFusion` built-ins that describe the *plan* rather than the data, so no
/// backend can evaluate them faithfully and every [`FunctionSupportBuilder`]
/// denies them: `arrow_typeof` answers with the plan's Arrow type, `arrow_field`
/// and `arrow_metadata` with the plan's field and its metadata, and
/// `with_metadata` attaches metadata to a plan field. A backend cannot be
/// assumed to define functions of these names — most SQL engines do not, so a
/// federated call fails remotely as an unknown function (issue #14334) — and a
/// backend that does define them, such as a `DataFusion`-based source, would
/// answer about or modify its own plan, not this one. Evaluating them locally,
/// above the federated scan, is the only reading that answers the question
/// asked.
pub const PLAN_INTROSPECTION_BUILTINS: &[&str] = &[
    "arrow_typeof",
    "arrow_field",
    "arrow_metadata",
    "with_metadata",
];

/// Removes from `names` everything the backend declares native.
fn excluding_native(names: impl IntoIterator<Item = String>, native: &[&str]) -> Vec<String> {
    if native.is_empty() {
        return names.into_iter().collect();
    }
    let native: HashSet<&str> = native.iter().copied().collect();
    names
        .into_iter()
        .filter(|name| !native.contains(name.as_str()))
        .collect()
}

/// Builds the [`FunctionSupport`] for one backend.
///
/// Defaults to denying every Spice function (link-time set plus user-registered)
/// the [`DATAFUSION_CAST_BUILTINS`] and the [`PLAN_INTROSPECTION_BUILTINS`], and
/// to refusing the aggregate and window calls whose clauses the unparser drops
/// ([`aggregate_clauses_survive_unparsing`], [`window_clauses_survive_unparsing`]),
/// and nothing else — correct for a source whose dialect rewrites none of the
/// Spice functions.
#[derive(Default)]
pub struct FunctionSupportBuilder<'a> {
    native: &'a [&'a str],
    deny_also: Vec<String>,
    scalar_call: Option<ScalarCallSupport>,
    aggregate_call: Option<AggregateCallSupport>,
    window_call: Option<WindowCallSupport>,
    renders_aggregate_order_by: Option<fn(&str) -> bool>,
}

impl<'a> FunctionSupportBuilder<'a> {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Spice function names this backend evaluates itself, normally its
    /// unparser dialect's native-function names. These federate instead of
    /// being denied.
    ///
    /// Applies only to the Spice set — user-registered functions are never
    /// carved out, since no remote source can have an equivalent.
    #[must_use]
    pub fn native(mut self, native: &'a [&'a str]) -> Self {
        self.native = native;
        self
    }

    /// Additional names to deny: `DataFusion` built-ins this backend cannot
    /// evaluate. Only the backend knows these, so it supplies them.
    #[must_use]
    pub fn deny_also(mut self, names: impl IntoIterator<Item = String>) -> Self {
        self.deny_also.extend(names);
        self
    }

    /// A per-call check refusing the *call shapes* this backend's dialect cannot
    /// translate, for a function whose name it carved out with [`Self::native`].
    ///
    /// Supply it here rather than through
    /// `FunctionSupport::with_scalar_call_support` after [`Self::build`]: that
    /// setter *replaces* the per-call check, and [`Self::build`] installs one of
    /// its own for the live user-function registry. Both are consulted, and a
    /// call has to satisfy both to federate.
    #[must_use]
    pub fn scalar_call(mut self, scalar_call: ScalarCallSupport) -> Self {
        self.scalar_call = Some(scalar_call);
        self
    }

    /// A per-call check refusing the aggregate *call shapes* this backend cannot
    /// evaluate.
    ///
    /// Supply it here rather than through
    /// `FunctionSupport::with_aggregate_call_support` after [`Self::build`], for the
    /// reason [`Self::scalar_call`] gives: that setter *replaces* the check, and
    /// [`Self::build`] installs [`aggregate_clauses_survive_unparsing`] for every
    /// backend. Both are consulted, and a call has to satisfy both to federate.
    #[must_use]
    pub fn aggregate_call(mut self, aggregate_call: AggregateCallSupport) -> Self {
        self.aggregate_call = Some(aggregate_call);
        self
    }

    /// The window counterpart of [`Self::aggregate_call`], consulted together
    /// with [`window_clauses_survive_unparsing`].
    #[must_use]
    pub fn window_call(mut self, window_call: WindowCallSupport) -> Self {
        self.window_call = Some(window_call);
        self
    }

    /// Whether this backend's unparser dialect renders the argument-list
    /// `ORDER BY` of the aggregate of this name itself, so
    /// [`aggregate_clauses_survive_unparsing`] lets it federate with its ordering.
    /// The `DuckDB` dialect does, for `array_agg(x ORDER BY y)` among others.
    #[must_use]
    pub fn renders_aggregate_order_by(mut self, renders: fn(&str) -> bool) -> Self {
        self.renders_aggregate_order_by = Some(renders);
        self
    }

    /// The denied scalar-function names, in the order the deny-list is built:
    /// Spice functions minus the native carve-out, then user functions, then
    /// any backend-specific additions, then the [`DATAFUSION_CAST_BUILTINS`] and
    /// the [`PLAN_INTROSPECTION_BUILTINS`], which no carve-out reaches because no
    /// backend can evaluate them.
    #[must_use]
    pub fn denied_names(self) -> Vec<String> {
        let spice = excluding_native(spice_function_names(), self.native);
        let user = user_function_names();
        spice
            .into_iter()
            .chain(user)
            .chain(self.deny_also)
            .chain(
                DATAFUSION_CAST_BUILTINS
                    .iter()
                    .chain(PLAN_INTROSPECTION_BUILTINS)
                    .map(|name| (*name).to_string()),
            )
            .collect()
    }

    /// The [`FunctionSupport`] to hand a federated provider or table-provider
    /// factory.
    ///
    /// The denied *names* are a snapshot, so they cannot answer for a user
    /// function registered after this call — and providers are built once while
    /// [`add_user_function`] runs for the life of the process. Every accelerator
    /// engine is constructed in `RuntimeBuilder::build` before that same `build`
    /// registers the spicepod's `functions:` entries, so a SQL accelerator's
    /// snapshot names no user function at all; a tool-backed SQL UDF registers
    /// while datasets and catalogs load; and a hot reload applies its function
    /// diff after the components, without rebuilding a component that did not
    /// itself change. A name absent from the snapshot federates, so the remote
    /// is asked to evaluate a function it does not have — or, where it happens
    /// to have one of that name, answers from a different function.
    ///
    /// So the per-call check reads the registry live. It only ever narrows what
    /// the name list allows, which is why the snapshot is left as it is rather
    /// than removed: it already denies everything registered before this call,
    /// and this closes the rest.
    ///
    /// The aggregate and window checks are installed here for the same reason:
    /// [`aggregate_clauses_survive_unparsing`] and
    /// [`window_clauses_survive_unparsing`] hold for every backend, so they are
    /// composed with the backend's own checks rather than left for each caller
    /// to remember.
    #[must_use]
    pub fn build(mut self) -> FunctionSupport {
        let backend_call = self.scalar_call.take();
        let backend_aggregate = self.aggregate_call.take();
        let backend_window = self.window_call.take();
        let renders_order_by = self.renders_aggregate_order_by;
        FunctionSupport::new(
            Some(FunctionRestriction::Deny(self.denied_names())),
            None,
            None,
        )
        .with_scalar_call_support(std::sync::Arc::new(
            move |call: &ScalarFunction, scope: Option<&datafusion::common::DFSchema>| {
                // The scope is the backend's to interpret, so it is passed
                // through untouched: a rendering whose correctness depends on an
                // operand's declared type reads it, and the user-function check
                // here does not.
                !is_user_function(call.func.name())
                    && backend_call
                        .as_ref()
                        .is_none_or(|supports| supports(call, scope))
            },
        ))
        .with_aggregate_call_support(std::sync::Arc::new(move |call: &AggregateFunction| {
            aggregate_clauses_survive_unparsing(call, |name| {
                renders_order_by.is_some_and(|renders| renders(name))
            }) && backend_aggregate
                .as_ref()
                .is_none_or(|supports| supports(call))
        }))
        .with_window_call_support(std::sync::Arc::new(move |call: &WindowFunction| {
            window_clauses_survive_unparsing(call)
                && backend_window
                    .as_ref()
                    .is_none_or(|supports| supports(call))
        }))
    }
}

/// Aggregates whose answer does not depend on the order of their input, though
/// `DataFusion` does not declare them order-insensitive the way it does `count`,
/// `sum`, `min` and `max`: they keep `AggregateUDFImpl::order_sensitivity`'s
/// conservative default. `median` is a function of the values alone. `avg` was
/// measured for the `BigQuery` dialect's filter rewriting, which relies on the
/// same reading: `1e16, 1.0, -1e16, 2.0, -1.0` averages to `0.4` under `ASC`,
/// under `DESC` and unordered.
const ORDER_INDEPENDENT_AGGREGATES: &[&str] = &["avg", "median"];

/// Whether the SQL the unparser writes for this aggregate call asks the backend
/// for the answer `DataFusion` would compute, so the call can federate.
///
/// The unparser drops two clauses. It never renders `IGNORE NULLS` — a dialect's
/// aggregate override is not even handed it — so `first_value(x) IGNORE NULLS`
/// would reach the backend respecting nulls. And it renders an argument-list
/// `ORDER BY` only as `WITHIN GROUP`, for the aggregates that take that form, so
/// `array_agg(x ORDER BY y)` reaches `PostgreSQL` as `array_agg(x)` and comes back
/// in whatever order the backend produced: `[1, 2, 3, 4, 5]` where `DataFusion`
/// answers `[5, 4, 3, 2, 1]` for `ORDER BY x DESC`. A call that would lose either
/// stays local — unless the dropped `ORDER BY` cannot change the answer, because
/// `DataFusion` declares the aggregate order-insensitive or it is one of the
/// `ORDER_INDEPENDENT_AGGREGATES`, or `renders_order_by` says the backend's
/// dialect renders it itself.
#[must_use]
pub fn aggregate_clauses_survive_unparsing(
    call: &AggregateFunction,
    renders_order_by: impl Fn(&str) -> bool,
) -> bool {
    if matches!(call.params.null_treatment, Some(NullTreatment::IgnoreNulls)) {
        return false;
    }
    let name = call.func.name();
    call.params.order_by.is_empty()
        || call.func.supports_within_group_clause()
        || matches!(
            call.func.order_sensitivity(),
            AggregateOrderSensitivity::Insensitive
        )
        || ORDER_INDEPENDENT_AGGREGATES
            .iter()
            .any(|independent| name.eq_ignore_ascii_case(independent))
        || renders_order_by(name)
}

/// Whether the SQL the unparser writes for this window call asks the backend for
/// the answer `DataFusion` would compute, so the call can federate.
///
/// The unparser never renders `IGNORE NULLS` on a window, so `lag`, `lead`,
/// `first_value`, `last_value` and `nth_value` would reach the backend respecting
/// nulls: `lag(v) IGNORE NULLS OVER (ORDER BY id)` came back from `PostgreSQL` as
/// `[NULL, 10, NULL, 30, NULL]` where `DataFusion` answers `[NULL, 10, 10, 30, 30]`.
/// Such a window stays local.
#[must_use]
pub fn window_clauses_survive_unparsing(call: &WindowFunction) -> bool {
    !matches!(call.params.null_treatment, Some(NullTreatment::IgnoreNulls))
}

/// The [`FunctionSupport`] for a backend that evaluates no Spice function and
/// every `DataFusion` built-in except the [`DATAFUSION_CAST_BUILTINS`] and the
/// [`PLAN_INTROSPECTION_BUILTINS`] — the conservative default.
#[must_use]
pub fn function_support() -> FunctionSupport {
    FunctionSupportBuilder::new().build()
}

/// The functions no remote source may be asked to evaluate: every Spice
/// function, every user-registered one, the [`DATAFUSION_CAST_BUILTINS`], and
/// the [`PLAN_INTROSPECTION_BUILTINS`]. Safe to call from per-query filter
/// pushdown paths.
#[must_use]
pub fn deny_spice_specific_functions() -> std::sync::Arc<FunctionSupport> {
    std::sync::Arc::new(FunctionSupportBuilder::new().build())
}

/// As [`deny_spice_specific_functions`], but allowing the functions the target
/// backend evaluates itself. The [`DATAFUSION_CAST_BUILTINS`] and the
/// [`PLAN_INTROSPECTION_BUILTINS`] stay denied.
///
/// `native` is normally that backend's unparser dialect's native-function names,
/// which is how the deny-list becomes backend-aware: a Spice function the
/// dialect rewrites into a real remote function pushes down instead of being
/// denied. User-registered functions are never carved out — no remote source has
/// an equivalent.
#[must_use]
pub fn deny_spice_specific_functions_excluding(native: &[&str]) -> std::sync::Arc<FunctionSupport> {
    std::sync::Arc::new(FunctionSupportBuilder::new().native(native).build())
}

/// Full deny-list as a value — every Spice function, every user-registered one,
/// the [`DATAFUSION_CAST_BUILTINS`] and the [`PLAN_INTROSPECTION_BUILTINS`] — for
/// any SQL connector whose unparser dialect has no Spice-function carve-out. See
/// issue #10703.
#[must_use]
pub fn deny_spice_functions_for_table_providers() -> FunctionSupport {
    FunctionSupportBuilder::new().build()
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::DataType;
    use datafusion::logical_expr::{
        ColumnarValue, Expr, ScalarUDF, Volatility, create_udf, expr::ScalarFunction,
    };
    use datafusion::scalar::ScalarValue;
    use std::sync::Arc;

    /// `USER_FUNCTION_NAMES` is process-global, so every test here uses a name
    /// of its own and drops it again; the assertions never depend on the
    /// registry being empty.
    struct Registered(&'static str);

    impl Registered {
        fn new(name: &'static str) -> Self {
            add_user_function(name);
            Self(name)
        }
    }

    impl Drop for Registered {
        fn drop(&mut self) {
            remove_user_function(self.0);
        }
    }

    fn udf(name: &str, arity: usize) -> Arc<ScalarUDF> {
        Arc::new(create_udf(
            name,
            vec![DataType::Utf8; arity],
            DataType::Utf8,
            Volatility::Immutable,
            Arc::new(|args: &[ColumnarValue]| Ok(args[0].clone())),
        ))
    }

    fn call(name: &str, arity: usize) -> Expr {
        Expr::ScalarFunction(ScalarFunction::new_udf(
            udf(name, arity),
            vec![Expr::Literal(ScalarValue::from("v"), None); arity],
        ))
    }

    /// The name list is a snapshot, so this is the case it already covered.
    #[test]
    fn a_user_function_registered_before_the_support_was_built_is_refused() {
        let _registered = Registered::new("early_user_fn_udfs_api");

        let support = FunctionSupportBuilder::new().build();

        assert!(
            !support.supports(&call("early_user_fn_udfs_api", 1), None),
            "a user function in the deny-list snapshot must not federate"
        );
    }

    /// The case the snapshot cannot answer: every provider is built once, and
    /// registrations keep arriving for the life of the process (#13726).
    #[test]
    fn a_user_function_registered_after_the_support_was_built_is_refused() {
        let support = FunctionSupportBuilder::new().build();
        let _registered = Registered::new("late_user_fn_udfs_api");

        assert!(
            !support.supports(&call("late_user_fn_udfs_api", 1), None),
            "a user function registered after the support was built must not federate"
        );
    }

    /// Dropping a registration must not leave the name refused for ever: the
    /// live read is the point, in both directions.
    #[test]
    fn a_name_that_is_no_longer_a_user_function_federates_again() {
        let support = FunctionSupportBuilder::new().build();
        drop(Registered::new("transient_user_fn_udfs_api"));

        assert!(
            support.supports(&call("transient_user_fn_udfs_api", 1), None),
            "an unregistered name is not a user function and has nothing to refuse it"
        );
    }

    #[test]
    fn a_function_nobody_registered_still_federates() {
        let support = FunctionSupportBuilder::new().build();

        assert!(
            support.supports(&call("some_remote_fn_udfs_api", 1), None),
            "the deny-list must not refuse a name it has no reason to"
        );
    }

    /// Regression test for #14444: each name in [`DATAFUSION_CAST_BUILTINS`] is
    /// the canonical name of a `DataFusion` built-in whose every alias is listed
    /// too — so a rename cannot leave the real function federating — and a call
    /// of it is refused even by a backend that claims the name as native.
    #[test]
    fn a_datafusion_cast_builtin_is_denied_whatever_the_backend_carves_out() {
        let state = util::session_state::session_context().state();
        let support = FunctionSupportBuilder::new()
            .native(DATAFUSION_CAST_BUILTINS)
            .build();

        for name in DATAFUSION_CAST_BUILTINS {
            let builtin = state
                .scalar_functions()
                .get(*name)
                .unwrap_or_else(|| panic!("{name} must be a DataFusion built-in"));
            for alias in builtin.aliases() {
                assert!(
                    DATAFUSION_CAST_BUILTINS.contains(&alias.as_str()),
                    "{name}'s alias {alias} must be denied alongside it"
                );
            }
            let cast = Expr::ScalarFunction(ScalarFunction::new_udf(
                Arc::clone(builtin),
                vec![
                    Expr::Literal(ScalarValue::from(1_i64), None),
                    Expr::Literal(ScalarValue::from("LargeUtf8"), None),
                ],
            ));
            assert!(
                !support.supports(&cast, None),
                "{name} must not federate, whatever the backend carves out"
            );
        }
    }

    /// A built-in that describes the plan is denied by every builder, and a
    /// backend's native carve-out cannot re-admit it (#14334).
    #[test]
    fn a_plan_introspection_builtin_is_denied_whatever_the_backend_carves_out() {
        let support = FunctionSupportBuilder::new()
            .native(PLAN_INTROSPECTION_BUILTINS)
            .build();

        for name in PLAN_INTROSPECTION_BUILTINS {
            assert!(
                !support.supports(&call(name, 1), None),
                "{name} answers about the DataFusion plan, so no remote may be asked to evaluate it"
            );
        }
    }

    /// A backend's own per-call check and the live user-function check are both
    /// consulted, so neither can mask the other. `build` installs the second,
    /// which is why the first has to be supplied through the builder rather
    /// than by `with_scalar_call_support` afterwards.
    #[test]
    fn a_backend_per_call_check_composes_with_the_live_user_function_check() {
        let one_argument_only: ScalarCallSupport = Arc::new(
            // The scope is the backend's to read; this check answers by arity
            // alone, which is what makes it a clean probe of composition.
            |call: &ScalarFunction, _scope: Option<&datafusion::common::DFSchema>| {
                call.args.len() == 1
            },
        );
        let support = FunctionSupportBuilder::new()
            .scalar_call(Arc::clone(&one_argument_only))
            .build();
        let _registered = Registered::new("composed_user_fn_udfs_api");

        assert!(
            support.supports(&call("translatable_fn_udfs_api", 1), None),
            "a call shape the backend declared it can translate must still federate"
        );
        assert!(
            !support.supports(&call("translatable_fn_udfs_api", 2), None),
            "the backend's own per-call check must survive the one `build` installs"
        );
        assert!(
            !support.supports(&call("composed_user_fn_udfs_api", 1), None),
            "a late-registered user function must be refused even in a shape the backend accepts"
        );
    }

    #[test]
    fn is_user_function_reads_the_registry_live() {
        assert!(!is_user_function("probe_user_fn_udfs_api"));
        let registered = Registered::new("probe_user_fn_udfs_api");
        assert!(is_user_function("probe_user_fn_udfs_api"));
        drop(registered);
        assert!(!is_user_function("probe_user_fn_udfs_api"));
    }

    /// `udaf` over `i`, ordered by `j` when `ordered`, ignoring nulls when
    /// `ignore_nulls`.
    fn aggregate(
        udaf: Arc<datafusion::logical_expr::AggregateUDF>,
        ordered: bool,
        ignore_nulls: bool,
    ) -> Expr {
        use datafusion::prelude::col;
        Expr::AggregateFunction(AggregateFunction::new_udf(
            udaf,
            vec![col("i")],
            false,
            None,
            if ordered {
                vec![col("j").sort(false, false)]
            } else {
                vec![]
            },
            ignore_nulls.then_some(NullTreatment::IgnoreNulls),
        ))
    }

    /// `udwf` over `i`, ordered by `j`, ignoring nulls when `ignore_nulls`.
    fn window(udwf: Arc<datafusion::logical_expr::WindowUDF>, ignore_nulls: bool) -> Expr {
        use datafusion::logical_expr::{ExprFunctionExt as _, WindowFunctionDefinition};
        use datafusion::prelude::col;
        let window = Expr::from(WindowFunction::new(
            WindowFunctionDefinition::WindowUDF(udwf),
            vec![col("i")],
        ))
        .order_by(vec![col("j").sort(true, false)]);
        if ignore_nulls {
            window.null_treatment(NullTreatment::IgnoreNulls)
        } else {
            window
        }
        .build()
        .expect("window expression")
    }

    /// The unparser drops `IGNORE NULLS` from every aggregate and window it
    /// writes, so no policy may federate one — not even with a backend check
    /// that admits everything, which composes with the rule rather than
    /// replacing it.
    #[test]
    fn no_policy_federates_ignore_nulls() {
        use datafusion::functions_aggregate::first_last::first_value_udaf;
        use datafusion::functions_window::lead_lag::lag_udwf;
        let admits_everything = FunctionSupportBuilder::new()
            .aggregate_call(Arc::new(|_: &AggregateFunction| true))
            .window_call(Arc::new(|_: &WindowFunction| true))
            .build();
        for support in [FunctionSupportBuilder::new().build(), admits_everything] {
            assert!(
                !support.supports(&window(lag_udwf(), true), None),
                "lag(i) IGNORE NULLS would reach the backend respecting nulls"
            );
            assert!(
                support.supports(&window(lag_udwf(), false), None),
                "lag(i) respecting nulls must keep its pushdown"
            );
            assert!(
                !support.supports(&aggregate(first_value_udaf(), false, true), None),
                "first_value(i) IGNORE NULLS would reach the backend respecting nulls"
            );
            assert!(
                support.supports(&aggregate(first_value_udaf(), false, false), None),
                "first_value(i) respecting nulls must keep its pushdown"
            );
        }
    }

    /// Of every aggregate `DataFusion` registers, called with an `ORDER BY` the
    /// unparser drops, only those whose answer the ordering cannot change
    /// federate — order-insensitive ones, `avg` and `median` — plus those it
    /// writes as `WITHIN GROUP`. `array_agg`, `string_agg`, `first_value`,
    /// `last_value` and `nth_value` would come back in the backend's order.
    #[test]
    fn an_ordered_aggregate_federates_only_where_the_ordering_cannot_be_lost() {
        let support = FunctionSupportBuilder::new().build();
        let mut federated: Vec<String> =
            datafusion::functions_aggregate::all_default_aggregate_functions()
                .into_iter()
                .filter(|udaf| support.supports(&aggregate(Arc::clone(udaf), true, false), None))
                .map(|udaf| udaf.name().to_string())
                .collect();
        federated.sort();
        assert_eq!(
            federated,
            [
                "any_value",
                "approx_percentile_cont",
                "approx_percentile_cont_with_weight",
                "avg",
                "bool_and",
                "bool_or",
                "count",
                "max",
                "median",
                "min",
                "percentile_cont",
                "sum",
            ],
        );
        for udaf in datafusion::functions_aggregate::all_default_aggregate_functions() {
            assert!(
                support.supports(&aggregate(Arc::clone(&udaf), false, false), None),
                "{} without an ORDER BY must keep its pushdown",
                udaf.name()
            );
        }
    }

    /// A backend whose dialect renders an aggregate's `ORDER BY` itself keeps
    /// that aggregate's pushdown, and only that one's; a backend's own check
    /// narrows what federates and cannot widen it.
    #[test]
    fn a_backend_renders_its_own_orderings_and_its_check_only_narrows() {
        use datafusion::functions_aggregate::array_agg::array_agg_udaf;
        use datafusion::functions_aggregate::string_agg::string_agg_udaf;
        use datafusion::functions_aggregate::sum::sum_udaf;
        let support = FunctionSupportBuilder::new()
            .renders_aggregate_order_by(|name| name == "array_agg")
            .aggregate_call(Arc::new(|call: &AggregateFunction| {
                call.func.name() != "sum"
            }))
            .build();
        assert!(
            support.supports(&aggregate(array_agg_udaf(), true, false), None),
            "the dialect renders array_agg's ORDER BY, so it must federate"
        );
        assert!(
            !support.supports(&aggregate(string_agg_udaf(), true, false), None),
            "the dialect renders no string_agg ORDER BY, so it must stay local"
        );
        assert!(
            !support.supports(&aggregate(array_agg_udaf(), true, true), None),
            "rendering the ORDER BY does not render IGNORE NULLS"
        );
        assert!(
            !support.supports(&aggregate(sum_udaf(), false, false), None),
            "the backend's own check must survive the one `build` installs"
        );
    }
}
