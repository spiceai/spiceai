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
//! every function in it. Two independent sets of names are unsafe to push down,
//! and they have opposite defaults:
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
//!
//! Set 1 lives here because Spice owns it: every Spice function registers its
//! name at its definition site with [`register_spice_function!`], collected into
//! [`SPICE_FUNCTION_REGISTRATIONS`] at link time. That is what keeps the
//! deny-list from drifting as UDFs are added — there is no separate list to
//! maintain, and no window during startup where the set is incomplete.

use std::collections::HashSet;
use std::sync::LazyLock;

use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::prelude::SessionContext;
use datafusion_table_providers::util::supported_functions::{
    FunctionRestriction, FunctionSupport, ScalarCallSupport,
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
        let mut ctx = SessionContext::new();
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
/// and nothing else — correct for a source whose dialect rewrites none of them.
#[derive(Default)]
pub struct FunctionSupportBuilder<'a> {
    native: &'a [&'a str],
    deny_also: Vec<String>,
    scalar_call: Option<ScalarCallSupport>,
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

    /// The denied scalar-function names, in the order the deny-list is built:
    /// Spice functions minus the native carve-out, then user functions, then
    /// any backend-specific additions.
    #[must_use]
    pub fn denied_names(self) -> Vec<String> {
        let spice = excluding_native(spice_function_names(), self.native);
        let user = user_function_names();
        let mut denied = Vec::with_capacity(spice.len() + user.len() + self.deny_also.len());
        denied.extend(spice);
        denied.extend(user);
        denied.extend(self.deny_also);
        denied
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
    #[must_use]
    pub fn build(mut self) -> FunctionSupport {
        let backend_call = self.scalar_call.take();
        FunctionSupport::new(
            Some(FunctionRestriction::Deny(self.denied_names())),
            None,
            None,
        )
        .with_scalar_call_support(std::sync::Arc::new(move |call: &ScalarFunction| {
            !is_user_function(call.func.name())
                && backend_call.as_ref().is_none_or(|supports| supports(call))
        }))
    }
}

/// The [`FunctionSupport`] for a backend that evaluates no Spice function and
/// every `DataFusion` built-in — the conservative default.
#[must_use]
pub fn function_support() -> FunctionSupport {
    FunctionSupportBuilder::new().build()
}

/// The functions no remote source may be asked to evaluate: every Spice
/// function plus every user-registered one. Safe to call from per-query filter
/// pushdown paths.
#[must_use]
pub fn deny_spice_specific_functions() -> std::sync::Arc<FunctionSupport> {
    std::sync::Arc::new(FunctionSupportBuilder::new().build())
}

/// As [`deny_spice_specific_functions`], but allowing the functions the target
/// backend evaluates itself.
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

/// Full deny-list as a value, for any SQL connector whose unparser dialect has
/// no Spice-function carve-out. See issue #10703.
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
            !support.supports(&call("early_user_fn_udfs_api", 1)),
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
            !support.supports(&call("late_user_fn_udfs_api", 1)),
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
            support.supports(&call("transient_user_fn_udfs_api", 1)),
            "an unregistered name is not a user function and has nothing to refuse it"
        );
    }

    #[test]
    fn a_function_nobody_registered_still_federates() {
        let support = FunctionSupportBuilder::new().build();

        assert!(
            support.supports(&call("some_remote_fn_udfs_api", 1)),
            "the deny-list must not refuse a name it has no reason to"
        );
    }

    /// A backend's own per-call check and the live user-function check are both
    /// consulted, so neither can mask the other. `build` installs the second,
    /// which is why the first has to be supplied through the builder rather
    /// than by `with_scalar_call_support` afterwards.
    #[test]
    fn a_backend_per_call_check_composes_with_the_live_user_function_check() {
        let one_argument_only: ScalarCallSupport =
            Arc::new(|call: &ScalarFunction| call.args.len() == 1);
        let support = FunctionSupportBuilder::new()
            .scalar_call(Arc::clone(&one_argument_only))
            .build();
        let _registered = Registered::new("composed_user_fn_udfs_api");

        assert!(
            support.supports(&call("translatable_fn_udfs_api", 1)),
            "a call shape the backend declared it can translate must still federate"
        );
        assert!(
            !support.supports(&call("translatable_fn_udfs_api", 2)),
            "the backend's own per-call check must survive the one `build` installs"
        );
        assert!(
            !support.supports(&call("composed_user_fn_udfs_api", 1)),
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
}
