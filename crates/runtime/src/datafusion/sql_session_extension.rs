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

//! Request-context extension that resolves which SQL session a request runs in.

use std::sync::Arc;

use datafusion::error::DataFusionError;
use datafusion::prelude::SessionContext;
use runtime_auth::AuthPrincipalRef;
use runtime_request_context::Extension;
use snafu::prelude::*;

use crate::sessions::SqlSession;

/// Why a request may not have the session it named.
#[derive(Debug, Snafu)]
pub enum SessionError {
    #[snafu(display(
        "Session '{session_id}' belongs to a different principal, so this request cannot run in \
        it. Use a session created with the credentials this request presents. \
        See: https://spiceai.org/docs/api/HTTP/post-sql"
    ))]
    NotOwned { session_id: String },
}

impl SessionError {
    /// Wraps the error so it can travel through a `DataFusionError` and be
    /// recovered by [`SessionError::from_datafusion`] at the protocol boundary,
    /// which is what lets Flight and HTTP each answer with their own status
    /// code rather than a generic failure.
    #[must_use]
    pub fn into_datafusion(self) -> DataFusionError {
        DataFusionError::External(Box::new(self))
    }

    /// Recovers a session error carried inside a `DataFusionError`.
    #[must_use]
    pub fn from_datafusion(error: &DataFusionError) -> Option<&Self> {
        match error {
            DataFusionError::External(inner) => inner.downcast_ref::<Self>(),
            _ => None,
        }
    }
}

/// The SQL session a request runs in.
///
/// The middlewares settle the id before authentication and put it here; this is
/// where the context behind it is reached, because only here is the principal
/// known and only here is it clear whether the request needs a context at all.
#[derive(Clone)]
pub struct SqlSessionExtension {
    session: Arc<SqlSession>,
}

impl SqlSessionExtension {
    #[must_use]
    pub fn new(session: Arc<SqlSession>) -> Self {
        Self { session }
    }

    /// The session's id, which the response hands back to the client.
    #[must_use]
    pub fn id(&self) -> &str {
        self.session.id()
    }

    /// The context this session already has, or `None` if nothing has needed
    /// one — in which case the request runs against the runtime's shared
    /// context and builds nothing.
    ///
    /// # Errors
    ///
    /// Returns [`SessionError`] when the session belongs to another principal.
    pub fn existing(
        &self,
        principal: Option<&AuthPrincipalRef>,
    ) -> Result<Option<Arc<SessionContext>>, SessionError> {
        let Some(ctx) = self.session.context() else {
            return Ok(None);
        };
        self.ensure_owned(principal)?;
        Ok(Some(Arc::clone(ctx)))
    }

    /// The context, building it if this is the first statement in the session
    /// to need one.
    ///
    /// Only statements that carry state between requests — `PREPARE`, `SET`,
    /// and the `EXECUTE`/`DEALLOCATE` that read it back — reach this. An
    /// ordinary query never does, so a caller that runs only queries never
    /// costs a context.
    ///
    /// # Errors
    ///
    /// Returns [`SessionError`] when the session belongs to another principal.
    pub fn activate(
        &self,
        principal: Option<&AuthPrincipalRef>,
        base_ctx: &SessionContext,
    ) -> Result<Arc<SessionContext>, SessionError> {
        // Checked before activating as well as after: an unactivated session is
        // unowned and would otherwise be claimed by whoever asked first, which
        // is the same check being skipped.
        self.ensure_owned(principal)?;
        let owner = principal.and_then(|principal| principal.stable_id());
        let ctx = Arc::clone(self.session.activate(base_ctx, owner.as_deref()));
        self.ensure_owned(principal)?;
        Ok(ctx)
    }

    fn ensure_owned(&self, principal: Option<&AuthPrincipalRef>) -> Result<(), SessionError> {
        ensure!(
            self.session.is_owned_by(principal),
            NotOwnedSnafu {
                session_id: self.session.id().to_string()
            }
        );
        Ok(())
    }
}

impl std::fmt::Debug for SqlSessionExtension {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqlSessionExtension")
            .field("session", &self.session)
            .finish_non_exhaustive()
    }
}

impl Extension for SqlSessionExtension {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sessions::SessionStore;
    use runtime_auth::AuthPrincipal;
    use spicepod::component::runtime::ApiKey;

    fn principal(key: &str) -> AuthPrincipalRef {
        Arc::new(ApiKey::parse_str(key)) as AuthPrincipalRef
    }

    /// A request that runs only ordinary queries never builds a context: the
    /// session is an id and nothing more until a statement needs state.
    #[test]
    fn a_session_has_no_context_until_something_needs_one() {
        let store = SessionStore::new();
        let ext = SqlSessionExtension::new(store.mint());

        assert!(
            ext.existing(Some(&principal("a")))
                .expect("an unactivated session is not an error")
                .is_none()
        );
    }

    /// The first statement that needs state builds the context, and later
    /// requests naming the same id come back to it.
    #[test]
    fn activating_builds_the_context_once() {
        let store = SessionStore::new();
        let base = SessionContext::new();
        let session = store.mint();

        let first = SqlSessionExtension::new(Arc::clone(&session))
            .activate(Some(&principal("a")), &base)
            .expect("the first statement builds it");
        let again = SqlSessionExtension::new(store.open(session.id()))
            .activate(Some(&principal("a")), &base)
            .expect("a later request comes back to it");

        assert!(Arc::ptr_eq(&first, &again));
        assert!(Arc::ptr_eq(
            &first,
            &SqlSessionExtension::new(store.open(session.id()))
                .existing(Some(&principal("a")))
                .expect("still owned by the same principal")
                .expect("and now has a context")
        ));
    }

    /// Regression: the id is client-supplied, so a second principal naming the
    /// same one must not reach the first's prepared statements.
    #[test]
    fn a_second_principal_naming_the_same_id_is_refused() {
        let store = SessionStore::new();
        let base = SessionContext::new();
        let session = store.mint();

        SqlSessionExtension::new(Arc::clone(&session))
            .activate(Some(&principal("a")), &base)
            .expect("the first principal activates it");

        let ext = SqlSessionExtension::new(store.open(session.id()));
        assert!(matches!(
            ext.activate(Some(&principal("b")), &base)
                .expect_err("a second principal is refused"),
            SessionError::NotOwned { .. }
        ));
        assert!(matches!(
            ext.existing(Some(&principal("b")))
                .expect_err("and cannot read it either"),
            SessionError::NotOwned { .. }
        ));
    }

    /// A runtime with no `runtime.auth` records no owner, so the session stays
    /// open — there are no identities to keep apart.
    #[test]
    fn an_unauthenticated_session_is_open() {
        let store = SessionStore::new();
        let base = SessionContext::new();
        let session = store.mint();

        SqlSessionExtension::new(Arc::clone(&session))
            .activate(None, &base)
            .expect("an unauthenticated caller may activate it");

        assert!(
            SqlSessionExtension::new(store.open(session.id()))
                .existing(Some(&principal("anyone")))
                .expect("an unowned session is open")
                .is_some()
        );
    }

    /// These messages are the only explanation a caller gets for a session that
    /// is not theirs, so a reword must not drop the session it names, what to
    /// do, or where to read more.
    #[test]
    fn a_session_error_names_the_session_the_fix_and_the_docs() {
        let message = SessionError::NotOwned {
            session_id: "sess-1".to_string(),
        }
        .to_string();

        assert!(message.contains("'sess-1'"), "names the session: {message}");
        assert!(
            message.contains("https://spiceai.org/docs/"),
            "links the docs: {message}"
        );
        assert!(
            !message.contains('\n'),
            "stays on one line so it is greppable: {message}"
        );
    }

    #[test]
    fn a_session_error_survives_a_round_trip_through_datafusion() {
        let error = SessionError::NotOwned {
            session_id: "abc".to_string(),
        };
        let message = error.to_string();
        let carried = error.into_datafusion();

        let recovered =
            SessionError::from_datafusion(&carried).expect("the session error is recoverable");
        assert_eq!(recovered.to_string(), message);
        assert!(
            SessionError::from_datafusion(&DataFusionError::Execution("other".to_string()))
                .is_none()
        );
    }
}
