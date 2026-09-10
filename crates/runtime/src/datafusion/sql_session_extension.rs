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

use crate::sessions::{RequestedSession, SessionStore, SqlSession};

/// Why a request may not have the session it named.
#[derive(Debug, Snafu)]
pub enum SessionError {
    #[snafu(display(
        "Session '{session_id}' was not found, so the prepared statements it held are gone and \
        this request cannot run in it. It expired after a period of inactivity, or it was never \
        created. Retry without `x-session-id` to use this principal's own session, or start a new \
        one with a Flight SQL handshake. See: https://spiceai.org/docs/api/HTTP/post-sql"
    ))]
    NotFound { session_id: String },

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

/// Resolves the [`SqlSession`] a request runs in.
///
/// Attached to the request context by the HTTP and Flight middlewares, which run
/// *before* authentication and so can only record what the request named. The
/// principal is known later, at the point the session is used, which is why
/// resolution — and the ownership check that goes with it — happens here rather
/// than in the middleware.
#[derive(Clone)]
pub struct SqlSessionExtension {
    store: SessionStore,
    requested: RequestedSession,
}

impl SqlSessionExtension {
    #[must_use]
    pub fn new(store: SessionStore, requested: RequestedSession) -> Self {
        Self { store, requested }
    }

    /// The session this request runs in, or `None` to run against the shared
    /// context.
    ///
    /// There is deliberately no accessor that hands back a session without a
    /// principal to check it against: the session id arrives in a
    /// client-controlled header, so every path that selects one has to prove the
    /// caller owns it.
    ///
    /// # Errors
    ///
    /// Returns [`SessionError`] when the request explicitly named a session that
    /// does not exist or that belongs to another principal.
    pub fn resolve(
        &self,
        principal: Option<&AuthPrincipalRef>,
        base_ctx: &SessionContext,
    ) -> Result<Option<Arc<SqlSession>>, SessionError> {
        // `x-session-id` is a request to use one specific session. Not finding
        // it is an error the caller needs to see: silently running against the
        // shared context instead would surface later as a missing prepared
        // statement, pointing at the query rather than at the expired session.
        if let Some(id) = self.requested.explicit_id.as_deref() {
            let session = self.store.get_issued(id).context(NotFoundSnafu {
                session_id: id.to_string(),
            })?;
            ensure!(
                session.is_owned_by(principal),
                NotOwnedSnafu {
                    session_id: id.to_string()
                }
            );
            return Ok(Some(session));
        }

        // A bearer token only *might* be a session id — most are just API keys —
        // so one that names no session is not an error, it names nothing.
        if let Some(token) = self.requested.bearer_token.as_deref()
            && let Some(session) = self.store.get_issued(token)
        {
            ensure!(
                session.is_owned_by(principal),
                NotOwnedSnafu {
                    session_id: session.id().to_string()
                }
            );
            return Ok(Some(session));
        }

        // A caller that named no session still gets one, derived from its
        // principal and created on first use. This is what makes
        // `PREPARE`/`EXECUTE` span requests without a client having to ask for
        // a session, and it is keyed on the principal so no client can choose
        // which session it lands in.
        if let Some(stable_id) = principal.and_then(|principal| principal.stable_id()) {
            return Ok(Some(self.store.implicit_for(base_ctx, stable_id.as_ref())));
        }

        Ok(None)
    }
}

impl std::fmt::Debug for SqlSessionExtension {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqlSessionExtension")
            .field("requested", &self.requested)
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
    use runtime_auth::AuthPrincipal;
    use spicepod::component::runtime::ApiKey;

    fn principal(key: &str) -> AuthPrincipalRef {
        Arc::new(ApiKey::parse_str(key)) as AuthPrincipalRef
    }

    fn stable_id_of(key: &str) -> String {
        ApiKey::parse_str(key)
            .stable_id()
            .map(std::borrow::Cow::into_owned)
            .expect("an api key principal always has a stable id")
    }

    fn named(explicit: Option<&str>, bearer: Option<&str>) -> RequestedSession {
        RequestedSession {
            explicit_id: explicit.map(str::to_string),
            bearer_token: bearer.map(str::to_string),
        }
    }

    #[test]
    fn an_explicitly_named_session_resolves_for_its_owner() {
        let store = SessionStore::new();
        let base = SessionContext::new();
        let session = store.issue(&base, Some(stable_id_of("a")), None);

        let ext = SqlSessionExtension::new(store, named(Some(session.id()), None));

        let resolved = ext
            .resolve(Some(&principal("a")), &base)
            .expect("the owner may use its own session")
            .expect("the session resolves");
        assert!(Arc::ptr_eq(resolved.context(), session.context()));
    }

    /// Regression: a session id reaching another principal — by a leak, or by
    /// both principals naming the same id — must not carry that principal into
    /// the owner's prepared statements.
    #[test]
    fn an_explicitly_named_session_is_refused_to_another_principal() {
        let store = SessionStore::new();
        let base = SessionContext::new();
        let session = store.issue(&base, Some(stable_id_of("a")), None);

        let ext = SqlSessionExtension::new(store, named(Some(session.id()), None));

        let error = ext
            .resolve(Some(&principal("b")), &base)
            .expect_err("a different principal is refused");
        assert!(matches!(error, SessionError::NotOwned { .. }), "{error}");
    }

    /// An `x-session-id` the store does not hold is reported, not quietly
    /// downgraded to a stateless request.
    #[test]
    fn an_unknown_explicit_session_is_an_error() {
        let store = SessionStore::new();
        let ext = SqlSessionExtension::new(store, named(Some("no-such-session"), None));

        let error = ext
            .resolve(Some(&principal("a")), &SessionContext::new())
            .expect_err("an unknown session id is an error");
        assert!(matches!(error, SessionError::NotFound { .. }), "{error}");
    }

    /// A bearer token is ordinarily an API key. One that names no session must
    /// fall through rather than fail the request, or every authenticated call
    /// made without a session would be refused.
    #[test]
    fn a_bearer_token_that_names_no_session_is_not_an_error() {
        let store = SessionStore::new();
        let ext = SqlSessionExtension::new(store, named(None, Some("an-api-key")));

        let resolved = ext
            .resolve(Some(&principal("an-api-key")), &SessionContext::new())
            .expect("an api key presented as a bearer token names no session");
        assert!(resolved.is_none());
    }

    #[test]
    fn a_bearer_token_that_is_a_session_id_selects_that_session() {
        let store = SessionStore::new();
        let base = SessionContext::new();
        let session = store.issue(&base, Some(stable_id_of("a")), Some("a".to_string()));

        let ext = SqlSessionExtension::new(store, named(None, Some(session.id())));

        let resolved = ext
            .resolve(Some(&principal("a")), &base)
            .expect("the owner may use its own session")
            .expect("the session resolves");
        assert_eq!(resolved.id(), session.id());
    }

    /// A caller that names no session gets one of its own, keyed on its
    /// principal — the same behavior on both protocols, and what lets
    /// `PREPARE`/`EXECUTE` span requests without a client asking for a session.
    #[test]
    fn a_caller_naming_no_session_gets_one_keyed_on_its_principal() {
        let store = SessionStore::new();
        let base = SessionContext::new();
        let ext = SqlSessionExtension::new(store, RequestedSession::default());

        let resolved = ext
            .resolve(Some(&principal("a")), &base)
            .expect("resolving is not an error")
            .expect("an implicit session is created on first use");

        assert!(resolved.is_owned_by(Some(&principal("a"))));
        assert!(
            !resolved.is_owned_by(Some(&principal("b"))),
            "another principal must not land in it"
        );
    }

    /// Without a principal there is nothing to key an implicit session on, so an
    /// unauthenticated caller runs against the shared context rather than
    /// joining a session others could reach.
    #[test]
    fn an_unauthenticated_caller_gets_no_implicit_session() {
        let ext = SqlSessionExtension::new(SessionStore::new(), RequestedSession::default());

        assert!(
            ext.resolve(None, &SessionContext::new())
                .expect("resolving is not an error")
                .is_none()
        );
    }

    /// These messages are the only explanation a caller gets for a session that
    /// is gone or not theirs, so a reword must not quietly drop the session it
    /// names, what the caller should do, or where to read more.
    #[test]
    fn a_session_error_names_the_session_the_fix_and_the_docs() {
        for error in [
            SessionError::NotFound {
                session_id: "sess-1".to_string(),
            },
            SessionError::NotOwned {
                session_id: "sess-1".to_string(),
            },
        ] {
            let message = error.to_string();
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

        assert!(
            SessionError::NotFound {
                session_id: "s".to_string(),
            }
            .to_string()
            .contains("x-session-id"),
            "a missing session tells the caller how to get a working one"
        );
        assert!(
            SessionError::NotOwned {
                session_id: "s".to_string(),
            }
            .to_string()
            .contains("credentials this request presents"),
            "a foreign session tells the caller which credentials to use"
        );
    }

    #[test]
    fn a_session_error_survives_a_round_trip_through_datafusion() {
        let error = SessionError::NotFound {
            session_id: "abc".to_string(),
        };
        let message = error.to_string();
        let carried = error.into_datafusion();

        let recovered =
            SessionError::from_datafusion(&carried).expect("the session error is recoverable");
        assert!(matches!(recovered, SessionError::NotFound { .. }));
        assert_eq!(recovered.to_string(), message);

        assert!(
            SessionError::from_datafusion(&DataFusionError::Execution("other".to_string()))
                .is_none()
        );
    }
}
