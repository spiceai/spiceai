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

//! SQL sessions shared by the HTTP and Flight SQL endpoints.
//!
//! A session is a [`SessionContext`] the runtime keeps alive across requests so
//! that `PREPARE` / `EXECUTE` / `DEALLOCATE` work: `DataFusion` stores prepared
//! plans in `SessionState`, reachable only through the context that owns them,
//! so the same context object has to serve every request in the session.
//!
//! # Session ids are issued, never accepted
//!
//! Only [`SessionStore::issue`] mints an id, and it always records the
//! principal that asked for it. A request *names* a session — with
//! `x-session-id`, or with an `Authorization` bearer token that happens to be a
//! session id — and naming an id the store does not hold creates nothing. This
//! is what keeps two principals that pick the same id from sharing a context,
//! and what keeps a leaked id from being usable by anyone but its owner (see
//! [`SqlSession::is_owned_by`]).
//!
//! # Interchangeable across protocols
//!
//! One store serves both endpoints, so an id issued by a Flight SQL handshake
//! works on `POST /v1/sql` and one issued by `POST /v1/sessions` works over
//! Flight — including as the bearer token, which both endpoints resolve back to
//! the credential the session was issued against (see
//! [`SessionStore::bearer_credential`]).

use std::sync::Arc;
use std::time::Duration;

use datafusion::prelude::SessionContext;
use http::{HeaderMap, HeaderName};
use moka::sync::Cache;
use runtime_auth::AuthPrincipalRef;
use util::session_state::builder_from_existing;
use uuid::Uuid;

/// How long a session survives without being used.
const SESSION_TTL: Duration = Duration::from_hours(1);

/// How many sessions the store holds before evicting the least recently used.
/// Each holds a `DataFusion` context, so this is what bounds the memory
/// sessions can occupy.
const MAX_SESSIONS: u64 = 10_000;

/// Header a request names its session with. A `HeaderName` rather than a `&str`
/// so the CORS allowlists in `http::routes` and the header lookups here share
/// one definition, checked at compile time.
pub const SESSION_ID_HEADER: HeaderName = HeaderName::from_static("x-session-id");

/// Key prefix for implicit sessions, which are addressed by the principal that
/// owns them rather than by an issued id. It is not a valid issued id (those are
/// UUIDs), so a request cannot name one.
const IMPLICIT_KEY_PREFIX: &str = "implicit:";

/// How a session came to exist.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SessionKind {
    /// The client asked for a session and was given an id — a Flight SQL
    /// handshake, or `POST /v1/sessions`. The id is what the client presents on
    /// later requests, so it is a CSPRNG-random `UUIDv4`: it must be
    /// unpredictable, and (unlike the time-ordered `UUIDv7`) must not leak when
    /// the session was created.
    Issued,

    /// Derived from the authenticated principal for a client that never asked
    /// for a session. This is what lets a Flight SQL client that skips the
    /// handshake — `spice sql --api-key …` among them — still run
    /// `PREPARE`/`EXECUTE` across requests. Its key is not an id any request can
    /// name, so it is reachable only by the principal it belongs to.
    Implicit,
}

/// A live `DataFusion` session and the principal it belongs to.
pub struct SqlSession {
    id: String,
    kind: SessionKind,
    ctx: Arc<SessionContext>,
    /// `stable_id()` of the principal that created the session, or `None` when
    /// it was created without authentication. An unowned session is usable by
    /// anyone, which is the right behavior on a runtime with no `runtime.auth`
    /// configured — there is no identity to bind it to.
    owner_stable_id: Option<String>,
    /// The API key the session stands in for when its id is presented as a
    /// bearer token. `None` when the creator authenticated by some other means
    /// (a client certificate) or not at all — such a session is still usable
    /// via `x-session-id`, it just is not a credential.
    bearer_api_key: Option<String>,
}

impl SqlSession {
    #[must_use]
    pub fn id(&self) -> &str {
        &self.id
    }

    #[must_use]
    pub fn context(&self) -> &Arc<SessionContext> {
        &self.ctx
    }

    /// Whether `principal` may use this session.
    ///
    /// An unowned session is open to everyone. An owned one is usable only by
    /// the principal that created it — this is the check that stops a leaked
    /// session id from carrying another principal's prepared statements, and it
    /// has to be applied at every point a session is selected, because the id is
    /// read from a client-controlled header before authentication runs.
    #[must_use]
    pub fn is_owned_by(&self, principal: Option<&AuthPrincipalRef>) -> bool {
        let Some(owner) = self.owner_stable_id.as_deref() else {
            return true;
        };
        principal
            .and_then(|principal| principal.stable_id())
            .is_some_and(|current| current.as_ref() == owner)
    }
}

impl std::fmt::Debug for SqlSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqlSession")
            .field("id", &self.id)
            .field("kind", &self.kind)
            .field("owner_stable_id", &self.owner_stable_id)
            .finish_non_exhaustive()
    }
}

/// Maps session ids to the [`SqlSession`] they name, expiring them after a
/// period of inactivity and evicting the least recently used past a capacity.
#[derive(Clone)]
pub struct SessionStore {
    sessions: Cache<String, Arc<SqlSession>>,
    ttl: Duration,
}

impl std::fmt::Debug for SessionStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SessionStore")
            .field("session_count", &self.sessions.entry_count())
            .field("ttl", &self.ttl)
            .finish_non_exhaustive()
    }
}

impl Default for SessionStore {
    fn default() -> Self {
        Self::new()
    }
}

impl SessionStore {
    #[must_use]
    pub fn new() -> Self {
        Self {
            sessions: Cache::builder()
                .max_capacity(MAX_SESSIONS)
                .time_to_idle(SESSION_TTL)
                .build(),
            ttl: SESSION_TTL,
        }
    }

    /// How long a session survives without being used. Reported to clients so
    /// they know when to expect one to lapse.
    #[must_use]
    pub fn ttl(&self) -> Duration {
        self.ttl
    }

    /// Creates a session and returns it. The id is the caller's to hand back to
    /// the client.
    ///
    /// `owner_stable_id` binds the session to a principal; pass `None` only when
    /// the request that asked for it was unauthenticated.
    ///
    /// The context is built from `base_ctx`'s state with a fresh `session_id`,
    /// which is what isolates its prepared plans from every other session. The
    /// catalog list is shared with `base_ctx` rather than copied, so datasets
    /// registered after the session is created remain visible inside it;
    /// configuration options and the function registries are snapshots.
    #[must_use]
    pub fn issue(
        &self,
        base_ctx: &SessionContext,
        owner_stable_id: Option<String>,
        bearer_api_key: Option<String>,
    ) -> Arc<SqlSession> {
        let id = Uuid::new_v4().hyphenated().to_string();
        let session = Arc::new(SqlSession {
            ctx: Self::context_from(base_ctx, &id),
            kind: SessionKind::Issued,
            id: id.clone(),
            owner_stable_id,
            bearer_api_key,
        });
        self.insert(id, &session);
        session
    }

    /// The session named by an id a request supplied, if the store holds one.
    ///
    /// Implicit sessions are deliberately unreachable here: their key is derived
    /// from a principal, not issued to a client, so treating a request-supplied
    /// string as one would let a caller address a session by guessing a
    /// principal id rather than by holding a credential.
    #[must_use]
    pub fn get_issued(&self, id: &str) -> Option<Arc<SqlSession>> {
        self.sessions
            .get(id)
            .filter(|session| session.kind == SessionKind::Issued)
    }

    /// The session a principal gets when it never asked for one, created on
    /// first use.
    ///
    /// Keyed by the principal's stable id, so two principals can never land in
    /// the same session and a client cannot choose which one it reaches.
    #[must_use]
    pub fn implicit_for(
        &self,
        base_ctx: &SessionContext,
        owner_stable_id: &str,
    ) -> Arc<SqlSession> {
        let key = format!("{IMPLICIT_KEY_PREFIX}{owner_stable_id}");
        if let Some(session) = self.sessions.get(&key) {
            return session;
        }

        // `get_with` collapses a concurrent race onto one initializer, so two
        // requests from the same principal arriving together share a context
        // rather than each building one and one of them losing its prepared
        // statements to the other's insert.
        let session = self.sessions.get_with(key.clone(), || {
            Arc::new(SqlSession {
                ctx: Self::context_from(base_ctx, &key),
                kind: SessionKind::Implicit,
                id: key.clone(),
                owner_stable_id: Some(owner_stable_id.to_string()),
                bearer_api_key: None,
            })
        });
        self.sessions.run_pending_tasks();
        session
    }

    /// The API key an issued session id stands in for, so a bearer token that is
    /// a session id authenticates as the principal the session was issued to.
    ///
    /// Returns `None` for anything that is not an issued session carrying a key,
    /// leaving the caller to fall through to its ordinary credential check.
    #[must_use]
    pub fn bearer_credential(&self, token: &str) -> Option<String> {
        self.get_issued(token)
            .and_then(|session| session.bearer_api_key.clone())
    }

    /// Drops a session. Returns whether one was there to drop.
    #[must_use]
    pub fn remove(&self, id: &str) -> bool {
        let removed = self.sessions.remove(id).is_some();
        self.sessions.run_pending_tasks();
        removed
    }

    /// The number of live sessions, implicit ones included.
    #[must_use]
    pub fn count(&self) -> usize {
        usize::try_from(self.sessions.entry_count()).unwrap_or(usize::MAX)
    }

    fn insert(&self, id: String, session: &Arc<SqlSession>) {
        self.sessions.insert(id, Arc::clone(session));
        self.sessions.run_pending_tasks();
    }

    /// A context sharing `base_ctx`'s catalogs, functions and custom rules but
    /// carrying its own `session_id`, which is what scopes prepared plans to
    /// this session.
    fn context_from(base_ctx: &SessionContext, session_id: &str) -> Arc<SessionContext> {
        let state = builder_from_existing(&base_ctx.state())
            .with_session_id(session_id.to_string())
            .build();
        Arc::new(SessionContext::new_with_state(state))
    }
}

/// The session id a request names, if any: `x-session-id` when the client set
/// it, otherwise the `Authorization` bearer token.
///
/// The two are not equivalent to the caller. `x-session-id` is a request to use
/// a specific session and is an error when the store does not hold it; a bearer
/// token is a credential that only *might* be a session id, so failing to
/// resolve one is ordinary. [`RequestedSession`] keeps them apart for that
/// reason.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RequestedSession {
    /// Value of the `x-session-id` header.
    pub explicit_id: Option<String>,
    /// The `Authorization` bearer token, which may or may not name a session.
    pub bearer_token: Option<String>,
}

impl RequestedSession {
    /// Reads the session a request names out of its headers.
    ///
    /// gRPC metadata is HTTP/2 headers, so the Flight middleware reads the same
    /// map this does and both endpoints agree on how a session is named.
    #[must_use]
    pub fn from_headers(headers: &HeaderMap) -> Self {
        // An empty header names nothing. A proxy that injects a blank
        // `x-session-id` should leave the request stateless, not fail it.
        let named = |value: &str| {
            let value = value.trim();
            (!value.is_empty()).then(|| value.to_string())
        };

        Self {
            explicit_id: headers
                .get(SESSION_ID_HEADER)
                .and_then(|value| value.to_str().ok())
                .and_then(named),
            bearer_token: headers
                .get(http::header::AUTHORIZATION)
                .and_then(|value| value.to_str().ok())
                .and_then(bearer_token)
                .and_then(named),
        }
    }
}

/// The token out of an `Authorization: Bearer <token>` header value, matching
/// the scheme name case-insensitively as RFC 7235 requires.
#[must_use]
pub fn bearer_token(header_value: &str) -> Option<&str> {
    let (scheme, token) = header_value.split_once(' ')?;
    scheme
        .eq_ignore_ascii_case("bearer")
        .then(|| token.trim_start())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, Int64Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};
    // `stable_id` is called here on a concrete `ApiKey` rather than through the
    // `AuthPrincipalRef` trait object the rest of this module uses, so the trait
    // has to be in scope.
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

    #[test]
    fn an_issued_session_is_returned_by_its_id() {
        let store = SessionStore::new();
        let base = SessionContext::new();

        let session = store.issue(&base, Some(stable_id_of("k")), Some("k".to_string()));
        let found = store
            .get_issued(session.id())
            .expect("the issued session is in the store");

        assert!(Arc::ptr_eq(session.context(), found.context()));
        assert_eq!(store.count(), 1);
    }

    /// Session ids are handed to clients and accepted as bearer tokens, so they
    /// must be CSPRNG-random (`UUIDv4`) — never the time-ordered `UUIDv7`, whose
    /// value leaks its creation time and is partly predictable.
    #[test]
    fn an_issued_id_is_a_random_uuid() {
        let store = SessionStore::new();
        let session = store.issue(&SessionContext::new(), None, None);

        let uuid = Uuid::parse_str(session.id()).expect("the id is a UUID");
        assert_eq!(
            uuid.get_version(),
            Some(uuid::Version::Random),
            "session id must be a UUIDv4, got {}",
            session.id()
        );
    }

    #[test]
    fn a_removed_session_is_gone() {
        let store = SessionStore::new();
        let session = store.issue(&SessionContext::new(), None, None);

        assert!(store.remove(session.id()));
        assert!(store.get_issued(session.id()).is_none());
        assert!(
            !store.remove(session.id()),
            "removing a session twice reports it was already gone"
        );
    }

    /// Naming an id the store never issued resolves to nothing. It must not
    /// create a session: a store that mints one for any string a client sends
    /// lets two principals that pick the same id share a context.
    #[test]
    fn naming_an_unknown_id_creates_nothing() {
        let store = SessionStore::new();

        assert!(store.get_issued("shared-guessable-id").is_none());
        assert_eq!(store.count(), 0);
    }

    #[test]
    fn a_session_is_usable_only_by_its_owner() {
        let store = SessionStore::new();
        let owned = store.issue(&SessionContext::new(), Some(stable_id_of("a")), None);

        assert!(owned.is_owned_by(Some(&principal("a"))));
        assert!(
            !owned.is_owned_by(Some(&principal("b"))),
            "another principal must not reach a session it does not own"
        );
        assert!(
            !owned.is_owned_by(None),
            "an unauthenticated caller must not reach an owned session"
        );
    }

    /// A runtime with no `runtime.auth` has no identity to bind a session to,
    /// so its sessions stay open — the behavior an unauthenticated deployment
    /// already has.
    #[test]
    fn an_unowned_session_is_open_to_everyone() {
        let store = SessionStore::new();
        let unowned = store.issue(&SessionContext::new(), None, None);

        assert!(unowned.is_owned_by(None));
        assert!(unowned.is_owned_by(Some(&principal("anyone"))));
    }

    /// A session shares the base context's catalog list rather than
    /// snapshotting it, so a dataset registered after the session was created is
    /// visible inside it. Were it a snapshot, a long-lived session would answer
    /// from a catalog that no longer matches the runtime's.
    #[tokio::test]
    async fn a_session_sees_a_table_registered_after_it_was_created() {
        let base = SessionContext::new();
        let store = SessionStore::new();
        let session = store.issue(&base, None, None);

        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)])),
            vec![Arc::new(Int64Array::from(vec![7])) as ArrayRef],
        )
        .expect("the test batch is well-formed");
        base.register_batch("registered_late", batch)
            .expect("the base context accepts the table");

        let rows = session
            .context()
            .sql("SELECT n FROM registered_late")
            .await
            .expect("the session resolves a table registered after it was created")
            .collect()
            .await
            .expect("the query runs");

        let total: usize = rows.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total, 1);
    }

    #[test]
    fn an_implicit_session_is_stable_per_principal() {
        let store = SessionStore::new();
        let base = SessionContext::new();

        let first = store.implicit_for(&base, &stable_id_of("a"));
        let again = store.implicit_for(&base, &stable_id_of("a"));
        let other = store.implicit_for(&base, &stable_id_of("b"));

        assert!(
            Arc::ptr_eq(first.context(), again.context()),
            "the same principal comes back to the same session"
        );
        assert!(
            !Arc::ptr_eq(first.context(), other.context()),
            "two principals never share an implicit session"
        );
    }

    /// An implicit session's key is derived from a principal, so a request that
    /// guesses the key must not reach it — only the owning principal does, and
    /// it gets there without naming anything.
    #[test]
    fn an_implicit_session_cannot_be_named_by_a_request() {
        let store = SessionStore::new();
        let owner = stable_id_of("a");
        let implicit = store.implicit_for(&SessionContext::new(), &owner);

        assert!(store.get_issued(implicit.id()).is_none());
        assert!(
            store
                .get_issued(&format!("{IMPLICIT_KEY_PREFIX}{owner}"))
                .is_none()
        );
    }

    /// The id of an issued session authenticates as the key it was issued
    /// against. An implicit session's key must not, and neither must a session
    /// created without one.
    #[test]
    fn only_an_issued_session_is_a_bearer_credential() {
        let store = SessionStore::new();
        let base = SessionContext::new();

        let issued = store.issue(&base, Some(stable_id_of("k")), Some("k".to_string()));
        assert_eq!(store.bearer_credential(issued.id()), Some("k".to_string()));

        let no_key = store.issue(&base, Some(stable_id_of("m")), None);
        assert_eq!(store.bearer_credential(no_key.id()), None);

        let implicit = store.implicit_for(&base, &stable_id_of("k"));
        assert_eq!(store.bearer_credential(implicit.id()), None);

        assert_eq!(store.bearer_credential("not-a-session"), None);
    }

    #[test]
    fn a_request_names_the_explicit_header_and_the_bearer_token() {
        let mut headers = HeaderMap::new();
        assert_eq!(
            RequestedSession::from_headers(&headers),
            RequestedSession::default()
        );

        headers.insert(
            http::header::AUTHORIZATION,
            "Bearer tok".parse().expect("a valid header value"),
        );
        let named = RequestedSession::from_headers(&headers);
        assert_eq!(named.explicit_id, None);
        assert_eq!(named.bearer_token.as_deref(), Some("tok"));

        headers.insert(
            SESSION_ID_HEADER,
            "sess".parse().expect("a valid header value"),
        );
        let named = RequestedSession::from_headers(&headers);
        assert_eq!(named.explicit_id.as_deref(), Some("sess"));
        assert_eq!(named.bearer_token.as_deref(), Some("tok"));
    }

    /// A blank header names nothing, so the request stays stateless instead of
    /// failing on a session id of `''`.
    #[test]
    fn a_blank_header_names_no_session() {
        let mut headers = HeaderMap::new();
        headers.insert(
            SESSION_ID_HEADER,
            "   ".parse().expect("a valid header value"),
        );
        headers.insert(
            http::header::AUTHORIZATION,
            "Bearer ".parse().expect("a valid header value"),
        );

        assert_eq!(
            RequestedSession::from_headers(&headers),
            RequestedSession::default()
        );
    }

    #[test]
    fn the_bearer_scheme_is_matched_case_insensitively() {
        assert_eq!(bearer_token("Bearer abc"), Some("abc"));
        assert_eq!(bearer_token("bearer abc"), Some("abc"));
        assert_eq!(bearer_token("BEARER abc"), Some("abc"));
        assert_eq!(bearer_token("Basic abc"), None);
        assert_eq!(
            bearer_token("abc"),
            None,
            "a token with no scheme is not a bearer token"
        );
    }
}
