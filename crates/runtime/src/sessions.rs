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
//! that `PREPARE` / `SET` / `EXECUTE` work: `DataFusion` stores prepared plans
//! and session settings in `SessionState`, reachable only through the context
//! that owns them, so the same context object has to serve every request in the
//! session.
//!
//! # Every request has a session id; few requests have a context
//!
//! Each request names its session with `x-session-id`, a `session-id` cookie,
//! or a bearer token that is one. A request naming none is given a freshly
//! minted id, returned to it in both the header and the cookie, so the next
//! request can come back to the same session.
//!
//! Minting an id registers the id and nothing else. The [`SessionContext`] —
//! the expensive part, a clone of the runtime's session state — is built on
//! first use by [`SqlSession::activate`], which only the statements that need
//! session state reach. A caller that only ever runs ordinary queries therefore
//! pays for an id and never for a context.
//!
//! # Ownership
//!
//! The id arrives in a client-controlled header, so it cannot by itself say who
//! the caller is. The principal is recorded when the context is built, and
//! every later use is checked against it ([`SqlSession::is_owned_by`]); that is
//! what stops a second principal naming the same id from reaching the first's
//! prepared statements. A session activated without authentication records no
//! owner and stays open, which is correct on a runtime with no `runtime.auth`
//! to distinguish callers.

use std::sync::{Arc, OnceLock};
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

/// Cookie a request names its session with, and which the runtime sets on the
/// response so a browser or any cookie-keeping client comes back to the same
/// session without having to read a header.
pub const SESSION_ID_COOKIE: &str = "session-id";

/// What a session holds once something has needed it: the `DataFusion` context,
/// and the principal that caused it to be built.
struct Active {
    ctx: Arc<SessionContext>,
    /// `stable_id()` of the principal that activated the session, or `None`
    /// when it was activated without authentication.
    owner_stable_id: Option<String>,
}

/// A session id, and the `DataFusion` context behind it once one is needed.
pub struct SqlSession {
    id: String,
    /// Built on first use rather than when the id is minted: most requests
    /// never run a statement that needs session state, and a context is a clone
    /// of the runtime's whole session state.
    active: OnceLock<Active>,
    /// The API key the session stands in for when its id is presented as a
    /// bearer token. `None` unless the session was issued against a credential
    /// — a Flight SQL handshake — so a minted id is never itself a credential.
    bearer_api_key: Option<String>,
}

impl SqlSession {
    #[must_use]
    pub fn id(&self) -> &str {
        &self.id
    }

    /// The context, if something has already needed one.
    #[must_use]
    pub fn context(&self) -> Option<&Arc<SessionContext>> {
        self.active.get().map(|active| &active.ctx)
    }

    /// The context, building it against `base_ctx` if this is the first
    /// statement to need one and recording `owner_stable_id` as its owner.
    ///
    /// The context is built from `base_ctx`'s state with this session's id as
    /// the `session_id`, which is what isolates its prepared plans from every
    /// other session. The catalog list is shared with `base_ctx` rather than
    /// copied, so datasets registered later remain visible inside it;
    /// configuration options and the function registries are snapshots.
    #[must_use]
    pub fn activate(
        &self,
        base_ctx: &SessionContext,
        owner_stable_id: Option<&str>,
    ) -> &Arc<SessionContext> {
        &self
            .active
            .get_or_init(|| Active {
                ctx: context_from(base_ctx, &self.id),
                owner_stable_id: owner_stable_id.map(str::to_string),
            })
            .ctx
    }

    /// Whether `principal` may use this session.
    ///
    /// A session nothing has activated yet belongs to nobody, so the first
    /// caller to reach it may have it. Once activated it is usable only by the
    /// principal recorded then — the check that stops a leaked or guessed id
    /// from carrying another principal's prepared statements. An unowned
    /// session (activated without authentication) stays open to everyone.
    #[must_use]
    pub fn is_owned_by(&self, principal: Option<&AuthPrincipalRef>) -> bool {
        let Some(owner) = self
            .active
            .get()
            .and_then(|active| active.owner_stable_id.as_deref())
        else {
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
            .field("activated", &self.active.get().is_some())
            .finish_non_exhaustive()
    }
}

/// A context sharing `base_ctx`'s catalogs, functions and custom rules but
/// carrying its own `session_id`, which is what scopes prepared plans to this
/// session.
fn context_from(base_ctx: &SessionContext, session_id: &str) -> Arc<SessionContext> {
    let state = builder_from_existing(&base_ctx.state())
        .with_session_id(session_id.to_string())
        .build();
    Arc::new(SessionContext::new_with_state(state))
}

/// Maps session ids to the [`SqlSession`] they name, expiring them after a
/// period of inactivity and evicting the least recently used past a capacity.
#[derive(Clone)]
pub struct SessionStore {
    sessions: Cache<String, Arc<SqlSession>>,
}

impl std::fmt::Debug for SessionStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SessionStore")
            .field("session_count", &self.sessions.entry_count())
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
        }
    }

    /// Registers a freshly minted id and returns the session it names.
    ///
    /// Only the id is created here. The [`SessionContext`] is built later, by
    /// [`SqlSession::activate`], if a statement in this session ever needs one.
    #[must_use]
    pub fn mint(&self) -> Arc<SqlSession> {
        self.register(Uuid::new_v4().hyphenated().to_string(), None)
    }

    /// Registers an id issued against a credential, so presenting the id as a
    /// bearer token authenticates as that credential. The Flight SQL handshake
    /// hands its id back in exactly that form.
    #[must_use]
    pub fn issue(&self, bearer_api_key: Option<String>) -> Arc<SqlSession> {
        self.register(Uuid::new_v4().hyphenated().to_string(), bearer_api_key)
    }

    /// The session an id names, registering the id if the store does not hold
    /// it.
    ///
    /// Registering an id a client chose is what lets a client pin its own
    /// sessions, and costs an id rather than a context — nothing is built until
    /// a statement needs one.
    #[must_use]
    pub fn open(&self, id: &str) -> Arc<SqlSession> {
        if let Some(session) = self.sessions.get(id) {
            return session;
        }
        self.register(id.to_string(), None)
    }

    /// The session an id names, if the store holds it. Registers nothing.
    #[must_use]
    pub fn get(&self, id: &str) -> Option<Arc<SqlSession>> {
        self.sessions.get(id)
    }

    /// The API key an id stands in for, so a bearer token that is a session id
    /// authenticates as the principal the session was issued to.
    ///
    /// `None` for anything that is not a session carrying a key, leaving the
    /// caller to fall through to its ordinary credential check.
    #[must_use]
    pub fn bearer_credential(&self, token: &str) -> Option<String> {
        self.get(token)
            .and_then(|session| session.bearer_api_key.clone())
    }

    /// Drops a session. Returns whether one was there to drop.
    #[must_use]
    pub fn remove(&self, id: &str) -> bool {
        let removed = self.sessions.remove(id).is_some();
        self.sessions.run_pending_tasks();
        removed
    }

    /// The number of live sessions, counting ids that have no context yet.
    #[must_use]
    pub fn count(&self) -> usize {
        usize::try_from(self.sessions.entry_count()).unwrap_or(usize::MAX)
    }

    fn register(&self, id: String, bearer_api_key: Option<String>) -> Arc<SqlSession> {
        // `get_with` collapses a concurrent race onto one initializer, so two
        // requests naming the same id share a session rather than each building
        // one and one of them losing its statements to the other's insert.
        let session = self.sessions.get_with(id.clone(), || {
            Arc::new(SqlSession {
                id,
                active: OnceLock::new(),
                bearer_api_key,
            })
        });
        self.sessions.run_pending_tasks();
        session
    }
}

/// Resolves the session a request runs in, minting an id when it names none.
///
/// Called by the HTTP and Flight middlewares, before authentication — gRPC
/// metadata is HTTP/2 headers, so both read the same map and agree on how a
/// session is named. Only the id is settled here; the context is built later,
/// and only if a statement needs one.
///
/// The order matters. `x-session-id` is the client saying which session it
/// wants. The `session-id` cookie is the same thing for a client that keeps
/// cookies rather than reading headers. A bearer token is checked last and only
/// against ids the store already holds, because it is usually an API key —
/// treating an unknown one as a session id would give every distinct key its
/// own session keyed on the credential itself.
#[must_use]
pub fn resolve_or_mint(store: &SessionStore, headers: &HeaderMap) -> Arc<SqlSession> {
    if let Some(id) = named_session(headers) {
        return store.open(&id);
    }

    // A bearer token that is a live session id — how a Flight SQL handshake
    // hands its session back to the client.
    if let Some(session) = headers
        .get(http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(bearer_token)
        .and_then(|token| store.get(token))
    {
        return session;
    }

    store.mint()
}

/// The id a request names in `x-session-id` or the `session-id` cookie.
///
/// A blank value names nothing: a proxy that injects an empty `x-session-id`
/// should leave the request to be given an id of its own, not be handed a
/// session called `''`.
#[must_use]
fn named_session(headers: &HeaderMap) -> Option<String> {
    let nonempty = |value: &str| {
        let value = value.trim();
        (!value.is_empty()).then(|| value.to_string())
    };

    headers
        .get(SESSION_ID_HEADER)
        .and_then(|value| value.to_str().ok())
        .and_then(nonempty)
        .or_else(|| {
            headers
                .get_all(http::header::COOKIE)
                .iter()
                .filter_map(|value| value.to_str().ok())
                .find_map(session_cookie)
                .and_then(nonempty)
        })
}

/// The `session-id` value out of a `Cookie` header, which carries every cookie
/// for the request as `name=value` pairs separated by `; `.
#[must_use]
fn session_cookie(header_value: &str) -> Option<&str> {
    header_value.split(';').find_map(|pair| {
        let (name, value) = pair.split_once('=')?;
        (name.trim() == SESSION_ID_COOKIE).then(|| value.trim())
    })
}

/// The `Set-Cookie` value handing `id` back to the client.
///
/// `HttpOnly` keeps it away from page scripts, `SameSite=Lax` keeps another
/// origin from driving a browser's session, and `Path=/` covers every endpoint.
/// Deliberately not `Secure`: the runtime is routinely served over plain HTTP
/// on a private network, and a `Secure` cookie would be silently dropped there.
#[must_use]
pub fn session_cookie_value(id: &str) -> String {
    format!("{SESSION_ID_COOKIE}={id}; Path=/; HttpOnly; SameSite=Lax")
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

    fn headers(pairs: &[(HeaderName, &str)]) -> HeaderMap {
        let mut headers = HeaderMap::new();
        for (name, value) in pairs {
            headers.append(name.clone(), value.parse().expect("a valid header value"));
        }
        headers
    }

    /// Minting registers an id and nothing more: the context is what costs, and
    /// most requests never need one.
    #[test]
    fn minting_an_id_builds_no_context() {
        let store = SessionStore::new();
        let session = store.mint();

        assert!(session.context().is_none());
        assert_eq!(store.count(), 1, "the id is registered so it can be named");
        assert!(
            Arc::ptr_eq(&session, &store.open(session.id())),
            "and naming it comes back to the same session"
        );
    }

    /// Session ids are handed to clients and accepted as bearer tokens, so they
    /// must be CSPRNG-random (UUIDv4) — never the time-ordered UUIDv7, whose
    /// value leaks its creation time and is partly predictable.
    #[test]
    fn a_minted_id_is_a_random_uuid() {
        let store = SessionStore::new();
        let session = store.mint();

        let uuid = Uuid::parse_str(session.id()).expect("the id is a UUID");
        assert_eq!(
            uuid.get_version(),
            Some(uuid::Version::Random),
            "{}",
            session.id()
        );
    }

    #[test]
    fn activating_builds_the_context_once() {
        let store = SessionStore::new();
        let base = SessionContext::new();
        let session = store.mint();

        let first = Arc::clone(session.activate(&base, Some(&stable_id_of("a"))));
        let again = Arc::clone(session.activate(&base, Some(&stable_id_of("b"))));

        assert!(Arc::ptr_eq(&first, &again), "the context is built once");
        assert!(
            session.is_owned_by(Some(&principal("a"))),
            "and keeps the owner recorded by the first activation"
        );
        assert!(!session.is_owned_by(Some(&principal("b"))));
    }

    #[test]
    fn an_unactivated_session_belongs_to_nobody() {
        let store = SessionStore::new();
        let session = store.mint();

        assert!(session.is_owned_by(None));
        assert!(session.is_owned_by(Some(&principal("anyone"))));
    }

    /// A runtime with no `runtime.auth` has no identity to record, so the
    /// session stays open — the behaviour an unauthenticated deployment has.
    #[test]
    fn an_unowned_session_is_open_to_everyone() {
        let store = SessionStore::new();
        let session = store.mint();
        session.activate(&SessionContext::new(), None);

        assert!(session.is_owned_by(None));
        assert!(session.is_owned_by(Some(&principal("anyone"))));
    }

    #[test]
    fn a_removed_session_is_gone() {
        let store = SessionStore::new();
        let session = store.mint();

        assert!(store.remove(session.id()));
        assert!(store.get(session.id()).is_none());
        assert!(
            !store.remove(session.id()),
            "removing a session twice reports it was already gone"
        );
    }

    /// Only an id issued against a credential stands in for one; a minted id
    /// must never authenticate.
    #[test]
    fn only_an_issued_session_is_a_bearer_credential() {
        let store = SessionStore::new();

        let issued = store.issue(Some("k".to_string()));
        assert_eq!(store.bearer_credential(issued.id()), Some("k".to_string()));

        assert_eq!(store.bearer_credential(store.mint().id()), None);
        assert_eq!(store.bearer_credential(store.issue(None).id()), None);
        assert_eq!(store.bearer_credential("not-a-session"), None);
    }

    /// A session shares the base context's catalog list rather than
    /// snapshotting it, so a dataset registered after the session was activated
    /// is visible inside it. Were it a snapshot, a long-lived session would
    /// answer from a catalog that no longer matches the runtime's.
    #[tokio::test]
    async fn a_session_sees_a_table_registered_after_it_was_activated() {
        let base = SessionContext::new();
        let store = SessionStore::new();
        let session = store.mint();
        let ctx = Arc::clone(session.activate(&base, None));

        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)])),
            vec![Arc::new(Int64Array::from(vec![7])) as ArrayRef],
        )
        .expect("the test batch is well-formed");
        base.register_batch("registered_late", batch)
            .expect("the base context accepts the table");

        let rows = ctx
            .sql("SELECT n FROM registered_late")
            .await
            .expect("the session resolves a table registered after it was activated")
            .collect()
            .await
            .expect("the query runs");

        let total: usize = rows.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total, 1);
    }

    #[test]
    fn a_request_naming_no_session_is_given_one() {
        let store = SessionStore::new();

        let first = resolve_or_mint(&store, &HeaderMap::new());
        let second = resolve_or_mint(&store, &HeaderMap::new());

        assert_ne!(
            first.id(),
            second.id(),
            "a request that names nothing cannot be put in someone else's session"
        );
    }

    #[test]
    fn a_request_is_put_in_the_session_its_header_names() {
        let store = SessionStore::new();
        let named = headers(&[(SESSION_ID_HEADER, "pinned")]);

        assert_eq!(resolve_or_mint(&store, &named).id(), "pinned");
        assert!(
            Arc::ptr_eq(
                &resolve_or_mint(&store, &named),
                &resolve_or_mint(&store, &named)
            ),
            "and comes back to it on the next request"
        );
    }

    #[test]
    fn a_request_is_put_in_the_session_its_cookie_names() {
        let store = SessionStore::new();
        let cookie = headers(&[(http::header::COOKIE, "foo=bar; session-id=pinned; baz=qux")]);

        assert_eq!(resolve_or_mint(&store, &cookie).id(), "pinned");
    }

    /// The header is the client asking directly, so it wins over a cookie a
    /// browser is replaying from an earlier session.
    #[test]
    fn the_header_wins_over_the_cookie() {
        let store = SessionStore::new();
        let both = headers(&[
            (SESSION_ID_HEADER, "from-header"),
            (http::header::COOKIE, "session-id=from-cookie"),
        ]);

        assert_eq!(resolve_or_mint(&store, &both).id(), "from-header");
    }

    /// A bearer token is usually an API key, so it names a session only when
    /// the store already holds one under it — how a Flight SQL handshake hands
    /// its session back. An API key must not become a session key.
    #[test]
    fn a_bearer_token_names_a_session_only_if_it_is_one() {
        let store = SessionStore::new();
        let issued = store.issue(Some("k".to_string()));

        let known = headers(&[(
            http::header::AUTHORIZATION,
            &format!("Bearer {}", issued.id()),
        )]);
        assert_eq!(resolve_or_mint(&store, &known).id(), issued.id());

        let api_key = headers(&[(http::header::AUTHORIZATION, "Bearer an-api-key")]);
        assert_ne!(
            resolve_or_mint(&store, &api_key).id(),
            "an-api-key",
            "an api key is a credential, not a session id"
        );
    }

    /// A blank header names nothing, so the request is given an id of its own
    /// rather than a session called `''`.
    #[test]
    fn a_blank_header_names_no_session() {
        let store = SessionStore::new();
        let blank = headers(&[(SESSION_ID_HEADER, "   ")]);

        let session = resolve_or_mint(&store, &blank);
        assert!(!session.id().trim().is_empty());
    }

    #[test]
    fn the_cookie_handed_back_carries_the_id_and_its_attributes() {
        let value = session_cookie_value("abc");

        assert!(value.starts_with("session-id=abc;"), "{value}");
        for attribute in ["Path=/", "HttpOnly", "SameSite=Lax"] {
            assert!(value.contains(attribute), "missing {attribute}: {value}");
        }
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
