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

//! Lightweight session tracking for Flight SQL
//!
//! This module provides session management to support SQL PREPARE/EXECUTE/DEALLOCATE
//! statements across multiple Flight SQL requests. Each session maintains its own
//! `SessionContext` which stores prepared statements in its `SessionState`.
//!
//! ## Session Lifecycle
//!
//! Sessions expire after 1 hour of inactivity (`SESSION_TTL_SECS`). The store
//! holds at most 10,000 sessions (`MAX_SESSIONS`). A new session is always
//! admitted: when the store is full, it evicts the least recently used session
//! of the principal holding the most sessions. One principal creating sessions
//! therefore evicts its own before any other principal's, and a principal
//! holding fewer sessions than another never loses one to that other's churn.
//! Without this, a client with any valid credential could evict every other
//! client's Handshake session, whose id is the Bearer token that client
//! authenticates with.
//!
//! ## Session ID Resolution
//!
//! The session ID is extracted from request metadata in the following priority order:
//!
//! 1. **`x-session-id` header** (preferred): Standard session tracking header returned
//!    from the handshake response. Clients that support custom headers should use this.
//!
//! 2. **Authorization Bearer token** (fallback): For compatibility with Flight SQL clients
//!    that don't support custom headers (like the arrow-flight `FlightSqlServiceClient`),
//!    the Bearer token from handshake is used as the session identifier.
//!
//! This two-tier approach ensures session persistence works with both:
//! - Modern clients that can send custom headers
//! - Standard Flight SQL clients that only support authorization headers
//!
//! ## Example Flow
//!
//! ```text
//! 1. Client calls handshake() -> receives session ID in both x-session-id header and payload
//! 2. Client sends PREPARE statement with Authorization: Bearer <session_id>
//!    -> Server creates prepared statement in session context
//! 3. Client sends EXECUTE statement with same Authorization header
//!    -> Server finds session, retrieves prepared statement, executes query
//! ```

use datafusion::prelude::SessionContext;
use http::HeaderMap;
use parking_lot::Mutex;
use runtime_auth::AuthPrincipal;
use spicepod::component::runtime::ApiKey;
use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use std::time::{Duration, Instant};
use util::session_state::builder_from_existing;
use uuid::Uuid;

/// Default session time-to-live in seconds (1 hour)
const SESSION_TTL_SECS: u64 = 3600;

/// Maximum number of sessions the store holds.
const MAX_SESSIONS: usize = 10_000;

/// Manages Flight SQL sessions, mapping session IDs to `DataFusion` `SessionContext` instances.
///
/// This enables stateful operations like SQL PREPARE/EXECUTE across multiple Flight SQL requests.
/// Sessions expire after `SESSION_TTL_SECS` of inactivity, and the store holds at most
/// `MAX_SESSIONS`, evicting by fair share when full (see the module documentation).
///
/// Each session is associated with an optional API key that was used during the handshake.
/// This allows validating session IDs in the auth layer.
#[derive(Clone)]
pub struct SessionStore {
    sessions: Arc<Mutex<Sessions>>,
    capacity: usize,
    ttl: Duration,
}

impl std::fmt::Debug for SessionStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SessionStore")
            .field("session_count", &self.session_count())
            .finish_non_exhaustive()
    }
}

/// The principal a session belongs to: its stable id, or `None` for a session
/// created without an authenticated principal.
type Owner = Option<Arc<str>>;

/// The sessions and the orders eviction and expiry read them in.
///
/// Every session is in `by_id`, at its last-use tick in `by_use`, and at the
/// same tick in its owner's map in `owners`; `by_size` holds `(count, owner)`
/// for each owner with at least one session. Ticks are unique and increase
/// with every use, so the lowest tick is the least recently used session.
#[derive(Default)]
struct Sessions {
    by_id: HashMap<Arc<str>, Slot>,
    by_use: BTreeMap<u64, Arc<str>>,
    owners: HashMap<Owner, BTreeMap<u64, Arc<str>>>,
    by_size: BTreeSet<(usize, Owner)>,
    next_tick: u64,
}

struct Slot {
    session: FlightSession,
    owner: Owner,
    tick: u64,
    last_used: Instant,
}

impl Sessions {
    fn tick(&mut self) -> u64 {
        let tick = self.next_tick;
        self.next_tick = self.next_tick.wrapping_add(1);
        tick
    }

    /// The session named `id`, marked as used now, or `None` if there is none
    /// or it has been idle for longer than `ttl` (it is then removed).
    fn get(&mut self, id: &str, now: Instant, ttl: Duration) -> Option<&FlightSession> {
        let (old_tick, owner, expired) = {
            let slot = self.by_id.get(id)?;
            (
                slot.tick,
                slot.owner.clone(),
                now.saturating_duration_since(slot.last_used) > ttl,
            )
        };
        if expired {
            self.remove(id);
            return None;
        }
        let tick = self.tick();
        let id = self.by_use.remove(&old_tick)?;
        if let Some(owned) = self.owners.get_mut(&owner) {
            owned.remove(&old_tick);
            owned.insert(tick, Arc::clone(&id));
        }
        self.by_use.insert(tick, Arc::clone(&id));
        let slot = self.by_id.get_mut(&id)?;
        slot.tick = tick;
        slot.last_used = now;
        Some(&slot.session)
    }

    /// Adds a session, first evicting expired sessions and then, while the
    /// store is full, the least recently used session of the owner holding the
    /// most. `owner` counts as holding the most when it ties, so a principal at
    /// its fair share evicts its own session rather than a peer's.
    fn insert(
        &mut self,
        id: Arc<str>,
        session: FlightSession,
        owner: Owner,
        now: Instant,
        ttl: Duration,
        capacity: usize,
    ) {
        self.remove(&id);
        self.remove_expired(now, ttl);
        while self.by_id.len() >= capacity.max(1) {
            let Some(victim) = self.eviction_victim(&owner) else {
                break;
            };
            if self.remove(&victim).is_none() {
                break;
            }
        }

        let tick = self.tick();
        let count = self.count(&owner);
        self.set_count(&owner, count, count.saturating_add(1));
        self.owners
            .entry(owner.clone())
            .or_default()
            .insert(tick, Arc::clone(&id));
        self.by_use.insert(tick, Arc::clone(&id));
        self.by_id.insert(
            id,
            Slot {
                session,
                owner,
                tick,
                last_used: now,
            },
        );
    }

    /// The least recently used session of the owner holding the most sessions,
    /// preferring `inserting` among owners tied for the most.
    fn eviction_victim(&self, inserting: &Owner) -> Option<Arc<str>> {
        let (largest, _) = self.by_size.last()?;
        let owner = if self.count(inserting) == *largest {
            inserting
        } else {
            &self.by_size.last()?.1
        };
        self.owners
            .get(owner)?
            .first_key_value()
            .map(|(_, id)| Arc::clone(id))
    }

    /// Removes the sessions idle for longer than `ttl`, least recently used
    /// first, stopping at the first that is not.
    fn remove_expired(&mut self, now: Instant, ttl: Duration) {
        while let Some((&tick, id)) = self.by_use.first_key_value() {
            if self
                .by_id
                .get(id)
                .is_some_and(|slot| now.saturating_duration_since(slot.last_used) <= ttl)
            {
                break;
            }
            let id = Arc::clone(id);
            self.remove(&id);
            self.by_use.remove(&tick);
        }
    }

    fn remove(&mut self, id: &str) -> Option<FlightSession> {
        let slot = self.by_id.remove(id)?;
        self.by_use.remove(&slot.tick);
        let count = self.count(&slot.owner);
        if let Some(owned) = self.owners.get_mut(&slot.owner) {
            owned.remove(&slot.tick);
            if owned.is_empty() {
                self.owners.remove(&slot.owner);
            }
        }
        self.set_count(&slot.owner, count, count.saturating_sub(1));
        Some(slot.session)
    }

    fn count(&self, owner: &Owner) -> usize {
        self.owners.get(owner).map_or(0, BTreeMap::len)
    }

    fn set_count(&mut self, owner: &Owner, old: usize, new: usize) {
        if old > 0 {
            self.by_size.remove(&(old, owner.clone()));
        }
        if new > 0 {
            self.by_size.insert((new, owner.clone()));
        }
    }
}

/// A Flight SQL session and the principal it belongs to.
///
/// The owner and the Handshake API key are stored in the same cache entry as
/// the context, so a session cannot lose either while it still exists.
#[derive(Clone)]
pub struct FlightSession {
    pub context: Arc<SessionContext>,
    /// Stable id of the principal that created the session, or `None` when it
    /// was created without an authenticated principal.
    pub owner_stable_id: Option<String>,
    /// The API key a Handshake authenticated with, which makes the session id
    /// usable as a Bearer token.
    api_key: Option<String>,
}

impl FlightSession {
    fn new(base_ctx: &SessionContext, session_id: &str, owner_stable_id: Option<String>) -> Self {
        // Each session needs its own `session_id` so that prepared statements
        // stored in `SessionState.prepared_plans` are isolated. Cloning the
        // existing state keeps catalog/function registrations and the custom
        // analyzer/optimizer rules.
        let state = builder_from_existing(&base_ctx.state())
            .with_session_id(session_id.to_string())
            .build();
        Self {
            context: Arc::new(SessionContext::new_with_state(state)),
            owner_stable_id,
            api_key: None,
        }
    }
}

impl SessionStore {
    /// Creates a new empty session store with default TTL and max capacity.
    #[must_use]
    pub fn new() -> Self {
        Self::with_limits(MAX_SESSIONS, Duration::from_secs(SESSION_TTL_SECS))
    }

    fn with_limits(capacity: usize, ttl: Duration) -> Self {
        Self {
            sessions: Arc::new(Mutex::new(Sessions::default())),
            capacity,
            ttl,
        }
    }

    /// Adds `session`, owned by `owner_stable_id`, evicting by fair share if
    /// the store is full.
    fn insert(&self, session_id: &str, session: FlightSession) {
        let owner: Owner = session.owner_stable_id.as_deref().map(Arc::from);
        self.sessions.lock().insert(
            Arc::from(session_id),
            session,
            owner,
            Instant::now(),
            self.ttl,
            self.capacity,
        );
    }

    /// Creates a new session with a unique ID and returns both the ID and the context.
    ///
    /// The session context is created from the provided base context's state.
    /// If an `api_key` is provided, it's associated with this session for auth validation.
    /// `owner_stable_id` is the principal the session belongs to.
    ///
    /// # Important: Session State Snapshot
    ///
    /// The session context is a **point-in-time snapshot** of the base context's state.
    /// Any changes to datasets, catalogs, or tables registered after session creation
    /// will **not** be visible within existing sessions. Clients will only see the
    /// schema and tables that existed at the moment of session creation.
    ///
    /// This design is intentional for consistency within a session's lifetime, but
    /// long-running sessions may see stale catalog information if the runtime's
    /// registered datasets change.
    #[must_use]
    pub fn create_session(
        &self,
        base_ctx: &SessionContext,
        api_key: Option<&str>,
        owner_stable_id: Option<String>,
    ) -> (String, Arc<SessionContext>) {
        // Use a CSPRNG-backed UUIDv4 rather than the time-ordered UUIDv7: this id
        // is returned to the client and accepted as a bearer session token, so it
        // must be unpredictable and must not leak its creation time.
        let session_id = Uuid::new_v4().hyphenated().to_string();

        let session = FlightSession {
            api_key: api_key.map(str::to_string),
            ..FlightSession::new(base_ctx, &session_id, owner_stable_id)
        };
        let session_ctx = Arc::clone(&session.context);
        self.insert(&session_id, session);
        (session_id, session_ctx)
    }

    /// Validates a session ID and returns the associated API key if valid.
    ///
    /// Returns `Some(api_key)` if the session exists and was created with an API key.
    /// Returns `None` if the session doesn't exist or wasn't created with an API key.
    #[must_use]
    pub fn validate_session(&self, session_id: &str) -> Option<String> {
        self.sessions
            .lock()
            .get(session_id, Instant::now(), self.ttl)?
            .api_key
            .clone()
    }

    /// Gets an existing session context by ID.
    ///
    /// Returns `None` if the session doesn't exist or has expired.
    /// Accessing a session refreshes its TTL.
    #[must_use]
    pub fn get_session(&self, session_id: &str) -> Option<Arc<SessionContext>> {
        self.sessions
            .lock()
            .get(session_id, Instant::now(), self.ttl)
            .map(|session| Arc::clone(&session.context))
    }

    /// The stable id of the principal an API key authenticates as.
    ///
    /// `ApiKey::stable_id` hashes only the key bytes (group-independent), so
    /// reconstructing the key yields the same id the authenticated principal
    /// exposes.
    #[must_use]
    pub fn api_key_stable_id(api_key: &str) -> Option<String> {
        ApiKey::parse_str(api_key).stable_id().map(Cow::into_owned)
    }

    /// Gets or creates the session named by HTTP headers.
    ///
    /// The session id is the `x-session-id` header, or else the Bearer token.
    /// An existing session is returned with the owner it was created with; a
    /// missing one is created and owned by `owner_stable_id`.
    ///
    /// Returns `None` if neither header is present.
    pub fn get_or_create_session_from_http(
        &self,
        headers: &http::HeaderMap,
        base_ctx: &SessionContext,
        owner_stable_id: Option<&str>,
    ) -> Option<FlightSession> {
        let session_id = extract_session_id_from_headers(headers)?;

        // The session id can be a Bearer credential, so it is never logged.
        tracing::debug!(
            existing_sessions = self.session_count(),
            "Flight SQL session lookup"
        );

        if let Some(session) = self
            .sessions
            .lock()
            .get(&session_id, Instant::now(), self.ttl)
        {
            return Some(session.clone());
        }

        // Built outside the lock: cloning the base session state is the
        // expensive part of creating a session.
        let created =
            FlightSession::new(base_ctx, &session_id, owner_stable_id.map(str::to_string));
        let mut sessions = self.sessions.lock();
        let now = Instant::now();
        // Another request may have created the session while this one built its own.
        if let Some(session) = sessions.get(&session_id, now, self.ttl) {
            return Some(session.clone());
        }
        let owner: Owner = owner_stable_id.map(Arc::from);
        sessions.insert(
            Arc::from(session_id.as_str()),
            created.clone(),
            owner,
            now,
            self.ttl,
            self.capacity,
        );
        drop(sessions);
        tracing::debug!("Created Flight SQL session from request");
        Some(created)
    }

    /// Removes a session from the store.
    ///
    /// Returns `true` if the session existed and was removed.
    #[must_use]
    pub fn remove_session(&self, session_id: &str) -> bool {
        self.sessions.lock().remove(session_id).is_some()
    }

    /// Returns the number of active sessions.
    #[must_use]
    pub fn session_count(&self) -> usize {
        self.sessions.lock().by_id.len()
    }
}

impl Default for SessionStore {
    fn default() -> Self {
        Self::new()
    }
}

/// Extracts the session ID from HTTP headers.
///
/// Tries to extract session ID in the following priority order:
/// 1. "x-session-id" header (standard session tracking header)
/// 2. Authorization header Bearer token (fallback for compatibility)
///
/// Returns None if neither header is present.
fn extract_session_id_from_headers(headers: &HeaderMap) -> Option<String> {
    // First try x-session-id header (preferred)
    if let Some(session_header) = headers.get("x-session-id")
        && let Ok(session_id) = session_header.to_str()
    {
        return Some(session_id.to_string());
    }

    // Fall back to authorization header for backward compatibility
    if let Some(auth_header) = headers.get("authorization")
        && let Ok(auth_str) = auth_header.to_str()
    {
        if let Some(token) = auth_str.strip_prefix("Bearer ") {
            return Some(token.to_string());
        }
        if let Some(token) = auth_str.strip_prefix("bearer ") {
            return Some(token.to_string());
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_and_get_session() {
        let store = SessionStore::new();
        let base_ctx = SessionContext::new();

        let (session_id, ctx1) = store.create_session(&base_ctx, Some("test-api-key"), None);
        assert!(store.get_session(&session_id).is_some());

        let ctx2 = store
            .get_session(&session_id)
            .expect("Session should exist");
        assert!(Arc::ptr_eq(&ctx1, &ctx2));

        // Validate session returns the API key
        assert_eq!(
            store.validate_session(&session_id),
            Some("test-api-key".to_string())
        );
    }

    #[test]
    fn test_create_session_without_api_key() {
        let store = SessionStore::new();
        let base_ctx = SessionContext::new();

        let (session_id, _) = store.create_session(&base_ctx, None, None);
        assert!(store.get_session(&session_id).is_some());

        // Validate session returns None when no API key was provided
        assert_eq!(store.validate_session(&session_id), None);
    }

    #[test]
    fn test_remove_session() {
        let store = SessionStore::new();
        let base_ctx = SessionContext::new();

        let (session_id, _) = store.create_session(&base_ctx, Some("key"), None);
        assert_eq!(store.session_count(), 1);

        assert!(store.remove_session(&session_id));
        assert_eq!(store.session_count(), 0);
        assert!(store.get_session(&session_id).is_none());
    }

    #[test]
    fn test_multiple_sessions() {
        let store = SessionStore::new();
        let base_ctx = SessionContext::new();

        let (id1, _) = store.create_session(&base_ctx, Some("key1"), None);
        let (id2, _) = store.create_session(&base_ctx, Some("key2"), None);

        assert_ne!(id1, id2);
        assert_eq!(store.session_count(), 2);

        assert!(store.get_session(&id1).is_some());
        assert!(store.get_session(&id2).is_some());

        // Validate each session returns its own API key
        assert_eq!(store.validate_session(&id1), Some("key1".to_string()));
        assert_eq!(store.validate_session(&id2), Some("key2".to_string()));
    }

    #[test]
    fn test_extract_session_id_from_headers() {
        let mut headers = HeaderMap::new();

        // No authorization header
        assert!(extract_session_id_from_headers(&headers).is_none());

        // With Bearer prefix
        headers.insert(
            "authorization",
            "Bearer test-session-id".parse().expect("Valid header"),
        );
        assert_eq!(
            extract_session_id_from_headers(&headers),
            Some("test-session-id".to_string())
        );

        // Without Bearer prefix - should return None since we require "Bearer " prefix
        headers.insert(
            "authorization",
            "test-session-id-2".parse().expect("Valid header"),
        );
        assert!(
            extract_session_id_from_headers(&headers).is_none(),
            "Authorization header without 'Bearer ' prefix should return None"
        );

        // `x-session-id` takes precedence over the Bearer token.
        headers.insert(
            "authorization",
            "Bearer test-session-id".parse().expect("Valid header"),
        );
        headers.insert("x-session-id", "chosen".parse().expect("Valid header"));
        assert_eq!(
            extract_session_id_from_headers(&headers),
            Some("chosen".to_string())
        );
    }

    #[test]
    fn test_get_or_create_session_from_http() {
        let store = SessionStore::new();
        let base_ctx = SessionContext::new();
        let mut headers = HeaderMap::new();
        headers.insert(
            "authorization",
            "Bearer new-session".parse().expect("Valid header"),
        );

        // First call creates a new session
        let first = store
            .get_or_create_session_from_http(&headers, &base_ctx, Some("apikey:creator"))
            .expect("Should create session");
        assert_eq!(store.session_count(), 1);

        // Second call returns the same session
        let second = store
            .get_or_create_session_from_http(&headers, &base_ctx, Some("apikey:creator"))
            .expect("Should return existing session");
        assert!(Arc::ptr_eq(&first.context, &second.context));
        assert_eq!(store.session_count(), 1);
    }

    #[test]
    fn test_session_id_is_csprng_uuid_v4() {
        // Regression: session ids are returned to clients and accepted as bearer
        // session tokens, so they must be CSPRNG-random (UUIDv4), never the
        // time-ordered/partly-predictable UUIDv7.
        let store = SessionStore::new();
        let base_ctx = SessionContext::new();
        let (session_id, _) = store.create_session(&base_ctx, Some("k"), None);

        let uuid = Uuid::parse_str(&session_id).expect("session id should be a valid UUID");
        assert_eq!(
            uuid.get_version(),
            Some(uuid::Version::Random),
            "session id must be a UUIDv4 (random), got {session_id}"
        );
    }

    #[test]
    fn a_session_keeps_the_owner_it_was_created_with() {
        let store = SessionStore::new();
        let base_ctx = SessionContext::new();
        let header = |session_id: &str| {
            let mut headers = HeaderMap::new();
            headers.insert(
                "x-session-id",
                session_id.parse().expect("valid header value"),
            );
            headers
        };

        // A Handshake session is owned by the principal of the key that created it.
        let owner = SessionStore::api_key_stable_id("owner-key");
        assert!(owner.is_some());
        let (session_id, _) = store.create_session(&base_ctx, Some("owner-key"), owner.clone());
        let named_by_another = store
            .get_or_create_session_from_http(&header(&session_id), &base_ctx, Some("apikey:other"))
            .expect("the session exists");
        assert_eq!(named_by_another.owner_stable_id, owner);

        // A session a principal creates by naming a new id is owned by that principal,
        // and naming it again as another principal does not change the owner.
        let created = store
            .get_or_create_session_from_http(&header("shared-label"), &base_ctx, Some("apikey:a"))
            .expect("creates the session");
        assert_eq!(created.owner_stable_id.as_deref(), Some("apikey:a"));
        let named_by_b = store
            .get_or_create_session_from_http(&header("shared-label"), &base_ctx, Some("apikey:b"))
            .expect("the session exists");
        assert!(Arc::ptr_eq(&created.context, &named_by_b.context));
        assert_eq!(named_by_b.owner_stable_id.as_deref(), Some("apikey:a"));

        // Without an authenticated principal, a session has no owner.
        let anonymous = store
            .get_or_create_session_from_http(&header("anonymous-label"), &base_ctx, None)
            .expect("creates the session");
        assert_eq!(anonymous.owner_stable_id, None);
    }

    fn handshake_session(store: &SessionStore, base_ctx: &SessionContext, key: &str) -> String {
        let owner = SessionStore::api_key_stable_id(key);
        store.create_session(base_ctx, Some(key), owner).0
    }

    /// Regression test: one principal creating sessions must not evict another
    /// principal's, whose session id is the Bearer token it authenticates with.
    #[test]
    fn a_churning_principal_evicts_only_its_own_sessions() {
        let store = SessionStore::with_limits(4, Duration::from_secs(SESSION_TTL_SECS));
        let base_ctx = SessionContext::new();

        let victim = handshake_session(&store, &base_ctx, "victim-key");
        let churned: Vec<String> = (0..20)
            .map(|_| handshake_session(&store, &base_ctx, "churn-key"))
            .collect();

        assert_eq!(store.session_count(), 4);
        assert_eq!(
            store.validate_session(&victim),
            Some("victim-key".to_string())
        );
        // The churner keeps its three most recent sessions, and every new one
        // is admitted.
        let kept: Vec<bool> = churned
            .iter()
            .map(|id| store.validate_session(id).is_some())
            .collect();
        let mut expected = vec![false; 17];
        expected.extend([true; 3]);
        assert_eq!(kept, expected);
    }

    /// Sessions a principal names with `x-session-id` count toward its share
    /// the same as Handshake sessions.
    #[test]
    fn sessions_named_by_header_are_evicted_from_their_owner() {
        let store = SessionStore::with_limits(4, Duration::from_secs(SESSION_TTL_SECS));
        let base_ctx = SessionContext::new();
        let header = |session_id: &str| {
            let mut headers = HeaderMap::new();
            headers.insert(
                "x-session-id",
                session_id.parse().expect("valid header value"),
            );
            headers
        };

        let a = store
            .get_or_create_session_from_http(&header("a-1"), &base_ctx, Some("apikey:a"))
            .expect("creates the session");
        store
            .get_or_create_session_from_http(&header("a-2"), &base_ctx, Some("apikey:a"))
            .expect("creates the session");
        for i in 0..10 {
            store
                .get_or_create_session_from_http(
                    &header(&format!("b-{i}")),
                    &base_ctx,
                    Some("apikey:b"),
                )
                .expect("creates the session");
        }

        // `a` holds two, fewer than `b`, so `b` evicted only its own.
        let a_again = store
            .get_or_create_session_from_http(&header("a-1"), &base_ctx, Some("apikey:a"))
            .expect("the session exists");
        assert!(Arc::ptr_eq(&a.context, &a_again.context));
        assert!(store.get_session("a-2").is_some());
        assert!(store.get_session("b-7").is_none());
        assert!(store.get_session("b-8").is_some());
        assert!(store.get_session("b-9").is_some());
        assert_eq!(store.session_count(), 4);
    }

    /// When the store is full, the principal holding the most sessions loses
    /// its least recently used one, even to another principal's new session.
    #[test]
    fn a_full_store_evicts_from_the_largest_owner() {
        let ttl = Duration::from_secs(SESSION_TTL_SECS);
        let base_ctx = SessionContext::new();
        let now = Instant::now();
        let mut sessions = Sessions::default();
        let session = |owner: &str| FlightSession::new(&base_ctx, "id", Some(owner.to_string()));
        let insert = |sessions: &mut Sessions, id: &str, owner: &str| {
            sessions.insert(
                Arc::from(id),
                session(owner),
                Some(Arc::from(owner)),
                now,
                ttl,
                4,
            );
        };

        insert(&mut sessions, "big-1", "big");
        insert(&mut sessions, "big-2", "big");
        insert(&mut sessions, "big-3", "big");
        insert(&mut sessions, "small-1", "small");
        // `big-1` is used, so `big-2` is now the least recently used of `big`'s.
        assert!(sessions.get("big-1", now, ttl).is_some());
        insert(&mut sessions, "small-2", "small");

        let mut held: Vec<&str> = sessions.by_id.keys().map(|id| &**id).collect();
        held.sort_unstable();
        assert_eq!(held, ["big-1", "big-3", "small-1", "small-2"]);
    }

    #[test]
    fn an_idle_session_expires() {
        let ttl = Duration::from_secs(60);
        let base_ctx = SessionContext::new();
        let start = Instant::now();
        let mut sessions = Sessions::default();
        let session = FlightSession::new(&base_ctx, "idle", None);
        sessions.insert(Arc::from("idle"), session.clone(), None, start, ttl, 4);
        sessions.insert(Arc::from("used"), session, None, start, ttl, 4);

        // Using a session restarts its idle time.
        let later = start + Duration::from_secs(45);
        assert!(sessions.get("used", later, ttl).is_some());

        let after_ttl = start + Duration::from_secs(61);
        assert!(sessions.get("idle", after_ttl, ttl).is_none());
        assert!(sessions.get("used", after_ttl, ttl).is_some());
        assert_eq!(sessions.by_id.len(), 1);
        assert_eq!(sessions.by_use.len(), 1);
        assert_eq!(sessions.by_size.len(), 1);
    }
}
