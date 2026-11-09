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

use std::{
    borrow::Cow,
    sync::Arc,
    task::{Context, Poll},
};

use runtime_auth::AuthRequestContext;
use runtime_request_context::RequestContext;
use tower::{Layer, Service};

use crate::{
    datafusion::{DataFusion, flight_session_extension::FlightSessionExtension},
    flight::SessionStore,
};

/// Binds a request to the Flight SQL session its `x-session-id` header or
/// Bearer token names, creating the session — owned by the request's
/// principal — if it does not exist.
///
/// Must run after authentication. The session id is client-controlled and an
/// unknown id creates a session, so when an authenticator is configured only a
/// request with an authenticated principal is bound; otherwise any client could
/// fill the bounded session store and evict authenticated sessions, whose
/// Bearer tokens then stop authenticating.
#[derive(Clone)]
pub struct FlightSessionLayer {
    df: Arc<DataFusion>,
    session_store: SessionStore,
    auth_configured: bool,
}

impl FlightSessionLayer {
    #[must_use]
    pub fn new(df: Arc<DataFusion>, session_store: SessionStore, auth_configured: bool) -> Self {
        Self {
            df,
            session_store,
            auth_configured,
        }
    }
}

impl<S> Layer<S> for FlightSessionLayer {
    type Service = FlightSessionMiddleware<S>;

    fn layer(&self, inner: S) -> Self::Service {
        FlightSessionMiddleware {
            inner,
            df: Arc::clone(&self.df),
            session_store: self.session_store.clone(),
            auth_configured: self.auth_configured,
        }
    }
}

#[derive(Clone)]
pub struct FlightSessionMiddleware<S> {
    inner: S,
    df: Arc<DataFusion>,
    session_store: SessionStore,
    auth_configured: bool,
}

impl<S, ReqBody> Service<http::Request<ReqBody>> for FlightSessionMiddleware<S>
where
    S: Service<http::Request<ReqBody>>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: http::Request<ReqBody>) -> Self::Future {
        // With an authenticator configured, only `Handshake` reaches this layer
        // unauthenticated, and it creates its own session.
        let principal = req
            .extensions()
            .get::<Arc<dyn AuthRequestContext + Send + Sync>>()
            .and_then(|context| context.auth_principal());
        let authenticated = principal.is_some();
        let principal_stable_id =
            principal.and_then(|principal| principal.stable_id().map(Cow::into_owned));

        if (authenticated || !self.auth_configured)
            && let Some(request_context) = req.extensions().get::<Arc<RequestContext>>()
            && let Some(session) = self.session_store.get_or_create_session_from_http(
                req.headers(),
                &self.df.ctx,
                principal_stable_id.as_deref(),
            )
        {
            request_context.insert_extension(FlightSessionExtension::new(
                session.context,
                session.owner_stable_id,
            ));
        }

        self.inner.call(req)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        dataaccelerator::AcceleratorEngineRegistry,
        datafusion::builder::DataFusionBuilder,
        flight::{
            middleware::{RequestContextLayer, request_context::RequestContextMiddleware},
            session_auth,
        },
        secrets,
        status::RuntimeStatus,
    };
    use axum::body::Body;
    use runtime_auth::{
        FlightBasicAuth,
        api_key::ApiKeyAuth,
        layer::flight::{BasicAuthLayer, BasicAuthMiddleware},
    };
    use runtime_request_context::AsyncMarker;
    use spicepod::component::runtime::ApiKey;
    use std::{future::Future, pin::Pin, sync::Mutex};
    use tokio::{runtime::Handle, sync::RwLock};

    const API_KEY: &str = "flight-session-key";
    const OTHER_API_KEY: &str = "other-flight-session-key";
    const LIST_ACTIONS: &str = "/arrow.flight.protocol.FlightService/ListActions";
    const HANDSHAKE: &str = "/arrow.flight.protocol.FlightService/Handshake";

    /// The session a request reached the terminal service bound to.
    #[derive(Clone, Debug, PartialEq, Eq)]
    enum Bound {
        NoSession,
        Session { owner: Option<String> },
    }

    /// Terminal service that records the session each request reached it bound to.
    #[derive(Clone, Default)]
    struct ObserveSession {
        sessions: Arc<Mutex<Vec<Bound>>>,
    }

    impl ObserveSession {
        fn sessions(&self) -> Vec<Bound> {
            self.sessions
                .lock()
                .expect("observations mutex is not poisoned")
                .clone()
        }
    }

    impl Service<http::Request<Body>> for ObserveSession {
        type Response = http::Response<Body>;
        type Error = std::convert::Infallible;
        type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _req: http::Request<Body>) -> Self::Future {
            let sessions = Arc::clone(&self.sessions);
            Box::pin(async move {
                let context = RequestContext::current(AsyncMarker::new().await);
                let bound = context.extension::<FlightSessionExtension>().map_or(
                    Bound::NoSession,
                    |session| Bound::Session {
                        owner: session.owner_stable_id().map(str::to_string),
                    },
                );
                sessions
                    .lock()
                    .expect("observations mutex is not poisoned")
                    .push(bound);
                Ok(http::Response::new(Body::empty()))
            })
        }
    }

    /// The public Flight server's order: request context, authentication, sessions.
    fn flight_stack(
        store: &SessionStore,
        observe: ObserveSession,
    ) -> RequestContextMiddleware<BasicAuthMiddleware<FlightSessionMiddleware<ObserveSession>>>
    {
        let df = Arc::new(
            DataFusionBuilder::new(
                RuntimeStatus::new(),
                Arc::new(AcceleratorEngineRegistry::new()),
                Handle::current(),
            )
            .build(),
        );
        let auth = Arc::new(ApiKeyAuth::new(vec![
            ApiKey::parse_str(&format!("{API_KEY}:rw")),
            ApiKey::parse_str(&format!("{OTHER_API_KEY}:rw")),
        ])) as Arc<dyn FlightBasicAuth + Send + Sync>;
        let auth_layer = BasicAuthLayer::new(session_auth::with_session_awareness(
            Some(auth),
            store.clone(),
        ));
        let session_layer = FlightSessionLayer::new(Arc::clone(&df), store.clone(), true);

        RequestContextLayer::new(
            Arc::new(RwLock::new(None)),
            df,
            Arc::new(RwLock::new(secrets::Secrets::new())),
        )
        .layer(auth_layer.layer(session_layer.layer(observe)))
    }

    fn request(path: &str, headers: &[(&str, &str)]) -> http::Request<Body> {
        let mut builder = http::Request::builder()
            .method(http::Method::POST)
            .uri(path)
            .header(http::header::CONTENT_TYPE, "application/grpc");
        for (name, value) in headers {
            builder = builder.header(*name, *value);
        }
        builder.body(Body::empty()).expect("request builds")
    }

    async fn call_authenticated(
        service: &mut RequestContextMiddleware<
            BasicAuthMiddleware<FlightSessionMiddleware<ObserveSession>>,
        >,
        headers: &[(&str, &str)],
    ) {
        let response = service
            .call(request(LIST_ACTIONS, headers))
            .await
            .expect("the stack answers");
        assert!(
            response.headers().get("grpc-status").is_none(),
            "the request is authenticated"
        );
    }

    #[tokio::test]
    async fn an_unauthenticated_request_creates_no_session() {
        let store = SessionStore::new();
        let observe = ObserveSession::default();
        let mut service = flight_stack(&store, observe.clone());

        let rejected = service
            .call(request(
                LIST_ACTIONS,
                &[
                    ("authorization", "Bearer not-an-api-key"),
                    ("x-session-id", "chosen-by-the-client"),
                ],
            ))
            .await
            .expect("the stack answers");
        assert_eq!(
            rejected
                .headers()
                .get("grpc-status")
                .and_then(|status| status.to_str().ok()),
            Some("16"),
            "the request is rejected as unauthenticated"
        );

        // `Handshake` is exempt from the auth layer, so it reaches the session layer unauthenticated.
        service
            .call(request(
                HANDSHAKE,
                &[("authorization", "Bearer not-an-api-key")],
            ))
            .await
            .expect("the stack answers");

        assert_eq!(store.session_count(), 0);
        assert_eq!(observe.sessions(), vec![Bound::NoSession]);
    }

    #[tokio::test]
    async fn an_authenticated_request_keeps_its_session_across_requests() {
        let store = SessionStore::new();
        let observe = ObserveSession::default();
        let mut service = flight_stack(&store, observe.clone());
        let bearer = format!("Bearer {API_KEY}");

        for _ in 0..2 {
            call_authenticated(&mut service, &[("authorization", &bearer)]).await;
        }

        let owner = SessionStore::api_key_stable_id(API_KEY);
        assert!(owner.is_some());
        let bound = Bound::Session { owner };
        assert_eq!(store.session_count(), 1);
        assert_eq!(observe.sessions(), vec![bound.clone(), bound]);
    }

    #[tokio::test]
    async fn naming_another_principals_session_does_not_take_it_over() {
        let store = SessionStore::new();
        let observe = ObserveSession::default();
        let mut service = flight_stack(&store, observe.clone());
        let creator = format!("Bearer {API_KEY}");
        let other = format!("Bearer {OTHER_API_KEY}");

        call_authenticated(
            &mut service,
            &[
                ("authorization", &creator),
                ("x-session-id", "shared-label"),
            ],
        )
        .await;
        call_authenticated(
            &mut service,
            &[("authorization", &other), ("x-session-id", "shared-label")],
        )
        .await;

        // Both requests name one session, which stays owned by its creator, so the
        // query engine will not run the second principal's request in it.
        let owner = SessionStore::api_key_stable_id(API_KEY);
        assert!(owner.is_some());
        assert_ne!(owner, SessionStore::api_key_stable_id(OTHER_API_KEY));
        let bound = Bound::Session { owner };
        assert_eq!(store.session_count(), 1);
        assert_eq!(observe.sessions(), vec![bound.clone(), bound]);
    }
}
