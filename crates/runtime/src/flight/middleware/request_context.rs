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

use crate::{
    datafusion::{
        DataFusion, flight_session_extension::FlightSessionExtension,
        job_executor_context_extension::JobExecutorContextExtension,
        request_context_extension::DataFusionContextExtension,
    },
    flight::SessionStore,
    jobs::JobExecutor,
    model::ModelContextExtension,
    secrets,
};
use app::App;
use runtime_request_context::{Protocol, RequestContext};
use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tokio::sync::RwLock;

use crate::datafusion::app_context_extension::AppContextExtension;
use crate::datafusion::secrets_context_extension::SecretsContextExtension;
use runtime_auth::AuthRequestContext;
use tower::{Layer, Service};

/// Extracts the request context from the HTTP headers and adds it to the task-local context.
#[derive(Clone)]
pub struct RequestContextLayer {
    app: Arc<RwLock<Option<Arc<App>>>>,
    df: Arc<DataFusion>,
    session_store: SessionStore,
    secrets: Arc<RwLock<secrets::Secrets>>,
    job_executor: Option<Arc<JobExecutor>>,
}

impl RequestContextLayer {
    #[must_use]
    pub fn new(
        app: Arc<RwLock<Option<Arc<App>>>>,
        df: Arc<DataFusion>,
        session_store: SessionStore,
        secrets: Arc<RwLock<secrets::Secrets>>,
    ) -> Self {
        Self {
            app,
            df,
            session_store,
            secrets,
            job_executor: None,
        }
    }

    /// Sets the job executor for async query operations (cluster mode only).
    #[must_use]
    pub fn with_job_executor(mut self, executor: Option<Arc<JobExecutor>>) -> Self {
        self.job_executor = executor;
        self
    }
}

impl<S> Layer<S> for RequestContextLayer {
    type Service = RequestContextMiddleware<S>;

    fn layer(&self, inner: S) -> Self::Service {
        RequestContextMiddleware {
            inner,
            app: Arc::clone(&self.app),
            df: Arc::clone(&self.df),
            session_store: self.session_store.clone(),
            secrets: Arc::clone(&self.secrets),
            job_executor: self.job_executor.clone(),
        }
    }
}

#[derive(Clone)]
pub struct RequestContextMiddleware<S> {
    inner: S,
    app: Arc<RwLock<Option<Arc<App>>>>,
    df: Arc<DataFusion>,
    session_store: SessionStore,
    secrets: Arc<RwLock<secrets::Secrets>>,
    job_executor: Option<Arc<JobExecutor>>,
}

impl<S, ReqBody, ResBody> Service<http::Request<ReqBody>> for RequestContextMiddleware<S>
where
    S: Service<http::Request<ReqBody>, Response = http::Response<ResBody>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    ResBody: http_body::Body + Send + 'static,
    ReqBody: Send + 'static,
{
    type Response = http::Response<util::cancel_guard_body::CancelGuardBody<ResBody>>;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut req: http::Request<ReqBody>) -> Self::Future {
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);

        // Try to get or create a session for this request. Capture the owning
        // principal's stable id (if the request names an existing, owned session)
        // so the session can be bound to its owner at execution time. This layer
        // runs before auth, so ownership is *recorded* here and *enforced* later
        // where the authenticated principal is known.
        let owner_stable_id = self.session_store.owner_stable_id_from_http(req.headers());
        let session_ext = self
            .session_store
            .get_or_create_session_from_http(req.headers(), &self.df.ctx)
            .map(|ctx| FlightSessionExtension::new(ctx, owner_stable_id));

        let app_lock = Arc::clone(&self.app);
        let df = Arc::clone(&self.df);
        let secrets = Arc::clone(&self.secrets);
        let job_executor = self.job_executor.clone();

        Box::pin(async move {
            // Read the app the runtime is currently serving, so a spicepod
            // reload is visible to the next request. The guard is released
            // before any `.await` in the request path.
            let app = app_lock.read().await.as_ref().map(Arc::clone);

            let mut builder = RequestContext::builder(Protocol::Flight)
                .with_app_opt(app.clone())
                .with_extension(DataFusionContextExtension::new(df))
                .with_extension(ModelContextExtension::new())
                .with_extension(AppContextExtension::new(app))
                .with_extension(SecretsContextExtension::new(secrets));

            // Add job executor extension if available (cluster mode)
            if let Some(executor) = job_executor {
                builder = builder.with_extension(JobExecutorContextExtension::new(executor));
            }

            // Add session extension if we have one
            if let Some(session_ext) = session_ext {
                builder = builder.with_extension(session_ext);
            }

            let request_context = Arc::new(builder.from_headers(req.headers()).build());

            req.extensions_mut()
                .insert::<Arc<dyn AuthRequestContext + Send + Sync>>(
                    Arc::clone(&request_context) as Arc<dyn AuthRequestContext + Send + Sync>
                );

            Arc::clone(&request_context)
                .scope(async move {
                    request_context.load_extensions().await;
                    // Drop guard cancels the request's cancellation token if the
                    // response body is dropped mid-flight (e.g. client disconnects
                    // during a long-running Flight DoGet stream). The guard is
                    // attached to the response body via `CancelGuardBody`, which
                    // disarms it once the body signals end-of-stream so normal
                    // completion does not cancel the token.
                    let cancel_guard = request_context.cancellation_token().clone().drop_guard();
                    let response = inner.call(req).await?;
                    let (parts, body) = response.into_parts();
                    let body = util::cancel_guard_body::CancelGuardBody::new(body, cancel_guard);
                    Ok(http::Response::from_parts(parts, body))
                })
                .await
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        dataaccelerator::AcceleratorEngineRegistry, datafusion::builder::DataFusionBuilder,
        status::RuntimeStatus,
    };
    use axum::body::Body;
    use runtime_request_context::{AsyncMarker, CacheControl, CacheKeyType};
    use spicepod::component::caching::{
        CacheKeyType as ConfiguredCacheKeyType, SQLResultsCacheConfig,
    };
    use spicepod::component::runtime::{Flight, FlightBatchSize, Query, UserAgentCollection};
    use std::sync::Mutex;
    use std::time::Duration;
    use tokio::runtime::Handle;

    /// The app-derived settings a request resolves, covering each channel the
    /// app reaches the request context through: `cache_control` and
    /// `query_timeout` are baked by `RequestContextBuilder::build`,
    /// `user_agent_collected` by `RequestContextBuilder::from_headers`, and
    /// `flight_batch_size` is read back off [`AppContextExtension`].
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    struct Observed {
        query_timeout: Option<Duration>,
        cache_control: CacheControl,
        user_agent_collected: bool,
        flight_batch_size: Option<FlightBatchSize>,
    }

    /// Terminal service that records what the surrounding request context
    /// resolves, so the assertions run against the context the middleware
    /// actually installed rather than against the app itself.
    #[derive(Clone)]
    struct ObserveContext {
        observed: Arc<Mutex<Vec<Observed>>>,
    }

    impl Service<http::Request<Body>> for ObserveContext {
        type Response = http::Response<Body>;
        type Error = std::convert::Infallible;
        type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _req: http::Request<Body>) -> Self::Future {
            let observed = Arc::clone(&self.observed);
            Box::pin(async move {
                let context = RequestContext::current(AsyncMarker::new().await);
                let flight_batch_size = context
                    .extension::<AppContextExtension>()
                    .and_then(|app_ext| app_ext.app())
                    .and_then(|app| app.runtime.flight.as_ref().map(|flight| flight.batch_size));
                let user_agent_collected = context
                    .to_dimensions()
                    .iter()
                    .any(|dimension| dimension.key.as_str() == "user_agent");

                observed
                    .lock()
                    .expect("observations mutex is not poisoned")
                    .push(Observed {
                        query_timeout: context.query_timeout(),
                        cache_control: context.cache_control(),
                        user_agent_collected,
                        flight_batch_size,
                    });

                Ok(http::Response::new(Body::empty()))
            })
        }
    }

    fn app_with(
        timeout: &str,
        cache_key_type: ConfiguredCacheKeyType,
        user_agent_collection: UserAgentCollection,
        max_batch_size: usize,
    ) -> Arc<App> {
        let mut app = app::AppBuilder::new("test").build();
        app.runtime.query = Some(Query {
            timeout: Some(timeout.to_string()),
            ..Default::default()
        });
        app.runtime.caching.sql_results = Some(SQLResultsCacheConfig {
            cache_key_type,
            ..SQLResultsCacheConfig::default()
        });
        app.runtime.telemetry.user_agent_collection = user_agent_collection;
        app.runtime.flight = Some(Flight {
            batch_size: FlightBatchSize::Adaptive {
                max: max_batch_size,
            },
            ..Flight::default()
        });
        Arc::new(app)
    }

    async fn observe(service: &mut RequestContextMiddleware<ObserveContext>) {
        let request = http::Request::builder()
            .uri("/arrow.flight.protocol.FlightService/DoGet")
            .header(http::header::USER_AGENT, "spice-test/1.0")
            .body(Body::empty())
            .expect("request builds");

        service
            .call(request)
            .await
            .expect("the middleware forwards to the observing service");
    }

    /// A Flight request resolves its app-derived settings from the app the
    /// runtime is currently serving, so a spicepod reload is visible to the
    /// next request and both protocols report the same value.
    #[tokio::test]
    async fn flight_request_context_follows_app_reload() {
        let app = Arc::new(RwLock::new(Some(app_with(
            "30s",
            ConfiguredCacheKeyType::Plan,
            UserAgentCollection::Full,
            1024,
        ))));
        let df = Arc::new(
            DataFusionBuilder::new(
                RuntimeStatus::new(),
                Arc::new(AcceleratorEngineRegistry::new()),
                Handle::current(),
            )
            .build(),
        );

        let observed = Arc::new(Mutex::new(Vec::new()));
        let mut service = RequestContextLayer::new(
            Arc::clone(&app),
            df,
            SessionStore::new(),
            Arc::new(RwLock::new(secrets::Secrets::new())),
        )
        .layer(ObserveContext {
            observed: Arc::clone(&observed),
        });

        observe(&mut service).await;

        *app.write().await = Some(app_with(
            "5s",
            ConfiguredCacheKeyType::Sql,
            UserAgentCollection::Disabled,
            4096,
        ));

        observe(&mut service).await;

        let http_context = RequestContext::builder(Protocol::Http)
            .with_app_opt(app.read().await.as_ref().map(Arc::clone))
            .build();

        let observed = observed
            .lock()
            .expect("observations mutex is not poisoned")
            .clone();

        assert_eq!(
            observed,
            vec![
                Observed {
                    query_timeout: Some(Duration::from_secs(30)),
                    cache_control: CacheControl::Cache(CacheKeyType::Default),
                    user_agent_collected: true,
                    flight_batch_size: Some(FlightBatchSize::Adaptive { max: 1024 }),
                },
                Observed {
                    query_timeout: Some(Duration::from_secs(5)),
                    cache_control: CacheControl::Cache(CacheKeyType::Raw),
                    user_agent_collected: false,
                    flight_batch_size: Some(FlightBatchSize::Adaptive { max: 4096 }),
                },
            ]
        );
        assert_eq!(observed[1].query_timeout, http_context.query_timeout());
        assert_eq!(observed[1].cache_control, http_context.cache_control());
    }
}
