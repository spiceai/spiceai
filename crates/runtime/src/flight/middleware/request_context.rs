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
    app: Option<Arc<App>>,
    df: Arc<DataFusion>,
    session_store: SessionStore,
    secrets: Arc<RwLock<secrets::Secrets>>,
    job_executor: Option<Arc<JobExecutor>>,
}

impl RequestContextLayer {
    #[must_use]
    pub fn new(
        app: Option<Arc<App>>,
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
            app: self.app.clone(),
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
    app: Option<Arc<App>>,
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

        let headers = req.headers();

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

        let mut builder = RequestContext::builder(Protocol::Flight)
            .with_app_opt(self.app.clone())
            .with_extension(DataFusionContextExtension::new(Arc::clone(&self.df)))
            .with_extension(ModelContextExtension::new())
            .with_extension(AppContextExtension::new(self.app.clone()))
            .with_extension(SecretsContextExtension::new(Arc::clone(&self.secrets)));

        // Add job executor extension if available (cluster mode)
        if let Some(ref executor) = self.job_executor {
            builder =
                builder.with_extension(JobExecutorContextExtension::new(Arc::clone(executor)));
        }

        // Add session extension if we have one
        if let Some(session_ext) = session_ext {
            builder = builder.with_extension(session_ext);
        }

        let request_context = Arc::new(builder.from_headers(headers).build());

        req.extensions_mut()
            .insert::<Arc<dyn AuthRequestContext + Send + Sync>>(
                Arc::clone(&request_context) as Arc<dyn AuthRequestContext + Send + Sync>
            );

        Box::pin(Arc::clone(&request_context).scope(async move {
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
        }))
    }
}
