use std::sync::Arc;

use axum::http;
use runtime_auth::{AuthPrincipalRef, AuthVerdict, HttpAuth, error};

use super::anonymous::Anonymous;

pub struct NoAuth;

impl HttpAuth for NoAuth {
    fn http_verify(&self, _req: &http::request::Parts) -> Result<AuthVerdict, error::Error> {
        Ok(AuthVerdict::Allow(Arc::new(Anonymous) as AuthPrincipalRef))
    }
}
