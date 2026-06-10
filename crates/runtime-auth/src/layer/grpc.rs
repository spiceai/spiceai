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

use crate::{AuthVerdict, GrpcAuth};
use std::sync::Arc;
use tonic::Status;
use tonic::service::Interceptor;

#[must_use]
pub fn make_interceptor(
    auth_verifier: Option<Arc<dyn GrpcAuth + Send + Sync>>,
) -> impl Interceptor + Send + Sync + Clone {
    move |mut req: tonic::Request<()>| {
        if let Some(auth_verifier) = &auth_verifier {
            match auth_verifier.grpc_verify(&req) {
                Ok(AuthVerdict::Allow(principal)) => {
                    // Preserve the authenticated principal on the request so that
                    // downstream read-only/write authorization is not silently
                    // disabled if this interceptor is ever wired into a service.
                    req.extensions_mut().insert(principal);
                    Ok(req)
                }
                Ok(AuthVerdict::Deny) => Err(Status::unauthenticated("Invalid credentials")),
                Err(e) => {
                    tracing::error!("Error verifying credentials: {e}");
                    Err(tonic::Status::internal("Internal server error"))
                }
            }
        } else {
            Ok(req)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{make_interceptor, AuthVerdict, GrpcAuth};
    use crate::AuthPrincipalRef;
    use app::spicepod::component::runtime::ApiKey;
    use std::sync::Arc;
    use tonic::service::Interceptor;

    struct AllowWith(AuthPrincipalRef);
    impl GrpcAuth for AllowWith {
        fn grpc_verify(
            &self,
            _req: &tonic::Request<()>,
        ) -> Result<AuthVerdict, crate::error::Error> {
            Ok(AuthVerdict::Allow(Arc::clone(&self.0)))
        }
    }

    #[test]
    fn interceptor_preserves_principal_on_allow() {
        let principal: AuthPrincipalRef = Arc::new(ApiKey::parse_str("test-key:rw"));
        let auth: Arc<dyn GrpcAuth + Send + Sync> = Arc::new(AllowWith(Arc::clone(&principal)));
        let mut interceptor = make_interceptor(Some(auth));

        let out = interceptor
            .call(tonic::Request::new(()))
            .expect("allow verdict should pass the request");

        // Regression: the authenticated principal must be carried on the request
        // (not dropped), so downstream authorization is not silently disabled.
        assert!(
            out.extensions().get::<AuthPrincipalRef>().is_some(),
            "interceptor must preserve the authenticated principal"
        );
    }
}
