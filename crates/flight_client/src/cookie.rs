/*
Copyright 2026 The Spice.ai OSS Authors

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

use http::header::{COOKIE, SET_COOKIE};
use http::{HeaderMap, HeaderValue};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tower::{Layer, Service};

#[derive(Debug, Default)]
pub struct CookieStore {
    cookies: RwLock<HashMap<String, String>>,
}

impl CookieStore {
    #[must_use]
    pub fn new() -> Self {
        Self {
            cookies: RwLock::new(HashMap::new()),
        }
    }

    #[must_use]
    pub fn cookie_header_value(&self) -> Option<HeaderValue> {
        let cookies = self.cookies.read();
        if cookies.is_empty() {
            return None;
        }

        let mut header = String::new();
        for (index, (name, value)) in cookies.iter().enumerate() {
            if index > 0 {
                header.push_str("; ");
            }
            header.push_str(name);
            header.push('=');
            header.push_str(value);
        }

        HeaderValue::from_str(&header).ok()
    }

    pub fn update_from_headers(&self, headers: &HeaderMap) {
        let mut cookies = self.cookies.write();
        for value in headers.get_all(SET_COOKIE) {
            if let Ok(value) = value.to_str()
                && let Some((name, val)) = parse_set_cookie(value)
            {
                cookies.insert(name, val);
            }
        }
    }
}

fn parse_set_cookie(value: &str) -> Option<(String, String)> {
    let entry = value.split(';').next()?.trim();
    let (name, val) = entry.split_once('=')?;
    let name = name.trim();
    if name.is_empty() {
        return None;
    }
    let val = val.trim();
    Some((name.to_string(), val.to_string()))
}

#[derive(Debug, Clone)]
pub struct CookieLayer {
    store: Arc<CookieStore>,
}

impl CookieLayer {
    #[must_use]
    pub fn new(store: Arc<CookieStore>) -> Self {
        Self { store }
    }
}

impl<S> Layer<S> for CookieLayer {
    type Service = CookieService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        CookieService::new(inner, Arc::clone(&self.store))
    }
}

#[derive(Debug, Clone)]
pub struct CookieService<S> {
    inner: S,
    store: Arc<CookieStore>,
}

impl<S> CookieService<S> {
    #[must_use]
    pub fn new(inner: S, store: Arc<CookieStore>) -> Self {
        Self { inner, store }
    }

    #[must_use]
    pub fn cookie_store(&self) -> Arc<CookieStore> {
        Arc::clone(&self.store)
    }
}

impl<S, ReqBody, ResBody> Service<http::Request<ReqBody>> for CookieService<S>
where
    S: Service<http::Request<ReqBody>, Response = http::Response<ResBody>> + Send,
    S::Future: Send + 'static,
{
    type Response = http::Response<ResBody>;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut req: http::Request<ReqBody>) -> Self::Future {
        if let Some(cookie_header) = self.store.cookie_header_value() {
            req.headers_mut().insert(COOKIE, cookie_header);
        }

        let store = Arc::clone(&self.store);
        let fut = self.inner.call(req);
        Box::pin(async move {
            let response = fut.await?;
            store.update_from_headers(response.headers());
            Ok(response)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::HeaderMap;

    #[test]
    fn cookie_store_builds_header() {
        let store = CookieStore::new();
        let mut headers = HeaderMap::new();
        headers.append(
            SET_COOKIE,
            HeaderValue::from_static("AWSALB=abc123; Path=/"),
        );
        headers.append(
            SET_COOKIE,
            HeaderValue::from_static("AWSALBTG=def456; Path=/"),
        );
        store.update_from_headers(&headers);
        let header = store
            .cookie_header_value()
            .expect("cookie header should be present")
            .to_str()
            .expect("cookie header should be valid UTF-8")
            .to_string();
        assert!(header.contains("AWSALB=abc123"));
        assert!(header.contains("AWSALBTG=def456"));
    }

    #[test]
    fn cookie_store_overwrites_cookie() {
        let store = CookieStore::new();
        let mut headers = HeaderMap::new();
        headers.append(SET_COOKIE, HeaderValue::from_static("AWSALB=old; Path=/"));
        store.update_from_headers(&headers);

        let mut updated_headers = HeaderMap::new();
        updated_headers.append(SET_COOKIE, HeaderValue::from_static("AWSALB=new; Path=/"));
        store.update_from_headers(&updated_headers);

        let header = store
            .cookie_header_value()
            .expect("cookie header should be present")
            .to_str()
            .expect("cookie header should be valid UTF-8")
            .to_string();
        assert!(header.contains("AWSALB=new"));
        assert!(!header.contains("AWSALB=old"));
    }

    #[test]
    fn cookie_store_trims_cookie_parts() {
        let store = CookieStore::new();
        let mut headers = HeaderMap::new();
        headers.append(
            SET_COOKIE,
            HeaderValue::from_static(" name = value ; Path=/"),
        );
        store.update_from_headers(&headers);
        let header = store
            .cookie_header_value()
            .expect("cookie header should be present")
            .to_str()
            .expect("cookie header should be valid UTF-8")
            .to_string();
        assert!(header.contains("name=value"));
    }

    #[test]
    fn cookie_store_ignores_invalid_set_cookie() {
        let store = CookieStore::new();
        let mut headers = HeaderMap::new();
        headers.append(SET_COOKIE, HeaderValue::from_static("invalid"));
        store.update_from_headers(&headers);
        assert!(store.cookie_header_value().is_none());
    }
}
