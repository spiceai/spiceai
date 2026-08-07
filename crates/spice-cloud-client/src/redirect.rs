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

//! Redirect policy for HTTP clients that carry a credential.
//!
//! `reqwest`'s default policy follows up to ten redirects, and on a cross-origin hop it
//! sanitises only the standard credential headers — `Authorization`, `Cookie`, `Cookie2`,
//! `Proxy-Authorization` and `WWW-Authenticate`. That leaves two ways for a credential to
//! follow a `Location` off the origin it was minted for:
//!
//! - a custom header such as `X-API-Key` is not on that list, so it rides along; and
//! - a 307 or 308 preserves the method *and* the body, so a token-exchange `POST` replays
//!   its auth code or device code to whatever the new origin is.
//!
//! The second one is why stripping headers is not sufficient on its own, and why every
//! client in this workspace that sends a credential — in a header or in a body — should be
//! built with [`same_origin_redirect_policy`].

use reqwest::Url;

/// How many same-origin redirects to follow before refusing, matching the depth of
/// `reqwest`'s own default policy.
///
/// Compared the same way `reqwest` compares it: because the first entry of `previous()` is
/// the initial URL rather than a redirect, the limit is exceeded only once
/// `previous().len() > MAX_REDIRECTS`.
const MAX_REDIRECTS: usize = 10;

/// Whether two URLs share an origin, per the URL standard's definition.
///
/// Defers to `Url::origin` rather than comparing scheme/host/port by hand, which buys two
/// things. For `http` and `https` it is the tuple origin — scheme, host and *effective*
/// port, so `https://host` and `https://host:443` match, and `Url` has already
/// canonicalised case, IDN and IPv4/IPv6 spellings by then. For every other scheme
/// (`file:`, `blob:`, anything custom) it is an opaque origin, which is unique per call and
/// so never compares equal — meaning such a hop is always refused rather than being
/// silently treated as same-origin because two URLs happen to share a scheme and have no
/// host.
fn is_same_origin(previous: &Url, next: &Url) -> bool {
    previous.origin() == next.origin()
}

/// A redirect policy that follows same-origin redirects and refuses to leave the origin.
///
/// Refusing means `Attempt::stop`, so the 3xx comes back to the caller as an ordinary
/// response. An unexpected redirect stays diagnosable that way, rather than surfacing as a
/// transport error indistinguishable from a network fault.
///
/// `Policy::custom` does not bound the redirect chain for you, so this counts hops too.
#[must_use]
pub fn same_origin_redirect_policy() -> reqwest::redirect::Policy {
    reqwest::redirect::Policy::custom(|attempt| {
        // Both predicates are resolved before acting, because `previous()` borrows the
        // attempt while `follow()` and `stop()` consume it.
        let stays_on_origin = attempt
            .previous()
            .last()
            .is_some_and(|previous| is_same_origin(previous, attempt.url()));
        let within_limit = attempt.previous().len() <= MAX_REDIRECTS;

        if stays_on_origin && within_limit {
            attempt.follow()
        } else {
            attempt.stop()
        }
    })
}

#[cfg(test)]
mod tests {
    use super::{MAX_REDIRECTS, is_same_origin, same_origin_redirect_policy};
    use reqwest::{StatusCode, Url};
    use std::io::{Read, Write};
    use std::net::{SocketAddr, TcpListener, TcpStream};
    use std::sync::{Arc, Mutex};
    use std::thread::JoinHandle;

    fn url(value: &str) -> Url {
        Url::parse(value).expect("test URL should parse")
    }

    #[test]
    fn test_same_origin_for_identical_origin_with_different_path() {
        let base = url("https://api.spice.ai/auth/token/exchange");
        let next = url("https://api.spice.ai/auth/token");

        assert!(is_same_origin(&base, &next));
    }

    #[test]
    fn test_same_origin_treats_implicit_and_explicit_default_ports_alike() {
        let implicit_https = url("https://host/a");
        let explicit_https = url("https://host:443/b");
        let implicit_http = url("http://host/a");
        let explicit_http = url("http://host:80/b");

        assert!(is_same_origin(&implicit_https, &explicit_https));
        assert!(is_same_origin(&implicit_http, &explicit_http));
    }

    /// The cases a credential must not follow: a different host is the exfiltrating one,
    /// and a different scheme or port is still a different origin.
    #[test]
    fn test_not_same_origin_across_host_scheme_or_port() {
        let base = url("https://api.spice.ai/auth/token/exchange");
        let other_host = url("https://evil.example/collect");
        let other_scheme = url("http://api.spice.ai/auth");
        let other_port = url("https://api.spice.ai:8443/auth");
        // A subdomain is a different host, so it is a different origin.
        let subdomain = url("https://evil.api.spice.ai/collect");

        assert!(!is_same_origin(&base, &other_host));
        assert!(!is_same_origin(&base, &other_scheme));
        assert!(!is_same_origin(&base, &other_port));
        assert!(!is_same_origin(&base, &subdomain));
    }

    /// A userinfo prefix must not be mistaken for the host: `https://api.spice.ai@evil`
    /// has host `evil`, and reading it as `api.spice.ai` would let the credential out.
    #[test]
    fn test_not_same_origin_when_userinfo_spoofs_the_host() {
        let base = url("https://api.spice.ai/auth/token/exchange");
        let spoofed = url("https://api.spice.ai@evil.example/collect");

        assert_eq!(spoofed.host_str(), Some("evil.example"));
        assert!(!is_same_origin(&base, &spoofed));
    }

    /// A scheme with no host has an opaque origin, which is unique per call and so never
    /// equal. Two `file:` URLs sharing a scheme must not be read as one origin just
    /// because both have `host_str() == None`.
    #[test]
    fn test_not_same_origin_for_hostless_schemes() {
        let one = url("file:///tmp/a");
        let two = url("file:///tmp/b");

        assert_eq!(one.host_str(), None);
        assert_eq!(two.host_str(), None);
        assert!(!is_same_origin(&one, &two));
        // Not even against itself, which is what makes the refusal unconditional.
        assert!(!is_same_origin(&one, &one));
    }

    /// The credential the policy exists to protect. A custom header is not one of the five
    /// `reqwest` sanitises on a cross-origin hop, so nothing but the policy keeps it on
    /// the origin it was minted for.
    const API_KEY: &str = "test-api-key-value";

    /// A one-connection-at-a-time HTTP/1.1 stub, enough to answer a redirect script.
    ///
    /// The workspace `tokio` carries no `net` feature, so this is a blocking `std::net`
    /// listener on its own thread. Every response closes the connection, so each hop
    /// arrives as its own accept and the recorded order is the request order.
    struct Stub {
        addr: SocketAddr,
        requests: Arc<Mutex<Vec<String>>>,
        worker: Option<JoinHandle<()>>,
    }

    impl Stub {
        /// Answer each request with `respond(nth)`, where `nth` is 1-based, until dropped.
        fn serve(respond: impl Fn(usize) -> String + Send + 'static) -> Self {
            let listener =
                TcpListener::bind("127.0.0.1:0").expect("stub should bind a loopback port");
            let addr = listener
                .local_addr()
                .expect("stub should report its address");
            let requests = Arc::new(Mutex::new(Vec::new()));

            let recorded = Arc::clone(&requests);
            let worker = std::thread::spawn(move || {
                for stream in listener.incoming() {
                    let Ok(mut stream) = stream else { break };
                    let head = read_request_head(&mut stream);
                    // The drop poke connects and sends nothing; that is the stop signal.
                    if head.is_empty() {
                        break;
                    }

                    let nth = {
                        let mut recorded =
                            recorded.lock().expect("request log should not be poisoned");
                        recorded.push(head);
                        recorded.len()
                    };

                    let _ = stream.write_all(respond(nth).as_bytes());
                    let _ = stream.flush();
                }
            });

            Self {
                addr,
                requests,
                worker: Some(worker),
            }
        }

        fn url(&self, path: &str) -> String {
            let addr = self.addr;
            format!("http://{addr}{path}")
        }

        /// The request heads seen so far, in order, lower-cased so an assertion does not
        /// depend on how the client happens to case a header name on the wire.
        fn requests(&self) -> Vec<String> {
            self.requests
                .lock()
                .expect("request log should not be poisoned")
                .clone()
        }
    }

    impl Drop for Stub {
        fn drop(&mut self) {
            // Unblock the accept the worker is parked in, then let it finish.
            let _ = TcpStream::connect(self.addr);
            if let Some(worker) = self.worker.take() {
                let _ = worker.join();
            }
        }
    }

    /// Read one request's head, stopping at the blank line that ends it. These requests
    /// carry no body, so nothing after it needs consuming.
    fn read_request_head(stream: &mut TcpStream) -> String {
        /// Far above any head these tests produce, so reaching it means the peer is not
        /// sending one and the read should stop rather than accumulate without a bound.
        const MAX_HEAD_BYTES: usize = 16 * 1024;

        let mut head = Vec::new();
        let mut chunk = [0_u8; 256];

        loop {
            match stream.read(&mut chunk) {
                Ok(0) | Err(_) => break,
                Ok(read) => {
                    head.extend_from_slice(&chunk[..read]);
                    if head.len() >= MAX_HEAD_BYTES
                        || head.windows(4).any(|window| window == b"\r\n\r\n")
                    {
                        break;
                    }
                }
            }
        }

        String::from_utf8_lossy(&head).to_lowercase()
    }

    fn redirect_to(location: &str) -> String {
        format!(
            "HTTP/1.1 307 Temporary Redirect\r\nLocation: {location}\r\n\
             Content-Length: 0\r\nConnection: close\r\n\r\n"
        )
    }

    fn ok_with(body: &str) -> String {
        let length = body.len();
        format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {length}\r\n\
             Connection: close\r\n\r\n{body}"
        )
    }

    /// A client that differs from the default in nothing but the policy under test and a
    /// deadline.
    ///
    /// The deadline is what a lost hop bound looks like from here: a policy that stopped
    /// counting would follow this stub's redirects forever, so without it the hop-limit
    /// test would hang instead of failing. Ten seconds is far above what a loopback
    /// exchange of eleven requests needs, so it cannot make a working policy flaky.
    fn client_under_test() -> reqwest::Client {
        reqwest::Client::builder()
            .redirect(same_origin_redirect_policy())
            .timeout(std::time::Duration::from_secs(10))
            .build()
            .expect("test client should build")
    }

    /// The case the policy exists for: a `Location` pointing off origin is not followed at
    /// all, so the credential header never reaches the other origin. The 3xx comes back to
    /// the caller as an ordinary response, which is what keeps it diagnosable.
    #[tokio::test]
    async fn test_a_cross_origin_redirect_is_refused_and_the_credential_stays_put() {
        let elsewhere = Stub::serve(|_| ok_with("collected"));
        let collection_url = elsewhere.url("/collect");
        let origin = Stub::serve(move |_| redirect_to(&collection_url));

        let response = client_under_test()
            .get(origin.url("/auth/token/exchange"))
            .header("x-api-key", API_KEY)
            .send()
            .await
            .expect("a refused redirect should come back as a response, not a transport error");

        assert_eq!(response.status(), StatusCode::TEMPORARY_REDIRECT);
        assert_eq!(
            origin.requests().len(),
            1,
            "the request should be made once and not followed anywhere"
        );
        assert!(
            elsewhere.requests().is_empty(),
            "the other origin must not be contacted at all, so the credential cannot reach it"
        );
    }

    /// The other half: a hop that stays on origin is still followed, credential included.
    /// A policy that refused everything would pass the test above and break every real
    /// redirect, so this is what keeps the refusal specific.
    #[tokio::test]
    async fn test_a_same_origin_redirect_is_followed_and_replays_the_credential() {
        let origin = Stub::serve(|nth| {
            if nth == 1 {
                redirect_to("/auth/token")
            } else {
                ok_with("exchanged")
            }
        });

        let response = client_under_test()
            .get(origin.url("/auth/token/exchange"))
            .header("x-api-key", API_KEY)
            .send()
            .await
            .expect("a same-origin redirect should be followed");

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.text().await.expect("the body should read back"),
            "exchanged"
        );

        let requests = origin.requests();
        assert_eq!(requests.len(), 2, "the redirect should be followed once");
        assert!(requests[0].contains("get /auth/token/exchange"));
        assert!(requests[1].contains("get /auth/token"));
        for request in &requests {
            assert!(
                request.contains(&format!("x-api-key: {API_KEY}")),
                "a same-origin hop keeps the credential, or the redirect is useless"
            );
        }
    }

    /// `Policy::custom` does not bound the chain for you, so the policy counts hops
    /// itself. A server that redirects on origin forever must still terminate: the
    /// initial request plus `MAX_REDIRECTS` follows, then the 3xx is returned.
    #[tokio::test]
    async fn test_a_same_origin_redirect_chain_stops_at_the_hop_limit() {
        let origin = Stub::serve(|nth| redirect_to(&format!("/hop-{nth}")));

        let response = client_under_test()
            .get(origin.url("/hop-0"))
            .send()
            .await
            .expect(
                "a bounded chain should come back as a response; a timeout here means the \
                 hop limit is no longer enforced",
            );

        assert_eq!(response.status(), StatusCode::TEMPORARY_REDIRECT);
        assert_eq!(
            origin.requests().len(),
            MAX_REDIRECTS + 1,
            "the chain should stop after {MAX_REDIRECTS} follows rather than run forever"
        );
    }
}
