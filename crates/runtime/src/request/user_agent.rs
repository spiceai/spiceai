/*
Copyright 2024 The Spice.ai OSS Authors

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
    fmt,
    ops::Deref,
    sync::{Arc, LazyLock},
};

use http::{header::USER_AGENT, HeaderMap};

pub static RUNTIME_VERSION: &str = env!("CARGO_PKG_VERSION");
pub static RUNTIME_NAME: &str = "spiced";
pub static RUNTIME_SYSTEM: LazyLock<Arc<str>> =
    LazyLock::new(|| Arc::from(get_runtime_os_string()));

/// `UserAgent` represents the client making a request to Spice and the platform Spice is running on.
///
/// It follows the format:
/// `<client_name>/<client_version> (<client_system>) <platform_name>/<platform_version> (<platform_system>)`
#[derive(Debug, Eq, PartialEq, Clone, Default)]
pub enum UserAgent {
    #[default]
    Absent,
    Raw(Raw),
    Parsed(Parsed),
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct Raw(Arc<str>);

impl Deref for Raw {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// TODO: Parsed should keep any remaining product identifiers other than the primary.
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct Parsed {
    pub client_name: Arc<str>,
    pub client_version: Arc<str>,
    pub client_system: Option<Arc<str>>,
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct Platform {
    pub name: Arc<str>,
    pub version: Arc<str>,
    pub system: Option<Arc<str>>,
}

impl UserAgent {
    #[must_use]
    pub fn from_headers(headers: &HeaderMap) -> Self {
        let user_agent = match headers.get(USER_AGENT) {
            Some(user_agent) => user_agent,
            None => return Self::Absent,
        };
        let user_agent_str = match user_agent.to_str() {
            Ok(user_agent_str) => user_agent_str,
            Err(_) => return Self::Absent,
        };
        match Parsed::try_from(user_agent_str) {
            Some(parsed) => Self::Parsed(parsed),
            None => Self::Raw(Raw(Arc::from(user_agent_str))),
        }
    }
}

impl fmt::Display for UserAgent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // First write the client-provided user agent string
        match self {
            UserAgent::Absent => (),
            UserAgent::Raw(raw) => write!(f, "{} ", raw.0)?,
            UserAgent::Parsed(parsed) => {
                write!(f, "{}/{} ", parsed.client_name, parsed.client_version)?;
                if let Some(client_system) = &parsed.client_system {
                    write!(f, "({client_system}) ")?;
                }
            }
        }

        // Then write the runtime part of the user agent string
        write!(
            f,
            "{RUNTIME_NAME}/{RUNTIME_VERSION} ({})",
            RUNTIME_SYSTEM.as_ref()
        )?;

        Ok(())
    }
}

impl Parsed {
    fn try_from(s: &str) -> Option<Self> {
        let mut parts = s.split_whitespace();

        // Get the first part which should be "name/version"
        let name_version = parts.next()?;

        let (name, version) = name_version.split_once('/')?;

        // Check for optional system info in parentheses
        let mut system = None;
        if let Some(next_part) = parts.next() {
            if next_part.starts_with('(') && next_part.ends_with(')') {
                // Strip the parentheses and convert to Arc<str>
                system = Some(Arc::from(&next_part[1..next_part.len() - 1]));
            }
        }

        Some(Self {
            client_name: Arc::from(name),
            client_version: Arc::from(version),
            client_system: system,
        })
    }
}

#[must_use]
fn os_type() -> &'static str {
    let os_type = std::env::consts::OS;
    match os_type {
        "" => "unknown",
        "macos" => "Darwin",
        "linux" => "Linux",
        "windows" => "Windows",
        "ios" => "iOS",
        "android" => "Android",
        "freebsd" => "FreeBSD",
        "dragonfly" => "DragonFlyBSD",
        "netbsd" => "NetBSD",
        "openbsd" => "OpenBSD",
        "solaris" => "Solaris",
        _ => os_type,
    }
}

#[must_use]
fn os_arch() -> &'static str {
    let os_arch = std::env::consts::ARCH;
    match os_arch {
        "" => "unknown",
        "x86" => "i386",
        _ => os_arch,
    }
}

pub type GenericError = Box<dyn std::error::Error + Send + Sync>;

#[cfg(target_family = "unix")]
fn get_os_version_internal() -> Result<String, GenericError> {
    // call uname -r to get release text
    use std::process::Command;
    let output = Command::new("uname").arg("-r").output()?;
    let version = String::from_utf8(output.stdout)?;

    Ok(version)
}

#[cfg(target_family = "windows")]
fn get_os_version_internal() -> Result<String, GenericError> {
    use winver::WindowsVersion;
    if let Some(version) = WindowsVersion::detect() {
        Ok(version.to_string())
    } else {
        Ok("unknown".to_string())
    }
}

#[must_use]
fn get_runtime_os_string() -> String {
    let os_type = os_type();
    let os_version = get_os_version_internal()
        .unwrap_or_else(|_| "unknown".to_string())
        .trim()
        .to_string();
    let os_arch = os_arch();

    format!("{os_type}/{os_version} {os_arch}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::HeaderValue;

    #[test]
    fn test_user_agent_from_headers_absent() {
        let headers = HeaderMap::new();
        assert_eq!(UserAgent::from_headers(&headers), UserAgent::Absent);
    }

    #[test]
    fn test_user_agent_from_headers_invalid_utf8() {
        let mut headers = HeaderMap::new();
        headers.insert(
            USER_AGENT,
            HeaderValue::from_bytes(&[0xFF]).expect("Valid header value"),
        );
        assert_eq!(UserAgent::from_headers(&headers), UserAgent::Absent);
    }

    #[test]
    fn test_user_agent_from_headers_raw() {
        let mut headers = HeaderMap::new();
        headers.insert(
            USER_AGENT,
            HeaderValue::from_static("Some Random User Agent String"),
        );
        match UserAgent::from_headers(&headers) {
            UserAgent::Raw(raw) => assert_eq!(&*raw, "Some Random User Agent String"),
            other => panic!("Expected Raw, got {:?}", other),
        }
    }

    #[test]
    fn test_user_agent_from_headers_parsed() {
        let mut headers = HeaderMap::new();
        headers.insert(
            USER_AGENT,
            HeaderValue::from_static("spicecli/1.0.0 (Darwin/21.6.0 arm64)"),
        );
        match UserAgent::from_headers(&headers) {
            UserAgent::Parsed(parsed) => {
                assert_eq!(&*parsed.client_name, "spicecli");
                assert_eq!(&*parsed.client_version, "1.0.0");
                assert_eq!(
                    &*parsed.client_system.expect("Should have system info"),
                    "Darwin/21.6.0 arm64"
                );
            }
            other => panic!("Expected Parsed, got {:?}", other),
        }
    }

    #[test]
    fn test_parsed_try_from() {
        // Test valid format without system info
        let result = Parsed::try_from("name/1.0.0");
        assert!(result.is_some());
        let parsed = result.expect("Should parse");
        assert_eq!(&*parsed.client_name, "name");
        assert_eq!(&*parsed.client_version, "1.0.0");
        assert!(parsed.client_system.is_none());

        // Test valid format with system info
        let result = Parsed::try_from("name/1.0.0 (system-info)");
        assert!(result.is_some());
        let parsed = result.expect("Should parse");
        assert_eq!(&*parsed.client_name, "name");
        assert_eq!(&*parsed.client_version, "1.0.0");
        assert_eq!(
            &*parsed.client_system.expect("Should have system info"),
            "system-info"
        );

        // Test invalid formats
        assert!(Parsed::try_from("").is_none());
        assert!(Parsed::try_from("name").is_none());
        assert!(Parsed::try_from("name/").is_none());
        assert!(Parsed::try_from("/version").is_none());
        assert!(Parsed::try_from("name/version extra stuff").is_none());
    }

    #[test]
    fn test_user_agent_display() {
        // Test Absent
        assert_eq!(
            UserAgent::Absent.to_string(),
            format!("{PLATFORM_NAME}/{PLATFORM_VERSION} ({})", *PLATFORM_SYSTEM)
        );

        // Test Raw
        let raw = UserAgent::Raw(Arc::from("raw-agent"));
        assert_eq!(
            raw.to_string(),
            format!(
                "raw-agent {PLATFORM_NAME}/{PLATFORM_VERSION} ({})",
                *PLATFORM_SYSTEM
            )
        );

        // Test Parsed without system info
        let parsed = UserAgent::Parsed(Parsed {
            client_name: Arc::from("client"),
            client_version: Arc::from("1.0"),
            client_system: None,
        });
        assert_eq!(
            parsed.to_string(),
            format!(
                "client/1.0 {PLATFORM_NAME}/{PLATFORM_VERSION} ({})",
                *PLATFORM_SYSTEM
            )
        );

        // Test Parsed with system info
        let parsed = UserAgent::Parsed(Parsed {
            client_name: Arc::from("client"),
            client_version: Arc::from("1.0"),
            client_system: Some(Arc::from("test-system")),
        });
        assert_eq!(
            parsed.to_string(),
            format!(
                "client/1.0 (test-system) {PLATFORM_NAME}/{PLATFORM_VERSION} ({})",
                *PLATFORM_SYSTEM
            )
        );
    }

    #[test]
    fn test_os_type() {
        let result = os_type();
        assert!(!result.is_empty());
        assert_ne!(result, "unknown");
    }

    #[test]
    fn test_os_arch() {
        let result = os_arch();
        assert!(!result.is_empty());
        assert_ne!(result, "unknown");
    }

    #[test]
    fn test_get_platform_os_string() {
        let result = get_platform_os_string();
        assert!(!result.is_empty());

        // Should contain OS type
        assert!(result.contains(os_type()));

        // Should contain architecture
        assert!(result.contains(os_arch()));

        // Should have expected format: "{os_type}/{os_version} {os_arch}"
        let parts: Vec<&str> = result.split_whitespace().collect();
        assert_eq!(parts.len(), 2);
        assert!(parts[0].contains('/'));
    }
}
