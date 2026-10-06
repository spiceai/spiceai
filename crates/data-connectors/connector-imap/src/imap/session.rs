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

use imap::{ImapConnection, Session};
use secrecy::{ExposeSecret, SecretString};
use snafu::prelude::*;
use std::{str::FromStr, sync::Arc};

use super::{ExamineMailboxSnafu, FailedToConnectSnafu};

#[derive(Debug, Copy, Clone, Default)]
pub enum ImapSSLMode {
    #[default]
    Tls,
    StartTls,
    Disabled,
    Auto,
}

impl FromStr for ImapSSLMode {
    type Err = super::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "tls" => Ok(Self::Tls),
            "starttls" => Ok(Self::StartTls),
            "disabled" => Ok(Self::Disabled),
            "auto" => Ok(Self::Auto),
            _ => Err(super::Error::InvalidSSLMode {
                ssl_mode: s.to_string(),
            }),
        }
    }
}

impl From<ImapSSLMode> for imap::ConnectionMode {
    fn from(mode: ImapSSLMode) -> Self {
        match mode {
            ImapSSLMode::Tls => imap::ConnectionMode::Tls,
            ImapSSLMode::StartTls => imap::ConnectionMode::StartTls,
            ImapSSLMode::Disabled => imap::ConnectionMode::Plaintext,
            ImapSSLMode::Auto => imap::ConnectionMode::Auto,
        }
    }
}

#[derive(Debug, Clone)]
pub enum ImapAuthModeParameter {
    OAuth,
    Plain,
}

impl FromStr for ImapAuthModeParameter {
    type Err = super::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "oauth" => Ok(Self::OAuth),
            "plain" => Ok(Self::Plain),
            _ => Err(super::Error::InvalidAuthMode {
                auth_mode: s.to_string(),
            }),
        }
    }
}

impl ImapAuthModeParameter {
    #[must_use]
    pub fn build(&self, username: SecretString, password: SecretString) -> ImapAuthMode {
        match self {
            Self::OAuth => ImapAuthMode::OAuth2 {
                user: username,
                access_token: password,
            },
            Self::Plain => ImapAuthMode::Plain { username, password },
        }
    }
}

#[derive(Debug, Clone)]
pub enum ImapAuthMode {
    OAuth2 {
        user: SecretString,
        access_token: SecretString,
    },
    Plain {
        username: SecretString,
        password: SecretString,
    },
}

impl imap::Authenticator for ImapAuthMode {
    type Response = String;
    fn process(&self, _data: &[u8]) -> Self::Response {
        match self {
            Self::OAuth2 { user, access_token } => {
                format!(
                    "user={}\x01auth=Bearer {}\x01\x01",
                    user.expose_secret(),
                    access_token.expose_secret()
                )
            }
            Self::Plain { .. } => {
                unimplemented!("Use `session.login()` instead");
            }
        }
    }
}

/// ``imap::Client`` cannot be sent between threads safely.
/// Because of this, we need a different wrapper that we can send between threads.
/// This wrapper builds new connections, and authenticates with them before returning the session.
/// This lets us close the session at the end of every query, which is good IMAP practice.
#[derive(Debug, Clone)]
pub struct ImapSession {
    auth_mode: ImapAuthMode,
    host: Arc<str>,
    port: u16,
    mailbox: Arc<str>,
    ssl_mode: ImapSSLMode,
}

impl ImapSession {
    #[must_use]
    pub fn mailbox(&self) -> Arc<str> {
        Arc::clone(&self.mailbox)
    }

    #[must_use]
    pub fn new(auth_mode: ImapAuthMode, host: Arc<str>, port: u16, mailbox: Arc<str>) -> Self {
        Self {
            auth_mode,
            host,
            port,
            mailbox,
            ssl_mode: ImapSSLMode::default(),
        }
    }

    #[must_use]
    pub fn with_ssl_mode(mut self, ssl_mode: ImapSSLMode) -> Self {
        self.ssl_mode = ssl_mode;
        self
    }

    pub fn connect(&self) -> Result<Session<Box<dyn ImapConnection>>, super::Error> {
        let client = imap::ClientBuilder::new(Arc::clone(&self.host), self.port)
            .mode(self.ssl_mode.into())
            .connect()
            .context(FailedToConnectSnafu)?;

        let mut session = match &self.auth_mode {
            ImapAuthMode::OAuth2 { .. } => client.authenticate("XOAUTH2", &self.auth_mode),
            ImapAuthMode::Plain { username, password } => {
                client.login(username.expose_secret(), password.expose_secret())
            }
        }
        .map_err(|source| super::Error::FailedToLogin { source: source.0 })?;

        session
            .examine(Arc::clone(&self.mailbox))
            .context(ExamineMailboxSnafu)?;

        Ok(session)
    }
}
