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
use std::sync::Arc;

use super::{ExamineMailboxSnafu, FailedToConnectSnafu};

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

impl ImapAuthMode {
    #[must_use]
    pub fn new_oauth2(user: SecretString, access_token: SecretString) -> Self {
        Self::OAuth2 { user, access_token }
    }

    #[must_use]
    pub fn new_plain(username: SecretString, password: SecretString) -> Self {
        Self::Plain { username, password }
    }
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
        }
    }

    pub fn connect(&self) -> Result<Session<Box<dyn ImapConnection>>, super::Error> {
        let client = imap::ClientBuilder::new(Arc::clone(&self.host), self.port)
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
