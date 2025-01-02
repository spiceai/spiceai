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

use logos::Logos;

pub struct ReplacementMatch {
    pub store_name: String,
    pub key: String,
    pub span: std::ops::Range<usize>,
}

#[derive(Logos, Debug, PartialEq)]
#[logos(skip r"[ \t\n\f]+")] // Ignore whitespace between tokens
enum SecretReplacementToken {
    #[token("${")]
    Start,

    #[regex(r"[a-zA-Z][a-zA-Z0-9_-]*", |lex| lex.slice().to_owned())]
    Identifier(String),

    #[token(":")]
    Colon,

    #[token("}")]
    End,
}

pub struct SecretReplacementMatcher<'a> {
    lexer: logos::Lexer<'a, SecretReplacementToken>,
}

impl<'a> SecretReplacementMatcher<'a> {
    pub fn new(input: &'a str) -> Self {
        Self {
            lexer: SecretReplacementToken::lexer(input),
        }
    }
}

impl Iterator for SecretReplacementMatcher<'_> {
    type Item = ReplacementMatch;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(token_result) = self.lexer.next() {
            // If this couldn't be parsed as an expected token, skip it.
            let Ok(token) = token_result else {
                continue;
            };

            let SecretReplacementToken::Start = token else {
                continue;
            };

            let start_span = self.lexer.span().start;

            let SecretReplacementToken::Identifier(store_name) = self.lexer.next()?.ok()? else {
                continue;
            };

            if self.lexer.next()?.ok()? != SecretReplacementToken::Colon {
                continue;
            }

            let SecretReplacementToken::Identifier(key) = self.lexer.next()?.ok()? else {
                continue;
            };

            if self.lexer.next()?.ok()? != SecretReplacementToken::End {
                continue;
            }

            let end_span = self.lexer.span().end;
            return Some(ReplacementMatch {
                store_name,
                key,
                span: start_span..end_span,
            });
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_secret_lexer_basic() {
        let input = "Hello ${ secret:my_secret } world";
        let lexer = SecretReplacementMatcher::new(input);

        let matches: Vec<ReplacementMatch> = lexer.collect();
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].store_name, "secret");
        assert_eq!(matches[0].key, "my_secret");
        assert_eq!(matches[0].span, 6..27);
    }

    #[test]
    fn test_secret_lexer_whitespace_variations() {
        let inputs = vec![
            ("Hello ${secret:my_secret} world", 25),
            ("Hello ${ secret: my_secret } world", 28),
            ("Hello ${  secret:my_secret  } world", 29),
            ("Hello ${secret :my_secret} world", 26),
        ];

        for (input, expected_end) in inputs {
            let lexer = SecretReplacementMatcher::new(input);

            let matches: Vec<ReplacementMatch> = lexer.collect();
            assert_eq!(matches.len(), 1);
            assert_eq!(matches[0].store_name, "secret");
            assert_eq!(matches[0].key, "my_secret");
            assert!(matches[0].span.start == 6 && matches[0].span.end == expected_end);
        }
    }

    #[test]
    fn test_secret_lexer_multiple_matches() {
        let input = "Start ${ secret:my_secret1 } middle ${ secret:my_secret2 } end";
        let lexer = SecretReplacementMatcher::new(input);

        let matches: Vec<ReplacementMatch> = lexer.collect();
        assert_eq!(matches.len(), 2);
        assert_eq!(matches[0].store_name, "secret");
        assert_eq!(matches[0].key, "my_secret1");
        assert_eq!(matches[0].span, 6..28);

        assert_eq!(matches[1].store_name, "secret");
        assert_eq!(matches[1].key, "my_secret2");
        assert_eq!(matches[1].span, 36..58);
    }

    #[test]
    fn test_secret_lexer_invalid_formats() {
        let inputs = vec![
            "Hello ${secret:} world",              // Missing key
            "Hello ${:my_secret} world",           // Missing store name
            "Hello ${ secret my_secret } world",   // Missing colon
            "Hello ${ secret: my secret } world",  // Invalid key format
            "Hello ${secret} world",               // Missing colon and key
            "Hello ${{ secret:my_secret }} world", // Invalid style
        ];

        for input in inputs {
            let lexer = SecretReplacementMatcher::new(input);

            let matches: Vec<ReplacementMatch> = lexer.collect();
            assert_eq!(matches.len(), 0);
        }
    }

    #[test]
    fn test_secret_lexer_no_matches() {
        let input = "Hello world";
        let lexer = SecretReplacementMatcher::new(input);

        let matches: Vec<ReplacementMatch> = lexer.collect();
        assert_eq!(matches.len(), 0);
    }

    #[test]
    fn test_mysql_connection_string() {
        let input = "mysql://${env:USER}:${env:PASSWORD}@localhost:3306/mysql_db";
        let lexer = SecretReplacementMatcher::new(input);

        let matches: Vec<ReplacementMatch> = lexer.collect();
        assert_eq!(matches.len(), 2);
        assert_eq!(matches[0].store_name, "env");
        assert_eq!(matches[0].key, "USER");
        assert_eq!(matches[0].span, 8..19);

        assert_eq!(matches[1].store_name, "env");
        assert_eq!(matches[1].key, "PASSWORD");
        assert_eq!(matches[1].span, 20..35);
    }
}
