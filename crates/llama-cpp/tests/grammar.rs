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

//! Grammars and JSON schemas whose repetition bounds are inverted fail instead of parsing
//! without end (CVE-2026-137057).

use std::sync::mpsc;
use std::time::Duration;

/// llama.cpp parses these grammars in milliseconds; one still running after this hangs.
const LIMIT: Duration = Duration::from_secs(30);

/// Runs `f` on its own thread and returns its result, failing the test if it runs past
/// `LIMIT`. A hung parse is left spinning on its thread; the test binary still exits.
fn within_limit<T: Send + 'static>(what: &str, f: impl FnOnce() -> T + Send + 'static) -> T {
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(f());
    });
    rx.recv_timeout(LIMIT)
        .unwrap_or_else(|_| panic!("{what} did not finish within {LIMIT:?}"))
}

fn check_grammar(grammar: &'static str) -> llama_cpp::Result<()> {
    within_limit(grammar, move || llama_cpp::check_grammar(grammar))
}

fn schema_grammar(schema: &'static str) -> llama_cpp::Result<String> {
    within_limit(schema, move || llama_cpp::json_schema_to_grammar(schema))
}

#[test]
fn inverted_repetition_fails_to_parse() {
    for grammar in [
        "root ::= \"x\"{3,1}\n",
        "root ::= (\"ab\" [c-d]){10,2}\n",
        "root ::= item{2,0}\nitem ::= [a-z]\n",
    ] {
        let err = check_grammar(grammar).expect_err(grammar);
        let message = err.to_string();
        assert!(
            message.contains("incorrect number of repetitions"),
            "{grammar}: {message}"
        );
    }
}

#[test]
fn valid_repetitions_parse() {
    for grammar in [
        "root ::= \"x\"{1,3}\n",
        "root ::= \"x\"{3,3}\n",
        "root ::= \"x\"{3}\n",
        "root ::= \"x\"{2,}\n",
        "# a comment first\nroot ::= \"x\"{0,5000}\n",
    ] {
        check_grammar(grammar).unwrap_or_else(|e| panic!("{grammar}: {e}"));
    }
}

#[test]
fn malformed_grammars_fail() {
    for grammar in ["", "root ::= missing\n", "root ::= \"x\"{,}\n", "root ::= <[42]>\n"] {
        check_grammar(grammar).expect_err(grammar);
    }
}

#[test]
fn schema_with_inverted_bounds_is_refused() {
    for (schema, keyword) in [
        (r#"{"type":"array","items":{"type":"integer"},"minItems":3,"maxItems":1}"#, "minItems 3 above maxItems 1"),
        (r#"{"type":"string","minLength":5,"maxLength":2}"#, "minLength 5 above maxLength 2"),
        (r#"{"type":"object","minProperties":4,"maxProperties":1}"#, "minProperties 4 above maxProperties 1"),
        (r#"{"type":"array","contains":{"type":"integer"},"minContains":3,"maxContains":2}"#, "minContains 3 above maxContains 2"),
        (r#"{"type":"integer","minimum":10,"maximum":1}"#, "minimum 10 above maximum 1"),
        (
            r#"{"type":"object","properties":{"tags":{"type":"array","items":{"type":"string"},"minItems":2,"maxItems":0}}}"#,
            "'/properties/tags' has minItems 2 above maxItems 0",
        ),
        (
            r#"{"anyOf":[{"type":"null"},{"type":"string","minLength":9,"maxLength":3}]}"#,
            "'/anyOf/1' has minLength 9 above maxLength 3",
        ),
    ] {
        let err = schema_grammar(schema).expect_err(schema);
        let message = err.to_string();
        assert!(message.contains(keyword), "{schema}: {message}");
        assert!(message.contains("which no value satisfies"), "{schema}: {message}");
    }
}

#[test]
fn schema_pattern_with_inverted_repetition_is_refused() {
    let err = schema_grammar(r#"{"type":"string","pattern":"^a{3,1}$"}"#)
        .expect_err("an inverted pattern repetition");
    assert!(
        err.to_string().contains("incorrect number of repetitions"),
        "{err}"
    );
}

#[test]
fn schema_with_consistent_bounds_converts() {
    for schema in [
        r#"{"type":"array","items":{"type":"integer"},"minItems":1,"maxItems":3}"#,
        r#"{"type":"array","items":{"type":"integer"},"minItems":2,"maxItems":2}"#,
        r#"{"type":"string","minLength":0,"maxLength":4}"#,
        r#"{"type":"integer","minimum":-5,"maximum":5}"#,
        r#"{"type":"string","pattern":"^a{1,3}$"}"#,
        // Values under `const`, `enum`, `default` and `examples` are data, not schemas.
        r#"{"type":"object","properties":{"limits":{"type":"object"}},"default":{"limits":{"minItems":5,"maxItems":1}}}"#,
    ] {
        let grammar = schema_grammar(schema).unwrap_or_else(|e| panic!("{schema}: {e}"));
        llama_cpp::check_grammar(&grammar).unwrap_or_else(|e| panic!("{schema}: {e}"));
    }
}
