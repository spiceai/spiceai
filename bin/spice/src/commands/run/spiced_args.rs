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

//! Which Spicepod `spiced` loads, read from the arguments `spice run` passes
//! it the way `spiced` itself parses them.
//!
//! `spiced` takes the Spicepod as its one positional argument, anywhere among
//! its options, so finding it means knowing which options take a value: in
//! `--tls-enabled false production.yaml` the Spicepod is `production.yaml`,
//! not `false`. [`SPICED_OPTIONS`] lists every option `spiced` accepts, and
//! `spiced`'s tests check that list and [`runtime_spicepod`] against its own
//! parser.

/// Whether a `spiced` option takes a value.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Arity {
    /// `--very-verbose`, `-v`.
    Flag,
    /// `--metrics 0.0.0.0:9090` or `--metrics=0.0.0.0:9090`.
    Value,
}

/// One option `spiced` accepts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SpicedOption {
    /// The long name, without `--`.
    pub long: &'static str,
    pub short: Option<char>,
    pub arity: Arity,
}

const fn flag(long: &'static str) -> SpicedOption {
    SpicedOption {
        long,
        short: None,
        arity: Arity::Flag,
    }
}

const fn value(long: &'static str) -> SpicedOption {
    SpicedOption {
        long,
        short: None,
        arity: Arity::Value,
    }
}

/// Every option `spiced` accepts.
pub const SPICED_OPTIONS: &[SpicedOption] = &[
    SpicedOption {
        long: "help",
        short: Some('h'),
        arity: Arity::Flag,
    },
    flag("version"),
    flag("repl"),
    SpicedOption {
        long: "verbose",
        short: Some('v'),
        arity: Arity::Flag,
    },
    flag("very-verbose"),
    flag("pods-watcher-enabled"),
    flag("allow-insecure-connections"),
    SpicedOption {
        long: "expanded",
        short: Some('x'),
        arity: Arity::Flag,
    },
    value("http"),
    value("flight"),
    value("metrics"),
    value("open_telemetry"),
    value("role"),
    value("node-bind-address"),
    value("node-advertise-address"),
    value("node-mtls-ca-certificate-file"),
    value("node-mtls-certificate-file"),
    value("node-mtls-key-file"),
    value("scheduler-address"),
    value("tls-enabled"),
    value("tls-certificate"),
    value("tls-certificate-file"),
    value("tls-key"),
    value("tls-key-file"),
    value("tls-client-auth-ca"),
    value("tls-client-auth-ca-file"),
    value("tls-client-auth-mode"),
    value("telemetry-enabled"),
    value("set-runtime"),
    value("cpu-cores"),
    value("service-log-dir"),
    value("token"),
    value("region"),
    value("repl-flight-endpoint"),
    value("http-endpoint"),
    value("tls-root-certificate-file"),
    value("client-tls-certificate-file"),
    value("client-tls-key-file"),
    value("api-key"),
    value("user-agent"),
    value("cache-control"),
    value("headers"),
];

/// Options after which `spiced` loads no Spicepod: it prints its help or its
/// version, or starts the SQL REPL, a client of another runtime.
const LOADS_NO_SPICEPOD: &[&str] = &["help", "version", "repl"];

/// The Spicepod `spiced` loads given its arguments.
#[derive(Debug, PartialEq, Eq)]
pub enum RuntimeSpicepod<'a> {
    /// No path argument: the Spicepod in the directory `spiced` runs in.
    WorkingDirectory,
    /// The path argument, as given: a manifest file, a directory holding
    /// `spicepod.yaml`, or a URL.
    Path(&'a str),
    /// None: `spiced` prints its help or version, or starts the SQL REPL.
    NotLoaded,
    /// Unknown: this argument is not one `spiced` accepts where it stands (an
    /// option it does not have, an option without its value, a value given to
    /// a flag, or a second path), so `spiced` refuses it, or `spiced` is a
    /// newer version with an option this CLI does not know.
    Unrecognized(&'a str),
}

/// The Spicepod `spiced` loads given `args`, the arguments after the program
/// name, read the way `spiced`'s parser reads them.
#[must_use]
pub fn runtime_spicepod(args: &[String]) -> RuntimeSpicepod<'_> {
    let mut path = None;
    let mut loads_spicepod = true;
    let mut tokens = args.iter().map(String::as_str);
    while let Some(token) = tokens.next() {
        if token == "--" {
            // Everything after `--` is positional.
            for positional in tokens.by_ref() {
                if path.replace(positional).is_some() {
                    return RuntimeSpicepod::Unrecognized(positional);
                }
            }
            break;
        }
        if let Some(long) = token.strip_prefix("--") {
            let (name, attached) = match long.split_once('=') {
                Some((name, attached)) => (name, Some(attached)),
                None => (long, None),
            };
            let Some(option) = SPICED_OPTIONS.iter().find(|option| option.long == name) else {
                return RuntimeSpicepod::Unrecognized(token);
            };
            match (option.arity, attached) {
                (Arity::Flag, Some(_)) => return RuntimeSpicepod::Unrecognized(token),
                (Arity::Value, None) if !tokens.next().is_some_and(is_value) => {
                    return RuntimeSpicepod::Unrecognized(token);
                }
                _ => {}
            }
            if option.long == "help" {
                // `spiced` prints its help as soon as it reads the option.
                return RuntimeSpicepod::NotLoaded;
            }
            loads_spicepod &= !LOADS_NO_SPICEPOD.contains(&option.long);
            continue;
        }
        if let Some(shorts) = token.strip_prefix('-').filter(|shorts| !shorts.is_empty()) {
            // A cluster of short options, such as `-vv`. A short option that
            // takes a value takes the rest of the cluster, or else the next
            // argument.
            for (at, short) in shorts.char_indices() {
                let Some(option) = SPICED_OPTIONS
                    .iter()
                    .find(|option| option.short == Some(short))
                else {
                    return RuntimeSpicepod::Unrecognized(token);
                };
                if option.long == "help" {
                    return RuntimeSpicepod::NotLoaded;
                }
                loads_spicepod &= !LOADS_NO_SPICEPOD.contains(&option.long);
                if option.arity == Arity::Value {
                    let rest = &shorts[at + short.len_utf8()..];
                    if rest.is_empty() && !tokens.next().is_some_and(is_value) {
                        return RuntimeSpicepod::Unrecognized(token);
                    }
                    break;
                }
            }
            continue;
        }
        // `spiced` takes one path.
        if path.replace(token).is_some() {
            return RuntimeSpicepod::Unrecognized(token);
        }
    }
    match path {
        _ if !loads_spicepod => RuntimeSpicepod::NotLoaded,
        Some(path) => RuntimeSpicepod::Path(path),
        None => RuntimeSpicepod::WorkingDirectory,
    }
}

/// `spiced` reads the argument after an option as its value unless it looks
/// like an option itself. A lone `-` is a value.
fn is_value(token: &str) -> bool {
    token == "-" || !token.starts_with('-')
}
