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

//! `spice run` builds the functions of the Spicepod `spiced` is about to
//! load, and finds that Spicepod among the arguments it passes `spiced` with
//! its own reading of them (`spice::commands::run::spiced_args`). These tests
//! hold that reading to `spiced`'s parser, so building one Spicepod while the
//! runtime loads another cannot come back when an option is added here.

use crate::Args;
use clap::{CommandFactory, Parser};
use spice::commands::run::spiced_args::{Arity, RuntimeSpicepod, SPICED_OPTIONS, runtime_spicepod};

/// `spice run`'s list of options names every option `spiced` accepts, each
/// with whether it takes a value, and no other.
#[test]
fn spice_run_knows_every_option_spiced_accepts() {
    let mut command = Args::command();
    command.build();
    let mut accepted: Vec<(String, Option<char>, Arity)> = command
        .get_arguments()
        .filter(|arg| !arg.is_positional())
        .map(|arg| {
            let id = arg.get_id();
            let arity = match arg.get_num_args() {
                Some(range) if range.max_values() == 0 => Arity::Flag,
                Some(range) if range.min_values() == 1 && range.max_values() == 1 => Arity::Value,
                other => panic!(
                    "`spice run` reads options that take no value or exactly one, but '{id}' takes {other:?}"
                ),
            };
            assert!(
                arg.get_all_aliases().is_none() && arg.get_all_short_aliases().is_none(),
                "'{id}' has an alias, which `spice run` does not read"
            );
            let long = arg
                .get_long()
                .unwrap_or_else(|| panic!("'{id}' has no long name"));
            (long.to_string(), arg.get_short(), arity)
        })
        .collect();
    let mut known: Vec<(String, Option<char>, Arity)> = SPICED_OPTIONS
        .iter()
        .map(|option| (option.long.to_string(), option.short, option.arity))
        .collect();
    accepted.sort_by(|a, b| a.0.cmp(&b.0));
    known.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(
        known, accepted,
        "update SPICED_OPTIONS in bin/spice/src/commands/run/spiced_args.rs to match spiced's options"
    );
}

/// For each argument list, `spice run` builds the Spicepod `spiced` loads:
/// the same path, the working directory's, or none when `spiced` prints its
/// help or version or starts the REPL. Where `spiced` refuses the arguments,
/// `spice run` builds nothing.
#[test]
fn spice_run_finds_the_spicepod_spiced_loads() {
    for args in [
        &[][..],
        &["production.yaml"],
        &["s3://bucket/spicepod.yaml"],
        &["--very-verbose", "production.yaml"],
        &["production.yaml", "--very-verbose"],
        &["--tls-enabled", "false", "production.yaml"],
        &["--tls-enabled=false", "production.yaml"],
        &[
            "--set-runtime",
            "results_cache.enabled=false",
            "production.yaml",
        ],
        &["-vv", "production.yaml", "--metrics", "127.0.0.1:9090"],
        &["-v", "-x", "production.yaml"],
        &["--metrics", "127.0.0.1:9090"],
        &["--cpu-cores", "-1", "production.yaml"],
        &["--cpu-cores", "-", "production.yaml"],
        &["--", "-odd.yaml"],
        &["-"],
        &["--version", "production.yaml"],
        &["--repl"],
        &["--help", "production.yaml"],
        &["-vh"],
        &["--not-an-option", "production.yaml"],
        &["-vq", "production.yaml"],
        &["--metrics"],
        &["--metrics", "--very-verbose", "production.yaml"],
        &["--very-verbose=true", "production.yaml"],
        &["a.yaml", "b.yaml"],
        &["--", "a.yaml", "b.yaml"],
    ] {
        let owned: Vec<String> = args.iter().map(ToString::to_string).collect();
        let spice_reads = runtime_spicepod(&owned);
        let parsed = Args::try_parse_from(std::iter::once("spiced").chain(args.iter().copied()));
        let spiced_loads = match &parsed {
            Err(err) if err.kind() == clap::error::ErrorKind::DisplayHelp => {
                RuntimeSpicepod::NotLoaded
            }
            Err(err) => {
                assert!(
                    matches!(spice_reads, RuntimeSpicepod::Unrecognized(_)),
                    "spiced refuses {args:?} ({}), so `spice run` must build nothing, but it reads {spice_reads:?}",
                    err.kind()
                );
                continue;
            }
            Ok(parsed) if parsed.version || parsed.repl => RuntimeSpicepod::NotLoaded,
            Ok(parsed) => match &parsed.spicepod {
                Some(path) => RuntimeSpicepod::Path(path.to_str().expect("a UTF-8 path")),
                None => RuntimeSpicepod::WorkingDirectory,
            },
        };
        assert_eq!(spice_reads, spiced_loads, "{args:?}");
    }
}
