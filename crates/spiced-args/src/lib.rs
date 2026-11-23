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

//! `spiced`'s command line, and the one parser of it.
//!
//! `spiced` parses its arguments with [`try_parse_from`], and `spice run` calls
//! the same function on the arguments it is about to start `spiced` with, so
//! the CLI knows whether the runtime will accept them and which Spicepod it
//! will load from the runtime's own reading of them, not from a copy.

use std::ffi::OsString;
use std::net::SocketAddr;
use std::path::PathBuf;

use clap::parser::ValueSource;
use clap::{ArgAction, CommandFactory, FromArgMatches, Parser, ValueEnum};
use repl::ReplConfig;
use runtime_config::Config as RuntimeConfig;
use spicepod::component::runtime::ClientAuthMode as SpicepodClientAuthMode;

/// Parse `spiced`'s command line, `command_line` starting with the program
/// name, the way `spiced` does: clap's parse, then the checks that clap cannot
/// express (a `--role` that contradicts `--scheduler-address`).
///
/// # Errors
///
/// Returns the error `spiced` reports and exits with: an unknown or repeated
/// option, an invalid value, a missing required option, or a conflict. Help
/// is returned as an error too, as clap does, to be printed by
/// [`clap::Error::exit`].
pub fn try_parse_from<I, T>(command_line: I) -> Result<Args, clap::Error>
where
    I: IntoIterator<Item = T>,
    T: Into<OsString> + Clone,
{
    let mut matches = Args::command().try_get_matches_from(command_line)?;
    let open_telemetry_deprecated = chosen_on_command_line(&matches, "open_telemetry_bind_address");
    // `--repl-flight-endpoint` moves only the REPL's SQL target, leaving the HTTP endpoint that
    // `nql` uses wherever it already was. Choosing one without the other leaves nothing pointing
    // the HTTP endpoint at that runtime, so `nql` says so instead of answering from whatever
    // that endpoint reaches. See #11005.
    let flight_chosen = chosen_on_command_line(&matches, "repl_flight_endpoint");
    let http_chosen = chosen_on_command_line(&matches, "http_endpoint");
    // Mutable extraction removes typed values from `ArgMatches`, so the
    // enrollment key is left only in the zeroizing `Args` value.
    let mut args = Args::from_arg_matches_mut(&mut matches)?;
    // A usage error like clap's own conflicts, reported before anything starts.
    if let Err(message) = args.runtime.cluster.validate_role() {
        return Err(Args::command().error(clap::error::ErrorKind::ArgumentConflict, message));
    }
    args.open_telemetry_deprecated = open_telemetry_deprecated;
    args.repl_config.http_endpoint_may_be_another_runtime =
        repl::http_endpoint_unpaired(flight_chosen, http_chosen);
    Ok(args)
}

/// Whether `id` was given on the command line, rather than falling back to its default.
fn chosen_on_command_line(matches: &clap::ArgMatches, id: &str) -> bool {
    matches.value_source(id) == Some(ValueSource::CommandLine)
}

/// Clap mirror of [`SpicepodClientAuthMode`]. Defined here so we can
/// derive `ValueEnum` for the `--tls-client-auth-mode` CLI flag
/// without pulling clap into the `spicepod` crate. The two enums
/// are trivially convertible via [`ClientAuthMode::into_spicepod`]
/// and [`ClientAuthMode::from_spicepod`].
#[derive(ValueEnum, Debug, Copy, Clone, PartialEq, Eq, Default)]
#[clap(rename_all = "snake_case")]
pub enum ClientAuthMode {
    /// No client-cert authentication. The server runs
    /// `with_no_client_auth()` at the rustls layer and never sends a
    /// `CertificateRequest`. This is the out-of-the-box default.
    #[default]
    None,
    /// Request but do not require a client cert. The server sends a
    /// `CertificateRequest`; presented certs must be signed by the
    /// configured client CA, but no-cert handshakes are admitted.
    /// Useful for migration windows and audit-only deployments.
    Request,
    /// Require every connection to present a valid X.509 client cert
    /// signed by the configured client CA.
    Required,
}

impl ClientAuthMode {
    #[must_use]
    pub fn into_spicepod(self) -> SpicepodClientAuthMode {
        match self {
            ClientAuthMode::None => SpicepodClientAuthMode::None,
            ClientAuthMode::Request => SpicepodClientAuthMode::Request,
            ClientAuthMode::Required => SpicepodClientAuthMode::Required,
        }
    }

    #[must_use]
    pub fn from_spicepod(value: SpicepodClientAuthMode) -> Self {
        match value {
            SpicepodClientAuthMode::None => ClientAuthMode::None,
            SpicepodClientAuthMode::Request => ClientAuthMode::Request,
            SpicepodClientAuthMode::Required => ClientAuthMode::Required,
        }
    }
}

// `spiced`'s command line. Parse it with `try_parse_from`, which also applies
// the checks clap cannot express, rather than with `Parser` directly. Not a doc
// comment: clap would print it as the long help's description.
#[derive(Parser, Debug)]
#[clap(name = "spiced", about = "Spice.ai OSS Runtime")]
#[clap(rename_all = "kebab-case")]
#[expect(clippy::struct_excessive_bools)]
pub struct Args {
    /// Enable Prometheus metrics. (disabled by default)
    #[arg(long, value_name = "BIND_ADDRESS", help_heading = "Metrics")]
    pub metrics: Option<SocketAddr>,

    /// Deprecated OpenTelemetry bind address (no effect).
    #[arg(
        long = "open_telemetry",
        value_name = "OPEN_TELEMETRY_BIND_ADDRESS",
        default_value = "127.0.0.1:50052",
        action
    )]
    pub open_telemetry_bind_address: SocketAddr,

    /// Print the version and exit.
    #[arg(long)]
    pub version: bool,

    /// All runtime related arguments
    #[clap(flatten)]
    pub runtime: RuntimeConfig,

    /// Starts a SQL REPL to interactively query against the runtime's Flight endpoint.
    #[arg(long, help_heading = "SQL REPL")]
    pub repl: bool,

    #[clap(flatten)]
    pub repl_config: ReplConfig,

    /// Enable TLS for the runtime.
    #[arg(long, default_value_t = false, action = ArgAction::Set)]
    pub tls_enabled: bool,

    /// The TLS PEM-encoded certificate.
    #[arg(long, value_name = "-----BEGIN CERTIFICATE-----...")]
    pub tls_certificate: Option<String>,

    /// Path to the TLS PEM-encoded certificate file.
    #[arg(long, value_name = "cert.pem")]
    pub tls_certificate_file: Option<String>,

    /// The TLS PEM-encoded private key.
    #[arg(long, value_name = "-----BEGIN PRIVATE KEY-----...")]
    pub tls_key: Option<String>,

    /// Path to the TLS PEM-encoded private key file.
    #[arg(long, value_name = "key.pem")]
    pub tls_key_file: Option<String>,

    /// Path to a PEM-encoded CA bundle used to verify client certificates
    /// when `--tls-client-auth-mode request` or `required` (or the
    /// equivalent spicepod `runtime.tls.client_auth_mode`) is set.
    /// Eligible for hot-reload via the same watcher that picks up
    /// server cert / key rotations.
    #[arg(long, value_name = "client-ca.pem")]
    pub tls_client_auth_ca_file: Option<String>,

    /// Inline PEM-encoded CA bundle used to verify client certificates.
    /// Mutually exclusive with `--tls-client-auth-ca-file`. Inline material
    /// is not hot-reloaded.
    #[arg(long, value_name = "-----BEGIN CERTIFICATE-----...")]
    pub tls_client_auth_ca: Option<String>,

    /// How the runtime treats client certificates on the public TLS
    /// endpoints. `none` disables client-cert authentication (default).
    /// `request` sends `CertificateRequest` but accepts no-cert
    /// handshakes; presented certs must be signed by the client CA.
    /// `required` enables strict mTLS — the server demands a valid
    /// cert signed by `--tls-client-auth-ca` / `--tls-client-auth-ca-file`.
    #[arg(long, value_enum)]
    pub tls_client_auth_mode: Option<ClientAuthMode>,

    /// Enable anonymous telemetry collection. In Open Source builds that include anonymous telemetry,
    /// `false` is ignored; build without the `anonymous_telemetry` feature to remove anonymous usage telemetry.
    #[arg(long)]
    pub telemetry_enabled: Option<bool>,

    /// Enable pods watcher (disabled by default).
    #[arg(long, default_value_t = false, action = ArgAction::SetTrue)]
    pub pods_watcher_enabled: bool,

    /// One-time Spice Cloud enrollment key (`spice-enroll-…`) for Cloud
    /// Connect. When set, spiced enrolls this instance with Spice Cloud
    /// BEFORE the runtime is built or any listener binds — the process
    /// stays unready until the enrolled identity is durable on disk — then
    /// discards the key and starts normally. If a valid identity already
    /// exists in this instance's `.spice` directory, it wins and the key is
    /// NOT redeemed. Without this flag, an existing identity alone
    /// reconnects the instance; a `spiced` with no identity never connects
    /// to the cloud. The value is visible to same-host process listings for
    /// the full lifetime of the enrollment process (the operating system retains
    /// its original argv even after this field is dropped); it is single-use,
    /// short-lived, and never written by spiced to any file. Container and
    /// pod deployments should be recreated without this flag immediately
    /// after the runtime becomes ready.
    #[arg(
        long,
        value_name = "ENROLLMENT_KEY",
        conflicts_with = "repl",
        help_heading = "Spice Cloud Connect"
    )]
    pub token: Option<EnrollmentKeyArg>,

    /// Optional host-location label recorded on the instance at enrollment
    /// (for example `us-west-2` or `on-prem-syd`): 2-64 lowercase letters,
    /// digits, or hyphens. Only meaningful with `--token`; omitting it
    /// leaves any previously recorded region unchanged.
    #[arg(
        long,
        value_name = "REGION",
        requires = "token",
        help_heading = "Spice Cloud Connect"
    )]
    pub region: Option<String>,

    #[arg(short, long, action = ArgAction::Count)]
    pub verbose: u8,

    /// Enable very verbose logging. In conjunction with `verbose` can be set via -vv or --very-verbose.
    #[arg(long)]
    pub very_verbose: bool,

    /// Path to the Spicepod directory or file. Supports local paths and remote URLs (i.e. `s3://my_bucket/spicepod.yaml`)
    ///
    /// When specified, the behavior to automatically reload changes to the Spicepod is disabled.
    #[arg(value_name = "PATH")]
    pub spicepod: Option<PathBuf>,

    /// Overrides for the runtime configuration (--set-runtime key1.subkey=value1)
    #[arg(long, action = ArgAction::Append, value_parser = parse_set_string)]
    pub set_runtime: Vec<(String, String)>,

    /// How many CPUs the runtime should behave as though it has, as a
    /// Kubernetes CPU quantity (`4`, `3.5`, `3500m`). `auto` (the default)
    /// detects it from the cgroup CPU limit, a bounded multiple of the pod's
    /// `requests.cpu`, or the host. `all` uses every available core regardless
    /// of the request, still respecting a CPU limit. Takes precedence over
    /// `SPICE_CPU_CORES` and `runtime.cpu.cores`.
    #[arg(long, value_name = "CORES")]
    pub cpu_cores: Option<String>,

    /// Write the console log to bounded, rotating files in this directory
    /// instead of relying on the supervisor to store it.
    ///
    /// Set by `spice cloud service install` on a supervisor that owns no log
    /// store of its own — launchd writes whatever a job prints to the files its
    /// definition names and never bounds them, so the runtime bounds its own
    /// output instead. The policy is fixed
    /// (`runtime_cloud_connect::service_log`) because `spice cloud service
    /// logs` reads these files back against the same constants.
    ///
    /// Hidden: this is the service installer's interface to the runtime, not an
    /// operator's. `spice cloud logs` is how the files are read.
    #[arg(long, value_name = "PATH", hide = true)]
    pub service_log_dir: Option<PathBuf>,

    /// Whether the deprecated `--open_telemetry` was given on the command line.
    /// Set by [`try_parse_from`].
    #[arg(skip)]
    pub open_telemetry_deprecated: bool,
}

/// The raw `--token` argument: an unvalidated enrollment-key candidate.
///
/// Deliberately accepts any string at parse time (`FromStr` is infallible)
/// so clap never rejects — and therefore never **echoes** — a malformed
/// secret in its own error output. Validation happens in the Cloud Connect
/// bootstrap through the canonical `spice-enroll-` parser, whose errors
/// never reproduce the value. `Debug` is redacted so the key cannot leak
/// through argument logging, error chains, traces, or panic reports.
#[derive(Clone)]
pub struct EnrollmentKeyArg(zeroize::Zeroizing<String>);

impl EnrollmentKeyArg {
    /// The raw candidate value, handed only to the canonical parser.
    #[must_use]
    pub fn expose_secret(&self) -> &str {
        &self.0
    }
}

impl std::str::FromStr for EnrollmentKeyArg {
    type Err = std::convert::Infallible;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        Ok(Self(zeroize::Zeroizing::new(raw.to_string())))
    }
}

impl std::fmt::Debug for EnrollmentKeyArg {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "EnrollmentKeyArg([REDACTED])")
    }
}

fn parse_set_string(s: &str) -> Result<(String, String), String> {
    let parts: Vec<&str> = s.split('=').collect();
    if parts.len() != 2 {
        return Err("Invalid set format. Use key=value".into());
    }

    Ok((parts[0].to_string(), parts[1].to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::error::ErrorKind;
    use std::path::Path;

    fn parse(args: &[&str]) -> Result<Args, clap::Error> {
        try_parse_from(std::iter::once("spiced").chain(args.iter().copied()))
    }

    /// Each refusal is the one `spiced` prints for these arguments: the first
    /// line of its error, as a `spiced` binary printed it.
    #[test]
    fn refuses_what_spiced_refuses() {
        for (args, first_line) in [
            (
                &["--tls-enabled", "maybe"][..],
                "error: invalid value 'maybe' for '--tls-enabled <TLS_ENABLED>'",
            ),
            (
                &["--metrics", "not-an-address"],
                "error: invalid value 'not-an-address' for '--metrics <BIND_ADDRESS>': invalid socket address syntax",
            ),
            (
                &["--region", "us-west-2"],
                "error: the following required arguments were not provided:",
            ),
            (
                &["--telemetry-enabled", "maybe"],
                "error: invalid value 'maybe' for '--telemetry-enabled <TELEMETRY_ENABLED>'",
            ),
            (
                &["--set-runtime", "noequals"],
                "error: invalid value 'noequals' for '--set-runtime <SET_RUNTIME>': Invalid set format. Use key=value",
            ),
            (
                &["--metrics", "127.0.0.1:9090", "--metrics", "127.0.0.1:9091"],
                "error: the argument '--metrics <BIND_ADDRESS>' cannot be used multiple times",
            ),
            (
                &["--not-an-option"],
                "error: unexpected argument '--not-an-option' found",
            ),
            (
                &[
                    "--role",
                    "server",
                    "--scheduler-address",
                    "https://127.0.0.1:50052",
                ],
                "error: `--scheduler-address` joins a cluster as an executor and cannot be used with `--role server`. Remove `--scheduler-address`, or use `--role executor`.",
            ),
        ] {
            let error = parse(args).expect_err("spiced refuses these arguments");
            let rendered = error.to_string();
            assert_eq!(
                rendered.lines().next(),
                Some(first_line),
                "{args:?}: {rendered}"
            );
            assert_eq!(error.exit_code(), 2, "{args:?}");
        }
    }

    /// The Spicepod is the one path argument, wherever it stands among the
    /// options, and none is loaded for `--version` or `--repl`.
    #[test]
    fn reads_the_spicepod_spiced_loads() {
        for (args, spicepod) in [
            (&[][..], None),
            (&["production.yaml"], Some("production.yaml")),
            (
                &["--tls-enabled", "false", "production.yaml"],
                Some("production.yaml"),
            ),
            (
                &[
                    "--set-runtime",
                    "results_cache.enabled=false",
                    "production.yaml",
                ],
                Some("production.yaml"),
            ),
            (
                &["-vv", "production.yaml", "--metrics", "127.0.0.1:9090"],
                Some("production.yaml"),
            ),
            (&["--", "-odd.yaml"], Some("-odd.yaml")),
            (
                &["s3://bucket/spicepod.yaml"],
                Some("s3://bucket/spicepod.yaml"),
            ),
        ] {
            let parsed = parse(args).expect("spiced accepts these arguments");
            assert_eq!(
                parsed.spicepod.as_deref(),
                spicepod.map(Path::new),
                "{args:?}"
            );
            assert!(!parsed.version && !parsed.repl, "{args:?}");
        }
        assert!(
            parse(&["--version", "production.yaml"])
                .expect("parses")
                .version
        );
        assert!(parse(&["--repl"]).expect("parses").repl);
        assert_eq!(
            parse(&["--help", "production.yaml"])
                .expect_err("help is printed, not run")
                .kind(),
            ErrorKind::DisplayHelp
        );
    }

    /// What the parse records beyond clap's own values: whether the deprecated
    /// `--open_telemetry` was given, and whether the REPL's HTTP endpoint may
    /// point at another runtime than its Flight endpoint.
    #[test]
    fn records_which_options_were_given() {
        let defaults = parse(&[]).expect("parses");
        assert!(!defaults.open_telemetry_deprecated);
        assert!(!defaults.repl_config.http_endpoint_may_be_another_runtime);

        let given = parse(&["--open_telemetry", "127.0.0.1:50052"]).expect("parses");
        assert!(given.open_telemetry_deprecated);

        let flight_only =
            parse(&["--repl", "--repl-flight-endpoint", "http://10.0.0.1:50051"]).expect("parses");
        assert!(flight_only.repl_config.http_endpoint_may_be_another_runtime);

        let both = parse(&[
            "--repl",
            "--repl-flight-endpoint",
            "http://10.0.0.1:50051",
            "--http-endpoint",
            "http://10.0.0.1:8090",
        ])
        .expect("parses");
        assert!(!both.repl_config.http_endpoint_may_be_another_runtime);
    }
}
