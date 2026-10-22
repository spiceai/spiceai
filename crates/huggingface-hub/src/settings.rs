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

//! Which Hub a component reads, with which token, and where its downloads are cached.
//!
//! The precedence is `huggingface_hub`'s, so one setting works for Spice, the `hf` CLI and
//! every other Hugging Face tool on the machine:
//!
//! | Setting  | Precedence                                                                       |
//! |----------|----------------------------------------------------------------------------------|
//! | Token    | `hf_token` → `HF_TOKEN` → `HUGGING_FACE_HUB_TOKEN` → the file at `HF_TOKEN_PATH` |
//! | Endpoint | `hf_endpoint` → `HF_ENDPOINT` → `https://huggingface.co`                         |
//! | Cache    | `HF_HUB_CACHE` → `HUGGINGFACE_HUB_CACHE` → `$HF_HOME/hub`                        |
//! | Offline  | `HF_HUB_OFFLINE` or `TRANSFORMERS_OFFLINE` set to `1`, `true`, `yes` or `on`     |
//!
//! `HF_HOME` defaults to `$XDG_CACHE_HOME/huggingface`, then `~/.cache/huggingface`, and
//! `HF_TOKEN_PATH` to `$HF_HOME/token`. `HF_HUB_TOKEN`, which mistral.rs read before Spice
//! resolved tokens itself, is still read after `HUGGING_FACE_HUB_TOKEN`, with a warning.
//!
//! A token from the environment or the token file is sent only to the public Hub or to
//! `HF_ENDPOINT`, endpoints set where the token is: an `hf_endpoint` from a Spicepod receives
//! only an `hf_token` from the same Spicepod.

use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};

use secrecy::{ExposeSecret, SecretString};
use snafu::prelude::*;
use url::Url;

use crate::client::DEFAULT_ENDPOINT;

pub const HF_TOKEN: &str = "HF_TOKEN";
pub const HUGGING_FACE_HUB_TOKEN: &str = "HUGGING_FACE_HUB_TOKEN";
/// Read by chat models before every component resolved its token here; deprecated.
pub const HF_HUB_TOKEN: &str = "HF_HUB_TOKEN";
pub const HF_TOKEN_PATH: &str = "HF_TOKEN_PATH";
pub const HF_HOME: &str = "HF_HOME";
pub const HF_HUB_CACHE: &str = "HF_HUB_CACHE";
pub const HUGGINGFACE_HUB_CACHE: &str = "HUGGINGFACE_HUB_CACHE";
pub const XDG_CACHE_HOME: &str = "XDG_CACHE_HOME";
pub const HF_ENDPOINT: &str = "HF_ENDPOINT";
pub const HF_HUB_OFFLINE: &str = "HF_HUB_OFFLINE";
pub const TRANSFORMERS_OFFLINE: &str = "TRANSFORMERS_OFFLINE";
pub const HF_HUB_DISABLE_XET: &str = "HF_HUB_DISABLE_XET";
pub const HF_XET_CACHE: &str = "HF_XET_CACHE";

/// The deprecated `HF_HUB_TOKEN` is warned about once per process, not once per model.
static WARNED_DEPRECATED_TOKEN: AtomicBool = AtomicBool::new(false);

#[derive(Debug, Snafu)]
pub enum Error {
    // Each message is worded as the cause clause a download error embeds it as.
    #[snafu(display(
        "{setting} {endpoint:?} is not a valid Hugging Face Hub endpoint: use an https:// URL such as '{DEFAULT_ENDPOINT}' (http:// is accepted only for a loopback address)"
    ))]
    InvalidEndpoint {
        setting: EndpointSource,
        endpoint: String,
    },

    #[snafu(display(
        "the Hugging Face token file '{}' cannot be read ({source}). Make it readable, or set `hf_token` or `HF_TOKEN`",
        display_path(path)
    ))]
    UnreadableTokenFile {
        path: PathBuf,
        source: std::io::Error,
    },

    #[snafu(display(
        "no directory to cache Hugging Face downloads in: `HF_HUB_CACHE`, `HF_HOME` and the home directory are all unset. Set `HF_HUB_CACHE` to a writable directory"
    ))]
    NoCacheDirectory,
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Where a token came from. An error about a token names its source, because each has its own
/// fix.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TokenSource {
    /// The component's `hf_token` parameter (or one of its aliases).
    Param,
    /// An environment variable.
    Env(&'static str),
    /// The token file `hf auth login` writes.
    File(PathBuf),
}

impl fmt::Display for TokenSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TokenSource::Param => f.write_str("`hf_token`"),
            TokenSource::Env(name) => write!(f, "`{name}`"),
            TokenSource::File(path) => write!(f, "'{}'", display_path(path)),
        }
    }
}

/// A component's `hf_token`, and where Spice got it, which decides where it may be sent.
#[derive(Debug, Clone)]
pub enum ParamToken {
    /// Set in the Spicepod: sent to the component's endpoint, whichever it is.
    Spicepod(SecretString),
    /// Loaded from a secret store because the Spicepod does not set it. The environment secret
    /// store reads `HF_TOKEN` for it, so it is sent only where `HF_TOKEN` is: to the public Hub
    /// or `HF_ENDPOINT`, never to an `hf_endpoint` the Spicepod sets.
    SecretStore(SecretString),
}

impl ParamToken {
    #[must_use]
    pub fn secret(&self) -> &SecretString {
        match self {
            ParamToken::Spicepod(secret) | ParamToken::SecretStore(secret) => secret,
        }
    }
}

/// A Hub token and where it came from.
#[derive(Clone)]
pub struct Token {
    secret: SecretString,
    source: TokenSource,
}

impl fmt::Debug for Token {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Token")
            .field("secret", &"[REDACTED]")
            .field("source", &self.source)
            .finish()
    }
}

impl Token {
    #[must_use]
    pub fn new(secret: SecretString, source: TokenSource) -> Self {
        Self { secret, source }
    }

    #[must_use]
    pub fn secret(&self) -> &SecretString {
        &self.secret
    }

    #[must_use]
    pub fn source(&self) -> &TokenSource {
        &self.source
    }
}

/// Which tokens an endpoint receives.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TokenScope {
    /// The public Hub or `HF_ENDPOINT`: any token, wherever it was set.
    Any,
    /// An `hf_endpoint` the Spicepod sets: only an `hf_token` the Spicepod sets.
    Spicepod,
}

impl TokenScope {
    /// How a message tells a user who sent no token to set one.
    #[must_use]
    pub fn how_to_set(self) -> &'static str {
        match self {
            TokenScope::Any => "set `hf_token`, set `HF_TOKEN`, or run `hf auth login`",
            TokenScope::Spicepod => {
                "set `hf_token` (a token from `HF_TOKEN` or `hf auth login` goes only to the public Hub or `HF_ENDPOINT`)"
            }
        }
    }
}

/// Where the endpoint came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EndpointSource {
    Param,
    Env,
    Default,
}

impl fmt::Display for EndpointSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            EndpointSource::Param => f.write_str("`hf_endpoint`"),
            EndpointSource::Env => write!(f, "`{HF_ENDPOINT}`"),
            EndpointSource::Default => f.write_str("The default endpoint"),
        }
    }
}

/// What a component reads the Hub with, resolved from its parameters and the environment.
#[derive(Debug, Clone)]
pub struct Settings {
    pub endpoint: Url,
    pub endpoint_source: EndpointSource,
    pub token: Option<Token>,
    pub token_scope: TokenScope,
    /// The root of the Hub's file cache: `models--<owner>--<name>/` and its siblings.
    pub cache_dir: PathBuf,
    /// The root of `hf-xet`'s shard cache.
    pub xet_cache_dir: PathBuf,
    /// Only the cache is read; the Hub is never contacted.
    pub offline: bool,
    /// Every file is read through the Hub's `resolve` redirect, never the Xet protocol.
    pub xet_disabled: bool,
}

/// The process environment, or a stand-in a test controls without mutating process state.
pub trait Environment: Sync {
    fn var(&self, name: &str) -> Option<String>;

    fn home_dir(&self) -> Option<PathBuf>;
}

/// The environment of this process.
#[derive(Debug, Clone, Copy, Default)]
pub struct ProcessEnvironment;

impl Environment for ProcessEnvironment {
    fn var(&self, name: &str) -> Option<String> {
        std::env::var(name).ok()
    }

    fn home_dir(&self) -> Option<PathBuf> {
        util::home_dir::home_dir()
    }
}

impl Settings {
    /// Resolves the settings of a component with the given `hf_token` and `hf_endpoint`
    /// parameters from the process environment.
    ///
    /// # Errors
    ///
    /// Returns an error if an endpoint is not a valid Hub URL, the token file exists but cannot
    /// be read, or no cache directory can be found.
    pub async fn resolve(token: Option<ParamToken>, endpoint: Option<&str>) -> Result<Self> {
        Self::resolve_in(&ProcessEnvironment, token, endpoint).await
    }

    /// [`Settings::resolve`] in `env`.
    ///
    /// # Errors
    ///
    /// See [`Settings::resolve`].
    pub async fn resolve_in(
        env: &impl Environment,
        token: Option<ParamToken>,
        endpoint: Option<&str>,
    ) -> Result<Self> {
        let var = |name: &str| env.var(name).filter(|value| !value.trim().is_empty());
        let Access {
            endpoint,
            endpoint_source,
            token,
            token_scope,
        } = Access::resolve_in(env, token, endpoint).await?;
        let hf_home = hf_home(env);
        let cache_dir = var(HF_HUB_CACHE)
            .or_else(|| var(HUGGINGFACE_HUB_CACHE))
            .map(|cache| expand_home(&cache, env))
            .or_else(|| hf_home.as_ref().map(|home| home.join("hub")))
            .context(NoCacheDirectorySnafu)?;
        let xet_cache_dir = var(HF_XET_CACHE)
            .map(|cache| expand_home(&cache, env))
            .or_else(|| hf_home.as_ref().map(|home| home.join("xet")))
            .context(NoCacheDirectorySnafu)?;

        Ok(Self {
            endpoint,
            endpoint_source,
            token,
            token_scope,
            cache_dir,
            xet_cache_dir,
            offline: is_true(var(HF_HUB_OFFLINE).as_deref())
                || is_true(var(TRANSFORMERS_OFFLINE).as_deref()),
            xet_disabled: is_true(var(HF_HUB_DISABLE_XET).as_deref()),
        })
    }
}

/// What a component that keeps no cache reads the Hub with, a dataset's stream: the endpoint
/// and the token, resolved as [`Settings::resolve`] resolves them.
#[derive(Debug, Clone)]
pub struct Access {
    pub endpoint: Url,
    pub endpoint_source: EndpointSource,
    pub token: Option<Token>,
    pub token_scope: TokenScope,
}

impl Access {
    /// Resolves the endpoint and the token of a component with the given `hf_token` and
    /// `hf_endpoint` parameters from the process environment.
    ///
    /// # Errors
    ///
    /// Returns an error if an endpoint is not a valid Hub URL, or the token file exists but
    /// cannot be read.
    pub async fn resolve(token: Option<ParamToken>, endpoint: Option<&str>) -> Result<Self> {
        Self::resolve_in(&ProcessEnvironment, token, endpoint).await
    }

    /// [`Access::resolve`] in `env`.
    ///
    /// # Errors
    ///
    /// See [`Access::resolve`].
    pub async fn resolve_in(
        env: &impl Environment,
        token: Option<ParamToken>,
        endpoint: Option<&str>,
    ) -> Result<Self> {
        let var = |name: &str| env.var(name).filter(|value| !value.trim().is_empty());

        let env_endpoint = var(HF_ENDPOINT)
            .map(|endpoint| parse_endpoint(&endpoint, EndpointSource::Env))
            .transpose()?;
        let default_endpoint = parse_endpoint(DEFAULT_ENDPOINT, EndpointSource::Default)?;
        let (endpoint, endpoint_source) = match endpoint.filter(|e| !e.trim().is_empty()) {
            Some(endpoint) => (
                parse_endpoint(endpoint, EndpointSource::Param)?,
                EndpointSource::Param,
            ),
            None => match &env_endpoint {
                Some(endpoint) => (endpoint.clone(), EndpointSource::Env),
                None => (default_endpoint.clone(), EndpointSource::Default),
            },
        };

        // A token the Spicepod does not set goes only to an endpoint set where it was: the
        // public Hub or `HF_ENDPOINT`, never a Spicepod's `hf_endpoint`.
        let token_scope = if endpoint.origin() == default_endpoint.origin()
            || env_endpoint
                .as_ref()
                .is_some_and(|env_endpoint| env_endpoint.origin() == endpoint.origin())
        {
            TokenScope::Any
        } else {
            TokenScope::Spicepod
        };
        let implicit_allowed = token_scope == TokenScope::Any;
        let param = |secret: &SecretString| Token {
            secret: SecretString::from(secret.expose_secret().trim().to_string()),
            source: TokenSource::Param,
        };
        let token = match token.filter(|token| !token.secret().expose_secret().trim().is_empty()) {
            Some(ParamToken::Spicepod(secret)) => Some(param(&secret)),
            Some(ParamToken::SecretStore(secret)) if implicit_allowed => Some(param(&secret)),
            None if implicit_allowed => implicit_token(env, hf_home(env).as_deref()).await?,
            Some(ParamToken::SecretStore(_)) | None => None,
        };

        Ok(Self {
            endpoint,
            endpoint_source,
            token,
            token_scope,
        })
    }
}

/// `HF_HOME`, else `$XDG_CACHE_HOME/huggingface`, else `~/.cache/huggingface`.
fn hf_home(env: &impl Environment) -> Option<PathBuf> {
    let var = |name: &str| env.var(name).filter(|value| !value.trim().is_empty());
    var(HF_HOME)
        .map(|home| expand_home(&home, env))
        .or_else(|| var(XDG_CACHE_HOME).map(|xdg| expand_home(&xdg, env).join("huggingface")))
        .or_else(|| {
            env.home_dir()
                .map(|home| home.join(".cache").join("huggingface"))
        })
}

/// The token from the environment or the token file, in `huggingface_hub`'s order.
async fn implicit_token(env: &impl Environment, hf_home: Option<&Path>) -> Result<Option<Token>> {
    for name in [HF_TOKEN, HUGGING_FACE_HUB_TOKEN, HF_HUB_TOKEN] {
        let Some(value) = env.var(name).map(|value| value.trim().to_string()) else {
            continue;
        };
        if value.is_empty() {
            continue;
        }
        if name == HF_HUB_TOKEN && !WARNED_DEPRECATED_TOKEN.swap(true, Ordering::Relaxed) {
            tracing::warn!(
                "`{HF_HUB_TOKEN}` is deprecated and will stop being read in a future release: set `{HF_TOKEN}` to the same token instead. See: https://spiceai.org/docs/components/models/huggingface"
            );
        }
        return Ok(Some(Token {
            secret: SecretString::from(value),
            source: TokenSource::Env(name),
        }));
    }

    let Some(path) = env
        .var(HF_TOKEN_PATH)
        .filter(|path| !path.trim().is_empty())
        .map(|path| expand_home(&path, env))
        .or_else(|| hf_home.map(|home| home.join("token")))
    else {
        return Ok(None);
    };
    match tokio::fs::read_to_string(&path).await {
        Ok(contents) => {
            let token = contents.trim();
            Ok((!token.is_empty()).then(|| Token {
                secret: SecretString::from(token.to_string()),
                source: TokenSource::File(path),
            }))
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(source) => Err(Error::UnreadableTokenFile { path, source }),
    }
}

/// Parses an endpoint. The token is sent to it, so it must be https unless it is loopback.
///
/// # Errors
///
/// Returns an error if `endpoint` is not an https URL, or an http URL of a loopback address,
/// or carries a query, fragment or credentials.
pub fn parse_endpoint(endpoint: &str, setting: EndpointSource) -> Result<Url> {
    let invalid = || Error::InvalidEndpoint {
        setting,
        endpoint: endpoint.to_string(),
    };
    let url = Url::parse(endpoint.trim()).map_err(|_| invalid())?;
    let loopback = match url.host() {
        Some(url::Host::Domain(domain)) => domain == "localhost",
        Some(url::Host::Ipv4(ip)) => ip.is_loopback(),
        Some(url::Host::Ipv6(ip)) => ip.is_loopback(),
        None => false,
    };
    let valid = url.has_host()
        && url.query().is_none()
        && url.fragment().is_none()
        && url.username().is_empty()
        && url.password().is_none()
        && (url.scheme() == "https" || (url.scheme() == "http" && loopback));
    ensure!(valid, InvalidEndpointSnafu { setting, endpoint });
    Ok(url)
}

/// `huggingface_hub`'s truthy values: `1`, `true`, `yes` and `on`, in any case.
fn is_true(value: Option<&str>) -> bool {
    value.is_some_and(|value| {
        matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        )
    })
}

/// Expands a leading `~` to the home directory, as `huggingface_hub` does.
fn expand_home(path: &str, env: &impl Environment) -> PathBuf {
    let path = path.trim();
    match (path.strip_prefix('~'), env.home_dir()) {
        (Some(rest), Some(home)) if rest.is_empty() || rest.starts_with(['/', '\\']) => {
            home.join(rest.trim_start_matches(['/', '\\']))
        }
        _ => PathBuf::from(path),
    }
}

/// A path for a message, with the home directory shown as `~`.
#[must_use]
pub fn display_path(path: &Path) -> String {
    match util::home_dir::home_dir()
        .and_then(|home| path.strip_prefix(home).ok().map(Path::to_path_buf))
    {
        Some(relative) => format!("~/{}", relative.display()),
        None => path.display().to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Env {
        vars: Vec<(&'static str, String)>,
        home: PathBuf,
    }

    impl Environment for Env {
        fn var(&self, name: &str) -> Option<String> {
            self.vars
                .iter()
                .find(|(key, _)| *key == name)
                .map(|(_, value)| value.clone())
        }

        fn home_dir(&self) -> Option<PathBuf> {
            Some(self.home.clone())
        }
    }

    fn env(vars: &[(&'static str, &str)]) -> Env {
        Env {
            vars: vars.iter().map(|(k, v)| (*k, (*v).to_string())).collect(),
            home: PathBuf::from("/home/u"),
        }
    }

    async fn resolve(env: &Env, token: Option<&str>, endpoint: Option<&str>) -> Settings {
        resolve_with(
            env,
            token.map(|t| ParamToken::Spicepod(SecretString::from(t.to_string()))),
            endpoint,
        )
        .await
    }

    async fn resolve_with(
        env: &Env,
        token: Option<ParamToken>,
        endpoint: Option<&str>,
    ) -> Settings {
        Settings::resolve_in(env, token, endpoint)
            .await
            .expect("valid settings")
    }

    fn token(settings: &Settings) -> Option<(String, TokenSource)> {
        settings.token.as_ref().map(|token| {
            (
                token.secret().expose_secret().to_string(),
                token.source().clone(),
            )
        })
    }

    #[tokio::test]
    async fn the_token_follows_huggingface_hubs_precedence() {
        let all = [
            (HF_TOKEN, "hf_env"),
            (HUGGING_FACE_HUB_TOKEN, "hf_legacy"),
            (HF_HUB_TOKEN, "hf_deprecated"),
        ];
        assert_eq!(
            token(&resolve(&env(&all), Some("hf_param"), None).await),
            Some(("hf_param".to_string(), TokenSource::Param))
        );
        assert_eq!(
            token(&resolve(&env(&all), None, None).await),
            Some(("hf_env".to_string(), TokenSource::Env(HF_TOKEN)))
        );
        assert_eq!(
            token(&resolve(&env(&all[1..]), None, None).await),
            Some((
                "hf_legacy".to_string(),
                TokenSource::Env(HUGGING_FACE_HUB_TOKEN)
            ))
        );
        assert_eq!(
            token(&resolve(&env(&all[2..]), None, None).await),
            Some(("hf_deprecated".to_string(), TokenSource::Env(HF_HUB_TOKEN)))
        );
        // A blank value is no token, as `huggingface_hub` treats one.
        assert_eq!(
            token(&resolve(&env(&[(HF_TOKEN, "  ")]), Some(" "), None).await),
            None
        );
    }

    #[tokio::test]
    async fn the_token_file_is_read_last() {
        let home = tempfile::tempdir().expect("a home");
        let hf_home = home.path().join("hf");
        std::fs::create_dir_all(&hf_home).expect("HF_HOME");
        std::fs::write(hf_home.join("token"), "hf_file\n").expect("a token file");
        let hf_home = hf_home.display().to_string();
        let settings = resolve(&env(&[(HF_HOME, &hf_home)]), None, None).await;
        assert_eq!(
            token(&settings),
            Some((
                "hf_file".to_string(),
                TokenSource::File(home.path().join("hf").join("token"))
            ))
        );
        // `HF_TOKEN_PATH` names another file.
        let other = home.path().join("other-token");
        std::fs::write(&other, "hf_other").expect("a token file");
        let other_path = other.display().to_string();
        let settings = resolve(
            &env(&[(HF_HOME, &hf_home), (HF_TOKEN_PATH, &other_path)]),
            None,
            None,
        )
        .await;
        assert_eq!(
            token(&settings),
            Some(("hf_other".to_string(), TokenSource::File(other)))
        );
        // The environment outranks the file.
        let settings = resolve(
            &env(&[(HF_HOME, &hf_home), (HF_TOKEN, "hf_env")]),
            None,
            None,
        )
        .await;
        assert_eq!(
            token(&settings).map(|(_, source)| source),
            Some(TokenSource::Env(HF_TOKEN))
        );
    }

    #[tokio::test]
    async fn an_implicit_token_goes_only_to_an_endpoint_set_beside_it() {
        let vars = [(HF_TOKEN, "hf_env")];
        // A Spicepod's mirror gets no token from the environment...
        let mirror = resolve(&env(&vars), None, Some("https://mirror.example")).await;
        assert_eq!(mirror.endpoint.as_str(), "https://mirror.example/");
        assert_eq!(mirror.endpoint_source, EndpointSource::Param);
        assert!(mirror.token.is_none());
        // ...but its own `hf_token`, and the public Hub named explicitly, do.
        let with_param = resolve(
            &env(&vars),
            Some("hf_param"),
            Some("https://mirror.example"),
        )
        .await;
        assert_eq!(token(&with_param).map(|(_, s)| s), Some(TokenSource::Param));
        let public = resolve(&env(&vars), None, Some("https://huggingface.co")).await;
        assert_eq!(
            token(&public).map(|(_, s)| s),
            Some(TokenSource::Env(HF_TOKEN))
        );
        // `HF_ENDPOINT` is set where the token is, so it receives it.
        let env_mirror = resolve(
            &env(&[
                (HF_TOKEN, "hf_env"),
                (HF_ENDPOINT, "https://mirror.example"),
            ]),
            None,
            None,
        )
        .await;
        assert_eq!(env_mirror.endpoint_source, EndpointSource::Env);
        assert_eq!(
            token(&env_mirror).map(|(_, s)| s),
            Some(TokenSource::Env(HF_TOKEN))
        );
    }

    /// A token Spice loaded from a secret store, which the environment secret store reads from
    /// `HF_TOKEN`, is sent where `HF_TOKEN` is and nowhere else.
    #[tokio::test]
    async fn a_token_from_a_secret_store_goes_only_where_an_implicit_token_goes() {
        let loaded = || Some(ParamToken::SecretStore(SecretString::from("hf_loaded")));
        let mirror = resolve_with(&env(&[]), loaded(), Some("https://mirror.example")).await;
        assert!(mirror.token.is_none());
        let public = resolve_with(&env(&[(HF_TOKEN, "hf_env")]), loaded(), None).await;
        assert_eq!(
            token(&public),
            Some(("hf_loaded".to_string(), TokenSource::Param))
        );
        let env_mirror = resolve_with(
            &env(&[(HF_ENDPOINT, "https://mirror.example")]),
            loaded(),
            None,
        )
        .await;
        assert_eq!(
            token(&env_mirror),
            Some(("hf_loaded".to_string(), TokenSource::Param))
        );
    }

    #[tokio::test]
    async fn the_cache_follows_huggingface_hubs_precedence() {
        let cache = |vars: &[(&'static str, &str)]| {
            let env = env(vars);
            async move { resolve(&env, None, None).await.cache_dir }
        };
        assert_eq!(
            cache(&[]).await,
            PathBuf::from("/home/u/.cache/huggingface/hub")
        );
        assert_eq!(
            cache(&[(XDG_CACHE_HOME, "/xdg")]).await,
            PathBuf::from("/xdg/huggingface/hub")
        );
        assert_eq!(
            cache(&[(XDG_CACHE_HOME, "/xdg"), (HF_HOME, "~/hf")]).await,
            PathBuf::from("/home/u/hf/hub")
        );
        assert_eq!(
            cache(&[(HF_HOME, "/hf"), (HUGGINGFACE_HUB_CACHE, "/legacy")]).await,
            PathBuf::from("/legacy")
        );
        assert_eq!(
            cache(&[(HUGGINGFACE_HUB_CACHE, "/legacy"), (HF_HUB_CACHE, "/hub")]).await,
            PathBuf::from("/hub")
        );
        let xet = resolve(&env(&[(HF_HOME, "/hf")]), None, None)
            .await
            .xet_cache_dir;
        assert_eq!(xet, PathBuf::from("/hf/xet"));
        let xet = resolve(&env(&[(HF_HOME, "/hf"), (HF_XET_CACHE, "/x")]), None, None)
            .await
            .xet_cache_dir;
        assert_eq!(xet, PathBuf::from("/x"));
    }

    #[tokio::test]
    async fn offline_and_xet_switches_take_huggingface_hubs_truthy_values() {
        for value in ["1", "true", "TRUE", "yes", "On"] {
            assert!(
                resolve(&env(&[(HF_HUB_OFFLINE, value)]), None, None)
                    .await
                    .offline,
                "{value}"
            );
            assert!(
                resolve(&env(&[(TRANSFORMERS_OFFLINE, value)]), None, None)
                    .await
                    .offline,
                "{value}"
            );
            assert!(
                resolve(&env(&[(HF_HUB_DISABLE_XET, value)]), None, None)
                    .await
                    .xet_disabled,
                "{value}"
            );
        }
        for value in ["0", "false", "no", "off", ""] {
            assert!(
                !resolve(&env(&[(HF_HUB_OFFLINE, value)]), None, None)
                    .await
                    .offline,
                "{value}"
            );
        }
    }

    #[tokio::test]
    async fn an_endpoint_must_be_https_or_loopback() {
        for valid in [
            "https://huggingface.co",
            "https://artifacts.example/api/huggingfaceml/hf-remote",
            "http://localhost:8080",
            "http://127.0.0.1:9000",
            "http://[::1]:9000",
        ] {
            resolve(&env(&[]), None, Some(valid)).await;
        }
        for invalid in [
            "http://mirror.example",
            "ftp://huggingface.co",
            "https://user:pass@huggingface.co",
            "https://huggingface.co?x=1",
            "not a url",
        ] {
            let error = Settings::resolve_in(&env(&[]), None, Some(invalid))
                .await
                .expect_err(invalid);
            assert_eq!(
                error.to_string(),
                format!(
                    "`hf_endpoint` {invalid:?} is not a valid Hugging Face Hub endpoint: use an https:// URL such as 'https://huggingface.co' (http:// is accepted only for a loopback address)"
                )
            );
        }
        let error =
            Settings::resolve_in(&env(&[(HF_ENDPOINT, "http://mirror.example")]), None, None)
                .await
                .expect_err("cleartext");
        assert!(
            error
                .to_string()
                .starts_with("`HF_ENDPOINT` \"http://mirror.example\""),
            "{error}"
        );
    }
}
