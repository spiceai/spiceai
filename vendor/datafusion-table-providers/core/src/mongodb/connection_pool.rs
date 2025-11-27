use crate::mongodb::utils::unnest::{DuplicateBehavior, UnnestBehavior, UnnestParameters};
use crate::mongodb::{
    connection::MongoDBConnection, ConnectionFailedSnafu, Error, InvalidUriSnafu, Result,
};
use mongodb::{
    bson::doc,
    error::ErrorKind,
    options::{ClientOptions, Tls, TlsOptions},
    Client,
};
use secrecy::{ExposeSecret, SecretString};
use snafu::ResultExt;
use std::{collections::HashMap, path::PathBuf, sync::Arc};

#[derive(Clone, Debug)]
pub struct MongoDBConnectionPool {
    client: Arc<Client>,
    db_name: String,
    tz: Option<String>,
    unnest_parameters: UnnestParameters,
    num_documents_to_infer_schema: i64,
}

const DEFAULT_HOST: &str = "localhost";
const DEFAULT_PORT: &str = "27017";
const DEFAULT_DATABASE: &str = "default";
const DEFAULT_MIN_POOL_SIZE: u32 = 10;
const DEFAULT_MAX_POOL_SIZE: u32 = 100;
const DEFAULT_SSL_MODE: &str = "required";
const DEFAULT_UNNEST_DEPTH: &str = "0";
const DEFAULT_SCHEMA_INFER_MAX_RECORDS: u32 = 400;

impl MongoDBConnectionPool {
    pub async fn new(params: HashMap<String, SecretString>) -> Result<Self> {
        let params = crate::util::remove_prefix_from_hashmap_keys(params, "mongodb_");

        let (uri, explicit_db_name) = build_connection_uri(&params)?;

        let mut client_options = ClientOptions::parse(&uri).await.context(InvalidUriSnafu)?;

        configure_pool_size(&mut client_options, &params)?;
        configure_tls(&mut client_options, &params)?;

        let db_name = explicit_db_name
            .or(client_options.default_database.clone())
            .unwrap_or(DEFAULT_DATABASE.to_string());

        let client = Client::with_options(client_options).context(ConnectionFailedSnafu)?;

        let unnest_depth: usize =
            get_param_or_default(&params, "unnest_depth", DEFAULT_UNNEST_DEPTH)
                .parse()
                .map_err(|_| Error::InvalidParameter {
                    parameter_name: "unnest_depth".to_string(),
                })?;

        let num_documents_to_infer_schema = parse_u32_param(
            &params,
            "schema_infer_max_records",
            DEFAULT_SCHEMA_INFER_MAX_RECORDS,
        )? as i64;

        test_connection(&client, &db_name).await?;

        Ok(Self {
            client: Arc::new(client),
            db_name,
            tz: params
                .get("time_zone")
                .map(|t| t.expose_secret().to_string()),
            unnest_parameters: UnnestParameters {
                behavior: UnnestBehavior::Depth(unnest_depth),
                duplicate_behavior: DuplicateBehavior::Error,
            },
            num_documents_to_infer_schema,
        })
    }

    pub async fn connect(&self) -> Result<Box<MongoDBConnection>> {
        Ok(Box::new(MongoDBConnection::new(
            Arc::clone(&self.client),
            self.db_name.clone(),
            self.tz.clone(),
            self.unnest_parameters.clone(),
            self.num_documents_to_infer_schema,
        )))
    }
}

fn build_connection_uri(
    params: &HashMap<String, SecretString>,
) -> Result<(String, Option<String>)> {
    if let Some(uri) = params.get("connection_string") {
        return Ok((uri.expose_secret().to_string(), None));
    }

    let db_name = get_param_or_default(params, "db", DEFAULT_DATABASE);
    let host = get_param_or_default(params, "host", DEFAULT_HOST);
    let port = get_param_or_default(params, "port", DEFAULT_PORT);

    let auth = match (params.get("user"), params.get("pass")) {
        (Some(user), Some(pass)) => {
            format!("{}:{}@", user.expose_secret(), pass.expose_secret())
        }
        (Some(_), None) => {
            return Err(Error::InvalidParameter {
                parameter_name: "pass".to_string(),
            });
        }
        (None, Some(_)) => {
            return Err(Error::InvalidParameter {
                parameter_name: "user".to_string(),
            });
        }
        (None, None) => String::new(),
    };

    let mut query_params = Vec::new();
    if let Some(auth_source) = params.get("auth_source") {
        query_params.push(format!("authSource={}", auth_source.expose_secret()));
    }

    if let Some(direct_connection) = params.get("direct_connection") {
        query_params.push(format!(
            "directConnection={}",
            direct_connection.expose_secret()
        ));
    }

    let query_string = if query_params.is_empty() {
        String::new()
    } else {
        format!("?{}", query_params.join("&"))
    };

    let uri = format!("mongodb://{auth}{host}:{port}/{db_name}{query_string}");
    Ok((uri, Some(db_name.to_string())))
}

fn configure_pool_size(
    client_options: &mut ClientOptions,
    params: &HashMap<String, SecretString>,
) -> Result<()> {
    let pool_min = parse_u32_param(params, "pool_min", DEFAULT_MIN_POOL_SIZE)?;
    let pool_max = parse_u32_param(params, "pool_max", DEFAULT_MAX_POOL_SIZE)?;

    if pool_min > pool_max {
        return Err(Error::InvalidParameter {
            parameter_name: "pool_min/pool_max".to_string(),
        });
    }

    client_options.min_pool_size = Some(pool_min);
    client_options.max_pool_size = Some(pool_max);

    Ok(())
}

fn configure_tls(
    client_options: &mut ClientOptions,
    params: &HashMap<String, SecretString>,
) -> Result<()> {
    let has_explicit_tls_params =
        params.contains_key("sslmode") || params.contains_key("sslrootcert");

    if client_options.tls.is_some() && !has_explicit_tls_params {
        return Ok(());
    }

    let ssl_mode = get_param_or_default(params, "sslmode", DEFAULT_SSL_MODE);

    match ssl_mode.to_lowercase().as_str() {
        "disabled" | "required" | "preferred" => {}
        _ => {
            return Err(Error::InvalidParameter {
                parameter_name: "sslmode".to_string(),
            });
        }
    }

    let ssl_rootcert_path = if let Some(cert_path) = params.get("sslrootcert") {
        let path = PathBuf::from(cert_path.expose_secret());
        if !path.exists() {
            return Err(Error::InvalidRootCertPath {
                path: cert_path.expose_secret().to_string(),
            });
        }
        Some(path)
    } else {
        None
    };

    client_options.tls = build_tls_options(&ssl_mode, ssl_rootcert_path);
    Ok(())
}

fn build_tls_options(ssl_mode: &str, rootcert_path: Option<PathBuf>) -> Option<Tls> {
    if ssl_mode == "disabled" {
        return Some(Tls::Disabled);
    }

    let tls_options = match (rootcert_path, ssl_mode) {
        // Root cert + preferred
        (Some(path), "preferred") => TlsOptions::builder()
            .ca_file_path(Some(path))
            .allow_invalid_certificates(Some(true))
            .allow_invalid_hostnames(Some(true))
            .build(),

        // Root cert + required
        (Some(path), _) => TlsOptions::builder().ca_file_path(Some(path)).build(),

        // No root cert + preferred
        (None, "preferred") => TlsOptions::builder()
            .allow_invalid_certificates(Some(true))
            .allow_invalid_hostnames(Some(true))
            .build(),

        // No root cert + required
        (None, _) => TlsOptions::builder().build(),
    };

    Some(Tls::Enabled(tls_options))
}

async fn test_connection(client: &Client, db_name: &str) -> Result<()> {
    client
        .database(db_name)
        .run_command(doc! { "ping": 1 })
        .await
        .map_err(|err| match *err.kind {
            ErrorKind::Authentication { .. } => Error::InvalidUsernameOrPassword {},
            _ => Error::ConnectionFailed { source: err },
        })?;
    Ok(())
}

fn get_param_or_default(
    params: &HashMap<String, SecretString>,
    key: &str,
    default: &str,
) -> String {
    params
        .get(key)
        .map(|s| s.expose_secret().to_string())
        .unwrap_or_else(|| default.to_string())
}

fn parse_u32_param(params: &HashMap<String, SecretString>, key: &str, default: u32) -> Result<u32> {
    params
        .get(key)
        .map(|s| s.expose_secret().parse::<u32>())
        .transpose()
        .map_err(|_| Error::InvalidParameter {
            parameter_name: key.to_string(),
        })
        .map(|opt| opt.unwrap_or(default))
}

#[cfg(test)]
mod tests {
    use super::*;
    use mongodb::options::{Tls, TlsOptions};
    use secrecy::SecretString;
    use std::collections::HashMap;

    fn create_secret_string(value: &str) -> SecretString {
        SecretString::new(value.to_string().into_boxed_str())
    }

    fn create_params(pairs: Vec<(&str, &str)>) -> HashMap<String, SecretString> {
        pairs
            .into_iter()
            .map(|(k, v)| (k.to_string(), create_secret_string(v)))
            .collect()
    }

    #[test]
    fn test_build_connection_uri_with_connection_string() {
        let params = create_params(vec![(
            "connection_string",
            "mongodb://user:pass@example.com:27017/testdb",
        )]);

        let result = build_connection_uri(&params).unwrap();
        assert_eq!(result.0, "mongodb://user:pass@example.com:27017/testdb");
        assert_eq!(result.1, None);
    }

    #[test]
    fn test_build_connection_uri_with_individual_params() {
        let params = create_params(vec![
            ("db", "mydb"),
            ("host", "example.com"),
            ("port", "27018"),
            ("user", "testuser"),
            ("pass", "testpass"),
        ]);

        let result = build_connection_uri(&params).unwrap();
        assert_eq!(
            result.0,
            "mongodb://testuser:testpass@example.com:27018/mydb"
        );
        assert_eq!(result.1, Some("mydb".to_string()));
    }

    #[test]
    fn test_build_connection_uri_with_defaults() {
        let params = HashMap::new();

        let result = build_connection_uri(&params).unwrap();
        assert_eq!(result.0, "mongodb://localhost:27017/default");
        assert_eq!(result.1, Some("default".to_string()));
    }

    #[test]
    fn test_build_connection_uri_without_auth() {
        let params = create_params(vec![
            ("db", "testdb"),
            ("host", "localhost"),
            ("port", "27017"),
        ]);

        let result = build_connection_uri(&params).unwrap();
        assert_eq!(result.0, "mongodb://localhost:27017/testdb");
        assert_eq!(result.1, Some("testdb".to_string()));
    }

    #[test]
    fn test_build_connection_uri_user_without_password() {
        let params = create_params(vec![("user", "testuser")]);

        let result = build_connection_uri(&params);
        assert!(result.is_err());
        if let Err(Error::InvalidParameter { parameter_name }) = result {
            assert_eq!(parameter_name, "pass");
        } else {
            panic!("Expected InvalidParameter error for pass");
        }
    }

    #[test]
    fn test_build_connection_uri_password_without_user() {
        let params = create_params(vec![("pass", "testpass")]);

        let result = build_connection_uri(&params);
        assert!(result.is_err());
        if let Err(Error::InvalidParameter { parameter_name }) = result {
            assert_eq!(parameter_name, "user");
        } else {
            panic!("Expected InvalidParameter error for user");
        }
    }

    #[test]
    fn test_configure_pool_size_with_valid_params() {
        let mut client_options = ClientOptions::default();
        let params = create_params(vec![("pool_min", "5"), ("pool_max", "50")]);

        let result = configure_pool_size(&mut client_options, &params);
        assert!(result.is_ok());
        assert_eq!(client_options.min_pool_size, Some(5));
        assert_eq!(client_options.max_pool_size, Some(50));
    }

    #[test]
    fn test_configure_pool_size_with_defaults() {
        let mut client_options = ClientOptions::default();
        let params = HashMap::new();

        let result = configure_pool_size(&mut client_options, &params);
        assert!(result.is_ok());
        assert_eq!(client_options.min_pool_size, Some(DEFAULT_MIN_POOL_SIZE));
        assert_eq!(client_options.max_pool_size, Some(DEFAULT_MAX_POOL_SIZE));
    }

    #[test]
    fn test_configure_pool_size_min_greater_than_max() {
        let mut client_options = ClientOptions::default();
        let params = create_params(vec![("pool_min", "100"), ("pool_max", "50")]);

        let result = configure_pool_size(&mut client_options, &params);
        assert!(result.is_err());
        if let Err(Error::InvalidParameter { parameter_name }) = result {
            assert_eq!(parameter_name, "pool_min/pool_max");
        } else {
            panic!("Expected InvalidParameter error for pool_min/pool_max");
        }
    }

    #[test]
    fn test_configure_tls_skips_when_already_configured() {
        let mut client_options = ClientOptions::default();
        client_options.tls = Some(Tls::Enabled(TlsOptions::builder().build()));

        let params = HashMap::new();

        let result = configure_tls(&mut client_options, &params);
        assert!(result.is_ok());

        assert!(client_options.tls.is_some());
        if let Some(Tls::Enabled(_)) = client_options.tls {
        } else {
            panic!("Expected TLS to remain enabled");
        }
    }

    #[test]
    fn test_configure_tls_overrides_when_explicit_params() {
        let mut client_options = ClientOptions::default();
        client_options.tls = Some(Tls::Enabled(TlsOptions::builder().build()));

        let params = create_params(vec![("sslmode", "disabled")]);

        let result = configure_tls(&mut client_options, &params);
        assert!(result.is_ok());

        if let Some(Tls::Disabled) = client_options.tls {
        } else {
            panic!("Expected TLS to be disabled");
        }
    }

    #[test]
    fn test_configure_tls_with_required_mode() {
        let mut client_options = ClientOptions::default();
        let params = create_params(vec![("sslmode", "required")]);

        let result = configure_tls(&mut client_options, &params);
        assert!(result.is_ok());

        if let Some(Tls::Enabled(_)) = client_options.tls {
        } else {
            panic!("Expected TLS to be enabled");
        }
    }

    #[test]
    fn test_configure_tls_with_preferred_mode() {
        let mut client_options = ClientOptions::default();
        let params = create_params(vec![("sslmode", "preferred")]);

        let result = configure_tls(&mut client_options, &params);
        assert!(result.is_ok());

        if let Some(Tls::Enabled(_)) = client_options.tls {
        } else {
            panic!("Expected TLS to be enabled");
        }
    }

    #[test]
    fn test_configure_tls_with_disabled_mode() {
        let mut client_options = ClientOptions::default();
        let params = create_params(vec![("sslmode", "disabled")]);

        let result = configure_tls(&mut client_options, &params);
        assert!(result.is_ok());

        if let Some(Tls::Disabled) = client_options.tls {
        } else {
            panic!("Expected TLS to be disabled");
        }
    }

    #[test]
    fn test_configure_tls_with_invalid_mode() {
        let mut client_options = ClientOptions::default();
        let params = create_params(vec![("sslmode", "invalid_mode")]);

        let result = configure_tls(&mut client_options, &params);
        assert!(result.is_err());
        if let Err(Error::InvalidParameter { parameter_name }) = result {
            assert_eq!(parameter_name, "sslmode");
        } else {
            panic!("Expected InvalidParameter error for sslmode");
        }
    }

    #[test]
    fn test_configure_tls_with_nonexistent_cert_path() {
        let mut client_options = ClientOptions::default();
        let params = create_params(vec![
            ("sslmode", "required"),
            ("sslrootcert", "/nonexistent/path/cert.pem"),
        ]);

        let result = configure_tls(&mut client_options, &params);
        assert!(result.is_err());
        if let Err(Error::InvalidRootCertPath { path }) = result {
            assert_eq!(path, "/nonexistent/path/cert.pem");
        } else {
            panic!("Expected InvalidRootCertPath error");
        }
    }

    #[test]
    fn test_build_tls_options_disabled() {
        let result = build_tls_options("disabled", None);

        if let Some(Tls::Disabled) = result {
        } else {
            panic!("Expected TLS to be disabled");
        }
    }

    #[test]
    fn test_build_tls_options_required_without_cert() {
        let result = build_tls_options("required", None);

        if let Some(Tls::Enabled(_)) = result {
        } else {
            panic!("Expected TLS to be enabled");
        }
    }

    #[test]
    fn test_build_tls_options_preferred_without_cert() {
        let result = build_tls_options("preferred", None);

        if let Some(Tls::Enabled(_)) = result {
        } else {
            panic!("Expected TLS to be enabled");
        }
    }

    #[test]
    fn test_build_tls_options_with_cert_path() {
        let temp_dir = std::env::temp_dir();
        let cert_path = temp_dir.join("test_cert.pem");
        std::fs::write(&cert_path, "dummy cert content").unwrap();

        let result = build_tls_options("required", Some(cert_path.clone()));
        std::fs::remove_file(&cert_path).ok();

        if let Some(Tls::Enabled(_)) = result {
        } else {
            panic!("Expected TLS to be enabled with certificate");
        }
    }

    #[test]
    fn test_build_tls_options_preferred_with_cert_path() {
        let temp_dir = std::env::temp_dir();
        let cert_path = temp_dir.join("test_cert_preferred.pem");
        std::fs::write(&cert_path, "dummy cert content").unwrap();

        let result = build_tls_options("preferred", Some(cert_path.clone()));
        std::fs::remove_file(&cert_path).ok();

        if let Some(Tls::Enabled(_)) = result {
        } else {
            panic!("Expected TLS to be enabled with certificate in preferred mode");
        }
    }

    #[test]
    fn test_configure_tls_case_insensitive_ssl_mode() {
        let mut client_options = ClientOptions::default();
        let params = create_params(vec![("sslmode", "REQUIRED")]);

        let result = configure_tls(&mut client_options, &params);
        assert!(result.is_ok());

        if let Some(Tls::Enabled(_)) = client_options.tls {
        } else {
            panic!("Expected TLS to be enabled with uppercase mode");
        }
    }

    #[test]
    fn test_configure_tls_mixed_case_ssl_mode() {
        let mut client_options = ClientOptions::default();
        let params = create_params(vec![("sslmode", "Preferred")]);

        let result = configure_tls(&mut client_options, &params);
        assert!(result.is_ok());

        if let Some(Tls::Enabled(_)) = client_options.tls {
        } else {
            panic!("Expected TLS to be enabled with mixed case mode");
        }
    }

    #[test]
    fn test_configure_tls_with_rootcert_param_triggers_override() {
        let mut client_options = ClientOptions::default();
        client_options.tls = Some(Tls::Enabled(TlsOptions::builder().build()));

        let temp_dir = std::env::temp_dir();
        let cert_path = temp_dir.join("test_override_cert.pem");
        std::fs::write(&cert_path, "dummy cert content").unwrap();

        let params = create_params(vec![("sslrootcert", cert_path.to_str().unwrap())]);

        let result = configure_tls(&mut client_options, &params);
        std::fs::remove_file(&cert_path).ok();

        assert!(result.is_ok());

        if let Some(Tls::Enabled(_)) = client_options.tls {
        } else {
            panic!("Expected TLS to be enabled after override");
        }
    }
}
