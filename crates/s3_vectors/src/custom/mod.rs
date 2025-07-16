use rusoto_core::RusotoError;
use snafu::Snafu;
use std::{collections::HashMap, fmt::Display};

/// Reexport core errors from Rusoto with generic type
#[derive(Debug, Snafu)]
pub enum CoreError {
    /// A service-specific error occurred.
    #[snafu(display("A service-specific error occurred. {message}"))]
    Service { message: String },
    /// An error occurred dispatching the HTTP request
    #[snafu(display("An error occurred dispatching the HTTP request. {message}"))]
    HttpDispatch { message: String },
    /// An error was encountered with AWS credentials.
    /// Credentials message string
    #[snafu(display("An error was encountered with AWS credentials. {message}"))]
    Credentials { message: String },
    /// A validation error occurred.  Details from AWS are provided.
    #[snafu(display("A validation error occurred. {message}"))]
    Validation { message: String },
    /// An error occurred parsing the response payload.
    #[snafu(display("An error occurred parsing the response payload. {message}"))]
    ParseError { message: String },
    /// An unknown error occurred.  The raw HTTP response is provided.
    #[snafu(display("An unknown error occurred."))]
    Unknown {
        status: u16,
        body: bytes::Bytes,
        headers: HashMap<String, String>,
    },
    /// An error occurred when attempting to run a future as blocking
    Blocking,
}

pub fn inner_service_error<E>(err: &RusotoError<E>) -> Option<&E> {
    match err {
        RusotoError::Service(inner) => Some(inner),
        _ => None,
    }
}

impl<E> From<RusotoError<E>> for CoreError
where
    E: Display,
{
    fn from(value: RusotoError<E>) -> Self {
        match value {
            RusotoError::Blocking => CoreError::Blocking,
            RusotoError::Unknown(resp) => CoreError::Unknown {
                status: resp.status.as_u16(),
                body: resp.body,
                headers: resp
                    .headers
                    .into_iter()
                    .filter_map(|(n, s)| Some((n?.to_string(), s)))
                    .collect(),
            },
            RusotoError::ParseError(e) => CoreError::ParseError { message: e },
            RusotoError::Validation(e) => CoreError::Validation { message: e },
            RusotoError::Credentials(e) => CoreError::Credentials { message: e.message },
            RusotoError::HttpDispatch(e) => CoreError::HttpDispatch {
                message: e.to_string(),
            },
            RusotoError::Service(e) => {
                println!("S3 vector service error: {e}");
                CoreError::Service {
                    message: e.to_string(),
                }
            }
        }
    }
}
