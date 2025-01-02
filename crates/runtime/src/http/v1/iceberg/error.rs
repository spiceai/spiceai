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

use axum::{
    http::status,
    response::{IntoResponse, Response},
    Json,
};
use serde::{Serialize, Serializer};

#[allow(dead_code)]
#[derive(Debug)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
enum IcebergErrorType {
    NoSuchNamespaceException,
    BadRequestException,
    InternalServerError,
}

impl IcebergErrorType {
    fn code(&self) -> u16 {
        match self {
            IcebergErrorType::NoSuchNamespaceException => 404,
            IcebergErrorType::BadRequestException => 400,
            IcebergErrorType::InternalServerError => 500,
        }
    }
}

impl Serialize for IcebergErrorType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            IcebergErrorType::NoSuchNamespaceException => {
                serializer.serialize_str("NoSuchNamespaceException")
            }
            IcebergErrorType::BadRequestException => {
                serializer.serialize_str("BadRequestException")
            }
            IcebergErrorType::InternalServerError => {
                serializer.serialize_str("InternalServerError")
            }
        }
    }
}

#[derive(Debug, Copy, Clone)]
pub enum InternalServerErrorCode {
    InvalidSchema,
}

impl std::fmt::Display for InternalServerErrorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            InternalServerErrorCode::InvalidSchema => write!(f, "DF_INVALID_SCHEMA"),
        }
    }
}

#[derive(Debug, Serialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
struct IcebergError {
    message: String,
    r#type: IcebergErrorType,
    code: u16,
}

#[derive(Debug, Serialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct IcebergResponseError {
    error: IcebergError,
}

impl IcebergResponseError {
    pub fn no_such_namespace(message: String) -> Self {
        Self {
            error: IcebergError {
                message,
                r#type: IcebergErrorType::NoSuchNamespaceException,
                code: IcebergErrorType::NoSuchNamespaceException.code(),
            },
        }
    }

    #[allow(dead_code)]
    pub fn bad_request(message: String) -> Self {
        Self {
            error: IcebergError {
                message,
                r#type: IcebergErrorType::BadRequestException,
                code: IcebergErrorType::BadRequestException.code(),
            },
        }
    }

    #[allow(dead_code)]
    pub fn internal(code: InternalServerErrorCode) -> Self {
        Self {
            error: IcebergError {
                message: format!("Internal Server Error: {code}"),
                r#type: IcebergErrorType::InternalServerError,
                code: IcebergErrorType::InternalServerError.code(),
            },
        }
    }
}

impl IntoResponse for IcebergResponseError {
    fn into_response(self) -> Response {
        match self.error.code {
            404 => (status::StatusCode::NOT_FOUND, Json(self)).into_response(),
            400 => (status::StatusCode::BAD_REQUEST, Json(self)).into_response(),
            _ => (status::StatusCode::INTERNAL_SERVER_ERROR, Json(self)).into_response(),
        }
    }
}
