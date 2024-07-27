use crate::response::ApiResponse;
use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
};
use derive_more::From;
use sqlx::Error as SqlxError;
use std::env::VarError;
use thiserror::Error;
use tracing::subscriber::SetGlobalDefaultError;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Database error: {0}")]
    SqlError(String),
    #[error("Request error: {0}")]
    TracingError(#[from] SetGlobalDefaultError),
    #[error("Io error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("Io error: {0}")]
    EnvVarError(#[from] VarError),
    #[error("General error: {0}")]
    GeneralError(#[from] Box<dyn std::error::Error + Send + Sync + 'static>),
    #[error("MBN error: {0}")]
    MbnError(#[from] mbn::error::Error),
}

impl From<SqlxError> for Error {
    fn from(error: SqlxError) -> Self {
        if let Some(pg_error) = error.as_database_error() {
            return Error::SqlError(pg_error.message().to_string());
        } else {
            Error::SqlError("Database error.".to_string())
        }
    }
}

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            Error::SqlError(ref msg) if msg.contains("Duplicate entry error") => {
                (StatusCode::CONFLICT, msg.clone())
            }
            Error::SqlError(ref msg) if msg.contains("Not null violation") => {
                (StatusCode::BAD_REQUEST, msg.clone())
            }
            Error::SqlError(ref msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg.clone()),
            Error::TracingError(ref msg) => (StatusCode::BAD_REQUEST, msg.to_string()),
            Error::IoError(ref msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg.to_string()),
            Error::EnvVarError(ref msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg.to_string()),
            Error::GeneralError(ref msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg.to_string()),
            Error::MbnError(ref msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg.to_string()),
        };

        ApiResponse {
            status: "failed".to_string(),
            message,
            code: status.as_u16(),
            data: None::<()>,
        }
        .into_response()
    }
}

pub type Result<T> = std::result::Result<T, Error>;
