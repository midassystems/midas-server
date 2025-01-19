use crate::response::ApiResponse;
use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
};
use bytes::Bytes;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Database error: {0}")]
    SqlError(#[from] sqlx::Error),
    #[error("Request error: {0}")]
    TracingError(#[from] tracing::subscriber::SetGlobalDefaultError),
    #[error("Io error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("Io error: {0}")]
    EnvVarError(#[from] std::env::VarError),
    #[error("General error: {0}")]
    GeneralError(#[from] Box<dyn std::error::Error + Send + Sync + 'static>),
    #[error("MBN error: {0}")]
    MbnError(#[from] mbn::error::Error),
    #[error("Custom error: {0}")]
    CustomError(String),
    // #[error("Stream error")]
    // StreamError(Bytes),
}

impl Error {
    pub fn bytes(self) -> Bytes {
        let response: ApiResponse<String> = self.into();
        response.bytes()
    }
}

impl Into<ApiResponse<String>> for Error {
    fn into(self) -> ApiResponse<String> {
        let (status, message) = match self {
            Error::SqlError(ref msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg.to_string()),
            Error::TracingError(ref msg) => (StatusCode::BAD_REQUEST, msg.to_string()),
            Error::IoError(ref msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg.to_string()),
            Error::EnvVarError(ref msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg.to_string()),
            Error::GeneralError(ref msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg.to_string()),
            Error::MbnError(ref msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg.to_string()),
            Error::CustomError(ref msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg.to_string()),
            // Error::StreamError(_) => panic!("StreamError should not be converted to ApiResponse"),
            // Error::StreamError(ref msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg),
        };

        ApiResponse {
            status: "failed".to_string(),
            message,
            code: status.as_u16(),
            data: "".to_string(),
        }
    }
}

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        let response: ApiResponse<String> = self.into();
        response.into_response()
    }
}

#[macro_export]
macro_rules! error {
    ($variant:ident, $($arg:tt)*) => {
        Error::$variant(format!($($arg)*))
    };
}

pub type Result<T> = std::result::Result<T, Error>;
// pub type ResponseREsult = std /:

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_macro() {
        let error = error!(CustomError, "Testing 123 : {}", 69);
        let x_error = Error::CustomError(format!("Testing 123 : {}", 69));

        // Test
        assert_eq!(error.to_string(), x_error.to_string());
    }
}
