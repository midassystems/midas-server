use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct ApiResponse<T> {
    pub status: String,
    pub message: String,
    pub code: u16,
    pub data: T,
}

impl<T: Serialize> ApiResponse<T> {
    pub fn new(status: &str, message: &str, code: StatusCode, data: T) -> Self {
        Self {
            status: status.to_string(),
            message: message.to_string(),
            code: code.as_u16(),
            data,
        }
    }

    pub fn bytes(&self) -> Bytes {
        let json_string = serde_json::to_string(self).unwrap();
        Bytes::from(json_string)
    }
}

impl<T: Serialize> IntoResponse for ApiResponse<T> {
    fn into_response(self) -> Response {
        let status = StatusCode::from_u16(self.code).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
        let body = axum::Json(json!({
            "status": self.status,
            "message": self.message,
            "code": self.code,
            "data": self.data,
        }));
        (status, body).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_response() {
        let status = "success";
        let msg = "message";
        let code = StatusCode::OK;
        let response = ApiResponse::new(status, msg, code, "".to_string());

        // Test
        assert_eq!(response.status, status);
        assert_eq!(response.code, 200);
        assert_eq!(response.message, msg);
        assert_eq!(response.data, "");
    }

    #[test]
    fn test_into_bytes() {
        let status = "success";
        let msg = "message";
        let code = StatusCode::OK;
        let response = ApiResponse::new(status, msg, code, "".to_string());

        let bytes = response.bytes();

        // Test
        assert!(bytes.len() > 0);
    }
}
