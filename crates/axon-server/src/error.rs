//! One error type for the HTTP surface, rendered as
//! `{"error": {"code": "...", "message": "..."}}`.

use axon_core::{
    gateway::GatewayError,
    job::{ReplayError, SpecError},
    store::StoreError,
    tool::ToolError,
};
use axum::{
    Json,
    extract::rejection::{JsonRejection, PathRejection},
    http::{HeaderValue, StatusCode, header::WWW_AUTHENTICATE},
    response::{IntoResponse, Response},
};
use serde_json::json;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiError {
    pub status: StatusCode,
    pub code: &'static str,
    pub message: String,
}

impl ApiError {
    pub fn new(status: StatusCode, code: &'static str, message: impl Into<String>) -> Self {
        Self {
            status,
            code,
            message: message.into(),
        }
    }

    pub fn unauthorized() -> Self {
        Self::new(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "missing or invalid bearer token",
        )
    }

    pub fn bad_request(message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, "bad_request", message)
    }

    pub fn conflict(message: impl Into<String>) -> Self {
        Self::new(StatusCode::CONFLICT, "conflict", message)
    }

    fn internal() -> Self {
        // Details go to the log, never to the caller.
        Self::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal",
            "internal error",
        )
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let body = Json(json!({ "error": { "code": self.code, "message": self.message } }));
        let mut response = (self.status, body).into_response();
        if self.status == StatusCode::UNAUTHORIZED {
            response
                .headers_mut()
                .insert(WWW_AUTHENTICATE, HeaderValue::from_static("Bearer"));
        }
        response
    }
}

impl From<StoreError> for ApiError {
    fn from(error: StoreError) -> Self {
        match error {
            StoreError::NotFound => Self::new(StatusCode::NOT_FOUND, "not_found", "job not found"),
            StoreError::Conflict { .. } => Self::conflict(error.to_string()),
            StoreError::InvalidAppend(_) => Self::bad_request(error.to_string()),
            StoreError::Backend(source) => {
                tracing::error!(error = %source, "storage backend error");
                Self::internal()
            }
        }
    }
}

impl From<GatewayError> for ApiError {
    fn from(error: GatewayError) -> Self {
        let (status, code) = match &error {
            GatewayError::UnknownTool(_) => (StatusCode::NOT_FOUND, "unknown_tool"),
            GatewayError::Forbidden { .. } => (StatusCode::FORBIDDEN, "forbidden"),
            GatewayError::Timeout { .. } => (StatusCode::GATEWAY_TIMEOUT, "tool_timeout"),
            GatewayError::OutputTooLarge { .. } => {
                (StatusCode::BAD_GATEWAY, "tool_output_too_large")
            }
            GatewayError::Tool(ToolError::InvalidInput(_)) => {
                (StatusCode::UNPROCESSABLE_ENTITY, "invalid_tool_input")
            }
            GatewayError::Tool(ToolError::Failed(_)) => {
                (StatusCode::UNPROCESSABLE_ENTITY, "tool_failed")
            }
        };
        Self::new(status, code, error.to_string())
    }
}

impl From<SpecError> for ApiError {
    fn from(error: SpecError) -> Self {
        Self::new(
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid_job_spec",
            error.to_string(),
        )
    }
}

impl From<ReplayError> for ApiError {
    fn from(error: ReplayError) -> Self {
        tracing::error!(%error, "corrupt job event log");
        Self::internal()
    }
}

impl From<JsonRejection> for ApiError {
    fn from(rejection: JsonRejection) -> Self {
        Self::new(rejection.status(), "invalid_body", rejection.body_text())
    }
}

impl From<PathRejection> for ApiError {
    fn from(_: PathRejection) -> Self {
        // A malformed job id cannot name an existing job.
        Self::new(StatusCode::NOT_FOUND, "not_found", "job not found")
    }
}
