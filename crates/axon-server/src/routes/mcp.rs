//! Model Context Protocol endpoint (Streamable HTTP transport, stateless).
//!
//! Exposes the gateway's tools to MCP clients: `initialize`, `ping`,
//! `tools/list` and `tools/call`. Each request is answered with a single JSON
//! response; server-initiated messages are not used, so no SSE channel or
//! session is needed. Authentication and authorization are exactly those of
//! the REST API, because both go through the same gateway.

use axon_core::{gateway::GatewayError, tool::ToolError};
use axum::{
    Json,
    extract::{State, rejection::JsonRejection},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::Deserialize;
use serde_json::{Value, json};

use crate::{AppState, auth::Caller};

const SUPPORTED_VERSIONS: [&str; 2] = ["2025-06-18", "2025-03-26"];

// JSON-RPC 2.0 error codes.
const PARSE_ERROR: i64 = -32700;
const INVALID_REQUEST: i64 = -32600;
const METHOD_NOT_FOUND: i64 = -32601;
const INVALID_PARAMS: i64 = -32602;

#[derive(Debug, Deserialize)]
struct Request {
    jsonrpc: String,
    /// Absent for notifications.
    #[serde(default)]
    id: Option<Value>,
    method: String,
    #[serde(default)]
    params: Value,
}

#[derive(Debug, Deserialize)]
struct CallParams {
    name: String,
    #[serde(default = "empty_object")]
    arguments: Value,
}

fn empty_object() -> Value {
    json!({})
}

struct RpcError {
    code: i64,
    message: String,
}

impl RpcError {
    fn new(code: i64, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

fn error_response(id: Value, error: RpcError) -> Response {
    Json(json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": { "code": error.code, "message": error.message },
    }))
    .into_response()
}

pub async fn handle(
    State(state): State<AppState>,
    Caller(caller): Caller,
    body: Result<Json<Value>, JsonRejection>,
) -> Response {
    let Ok(Json(body)) = body else {
        return error_response(
            Value::Null,
            RpcError::new(PARSE_ERROR, "request body is not valid JSON"),
        );
    };
    // Keep the id for the error path even if the envelope is otherwise invalid.
    let id = body.get("id").cloned().unwrap_or(Value::Null);
    let request: Request = match serde_json::from_value(body) {
        Ok(request) => request,
        Err(error) => return error_response(id, RpcError::new(INVALID_REQUEST, error.to_string())),
    };
    if request.jsonrpc != "2.0" {
        return error_response(
            id,
            RpcError::new(INVALID_REQUEST, "jsonrpc must be \"2.0\""),
        );
    }
    // Notifications (no id) are acknowledged and never answered.
    let Some(id) = request.id else {
        return StatusCode::ACCEPTED.into_response();
    };

    let result = match request.method.as_str() {
        "initialize" => Ok(initialize(&request.params)),
        "ping" => Ok(json!({})),
        "tools/list" => Ok(list_tools(&state, &caller)),
        "tools/call" => call_tool(&state, &caller, request.params).await,
        other => Err(RpcError::new(
            METHOD_NOT_FOUND,
            format!("method `{other}` is not supported"),
        )),
    };
    match result {
        Ok(result) => Json(json!({ "jsonrpc": "2.0", "id": id, "result": result })).into_response(),
        Err(error) => error_response(id, error),
    }
}

fn initialize(params: &Value) -> Value {
    // Agree to the client's version if we speak it, otherwise offer our latest.
    let requested = params.get("protocolVersion").and_then(Value::as_str);
    let version = SUPPORTED_VERSIONS
        .into_iter()
        .find(|v| Some(*v) == requested)
        .unwrap_or(SUPPORTED_VERSIONS[0]);
    json!({
        "protocolVersion": version,
        "capabilities": { "tools": { "listChanged": false } },
        "serverInfo": { "name": "axon", "version": env!("CARGO_PKG_VERSION") },
        "instructions": "Scientific tools behind the axon gateway. Every result carries provenance in `_meta`.",
    })
}

fn list_tools(state: &AppState, caller: &axon_core::PrincipalId) -> Value {
    let tools: Vec<Value> = state
        .gateway
        .tools_for(caller)
        .into_iter()
        .map(|tool| {
            json!({
                "name": tool.name,
                "description": tool.description,
                "inputSchema": tool.input_schema,
                "_meta": { "axon/version": tool.version },
            })
        })
        .collect();
    json!({ "tools": tools })
}

async fn call_tool(
    state: &AppState,
    caller: &axon_core::PrincipalId,
    params: Value,
) -> Result<Value, RpcError> {
    let params: CallParams =
        serde_json::from_value(params).map_err(|e| RpcError::new(INVALID_PARAMS, e.to_string()))?;

    match state
        .gateway
        .invoke(caller, &params.name, params.arguments)
        .await
    {
        Ok(invocation) => Ok(json!({
            "content": [{ "type": "text", "text": invocation.output.to_string() }],
            "structuredContent": invocation.output,
            "isError": false,
            "_meta": { "axon/provenance": invocation.provenance },
        })),
        // Per the MCP spec, a tool the client cannot call is a protocol error.
        // Unknown and forbidden are reported identically so the endpoint does
        // not reveal which tools exist to callers who may not use them.
        Err(GatewayError::UnknownTool(_) | GatewayError::Forbidden { .. }) => Err(RpcError::new(
            INVALID_PARAMS,
            format!("unknown tool `{}`", params.name),
        )),
        Err(GatewayError::Tool(ToolError::InvalidInput(message))) => {
            Err(RpcError::new(INVALID_PARAMS, message))
        }
        // ...while a tool that ran and failed is a result the model can read
        // and react to.
        Err(error) => Ok(json!({
            "content": [{ "type": "text", "text": error.to_string() }],
            "isError": true,
        })),
    }
}
