use axon_core::gateway::Invocation;
use axum::{
    Json,
    extract::{Path, State, rejection::JsonRejection},
};
use serde_json::{Value, json};

use crate::{AppState, auth::Caller, error::ApiError};

/// Tools the caller may invoke. Tools they may not use are not advertised.
pub async fn list(State(state): State<AppState>, Caller(caller): Caller) -> Json<Value> {
    Json(json!({ "tools": state.gateway.tools_for(&caller) }))
}

pub async fn invoke(
    State(state): State<AppState>,
    Caller(caller): Caller,
    Path(name): Path<String>,
    input: Result<Json<Value>, JsonRejection>,
) -> Result<Json<Invocation>, ApiError> {
    let Json(input) = input?;
    Ok(Json(state.gateway.invoke(&caller, &name, input).await?))
}
