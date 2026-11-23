//! The tool abstraction.
//!
//! Tool authors implement the statically typed [`Tool`] trait. The gateway
//! works with `Arc<dyn DynTool>`, and [`erase`] bridges the two: it derives the
//! JSON Schema from the input type and moves (de)serialization to the boundary,
//! so a tool body never touches untyped JSON.

use std::{future::Future, sync::Arc};

use async_trait::async_trait;
use schemars::JsonSchema;
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;
use uuid::Uuid;

use crate::ids::PrincipalId;

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ToolDescriptor {
    pub name: &'static str,
    pub version: &'static str,
    pub description: &'static str,
    pub input_schema: Value,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ToolError {
    #[error("invalid input: {0}")]
    InvalidInput(String),
    #[error("{0}")]
    Failed(String),
}

/// Per-invocation context handed to a tool.
#[derive(Debug, Clone)]
pub struct ToolContext {
    pub principal: PrincipalId,
    pub invocation_id: Uuid,
}

/// A statically typed tool.
pub trait Tool: Send + Sync + 'static {
    /// Stable, dot-namespaced name, e.g. `stats.describe`.
    const NAME: &'static str;
    /// Bumped whenever outputs may change for the same input.
    const VERSION: &'static str;
    const DESCRIPTION: &'static str;

    type Input: DeserializeOwned + JsonSchema + Send;
    type Output: Serialize;

    fn call(
        &self,
        ctx: &ToolContext,
        input: Self::Input,
    ) -> impl Future<Output = Result<Self::Output, ToolError>> + Send;
}

/// Object-safe view of a tool, speaking JSON.
#[async_trait]
pub trait DynTool: Send + Sync + 'static {
    fn descriptor(&self) -> &ToolDescriptor;
    async fn call_json(&self, ctx: &ToolContext, input: Value) -> Result<Value, ToolError>;
}

struct Erased<T> {
    tool: T,
    descriptor: ToolDescriptor,
}

#[async_trait]
impl<T: Tool> DynTool for Erased<T> {
    fn descriptor(&self) -> &ToolDescriptor {
        &self.descriptor
    }

    async fn call_json(&self, ctx: &ToolContext, input: Value) -> Result<Value, ToolError> {
        let input: T::Input =
            serde_json::from_value(input).map_err(|e| ToolError::InvalidInput(e.to_string()))?;
        let output = self.tool.call(ctx, input).await?;
        serde_json::to_value(output)
            .map_err(|e| ToolError::Failed(format!("output is not representable as JSON: {e}")))
    }
}

/// Type-erases a [`Tool`] for registration with the gateway.
pub fn erase<T: Tool>(tool: T) -> Arc<dyn DynTool> {
    Arc::new(Erased {
        tool,
        descriptor: ToolDescriptor {
            name: T::NAME,
            version: T::VERSION,
            description: T::DESCRIPTION,
            input_schema: schemars::schema_for!(T::Input).to_value(),
        },
    })
}
