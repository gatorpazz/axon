//! The tool gateway: the single choke point through which tools are called.
//!
//! Every invocation is authorized, bounded in time and output size, counted,
//! and returned with [`Provenance`]. Agents, the job runner, the REST API and
//! the MCP endpoint all go through [`Gateway::invoke`]; none of them hold a
//! tool directly.

use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{Duration, Instant},
};

use serde::Serialize;
use serde_json::Value;
use uuid::Uuid;

use crate::{
    authz::{Authorizer, ObjectRef},
    ids::PrincipalId,
    metrics::Metrics,
    provenance::{self, Provenance},
    tool::{DynTool, Tool, ToolContext, ToolDescriptor, ToolError, erase},
};

/// The relation a principal needs on `tool:<name>` to call it.
pub const INVOKER: &str = "invoker";

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Invocation {
    pub output: Value,
    pub provenance: Provenance,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum GatewayError {
    #[error("unknown tool `{0}`")]
    UnknownTool(String),
    #[error("`{principal}` may not invoke `{tool}`")]
    Forbidden {
        principal: PrincipalId,
        tool: String,
    },
    #[error("`{tool}` timed out after {after:?}")]
    Timeout { tool: String, after: Duration },
    #[error("`{tool}` produced {bytes} bytes of output, over the {limit} byte limit")]
    OutputTooLarge {
        tool: String,
        bytes: usize,
        limit: usize,
    },
    #[error(transparent)]
    Tool(#[from] ToolError),
}

impl GatewayError {
    /// Bounded label for metrics.
    fn outcome(&self) -> &'static str {
        match self {
            Self::UnknownTool(_) => "unknown_tool",
            Self::Forbidden { .. } => "forbidden",
            Self::Timeout { .. } => "timeout",
            Self::OutputTooLarge { .. } => "output_too_large",
            Self::Tool(ToolError::InvalidInput(_)) => "invalid_input",
            Self::Tool(ToolError::Failed(_)) => "error",
        }
    }
}

pub struct Gateway {
    tools: BTreeMap<&'static str, Arc<dyn DynTool>>,
    authz: Arc<Authorizer>,
    metrics: Arc<Metrics>,
    timeout: Duration,
    max_output_bytes: usize,
}

impl std::fmt::Debug for Gateway {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Gateway")
            .field("tools", &self.tools.keys().collect::<Vec<_>>())
            .field("timeout", &self.timeout)
            .field("max_output_bytes", &self.max_output_bytes)
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
pub struct GatewayBuilder {
    gateway: Gateway,
}

impl GatewayBuilder {
    /// Registers a tool. A later registration under the same name replaces an
    /// earlier one.
    pub fn register<T: Tool>(mut self, tool: T) -> Self {
        self.gateway.tools.insert(T::NAME, erase(tool));
        self
    }

    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.gateway.timeout = timeout;
        self
    }

    pub fn max_output_bytes(mut self, limit: usize) -> Self {
        self.gateway.max_output_bytes = limit;
        self
    }

    pub fn build(self) -> Gateway {
        self.gateway
    }
}

impl Gateway {
    pub fn builder(authz: Arc<Authorizer>, metrics: Arc<Metrics>) -> GatewayBuilder {
        GatewayBuilder {
            gateway: Self {
                tools: BTreeMap::new(),
                authz,
                metrics,
                timeout: Duration::from_secs(30),
                max_output_bytes: 1 << 20,
            },
        }
    }

    /// Resolves a tool and checks that `principal` may invoke it.
    pub fn authorize(
        &self,
        principal: &PrincipalId,
        tool: &str,
    ) -> Result<&Arc<dyn DynTool>, GatewayError> {
        let found = self
            .tools
            .get(tool)
            .ok_or_else(|| GatewayError::UnknownTool(tool.to_owned()))?;
        if self.authz.check(principal, INVOKER, &ObjectRef::tool(tool)) {
            Ok(found)
        } else {
            Err(GatewayError::Forbidden {
                principal: principal.clone(),
                tool: tool.to_owned(),
            })
        }
    }

    /// The tools `principal` is allowed to invoke, sorted by name.
    pub fn tools_for(&self, principal: &PrincipalId) -> Vec<&ToolDescriptor> {
        self.tools
            .values()
            .map(|tool| tool.descriptor())
            .filter(|d| {
                self.authz
                    .check(principal, INVOKER, &ObjectRef::tool(d.name))
            })
            .collect()
    }

    pub async fn invoke(
        &self,
        principal: &PrincipalId,
        tool: &str,
        input: Value,
    ) -> Result<Invocation, GatewayError> {
        let result = self.invoke_inner(principal, tool, input).await;
        // Label with the registered name only, so callers cannot mint
        // unbounded label values by probing for tools that do not exist.
        let label = self
            .tools
            .get_key_value(tool)
            .map_or("unknown", |(name, _)| name);
        let outcome = result.as_ref().map_or_else(GatewayError::outcome, |_| "ok");
        self.metrics.inc(
            "axon_tool_invocations_total",
            &[("tool", label), ("outcome", outcome)],
        );
        match &result {
            Ok(inv) => {
                self.metrics.add(
                    "axon_tool_duration_ms_total",
                    &[("tool", label)],
                    inv.provenance.duration_ms,
                );
                tracing::debug!(%principal, tool, invocation_id = %inv.provenance.invocation_id, "tool ok");
            }
            Err(error) => tracing::warn!(%principal, tool, %error, "tool invocation failed"),
        }
        result
    }

    async fn invoke_inner(
        &self,
        principal: &PrincipalId,
        name: &str,
        input: Value,
    ) -> Result<Invocation, GatewayError> {
        let tool = self.authorize(principal, name)?;
        let descriptor = tool.descriptor();
        let ctx = ToolContext {
            principal: principal.clone(),
            invocation_id: Uuid::now_v7(),
        };
        let input_sha256 = provenance::digest(&input);
        let started_at = provenance::now();
        let clock = Instant::now();

        let output = tokio::time::timeout(self.timeout, tool.call_json(&ctx, input))
            .await
            .map_err(|_| GatewayError::Timeout {
                tool: name.to_owned(),
                after: self.timeout,
            })??;

        let encoded = serde_json::to_vec(&output).unwrap_or_default();
        if encoded.len() > self.max_output_bytes {
            return Err(GatewayError::OutputTooLarge {
                tool: name.to_owned(),
                bytes: encoded.len(),
                limit: self.max_output_bytes,
            });
        }

        Ok(Invocation {
            provenance: Provenance {
                invocation_id: ctx.invocation_id,
                tool: descriptor.name.to_owned(),
                tool_version: descriptor.version.to_owned(),
                principal: ctx.principal,
                input_sha256,
                output_sha256: provenance::digest_bytes(&encoded),
                started_at,
                duration_ms: u64::try_from(clock.elapsed().as_millis()).unwrap_or(u64::MAX),
            },
            output,
        })
    }
}

#[cfg(test)]
mod tests {
    use schemars::JsonSchema;
    use serde::Deserialize;
    use serde_json::json;

    use super::*;

    #[derive(Deserialize, JsonSchema)]
    #[serde(deny_unknown_fields)]
    struct EchoInput {
        text: String,
        #[serde(default)]
        repeat: Option<usize>,
        #[serde(default)]
        sleep_ms: u64,
    }

    struct Echo;

    impl Tool for Echo {
        const NAME: &'static str = "test.echo";
        const VERSION: &'static str = "1.2.3";
        const DESCRIPTION: &'static str = "Echoes its input.";
        type Input = EchoInput;
        type Output = String;

        async fn call(&self, _: &ToolContext, input: EchoInput) -> Result<String, ToolError> {
            tokio::time::sleep(Duration::from_millis(input.sleep_ms)).await;
            Ok(input.text.repeat(input.repeat.unwrap_or(1)))
        }
    }

    fn alice() -> PrincipalId {
        "user:alice".parse().expect("valid")
    }

    fn gateway() -> (Gateway, Arc<Metrics>) {
        let authz = Authorizer::parse(["tool:test.echo#invoker@user:alice"]).expect("tuples");
        let metrics = Arc::new(Metrics::default());
        let gateway = Gateway::builder(Arc::new(authz), metrics.clone())
            .register(Echo)
            .timeout(Duration::from_millis(50))
            .max_output_bytes(64)
            .build();
        (gateway, metrics)
    }

    #[tokio::test]
    async fn stamps_provenance_on_success() {
        let (gateway, metrics) = gateway();
        let input = json!({"text": "hi"});
        let inv = gateway
            .invoke(&alice(), "test.echo", input.clone())
            .await
            .expect("invocation succeeds");

        assert_eq!(inv.output, json!("hi"));
        assert_eq!(inv.provenance.tool, "test.echo");
        assert_eq!(inv.provenance.tool_version, "1.2.3");
        assert_eq!(inv.provenance.principal, alice());
        assert_eq!(inv.provenance.input_sha256, provenance::digest(&input));
        assert_eq!(
            inv.provenance.output_sha256,
            provenance::digest(&inv.output)
        );
        assert_eq!(
            metrics.get(
                "axon_tool_invocations_total",
                &[("tool", "test.echo"), ("outcome", "ok")]
            ),
            1
        );
    }

    #[tokio::test]
    async fn rejects_unauthorized_unknown_and_invalid_calls() {
        let (gateway, metrics) = gateway();
        let bob: PrincipalId = "user:bob".parse().expect("valid");

        assert!(matches!(
            gateway
                .invoke(&bob, "test.echo", json!({"text": "hi"}))
                .await,
            Err(GatewayError::Forbidden { .. })
        ));
        assert!(matches!(
            gateway.invoke(&alice(), "nope", json!({})).await,
            Err(GatewayError::UnknownTool(_))
        ));
        assert!(matches!(
            gateway
                .invoke(&alice(), "test.echo", json!({"text": 1}))
                .await,
            Err(GatewayError::Tool(ToolError::InvalidInput(_)))
        ));
        assert!(matches!(
            gateway
                .invoke(&alice(), "test.echo", json!({"text": "x", "extra": true}))
                .await,
            Err(GatewayError::Tool(ToolError::InvalidInput(_)))
        ));
        assert_eq!(
            metrics.get(
                "axon_tool_invocations_total",
                &[("tool", "unknown"), ("outcome", "unknown_tool")]
            ),
            1
        );
        assert!(gateway.tools_for(&bob).is_empty());
        assert_eq!(gateway.tools_for(&alice()).len(), 1);
    }

    #[tokio::test]
    async fn enforces_time_and_size_limits() {
        let (gateway, _) = gateway();
        assert!(matches!(
            gateway
                .invoke(&alice(), "test.echo", json!({"text": "x", "sleep_ms": 500}))
                .await,
            Err(GatewayError::Timeout { .. })
        ));
        assert!(matches!(
            gateway
                .invoke(&alice(), "test.echo", json!({"text": "x", "repeat": 100}))
                .await,
            Err(GatewayError::OutputTooLarge {
                bytes: 102,
                limit: 64,
                ..
            })
        ));
    }

    #[test]
    fn schema_is_derived_from_the_input_type() {
        let (gateway, _) = gateway();
        let schema = &gateway.tools_for(&alice())[0].input_schema;
        assert_eq!(schema["type"], "object");
        assert_eq!(schema["required"], json!(["text"]));
        assert_eq!(schema["properties"]["text"]["type"], "string");
    }
}
