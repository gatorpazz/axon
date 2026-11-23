//! Process configuration: CLI/environment options and the access file.

use std::{net::SocketAddr, path::PathBuf};

use axon_core::PrincipalId;
use clap::Parser;
use serde::Deserialize;
use sha2::{Digest, Sha256};

/// axon: a tool gateway and durable job runner for agent workloads.
#[derive(Debug, Parser)]
#[command(version, about)]
pub struct Cli {
    /// Address to listen on.
    #[arg(long, env = "AXON_BIND", default_value = "127.0.0.1:8080")]
    pub bind: SocketAddr,

    /// PostgreSQL connection string. Without it, jobs are kept in memory and
    /// lost on restart.
    #[arg(long, env = "AXON_DATABASE_URL", hide_env_values = true)]
    pub database_url: Option<String>,

    /// TOML file with principals and authorization tuples. Without it, the
    /// server starts with well-known development credentials.
    #[arg(long, env = "AXON_ACCESS_FILE")]
    pub access_file: Option<PathBuf>,

    /// Jobs allowed to run at once; the rest wait in `queued`.
    #[arg(long, env = "AXON_MAX_CONCURRENT_JOBS", default_value_t = 8)]
    pub max_concurrent_jobs: usize,

    /// Per-invocation tool timeout in seconds.
    #[arg(long, env = "AXON_TOOL_TIMEOUT_SECS", default_value_t = 30)]
    pub tool_timeout_secs: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("invalid access file: {0}")]
    Toml(#[from] toml::de::Error),
    #[error("principal `{0}`: token_sha256 must be 64 hex characters")]
    TokenHash(PrincipalId),
    #[error("two principals share the same token hash")]
    DuplicateToken,
    #[error(transparent)]
    Tuple(#[from] axon_core::authz::TupleParseError),
}

/// Who may authenticate, and what they may do.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AccessConfig {
    pub principals: Vec<PrincipalConfig>,
    /// Authorization tuples, `object#relation@subject`.
    #[serde(default)]
    pub tuples: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PrincipalConfig {
    pub id: PrincipalId,
    /// Hex SHA-256 of the bearer token. Only the hash is ever stored.
    pub token_sha256: String,
}

pub fn hash_token(token: &str) -> [u8; 32] {
    Sha256::digest(token.as_bytes()).into()
}

/// Development credentials: `(principal, bearer token)`.
pub const DEV_TOKENS: [(&str, &str); 2] = [
    ("user:alice", "dev-alice-token"),
    ("user:bob", "dev-bob-token"),
];

impl AccessConfig {
    pub fn from_toml(text: &str) -> Result<Self, ConfigError> {
        Ok(toml::from_str(text)?)
    }

    /// Fixed, publicly known credentials for local development and tests:
    /// alice is a researcher with access to every tool, bob may only call
    /// `stats.describe`.
    pub fn dev() -> Self {
        Self {
            principals: DEV_TOKENS
                .iter()
                .map(|(id, token)| PrincipalConfig {
                    id: id.parse().expect("dev principal ids are valid"),
                    token_sha256: hex::encode(hash_token(token)),
                })
                .collect(),
            tuples: [
                "group:researchers#member@user:alice",
                "tool:*#invoker@group:researchers#member",
                "tool:stats.describe#invoker@user:bob",
            ]
            .map(str::to_owned)
            .to_vec(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_the_example_access_file() {
        let example = include_str!("../../../access.example.toml");
        let config = AccessConfig::from_toml(example).expect("example parses");
        assert_eq!(config.principals.len(), 2);
        assert!(
            axon_core::authz::Authorizer::parse(config.tuples.iter().map(String::as_str)).is_ok()
        );
    }

    #[test]
    fn rejects_unknown_keys_and_bad_principals() {
        assert!(AccessConfig::from_toml("principals = []\nextra = 1").is_err());
        assert!(
            AccessConfig::from_toml("[[principals]]\nid = \"alice\"\ntoken_sha256 = \"00\"")
                .is_err()
        );
    }
}
