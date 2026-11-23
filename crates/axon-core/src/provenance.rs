//! Provenance: who ran which tool version on what input, and what came out.

use chrono::{DateTime, SubsecRound, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::ids::PrincipalId;

/// Attached to every tool result the gateway returns.
///
/// The digests make results content-addressable: two invocations with the same
/// `tool`, `tool_version` and `input_sha256` are comparable, and a consumer can
/// verify an output it was handed against `output_sha256`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Provenance {
    pub invocation_id: Uuid,
    pub tool: String,
    pub tool_version: String,
    pub principal: PrincipalId,
    pub input_sha256: String,
    pub output_sha256: String,
    pub started_at: DateTime<Utc>,
    pub duration_ms: u64,
}

/// SHA-256 of a JSON value's canonical encoding, hex encoded.
///
/// `serde_json` keeps object keys sorted (the `preserve_order` feature is off),
/// so equal values always serialize to equal bytes.
pub fn digest(value: &Value) -> String {
    // Serializing a `Value` cannot fail: its keys are strings and it holds no
    // non-finite floats.
    digest_bytes(&serde_json::to_vec(value).unwrap_or_default())
}

pub fn digest_bytes(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

/// Current time at microsecond precision, which is what PostgreSQL's
/// `timestamptz` stores. Truncating up front keeps events identical before and
/// after a round trip through the database.
pub fn now() -> DateTime<Utc> {
    Utc::now().trunc_subsecs(6)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn digest_ignores_key_order_but_not_content() {
        let a: Value = serde_json::from_str(r#"{"a":1,"b":[1,2]}"#).expect("json");
        let b: Value = serde_json::from_str(r#"{"b":[1,2],"a":1}"#).expect("json");
        assert_eq!(digest(&a), digest(&b));
        assert_ne!(digest(&a), digest(&json!({"a": 1, "b": [2, 1]})));
    }

    #[test]
    fn digest_matches_known_vector() {
        // sha256("{}")
        assert_eq!(
            digest(&json!({})),
            "44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a"
        );
    }
}
