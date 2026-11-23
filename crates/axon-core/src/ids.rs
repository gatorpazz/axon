//! Validated identifier newtypes.

use std::{fmt, str::FromStr};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// `true` for non-empty strings of `[A-Za-z0-9_.-]`.
pub(crate) fn is_ident(s: &str) -> bool {
    !s.is_empty()
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'.' | b'-'))
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid principal id `{0}`: expected `<kind>:<name>` using [A-Za-z0-9_.-]")]
pub struct InvalidPrincipalId(String);

/// An authenticated caller, e.g. `user:alice` or `service:ci`.
///
/// The `kind:name` shape is enforced at construction, so a principal can never
/// collide with reserved bare words such as the store's `system` actor.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct PrincipalId(String);

impl PrincipalId {
    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn kind(&self) -> &str {
        self.0.split_once(':').map_or("", |(kind, _)| kind)
    }
}

impl FromStr for PrincipalId {
    type Err = InvalidPrincipalId;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.split_once(':') {
            Some((kind, name)) if is_ident(kind) && is_ident(name) => Ok(Self(s.to_owned())),
            _ => Err(InvalidPrincipalId(s.to_owned())),
        }
    }
}

impl TryFrom<String> for PrincipalId {
    type Error = InvalidPrincipalId;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        s.parse()
    }
}

impl From<PrincipalId> for String {
    fn from(id: PrincipalId) -> Self {
        id.0
    }
}

impl fmt::Display for PrincipalId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Job identifier. UUIDv7, so ids sort by creation time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct JobId(Uuid);

impl JobId {
    #[allow(clippy::new_without_default)] // a "default" id that is random would be surprising
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }

    pub fn as_uuid(&self) -> Uuid {
        self.0
    }
}

impl From<Uuid> for JobId {
    fn from(id: Uuid) -> Self {
        Self(id)
    }
}

impl FromStr for JobId {
    type Err = uuid::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.parse().map(Self)
    }
}

impl fmt::Display for JobId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn principal_ids_require_kind_and_name() {
        assert!("user:alice".parse::<PrincipalId>().is_ok());
        assert_eq!(
            "service:ci-runner"
                .parse::<PrincipalId>()
                .map(|p| p.kind().to_owned()),
            Ok("service".to_owned())
        );
        for bad in [
            "", "alice", "system", "user:", ":alice", "user:*", "user:a b",
        ] {
            assert!(
                bad.parse::<PrincipalId>().is_err(),
                "{bad:?} should be rejected"
            );
        }
    }

    #[test]
    fn principal_id_validation_applies_to_deserialization() {
        assert!(serde_json::from_str::<PrincipalId>("\"user:alice\"").is_ok());
        assert!(serde_json::from_str::<PrincipalId>("\"system\"").is_err());
    }
}
