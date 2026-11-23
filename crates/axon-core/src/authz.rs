//! Relationship-based authorization in the style of Zanzibar / OpenFGA.
//!
//! Access is expressed as tuples `object#relation@subject`:
//!
//! ```text
//! group:researchers#member@user:alice          alice is a member of researchers
//! tool:*#invoker@group:researchers#member      members of researchers may invoke any tool
//! tool:stats.describe#invoker@user:*           every user may invoke stats.describe
//! ```
//!
//! [`Authorizer::check`] answers "does this principal have `relation` on
//! `object`?" by walking usersets, with cycle protection.

use std::{
    collections::{HashMap, HashSet},
    fmt,
    str::FromStr,
};

use crate::ids::{PrincipalId, is_ident};

const MAX_DEPTH: usize = 16;

/// A typed object reference such as `tool:stats.describe` or `group:researchers`.
/// The name `*` stands for every object of that kind.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ObjectRef {
    pub kind: String,
    pub name: String,
}

impl ObjectRef {
    pub fn new(kind: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            kind: kind.into(),
            name: name.into(),
        }
    }

    pub fn tool(name: &str) -> Self {
        Self::new("tool", name)
    }

    fn wildcard(&self) -> Self {
        Self::new(self.kind.clone(), "*")
    }
}

impl fmt::Display for ObjectRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.kind, self.name)
    }
}

impl FromStr for ObjectRef {
    type Err = TupleParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.split_once(':') {
            Some((kind, name)) if is_ident(kind) && (name == "*" || is_ident(name)) => {
                Ok(Self::new(kind, name))
            }
            _ => Err(TupleParseError::Object(s.to_owned())),
        }
    }
}

/// Who a tuple grants the relation to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Subject {
    /// One principal: `user:alice`.
    Principal(PrincipalId),
    /// Every principal of a kind: `user:*`.
    Anyone { kind: String },
    /// Everyone holding `relation` on `object`: `group:researchers#member`.
    Userset { object: ObjectRef, relation: String },
}

impl fmt::Display for Subject {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Principal(p) => p.fmt(f),
            Self::Anyone { kind } => write!(f, "{kind}:*"),
            Self::Userset { object, relation } => write!(f, "{object}#{relation}"),
        }
    }
}

impl FromStr for Subject {
    type Err = TupleParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Some((object, relation)) = s.split_once('#') {
            if !is_ident(relation) {
                return Err(TupleParseError::Relation(relation.to_owned()));
            }
            return Ok(Self::Userset {
                object: object.parse()?,
                relation: relation.to_owned(),
            });
        }
        let object: ObjectRef = s.parse()?;
        if object.name == "*" {
            Ok(Self::Anyone { kind: object.kind })
        } else {
            s.parse()
                .map(Self::Principal)
                .map_err(|_| TupleParseError::Object(s.to_owned()))
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tuple {
    pub object: ObjectRef,
    pub relation: String,
    pub subject: Subject,
}

impl fmt::Display for Tuple {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}#{}@{}", self.object, self.relation, self.subject)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TupleParseError {
    #[error("tuple `{0}` is not of the form `object#relation@subject`")]
    Shape(String),
    #[error("invalid object reference `{0}`: expected `<kind>:<name>`")]
    Object(String),
    #[error("invalid relation `{0}`")]
    Relation(String),
}

impl FromStr for Tuple {
    type Err = TupleParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let shape = || TupleParseError::Shape(s.to_owned());
        let (target, subject) = s.split_once('@').ok_or_else(shape)?;
        let (object, relation) = target.split_once('#').ok_or_else(shape)?;
        if !is_ident(relation) {
            return Err(TupleParseError::Relation(relation.to_owned()));
        }
        Ok(Self {
            object: object.parse()?,
            relation: relation.to_owned(),
            subject: subject.parse()?,
        })
    }
}

/// An immutable set of tuples indexed for checks.
#[derive(Debug, Default)]
pub struct Authorizer {
    index: HashMap<(ObjectRef, String), Vec<Subject>>,
}

impl Authorizer {
    pub fn new(tuples: impl IntoIterator<Item = Tuple>) -> Self {
        let mut index: HashMap<_, Vec<_>> = HashMap::new();
        for tuple in tuples {
            index
                .entry((tuple.object, tuple.relation))
                .or_default()
                .push(tuple.subject);
        }
        Self { index }
    }

    /// Parses tuples from their textual form, failing on the first bad one.
    pub fn parse<'a>(tuples: impl IntoIterator<Item = &'a str>) -> Result<Self, TupleParseError> {
        tuples
            .into_iter()
            .map(str::parse)
            .collect::<Result<Vec<Tuple>, _>>()
            .map(Self::new)
    }

    /// Does `principal` hold `relation` on `object`, directly or via usersets?
    pub fn check(&self, principal: &PrincipalId, relation: &str, object: &ObjectRef) -> bool {
        self.walk(principal, relation, object, &mut HashSet::new(), 0)
    }

    fn walk(
        &self,
        principal: &PrincipalId,
        relation: &str,
        object: &ObjectRef,
        seen: &mut HashSet<(ObjectRef, String)>,
        depth: usize,
    ) -> bool {
        // `seen` is global rather than per-path: a node that was already
        // explored either failed (no need to retry) or is an ancestor (a cycle).
        if depth > MAX_DEPTH || !seen.insert((object.clone(), relation.to_owned())) {
            return false;
        }
        [object.clone(), object.wildcard()]
            .into_iter()
            .filter_map(|candidate| self.index.get(&(candidate, relation.to_owned())))
            .flatten()
            .any(|subject| match subject {
                Subject::Principal(p) => p == principal,
                Subject::Anyone { kind } => kind == principal.kind(),
                Subject::Userset { object, relation } => {
                    self.walk(principal, relation, object, seen, depth + 1)
                }
            })
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    fn principal(s: &str) -> PrincipalId {
        s.parse().expect("valid principal")
    }

    fn authorizer(tuples: &[&str]) -> Authorizer {
        Authorizer::parse(tuples.iter().copied()).expect("valid tuples")
    }

    #[test]
    fn direct_grant() {
        let authz = authorizer(&["tool:stats.describe#invoker@user:bob"]);
        assert!(authz.check(
            &principal("user:bob"),
            "invoker",
            &ObjectRef::tool("stats.describe")
        ));
        assert!(!authz.check(
            &principal("user:bob"),
            "invoker",
            &ObjectRef::tool("cohort.query")
        ));
        assert!(!authz.check(
            &principal("user:eve"),
            "invoker",
            &ObjectRef::tool("stats.describe")
        ));
        assert!(!authz.check(
            &principal("user:bob"),
            "admin",
            &ObjectRef::tool("stats.describe")
        ));
    }

    #[test]
    fn grant_through_nested_usersets_and_object_wildcard() {
        let authz = authorizer(&[
            "group:lab#member@user:alice",
            "group:researchers#member@group:lab#member",
            "tool:*#invoker@group:researchers#member",
        ]);
        assert!(authz.check(
            &principal("user:alice"),
            "invoker",
            &ObjectRef::tool("cohort.query")
        ));
        assert!(!authz.check(
            &principal("user:bob"),
            "invoker",
            &ObjectRef::tool("cohort.query")
        ));
    }

    #[test]
    fn subject_wildcard_is_scoped_to_kind() {
        let authz = authorizer(&["tool:stats.describe#invoker@user:*"]);
        assert!(authz.check(
            &principal("user:zed"),
            "invoker",
            &ObjectRef::tool("stats.describe")
        ));
        assert!(!authz.check(
            &principal("service:ci"),
            "invoker",
            &ObjectRef::tool("stats.describe")
        ));
    }

    #[test]
    fn cycles_terminate() {
        let authz = authorizer(&[
            "group:a#member@group:b#member",
            "group:b#member@group:a#member",
            "tool:x#invoker@group:a#member",
        ]);
        assert!(!authz.check(&principal("user:alice"), "invoker", &ObjectRef::tool("x")));
    }

    #[test]
    fn malformed_tuples_are_rejected() {
        for bad in [
            "tool:x",
            "tool:x#invoker",
            "tool:x@user:a",
            "toolx#invoker@user:a",
            "tool:x#in voker@user:a",
            "tool:x#invoker@alice",
            "tool:x#invoker@group:g#",
        ] {
            assert!(bad.parse::<Tuple>().is_err(), "{bad:?} should be rejected");
        }
    }

    fn ident() -> impl Strategy<Value = String> {
        "[a-z][a-z0-9_.-]{0,8}"
    }

    fn object() -> impl Strategy<Value = ObjectRef> {
        (ident(), prop_oneof![ident(), Just("*".to_owned())])
            .prop_map(|(kind, name)| ObjectRef::new(kind, name))
    }

    fn subject() -> impl Strategy<Value = Subject> {
        prop_oneof![
            (ident(), ident())
                .prop_map(|(k, n)| Subject::Principal(principal(&format!("{k}:{n}")))),
            ident().prop_map(|kind| Subject::Anyone { kind }),
            (object(), ident())
                .prop_map(|(object, relation)| Subject::Userset { object, relation }),
        ]
    }

    proptest! {
        #[test]
        fn tuples_round_trip_through_text(object in object(), relation in ident(), subject in subject()) {
            let tuple = Tuple { object, relation, subject };
            prop_assert_eq!(tuple.to_string().parse::<Tuple>(), Ok(tuple));
        }

        #[test]
        fn parser_never_panics(s in "\\PC{0,40}") {
            let _ = s.parse::<Tuple>();
        }
    }
}
