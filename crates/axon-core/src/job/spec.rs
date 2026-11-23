//! Job specifications: an ordered plan of tool calls whose inputs may reference
//! the outputs of earlier steps.
//!
//! A reference is a JSON object of the exact shape `{"$ref": "<step>#<pointer>"}`
//! where `<pointer>` is an RFC 6901 JSON Pointer into that step's output (empty
//! for the whole output):
//!
//! ```json
//! { "id": "test", "tool": "stats.welch_t_test",
//!   "input": { "a": {"$ref": "select#/groups/carrier"},
//!              "b": {"$ref": "select#/groups/non_carrier"} } }
//! ```

use std::collections::HashSet;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::ids::is_ident;

pub const MAX_STEPS: usize = 32;
const REF_KEY: &str = "$ref";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobSpec {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    pub steps: Vec<StepSpec>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StepSpec {
    pub id: String,
    pub tool: String,
    /// Defaults to `{}` so tools whose inputs are all optional need no input.
    #[serde(default = "empty_object")]
    pub input: Value,
}

fn empty_object() -> Value {
    Value::Object(serde_json::Map::new())
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SpecError {
    #[error("a job needs between 1 and {MAX_STEPS} steps, got {0}")]
    StepCount(usize),
    #[error("step id `{0}` must be non-empty and use only [A-Za-z0-9_.-]")]
    InvalidStepId(String),
    #[error("duplicate step id `{0}`")]
    DuplicateStepId(String),
    #[error("step `{step}`: malformed reference `{reference}`, expected `<step>#<json-pointer>`")]
    MalformedRef { step: String, reference: String },
    #[error("step `{step}` references `{target}`, which is not an earlier step")]
    UnknownRefTarget { step: String, target: String },
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RefError {
    #[error("malformed reference `{0}`")]
    Malformed(String),
    #[error("reference `{0}` points at a step with no output")]
    NoOutput(String),
    #[error("reference `{0}` does not resolve to a value in that step's output")]
    Dangling(String),
}

impl JobSpec {
    /// Structural validation. Whether the tools exist and the caller may use
    /// them is the gateway's concern, not the spec's.
    pub fn validate(&self) -> Result<(), SpecError> {
        if self.steps.is_empty() || self.steps.len() > MAX_STEPS {
            return Err(SpecError::StepCount(self.steps.len()));
        }
        let mut earlier = HashSet::new();
        for step in &self.steps {
            if !is_ident(&step.id) {
                return Err(SpecError::InvalidStepId(step.id.clone()));
            }
            let mut result = Ok(());
            visit_refs(&step.input, &mut |reference| {
                if result.is_ok() {
                    result = match parse_ref(reference) {
                        None => Err(SpecError::MalformedRef {
                            step: step.id.clone(),
                            reference: reference.to_owned(),
                        }),
                        Some((target, _)) if !earlier.contains(target) => {
                            Err(SpecError::UnknownRefTarget {
                                step: step.id.clone(),
                                target: target.to_owned(),
                            })
                        }
                        Some(_) => Ok(()),
                    };
                }
            });
            result?;
            if !earlier.insert(step.id.as_str()) {
                return Err(SpecError::DuplicateStepId(step.id.clone()));
            }
        }
        Ok(())
    }
}

/// Returns the reference string if `value` is exactly `{"$ref": "<string>"}`.
fn as_ref(value: &Value) -> Option<&str> {
    match value {
        Value::Object(map) if map.len() == 1 => map.get(REF_KEY)?.as_str(),
        _ => None,
    }
}

/// Splits `step#/pointer` into its parts.
fn parse_ref(reference: &str) -> Option<(&str, &str)> {
    let (step, pointer) = reference.split_once('#')?;
    (is_ident(step) && (pointer.is_empty() || pointer.starts_with('/'))).then_some((step, pointer))
}

fn visit_refs<'a>(value: &'a Value, f: &mut impl FnMut(&'a str)) {
    if let Some(reference) = as_ref(value) {
        return f(reference);
    }
    match value {
        Value::Array(items) => items.iter().for_each(|v| visit_refs(v, f)),
        Value::Object(map) => map.values().for_each(|v| visit_refs(v, f)),
        _ => {}
    }
}

/// Returns a copy of `value` with every reference replaced by the value it
/// points at. `output_of` yields the output of a completed step.
pub fn resolve_refs<'a>(
    value: &Value,
    output_of: &impl Fn(&str) -> Option<&'a Value>,
) -> Result<Value, RefError> {
    if let Some(reference) = as_ref(value) {
        let (step, pointer) =
            parse_ref(reference).ok_or_else(|| RefError::Malformed(reference.to_owned()))?;
        let output = output_of(step).ok_or_else(|| RefError::NoOutput(reference.to_owned()))?;
        return output
            .pointer(pointer)
            .cloned()
            .ok_or_else(|| RefError::Dangling(reference.to_owned()));
    }
    Ok(match value {
        Value::Array(items) => Value::Array(
            items
                .iter()
                .map(|v| resolve_refs(v, output_of))
                .collect::<Result<_, _>>()?,
        ),
        Value::Object(map) => Value::Object(
            map.iter()
                .map(|(k, v)| Ok((k.clone(), resolve_refs(v, output_of)?)))
                .collect::<Result<_, RefError>>()?,
        ),
        scalar => scalar.clone(),
    })
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn spec(steps: Value) -> JobSpec {
        serde_json::from_value(json!({ "steps": steps })).expect("spec deserializes")
    }

    #[test]
    fn accepts_a_pipeline_with_backward_refs() {
        let spec = spec(json!([
            {"id": "a", "tool": "t"},
            {"id": "b", "tool": "t", "input": {"xs": [{"$ref": "a#/values"}], "all": {"$ref": "a#"}}},
        ]));
        assert_eq!(spec.validate(), Ok(()));
    }

    #[test]
    fn rejects_structural_problems() {
        assert_eq!(spec(json!([])).validate(), Err(SpecError::StepCount(0)));
        assert_eq!(
            spec(json!([{"id": "a b", "tool": "t"}])).validate(),
            Err(SpecError::InvalidStepId("a b".into()))
        );
        assert_eq!(
            spec(json!([{"id": "a", "tool": "t"}, {"id": "a", "tool": "t"}])).validate(),
            Err(SpecError::DuplicateStepId("a".into()))
        );
        // Forward and self references are both "not an earlier step".
        for target in ["b", "a"] {
            assert_eq!(
                spec(json!([
                    {"id": "a", "tool": "t", "input": {"$ref": format!("{target}#/x")}},
                    {"id": "b", "tool": "t"},
                ]))
                .validate(),
                Err(SpecError::UnknownRefTarget {
                    step: "a".into(),
                    target: target.into()
                })
            );
        }
        assert!(matches!(
            spec(json!([{"id": "a", "tool": "t", "input": {"$ref": "no-pointer"}}])).validate(),
            Err(SpecError::MalformedRef { .. })
        ));
    }

    #[test]
    fn unknown_fields_are_rejected_at_the_boundary() {
        let result = serde_json::from_value::<JobSpec>(json!({
            "steps": [{"id": "a", "tool": "t", "inputs": {}}]
        }));
        assert!(result.is_err());
    }

    #[test]
    fn resolves_nested_refs() {
        let output = json!({"groups": {"carrier": [1.0, 2.0]}, "n": 2});
        let lookup = |step: &str| (step == "select").then_some(&output);

        let resolved = resolve_refs(
            &json!({
                "a": {"$ref": "select#/groups/carrier"},
                "meta": [{"$ref": "select#/n"}, "literal"],
                "whole": {"$ref": "select#"},
                "not_a_ref": {"$ref": "select#/n", "other": 1},
            }),
            &lookup,
        );
        assert_eq!(
            resolved,
            Ok(json!({
                "a": [1.0, 2.0],
                "meta": [2, "literal"],
                "whole": output,
                "not_a_ref": {"$ref": "select#/n", "other": 1},
            }))
        );
    }

    #[test]
    fn reports_unresolvable_refs() {
        let output = json!({"n": 2});
        let lookup = |step: &str| (step == "select").then_some(&output);
        assert_eq!(
            resolve_refs(&json!({"$ref": "select#/missing"}), &lookup),
            Err(RefError::Dangling("select#/missing".into()))
        );
        assert_eq!(
            resolve_refs(&json!({"$ref": "other#/n"}), &lookup),
            Err(RefError::NoOutput("other#/n".into()))
        );
    }
}
