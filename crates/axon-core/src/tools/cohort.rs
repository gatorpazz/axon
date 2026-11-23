//! `cohort.query`: select a biomarker from a **synthetic** research cohort.
//!
//! No real participant data is involved. The cohort is generated
//! deterministically from a fixed seed with a hand-rolled PRNG, so the same
//! query always returns the same values on every platform, which is what makes
//! the provenance digests meaningful. The effect sizes are invented to give
//! the demo pipeline something to find; they are not estimates of anything.

use std::collections::BTreeMap;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::tool::{Tool, ToolContext, ToolError};

const DATASET: &str = "synthetic-aging-cohort";
const SEED: u64 = 0x00C0_FFEE_2026;
const PARTICIPANTS: usize = 600;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Biomarker {
    Age,
    /// Plasma p-tau217, pg/mL.
    PlasmaPtau217,
    /// Amyloid PET, Centiloids.
    AmyloidCentiloid,
    /// Mini-Mental State Examination score, 0–30.
    Mmse,
}

impl Biomarker {
    fn unit(self) -> &'static str {
        match self {
            Self::Age => "years",
            Self::PlasmaPtau217 => "pg/mL",
            Self::AmyloidCentiloid => "Centiloid",
            Self::Mmse => "points",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum GroupBy {
    Apoe4Carrier,
    Diagnosis,
    Sex,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Diagnosis {
    /// Cognitively unimpaired.
    Cu,
    /// Mild cognitive impairment.
    Mci,
    /// Alzheimer's disease dementia.
    Ad,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Sex {
    Female,
    Male,
}

#[derive(Debug, Clone, Default, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Filter {
    /// Inclusive lower bound on age in years.
    #[serde(default)]
    pub min_age: Option<f64>,
    /// Inclusive upper bound on age in years.
    #[serde(default)]
    pub max_age: Option<f64>,
    /// Keep only these diagnostic groups.
    #[serde(default)]
    pub diagnosis: Option<Vec<Diagnosis>>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct QueryInput {
    /// The measurement to return.
    pub biomarker: Biomarker,
    /// Split values into groups. Omit to get a single group named `all`.
    #[serde(default)]
    pub group_by: Option<GroupBy>,
    #[serde(default)]
    pub filter: Filter,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct DatasetInfo {
    pub name: &'static str,
    pub synthetic: bool,
    pub seed: u64,
    pub participants: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct QueryOutput {
    pub dataset: DatasetInfo,
    pub biomarker: Biomarker,
    pub unit: &'static str,
    /// Participants that passed the filter.
    pub matched: usize,
    /// Values per group, in participant order.
    pub groups: BTreeMap<&'static str, Vec<f64>>,
}

#[derive(Debug, Clone)]
struct Participant {
    age: f64,
    sex: Sex,
    apoe4_alleles: u8,
    diagnosis: Diagnosis,
    plasma_ptau217: f64,
    amyloid_centiloid: f64,
    mmse: f64,
}

impl Participant {
    fn value(&self, biomarker: Biomarker) -> f64 {
        match biomarker {
            Biomarker::Age => self.age,
            Biomarker::PlasmaPtau217 => self.plasma_ptau217,
            Biomarker::AmyloidCentiloid => self.amyloid_centiloid,
            Biomarker::Mmse => self.mmse,
        }
    }

    fn group(&self, by: Option<GroupBy>) -> &'static str {
        match by {
            None => "all",
            Some(GroupBy::Apoe4Carrier) if self.apoe4_alleles > 0 => "carrier",
            Some(GroupBy::Apoe4Carrier) => "non_carrier",
            Some(GroupBy::Diagnosis) => match self.diagnosis {
                Diagnosis::Cu => "cu",
                Diagnosis::Mci => "mci",
                Diagnosis::Ad => "ad",
            },
            Some(GroupBy::Sex) => match self.sex {
                Sex::Female => "female",
                Sex::Male => "male",
            },
        }
    }
}

/// SplitMix64. Tiny, portable and fully specified, unlike `rand`'s default
/// generators whose streams may change between releases.
struct SplitMix64(u64);

impl SplitMix64 {
    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform in `[0, 1)` with 53 bits of precision.
    fn uniform(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }

    /// Normal deviate via Box–Muller.
    fn normal(&mut self, mean: f64, sd: f64) -> f64 {
        let u1 = 1.0 - self.uniform(); // (0, 1], so ln is finite
        let u2 = self.uniform();
        mean + sd * (-2.0 * u1.ln()).sqrt() * (std::f64::consts::TAU * u2).cos()
    }
}

fn round3(v: f64) -> f64 {
    (v * 1000.0).round() / 1000.0
}

fn generate(seed: u64, n: usize) -> Vec<Participant> {
    let mut rng = SplitMix64(seed);
    (0..n)
        .map(|_| {
            let age = rng.normal(72.0, 7.0).clamp(55.0, 90.0).round();
            let sex = if rng.uniform() < 0.55 {
                Sex::Female
            } else {
                Sex::Male
            };
            let apoe4_alleles = match rng.uniform() {
                u if u < 0.60 => 0,
                u if u < 0.93 => 1,
                _ => 2,
            };
            // Risk of impairment rises with age and APOE4 dose.
            let risk = 0.15 + 0.012 * (age - 55.0) + 0.15 * f64::from(apoe4_alleles);
            let diagnosis = match rng.uniform() {
                u if u < risk * 0.45 => Diagnosis::Ad,
                u if u < risk => Diagnosis::Mci,
                _ => Diagnosis::Cu,
            };
            let (ptau, centiloid, mmse) = match diagnosis {
                Diagnosis::Cu => (0.25, 10.0, 29.0),
                Diagnosis::Mci => (0.45, 45.0, 26.0),
                Diagnosis::Ad => (0.80, 85.0, 20.0),
            };
            let dose = f64::from(apoe4_alleles);
            Participant {
                age,
                sex,
                apoe4_alleles,
                diagnosis,
                plasma_ptau217: round3((ptau + 0.08 * dose + rng.normal(0.0, 0.12)).max(0.02)),
                amyloid_centiloid: round3(centiloid + 12.0 * dose + rng.normal(0.0, 18.0)),
                mmse: (mmse + rng.normal(0.0, 1.5)).clamp(0.0, 30.0).round(),
            }
        })
        .collect()
}

#[derive(Debug)]
pub struct CohortQuery {
    participants: Vec<Participant>,
}

impl Default for CohortQuery {
    fn default() -> Self {
        Self {
            participants: generate(SEED, PARTICIPANTS),
        }
    }
}

impl CohortQuery {
    fn query(&self, input: &QueryInput) -> Result<QueryOutput, ToolError> {
        let Filter {
            min_age,
            max_age,
            diagnosis,
        } = &input.filter;
        if let (Some(min), Some(max)) = (min_age, max_age) {
            if min > max {
                return Err(ToolError::InvalidInput(format!(
                    "filter.min_age ({min}) is greater than filter.max_age ({max})"
                )));
            }
        }
        let mut groups: BTreeMap<&'static str, Vec<f64>> = BTreeMap::new();
        let mut matched = 0;
        for p in &self.participants {
            let keep = min_age.is_none_or(|min| p.age >= min)
                && max_age.is_none_or(|max| p.age <= max)
                && diagnosis
                    .as_ref()
                    .is_none_or(|wanted| wanted.contains(&p.diagnosis));
            if keep {
                matched += 1;
                groups
                    .entry(p.group(input.group_by))
                    .or_default()
                    .push(p.value(input.biomarker));
            }
        }
        Ok(QueryOutput {
            dataset: DatasetInfo {
                name: DATASET,
                synthetic: true,
                seed: SEED,
                participants: self.participants.len(),
            },
            biomarker: input.biomarker,
            unit: input.biomarker.unit(),
            matched,
            groups,
        })
    }
}

impl Tool for CohortQuery {
    const NAME: &'static str = "cohort.query";
    const VERSION: &'static str = "1.0.0";
    const DESCRIPTION: &'static str = "Select a biomarker from a SYNTHETIC aging cohort (generated data, not real participants), optionally filtered and grouped by APOE4 carrier status, diagnosis or sex.";
    type Input = QueryInput;
    type Output = QueryOutput;

    async fn call(&self, _: &ToolContext, input: QueryInput) -> Result<QueryOutput, ToolError> {
        self.query(&input)
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn query(input: serde_json::Value) -> Result<QueryOutput, ToolError> {
        let input: QueryInput = serde_json::from_value(input).expect("input deserializes");
        CohortQuery::default().query(&input)
    }

    #[test]
    fn generation_is_deterministic() {
        let a = query(json!({"biomarker": "plasma_ptau217", "group_by": "diagnosis"})).expect("ok");
        let b = query(json!({"biomarker": "plasma_ptau217", "group_by": "diagnosis"})).expect("ok");
        assert_eq!(a, b);
        assert!(a.dataset.synthetic);
    }

    #[test]
    fn splitmix64_matches_the_reference_stream() {
        // First outputs for seed 0 from the reference implementation.
        let mut rng = SplitMix64(0);
        assert_eq!(rng.next_u64(), 0xE220_A839_7B1D_CDAF);
        assert_eq!(rng.next_u64(), 0x6E78_9E6A_A1B9_65F4);
    }

    #[test]
    fn groups_partition_the_matched_participants() {
        for group_by in ["apoe4_carrier", "diagnosis", "sex"] {
            let out = query(json!({"biomarker": "age", "group_by": group_by})).expect("ok");
            assert_eq!(out.matched, PARTICIPANTS);
            assert_eq!(
                out.groups.values().map(Vec::len).sum::<usize>(),
                PARTICIPANTS
            );
            assert!(
                out.groups.len() >= 2,
                "{group_by} should yield several groups"
            );
        }
        let all = query(json!({"biomarker": "mmse"})).expect("ok");
        assert_eq!(all.groups.keys().copied().collect::<Vec<_>>(), vec!["all"]);
    }

    #[test]
    fn filters_apply() {
        let out = query(json!({
            "biomarker": "age",
            "filter": {"min_age": 70, "max_age": 75, "diagnosis": ["mci", "ad"]},
            "group_by": "diagnosis",
        }))
        .expect("ok");
        assert!(out.matched > 0 && out.matched < PARTICIPANTS);
        assert!(!out.groups.contains_key("cu"));
        assert!(
            out.groups
                .values()
                .flatten()
                .all(|age| (70.0..=75.0).contains(age))
        );
    }

    #[test]
    fn rejects_contradictory_filters() {
        assert!(matches!(
            query(json!({"biomarker": "age", "filter": {"min_age": 80, "max_age": 60}})),
            Err(ToolError::InvalidInput(_))
        ));
    }

    #[test]
    fn values_stay_in_plausible_ranges() {
        let ptau = query(json!({"biomarker": "plasma_ptau217"})).expect("ok");
        assert!(ptau.groups["all"].iter().all(|v| (0.02..3.0).contains(v)));
        let mmse = query(json!({"biomarker": "mmse"})).expect("ok");
        assert!(mmse.groups["all"].iter().all(|v| (0.0..=30.0).contains(v)));
    }
}
