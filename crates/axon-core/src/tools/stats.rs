//! Descriptive statistics and Welch's unequal-variance t-test.
//!
//! The p-value needs Student's t CDF, which is computed from the regularized
//! incomplete beta function (Lentz's continued fraction) over a Lanczos
//! log-gamma. No external numerics crate is involved.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::tool::{Tool, ToolContext, ToolError};

const MAX_VALUES: usize = 100_000;

fn validated<'a>(name: &str, values: &'a [f64], min_len: usize) -> Result<&'a [f64], ToolError> {
    if values.len() < min_len {
        return Err(ToolError::InvalidInput(format!(
            "`{name}` needs at least {min_len} value(s), got {}",
            values.len()
        )));
    }
    if values.len() > MAX_VALUES {
        return Err(ToolError::InvalidInput(format!(
            "`{name}` has more than {MAX_VALUES} values"
        )));
    }
    if values.iter().any(|v| !v.is_finite()) {
        return Err(ToolError::InvalidInput(format!(
            "`{name}` contains non-finite values"
        )));
    }
    Ok(values)
}

fn mean(values: &[f64]) -> f64 {
    values.iter().sum::<f64>() / values.len() as f64
}

/// Unbiased sample variance; 0 for a single value.
fn variance(values: &[f64], mean: f64) -> f64 {
    if values.len() < 2 {
        return 0.0;
    }
    values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / (values.len() - 1) as f64
}

/// Quantile by linear interpolation between order statistics (R type 7).
fn quantile(sorted: &[f64], q: f64) -> f64 {
    let position = q * (sorted.len() - 1) as f64;
    let (lo, hi) = (position.floor() as usize, position.ceil() as usize);
    sorted[lo] + (sorted[hi] - sorted[lo]) * (position - lo as f64)
}

// ---------------------------------------------------------------- describe

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DescribeInput {
    /// The sample to summarize. Must be non-empty and finite.
    pub values: Vec<f64>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Summary {
    pub n: usize,
    pub mean: f64,
    /// Sample standard deviation (n - 1 denominator).
    pub sd: f64,
    pub min: f64,
    pub q1: f64,
    pub median: f64,
    pub q3: f64,
    pub max: f64,
}

pub fn describe(values: &[f64]) -> Result<Summary, ToolError> {
    let values = validated("values", values, 1)?;
    let mut sorted = values.to_vec();
    sorted.sort_by(f64::total_cmp);
    let mean = mean(values);
    Ok(Summary {
        n: values.len(),
        mean,
        sd: variance(values, mean).sqrt(),
        min: sorted[0],
        q1: quantile(&sorted, 0.25),
        median: quantile(&sorted, 0.5),
        q3: quantile(&sorted, 0.75),
        max: sorted[sorted.len() - 1],
    })
}

#[derive(Debug)]
pub struct Describe;

impl Tool for Describe {
    const NAME: &'static str = "stats.describe";
    const VERSION: &'static str = "1.0.0";
    const DESCRIPTION: &'static str =
        "Descriptive statistics for a numeric sample: n, mean, sample SD and five-number summary.";
    type Input = DescribeInput;
    type Output = Summary;

    async fn call(&self, _: &ToolContext, input: DescribeInput) -> Result<Summary, ToolError> {
        describe(&input.values)
    }
}

// ------------------------------------------------------------ welch t-test

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct WelchInput {
    /// First sample (at least two finite values).
    pub a: Vec<f64>,
    /// Second sample (at least two finite values).
    pub b: Vec<f64>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct WelchResult {
    pub n_a: usize,
    pub n_b: usize,
    pub mean_a: f64,
    pub mean_b: f64,
    /// `mean_a - mean_b`.
    pub mean_difference: f64,
    pub t: f64,
    /// Welch–Satterthwaite degrees of freedom.
    pub df: f64,
    /// Two-sided p-value.
    pub p_value: f64,
    /// Standardized effect size using the pooled standard deviation.
    pub cohens_d: f64,
}

pub fn welch_t_test(a: &[f64], b: &[f64]) -> Result<WelchResult, ToolError> {
    let a = validated("a", a, 2)?;
    let b = validated("b", b, 2)?;
    let (n_a, n_b) = (a.len() as f64, b.len() as f64);
    let (mean_a, mean_b) = (mean(a), mean(b));
    let (var_a, var_b) = (variance(a, mean_a), variance(b, mean_b));

    let (se_a, se_b) = (var_a / n_a, var_b / n_b);
    let se2 = se_a + se_b;
    if se2 == 0.0 {
        return Err(ToolError::Failed(
            "both samples have zero variance; the t statistic is undefined".into(),
        ));
    }
    let t = (mean_a - mean_b) / se2.sqrt();
    let df = se2.powi(2) / (se_a.powi(2) / (n_a - 1.0) + se_b.powi(2) / (n_b - 1.0));
    let pooled_sd = (((n_a - 1.0) * var_a + (n_b - 1.0) * var_b) / (n_a + n_b - 2.0)).sqrt();

    Ok(WelchResult {
        n_a: a.len(),
        n_b: b.len(),
        mean_a,
        mean_b,
        mean_difference: mean_a - mean_b,
        t,
        df,
        p_value: t_two_sided_p(t, df),
        cohens_d: (mean_a - mean_b) / pooled_sd,
    })
}

#[derive(Debug)]
pub struct WelchTTest;

impl Tool for WelchTTest {
    const NAME: &'static str = "stats.welch_t_test";
    const VERSION: &'static str = "1.0.0";
    const DESCRIPTION: &'static str = "Welch's unequal-variance t-test between two samples: t, degrees of freedom, two-sided p-value and Cohen's d.";
    type Input = WelchInput;
    type Output = WelchResult;

    async fn call(&self, _: &ToolContext, input: WelchInput) -> Result<WelchResult, ToolError> {
        welch_t_test(&input.a, &input.b)
    }
}

// ---------------------------------------------------------------- numerics

/// Two-sided p-value for Student's t: `P(|T| >= |t|) = I_x(df/2, 1/2)` with
/// `x = df / (df + t^2)`.
fn t_two_sided_p(t: f64, df: f64) -> f64 {
    regularized_incomplete_beta(df / 2.0, 0.5, df / (df + t * t)).clamp(0.0, 1.0)
}

/// Lanczos approximation (g = 7, n = 9) of `ln Γ(x)` for `x > 0`.
fn ln_gamma(x: f64) -> f64 {
    const G: f64 = 7.0;
    #[allow(clippy::excessive_precision)] // coefficients are quoted as published
    const COEFFICIENTS: [f64; 9] = [
        0.999_999_999_999_809_93,
        676.520_368_121_885_1,
        -1_259.139_216_722_402_8,
        771.323_428_777_653_13,
        -176.615_029_162_140_59,
        12.507_343_278_686_905,
        -0.138_571_095_265_720_12,
        9.984_369_578_019_571_6e-6,
        1.505_632_735_149_311_6e-7,
    ];
    if x < 0.5 {
        // Reflection formula.
        return (std::f64::consts::PI / (std::f64::consts::PI * x).sin()).ln() - ln_gamma(1.0 - x);
    }
    let x = x - 1.0;
    let t = x + G + 0.5;
    let series = COEFFICIENTS[1..]
        .iter()
        .zip(1..)
        .fold(COEFFICIENTS[0], |acc, (c, i)| acc + c / (x + f64::from(i)));
    0.5 * (2.0 * std::f64::consts::PI).ln() + (x + 0.5) * t.ln() - t + series.ln()
}

/// `I_x(a, b)`, the regularized incomplete beta function.
fn regularized_incomplete_beta(a: f64, b: f64, x: f64) -> f64 {
    if x <= 0.0 {
        return 0.0;
    }
    if x >= 1.0 {
        return 1.0;
    }
    let front =
        (ln_gamma(a + b) - ln_gamma(a) - ln_gamma(b) + a * x.ln() + b * (1.0 - x).ln()).exp();
    // The continued fraction converges quickly only on one side of the mode;
    // use the symmetry I_x(a, b) = 1 - I_{1-x}(b, a) on the other.
    if x < (a + 1.0) / (a + b + 2.0) {
        front * beta_continued_fraction(a, b, x) / a
    } else {
        1.0 - front * beta_continued_fraction(b, a, 1.0 - x) / b
    }
}

/// Modified Lentz evaluation of the incomplete beta continued fraction.
fn beta_continued_fraction(a: f64, b: f64, x: f64) -> f64 {
    const TINY: f64 = 1e-300;
    const EPSILON: f64 = 1e-15;
    let guard = |v: f64| if v.abs() < TINY { TINY } else { v };

    let mut c = 1.0;
    let mut d = 1.0 / guard(1.0 - (a + b) * x / (a + 1.0));
    let mut h = d;
    for m in 1..=500 {
        let m = f64::from(m);
        let even = m * (b - m) * x / ((a + 2.0 * m - 1.0) * (a + 2.0 * m));
        d = 1.0 / guard(1.0 + even * d);
        c = guard(1.0 + even / c);
        h *= d * c;

        let odd = -(a + m) * (a + b + m) * x / ((a + 2.0 * m) * (a + 2.0 * m + 1.0));
        d = 1.0 / guard(1.0 + odd * d);
        c = guard(1.0 + odd / c);
        let delta = d * c;
        h *= delta;
        if (delta - 1.0).abs() < EPSILON {
            break;
        }
    }
    h
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    fn close(actual: f64, expected: f64, tolerance: f64) -> bool {
        (actual - expected).abs() <= tolerance
    }

    #[test]
    fn ln_gamma_matches_factorials_and_half_integers() {
        assert!(close(ln_gamma(1.0), 0.0, 1e-12));
        assert!(close(ln_gamma(5.0), 24f64.ln(), 1e-12));
        assert!(close(
            ln_gamma(0.5),
            std::f64::consts::PI.sqrt().ln(),
            1e-12
        ));
        assert!(close(ln_gamma(11.0), 3_628_800f64.ln(), 1e-11));
    }

    #[test]
    fn t_distribution_p_values_match_reference_points() {
        // t = 0 is the centre of the distribution.
        assert!(close(t_two_sided_p(0.0, 7.0), 1.0, 1e-12));
        // df = 1 is the Cauchy distribution: P(|T| >= 1) = 1/2 exactly.
        assert!(close(t_two_sided_p(1.0, 1.0), 0.5, 1e-12));
        // df = 2 has the closed form P(|T| >= t) = 1 - t / sqrt(2 + t^2).
        assert!(close(
            t_two_sided_p(2.0, 2.0),
            1.0 - 2.0 / 6f64.sqrt(),
            1e-12
        ));
        // Standard table critical values at alpha = 0.05 and 0.01.
        assert!(close(
            t_two_sided_p(2.228_138_851_986_274, 10.0),
            0.05,
            1e-9
        ));
        assert!(close(
            t_two_sided_p(2.845_339_709_776_814, 20.0),
            0.01,
            1e-9
        ));
        // Large df approaches the normal distribution: 2 * (1 - Phi(1.96)).
        assert!(close(t_two_sided_p(1.96, 1e6), 0.049_995_8, 1e-5));
    }

    #[test]
    fn describe_matches_hand_computed_values() {
        let summary = describe(&[2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0]).expect("valid");
        assert_eq!(summary.n, 8);
        assert!(close(summary.mean, 5.0, 1e-12));
        assert!(close(summary.sd, (32.0f64 / 7.0).sqrt(), 1e-12));
        assert_eq!((summary.min, summary.max), (2.0, 9.0));
        assert!(close(summary.median, 4.5, 1e-12));
        assert!(close(summary.q1, 4.0, 1e-12));
        assert!(close(summary.q3, 5.5, 1e-12));

        let single = describe(&[3.0]).expect("valid");
        assert_eq!((single.sd, single.median), (0.0, 3.0));
    }

    #[test]
    fn welch_matches_a_worked_example() {
        // Hand-checkable: means 3 and 6, variances 2.5 and 10, n = 5 each.
        let result =
            welch_t_test(&[1.0, 2.0, 3.0, 4.0, 5.0], &[2.0, 4.0, 6.0, 8.0, 10.0]).expect("valid");
        assert!(close(result.mean_difference, -3.0, 1e-12));
        assert!(close(result.t, -3.0 / 2.5f64.sqrt(), 1e-12));
        // df = (0.5 + 2)^2 / (0.5^2/4 + 2^2/4) = 6.25 / 1.0625
        assert!(close(result.df, 6.25 / 1.0625, 1e-12));
        assert!(close(result.cohens_d, -3.0 / 6.25f64.sqrt(), 1e-12));
        // Reference: scipy.stats.ttest_ind(a, b, equal_var=False).pvalue
        assert!(
            close(result.p_value, 0.107_531_194_930_627, 1e-10),
            "p = {}",
            result.p_value
        );
    }

    #[test]
    fn rejects_degenerate_input() {
        assert!(matches!(describe(&[]), Err(ToolError::InvalidInput(_))));
        assert!(matches!(
            describe(&[1.0, f64::NAN]),
            Err(ToolError::InvalidInput(_))
        ));
        assert!(matches!(
            welch_t_test(&[1.0], &[1.0, 2.0]),
            Err(ToolError::InvalidInput(_))
        ));
        assert!(matches!(
            welch_t_test(&[1.0, 1.0], &[2.0, 2.0]),
            Err(ToolError::Failed(_))
        ));
    }

    fn sample(min_len: usize) -> impl Strategy<Value = Vec<f64>> {
        prop::collection::vec(-1e6f64..1e6, min_len..60)
    }

    proptest! {
        #[test]
        fn summary_is_ordered(values in sample(1)) {
            let s = describe(&values).expect("valid");
            prop_assert!(s.min <= s.q1 && s.q1 <= s.median && s.median <= s.q3 && s.q3 <= s.max);
            prop_assert!(s.min - 1e-6 <= s.mean && s.mean <= s.max + 1e-6);
            prop_assert!(s.sd >= 0.0);
        }

        #[test]
        fn welch_is_antisymmetric_with_a_valid_p(a in sample(2), b in sample(2)) {
            if let (Ok(ab), Ok(ba)) = (welch_t_test(&a, &b), welch_t_test(&b, &a)) {
                prop_assert!((0.0..=1.0).contains(&ab.p_value), "p = {}", ab.p_value);
                prop_assert!(ab.df > 0.0);
                prop_assert!((ab.t + ba.t).abs() <= 1e-9 * ab.t.abs().max(1.0));
                prop_assert!((ab.p_value - ba.p_value).abs() <= 1e-9);
            }
        }

        #[test]
        fn p_value_decreases_as_t_grows(t in 0.0f64..50.0, step in 0.01f64..5.0, df in 1.0f64..200.0) {
            prop_assert!(t_two_sided_p(t + step, df) <= t_two_sided_p(t, df) + 1e-12);
        }
    }
}
