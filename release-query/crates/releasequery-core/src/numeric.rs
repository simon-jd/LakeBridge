//! Numeric comparison policy for the ordered five-column snapshot profile.
use crate::exec::error;
use datafusion::common::Result;
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NanPolicy {
    #[default]
    Equal,
    Unequal,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FloatComparison {
    pub absolute: f64,
    pub relative: f64,
    pub nan_policy: NanPolicy,
}

impl Default for FloatComparison {
    fn default() -> Self {
        Self {
            absolute: 1e-8,
            relative: 1e-6,
            nan_policy: NanPolicy::Equal,
        }
    }
}

impl FloatComparison {
    pub fn validate(self) -> Result<()> {
        if !self.absolute.is_finite()
            || !self.relative.is_finite()
            || self.absolute < 0.0
            || !(0.0..=1.0).contains(&self.relative)
        {
            return Err(error(
                "INVALID_REQUEST: tolerances must be finite, absolute >= 0 and relative in [0,1]",
            ));
        }
        Ok(())
    }

    pub fn equivalent(self, left: f64, right: f64) -> bool {
        if left.is_nan() || right.is_nan() {
            return left.is_nan() && right.is_nan() && matches!(self.nan_policy, NanPolicy::Equal);
        }
        // This includes equal signed infinities and either sign of zero.
        if left == right {
            return true;
        }
        if left.is_infinite() || right.is_infinite() {
            return false;
        }
        let scale = left.abs().max(right.abs());
        let difference = (left - right).abs();
        let threshold = self.absolute + self.relative * scale;
        if difference.is_finite() && threshold.is_finite() {
            difference <= threshold
        } else {
            // Avoid inf <= inf accepting opposite large finite values.
            (left / scale - right / scale).abs() <= self.absolute / scale + self.relative
        }
    }
}
