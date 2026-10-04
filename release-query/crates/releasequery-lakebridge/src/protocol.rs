use datafusion::common::Result;
use releasequery_core::exec::error;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{io::Read, path::Path};
pub const FINGERPRINT: &str = "a85c81c8fe904119e0433d8ac86676c3efbd7d20bc4043954d089121eda5c378";
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TablePin {
    pub name: String,
    pub table: String,
    pub snapshot_id: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReleasePin {
    pub dataset_id: String,
    pub release_id: String,
    pub tables: Vec<TablePin>,
    pub schema_fingerprint: String,
    pub semantic_view_version: String,
    pub protocol_version: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Scope {
    pub tenant_id: String,
    pub start: String,
    pub end: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DiffRequest {
    pub from_pin: Option<ReleasePin>,
    pub to_pin: ReleasePin,
    pub scope: Scope,
    pub key_columns: Vec<String>,
    pub compare_columns: Vec<String>,
    pub comparison_version: String,
    pub protocol_version: String,
}
pub fn logical_id(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 128
        && s.as_bytes()[0].is_ascii_alphanumeric()
        && s.bytes()
            .all(|c| c.is_ascii_alphanumeric() || b"_.-".contains(&c))
}
impl ReleasePin {
    pub fn detail(&self) -> Result<&TablePin> {
        self.tables
            .iter()
            .find(|t| t.name == "detail")
            .ok_or_else(|| error("INVALID_REQUEST: detail snapshot is required"))
    }
    pub fn validate(&self) -> Result<()> {
        if !logical_id(&self.dataset_id)
            || !logical_id(&self.release_id)
            || self.protocol_version != "lakebridge.consumer.v1"
        {
            return Err(error("INVALID_REQUEST: release identity/protocol"));
        }
        if self.schema_fingerprint != FINGERPRINT || self.semantic_view_version != "detail.utc.v1" {
            return Err(error("INCOMPATIBLE_SCHEMA: explicit view adapter required"));
        }
        self.detail()?;
        let mut prev = "";
        for t in &self.tables {
            let parts = t.table.split('.').collect::<Vec<_>>();
            if !logical_id(&t.name)
                || t.name.as_str() <= prev
                || parts.len() != 3
                || parts.iter().any(|s| {
                    s.is_empty()
                        || !s.as_bytes()[0].is_ascii_lowercase()
                        || !s
                            .bytes()
                            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'_')
                })
                || !t.snapshot_id.bytes().all(|b| b.is_ascii_digit())
                || t.snapshot_id.starts_with('0')
                || !t.snapshot_id.parse::<i64>().is_ok_and(|v| v > 0)
            {
                return Err(error(
                    "INVALID_REQUEST: table pin must be sorted with positive int64 string",
                ));
            }
            prev = &t.name;
        }
        Ok(())
    }
}
impl Scope {
    pub fn micros(&self) -> Result<(i64, i64)> {
        let date = |s: &str| -> Result<i64> {
            if s.len() != 10
                || !s.as_bytes()[..4].iter().all(u8::is_ascii_digit)
                || &s[..4] == "0000"
            {
                return Err(error("INVALID_REQUEST: canonical Gregorian date"));
            }
            let d = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d")
                .map_err(|_| error("INVALID_REQUEST: UTC date"))?;
            if d.format("%Y-%m-%d").to_string() != s {
                return Err(error("INVALID_REQUEST: canonical UTC date"));
            }
            Ok(d.and_hms_opt(0, 0, 0).unwrap().and_utc().timestamp_micros())
        };
        let start = date(&self.start)?;
        let end = date(&self.end)?;
        if !logical_id(&self.tenant_id) || start >= end {
            return Err(error(
                "INVALID_REQUEST: tenant and nonempty UTC [start,end) required",
            ));
        }
        Ok((start, end))
    }
}
impl DiffRequest {
    pub fn validate(&self) -> Result<()> {
        self.to_pin.validate()?;
        if let Some(p) = &self.from_pin {
            p.validate()?;
            if p.dataset_id != self.to_pin.dataset_id {
                return Err(error("INVALID_REQUEST: datasets differ"));
            }
        }
        self.scope.micros()?;
        if self.key_columns != ["tenant_id", "event_id"]
            || self.compare_columns
                != [
                    "event_time",
                    "units",
                    "plan_id",
                    "billable_units",
                    "rule_version",
                ]
            || self.comparison_version != "usage.exact.v1"
            || self.protocol_version != "lakebridge.consumer.v1"
        {
            return Err(error("INVALID_REQUEST: comparison contract"));
        }
        Ok(())
    }
}
pub fn canonical(value: &Value) -> Result<Vec<u8>> {
    fn validate(v: &Value) -> bool {
        match v {
            Value::Number(n) => n
                .as_i64()
                .is_some_and(|n| n.unsigned_abs() <= 9_007_199_254_740_991),
            Value::Array(a) => a.iter().all(validate),
            Value::Object(m) => m.iter().all(|(k, v)| k.is_ascii() && validate(v)),
            _ => true,
        }
    }
    if !validate(value) {
        return Err(error(
            "INVALID_REQUEST: non-canonical JSON (floats/large numbers/keys)",
        ));
    }
    // serde_json's default Map uses sorted BTreeMap keys; preserve_order is not enabled.
    serde_json::to_vec(value).map_err(|e| error(e.to_string()))
}
pub fn hash(value: &Value) -> Result<String> {
    Ok(format!("{:x}", Sha256::digest(canonical(value)?)))
}
pub fn digest(path: &Path) -> Result<String> {
    let mut file = std::fs::File::open(path)?;
    let mut h = Sha256::new();
    let mut buffer = [0; 65536];
    loop {
        let n = file.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        h.update(&buffer[..n]);
    }
    Ok(format!("{:x}", h.finalize()))
}
pub fn read_json(path: &Path) -> Result<Value> {
    if std::fs::metadata(path)?.len() > 16 * 1024 * 1024 {
        return Err(error("RESOURCE_EXHAUSTED: JSON descriptor too large"));
    }
    let data = std::fs::read(path)?;
    if data.len() > 16 * 1024 * 1024 {
        return Err(error("RESOURCE_EXHAUSTED: JSON descriptor too large"));
    }
    serde_json::from_slice(&data).map_err(|e| error(e.to_string()))
}
