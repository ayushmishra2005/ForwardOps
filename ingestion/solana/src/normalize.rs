use chrono::{DateTime, TimeZone, Utc};
use serde_json::Value;
use sha2::{Digest, Sha256};

pub const LOG_LIMIT: usize = 20;
pub const LOG_CHARS: usize = 200;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NormalizedRecord {
    pub signature: String,
    pub slot: i64,
    pub block_time: Option<DateTime<Utc>>,
    pub outcome: &'static str,
    pub program_ids: Vec<String>,
    pub instruction_errors: Value,
    pub log_messages: Vec<String>,
    pub logs_truncated: bool,
    pub payload_sha256: Option<String>,
    pub gap_reason: Option<String>,
}

pub fn digest(value: &Value) -> String {
    hex::encode(Sha256::digest(
        serde_json::to_vec(value).unwrap_or_default(),
    ))
}

pub fn normalize_transaction(
    signature: &str,
    fallback_slot: i64,
    fallback_block_time: Option<i64>,
    body: &Value,
) -> NormalizedRecord {
    let slot = body
        .get("slot")
        .and_then(Value::as_i64)
        .filter(|slot| *slot >= 0)
        .unwrap_or(fallback_slot);
    let block_time = body
        .get("blockTime")
        .and_then(Value::as_i64)
        .or(fallback_block_time)
        .and_then(|seconds| Utc.timestamp_opt(seconds, 0).single());
    let program_ids = program_ids(body);
    let instruction_errors = instruction_errors(body);
    let (log_messages, logs_truncated) = logs(body);
    NormalizedRecord {
        signature: signature.to_owned(),
        slot,
        block_time,
        outcome: "stored",
        program_ids,
        instruction_errors,
        log_messages,
        logs_truncated,
        payload_sha256: Some(digest(body)),
        gap_reason: None,
    }
}

pub fn gap(
    signature: &str,
    slot: i64,
    block_time: Option<i64>,
    outcome: &'static str,
    reason: &str,
) -> NormalizedRecord {
    NormalizedRecord {
        signature: signature.to_owned(),
        slot,
        block_time: block_time.and_then(|seconds| Utc.timestamp_opt(seconds, 0).single()),
        outcome,
        program_ids: Vec::new(),
        instruction_errors: Value::Array(Vec::new()),
        log_messages: Vec::new(),
        logs_truncated: false,
        payload_sha256: None,
        gap_reason: Some(reason.to_owned()),
    }
}

fn program_ids(body: &Value) -> Vec<String> {
    let message = body.pointer("/transaction/message");
    let Some(message) = message else {
        return Vec::new();
    };
    let Some(keys) = message.get("accountKeys").and_then(Value::as_array) else {
        return Vec::new();
    };
    let Some(instructions) = message.get("instructions").and_then(Value::as_array) else {
        return Vec::new();
    };
    let mut ids = Vec::new();
    for instruction in instructions {
        let Some(index) = instruction.get("programIdIndex").and_then(Value::as_u64) else {
            continue;
        };
        let Some(key) = keys.get(index as usize) else {
            continue;
        };
        if let Some(text) = key.as_str() {
            ids.push(text.to_owned());
        } else if let Some(text) = key.get("pubkey").and_then(Value::as_str) {
            ids.push(text.to_owned());
        }
    }
    ids.sort();
    ids.dedup();
    ids
}

fn instruction_errors(body: &Value) -> Value {
    match body.pointer("/meta/err") {
        None | Some(Value::Null) => Value::Array(Vec::new()),
        Some(value) => Value::Array(vec![value.clone()]),
    }
}

fn logs(body: &Value) -> (Vec<String>, bool) {
    let Some(raw) = body.pointer("/meta/logMessages").and_then(Value::as_array) else {
        return (Vec::new(), false);
    };
    let truncated = raw.len() > LOG_LIMIT
        || raw
            .iter()
            .any(|item| item.as_str().unwrap_or("").chars().count() > LOG_CHARS);
    let messages = raw
        .iter()
        .take(LOG_LIMIT)
        .filter_map(Value::as_str)
        .map(|text| text.chars().take(LOG_CHARS).collect())
        .collect();
    (messages, truncated)
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;
    use chrono::Utc;
    use serde_json::json;

    use super::{digest, normalize_transaction};

    fn sample() -> serde_json::Value {
        json!({
            "slot": 42,
            "blockTime": null,
            "transaction": {
                "message": {
                    "accountKeys": ["11111111111111111111111111111111", "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"],
                    "instructions": [{"programIdIndex": 1}, {"programIdIndex": 1}, {"programIdIndex": 9}]
                }
            },
            "meta": {
                "err": {"InstructionError": [0, {"Custom": 1}]},
                "logMessages": ["one", "two"]
            }
        })
    }

    #[test]
    fn unknown_transactions_stay_generic() {
        let record = normalize_transaction("sig", 7, None, &sample());
        assert_eq!(record.outcome, "stored");
        assert_eq!(record.slot, 42);
        assert_eq!(record.block_time, None);
        assert_eq!(
            record.program_ids,
            vec!["TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA".to_owned()]
        );
        assert_eq!(
            record.instruction_errors,
            json!([{"InstructionError": [0, {"Custom": 1}]}])
        );
        assert_eq!(
            record.log_messages,
            vec!["one".to_owned(), "two".to_owned()]
        );
        assert!(!record.logs_truncated);
        assert_eq!(
            record.payload_sha256.as_deref(),
            Some(digest(&sample()).as_str())
        );
    }

    #[test]
    fn logs_are_capped_and_a_missing_slot_uses_the_signature_slot() {
        let mut body = sample();
        body["slot"] = json!(null);
        body["blockTime"] = json!(1_700_000_000);
        body["meta"]["logMessages"] = json!(["abcdefghij".repeat(30), "tail"]);
        let record = normalize_transaction("sig", 7, Some(1_600_000_000), &body);
        assert_eq!(record.slot, 7);
        assert_eq!(
            record.block_time,
            Some(Utc.timestamp_opt(1_700_000_000, 0).single().unwrap())
        );
        assert!(record.logs_truncated);
        assert_eq!(record.log_messages[0].chars().count(), 200);
    }
}
