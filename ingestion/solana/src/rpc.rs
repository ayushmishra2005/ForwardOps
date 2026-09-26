use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};

use serde_json::{json, Value};

use crate::config::{validate_signature, SourceConfig};
use crate::IngestError;

pub const ALLOWED_METHODS: &[&str] = &["getSignaturesForAddress", "getTransaction"];
pub const PAGE_LIMIT: usize = 100;
const BACKOFF_START: Duration = Duration::from_millis(50);
const BACKOFF_CAP: Duration = Duration::from_secs(2);
const RETRY_AFTER_CAP: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignatureInfo {
    pub signature: String,
    pub slot: i64,
    pub block_time: Option<i64>,
}

#[derive(Debug, Clone)]
pub enum RpcError {
    Retryable {
        retry_after: Option<Duration>,
        message: String,
    },
    Fatal(String),
}

impl RpcError {
    pub fn message(&self) -> &str {
        match self {
            Self::Retryable { message, .. } | Self::Fatal(message) => message,
        }
    }
}

pub fn reject_method(method: &str) -> Result<(), RpcError> {
    if ALLOWED_METHODS.contains(&method) {
        Ok(())
    } else {
        Err(RpcError::Fatal(format!(
            "rpc method {method} is not allowed"
        )))
    }
}

pub trait SolanaSource: Send + Sync {
    fn signature_page(
        &self,
        address: &str,
        before: Option<&str>,
    ) -> impl std::future::Future<Output = Result<Vec<SignatureInfo>, RpcError>> + Send;
    fn transaction(
        &self,
        signature: &str,
    ) -> impl std::future::Future<Output = Result<Option<Value>, RpcError>> + Send;
}

pub struct RpcClient {
    http: reqwest::Client,
    url: String,
    commitment: String,
}

impl RpcClient {
    pub fn new(source: &SourceConfig, timeout: Duration) -> Result<Self, IngestError> {
        let http = reqwest::Client::builder()
            .timeout(timeout)
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|error| IngestError::Config(format!("cannot build rpc client: {error}")))?;
        Ok(Self {
            http,
            url: source.rpc_url.clone(),
            commitment: source.commitment.clone(),
        })
    }

    async fn call(&self, method: &str, params: Value) -> Result<Value, RpcError> {
        reject_method(method)?;
        let response = self
            .http
            .post(&self.url)
            .json(&json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}))
            .send()
            .await
            .map_err(classify_transport)?;
        let status = response.status();
        let retry_after = retry_after_header(response.headers());
        if status == reqwest::StatusCode::TOO_MANY_REQUESTS || status.is_server_error() {
            return Err(RpcError::Retryable {
                retry_after,
                message: format!("{method} returned HTTP {}", status.as_u16()),
            });
        }
        if !status.is_success() {
            return Err(RpcError::Fatal(format!(
                "{method} returned HTTP {}",
                status.as_u16()
            )));
        }
        let body: Value = response
            .json()
            .await
            .map_err(|_| RpcError::Fatal(format!("{method} returned malformed JSON")))?;
        if let Some(error) = body.get("error") {
            return Err(classify_rpc_error(method, error));
        }
        body.get("result")
            .cloned()
            .ok_or_else(|| RpcError::Fatal(format!("{method} response has no result")))
    }
}

impl SolanaSource for RpcClient {
    async fn signature_page(
        &self,
        address: &str,
        before: Option<&str>,
    ) -> Result<Vec<SignatureInfo>, RpcError> {
        let mut options = json!({"commitment": self.commitment, "limit": PAGE_LIMIT});
        if let Some(cursor) = before {
            options["before"] = json!(cursor);
        }
        let result = self
            .call("getSignaturesForAddress", json!([address, options]))
            .await?;
        parse_signatures(&result)
    }

    async fn transaction(&self, signature: &str) -> Result<Option<Value>, RpcError> {
        let params = json!([
            signature,
            {"encoding": "json", "commitment": self.commitment, "maxSupportedTransactionVersion": 0}
        ]);
        let result = self.call("getTransaction", params).await?;
        if result.is_null() {
            Ok(None)
        } else {
            Ok(Some(result))
        }
    }
}

pub async fn with_retry<T, F, Fut>(
    deadline: Instant,
    max_retries: u32,
    retries: &AtomicU32,
    mut operation: F,
) -> Result<T, RpcError>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, RpcError>>,
{
    let mut attempt = 0u32;
    let mut backoff = BACKOFF_START;
    loop {
        if Instant::now() >= deadline {
            return Err(RpcError::Fatal("run deadline elapsed".into()));
        }
        match operation().await {
            Ok(value) => return Ok(value),
            Err(RpcError::Fatal(message)) => return Err(RpcError::Fatal(message)),
            Err(RpcError::Retryable {
                retry_after,
                message,
            }) => {
                if attempt >= max_retries {
                    return Err(RpcError::Retryable {
                        retry_after,
                        message,
                    });
                }
                retries.fetch_add(1, Ordering::Relaxed);
                let pause = retry_after.unwrap_or(backoff).min(RETRY_AFTER_CAP);
                let remaining = deadline.saturating_duration_since(Instant::now());
                if pause >= remaining {
                    return Err(RpcError::Fatal("run deadline elapsed".into()));
                }
                tokio::time::sleep(pause).await;
                backoff = (backoff * 2).min(BACKOFF_CAP);
                attempt += 1;
            }
        }
    }
}

fn classify_transport(error: reqwest::Error) -> RpcError {
    if error.is_timeout() {
        RpcError::Retryable {
            retry_after: None,
            message: "rpc timeout".into(),
        }
    } else {
        RpcError::Retryable {
            retry_after: None,
            message: "temporary rpc failure".into(),
        }
    }
}

fn classify_rpc_error(method: &str, error: &Value) -> RpcError {
    let message = error
        .get("message")
        .and_then(Value::as_str)
        .unwrap_or("rpc error");
    let code = error.get("code").and_then(Value::as_i64).unwrap_or(0);
    if code == 429 || code == -32005 || message.to_ascii_lowercase().contains("rate limit") {
        return RpcError::Retryable {
            retry_after: None,
            message: format!("{method} rate limited"),
        };
    }
    if message.to_ascii_lowercase().contains("version") {
        return RpcError::Fatal(format!("unsupported transaction version: {message}"));
    }
    RpcError::Fatal(format!("{method} rejected the request: {message}"))
}

fn retry_after_header(headers: &reqwest::header::HeaderMap) -> Option<Duration> {
    let value = headers.get(reqwest::header::RETRY_AFTER)?.to_str().ok()?;
    let seconds: u64 = value.parse().ok()?;
    Some(Duration::from_secs(seconds.min(RETRY_AFTER_CAP.as_secs())))
}

pub fn parse_signatures(result: &Value) -> Result<Vec<SignatureInfo>, RpcError> {
    let Some(rows) = result.as_array() else {
        return Err(RpcError::Fatal("signature page is malformed".into()));
    };
    let mut parsed = Vec::with_capacity(rows.len());
    for row in rows {
        let signature = row
            .get("signature")
            .and_then(Value::as_str)
            .ok_or_else(|| RpcError::Fatal("signature page is malformed".into()))?;
        validate_signature(signature)
            .map_err(|_| RpcError::Fatal("signature page is malformed".into()))?;
        let slot = row
            .get("slot")
            .and_then(Value::as_i64)
            .filter(|slot| *slot >= 0)
            .ok_or_else(|| RpcError::Fatal("signature page is malformed".into()))?;
        let block_time = match row.get("blockTime") {
            None | Some(Value::Null) => None,
            Some(value) => Some(
                value
                    .as_i64()
                    .ok_or_else(|| RpcError::Fatal("signature page is malformed".into()))?,
            ),
        };
        parsed.push(SignatureInfo {
            signature: signature.to_owned(),
            slot,
            block_time,
        });
    }
    Ok(parsed)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{classify_rpc_error, parse_signatures, reject_method, RpcError};

    #[test]
    fn only_the_two_read_methods_are_allowed() {
        assert!(reject_method("getSignaturesForAddress").is_ok());
        assert!(reject_method("getTransaction").is_ok());
        assert!(reject_method("sendTransaction").is_err());
        assert!(reject_method("simulateTransaction").is_err());
    }

    #[test]
    fn rate_limits_are_retryable_and_version_errors_are_not() {
        let limited = classify_rpc_error(
            "getTransaction",
            &json!({"code": 429, "message": "rate limit"}),
        );
        assert!(matches!(limited, RpcError::Retryable { .. }));
        let version = classify_rpc_error(
            "getTransaction",
            &json!({"code": -32015, "message": "Transaction version (1) is not supported"}),
        );
        assert!(matches!(version, RpcError::Fatal(message) if message.contains("unsupported")));
    }

    #[test]
    fn a_malformed_signature_page_is_rejected() {
        let error = parse_signatures(&json!({"signature": "nope"})).unwrap_err();
        assert!(matches!(error, RpcError::Fatal(message) if message.contains("malformed")));
    }

    #[tokio::test]
    async fn retries_honor_the_budget_and_retry_after() {
        use std::sync::atomic::{AtomicU32, Ordering};
        use std::sync::Arc;
        use std::time::{Duration, Instant};

        use super::with_retry;

        let retries = AtomicU32::new(0);
        let calls = Arc::new(AtomicU32::new(0));
        let deadline = Instant::now() + Duration::from_secs(2);
        let calls_for_success = calls.clone();
        let value = with_retry(deadline, 2, &retries, move || {
            let calls = calls_for_success.clone();
            async move {
                let seen = calls.fetch_add(1, Ordering::Relaxed);
                if seen < 2 {
                    Err(RpcError::Retryable {
                        retry_after: Some(Duration::from_millis(1)),
                        message: "429".into(),
                    })
                } else {
                    Ok(7)
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(value, 7);
        assert_eq!(retries.load(Ordering::Relaxed), 2);

        let exhausted = AtomicU32::new(0);
        let error = with_retry(
            Instant::now() + Duration::from_millis(50),
            1,
            &exhausted,
            || async {
                Err::<(), RpcError>(RpcError::Retryable {
                    retry_after: Some(Duration::from_secs(30)),
                    message: "429".into(),
                })
            },
        )
        .await
        .unwrap_err();
        assert!(matches!(error, RpcError::Fatal(message) if message.contains("deadline")));
        assert_eq!(exhausted.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn a_null_block_time_stays_absent() {
        let signature = bs58::encode([7u8; 64]).into_string();
        let rows =
            parse_signatures(&json!([{"signature": signature, "slot": 12, "blockTime": null}]))
                .unwrap();
        assert_eq!(rows[0].block_time, None);
        assert_eq!(rows[0].slot, 12);
    }
}
