use std::collections::BTreeMap;
use std::path::Path;

use serde::Deserialize;

use crate::IngestError;

#[derive(Clone)]
pub struct SourceConfig {
    pub id: String,
    pub cluster: String,
    pub rpc_url: String,
    pub commitment: String,
    pub addresses: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct Bounds {
    pub from_slot: i64,
    pub to_slot: i64,
    pub max_transactions: u32,
    pub concurrency: usize,
    pub timeout: std::time::Duration,
    pub max_retries: u32,
    pub deadline: std::time::Duration,
}

#[derive(Deserialize)]
struct FileConfig {
    sources: BTreeMap<String, SourceFile>,
}

#[derive(Deserialize)]
struct SourceFile {
    cluster: String,
    rpc_url: String,
    commitment: String,
    addresses: Vec<String>,
}

pub fn load_source(
    path: &Path,
    source_id: &str,
    address: &str,
) -> Result<SourceConfig, IngestError> {
    let text = std::fs::read_to_string(path)
        .map_err(|error| IngestError::Config(format!("cannot read config: {error}")))?;
    let file: FileConfig = serde_yaml::from_str(&text)
        .map_err(|error| IngestError::Config(format!("cannot parse config: {error}")))?;
    let Some(source) = file.sources.get(source_id) else {
        return Err(IngestError::Config(format!(
            "unknown source id {source_id}"
        )));
    };
    if !matches!(
        source.commitment.as_str(),
        "processed" | "confirmed" | "finalized"
    ) {
        return Err(IngestError::Config(
            "commitment must be processed, confirmed, or finalized".into(),
        ));
    }
    if !source.rpc_url.starts_with("https://")
        && !source.rpc_url.starts_with("http://127.0.0.1")
        && !source.rpc_url.starts_with("http://localhost")
    {
        return Err(IngestError::Config(
            "rpc url must be https, or http on localhost".into(),
        ));
    }
    for configured in &source.addresses {
        validate_address(configured)?;
    }
    validate_address(address)?;
    if !source.addresses.iter().any(|item| item == address) {
        return Err(IngestError::Config(
            "address is not on the configured allowlist".into(),
        ));
    }
    Ok(SourceConfig {
        id: source_id.to_owned(),
        cluster: source.cluster.clone(),
        rpc_url: source.rpc_url.clone(),
        commitment: source.commitment.clone(),
        addresses: source.addresses.clone(),
    })
}

pub fn validate_address(address: &str) -> Result<(), IngestError> {
    let bytes = bs58::decode(address)
        .into_vec()
        .map_err(|_| IngestError::Config("address is not base58".into()))?;
    if bytes.len() != 32 {
        return Err(IngestError::Config(
            "address must decode to 32 bytes".into(),
        ));
    }
    Ok(())
}

pub fn validate_signature(signature: &str) -> Result<(), IngestError> {
    let bytes = bs58::decode(signature)
        .into_vec()
        .map_err(|_| IngestError::Config("signature is not base58".into()))?;
    if bytes.len() != 64 {
        return Err(IngestError::Config(
            "signature must decode to 64 bytes".into(),
        ));
    }
    Ok(())
}

pub fn validate_bounds(bounds: &Bounds) -> Result<(), IngestError> {
    if bounds.from_slot < 0 || bounds.to_slot < bounds.from_slot {
        return Err(IngestError::Config(
            "slot window is empty or negative".into(),
        ));
    }
    if !(1..=500).contains(&bounds.max_transactions) {
        return Err(IngestError::Config(
            "max transactions must be between 1 and 500".into(),
        ));
    }
    if !(1..=16).contains(&bounds.concurrency) {
        return Err(IngestError::Config(
            "concurrency must be between 1 and 16".into(),
        ));
    }
    if bounds.timeout.as_secs() == 0 || bounds.timeout.as_secs() > 30 {
        return Err(IngestError::Config(
            "timeout must be between 1 and 30 seconds".into(),
        ));
    }
    if bounds.max_retries > 5 {
        return Err(IngestError::Config("max retries must be at most 5".into()));
    }
    if bounds.deadline.as_secs() == 0 || bounds.deadline.as_secs() > 600 {
        return Err(IngestError::Config(
            "deadline must be between 1 and 600 seconds".into(),
        ));
    }
    Ok(())
}

pub fn database_url() -> Result<String, IngestError> {
    let url = std::env::var("FORWARDOPS_INGEST_DATABASE_URL")
        .map_err(|_| IngestError::Config("FORWARDOPS_INGEST_DATABASE_URL is required".into()))?;
    if !url.starts_with("postgres://") && !url.starts_with("postgresql://") {
        return Err(IngestError::Config(
            "database url must be a postgres URL".into(),
        ));
    }
    Ok(url)
}

impl std::fmt::Debug for SourceConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SourceConfig")
            .field("id", &self.id)
            .field("cluster", &self.cluster)
            .field("rpc_url", &"[redacted]")
            .field("commitment", &self.commitment)
            .field("addresses", &self.addresses.len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::load_source;

    #[test]
    fn the_address_must_be_allowlisted_and_the_url_stays_out_of_debug_output() {
        let address = bs58::encode([9u8; 32]).into_string();
        let other = bs58::encode([8u8; 32]).into_string();
        let path =
            std::env::temp_dir().join(format!("forwardops-solana-{}.yaml", std::process::id()));
        std::fs::write(
            &path,
            format!(
                "sources:\n  solana-mainnet:\n    cluster: mainnet-beta\n    rpc_url: https://rpc.example\n    commitment: finalized\n    addresses:\n      - \"{address}\"\n"
            ),
        )
        .unwrap();
        assert!(load_source(&path, "solana-mainnet", &other).is_err());
        assert!(load_source(&path, "missing", &address).is_err());
        let source = load_source(&path, "solana-mainnet", &address).unwrap();
        let rendered = format!("{source:?}");
        assert!(rendered.contains("[redacted]"));
        assert!(!rendered.contains("rpc.example"));
        std::fs::write(
            &path,
            format!(
                "sources:\n  solana-mainnet:\n    cluster: mainnet-beta\n    rpc_url: http://rpc.example\n    commitment: finalized\n    addresses:\n      - \"{address}\"\n"
            ),
        )
        .unwrap();
        assert!(load_source(&path, "solana-mainnet", &address).is_err());
        let _ = std::fs::remove_file(path);
    }
}
