//! Bounded Solana backfill. This crate does not investigate incidents.

pub mod checkpoint;
pub mod config;
pub mod ingest;
pub mod normalize;
pub mod rpc;
pub mod storage;

pub const DECODER_VERSION: &str = "generic-v1";
pub const RECORD_IDENTITY: &str = "transaction";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IngestError {
    Config(String),
    Rpc(String),
    Storage(String),
    Blocked(String),
}

impl std::fmt::Display for IngestError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Config(message)
            | Self::Rpc(message)
            | Self::Storage(message)
            | Self::Blocked(message) => formatter.write_str(message),
        }
    }
}

impl std::error::Error for IngestError {}

pub type Result<T> = std::result::Result<T, IngestError>;
