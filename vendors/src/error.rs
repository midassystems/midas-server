use databento;
use serde_json;
use std::env::VarError;
use thiserror::Error;
use tracing::subscriber::SetGlobalDefaultError;
use walkdir;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Request error: {0}")]
    TracingError(#[from] SetGlobalDefaultError),
    #[error("Io error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("Io error: {0}")]
    EnvVarError(#[from] VarError),
    #[error("General error: {0}")]
    GeneralError(#[from] Box<dyn std::error::Error + Send + Sync + 'static>),
    #[error("MBN error: {0}")]
    MbnError(#[from] mbn::error::Error),
    #[error("Custom error: {0}")]
    CustomError(String),
    #[error("Date error: {0}")]
    DateError(String),
    #[error("Encoding error: {0}")]
    Encode(String),
    #[error("Decoding error: {0}")]
    Decode(String),
    #[error("Conversion error: {0}")]
    Conversion(String),
    #[error("Ticker Loading error: {0}")]
    TickerLoading(String),
    #[error("Databent error: {0}")]
    DatabentoError(#[from] databento::Error),
    #[error("Time Format error: {0}")]
    FormatError(#[from] time::error::Format),
    #[error("Dbn error: {0}")]
    DbnError(#[from] databento::dbn::Error),
    #[error("WalkDir error: {0}")]
    WalkDirError(#[from] walkdir::Error),
    #[error("Midas error: {0}")]
    MidasError(#[from] midas_client::error::Error),
    #[error("Serde Json error: {0}")]
    SerdeJsonError(#[from] serde_json::Error),
    #[error("No data was returned")]
    NoDataError,
    #[error("File not found: {0}")]
    FileNotFoundError(String),
    #[error("Invalid DatabentoDownloadType")]
    InvalidDownloadType,
    // #[error("Tokio io: {0}")]
    // TokioError(#[from] tokio::io::Error),
}

impl Error {
    pub fn extract_message(&self) -> String {
        let error_string = self.to_string();
        if let Some(index) = error_string.find(':') {
            error_string[index + 1..].trim().to_string()
        } else {
            error_string
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;
