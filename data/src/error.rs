use databento;
use polars::error::PolarsError;
use serde_json;
use std::{env::VarError, io};
use thiserror::Error;
use walkdir;

#[derive(Error, Debug)]
pub enum Error {
    #[error("IO error: {0}")]
    Io(#[from] io::Error),
    #[error("Encoding error: {0}")]
    Encode(String),
    #[error("Decoding error: {0}")]
    Decode(String),
    #[error("Conversion error: {0}")]
    Conversion(String),
    #[error("Var error: {0}")]
    VarError(#[from] VarError),
    #[error("Databent error: {0}")]
    DatabentoError(#[from] databento::Error),
    #[error("Time Format error: {0}")]
    FormatError(#[from] time::error::Format),
    #[error("Dbn error: {0}")]
    DbnError(#[from] databento::dbn::Error),
    #[error("WalkDir error: {0}")]
    WalkDirError(#[from] walkdir::Error),
    #[error("Polars error: {0}")]
    PolarsError(#[from] PolarsError),
    #[error("Midas error: {0}")]
    MidasError(#[from] midas_client::error::Error),
    #[error("Serde Json error: {0}")]
    SerdeJsonError(#[from] serde_json::Error),
    #[error("Mbn error: {0}")]
    MbnError(#[from] mbn::error::Error),
    #[error("Anyhow Error: {0}")]
    AnyhotError(#[from] anyhow::Error),
    #[error("Config Error: {0}")]
    ConfigError(#[from] config::error::Error),
    #[error("No data was returned")]
    NoDataError,
    #[error("File not found: {0}")]
    FileNotFoundError(String), // New custom error for file not found
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
