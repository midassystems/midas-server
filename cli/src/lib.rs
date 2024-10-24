pub mod cli;
pub mod commands;
pub mod error;

pub use error::{Error, Result};
const TICKER_FILE: &str = "~/.config/midas/tickers.json";
