use crate::Error;
use std::env;
use std::path::PathBuf;

// Function to get the full path for the tickers.json file
pub fn get_ticker_file() -> crate::Result<String> {
    let home_dir = env::var("HOME").expect("HOME environment variable not set");
    let ticker_file = PathBuf::from(home_dir).join(".config/midas/tickers.json");

    // Convert PathBuf to &str
    let ticker_file_str = ticker_file
        .to_str()
        .ok_or(Error::CustomError(
            "Invalid path: non-UTF-8 characters".to_string(),
        ))?
        .to_string();

    Ok(ticker_file_str)
}
