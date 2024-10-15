use crate::error::{Error, Result};
use mbn::symbols::Instrument;
use midas_client::client::ApiClient;
use serde::Deserialize;
use serde_json;
use std::collections::HashMap;
use std::path::PathBuf;
use tokio::fs as async_fs;

#[derive(Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Ticker {
    pub ticker: String,
    pub name: String,
    pub stype: String,
    pub dataset: String,
}

impl Ticker {
    pub fn new(ticker: &str, name: &str, stype: &str, dataset: &str) -> Self {
        Ticker {
            ticker: ticker.to_string(),
            name: name.to_string(),
            stype: stype.to_string(),
            dataset: dataset.to_string(),
        }
    }
    fn to_instrument(&self) -> Instrument {
        Instrument::new(&self.ticker, &self.name, None)
    }
}
pub async fn load_tickers(file_path: &str, source: &str) -> Result<Vec<Ticker>> {
    // Determine the config path
    let config_path = PathBuf::from(file_path);

    // Read the configuration file
    if !config_path.exists() {
        panic!(
            "JSON file not found: {}. Please ensure it exists.",
            config_path.display()
        );
    }

    // Read the JSON file as a string asynchronously
    let data: String = async_fs::read_to_string(config_path).await?;

    // Parse the JSON data into a HashMap where the key is a string and the value is a list of tickers
    let json_data: HashMap<String, Vec<Ticker>> = serde_json::from_str(&data)?;

    // Find and return the tickers associated with the given source (key)
    match json_data.get(source) {
        Some(tickers) => Ok(tickers.clone()), // Just return the found Vec<Ticker>
        None => Err(Error::TickerLoading(format!(
            "Source '{}' not found in the JSON",
            source,
        ))),
    }
}

pub async fn create_tickers(
    tickers: &Vec<Ticker>,
    client: &ApiClient,
) -> Result<HashMap<String, u32>> {
    let mut ticker_map: HashMap<String, u32> = HashMap::new();

    for ticker in tickers {
        let response = client.get_symbol(&ticker.ticker).await?;
        let id: Option<u32> = response.data;

        match id {
            Some(value) => {
                ticker_map.insert(ticker.ticker.clone(), value);
            }
            None => {
                let response = client.create_symbol(&ticker.to_instrument()).await?;
                let id = response.data.ok_or_else(|| {
                    Error::Conversion(format!("Error creating ticker: {}", ticker.ticker.clone()))
                })?;
                ticker_map.insert(ticker.ticker.clone(), id);
            }
        }
    }
    Ok(ticker_map)
}

pub async fn process_tickers(
    tickers: &Vec<Ticker>,
) -> Result<HashMap<(String, String), Vec<String>>> {
    // Group tickers by (dataset, stype)
    let mut grouped_tickers: HashMap<(String, String), Vec<String>> = HashMap::new();

    for ticker in tickers {
        grouped_tickers
            .entry((ticker.dataset.clone(), ticker.stype.clone()))
            .or_insert_with(Vec::new)
            .push(ticker.ticker.clone());
    }

    Ok(grouped_tickers)
}

pub async fn get_tickers(
    file_path: &str,
    source: &str,
    client: &ApiClient,
) -> Result<(HashMap<String, u32>, HashMap<(String, String), Vec<String>>)> {
    let tickers: Vec<Ticker> = load_tickers(file_path, source).await?;
    let mbn_map: HashMap<String, u32> = create_tickers(&tickers, client).await?;
    let grouped_tickers: HashMap<(String, String), Vec<String>> = process_tickers(&tickers).await?;
    Ok((mbn_map, grouped_tickers))
}

#[allow(dead_code)]
fn instruments_to_map(instruments: Vec<Instrument>) -> HashMap<String, u32> {
    instruments
        .into_iter()
        .filter_map(|instrument| instrument.instrument_id.map(|id| (instrument.ticker, id)))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use dotenv::dotenv;
    use serial_test::serial;

    #[tokio::test]
    #[serial]
    async fn test_load_tickers() -> Result<()> {
        // Test
        let tickers = load_tickers("tests/tickers.json", "databento").await?;

        // Validate
        assert!(tickers.len() > 0);

        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn test_create_tickers() -> Result<()> {
        dotenv().ok();
        let client = ApiClient::new("http://127.0.0.1:8080");

        // Test
        let tickers = load_tickers("tests/tickers.json", "databento").await?;
        let mbn_tickers = create_tickers(&tickers, &client).await?;
        assert_eq!(mbn_tickers.len(), 2);

        // Cleanup
        for instrument in mbn_tickers {
            let _ = client.delete_symbol(&(instrument.1 as i32)).await?;
        }

        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn test_process_tickers() -> Result<()> {
        let tickers = load_tickers("tests/tickers.json", "databento").await?;

        // Test
        let grouped_tickers = process_tickers(&tickers).await?;

        // Validate
        for group in grouped_tickers {
            assert_eq!(group.0, ("GLBX.MDP3".to_string(), "continuous".to_string()));
            assert_eq!(group.1, vec!["ZM.n.0".to_string(), "GC.n.0".to_string()]);
        }
        //
        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn test_get_tickers() -> Result<()> {
        let base_url = "http://localhost:8080"; // Update with your actual base URL
        let client = ApiClient::new(base_url);

        // Test
        let (mbn_map, _grouped_tickers) =
            get_tickers("tests/tickers.json", "databento", &client).await?;

        // Validate
        for x in vec!["ZM.n.0", "GC.n.0"] {
            assert!(mbn_map.contains_key(x));
        }

        // Cleanup
        for value in mbn_map.values() {
            let _ = client.delete_symbol(&(*value as i32)).await?;
        }

        Ok(())
    }
}
