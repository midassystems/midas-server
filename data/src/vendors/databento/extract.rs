use crate::error::Result;
use databento::dbn::Mbp1Msg;
use databento::{dbn, historical::timeseries::AsyncDbnDecoder};
use std::collections::HashMap;
use std::path::PathBuf;
use tokio::fs::File;
use walkdir::WalkDir;

pub fn symbol_map(metadata: &dbn::Metadata) -> Result<HashMap<String, String>> {
    let mut symbol_map_hash = HashMap::new();

    for mapping in &metadata.mappings {
        for interval in &mapping.intervals {
            symbol_map_hash.insert(interval.symbol.clone(), mapping.raw_symbol.to_string());
        }
    }
    Ok(symbol_map_hash)
}

/// Read stream dbn file.
pub async fn read_dbn_file(filepath: PathBuf) -> Result<(Vec<Mbp1Msg>, HashMap<String, String>)> {
    // Read the file
    let mut decoder = AsyncDbnDecoder::from_zstd_file(filepath)
        .await
        .expect("Replace later.");

    // Extract Symbol Map
    let metadata = decoder.metadata();
    let map = symbol_map(&metadata)?;

    // Decode to vector of messages
    let mut records = Vec::new();
    while let Some(record) = decoder.decode_record::<Mbp1Msg>().await? {
        records.push(record.clone());
    }

    Ok((records, map))
}

/// Read directory created by dbn batch job.
pub async fn read_dbn_batch_dir(
    dir_path: PathBuf,
) -> Result<(Vec<Mbp1Msg>, HashMap<String, String>)> {
    // List files in batch dir
    let mut files = Vec::new();
    for entry in WalkDir::new(dir_path)
        .into_iter()
        .filter_map(|e| e.ok()) // Use a closure to handle errors
        .filter(|e| e.file_type().is_file())
    {
        files.push(entry.path().to_path_buf());
    }

    // Read files into decoder
    let mut records = Vec::new();
    let mut symbol_map_hash = HashMap::new();

    // Decode Files
    for downloaded_file in files {
        if let Some(extension) = downloaded_file.extension() {
            if extension == "zst" {
                let mut decoder =
                    AsyncDbnDecoder::with_zstd(File::open(&downloaded_file).await?).await?;

                // Extract Symbol Map
                let metadata = decoder.metadata();
                let map = symbol_map(&metadata)?;

                // Merge the symbol map into the global map
                for (id, ticker) in map {
                    symbol_map_hash.insert(id, ticker.clone());
                }

                // Decode to vector of messages
                while let Some(record) = decoder.decode_record::<Mbp1Msg>().await? {
                    records.push(record.clone());
                }
            }
        }
    }

    Ok((records, symbol_map_hash))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Result;
    use crate::vendors::databento::client::databento_file_path;
    use databento::dbn::{Dataset, Schema};
    use time;

    fn setup(dir_path: &str) -> Result<PathBuf> {
        // Parameters
        let dataset = Dataset::GlbxMdp3;
        let start = time::macros::datetime!(2022-06-10 14:30 UTC);
        let end = time::macros::datetime!(2022-06-10 14:32 UTC);
        let schema = Schema::Mbp1;

        // Construct file path
        let file_path = databento_file_path(dir_path, &dataset, &schema, &start, &end)?;

        Ok(file_path)
    }

    #[tokio::test]
    async fn test_read_dbn_stream_file() -> Result<()> {
        let file_path = setup("tests/data/databento").unwrap();

        // Test
        let (records, map) = read_dbn_file(file_path).await?;

        // Validate
        assert!(records.len() > 0);
        assert!(!map.is_empty(), "The map should not be empty");

        Ok(())
    }

    #[tokio::test]
    async fn test_read_dbn_batch_file() -> Result<()> {
        let dir_path = setup("tests/data/databento/batch").unwrap();

        // Test
        let (records, map) = read_dbn_batch_dir(dir_path).await?;

        // Validate
        assert!(records.len() > 0);
        assert!(!map.is_empty(), "The map should not be empty");

        Ok(())
    }
}
