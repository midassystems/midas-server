use crate::error::Result;
use async_compression::tokio::bufread::ZstdDecoder;
use databento::{dbn, historical::timeseries::AsyncDbnDecoder};
use std::collections::HashMap;
use std::path::PathBuf;
use tokio::fs::File;
use tokio::io::BufReader;
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
pub async fn read_dbn_file(
    filepath: PathBuf,
) -> Result<(
    AsyncDbnDecoder<ZstdDecoder<BufReader<File>>>,
    HashMap<String, String>,
)> {
    // Read the file
    let decoder = AsyncDbnDecoder::from_zstd_file(filepath)
        .await
        .expect("Replace later.");

    // Extract Symbol Map
    let metadata = decoder.metadata();
    let map = symbol_map(&metadata)?;

    Ok((decoder, map))
}

/// Read directory created by dbn batch job.
pub async fn read_dbn_batch_dir(
    dir_path: PathBuf,
) -> Result<(
    AsyncDbnDecoder<ZstdDecoder<BufReader<File>>>,
    HashMap<String, String>,
)> {
    // List files in batch dir
    let mut files = Vec::new();
    for entry in WalkDir::new(dir_path)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file())
    {
        files.push(entry.path().to_path_buf());
    }

    let mut symbol_map_hash = HashMap::new();
    let mut decoder = None;

    // Decode Files
    for downloaded_file in files {
        if let Some(extension) = downloaded_file.extension() {
            if extension == "zst" {
                let current_decoder =
                    AsyncDbnDecoder::with_zstd(File::open(&downloaded_file).await?).await?;

                // Extract Symbol Map
                let metadata = current_decoder.metadata();
                let map = symbol_map(&metadata)?;

                // Merge the symbol map into the global map
                for (id, ticker) in map {
                    symbol_map_hash.insert(id, ticker.clone());
                }

                // Set decoder to the first file found
                if decoder.is_none() {
                    decoder = Some(current_decoder);
                }
            }
        }
    }

    // Ensure a decoder was found
    if let Some(decoder) = decoder {
        Ok((decoder, symbol_map_hash))
    } else {
        Err(crate::Error::CustomError(
            "No Zstd compressed files found".to_string(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::databento::client::databento_file_path;
    use crate::error::Result;
    use databento::dbn::{Dataset, Schema};
    use time;

    fn setup(dir_path: &PathBuf) -> Result<PathBuf> {
        // Parameters
        let dataset = Dataset::GlbxMdp3;
        let start = time::macros::datetime!(2024-08-20 00:00 UTC);
        let end = time::macros::datetime!(2024-08-20 05:00 UTC);
        let schema = Schema::Mbp1;
        let symbols = vec!["ZM.n.0".to_string(), "GC.n.0".to_string()];

        // Construct file path
        let file_path = databento_file_path(dir_path, &dataset, &schema, &start, &end, &symbols)?;

        Ok(file_path)
    }

    #[tokio::test]
    async fn test_read_dbn_stream_file() -> Result<()> {
        let file_path = setup(&PathBuf::from("tests/data/databento"))?;

        // Test
        let (mut decoder, map) = read_dbn_file(file_path).await?;

        let mut records = Vec::new();
        while let Some(record) = decoder.decode_record::<dbn::Mbp1Msg>().await? {
            records.push(record.clone());
        }

        // Validate
        assert!(records.len() > 0);
        assert!(!map.is_empty(), "The map should not be empty");

        Ok(())
    }

    #[tokio::test]
    async fn test_read_dbn_batch_file() -> Result<()> {
        let dir_path = setup(&PathBuf::from("tests/data/databento/batch"))?;

        // Test
        let (mut decoder, map) = read_dbn_batch_dir(dir_path).await?;

        let mut records = Vec::new();
        while let Some(record) = decoder.decode_record::<dbn::Mbp1Msg>().await? {
            records.push(record.clone());
        }

        // Validate
        assert!(records.len() > 0);
        assert!(!map.is_empty(), "The map should not be empty");

        Ok(())
    }
}
