use crate::error::{Error, Result};
use crate::vendors::databento::extract::symbol_map;
use databento::{dbn, historical::timeseries::AsyncDbnDecoder};
use mbn::decode::CombinedDecoder;
use std::collections::HashMap;
use std::fs::File;
use std::io::BufReader;
use std::path::PathBuf;

pub async fn read_dbn_file(
    filepath: PathBuf,
) -> Result<(Vec<dbn::Mbp1Msg>, HashMap<String, String>)> {
    // Read the file
    let mut decoder = AsyncDbnDecoder::from_zstd_file(filepath.clone())
        .await
        .map_err(|_| Error::FileNotFoundError(format!("File not found: {:?}", filepath)))?;

    // Extract Symbol Map
    let metadata = decoder.metadata();
    let map = symbol_map(&metadata)?;

    // Decode to vector of messages
    let mut records = Vec::new();
    while let Some(record) = decoder.decode_record::<dbn::Mbp1Msg>().await? {
        records.push(record.clone());
    }

    Ok((records, map))
}

pub async fn read_mbn_file(filepath: &str) -> Result<Vec<mbn::record_enum::RecordEnum>> {
    let mut decoder = CombinedDecoder::<BufReader<File>>::from_file(filepath)
        .map_err(|_| Error::FileNotFoundError(format!("File not found: {:?}", filepath)))?;

    // Use the iterator to decode records one by one
    if let Some(metadata) = decoder.decode_metadata()? {
        println!("Metadata: {:?}", metadata);
    }

    // Decode all records
    let records = decoder.decode_all_records()?;

    Ok(records)
}

pub async fn compare_dbn(dbn_filepath: PathBuf, mbn_filepath: &str) -> Result<()> {
    let (dbn_records, _map) = read_dbn_file(dbn_filepath).await?;
    let mbn_records = read_mbn_file(mbn_filepath).await?;

    println!("Length dbn: {:?}", dbn_records.len());
    println!("Length mbn: {:?}", mbn_records.len());

    Ok(())
}
