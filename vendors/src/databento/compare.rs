use crate::databento::extract::symbol_map;
use crate::error::{Error, Result};
use databento::{dbn, historical::timeseries::AsyncDbnDecoder};
use mbn::decode::{CombinedDecoder, RecordDecoder};
use std::collections::HashMap;
use std::fs::File;
use std::io::BufReader;
use std::path::PathBuf;

pub async fn read_dbn_file(
    filepath: PathBuf,
) -> Result<(Vec<dbn::RecordEnum>, HashMap<String, String>)> {
    // Read the file
    let mut decoder = AsyncDbnDecoder::from_zstd_file(filepath.clone())
        .await
        .map_err(|_| Error::FileNotFoundError(format!("File not found: {:?}", filepath)))?;

    // Extract Symbol Map
    let metadata = decoder.metadata();
    let map = symbol_map(&metadata)?;

    // Decode to vector of messages
    let mut records = Vec::new();
    while let Some(record) = decoder.decode_record_ref().await? {
        let record_enum = record.as_enum()?;
        records.push(record_enum.to_owned());
    }

    Ok((records, map))
}

pub async fn read_mbn_file(
    filepath: &PathBuf,
    metadata_flag: bool,
) -> Result<Vec<mbn::record_enum::RecordEnum>> {
    let records;
    if metadata_flag {
        let mut decoder = CombinedDecoder::<BufReader<File>>::from_file(filepath)
            .map_err(|_| Error::FileNotFoundError(format!("File not found: {:?}", filepath)))?;

        // Use the iterator to decode records one by one
        if let Some(metadata) = decoder.decode_metadata()? {
            println!("Metadata: {:?}", metadata);
        }

        // Decode all records
        records = decoder.decode_records()?;
    } else {
        let mut decoder = RecordDecoder::<BufReader<File>>::from_file(filepath)
            .map_err(|_| Error::FileNotFoundError(format!("File not found: {:?}", filepath)))?;
        // let mut records_ref = decoder.decode_ref()?;

        // Validate
        records = decoder.decode_to_owned().map_err(|_| {
            Error::Conversion(format!("Error decoding mbn records : {:?}", filepath))
        })?;
    }
    Ok(records)
}

pub async fn compare_dbn(
    dbn_filepath: PathBuf,
    mbn_filepath: &PathBuf,
    metadata_flag: bool,
) -> Result<()> {
    let mut mbn_records = read_mbn_file(mbn_filepath, metadata_flag).await?;
    let (dbn_records, _map) = read_dbn_file(dbn_filepath).await?;

    println!("Length mbn: {:?}", mbn_records.len());
    println!("Length dbn: {:?}", dbn_records.len());

    // Track unmatched dbn_records
    let mut unmatched_dbn_records = Vec::new();

    // For each dbn_record, check if there is a matching record in mbn_records using PartialEq
    for dbn_record in &dbn_records {
        if let Some(pos) = mbn_records
            .iter()
            .position(|mbn_record| mbn_record == dbn_record)
        {
            // Remove the matched record from mbn_records
            mbn_records.remove(pos);
        } else {
            // If no match is found, add the dbn_record to unmatched list
            unmatched_dbn_records.push(dbn_record);
        }
    }

    // If there are remaining unmatched records in dbn_records, report them
    if !unmatched_dbn_records.is_empty() {
        return Err(Error::CustomError(format!(
            "Unmatched records found in dbn_records: {:?}",
            unmatched_dbn_records
        )));
    }

    // Finally, check if mbn_records is empty
    if !mbn_records.is_empty() {
        return Err(Error::CustomError(format!(
            "Unmatched records found in mbn_records: {:?}",
            mbn_records
        )));
    }

    println!("All records match successfully.");
    Ok(())
}

#[cfg(test)]
mod tests {
    // use dbn::Mbp1Msg;

    use super::*;

    #[tokio::test]
    async fn test_read_mbn_file() -> Result<()> {
        let file_path = PathBuf::from("tests/data/load_testing_file.bin");

        // Test
        let records = read_mbn_file(&file_path, false).await?;

        // Validate
        assert!(records.len() > 0);

        Ok(())
    }
    #[tokio::test]
    async fn test_read_dbn_file() -> Result<()> {
        let file_path = PathBuf::from(
            "tests/data/databento/GLBX.MDP3_mbp-1_2024-08-20T00:00:00Z_2024-08-20T05:00:00Z.dbn",
        );

        // Test
        let (records, _) = read_dbn_file(file_path).await?;

        // Validate
        assert!(records.len() > 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_compare_dbn() -> Result<()> {
        let mbn_file_path = PathBuf::from("tests/data/load_testing_file.bin");
        let dbn_file_path = PathBuf::from(
            "tests/data/databento/GLBX.MDP3_mbp-1_2024-08-20T00:00:00Z_2024-08-20T05:00:00Z.dbn",
        );

        // Test
        let x = compare_dbn(dbn_file_path, &mbn_file_path, false).await?;

        // Validate
        assert!(x == ());

        Ok(())
    }
}
