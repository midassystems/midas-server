use crate::databento::extract::read_dbn_file;
use crate::error::{Error, Result};
use mbn::decode::AsyncDecoder;
use mbn::record_enum::RecordEnum;
use std::path::PathBuf;
use tokio::fs::File;
use tokio::io::BufReader;

pub async fn read_mbn_file(filepath: &PathBuf) -> Result<AsyncDecoder<BufReader<File>>> {
    let decoder = AsyncDecoder::<BufReader<File>>::from_file(filepath).await?;

    Ok(decoder)
}

pub async fn compare_dbn(dbn_filepath: PathBuf, mbn_filepath: &PathBuf) -> Result<()> {
    let batch_size = 1000; // New parameter to control batch size
    let mut mbn_decoder = read_mbn_file(mbn_filepath).await?;
    let (mut dbn_decoder, _map) = read_dbn_file(dbn_filepath).await?;

    let mut mbn_batch = Vec::new();
    let mut mbn_decoder_done = false;

    // Keep track of any unmatched DBN records
    let mut unmatched_dbn_records = Vec::new();

    // Start decoding and comparing
    while let Some(dbn_record) = dbn_decoder.decode_record_ref().await? {
        let dbn_record_enum = dbn_record.as_enum()?.to_owned();

        // If MBN batch is empty, refill it
        if mbn_batch.len() < batch_size && !mbn_decoder_done {
            while let Some(mbn_record) = mbn_decoder.decode_ref().await? {
                mbn_batch.push(RecordEnum::from_ref(mbn_record)?);
            }
            if mbn_batch.is_empty() {
                mbn_decoder_done = true; // No more MBN records
            }
        }

        // Try to find a match for the current DBN record in the MBN batch
        if let Some(pos) = mbn_batch
            .iter()
            .position(|mbn_record| mbn_record == &dbn_record_enum)
        {
            mbn_batch.remove(pos); // Remove matched record
        } else {
            // If no match found, add to unmatched DBN list
            unmatched_dbn_records.push(dbn_record_enum.to_owned());
        }
    }

    // Check for remaining unmatched MBN records
    if !mbn_batch.is_empty() {
        return Err(Error::CustomError(format!(
            "Unmatched records found in mbn_records: {:?}",
            mbn_batch
        )));
    }

    // Check for remaining unmatched DBN records
    if !unmatched_dbn_records.is_empty() {
        return Err(Error::CustomError(format!(
            "Unmatched records found in dbn_records: {:?}",
            unmatched_dbn_records
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
    // #[ignore]
    async fn test_read_mbn_file() -> Result<()> {
        let file_path = PathBuf::from("tests/data/load_testing_file.bin");

        // Test
        let mut decoder = read_mbn_file(&file_path).await?;

        // Validate
        let mbn_records = decoder.decode().await?;
        assert!(mbn_records.len() > 0);

        Ok(())
    }
    #[tokio::test]
    // #[ignore]
    async fn test_read_dbn_file() -> Result<()> {
        let file_path = PathBuf::from(
            "tests/data/databento/GLBX.MDP3_mbp-1_2024-08-20T00:00:00Z_2024-08-20T05:00:00Z.dbn",
        );

        // Test
        let (mut decoder, _) = read_dbn_file(file_path).await?;

        // Validate
        let _metadata = decoder.metadata();

        // Decode to vector of messages
        let mut dbn_records = Vec::new();
        while let Some(record) = decoder.decode_record_ref().await? {
            let record_enum = record.as_enum()?;
            dbn_records.push(record_enum.to_owned());
        }

        assert!(dbn_records.len() > 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_compare_dbn() -> Result<()> {
        let mbn_file_path = PathBuf::from("tests/data/load_testing_file.bin");
        let dbn_file_path = PathBuf::from(
            "tests/data/databento/GLBX.MDP3_mbp-1_2024-08-20T00:00:00Z_2024-08-20T05:00:00Z.dbn",
        );

        // Test
        let x = compare_dbn(dbn_file_path, &mbn_file_path).await?;

        // Validate
        assert!(x == ());

        Ok(())
    }
}
