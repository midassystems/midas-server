use crate::databento::{
    client::{databento_file_path, DatabentoClient, DatabentoDownloadType},
    extract::{read_dbn_batch_dir, read_dbn_file},
    load::load_file_to_db,
    transform::find_duplicates,
    transform::{instrument_id_map, mbn_to_file, to_mbn},
};
use crate::error::{Error, Result};
use crate::tickers::get_tickers;
use crate::{DATA_DIR, MBN_DATA_DIR};
use databento::dbn::{Dataset, SType, Schema};
use midas_client::client::ApiClient;
use std::collections::HashMap;
use std::env;
use std::path::PathBuf;
use std::str::FromStr;
use time::{self, OffsetDateTime};

pub async fn update(
    schema: Schema,
    start: OffsetDateTime,
    end: OffsetDateTime,
    tickers_filepath: &str,
    client: &ApiClient,
) -> Result<()> {
    // Load ticker file
    let (mbn_map, grouped_tickers) = get_tickers(tickers_filepath, "databento", client).await?;

    // Iterate over different request
    for ((dataset, stype), tickers) in grouped_tickers {
        println!("Processing dataset: {}, stype: {}", dataset, stype);

        // Download
        let (download_type, download_path) =
            download(tickers, schema, start, end, &dataset, &stype).await?;

        // Mbn file path
        let mbn_dir = &PathBuf::from(&*MBN_DATA_DIR);
        let mbn_file_name = format!("{}_{}_{}_{}.bin", dataset, stype, start.date(), end.date());
        let mbn_filepath = mbn_dir.join(&mbn_file_name);

        // Upload
        let _ = upload(
            &download_path,
            &download_type,
            tickers_filepath,
            &mbn_filepath,
            client,
        )
        .await?;
    }

    Ok(())
}

pub async fn download(
    tickers: Vec<String>,
    schema: Schema,
    start: OffsetDateTime,
    end: OffsetDateTime,
    dataset: &str,
    stype: &str,
) -> Result<(DatabentoDownloadType, PathBuf)> {
    // Create the DatabentoClient
    let api_key = env::var("DATABENTO_KEY").expect("DATABENTO_KEY not set.");
    let mut client = DatabentoClient::new(api_key)?;

    // Download
    let (download_type, download_path) = client
        .get_historical(
            &Dataset::from_str(&dataset)?,
            &start,
            &end,
            &tickers,
            &schema,
            &SType::from_str(&stype)?,
            &PathBuf::from(&*DATA_DIR),
        )
        .await?
        .ok_or(Error::NoDataError)?;

    Ok((download_type, download_path))
}

pub async fn upload(
    download_path: &PathBuf,
    download_type: &DatabentoDownloadType,
    tickers_filepath: &str,
    mbn_filepath: &PathBuf,
    client: &ApiClient,
) -> Result<()> {
    // Load symbol file
    let (mbn_map, _) = get_tickers(tickers_filepath, "databento", client).await?;

    let _ = etl_pipeline(
        mbn_map.clone(),
        download_type,
        download_path,
        mbn_filepath,
        client,
    )
    .await?;

    Ok(())
}

pub async fn etl_pipeline(
    mbn_map: HashMap<String, u32>,
    download_type: &DatabentoDownloadType,
    download_path: &PathBuf,
    mbn_filepath: &PathBuf,
    client: &ApiClient,
) -> Result<()> {
    // -- EXTRACT
    let records;
    let dbn_map;
    if download_type == &DatabentoDownloadType::Stream {
        (records, dbn_map) = read_dbn_file(download_path.clone()).await?;
    } else {
        (records, dbn_map) = read_dbn_batch_dir(download_path.clone()).await?;
    }

    // -- TRANSFORM
    // Map DBN instrument to MBN insturment
    let new_map = instrument_id_map(dbn_map, mbn_map.clone())?;
    let mbn_records = to_mbn(records, &new_map)?;
    let num_duplicates = find_duplicates(&mbn_records);
    println!("Duplicates : {:?}", num_duplicates);

    // -- To MBN file
    let _ = mbn_to_file(&mbn_records, &mbn_filepath).await?;
    println!("MBN Path : {:?}", mbn_filepath);

    // -- LOAD
    let _ = load_file_to_db(&mbn_filepath, client).await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use dotenv::dotenv;
    use serial_test::serial;
    use std::env;
    use time::OffsetDateTime;

    #[allow(dead_code)]
    fn setup() -> (
        DatabentoClient,
        Dataset,
        OffsetDateTime,
        OffsetDateTime,
        Vec<String>,
        Schema,
        SType,
    ) {
        dotenv().ok();
        let api_key =
            env::var("DATABENTO_KEY").expect("Expected API key in environment variables.");

        // Parameters
        let dataset = Dataset::GlbxMdp3;
        let start = time::macros::datetime!(2024-08-20 00:00 UTC);
        let end = time::macros::datetime!(2024-08-20 05:00 UTC);
        let symbols = vec!["ZM.n.0".to_string(), "GC.n.0".to_string()];
        let schema = Schema::Mbp1;
        let stype = SType::Continuous;

        let client = DatabentoClient::new(api_key).expect("Failed to create DatabentoClient");
        (client, dataset, start, end, symbols, schema, stype)
    }

    #[tokio::test]
    #[serial]
    #[ignore]
    async fn test_update() -> Result<()> {
        let base_url = "http://localhost:8080";
        let client = ApiClient::new(base_url);

        // Parameters
        let schema = Schema::Mbp1;
        let start = time::macros::datetime!(2024-08-20 00:00 UTC);
        let end = time::macros::datetime!(2024-08-20 05:00 UTC);
        let file_path = "tests/tickers.json";

        // Test
        let _ = update(
            schema,
            start,
            end,
            &client,
            file_path,
            Some(true),
            Some(true),
        )
        .await?;

        // Clean-up
        let (mbn_map, _) = get_tickers(file_path, "databento", &client).await?;

        for value in mbn_map.values() {
            let _ = client.delete_symbol(&(*value as i32)).await?;
        }
        Ok(())
    }
}
