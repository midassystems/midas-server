pub mod client;
pub mod compare;
pub mod extract;
pub mod load;
pub mod transform;

use crate::databento::{
    client::{DatabentoClient, DatabentoDownloadType},
    extract::{read_dbn_batch_dir, read_dbn_file},
    load::load_file_to_db,
    transform::find_duplicates,
    transform::{instrument_id_map, to_mbn},
};
use crate::error::{Error, Result};
use crate::tickers::get_tickers;
use databento::dbn::{Dataset, SType, Schema};
use midas_client::historical::Historical;
use std::collections::HashMap;
use std::env;
use std::path::PathBuf;
use std::str::FromStr;
use time::{self, OffsetDateTime};

pub async fn update(tickers_filepath: &str, client: &Historical) -> Result<()> {
    // Load ticker file
    let (mbn_map, grouped_tickers) = get_tickers(tickers_filepath, "databento", client).await?;

    // End date
    let now = OffsetDateTime::now_utc();
    let end = now.replace_time(time::macros::time!(00:00));

    // Iterate over different request
    for ((dataset, stype, start), tickers) in grouped_tickers {
        println!("Processing dataset: {}, stype: {}", dataset, stype);
        println!("Start: {:?} => End: {:?}", start, end);
        println!("Tickers: {:?}", tickers);

        // Download
        let (download_type, download_path) =
            download(&tickers, Schema::Mbp1, start, end, &dataset, &stype).await?;

        // Mbn file path
        let mbn_filename = PathBuf::from(format!(
            "{}_{}_{}_{}.bin",
            dataset,
            stype,
            start.date(),
            end.date()
        ));

        // Upload
        let _ = upload(
            &download_path,
            &download_type,
            tickers_filepath,
            &mbn_filename,
            client,
            Some(mbn_map.clone()),
        )
        .await?;
    }

    Ok(())
}

pub async fn download(
    tickers: &Vec<String>,
    schema: Schema,
    start: OffsetDateTime,
    end: OffsetDateTime,
    dataset: &str,
    stype: &str,
) -> Result<(DatabentoDownloadType, PathBuf)> {
    // Create the DatabentoClient
    let api_key = env::var("DATABENTO_KEY").expect("DATABENTO_KEY not set.");
    let raw_dir = env::var("RAW_DIR").expect("RAW_DIR not set.");

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
            &PathBuf::from(raw_dir),
        )
        .await?
        .ok_or(Error::NoDataError)?;

    Ok((download_type, download_path))
}

pub async fn upload(
    download_path: &PathBuf,
    download_type: &DatabentoDownloadType,
    tickers_filepath: &str,
    mbn_filename: &PathBuf,
    client: &Historical,
    mbn_map: Option<HashMap<String, u32>>,
) -> Result<()> {
    // If mbn_map is provided, use it; otherwise, fetch it asynchronously
    let mbn_map = if let Some(map) = mbn_map {
        map.clone()
    } else {
        // Fetch mbn_map if it's not provided
        let (mbn_map, _) = get_tickers(tickers_filepath, "databento", client).await?;
        mbn_map
    };

    let _ = etl_pipeline(mbn_map, download_type, download_path, mbn_filename, client).await?;

    Ok(())
}

pub async fn etl_pipeline(
    mbn_map: HashMap<String, u32>,
    download_type: &DatabentoDownloadType,
    download_path: &PathBuf,
    mbn_filename: &PathBuf,
    client: &Historical,
) -> Result<()> {
    let processed_dir = env::var("PROCESSED_DIR").expect("PROCESSED_DIR not set.");

    // -- EXTRACT
    let mut records;
    let dbn_map;
    if download_type == &DatabentoDownloadType::Stream {
        (records, dbn_map) = read_dbn_file(download_path.clone()).await?;
    } else {
        (records, dbn_map) = read_dbn_batch_dir(download_path.clone()).await?;
    }

    // -- TRANSFORM
    // Map DBN instrument to MBN insturment
    let mbn_filepath = &PathBuf::from(processed_dir).join(mbn_filename);
    let new_map = instrument_id_map(dbn_map, mbn_map.clone())?;
    let _ = to_mbn(&mut records, &new_map, mbn_filepath).await?;
    let _ = drop(records);
    println!("MBN Path : {:?}", mbn_filepath);

    // -- Check duplicates
    // let num_duplicates = find_duplicates(&mbn_records);
    // println!("Duplicates : {:?}", num_duplicates);

    // -- To MBN file
    // let _ = mbn_to_file(&mbn_records, &mbn_filepath).await?;

    // -- LOAD
    let mbn_filepath = &PathBuf::from("data/processed_data").join(mbn_filename);
    let _ = load_file_to_db(&mbn_filepath, client).await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use dotenv::dotenv;
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

    // #[tokio::test]
    // #[serial]
    // #[ignore]
    // async fn test_update() -> Result<()> {
    //     let base_url = "http://localhost:8080";
    //     let client = ApiClient::new(base_url);
    //
    //     // Parameters
    //     let schema = Schema::Mbp1;
    //     let start = time::macros::datetime!(2024-08-20 00:00 UTC);
    //     let end = time::macros::datetime!(2024-08-20 05:00 UTC);
    //     let file_path = "tests/tickers.json";
    //
    //     // Test
    //     let _ = update(
    //         schema,
    //         start,
    //         end,
    //         &client,
    //         file_path,
    //         Some(true),
    //         Some(true),
    //     )
    //     .await?;
    //
    //     // Clean-up
    //     let (mbn_map, _) = get_tickers(file_path, "databento", &client).await?;
    //
    //     for value in mbn_map.values() {
    //         let _ = client.delete_symbol(&(*value as i32)).await?;
    //     }
    //     Ok(())
    // }
}
