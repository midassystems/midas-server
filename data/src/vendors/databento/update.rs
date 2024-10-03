use crate::error::{Error, Result};
use crate::vendors::databento::client::{DatabentoClient, DatabentoDownloadType};
use crate::vendors::databento::extract::{read_dbn_batch_dir, read_dbn_file};
use crate::vendors::databento::load::load_to_db;
use crate::vendors::databento::transform::find_duplicates;
use crate::vendors::databento::transform::{instrument_id_map, to_mbn};
use config::{settings::Config, tickers::get_tickers};
use databento::dbn::{Dataset, SType, Schema};
use mbn::{self, encode::RecordEncoder, record_ref::RecordRef, records::Mbp1Msg};
use midas_client::client::ApiClient;
use std::str::FromStr;
use time::{self, OffsetDateTime};

pub enum UpdateTypes {
    Stream,
    Bulk,
}

pub async fn update(
    schema: Schema,
    start: OffsetDateTime,
    end: OffsetDateTime,
    update_type: &UpdateTypes,
    config: Config,
    client: &ApiClient,
) -> Result<()> {
    // Create the DatabentoClient
    let api_key = config.data.databento_api_key;
    let mut databento_client = DatabentoClient::new(api_key)?;

    // Load symbol file
    let (mbn_map, grouped_tickers) = get_tickers("databento", client).await?;

    // Iterate over different request
    for ((dataset, stype), tickers) in grouped_tickers {
        println!("Processing dataset: {}, stype: {}", dataset, stype);

        // -- PULL
        let (download_type, download_path) = databento_client
            .get_historical(
                &Dataset::from_str(&dataset)?,
                &start,
                &end,
                &tickers,
                &schema,
                &SType::from_str(&stype)?,
            )
            .await?
            .ok_or(Error::NoDataError)?;

        // // TODO: Dummy Delete
        // let download_path =
        //     databento_file_path("data", &Dataset::from_str(&dataset)?, &schema, &start, &end)?;
        // let download_type = DatabentoDownloadType::Stream;

        // -- EXTRACT
        let records;
        let dbn_map;
        if download_type == DatabentoDownloadType::Stream {
            (records, dbn_map) = read_dbn_file(download_path).await?;
        } else {
            (records, dbn_map) = read_dbn_batch_dir(download_path).await?;
        }

        // -- TRANSFORM
        // Map DBN instrument to MBN insturment
        let new_map = instrument_id_map(dbn_map, mbn_map.clone())?;
        let mbn_records = to_mbn(records, &new_map)?;
        let num_duplicates = find_duplicates(&mbn_records);
        println!("Duplicates : {:?}", num_duplicates);

        // -- LOAD
        match update_type {
            UpdateTypes::Stream => stream_update(mbn_records, client).await?,
            UpdateTypes::Bulk => {
                let file_name = format!(
                    "bulk_update_{}_{}_{}_{}.bin",
                    dataset,
                    stype,
                    start.date(),
                    end.date()
                );
                bulk_update(mbn_records, &file_name).await?;
            }
        }
    }
    Ok(())
}

pub async fn stream_update(records: Vec<Mbp1Msg>, client: &ApiClient) -> Result<()> {
    for chunk in records.chunks(5000) {
        if let (Some(first_record), Some(last_record)) = (chunk.first(), chunk.last()) {
            println!(
                "Inserting the following records: first ts_event = {:?}, last ts_event = {:?}",
                first_record.hd.ts_event, last_record.hd.ts_event
            );
        } else {
            println!("Chunk is empty or invalid!");
        }

        // Convert the slice to a Vec before passing it to the function
        let chunk_vec = chunk.to_vec();
        let _ = load_to_db(&chunk_vec, client).await?;
    }

    Ok(())
}

pub async fn bulk_update(records: Vec<Mbp1Msg>, file_name: &str) -> Result<()> {
    // Create RecordRef vector.
    let mut refs = Vec::new();
    for msg in &records {
        refs.push(RecordRef::from(msg));
    }

    // Enocde records.
    let mut buffer = Vec::new();
    let mut encoder = RecordEncoder::new(&mut buffer);
    encoder.encode_records(&refs)?;

    // Output to file
    let exec_dir = std::env::current_exe().expect("Unable to get executable directory");
    let exec_dir = exec_dir
        .parent()
        .expect("Unable to find parent directory of executable");

    // Create the `data` directory path and join with the file name
    let bulk_file_path = exec_dir.join("data").join(file_name);

    // let bulk_file_path = format!("data/{}", file_name);
    let _ = mbn::encode::write_to_file(bulk_file_path.to_str().unwrap(), &buffer);
    println!("Data written to file: {:?}", bulk_file_path);

    Ok(())
}
