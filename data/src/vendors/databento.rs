pub mod client;
pub mod dbn_compare; // comparing already sotred data pulle  otfiel with raw dbn
pub mod extract;
pub mod load;
pub mod transform;
pub mod update;

use crate::error::{Error, Result};
use crate::vendors::databento::update::{update, UpdateTypes};
use config::settings::Config;
use databento::dbn::Schema;
use dbn_compare::compare_dbn;
use load::load_file_to_db;
use midas_client::client::ApiClient;
use std::path::PathBuf;
use time;
use time::{Duration, OffsetDateTime};

pub async fn update_yesterday(config: Config, client: &ApiClient) -> Result<()> {
    let now = OffsetDateTime::now_utc();
    let start = (now - Duration::days(1)).replace_time(time::macros::time!(00:00));
    let end = now.replace_time(time::macros::time!(00:00));
    println!("\nUpdating Database: {:?}", now);

    run_stream(start, end, config, client).await?;

    Ok(())
}

pub async fn run_stream(
    start: OffsetDateTime,
    end: OffsetDateTime,
    config: Config,
    client: &ApiClient,
) -> Result<()> {
    let schema = Schema::Mbp1;
    let result = update(schema, start, end, &UpdateTypes::Stream, config, client).await;

    // Handle error propagation here
    if let Err(Error::NoDataError) = result {
        println!("No data retrieved.");
        return Ok(());
    }

    Ok(())
}

pub async fn run_bulk(
    start: OffsetDateTime,
    end: OffsetDateTime,
    config: Config,
    client: &ApiClient,
) -> Result<()> {
    let schema = Schema::Mbp1;
    let result = update(schema, start, end, &UpdateTypes::Bulk, config, client).await;

    // Handle error propagation here
    if let Err(Error::NoDataError) = result {
        println!("No data retrieved.");
        return Ok(());
    }

    Ok(())
}

pub async fn load_bulk(file_path: &str, client: &ApiClient) -> Result<()> {
    load_file_to_db(file_path, client).await?;
    Ok(())
}

pub async fn compare_files(dbn_filepath: &str, mbn_filepath: &str) -> Result<()> {
    let _ = compare_dbn(PathBuf::from(dbn_filepath), mbn_filepath).await?;
    Ok(())
}
