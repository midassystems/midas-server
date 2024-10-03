use crate::error::Result;
use crate::utils::user_input;
use databento::{
    dbn::{self, Dataset, SType, Schema},
    historical::batch::{DownloadParams, JobState, ListJobsParams, SubmitJobParams},
    historical::metadata::GetBillableSizeParams,
    historical::metadata::GetCostParams,
    historical::timeseries::{GetRangeParams, GetRangeToFileParams},
    HistoricalClient,
};
use std::path::PathBuf;
use std::time::Duration;
use time::OffsetDateTime;
use tokio::io::AsyncReadExt;

/// Create the file path for the raw download from databento.
pub fn databento_file_path(
    dir_path: &str,
    dataset: &Dataset,
    schema: &Schema,
    start: &OffsetDateTime,
    end: &OffsetDateTime,
) -> Result<PathBuf> {
    let file_path = PathBuf::from(format!(
        "{}/{}_{}_{}_{}.dbn",
        dir_path,
        dataset.as_str(),
        schema.as_str(),
        start.format(&time::format_description::well_known::Rfc3339)?,
        end.format(&time::format_description::well_known::Rfc3339)?
    ));
    Ok(file_path)
}

#[derive(PartialEq, Eq)]
pub enum DatabentoDownloadType {
    Stream,
    Batch,
}

#[derive(Debug)]
pub struct DatabentoClient {
    hist_client: HistoricalClient,
}

impl DatabentoClient {
    pub fn new(api_key: String) -> Result<Self> {
        let hist_client = HistoricalClient::builder().key(api_key)?.build()?;

        Ok(Self { hist_client })
    }

    /// Gets the billable uncompressed raw binary size for historical streaming or batched files.
    async fn check_size(
        &mut self,
        dataset: &Dataset,
        start: &OffsetDateTime,
        end: &OffsetDateTime,
        symbols: &Vec<String>,
        schema: &Schema,
        stype: &SType,
    ) -> Result<u64> {
        let params = GetBillableSizeParams::builder()
            .dataset(*dataset)
            .date_time_range((start.clone(), end.clone()))
            .symbols(symbols.clone())
            .schema(*schema)
            .stype_in(*stype)
            .build();

        let size = self
            .hist_client
            .metadata()
            .get_billable_size(&params)
            .await?;

        let size_gb = size / 1_000_000_000;

        Ok(size_gb)
    }

    /// Gets the cost in US dollars for a historical streaming or batch download request.
    pub async fn check_cost(
        &mut self,
        dataset: &Dataset,
        start: &OffsetDateTime,
        end: &OffsetDateTime,
        symbols: &Vec<String>,
        schema: &Schema,
        stype: &SType,
    ) -> Result<f64> {
        let params = GetCostParams::builder()
            .dataset(*dataset)
            .date_time_range((start.clone(), end.clone()))
            .symbols(symbols.clone())
            .schema(*schema)
            .stype_in(*stype)
            .build();

        let cost = self.hist_client.metadata().get_cost(&params).await?;

        Ok(cost)
    }

    /// Makes a streaming request for timeseries data from Databento and saves to file.
    pub async fn fetch_historical_stream_to_file(
        &mut self,
        dataset: &Dataset,
        start: &OffsetDateTime,
        end: &OffsetDateTime,
        symbols: &Vec<String>,
        schema: &Schema,
        stype: &SType,
        filepath: &PathBuf,
    ) -> Result<dbn::decode::AsyncDbnDecoder<impl AsyncReadExt>> {
        // Define the parameters for the timeseries data request
        let params = GetRangeToFileParams::builder()
            .dataset(*dataset)
            .date_time_range((start.clone(), end.clone()))
            .symbols(symbols.clone())
            .schema(*schema)
            .stype_in(*stype)
            .path(filepath)
            .build();

        let decoder = self
            .hist_client
            .timeseries()
            .get_range_to_file(&params)
            .await?;

        println!("Saved to file.");

        Ok(decoder)
    }

    #[allow(dead_code)]
    pub async fn fetch_historical_stream(
        &mut self,
        dataset: &Dataset,
        start: &OffsetDateTime,
        end: &OffsetDateTime,
        symbols: &Vec<String>,
        schema: &Schema,
        stype: &SType,
    ) -> Result<dbn::decode::AsyncDbnDecoder<impl AsyncReadExt>> {
        // Define the parameters for the timeseries data request
        let params = GetRangeParams::builder()
            .dataset(*dataset)
            .date_time_range((start.clone(), end.clone()))
            .symbols(symbols.clone())
            .schema(*schema)
            .stype_in(*stype)
            .build();

        let decoder = self.hist_client.timeseries().get_range(&params).await?;

        Ok(decoder)
    }

    /// Makes a batch request for timeseries data from Databento and saves to file.
    pub async fn fetch_historical_batch_to_file(
        &mut self,
        dataset: &Dataset,
        start: &OffsetDateTime,
        end: &OffsetDateTime,
        symbols: &Vec<String>,
        schema: &Schema,
        stype: &SType,
        filepath: &PathBuf,
    ) -> Result<()> {
        // Define the parameters for the timeseries data request
        let params = SubmitJobParams::builder()
            .dataset(*dataset)
            .date_time_range((start.clone(), end.clone()))
            .symbols(symbols.clone())
            .schema(*schema)
            .stype_in(*stype)
            .build();

        let job = self.hist_client.batch().submit_job(&params).await?;

        let now = OffsetDateTime::now_utc();
        let list_jobs_query = ListJobsParams::builder()
            .since(now - Duration::from_secs(60))
            .states(vec![JobState::Done])
            .build();
        // Now we wait for the job to complete
        loop {
            let finished_jobs = self.hist_client.batch().list_jobs(&list_jobs_query).await?;
            if finished_jobs.iter().any(|j| j.id == job.id) {
                break;
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }

        // Once complete, we download the files
        let files = self
            .hist_client
            .batch()
            .download(
                &DownloadParams::builder()
                    .output_dir(filepath)
                    .job_id(job.id)
                    .build(),
            )
            .await?;
        println!("{:?}", files);

        Ok(())
    }

    pub async fn get_historical(
        &mut self,
        dataset: &Dataset,
        start: &OffsetDateTime,
        end: &OffsetDateTime,
        symbols: &Vec<String>,
        schema: &Schema,
        stype: &SType,
    ) -> Result<Option<(DatabentoDownloadType, PathBuf)>> {
        // Cost check
        let cost = self
            .check_cost(&dataset, &start, &end, &symbols, &schema, &stype)
            .await?;

        // Check with user before proceeding
        println!("The estimated cost for this operation is: $ {}", cost);
        let proceed = user_input()?;
        if proceed == false {
            return Ok(None);
        }
        println!("Operation is continuing...");

        // Size check
        let size = self
            .check_size(&dataset, &start, &end, &symbols, &schema, &stype)
            .await?;

        let download_type;
        let file_path;

        // Get the directory of the executable
        let exec_dir = std::env::current_exe().expect("Unable to get executable directory");
        let exec_dir = exec_dir
            .parent()
            .expect("Unable to find parent directory of executable");

        // Dynamic load based on size
        if size < 5 {
            println!("Download size is {}GB : Stream Downloading.", size);
            download_type = DatabentoDownloadType::Stream;
            let file_name = databento_file_path("data", &dataset, &schema, &start, &end)?;
            // Join the exec_dir with the file_path
            file_path = exec_dir.join(file_name);
            println!("{:?}", file_path);

            let _ = self
                .fetch_historical_stream_to_file(
                    &dataset, &start, &end, &symbols, &schema, &stype, &file_path,
                )
                .await?;
        } else {
            println!("Download size is {}GB : Batch Downloading", size);
            download_type = DatabentoDownloadType::Batch;
            let file_name = databento_file_path("data/batch", &dataset, &schema, &start, &end)?;
            file_path = exec_dir.join(&file_name);
            println!("{:?}", file_path);

            let _ = self
                .fetch_historical_batch_to_file(
                    &dataset, &start, &end, &symbols, &schema, &stype, &file_path,
                )
                .await?;
        }
        // let path_string = file_path.to_string_lossy().into_owned();

        Ok(Some((download_type, file_path)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    // use crate::data::pull::databento_file_path;
    use databento::dbn::{FlagSet, Mbp1Msg};
    use dotenv::dotenv;
    use std::collections::HashMap;
    use std::env;

    use std::fs;
    use time::OffsetDateTime;

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
            env::var("DATABENTO_API_KEY").expect("Expected API key in environment variables.");

        // Parameters
        let dataset = Dataset::GlbxMdp3;
        let start = time::macros::datetime!(2024-08-20 00:00 UTC);
        let end = time::macros::datetime!(2024-08-20 05:00 UTC);
        let symbols = vec!["HE.n.0".to_string(), "ZC.n.0".to_string()];
        let schema = Schema::Mbp1;
        let stype = SType::Continuous;

        let client = DatabentoClient::new(api_key).expect("Failed to create DatabentoClient");
        (client, dataset, start, end, symbols, schema, stype)
    }

    #[tokio::test]
    #[ignore]
    async fn test_check_size() {
        let (mut client, dataset, start, end, symbols, schema, stype) = setup();
        let size = client
            .check_size(&dataset, &start, &end, &symbols, &schema, &stype)
            .await
            .expect("Error calculating size");

        assert!(size > 0);
    }

    #[tokio::test]
    #[ignore]
    async fn test_check_cost() {
        let (mut client, dataset, start, end, symbols, schema, stype) = setup();
        let cost = client
            .check_cost(&dataset, &start, &end, &symbols, &schema, &stype)
            .await
            .expect("Error calculating cost");

        assert!(cost > 0.0);
    }

    #[tokio::test]
    #[ignore]
    async fn test_stream_to_file() {
        let (mut client, dataset, start, end, symbols, schema, stype) = setup();

        let file_path =
            databento_file_path("tests/data/databento", &dataset, &schema, &start, &end)
                .expect("Error creatign fiel_path");
        let _ = client
            .fetch_historical_stream_to_file(
                &dataset, &start, &end, &symbols, &schema, &stype, &file_path,
            )
            .await
            .expect("Error with stream.");

        assert!(fs::metadata(&file_path).is_ok(), "File does not exist");
    }

    #[tokio::test]
    #[ignore]
    async fn test_batch_to_file() {
        let (mut client, dataset, start, end, symbols, schema, stype) = setup();

        let file_path = databento_file_path(
            "tests/data/databento/batch",
            &dataset,
            &schema,
            &start,
            &end,
        )
        .expect("Error creatign fiel_path");

        let _ = client
            .fetch_historical_batch_to_file(
                &dataset, &start, &end, &symbols, &schema, &stype, &file_path,
            )
            .await
            .expect("Error with stream.");

        assert!(fs::metadata(&file_path).is_ok(), "File does not exist");
    }

    fn find_duplicates(mbps: Vec<databento::dbn::Mbp1Msg>) -> Vec<databento::dbn::Mbp1Msg> {
        let mut occurrences = HashMap::new();
        let mut duplicates = Vec::new();

        for msg in &mbps {
            // Only consider messages with non-zero flags as potential duplicates
            if msg.flags != FlagSet::empty() {
                let key = (
                    msg.hd.instrument_id,
                    msg.hd.ts_event,
                    msg.sequence,
                    msg.flags, // Include flags in the key
                );
                let count = occurrences.entry(key).or_insert(0);
                *count += 1;
            }
        }

        for msg in mbps {
            // Again, consider only messages with non-zero flags
            if msg.flags != FlagSet::empty() {
                let key = (
                    msg.hd.instrument_id,
                    msg.hd.ts_event,
                    msg.sequence,
                    msg.flags, // Include flags in the key
                );
                if let Some(&count) = occurrences.get(&key) {
                    if count > 1 {
                        duplicates.push(msg);
                    }
                }
            }
        }

        duplicates
    }

    #[tokio::test]
    // #[ignore]
    async fn test_stream() {
        let (mut client, dataset, start, end, symbols, schema, stype) = setup();

        let mut decoder = client
            .fetch_historical_stream(&dataset, &start, &end, &symbols, &schema, &stype)
            .await
            .expect("Error with stream.");

        // let records = decoder
        // .decode_record::<Mbp1Msg>()
        // .await
        // .expect("tetst")
        // .unwrap();
        let mut records = Vec::new();
        while let Some(record) = decoder
            .decode_record::<Mbp1Msg>()
            .await
            .expect("errro decoding")
        {
            records.push(record.clone());
        }
        // println!("{:?}", records);

        let duplicates = find_duplicates(records);
        println!("{:?}", duplicates);

        // println!("record {:#?}", records);
    }
}
