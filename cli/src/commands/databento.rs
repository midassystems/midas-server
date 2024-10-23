use crate::cli::ProcessCommand;
use crate::error::{Error, Result};
use async_trait::async_trait;
use clap::{Args, Subcommand};
use databento::dbn::Schema;
use midas_client::historical::Historical;
use std::path::PathBuf;
use std::str::FromStr;
use time::{format_description::well_known::Rfc3339, macros::time, OffsetDateTime};
use vendors::databento::client::DatabentoDownloadType;

fn process_start_date(start: &String) -> Result<OffsetDateTime> {
    // Append T00:00 to make it full day at 00:00 UTC
    let start_datetime = format!("{}T00:00:00Z", start);

    // Try to parse the datetime string as an OffsetDateTime
    let start_date = OffsetDateTime::parse(&start_datetime, &Rfc3339).map_err(|_| {
        Error::DateError(
            "Error: Invalid start date format. Expected format: YYYY-MM-DD".to_string(),
        )
    })?;

    Ok(start_date)
}

fn processs_end_date(end: Option<String>) -> Result<OffsetDateTime> {
    let end_date = end
        .as_ref()
        .map(|s| s.clone()) // Clone the string if it exists
        .unwrap_or_else(|| {
            let now = OffsetDateTime::now_utc();
            let end_of_today = now.replace_time(time!(00:00));
            end_of_today.date().to_string() // Return the date part only
        });

    // Append T00:00 to make it full day at 00:00 UTC
    let end_datetime = format!("{}T00:00:00Z", end_date);
    let end_date = OffsetDateTime::parse(&end_datetime, &Rfc3339).map_err(|_| {
        Error::DateError(
            "Error: Invalid start date format. Expected format: YYYY-MM-DD".to_string(),
        )
    })?;
    Ok(end_date)
}

#[derive(Debug, Args)]
pub struct DatabentoArgs {
    #[command(subcommand)]
    pub subcommand: DatabentoCommands,
}

#[derive(Debug, Subcommand)]
pub enum DatabentoCommands {
    /// Standard update, adds mbp for tickers already in the database for entire previous day.
    Update {
        #[arg(long, default_value = "config/tickers.json")]
        tickers_filepath: String,
    },
    /// Download databento data to file
    Download {
        /// --tickers AAPL GOOGL TSLA
        #[arg(num_args(0..))]
        tickers: Vec<String>,

        /// Start date in YYYY-MM-DD HH:MM:SS format.
        #[arg(long)]
        start: String,

        /// End date in YYYY-MM-DD HH:MM:SS format.
        #[arg(long)]
        end: String,

        /// Schema ex. Mbp1, Ohlcv
        #[arg(long)]
        schema: String,

        /// Start date in YYYY-MM-DD HH:MM:SS format.
        #[arg(long)]
        dataset: String,

        /// End date in YYYY-MM-DD HH:MM:SS format.
        #[arg(long)]
        stype: String,
    },
    /// Upload a databento file to database
    Upload {
        /// Schema ex. Mbp1, Ohlcv
        #[arg(long)]
        dbn_filepath: String,

        /// End date in YYYY-MM-DD HH:MM:SS format.
        #[arg(long)]
        dbn_downloadtype: String,

        /// Schema ex. Mbp1, Ohlcv
        #[arg(long, default_value = "config/tickers.json")]
        tickers_filepath: String,

        /// File path to save the downloaded binary data.
        #[arg(long)]
        mbn_filepath: String,
    },
    /// Compare databento and midas data
    Compare {
        #[arg(long)]
        dbn_filepath: String,
        #[arg(long)]
        mbn_filepath: String,
    },
}

#[async_trait]
impl ProcessCommand for DatabentoCommands {
    async fn process_command(&self, client: &Historical) -> Result<()> {
        match self {
            DatabentoCommands::Update { tickers_filepath } => {
                // Update
                let _ = vendors::databento::update(tickers_filepath, client).await?;

                Ok(())
            }
            DatabentoCommands::Download {
                tickers,
                start,
                end,
                schema,
                dataset,
                stype,
            } => {
                let start_date = process_start_date(start)?;
                let end_date = processs_end_date(Some(end.clone()))?;
                let schema_enum = Schema::from_str(schema.as_str())
                    .expect(format!("Invalid schema : {}", schema.as_str()).as_str());

                println!("{}, {}", start_date, end_date);
                // Download
                let _ = vendors::databento::download(
                    tickers,
                    schema_enum,
                    start_date,
                    end_date,
                    dataset,
                    stype,
                )
                .await?;

                Ok(())
            }
            DatabentoCommands::Upload {
                dbn_filepath,
                dbn_downloadtype,
                tickers_filepath,
                mbn_filepath,
            } => {
                let dbn_filepath = PathBuf::from(dbn_filepath);
                let mbn_filepath = PathBuf::from(mbn_filepath);
                let download_type = DatabentoDownloadType::try_from(dbn_downloadtype.as_str())?;

                // Update
                let _ = vendors::databento::upload(
                    &dbn_filepath,
                    &download_type,
                    tickers_filepath,
                    &mbn_filepath,
                    &client,
                    None,
                )
                .await?;

                Ok(())
            }
            DatabentoCommands::Compare {
                dbn_filepath,
                mbn_filepath,
            } => {
                let _ = vendors::databento::compare::compare_dbn(
                    PathBuf::from(dbn_filepath),
                    &PathBuf::from(mbn_filepath),
                )
                .await?;

                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use time::macros::datetime;

    #[test]
    fn test_process_start_date() -> Result<()> {
        let start_str = "2024-01-01".to_string();

        // Test
        let start = process_start_date(&start_str)?;

        // Validate
        assert_eq!(start, datetime!(2024-01-01 0:00:00.0 +00:00:00));

        Ok(())
    }

    #[test]
    fn test_process_end_date() -> Result<()> {
        let end_str = Some("2024-01-01".to_string());

        // Test
        let end = processs_end_date(end_str)?;

        // Validate
        assert_eq!(end, datetime!(2024-01-01 0:00:00.0 +00:00:00));

        Ok(())
    }

    #[test]
    fn test_process_end_date_auto() -> Result<()> {
        let end_str = None;

        // Test
        let end = processs_end_date(end_str)?;

        // Validate
        let now = OffsetDateTime::now_utc();
        let expt_end = now.replace_time(time::macros::time!(00:00));

        assert_eq!(end, expt_end);

        Ok(())
    }
}
