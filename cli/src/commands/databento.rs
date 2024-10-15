use crate::cli::ProcessCommand;
use crate::error::{Error, Result};
use async_trait::async_trait;
use clap::{Args, Subcommand};
use databento::dbn::Schema;
use midas_client::client::ApiClient;
use std::path::PathBuf;
use std::str::FromStr;
use time::Duration;
use time::{format_description::well_known::Rfc3339, macros::time, OffsetDateTime};

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
        file_path: String,
    },
    /// Update tickers already in the database on a custom timeframe.
    BulkUpdate {
        #[arg(long, default_value = "config/update_tickers.json")]
        file_path: String,
        #[arg(long)]
        start: String,
        #[arg(long)]
        end: Option<String>,
    },
    /// Adds tickers and backfills histroical data to desired date.
    Add {
        #[arg(long, default_value = "config/add_tickers.json")]
        file_path: String,
        #[arg(long)]
        start: String,
        #[arg(long)]
        end: Option<String>,
    },
    /// Compare files
    Compare {
        #[arg(long)]
        dbn_filepath: String,
        #[arg(long)]
        mbn_filepath: String,
    },
    // To  file(mainly for testing for schemas not stored)
    ToFile {
        /// Start date in YYYY-MM-DD HH:MM:SS format.
        #[arg(long)]
        start: String,

        /// End date in YYYY-MM-DD HH:MM:SS format.
        #[arg(long)]
        end: String,

        /// Schema ex. Mbp1, Ohlcv
        #[arg(long)]
        schema: String,

        /// File path to save the downloaded binary data.
        #[arg(long)]
        file_path: String,
    },
}

#[async_trait]
impl ProcessCommand for DatabentoCommands {
    async fn process_command(&self, client: &ApiClient) -> Result<()> {
        // Ensure the client is available
        match self {
            DatabentoCommands::Update { file_path } => {
                let now = OffsetDateTime::now_utc();
                let start = (now - Duration::days(1)).replace_time(time::macros::time!(00:00));
                let end = now.replace_time(time::macros::time!(00:00));

                println!("\nUpdating Database: {:?}", now);
                println!("start {:?}, end {:?}", start, end);

                // Update
                let _ = vendors::databento::update::update(
                    Schema::Mbp1,
                    start,
                    end,
                    client,
                    file_path,
                    None,
                    None,
                )
                .await?;

                Ok(())
            }
            DatabentoCommands::BulkUpdate {
                file_path,
                start,
                end,
            } => {
                let start_date = process_start_date(start)?;
                let end_date = processs_end_date(end.clone())?;

                println!("{}, {}", start_date, end_date);
                // Update
                let _ = vendors::databento::update::update(
                    Schema::Mbp1,
                    start_date,
                    end_date,
                    client,
                    file_path,
                    None,
                    None,
                )
                .await?;

                Ok(())
            }
            DatabentoCommands::Add {
                file_path,
                start,
                end,
            } => {
                let start_date = process_start_date(start)?;
                let end_date = processs_end_date(end.clone())?;

                println!("{}, {}", start_date, end_date);

                // Update
                let _ = vendors::databento::update::update(
                    Schema::Mbp1,
                    start_date,
                    end_date,
                    client,
                    file_path,
                    None,
                    None,
                )
                .await?;

                Ok(())
            }
            DatabentoCommands::ToFile {
                file_path,
                start,
                end,
                schema,
            } => {
                let start_date = process_start_date(start)?;
                let end_date = processs_end_date(Some(end.clone()))?;
                let schema_enum = Schema::from_str(schema.as_str())
                    .expect(format!("Invalid schema : {}", schema.as_str()).as_str());

                println!("{}, {}", start_date, end_date);
                // Update
                let _ = vendors::databento::update::update(
                    schema_enum,
                    start_date,
                    end_date,
                    client,
                    file_path,
                    Some(false),
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
                    true,
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
