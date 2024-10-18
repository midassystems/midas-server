use crate::commands::databento::DatabentoArgs;
use crate::commands::historical::HistoricalArgs;
use crate::Result;
use async_trait::async_trait;
use clap::{Parser, Subcommand};
use midas_client::historical::Historical;
use std::fmt::Debug;

/// Trait for processing commands
#[async_trait]
pub trait ProcessCommand {
    async fn process_command(&self, client: &Historical) -> Result<()>;
}

#[derive(Debug, Parser)]
pub struct CliArgs {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Debug, Subcommand)]
pub enum Commands {
    /// Download historical price data.
    Historical(HistoricalArgs),
    /// Commands for Databento source
    Databento(DatabentoArgs),
    /// Load data filesuse historical::HistoricalArgs;
    FileLoad {
        #[arg(long)]
        file_path: String,
    },
}

#[async_trait]
impl ProcessCommand for Commands {
    async fn process_command(&self, client: &Historical) -> Result<()> {
        match self {
            Commands::FileLoad { file_path } => {
                load_file(file_path, client).await?;
                Ok(())
            }
            Commands::Databento(args) => {
                // Delegate Databento subcommands
                args.subcommand.process_command(client).await
            }
            Commands::Historical(args) => {
                args.process_command(client).await?;
                Ok(())
            }
        }
    }
}

pub async fn load_file(file_name: &str, client: &Historical) -> Result<()> {
    let response = client.create_mbp_from_file(file_name).await?;
    println!("{:?}", response);

    Ok(())
}
