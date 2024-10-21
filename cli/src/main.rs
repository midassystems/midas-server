use clap::Parser;
use cli::{
    cli::{CliArgs, ProcessCommand},
    Result,
};
use midas_client::historical::Historical;

#[tokio::main]
async fn main() -> Result<()> {
    // Parse CLI arguments
    let args = CliArgs::parse();

    // Initialize the MidasClient, which holds the ApiClient
    let historic_url = std::env::var("HISTORICAL_URL").expect("HISTORICAL_URL not set");
    let historical_client = Historical::new(&historic_url);

    // Process the command and pass the ApiClient to it
    args.command.process_command(&historical_client).await?;

    Ok(())
}
