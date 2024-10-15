use clap::Parser;
use cli::{
    cli::{CliArgs, ProcessCommand},
    Result,
};
use midas_client::client::ApiClient;
use std::env;

#[tokio::main]
async fn main() -> Result<()> {
    // Parse CLI arguments
    let args = CliArgs::parse();

    // Initialize the MidasClient, which holds the ApiClient
    let midas_url = env::var("MIDAS_URL").expect("MIDAS_URL not set");
    let midas_client = ApiClient::new(&midas_url);

    // Process the command and pass the ApiClient to it
    args.command.process_command(&midas_client).await?;

    Ok(())
}
