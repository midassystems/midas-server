use anyhow::Result;
use cli::{self, cli::ProcessCommand};
use dotenv::dotenv;
use midas_client::historical::Historical;
use once_cell::sync::Lazy;
use serial_test::serial;
use std::vec::Vec;

// Set the environment variable for test mode
const MODE: &str = "MODE";
const URL: &str = "http://localhost:8080";
const SYMBOLS: &str = "HE.n.0,ZC.n.0";
const START: &str = "2024-01-02";
const END: &str = "2024-01-04";
const TICKER_FILE: &str = "tests/tickers.json"; // DO NOT CHANGE CONTENTS!!
static TICKERS: Lazy<Vec<String>> = Lazy::new(|| vec!["HE.n.0".to_string(), "ZC.n.0".to_string()]);

// NOTE: If need to test databento pulls uncomment the ignore in order, and clear the tests/data
// files
#[tokio::test]
#[serial]
#[ignore]
async fn test_update() -> Result<()> {
    std::env::set_var(MODE, "1");

    // Parameters
    let client = Historical::new(URL);

    let update_command = cli::commands::databento::DatabentoCommands::Update {
        tickers_filepath: TICKER_FILE.to_string(),
    };

    update_command.process_command(&client).await?;

    Ok(())
}

#[tokio::test]
#[serial]
#[ignore]
async fn test_databento_download() -> Result<()> {
    std::env::set_var(MODE, "1");
    dotenv().ok();

    // Parameters
    let client = Historical::new(URL);

    // Mbp1
    let to_file_command = cli::commands::databento::DatabentoCommands::Download {
        tickers: TICKERS.to_vec(),
        start: START.to_string(),
        end: END.to_string(),
        schema: "mbp-1".to_string(),
        dataset: "GLBX.MDP3".to_string(),
        stype: "continuous".to_string(),
    };

    to_file_command.process_command(&client).await?;

    // Ohlcv
    let to_file_command = cli::commands::databento::DatabentoCommands::Download {
        tickers: TICKERS.to_vec(),
        start: START.to_string(),
        end: END.to_string(),
        schema: "ohlcv-1h".to_string(),
        dataset: "GLBX.MDP3".to_string(),
        stype: "continuous".to_string(),
    };

    to_file_command.process_command(&client).await?;

    // Trades
    let to_file_command = cli::commands::databento::DatabentoCommands::Download {
        tickers: TICKERS.to_vec(),
        start: START.to_string(),
        end: END.to_string(),
        schema: "trades".to_string(),
        dataset: "GLBX.MDP3".to_string(),
        stype: "continuous".to_string(),
    };

    to_file_command.process_command(&client).await?;

    // Tbbo
    let to_file_command = cli::commands::databento::DatabentoCommands::Download {
        tickers: TICKERS.to_vec(),
        start: START.to_string(),
        end: END.to_string(),
        schema: "tbbo".to_string(),
        dataset: "GLBX.MDP3".to_string(),
        stype: "continuous".to_string(),
    };

    to_file_command.process_command(&client).await?;

    // Bbo
    let to_file_command = cli::commands::databento::DatabentoCommands::Download {
        tickers: TICKERS.to_vec(),
        start: START.to_string(),
        end: END.to_string(),
        schema: "bbo-1m".to_string(),
        dataset: "GLBX.MDP3".to_string(),
        stype: "continuous".to_string(),
    };

    to_file_command.process_command(&client).await?;

    Ok(())
}

#[tokio::test]
#[serial]
#[ignore]
async fn test_databento_upload() -> Result<()> {
    std::env::set_var(MODE, "1");

    // Parameters
    let client = Historical::new(URL);

    // Mbp1
    let upload_cmd = cli::commands::databento::DatabentoCommands::Upload {
        dbn_filepath:
            "tests/data/databento/GLBX.MDP3_mbp-1_2024-01-02T00:00:00Z_2024-01-04T00:00:00Z.dbn"
                .to_string(),
        dbn_downloadtype: "stream".to_string(),
        tickers_filepath: TICKER_FILE.to_string(),
        mbn_filepath: "GLBX.MDP3_continuous_2024-01-02_2024-01-04.bin".to_string(),
    };

    upload_cmd.process_command(&client).await?;

    Ok(())
}

#[tokio::test]
#[serial]
#[ignore]
async fn test_get_records() -> Result<()> {
    std::env::set_var(MODE, "1");

    // Mbp-1
    let client = Historical::new(URL);
    let schema = "mbp-1".to_string();
    let file_path = "tests/data/midas/mbp1_test.bin".to_string();

    let historical_command = cli::commands::historical::HistoricalArgs {
        symbols: SYMBOLS.to_string(),
        start: START.to_string(),
        end: END.to_string(),
        schema,
        file_path,
    };

    historical_command.process_command(&client).await?;

    // Ohlcv
    let client = Historical::new(URL);
    let schema = "ohlcv-1h".to_string();
    let file_path = "tests/data/midas/ohlcv1h_test.bin".to_string();

    let historical_command = cli::commands::historical::HistoricalArgs {
        symbols: SYMBOLS.to_string(),
        start: START.to_string(),
        end: END.to_string(),
        schema,
        file_path,
    };

    historical_command.process_command(&client).await?;

    // Trades
    let client = Historical::new(URL);
    let schema = "trade".to_string();
    let file_path = "tests/data/midas/trades_test.bin".to_string();

    let historical_command = cli::commands::historical::HistoricalArgs {
        symbols: SYMBOLS.to_string(),
        start: START.to_string(),
        end: END.to_string(),
        schema,
        file_path,
    };

    historical_command.process_command(&client).await?;

    // Tbbo
    let client = Historical::new(URL);
    let schema = "tbbo".to_string();
    let file_path = "tests/data/midas/tbbo_test.bin".to_string();

    let historical_command = cli::commands::historical::HistoricalArgs {
        symbols: SYMBOLS.to_string(),
        start: START.to_string(),
        end: END.to_string(),
        schema,
        file_path,
    };

    historical_command.process_command(&client).await?;

    // Bbo
    let client = Historical::new(URL);
    let schema = "bbo-1m".to_string();
    let file_path = "tests/data/midas/bbo1m_test.bin".to_string();

    let historical_command = cli::commands::historical::HistoricalArgs {
        symbols: SYMBOLS.to_string(),
        start: START.to_string(),
        end: END.to_string(),
        schema,
        file_path,
    };

    historical_command.process_command(&client).await?;
    Ok(())
}

#[tokio::test]
#[serial]
// #[ignore]
async fn test_compare_files() -> Result<()> {
    std::env::set_var(MODE, "1");

    // // Mbp-1 -- TAKES A WHILE TO RUN
    // let client = Historical::new(URL);
    // let compare_command = cli::commands::databento::DatabentoCommands::Compare {
    //     dbn_filepath:
    //         "tests/data/databento/GLBX.MDP3_mbp-1_2024-01-02T00:00:00Z_2024-01-04T00:00:00Z.dbn"
    //             .to_string(),
    //     mbn_filepath: "tests/data/midas/mbp1_test.bin".to_string(),
    // };
    //
    // compare_command.process_command(&client).await?;

    // Ohlcv
    let client = Historical::new(URL);
    let compare_command = cli::commands::databento::DatabentoCommands::Compare {
        dbn_filepath:
            "tests/data/databento/GLBX.MDP3_ohlcv-1h_2024-01-02T00:00:00Z_2024-01-04T00:00:00Z.dbn"
                .to_string(),
        mbn_filepath: "tests/data/midas/ohlcv1h_test.bin".to_string(),
        mbn_metadata: true,
    };

    compare_command.process_command(&client).await?;

    // Trades
    let client = Historical::new(URL);
    let compare_command = cli::commands::databento::DatabentoCommands::Compare {
        dbn_filepath:
            "tests/data/databento/GLBX.MDP3_trades_2024-01-02T00:00:00Z_2024-01-04T00:00:00Z.dbn"
                .to_string(),
        mbn_filepath: "tests/data/midas/trades_test.bin".to_string(),
        mbn_metadata: true,
    };

    compare_command.process_command(&client).await?;
    //
    // Tbbo
    let client = Historical::new(URL);
    let compare_command = cli::commands::databento::DatabentoCommands::Compare {
        dbn_filepath:
            "tests/data/databento/GLBX.MDP3_tbbo_2024-01-02T00:00:00Z_2024-01-04T00:00:00Z.dbn"
                .to_string(),
        mbn_filepath: "tests/data/midas/tbbo_test.bin".to_string(),
        mbn_metadata: true,
    };

    compare_command.process_command(&client).await?;

    // Bbo
    let client = Historical::new(URL);
    let compare_command = cli::commands::databento::DatabentoCommands::Compare {
        dbn_filepath:
            "tests/data/databento/GLBX.MDP3_bbo-1m_2024-01-02T00:00:00Z_2024-01-04T00:00:00Z.dbn"
                .to_string(),
        mbn_filepath: "tests/data/midas/bbo1m_test.bin".to_string(),
        mbn_metadata: true,
    };

    compare_command.process_command(&client).await?;

    Ok(())
}
