use async_compression::tokio::bufread::ZstdDecoder;
use databento::dbn::Record as dbnRecord;
use databento::{dbn, historical::timeseries::AsyncDbnDecoder};
use mbn::decode::AsyncDecoder;
use mbn::record_enum::RecordEnum;
use mbn::records::Record;
use mbn::{self};
use std::collections::HashMap;
use std::path::PathBuf;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use tokio::io::BufReader;

pub fn symbol_map(metadata: &dbn::Metadata) -> anyhow::Result<HashMap<String, String>> {
    let mut symbol_map_hash = HashMap::new();

    for mapping in &metadata.mappings {
        for interval in &mapping.intervals {
            symbol_map_hash.insert(interval.symbol.clone(), mapping.raw_symbol.to_string());
        }
    }
    Ok(symbol_map_hash)
}

pub async fn read_dbn_file(
    filepath: PathBuf,
) -> anyhow::Result<(
    AsyncDbnDecoder<ZstdDecoder<BufReader<File>>>,
    HashMap<String, String>,
)> {
    // Read the file
    let decoder = AsyncDbnDecoder::from_zstd_file(filepath)
        .await
        .map_err(|_| anyhow::anyhow!("Error opeing dbn file."))?;

    // Extract Symbol Map
    let metadata = decoder.metadata();
    let map = symbol_map(&metadata)?;

    Ok((decoder, map))
}

pub async fn read_mbn_file(filepath: &PathBuf) -> anyhow::Result<AsyncDecoder<BufReader<File>>> {
    let decoder = AsyncDecoder::<BufReader<File>>::from_file(filepath).await?;

    Ok(decoder)
}

pub async fn compare_dbn_raw_output(
    dbn_filepath: PathBuf,
    mbn_filepath: &PathBuf,
) -> anyhow::Result<()> {
    let mut mbn_decoder = read_mbn_file(mbn_filepath).await?;
    let (mut dbn_decoder, _map) = read_dbn_file(dbn_filepath).await?;

    // Output files
    let mbn_output_file = "raw_mbn_records.txt";
    let dbn_output_file = "raw_dbn_records.txt";

    // Create or truncate output files
    let mut mbn_file = File::create(mbn_output_file).await?;
    let mut dbn_file = File::create(dbn_output_file).await?;

    let mut mbn_count = 0;
    // Write MBN records to file
    while let Some(mbn_record) = mbn_decoder.decode_ref().await? {
        mbn_count += 1;
        let record_enum = RecordEnum::from_ref(mbn_record)?;
        mbn_file
            .write_all(format!("{:?}\n", record_enum).as_bytes())
            .await?;
    }

    let mut dbn_count = 0;
    // Write DBN records to file
    while let Some(dbn_record) = dbn_decoder.decode_record_ref().await? {
        dbn_count += 1;
        let dbn_record_enum = dbn_record.as_enum()?.to_owned();
        dbn_file
            .write_all(format!("{:?}\n", dbn_record_enum).as_bytes())
            .await?;
    }
    println!("MBN length: {:?}", mbn_count);
    println!("DBN length: {:?}", dbn_count);

    Ok(())
}

pub async fn compare_dbn(dbn_filepath: PathBuf, mbn_filepath: &PathBuf) -> anyhow::Result<()> {
    let batch_size = 1000; // New parameter to control batch size
    let mut mbn_decoder = read_mbn_file(mbn_filepath).await?;
    let (mut dbn_decoder, _map) = read_dbn_file(dbn_filepath).await?;

    let mut mbn_batch: HashMap<u64, Vec<RecordEnum>> = HashMap::new();
    let mut mbn_decoder_done = false;

    // Keep track of any unmatched DBN records
    let mut unmatched_dbn_records = Vec::new();

    // Start decoding and comparing
    while let Some(dbn_record) = dbn_decoder.decode_record_ref().await? {
        // If MBN batch is empty, refill it
        if mbn_batch.len() < batch_size && !mbn_decoder_done {
            while let Some(mbn_record) = mbn_decoder.decode_ref().await? {
                let record_enum = RecordEnum::from_ref(mbn_record)?;
                let ts_event = record_enum.header().ts_event;
                mbn_batch.entry(ts_event).or_default().push(record_enum);
            }
            if mbn_batch.is_empty() {
                mbn_decoder_done = true; // No more MBN records
            }
        }
        let dbn_record_enum = dbn_record.as_enum()?.to_owned();
        let ts_event = dbn_record_enum.header().ts_event; // Extract ts_event from DBN record

        // Check if the ts_event exists in the MBN map
        if let Some(mbn_group) = mbn_batch.get_mut(&ts_event) {
            // Try to find a match within the group
            if let Some(pos) = mbn_group
                .iter()
                .position(|mbn_record| mbn_record == &dbn_record_enum)
            {
                mbn_group.remove(pos); // Remove matched record
                if mbn_group.is_empty() {
                    mbn_batch.remove(&ts_event); // Remove the key if the group is empty
                }
            } else {
                unmatched_dbn_records.push(dbn_record_enum); // No match found in the group
            }
        } else {
            unmatched_dbn_records.push(dbn_record_enum); // No group found for the ts_event
        }
    }

    // Create or truncate the output file
    let output_file = "compare_results.txt";
    let mut file = File::create(&output_file).await?;

    // Check for remaining unmatched MBN records and write them to the file
    if !mbn_batch.is_empty() {
        file.write_all(b"Unmatched MBN Records:\n").await?;
        for mbn_record in &mbn_batch {
            file.write_all(format!("{:?}\n", mbn_record).as_bytes())
                .await?;
        }
    }

    // Check for remaining unmatched DBN records and write them to the file
    if !unmatched_dbn_records.is_empty() {
        file.write_all(b"Unmatched DBN Records:\n").await?;
        for dbn_record in &unmatched_dbn_records {
            file.write_all(format!("{:?}\n", dbn_record).as_bytes())
                .await?;
        }
    }

    // Return an error if there are unmatched records in either batch
    if mbn_batch.is_empty() && unmatched_dbn_records.is_empty() {
        println!("All records match successfully.");
    } else {
        eprintln!(
            "Unmatched records detected. Check the output file: {:?}",
            output_file
        );
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use dotenv::dotenv;
    use mbn::enums::{Dataset, Schema};
    use mbn::params::RetrieveParams;
    use mbn::symbols::Instrument;
    use mbn::vendors::{DatabentoData, VendorData, Vendors};
    use midas_client::historical::Historical;
    use midas_client::instrument::Instruments;
    use repl_shell::{self, cli::ProcessCommand};
    use serial_test::serial;
    use std::str::FromStr;

    async fn create_tickers() -> anyhow::Result<()> {
        dotenv().ok();
        let base_url = std::env::var("INSTRUMENT_URL").expect("Expected database_url.");
        let client = Instruments::new(&base_url);

        let schema = dbn::Schema::from_str("mbp-1")?;
        let dbn_dataset = dbn::Dataset::from_str("GLBX.MDP3")?;
        let stype = dbn::SType::from_str("raw_symbol")?;
        let vendor_data = VendorData::Databento(DatabentoData {
            schema,
            dataset: dbn_dataset,
            stype,
        });
        let vendor = Vendors::Databento;
        let dataset = Dataset::Futures;
        let mut instruments = Vec::new();

        // LEG4
        instruments.push(Instrument::new(
            None,
            "LEG4",
            "LiveCattle-0224",
            dataset,
            vendor,
            vendor_data.encode(),
            1709229600000000000,
            1704067200000000000,
            1709229600000000000,
            true,
        ));

        // HEG4
        instruments.push(Instrument::new(
            None,
            "HEG4",
            "LeanHogs-0224",
            dataset,
            vendor,
            vendor_data.encode(),
            1707933600000000000,
            1704067200000000000,
            1707933600000000000,
            true,
        ));

        // HEJ4
        instruments.push(Instrument::new(
            None,
            "HEJ4",
            "LeanHogs-0424",
            dataset,
            vendor,
            vendor_data.encode(),
            1712941200000000000,
            1704067200000000000,
            1712941200000000000,
            true,
        ));

        // LEJ4
        instruments.push(Instrument::new(
            None,
            "LEJ4",
            "LiveCattle-0424",
            dataset,
            vendor,
            vendor_data.encode(),
            1714496400000000000,
            1704067200000000000,
            1714496400000000000,
            true,
        ));

        // HEK4
        instruments.push(Instrument::new(
            None,
            "HEK4",
            "LeanHogs-0524",
            dataset,
            vendor,
            vendor_data.encode(),
            1715706000000000000,
            1704067200000000000,
            1715706000000000000,
            true,
        ));

        // HEM4
        instruments.push(Instrument::new(
            None,
            "HEM4",
            "LeanHogs-0624",
            dataset,
            vendor,
            vendor_data.encode(),
            1718384400000000000,
            1704067200000000000,
            1718384400000000000,
            true,
        ));

        // LEM4
        instruments.push(Instrument::new(
            None,
            "LEM4",
            "LiveCattle-0624",
            dataset,
            vendor,
            vendor_data.encode(),
            1719594000000000000,
            1704067200000000000,
            1719594000000000000,
            true,
        ));

        for i in &instruments {
            let create_response = client.create_symbol(i).await?;
            let id = create_response.data as i32;
            println!("{:?} : {}", i.ticker, id);
        }

        Ok(())
    }

    /// Deletes the tickers created during setup
    async fn teardown_tickers() -> anyhow::Result<()> {
        dotenv().ok();
        let base_url = std::env::var("INSTRUMENT_URL").expect("Expected INSTRUMENT_URL.");
        let client = Instruments::new(&base_url);

        let tickers_to_delete = vec![
            "LEG4".to_string(),
            "HEG4".to_string(),
            "HEJ4".to_string(),
            "LEJ4".to_string(),
            "HEK4".to_string(),
            "HEM4".to_string(),
            "LEM4".to_string(),
        ];

        for ticker in tickers_to_delete {
            let response = client.get_symbol(&ticker, &Dataset::Futures).await?;
            let id = response.data[0].instrument_id.unwrap() as i32;
            client.delete_symbol(&id).await?;
            println!("Deleted ticker: {}", ticker);
        }

        Ok(())
    }

    // A function to seed the database (runs once before tests)
    async fn global_setup() {
        dotenv().ok();

        create_tickers().await.expect("Error creating tickers");

        // Parameters
        let dataset = Dataset::Futures;
        let context = repl_shell::context::Context::init().expect("Error on context creation.");

        // Mbp1
        let upload_cmd = repl_shell::cli::vendors::databento::DatabentoCommands::Upload {
                dataset: dataset.as_str().to_string(),
                dbn_filepath:
                "GLBX.MDP3_mbp-1_HEG4_HEJ4_LEG4_LEJ4_LEM4_HEM4_HEK4_2024-02-09T00:00:00Z_2024-02-17T00:00:00Z.dbn".to_string(),
                dbn_downloadtype: "stream".to_string(),
                mbn_filepath: "system_tests_data.bin".to_string(),
            };

        upload_cmd
            .process_command(&context)
            .await
            .expect("Error on upload.");
    }

    async fn pull_continuous_files(schema: &Schema, file: String) -> anyhow::Result<()> {
        dotenv().ok();

        let base_url = std::env::var("HISTORICAL_URL").expect("Expected database_url.");
        let client = Historical::new(&base_url);

        let query_params = RetrieveParams::new(
            vec![
                "HE.c.0".to_string(),
                "HE.c.1".to_string(),
                "LE.c.0".to_string(),
                "LE.c.1".to_string(),
            ],
            "2024-02-13 00:00:00",
            "2024-02-16 00:00:00",
            schema.clone(),
            Dataset::Futures,
            mbn::enums::Stype::Continuous,
        )?;

        let _response = client.get_records_to_file(&query_params, &file).await?;

        Ok(())
    }

    async fn pull_raw_files(schema: &Schema, file: String) -> anyhow::Result<()> {
        dotenv().ok();

        let base_url = std::env::var("HISTORICAL_URL").expect("Expected database_url.");
        let client = Historical::new(&base_url);

        let query_params = RetrieveParams::new(
            vec![
                "LEG4".to_string(),
                "HEG4".to_string(),
                "HEJ4".to_string(),
                "LEJ4".to_string(),
                "HEK4".to_string(),
                "HEM4".to_string(),
                "LEM4".to_string(),
            ],
            "2024-02-13 00:00:00",
            "2024-02-17 00:00:00",
            schema.clone(),
            Dataset::Futures,
            mbn::enums::Stype::Raw,
        )?;

        let _response = client.get_records_to_file(&query_params, &file).await?;

        Ok(())
    }

    // NOTE: Not sure why BBOS-1 has two etrax records from MBN
    #[tokio::test]
    #[serial]
    async fn test_data_integrity() -> anyhow::Result<()> {
        // Setup
        global_setup().await;

        // Raw
        test_get_records_vs_dbn_raw().await?;

        // Conitnuous
        test_get_records_vs_dbn_continuous_calendar().await?;

        // Rolllover flag
        // test_rollover2().await?;
        test_rollover().await?;

        // Cleanup
        teardown_tickers().await?;

        Ok(())
    }

    async fn test_get_records_vs_dbn_continuous_calendar() -> anyhow::Result<()> {
        let schemas = vec![
            Schema::Mbp1,
            Schema::Tbbo,
            Schema::Trades,
            Schema::Bbo1S,
            Schema::Bbo1M,
            Schema::Ohlcv1S,
            Schema::Ohlcv1M,
            Schema::Ohlcv1H,
            Schema::Ohlcv1D,
        ];

        println!("Testing Continuous Tickers: ");
        for schema in &schemas {
            println!("Schema : {:?}", schema);

            let mbn_file = format!(
                "data/midas/HE.c.0_HE.c.1_LE.c.0_LE.c.1_{}.bin",
                schema.to_string()
            );
            let _ = pull_continuous_files(schema, mbn_file.clone()).await?;

            let dbn_file = PathBuf::from(format!(
                "data/databento/GLBX.MDP3_{}_HE.c.0_HE.c.1_LE.c.0_LE.c.1_2024-02-13T00:00:00Z_2024-02-16T00:00:00Z.dbn",
                schema.to_string()
            ));

            // Test
            compare_dbn_raw_output(dbn_file.clone(), &PathBuf::from(mbn_file.clone())).await?;
            compare_dbn(dbn_file, &PathBuf::from(mbn_file)).await?;
        }

        Ok(())
    }

    async fn test_get_records_vs_dbn_raw() -> anyhow::Result<()> {
        let schemas = vec![
            Schema::Mbp1,
            Schema::Tbbo,
            Schema::Trades,
            Schema::Bbo1S,
            Schema::Bbo1M,
            Schema::Ohlcv1S,
            Schema::Ohlcv1M,
            Schema::Ohlcv1H,
            Schema::Ohlcv1D,
        ];

        println!("Testing Raw Tickers: ");
        for schema in &schemas {
            println!("Schema : {:?}", schema);
            /*             let schema = Schema::Mbp1; */
            let mbn_file = format!(
                "data/midas/HEG4_HEJ4_LEG4_LEJ4_LEM4_HEM4_HEK4_{}.bin",
                schema.to_string()
            );
            let _ = pull_raw_files(schema, mbn_file.clone()).await?;

            let dbn_file = PathBuf::from(format!(
            "data/databento/GLBX.MDP3_{}_HEG4_HEJ4_LEG4_LEJ4_LEM4_HEM4_HEK4_2024-02-13T00:00:00Z_2024-02-17T00:00:00Z.dbn",
            schema.to_string()
        ));

            // Test
            compare_dbn_raw_output(dbn_file.clone(), &PathBuf::from(mbn_file.clone())).await?;
            compare_dbn(dbn_file, &PathBuf::from(mbn_file)).await?;
        }

        Ok(())
    }

    async fn test_rollover() -> anyhow::Result<()> {
        let schemas = vec![
            Schema::Mbp1,
            Schema::Tbbo,
            Schema::Trades,
            Schema::Bbo1S,
            Schema::Bbo1M,
            Schema::Ohlcv1S,
            Schema::Ohlcv1M,
            Schema::Ohlcv1H,
            Schema::Ohlcv1D,
        ];

        for schema in &schemas {
            let mbn_file = format!(
                "data/midas/HE.c.0_HE.c.1_LE.c.0_LE.c.1_{}.bin",
                schema.to_string()
            );
            let mut decoder = AsyncDecoder::<BufReader<File>>::from_file(mbn_file).await?;

            // Write MBN records to file
            let mut rollover_records = Vec::new();
            while let Some(mbn_record) = decoder.decode_ref().await? {
                let record_enum = RecordEnum::from_ref(mbn_record)?;
                if record_enum.msg().header().rollover_flag == 1 {
                    rollover_records.push(record_enum);
                }
            }
            assert_eq!(rollover_records.len(), 2);
        }

        Ok(())
    }

    use std::collections::HashMap;

    #[allow(dead_code)]
    async fn test_rollover2() -> anyhow::Result<()> {
        let schemas = vec![
            Schema::Mbp1,
            Schema::Tbbo,
            Schema::Trades,
            Schema::Bbo1S,
            Schema::Bbo1M,
            Schema::Ohlcv1S,
            Schema::Ohlcv1M,
            Schema::Ohlcv1H,
            Schema::Ohlcv1D,
        ];

        println!("Testing Rollovers: ");

        // Iterate over each schema
        for schema in &schemas {
            let mbn_file = format!(
                "data/midas/HE.c.0_HE.c.1_LE.c.0_LE.c.1_{}.bin",
                schema.to_string()
            );

            let mut decoder = AsyncDecoder::<BufReader<File>>::from_file(mbn_file).await?;
            println!("{:?}", decoder.metadata());

            // Create file to store rollover records
            let mut mbn_rollover_file =
                File::create(format!("test_rollover_{}.txt", schema.to_string())).await?;

            // Initialize a HashMap to count occurrences of instrument_ids
            let mut instrument_count: HashMap<u32, u32> = HashMap::new();

            // Decode and process the records
            while let Some(mbn_record) = decoder.decode_ref().await? {
                let record_enum = RecordEnum::from_ref(mbn_record)?;

                // Track the instrument_id count
                let instrument_id = record_enum.msg().header().instrument_id;
                *instrument_count.entry(instrument_id).or_insert(0) += 1;

                // If rollover_flag is 1, write to the file
                if record_enum.msg().header().rollover_flag == 1 {
                    mbn_rollover_file
                        .write_all(format!("{:?}\n", record_enum).as_bytes())
                        .await?;
                }
            }

            // Print the count of each instrument_id to confirm distribution
            println!(
                "Instrument ID Counts for schema {}: {:?}",
                schema.to_string(),
                instrument_count
            );
        }

        Ok(())
    }
}
