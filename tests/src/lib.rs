use async_compression::tokio::bufread::ZstdDecoder;
use databento::dbn::Record as dbnRecord;
use databento::historical::timeseries::AsyncDbnDecoder;
use mbinary::decode::AsyncDecoder;
use mbinary::record_enum::RecordEnum;
use mbinary::records::Record;
use mbinary::{self};
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

pub async fn read_mbinary_file(
    filepath: &PathBuf,
) -> anyhow::Result<AsyncDecoder<BufReader<File>>> {
    let decoder = AsyncDecoder::<BufReader<File>>::from_file(filepath).await?;

    Ok(decoder)
}

pub async fn dbn_raw_count(dbn_filepath: PathBuf) -> anyhow::Result<()> {
    let (mut dbn_decoder, _map) = read_dbn_file(dbn_filepath).await?;

    // Output files

    let mut dbn_count = 0;
    // Write DBN records to file
    while let Some(_dbn_record) = dbn_decoder.decode_record_ref().await? {
        dbn_count += 1;
    }
    println!("DBN length: {:?}", dbn_count);

    Ok(())
}

pub async fn chronological_check(mbinary_filepath: &PathBuf) -> anyhow::Result<()> {
    let mut mbinary_decoder = read_mbinary_file(mbinary_filepath).await?;

    let mbinary_output_file = "chronological_records.txt";
    let mut mbinary_file = File::create(mbinary_output_file).await?;

    let mut last_ts: u64 = 0;

    let mut old_record = None;
    // Write MBN records to file
    while let Some(mbinary_record) = mbinary_decoder.decode_ref().await? {
        let record_enum = RecordEnum::from_ref(mbinary_record)?;
        let new_ts = record_enum.timestamp();
        if let Some(old_record) = old_record {
            mbinary_file
                .write_all(format!("old : {:?}\n new {:?}\n", old_record, record_enum).as_bytes())
                .await?;
        };

        assert!(new_ts >= last_ts);
        last_ts = new_ts;
        old_record = Some(record_enum);
    }

    Ok(())
}

pub async fn compare_dbn_raw_output(
    dbn_filepath: PathBuf,
    mbinary_filepath: &PathBuf,
) -> anyhow::Result<()> {
    let mut mbinary_decoder = read_mbinary_file(mbinary_filepath).await?;
    let (mut dbn_decoder, _map) = read_dbn_file(dbn_filepath).await?;

    // println!("{:?}", mbinary_decoder.metadata());
    // println!("{:?}", dbn_decoder.metadata());

    // Output files
    let mbinary_output_file = "raw_mbinary_records.txt";
    let dbn_output_file = "raw_dbn_records.txt";

    // Create or truncate output files
    let mut mbinary_file = File::create(mbinary_output_file).await?;
    let mut dbn_file = File::create(dbn_output_file).await?;

    let mut mbinary_count = 0;
    // Write MBN records to file
    while let Some(mbinary_record) = mbinary_decoder.decode_ref().await? {
        mbinary_count += 1;
        let record_enum = RecordEnum::from_ref(mbinary_record)?;
        mbinary_file
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
    println!("MBN length: {:?}", mbinary_count);
    println!("DBN length: {:?}", dbn_count);

    Ok(())
}

pub async fn compare_dbn(
    dbn_filepath: PathBuf,
    mbinary_filepath: &PathBuf,
) -> anyhow::Result<bool> {
    let batch_size = 100000; // New parameter to control batch size
    let mut mbinary_decoder = read_mbinary_file(mbinary_filepath).await?;
    let (mut dbn_decoder, _map) = read_dbn_file(dbn_filepath).await?;

    let mut mbinary_batch: HashMap<u64, Vec<RecordEnum>> = HashMap::new();
    let mut mbinary_decoder_done = false;
    let mut unmatched_dbn_records = Vec::new();

    // Start decoding and comparing
    while let Some(dbn_record) = dbn_decoder.decode_record_ref().await? {
        // If MBN batch is empty, refill it
        if mbinary_batch.len() < batch_size && !mbinary_decoder_done {
            while let Some(mbinary_record) = mbinary_decoder.decode_ref().await? {
                let record_enum = RecordEnum::from_ref(mbinary_record)?;
                let ts_event = record_enum.header().ts_event;
                mbinary_batch.entry(ts_event).or_default().push(record_enum);
            }
            if mbinary_batch.is_empty() {
                mbinary_decoder_done = true; // No more MBN records
            }
        }
        let dbn_record_enum: dbn::RecordEnum = dbn_record.as_enum()?.to_owned();
        let ts_event = match &dbn_record_enum.rtype().unwrap() {
            dbn::RType::Bbo1S | dbn::RType::Bbo1M => match &dbn_record_enum {
                dbn::RecordEnum::Mbp1(msg) => msg.ts_recv,
                _ => return Err(anyhow::Error::msg("Invalid record type")),
            },
            _ => dbn_record_enum.header().ts_event,
        };

        // Check if the ts_event exists in the MBN map
        if let Some(mbinary_group) = mbinary_batch.get_mut(&ts_event) {
            // Try to find a match within the group
            if let Some(pos) = mbinary_group
                .iter()
                .position(|mbinary_record| mbinary_record == &dbn_record_enum)
            {
                mbinary_group.remove(pos); // Remove matched record
                if mbinary_group.is_empty() {
                    mbinary_batch.remove(&ts_event); // Remove the key if the group is empty
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
    if !mbinary_batch.is_empty() {
        file.write_all(b"Unmatched MBN Records:\n").await?;
        for mbinary_record in &mbinary_batch {
            file.write_all(format!("{:?}\n", mbinary_record).as_bytes())
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
    if mbinary_batch.is_empty() && unmatched_dbn_records.is_empty() {
        return Ok(true);
    } else {
        return Ok(false);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dotenv::dotenv;
    use mbinary::enums::{Dataset, Schema};
    use mbinary::params::RetrieveParams;
    use mbinary::symbols::Instrument;
    use mbinary::vendors::{DatabentoData, VendorData, Vendors};
    use midas_client::historical::Historical;
    use midas_client::instrument::Instruments;
    use midas_clilib::cli::commands::vendors::databento::DatabentoCommands;
    use midas_clilib::{self, TaskManager};
    use serial_test::serial;
    use sqlx::PgPool;
    use std::str::FromStr;

    async fn create_tickers() -> anyhow::Result<()> {
        dotenv().ok();

        let base_url = "http://127.0.0.1:8080";
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
            false,
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
            false,
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
            false,
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
            false,
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
            false,
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
            false,
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
            false,
            true,
        ));

        // LEM4
        instruments.push(Instrument::new(
            None,
            "LE.c.0",
            "LiveCattle-c-0",
            dataset,
            Vendors::Internal,
            0,
            0,
            0,
            0,
            true,
            true,
        ));

        // LEM4
        instruments.push(Instrument::new(
            None,
            "LE.c.1",
            "LiveCattle-c-1",
            dataset,
            Vendors::Internal,
            0,
            0,
            0,
            0,
            true,
            true,
        ));

        // LEM4
        instruments.push(Instrument::new(
            None,
            "LE.v.0",
            "LiveCattle-v-0",
            dataset,
            Vendors::Internal,
            0,
            0,
            0,
            0,
            true,
            true,
        ));

        // LEM4
        instruments.push(Instrument::new(
            None,
            "LE.v.1",
            "LiveCattle-v-1",
            dataset,
            Vendors::Internal,
            0,
            0,
            0,
            0,
            true,
            true,
        ));

        // LEM4
        instruments.push(Instrument::new(
            None,
            "HE.c.0",
            "LeanHogs-c-0",
            dataset,
            Vendors::Internal,
            0,
            0,
            0,
            0,
            true,
            true,
        ));

        // LEM4
        instruments.push(Instrument::new(
            None,
            "HE.c.1",
            "LeanHogs-c-1",
            dataset,
            Vendors::Internal,
            0,
            0,
            0,
            0,
            true,
            true,
        ));

        // LEM4
        instruments.push(Instrument::new(
            None,
            "HE.v.0",
            "LeanHogs-v-0",
            dataset,
            Vendors::Internal,
            0,
            0,
            0,
            0,
            true,
            true,
        ));

        // LEM4
        instruments.push(Instrument::new(
            None,
            "HE.v.1",
            "LeanHogs-v-1",
            dataset,
            Vendors::Internal,
            0,
            0,
            0,
            0,
            true,
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

        let base_url = "http://127.0.0.1:8080";
        let client = Instruments::new(&base_url);

        let tickers_to_delete = vec![
            "LEG4".to_string(),
            "HEG4".to_string(),
            "HEJ4".to_string(),
            "LEJ4".to_string(),
            "HEK4".to_string(),
            "HEM4".to_string(),
            "LEM4".to_string(),
            "LE.c.0".to_string(),
            "LE.c.1".to_string(),
            "LE.v.0".to_string(),
            "LE.v.1".to_string(),
            "HE.c.0".to_string(),
            "HE.c.1".to_string(),
            "HE.v.0".to_string(),
            "HE.v.1".to_string(),
        ];

        for ticker in tickers_to_delete {
            let response = client.get_symbol(&ticker, &Dataset::Futures).await?;
            let id = response.data[0].instrument_id.unwrap() as i32;
            client.delete_symbol(&id).await?;
            println!("Deleted ticker: {}", ticker);
        }

        Ok(())
    }

    async fn pull_raw_files(
        schema: &Schema,
        file: String,
        start: &str,
        end: &str,
    ) -> anyhow::Result<()> {
        dotenv().ok();

        let base_url = "http://127.0.0.1:8080";
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
            start,
            end,
            schema.clone(),
            Dataset::Futures,
            mbinary::enums::Stype::Raw,
        )?;

        let _response = client.get_records_to_file(&query_params, &file).await?;

        Ok(())
    }

    // Continuous Calendar
    #[tokio::test]
    #[serial]
    // #[ignore]
    async fn test_data_integrity_continuous_calendar() -> anyhow::Result<()> {
        let path_1 = PathBuf::from("data/databento/GLBX.MDP3_mbp-1_HEG4_HEJ4_LEG4_LEJ4_LEM4_HEM4_HEK4_2024-02-09T00:00:00Z_2024-02-17T00:00:00Z.dbn");
        dbn_raw_count(path_1).await?;

        // Parameters
        let dataset = Dataset::Futures;
        let context = midas_clilib::context::Context::init().expect("Error on context creation.");
        let task_manager = TaskManager::new(context);

        // Seed Database
        create_tickers().await.expect("Error creating tickers");
        let upload_cmd = DatabentoCommands::Upload {
            dataset: dataset.as_str().to_string(),
            dbn_filepath: "GLBX.MDP3_mbp-1_HEG4_HEJ4_LEG4_LEJ4_LEM4_HEM4_HEK4_2024-02-09T00:00:00Z_2024-02-17T00:00:00Z.dbn".to_string(),
            dbn_downloadtype: "stream".to_string(),
            midas_filepath: "system_tests_data2.bin".to_string(),
        };

        upload_cmd
            .process_command(task_manager)
            .await
            .expect("Error on upload.");

        // Connection string for the PostgreSQL container
        let database_url = "postgres://postgres:password@127.0.0.1:5434/market_data";

        let pool = PgPool::connect(database_url)
            .await
            .expect("Failed to connect to the database");
        let query = "REFRESH MATERIALIZED VIEW futures_continuous;";
        sqlx::query(query).execute(&pool).await?;

        // Raw
        test_raw_records_calendar().await?;

        // Continuous Volume
        test_continuous_calendar().await?;

        // Rolllover flag
        test_rollover_calendar().await?;

        // Cleanup
        teardown_tickers().await?;

        Ok(())
    }

    async fn pull_continuous_calendar_files(
        symbols: Vec<String>,
        schema: &Schema,
        file: String,
    ) -> anyhow::Result<()> {
        dotenv().ok();

        let base_url = "http://127.0.0.1:8080";
        let client = Historical::new(&base_url);

        let query_params = RetrieveParams::new(
            symbols,
            "2024-02-13 00:00:00",
            "2024-02-16 00:00:00",
            schema.clone(),
            Dataset::Futures,
            mbinary::enums::Stype::Continuous,
        )?;

        let _response = client.get_records_to_file(&query_params, &file).await?;

        Ok(())
    }

    async fn test_raw_records_calendar() -> anyhow::Result<()> {
        let schemas = vec![
            Schema::Mbp1,
            Schema::Tbbo,
            Schema::Trades,
            Schema::Ohlcv1S,
            Schema::Ohlcv1M,
            Schema::Ohlcv1H,
            Schema::Ohlcv1D,
            Schema::Bbo1M,
            // Schema::Bbo1S, // 2 record not aligned appears they are missing from dbn
        ];

        println!("Testing Raw Tickers: ");
        for schema in &schemas {
            println!("Schema : {:?}", schema);

            let mbinary_file = format!(
                "data/HEG4_HEJ4_LEG4_LEJ4_LEM4_HEM4_HEK4_{}.bin",
                schema.to_string()
            );

            let start: &str = "2024-02-13 00:00:00";
            let end: &str = "2024-02-17 00:00:00";

            let _ = pull_raw_files(schema, mbinary_file.clone(), start, end).await?;

            let dbn_file = PathBuf::from(format!(
            "data/databento/GLBX.MDP3_{}_HEG4_HEJ4_LEG4_LEJ4_LEM4_HEM4_HEK4_2024-02-13T00:00:00Z_2024-02-17T00:00:00Z.dbn",
            schema.to_string()
        ));

            // Test
            compare_dbn_raw_output(dbn_file.clone(), &PathBuf::from(mbinary_file.clone())).await?;
            let equal = compare_dbn(dbn_file, &PathBuf::from(mbinary_file.clone())).await?;
            chronological_check(&PathBuf::from(mbinary_file)).await?;

            assert!(equal);
        }

        Ok(())
    }

    async fn test_continuous_calendar() -> anyhow::Result<()> {
        let schemas = vec![
            Schema::Mbp1,
            Schema::Tbbo,
            Schema::Trades,
            Schema::Ohlcv1S,
            Schema::Ohlcv1M,
            Schema::Ohlcv1H,
            Schema::Ohlcv1D,
            Schema::Bbo1S,
            Schema::Bbo1M,
        ];

        println!("Testing Continuous Calendar ");
        for schema in &schemas {
            println!("Schema : {:?}", schema);

            // Rank 0
            let mbinary_file = format!("data/HE.c.0_LE.c.0_{}.bin", schema.to_string());
            let tickers = vec!["HE.c.0".to_string(), "LE.c.0".to_string()];
            let _ = pull_continuous_calendar_files(tickers, schema, mbinary_file.clone()).await?;

            let dbn_file = PathBuf::from(format!(
                "data/databento/GLBX.MDP3_{}_HE.c.0_LE.c.0_2024-02-13T00:00:00Z_2024-02-16T00:00:00Z.dbn",
                schema.to_string()
            ));

            // Test
            compare_dbn_raw_output(dbn_file.clone(), &PathBuf::from(mbinary_file.clone())).await?;
            let equal = compare_dbn(dbn_file, &PathBuf::from(mbinary_file.clone())).await?;
            chronological_check(&PathBuf::from(mbinary_file)).await?;

            assert!(equal);

            // Rank 1
            let mbinary_file = format!("data/HE.c.1_LE.c.1_{}.bin", schema.to_string());
            let tickers = vec!["HE.c.1".to_string(), "LE.c.1".to_string()];

            let _ = pull_continuous_calendar_files(tickers, schema, mbinary_file.clone()).await?;

            let dbn_file = PathBuf::from(format!(
                "data/databento/GLBX.MDP3_{}_HE.c.1_LE.c.1_2024-02-13T00:00:00Z_2024-02-16T00:00:00Z.dbn",
                schema.to_string()
            ));

            // Test
            compare_dbn_raw_output(dbn_file.clone(), &PathBuf::from(mbinary_file.clone())).await?;
            let equal = compare_dbn(dbn_file, &PathBuf::from(mbinary_file.clone())).await?;
            chronological_check(&PathBuf::from(mbinary_file)).await?;

            assert!(equal);
        }

        Ok(())
    }

    async fn test_rollover_calendar() -> anyhow::Result<()> {
        let schemas = vec![
            Schema::Mbp1,
            Schema::Tbbo,
            Schema::Trades,
            Schema::Ohlcv1S,
            Schema::Ohlcv1M,
            Schema::Ohlcv1H,
            Schema::Ohlcv1D,
            Schema::Bbo1S,
            Schema::Bbo1M,
        ];

        for schema in &schemas {
            // Rank 0
            let mbinary_file = format!("data/HE.c.0_LE.c.0_{}.bin", schema.to_string());
            let mut decoder =
                AsyncDecoder::<BufReader<File>>::from_file(mbinary_file.clone()).await?;

            // Write MBN records to file
            let mut rollover_records = Vec::new();
            while let Some(mbinary_record) = decoder.decode_ref().await? {
                let record_enum = RecordEnum::from_ref(mbinary_record)?;
                if record_enum.msg().header().rollover_flag == 1 {
                    // println!("{:?}", record_enum);
                    rollover_records.push(record_enum);
                }
            }
            assert_eq!(
                rollover_records.len(),
                1,
                "More rollover flags than expected : {}",
                mbinary_file
            );

            // Rank 1
            let mbinary_file = format!("data/HE.c.1_LE.c.1_{}.bin", schema.to_string());
            let mut decoder =
                AsyncDecoder::<BufReader<File>>::from_file(mbinary_file.clone()).await?;

            // Write MBN records to file
            let mut rollover_records = Vec::new();
            while let Some(mbinary_record) = decoder.decode_ref().await? {
                let record_enum = RecordEnum::from_ref(mbinary_record)?;
                if record_enum.msg().header().rollover_flag == 1 {
                    rollover_records.push(record_enum);
                }
            }
            assert_eq!(
                rollover_records.len(),
                1,
                "More rollover flags than expected : {}",
                mbinary_file
            );
        }

        Ok(())
    }

    #[tokio::test]
    #[serial]
    // #[ignore]
    async fn test_data_integrity_continuous_volume() -> anyhow::Result<()> {
        let path_1 = PathBuf::from("data/databento/GLBX.MDP3_mbp-1_HEG4_HEJ4_LEG4_LEJ4_LEM4_HEM4_HEK4_2024-01-17T00:00:00Z_2024-01-24T00:00:00Z.dbn") ;
        dbn_raw_count(path_1).await?;

        // Parameters
        let dataset = Dataset::Futures;
        let context = midas_clilib::context::Context::init().expect("Error on context creation.");
        let task_manager = TaskManager::new(context);

        // Seeed database
        create_tickers().await.expect("Error creating tickers");
        let upload_cmd = DatabentoCommands::Upload {
            dataset: dataset.as_str().to_string(),
            dbn_filepath: "GLBX.MDP3_mbp-1_HEG4_HEJ4_LEG4_LEJ4_LEM4_HEM4_HEK4_2024-01-17T00:00:00Z_2024-01-24T00:00:00Z.dbn".to_string(),
            dbn_downloadtype: "stream".to_string(),
            midas_filepath: "system_tests_data.bin".to_string(),
        };

        upload_cmd
            .process_command(task_manager)
            .await
            .expect("Error on upload.");

        // Connection string for the PostgreSQL container
        let database_url = "postgres://postgres:password@127.0.0.1:5434/market_data";
        let pool = PgPool::connect(database_url)
            .await
            .expect("Failed to connect to the database");
        let query = "REFRESH MATERIALIZED VIEW futures_continuous;";
        sqlx::query(query).execute(&pool).await?;

        // Raw
        test_raw_records_volume().await?;

        // Continuous Calendar
        test_continuous_volume().await?;

        // Rolllover flag
        test_rollover_volume().await?;

        // Cleanup
        teardown_tickers().await?;

        Ok(())
    }

    async fn pull_continuous_volume_files(
        tickers: Vec<String>,
        schema: &Schema,
        file: String,
    ) -> anyhow::Result<()> {
        dotenv().ok();

        let base_url = "http://127.0.0.1:8080";
        let client = Historical::new(&base_url);

        let query_params = RetrieveParams::new(
            tickers,
            "2024-01-19 00:00:00",
            "2024-01-24 00:00:00",
            schema.clone(),
            Dataset::Futures,
            mbinary::enums::Stype::Continuous,
        )?;

        let _response = client.get_records_to_file(&query_params, &file).await?;

        Ok(())
    }

    async fn test_continuous_volume() -> anyhow::Result<()> {
        let schemas = vec![
            Schema::Mbp1,
            Schema::Tbbo,
            Schema::Trades,
            Schema::Ohlcv1S,
            Schema::Ohlcv1M,
            Schema::Ohlcv1H,
            Schema::Ohlcv1D,
            Schema::Bbo1M,
            Schema::Bbo1S,
        ];

        println!("Testing Continuous Volume");
        for schema in &schemas {
            println!("Schema : {:?}", schema);

            let mbinary_file = format!("data/HE.v.0_LE.v.0_{}.bin", schema.to_string());
            let tickers = vec!["HE.v.0".to_string(), "LE.v.0".to_string()];
            let _ = pull_continuous_volume_files(tickers, schema, mbinary_file.clone()).await?;

            let dbn_file = PathBuf::from(format!(
                "data/databento/GLBX.MDP3_{}_HE.v.0_LE.v.0_2024-01-19T00:00:00Z_2024-01-24T00:00:00Z.dbn",
                schema.to_string()
            ));

            // Test
            compare_dbn_raw_output(dbn_file.clone(), &PathBuf::from(mbinary_file.clone())).await?;
            let equal = compare_dbn(dbn_file, &PathBuf::from(mbinary_file.clone())).await?;
            chronological_check(&PathBuf::from(mbinary_file)).await?;

            assert!(equal);

            let mbinary_file = format!("data/HE.v.1_LE.v.1_{}.bin", schema.to_string());
            let tickers = vec!["HE.v.1".to_string(), "LE.v.1".to_string()];
            let _ = pull_continuous_volume_files(tickers, schema, mbinary_file.clone()).await?;

            let dbn_file = PathBuf::from(format!(
                "data/databento/GLBX.MDP3_{}_HE.v.1_LE.v.1_2024-01-19T00:00:00Z_2024-01-24T00:00:00Z.dbn",
                schema.to_string()
            ));

            // Test
            compare_dbn_raw_output(dbn_file.clone(), &PathBuf::from(mbinary_file.clone())).await?;
            let equal = compare_dbn(dbn_file, &PathBuf::from(mbinary_file.clone())).await?;
            chronological_check(&PathBuf::from(mbinary_file)).await?;

            assert!(equal);
        }

        Ok(())
    }

    async fn test_raw_records_volume() -> anyhow::Result<()> {
        let schemas = vec![
            Schema::Mbp1,
            Schema::Tbbo,
            Schema::Trades,
            Schema::Ohlcv1S,
            Schema::Ohlcv1M,
            Schema::Ohlcv1H,
            Schema::Ohlcv1D,
            Schema::Bbo1M,
            Schema::Bbo1S,
        ];

        println!("Testing Raw Tickers: ");
        for schema in &schemas {
            println!("Schema : {:?}", schema);
            let mbinary_file = format!(
                "data/v_HEG4_HEJ4_LEG4_LEJ4_LEM4_HEM4_HEK4_{}.bin",
                schema.to_string()
            );

            let start: &str = "2024-01-18 00:00:00";
            let end: &str = "2024-01-24 00:00:00";

            let _ = pull_raw_files(schema, mbinary_file.clone(), start, end).await?;

            let dbn_file = PathBuf::from(format!(
            "data/databento/GLBX.MDP3_{}_HEG4_HEJ4_LEG4_LEJ4_LEM4_HEM4_HEK4_2024-01-18T00:00:00Z_2024-01-24T00:00:00Z.dbn",
            schema.to_string()
        ));

            // Test
            compare_dbn_raw_output(dbn_file.clone(), &PathBuf::from(mbinary_file.clone())).await?;
            let equal = compare_dbn(dbn_file, &PathBuf::from(mbinary_file.clone())).await?;
            chronological_check(&PathBuf::from(mbinary_file)).await?;

            assert!(equal);
        }

        Ok(())
    }

    async fn test_rollover_volume() -> anyhow::Result<()> {
        let schemas = vec![
            Schema::Mbp1,
            Schema::Tbbo,
            Schema::Trades,
            Schema::Ohlcv1S,
            Schema::Ohlcv1M,
            Schema::Ohlcv1H,
            Schema::Ohlcv1D,
            Schema::Bbo1M,
            Schema::Bbo1S,
        ];

        for schema in &schemas {
            let mbinary_file = format!("data/HE.v.0_LE.v.0_{}.bin", schema.to_string());
            let mut decoder = AsyncDecoder::<BufReader<File>>::from_file(mbinary_file).await?;

            // Write MBN records to file
            let mut rollover_records = Vec::new();
            while let Some(mbinary_record) = decoder.decode_ref().await? {
                let record_enum = RecordEnum::from_ref(mbinary_record)?;
                if record_enum.msg().header().rollover_flag == 1 {
                    rollover_records.push(record_enum);
                }
            }

            assert_eq!(
                rollover_records.len(),
                1,
                "Rollover count was not 1 for schema: {:?}",
                schema
            );

            let mbinary_file = format!("data/HE.v.1_LE.v.1_{}.bin", schema.to_string());
            let mut decoder = AsyncDecoder::<BufReader<File>>::from_file(mbinary_file).await?;

            // Write MBN records to file
            let mut rollover_records2 = Vec::new();
            while let Some(mbinary_record) = decoder.decode_ref().await? {
                let record_enum = RecordEnum::from_ref(mbinary_record)?;
                if record_enum.msg().header().rollover_flag == 1 {
                    rollover_records2.push(record_enum);
                }
            }
            assert_eq!(
                rollover_records2.len(),
                1,
                "Rollover count was not 1 for schema: {:?}",
                schema
            );
        }

        Ok(())
    }
}
