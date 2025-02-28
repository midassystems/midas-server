use super::heap::MinHeap;
use super::retriever::{ContinuousKind, Records};
use crate::database::read::common::{RecordsQuery, RollingWindow};
use crate::database::read::rows::get_from_row_fn;
use crate::services::utils::query_symbols_map;
use crate::Result;
use futures::stream::StreamExt;
use mbinary::enums::{RType, Stype};
use mbinary::params::RetrieveParams;
use mbinary::{record_enum::RecordEnum, symbols::SymbolMap};
use sqlx::{PgPool, Row};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::error;

#[derive(Debug)]
pub struct QueryTask {
    id: String,
    continuous_id: u32,
    stype: Stype,
    final_end: i64,
    tickers: Vec<String>,
    pub symbol_map: SymbolMap,
    params: RetrieveParams,
    continuous_map: HashMap<String, Vec<RollingWindow>>,
    id_map: HashMap<String, u32>,
    heap: Arc<Mutex<MinHeap<Records>>>,
    pool: PgPool,
    pub queried_flag: Arc<Mutex<bool>>,
    rollover_map: HashMap<String, u8>,
}

impl QueryTask {
    pub async fn new(
        id: String,
        continuous_id: u32,
        mut params: RetrieveParams,
        tickers: Vec<String>,
        rank: i32,
        kind: ContinuousKind,
        pool: PgPool,
        heap: Arc<Mutex<MinHeap<Records>>>,
        queried_flag: Arc<Mutex<bool>>,
    ) -> Result<Self> {
        // Adjust start/end to be start/end of respective interval
        let _ = params.interval_adjust_ts_start()?;
        let _ = params.interval_adjust_ts_end()?;
        let final_end = params.end_ts;
        let stype = params.stype;

        // Initialize the QueryTask struct
        let mut task = QueryTask {
            id,
            continuous_id,
            stype,
            final_end,
            tickers,
            symbol_map: SymbolMap::new(),
            params,
            continuous_map: HashMap::new(),
            id_map: HashMap::new(),
            heap,
            pool,
            queried_flag,
            rollover_map: HashMap::new(),
        };

        task.initialize(rank, kind).await?;
        task.adjust_params_end().await?;

        Ok(task)
    }

    pub async fn adjust_params_end(&mut self) -> Result<()> {
        let interval_ns = 86_400_000_000_000;
        let start_day_end =
            self.params.start_ts + (interval_ns - (self.params.start_ts % interval_ns));

        if self.params.end_ts > start_day_end {
            self.params.end_ts = start_day_end;
        }
        Ok(())
    }
    pub async fn initialize(&mut self, rank: i32, kind: ContinuousKind) -> Result<()> {
        match self.params.stype {
            Stype::Raw => {
                self.symbol_map =
                    query_symbols_map(&self.pool, &self.params.symbols, self.params.dataset)
                        .await?;
            }
            Stype::Continuous => {
                // Build the id_map and continuous_map for continuous tasks
                for (index, ticker) in self.tickers.iter().enumerate() {
                    let synthetic_id = self.continuous_id + index as u32; // Generate synthetic ID
                    self.id_map.insert(ticker.to_string(), synthetic_id);
                    self.symbol_map.add_instrument(ticker, synthetic_id);
                }

                let symbols = self
                    .tickers
                    .iter()
                    .map(|s| s.split('.').next().unwrap_or(s).to_string() + "%")
                    .collect();

                // Now retrieve continuous data using RollingWindow
                let window_data = RollingWindow::retrieve_continuous_window(
                    &self.pool,
                    self.params.start_ts,
                    self.final_end,
                    rank,
                    symbols,
                    &kind,
                )
                .await?;
                self.continuous_map.extend(window_data);
            }
        }

        Ok(())
    }

    pub async fn get_records(&mut self, continuous_flag: bool) -> Result<()> {
        let rtype = RType::from(self.params.rtype().unwrap());
        let from_row_fn = get_from_row_fn(rtype);

        while *self.queried_flag.lock().await {
            tokio::task::yield_now().await;
        }

        let mut cursor = RecordEnum::retrieve_query(
            &self.pool,
            &self.params,
            continuous_flag,
            format!(".{}", self.id.clone()),
        )
        .await?;

        while let Some(row_result) = cursor.next().await {
            match row_result {
                Ok(row) => match self.stype {
                    Stype::Raw => {
                        let record = Records {
                            record: from_row_fn(&row, None, None)?,
                        };

                        {
                            let mut heap = self.heap.lock().await;
                            heap.push(record);
                        }
                    }
                    Stype::Continuous => {
                        let ticker: String = row.try_get("ticker")?;
                        let new_id = self.id_map.get(&ticker).copied();
                        let rollover = self.rollover_map.get(&ticker).copied();

                        let record = Records {
                            record: from_row_fn(&row, new_id, rollover)?,
                        };

                        if let Some(_r) = rollover {
                            self.rollover_map.remove(&ticker);
                        }

                        {
                            let mut heap = self.heap.lock().await;
                            heap.push(record);
                        }
                    }
                },
                Err(e) => {
                    error!("Error processing row: {:?}", e);
                    return Err(e.into());
                }
            }
        }

        *self.queried_flag.lock().await = true;

        Ok(())
    }

    pub async fn process_records_raw(&mut self) -> Result<()> {
        while self.params.end_ts < self.final_end {
            self.get_records(false).await?;
            self.params.start_ts = self.params.end_ts;
            self.params.end_ts += 86_400_000_000_000;
        }

        self.params.end_ts = self.final_end;
        self.get_records(false).await?;

        Ok(())
    }

    fn update_tickers(&mut self, current_tickers: &mut HashMap<String, RollingWindow>) {
        let mut to_remove = Vec::new();

        for (ticker, obj) in current_tickers.iter_mut() {
            if obj.end_time < self.params.start_ts {
                if let Some(new_value) = self.continuous_map.get_mut(ticker).and_then(|v| v.pop()) {
                    *obj = new_value;
                    self.rollover_map.insert(ticker.clone(), 1);
                } else {
                    to_remove.push(ticker.clone());
                }
            }
        }

        // Remove tickers that have no more data
        for ticker in to_remove {
            current_tickers.remove(&ticker);
        }

        // Collect values into a Vec
        self.params.symbols = current_tickers
            .values()
            .map(|obj| obj.ticker.clone()) // Extracts ticker field
            .collect::<Vec<String>>();
    }

    pub async fn process_records_continuous(&mut self) -> Result<()> {
        let mut current_tickers = HashMap::new();
        for (ticker, obj) in &mut self.continuous_map {
            if let Some(value) = obj.pop() {
                current_tickers.insert(ticker.clone(), value);
            }
        }

        // Wait for all tasks to finish
        while self.params.end_ts < self.final_end {
            self.update_tickers(&mut current_tickers);
            self.get_records(true).await?;

            self.params.start_ts = self.params.end_ts;
            self.params.end_ts += 86_400_000_000_000;
        }
        self.update_tickers(&mut current_tickers);

        self.params.end_ts = self.final_end;
        self.get_records(true).await?;

        Ok(())
    }

    pub async fn process_records(&mut self) -> Result<()> {
        match self.stype {
            Stype::Continuous => {
                self.process_records_continuous().await?;
            }
            Stype::Raw => {
                self.process_records_raw().await?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::database::init::init_db;
    use crate::response::ApiResponse;
    use crate::services::load::create_record;
    use axum::response::IntoResponse;
    use axum::{Extension, Json};
    use dotenv;
    use mbinary::encode::CombinedEncoder;
    use mbinary::enums::Schema;
    use mbinary::enums::{Action, Dataset, Side, Stype};
    use mbinary::metadata::Metadata;
    use mbinary::record_ref::RecordRef;
    use mbinary::records::{BidAskPair, Mbp1Msg, RecordHeader};
    use mbinary::symbols::Instrument;
    use mbinary::vendors::Vendors;
    use mbinary::vendors::{DatabentoData, VendorData};
    use serial_test::serial;
    use sqlx::postgres::PgPoolOptions;
    use std::os::raw::c_char;
    use std::str::FromStr;

    // -- Helper functions
    async fn create_instrument(instrument: Instrument) -> Result<i32> {
        let database_url = std::env::var("INSTRUMENT_DATABASE_URL")?;
        let pool = PgPoolOptions::new()
            .max_connections(10)
            .connect(&database_url)
            .await?;

        let mut tx = pool.begin().await.expect("Error settign up database.");

        // Insert dataset into the instrument table and fetch the ID
        let instrument_id: i32 = sqlx::query_scalar(
            r#"
            INSERT INTO instrument (dataset)
            VALUES ($1)
            RETURNING id
            "#,
        )
        .bind(instrument.dataset.clone() as i16)
        .fetch_one(&mut *tx) // Borrow tx mutably
        .await?;

        let query = format!(
            r#"
            INSERT INTO {} (instrument_id, ticker, name, vendor,vendor_data, last_available, first_available,expiration_date, active)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            RETURNING id
            "#,
            instrument.dataset.as_str()
        );

        let _ = sqlx::query(&query)
            .bind(instrument_id)
            .bind(instrument.ticker)
            .bind(instrument.name)
            .bind(instrument.vendor.as_str())
            .bind(instrument.vendor_data as i64)
            .bind(instrument.last_available as i64)
            .bind(instrument.first_available as i64)
            .bind(instrument.expiration_date as i64)
            .bind(instrument.active)
            .execute(&mut *tx) // Borrow tx mutably
            .await?;

        let _ = tx.commit().await;

        Ok(instrument_id)
    }

    async fn delete_instrument(id: i32) -> Result<()> {
        let database_url = std::env::var("INSTRUMENT_DATABASE_URL")?;
        let pool = PgPoolOptions::new()
            .max_connections(10)
            .connect(&database_url)
            .await?;

        let mut tx = pool.begin().await.expect("Error settign up database.");

        let _ = sqlx::query(
            r#"
            DELETE FROM instrument WHERE id = $1
            "#,
        )
        .bind(id)
        .execute(&mut *tx)
        .await?;
        let _ = tx.commit().await;

        Ok(())
    }

    async fn create_futures() -> anyhow::Result<Vec<i32>> {
        //  Vendor data
        let schema = dbn::Schema::from_str("mbp-1")?;
        let dbn_dataset = dbn::Dataset::from_str("GLBX.MDP3")?;
        let stype = dbn::SType::from_str("raw_symbol")?;
        let vendor_data = VendorData::Databento(DatabentoData {
            schema,
            dataset: dbn_dataset,
            stype,
        });

        // Create instrument
        let dataset = Dataset::Futures;
        let mut instruments = Vec::new();

        // LEG4
        instruments.push(Instrument::new(
            None,
            "HEG4",
            "LeanHogs-0224",
            dataset,
            Vendors::Databento,
            vendor_data.encode(),
            1707937194000000000,
            1704067200000000000,
            1707937194000000000,
            true,
        ));

        instruments.push(Instrument::new(
            None,
            "HEJ4",
            "LeanHogs-0424",
            dataset,
            Vendors::Databento,
            vendor_data.encode(),
            1712941200000000000,
            1704067200000000000,
            1712941200000000000,
            true,
        ));

        instruments.push(Instrument::new(
            None,
            "LEG4",
            "LeanHogs-0224",
            dataset,
            Vendors::Databento,
            vendor_data.encode(),
            1707937194000000000,
            1704067200000000000,
            1707937194000000000,
            true,
        ));

        instruments.push(Instrument::new(
            None,
            "LEJ4",
            "LeanHogs-0424",
            dataset,
            Vendors::Databento,
            vendor_data.encode(),
            1712941200000000000,
            1704067200000000000,
            1713326400000000000,
            true,
        ));

        let mut ids = Vec::new();
        for i in instruments {
            let id = create_instrument(i).await?;
            ids.push(id);
        }

        Ok(ids)
    }

    async fn create_equities() -> anyhow::Result<Vec<i32>> {
        //  Vendor data
        let schema = dbn::Schema::from_str("mbp-1")?;
        let dbn_dataset = dbn::Dataset::from_str("GLBX.MDP3")?;
        let stype = dbn::SType::from_str("raw_symbol")?;
        let vendor_data = VendorData::Databento(DatabentoData {
            schema,
            dataset: dbn_dataset,
            stype,
        });

        // Create instrument
        let dataset = Dataset::Equities;

        let mut ids = Vec::new();
        let instruments = vec![
            Instrument::new(
                None,
                "AAPL",
                "aaple",
                dataset.clone(),
                Vendors::Databento,
                vendor_data.encode(),
                1704672000000000000,
                1704672000000000000,
                0,
                true,
            ),
            Instrument::new(
                None,
                "TSLA",
                "tesla",
                dataset.clone(),
                Vendors::Databento,
                vendor_data.encode(),
                1704672000000000000,
                1704672000000000000,
                0,
                true,
            ),
        ];

        for i in instruments {
            let id = create_instrument(i).await?;
            ids.push(id);
        }

        Ok(ids)
    }

    async fn create_equity_records(ids: &Vec<i32>) -> anyhow::Result<Vec<Mbp1Msg>> {
        let records = vec![
            Mbp1Msg {
                hd: { RecordHeader::new::<Mbp1Msg>(ids[0] as u32, 1704209103644092564, 0) },
                price: 6770,
                size: 1,
                action: 1,
                side: 2,
                depth: 0,
                flags: 10,
                ts_recv: 1704209103644092564,
                ts_in_delta: 17493,
                sequence: 739763,
                discriminator: 0,
                levels: [BidAskPair {
                    bid_px: 1,
                    ask_px: 1,
                    bid_sz: 1,
                    ask_sz: 1,
                    bid_ct: 10,
                    ask_ct: 20,
                }],
            },
            Mbp1Msg {
                hd: { RecordHeader::new::<Mbp1Msg>(ids[1] as u32, 1704209103644092565, 0) },
                price: 6870,
                size: 2,
                action: 1,
                side: 1,
                depth: 0,
                flags: 0,
                ts_recv: 1704209103644092565,
                ts_in_delta: 17493,
                sequence: 739763,
                discriminator: 0,
                levels: [BidAskPair {
                    bid_px: 1,
                    ask_px: 1,
                    bid_sz: 1,
                    ask_sz: 1,
                    bid_ct: 10,
                    ask_ct: 20,
                }],
            },
        ];

        let metadata = Metadata::new(
            Schema::Mbp1,
            Dataset::Equities,
            1704209103644092563,
            1704209103644092566,
            SymbolMap::new(),
        );

        let mut buffer = Vec::new();
        let mut encoder = CombinedEncoder::new(&mut buffer);
        encoder.encode_metadata(&metadata)?;

        for r in &records {
            encoder
                .encode_record(&RecordRef::from(r))
                .expect("Encoding failed");
        }

        let pool = init_db().await.unwrap();
        let response = create_record(Extension(pool.clone()), Json(buffer))
            .await
            .expect("Error creating records.")
            .into_response();

        let mut stream = response.into_body().into_data_stream();

        // Collect streamed responses
        while let Some(chunk) = stream.next().await {
            match chunk {
                Ok(bytes) => {
                    let bytes_str = String::from_utf8_lossy(&bytes);

                    match serde_json::from_str::<ApiResponse<String>>(&bytes_str) {
                        Ok(response) => if response.status == "success" {},
                        Err(e) => {
                            eprintln!("Failed to parse chunk: {:?}, raw chunk: {}", e, bytes_str);
                        }
                    }
                }
                Err(e) => {
                    panic!("Error while reading chunk: {:?}", e);
                }
            }
        }
        Ok(records)
    }

    async fn create_future_records(ids: &Vec<i32>) -> anyhow::Result<Vec<Mbp1Msg>> {
        let records = vec![
            Mbp1Msg {
                hd: { RecordHeader::new::<Mbp1Msg>(ids[0] as u32, 1704209103644092562, 0) },
                price: 500,
                size: 1,
                action: Action::Trade as c_char,
                side: Side::Bid as c_char,
                depth: 0,
                flags: 0,
                ts_recv: 1704209103644092562,
                ts_in_delta: 17493,
                sequence: 739763,
                discriminator: 0,
                levels: [BidAskPair {
                    bid_px: 1,
                    ask_px: 1,
                    bid_sz: 1,
                    ask_sz: 1,
                    bid_ct: 10,
                    ask_ct: 20,
                }],
            },
            Mbp1Msg {
                hd: { RecordHeader::new::<Mbp1Msg>(ids[0] as u32, 1704240000000000001, 0) },
                price: 500,
                size: 1,
                action: Action::Trade as c_char,
                side: Side::Bid as c_char,
                depth: 0,
                flags: 0,
                ts_recv: 1704240000000000001,
                ts_in_delta: 17493,
                sequence: 739763,
                discriminator: 0,
                levels: [BidAskPair {
                    bid_px: 1,
                    ask_px: 1,
                    bid_sz: 1,
                    ask_sz: 1,
                    bid_ct: 10,
                    ask_ct: 20,
                }],
            },
            Mbp1Msg {
                hd: { RecordHeader::new::<Mbp1Msg>(ids[1] as u32, 1707117590000000000, 0) },
                price: 6770,
                size: 1,
                action: Action::Trade as c_char,
                side: 2,
                depth: 0,
                flags: 0,
                ts_recv: 1707117590000000000,
                ts_in_delta: 17493,
                sequence: 739763,
                discriminator: 0,

                levels: [BidAskPair {
                    bid_px: 1,
                    ask_px: 1,
                    bid_sz: 1,
                    ask_sz: 1,
                    bid_ct: 10,
                    ask_ct: 20,
                }],
            },
            Mbp1Msg {
                hd: { RecordHeader::new::<Mbp1Msg>(ids[2] as u32, 1704295503644092562, 0) },
                price: 6870,
                size: 2,
                action: Action::Trade as c_char,
                side: 2,
                depth: 0,
                flags: 0,
                ts_recv: 1704295503644092562,
                ts_in_delta: 17493,
                sequence: 739763,
                discriminator: 0,

                levels: [BidAskPair {
                    bid_px: 1,
                    ask_px: 1,
                    bid_sz: 1,
                    ask_sz: 1,
                    bid_ct: 10,
                    ask_ct: 20,
                }],
            },
            Mbp1Msg {
                hd: { RecordHeader::new::<Mbp1Msg>(ids[3] as u32, 1707117590000000000, 0) },
                price: 6870,
                size: 2,
                action: Action::Trade as c_char,
                side: 2,
                depth: 0,
                flags: 0,
                ts_recv: 1707117590000000000,
                ts_in_delta: 17493,
                sequence: 739763,
                discriminator: 0,

                levels: [BidAskPair {
                    bid_px: 1,
                    ask_px: 1,
                    bid_sz: 1,
                    ask_sz: 1,
                    bid_ct: 10,
                    ask_ct: 20,
                }],
            },
        ];

        let metadata = Metadata::new(
            Schema::Mbp1,
            Dataset::Futures,
            1704295503644092562,
            1707117590000000000,
            SymbolMap::new(),
        );

        let mut buffer = Vec::new();
        let mut encoder = CombinedEncoder::new(&mut buffer);
        encoder.encode_metadata(&metadata)?;

        for r in &records {
            encoder
                .encode_record(&RecordRef::from(r))
                .expect("Encoding failed");
        }

        let pool = init_db().await.unwrap();
        let response = create_record(Extension(pool.clone()), Json(buffer))
            .await
            .expect("Error creating records.")
            .into_response();

        let mut stream = response.into_body().into_data_stream();

        // Collect streamed responses
        while let Some(chunk) = stream.next().await {
            match chunk {
                Ok(bytes) => {
                    let bytes_str = String::from_utf8_lossy(&bytes);

                    match serde_json::from_str::<ApiResponse<String>>(&bytes_str) {
                        Ok(response) => if response.status == "success" {},
                        Err(e) => {
                            eprintln!("Failed to parse chunk: {:?}, raw chunk: {}", e, bytes_str);
                        }
                    }
                }
                Err(e) => {
                    panic!("Error while reading chunk: {:?}", e);
                }
            }
        }

        // Populare materialized views
        let query = "REFRESH MATERIALIZED VIEW futures_continuous_calendar_windows";
        sqlx::query(query).execute(&pool).await?;

        let query = "REFRESH MATERIALIZED VIEW futures_continuous_volume_windows";
        sqlx::query(query).execute(&pool).await?;

        Ok(records)
    }

    #[sqlx::test]
    #[serial]
    // #[ignore]
    async fn test_query_task_symbols_map_raw() -> anyhow::Result<()> {
        dotenv::dotenv().ok();
        let pool = init_db().await.unwrap();
        let ids = create_futures().await?;
        let _records = create_future_records(&ids).await?;

        let tickers = vec![
            "HEJ4".to_string(),
            "LEJ4".to_string(),
            "HEG4".to_string(),
            "LEG4".to_string(),
        ];

        let params = RetrieveParams {
            symbols: tickers.clone(),
            start_ts: 1704085200000000000,
            end_ts: 1714536000000000000,
            schema: Schema::Mbp1,
            dataset: Dataset::Futures,
            stype: Stype::Raw,
        };

        // Test
        let heap = Arc::new(Mutex::new(MinHeap::new()));
        let queried_flag = Arc::new(Mutex::new(false));

        let task = QueryTask::new(
            "raw".to_string(),
            0,
            params,
            tickers,
            0,
            ContinuousKind::None,
            pool,
            heap,
            queried_flag,
        )
        .await?;

        // Validate
        assert!(task.symbol_map.map.keys().len() == 4);

        // Cleanup
        for id in ids {
            delete_instrument(id).await.expect("Error on delete");
        }

        Ok(())
    }

    #[sqlx::test]
    #[serial]
    // #[ignore]
    async fn test_query_task_symbols_map_continuous_calendar() -> anyhow::Result<()> {
        dotenv::dotenv().ok();
        let pool = init_db().await.unwrap();
        let ids = create_futures().await?;
        let _records = create_future_records(&ids).await?;

        let tickers = vec!["HE".to_string(), "LE".to_string()];

        let params = RetrieveParams {
            symbols: tickers.clone(),
            start_ts: 1704085200000000000,
            end_ts: 1714536000000000000,
            schema: Schema::Mbp1,
            dataset: Dataset::Futures,
            stype: Stype::Continuous,
        };

        // Test
        let heap = Arc::new(Mutex::new(MinHeap::new()));
        let queried_flag = Arc::new(Mutex::new(false));

        let task = QueryTask::new(
            "c.0".to_string(),
            1_00,
            params.clone(),
            tickers.clone(),
            0,
            ContinuousKind::Calendar,
            pool.clone(),
            Arc::clone(&heap),
            Arc::clone(&queried_flag),
        )
        .await?;

        let task2 = QueryTask::new(
            "c.1".to_string(),
            1_100,
            params,
            tickers,
            1,
            ContinuousKind::Calendar,
            pool.clone(),
            Arc::clone(&heap),
            Arc::clone(&queried_flag),
        )
        .await?;

        // Validate
        assert!(task.symbol_map.map.keys().len() == 2);
        assert!(task2.symbol_map.map.keys().len() == 2);

        // Cleanup
        for id in ids {
            delete_instrument(id).await.expect("Error on delete");
        }

        Ok(())
    }
    #[sqlx::test]
    #[serial]
    // #[ignore]
    async fn test_query_task_symbols_map_continuous_volume() -> anyhow::Result<()> {
        dotenv::dotenv().ok();
        let pool = init_db().await.unwrap();
        let ids = create_futures().await?;
        let _records = create_future_records(&ids).await?;

        let tickers = vec!["HE".to_string(), "LE".to_string()];

        let params = RetrieveParams {
            symbols: tickers.clone(),
            start_ts: 1704085200000000000,
            end_ts: 1714536000000000000,
            schema: Schema::Mbp1,
            dataset: Dataset::Futures,
            stype: Stype::Continuous,
        };

        // Test
        let heap = Arc::new(Mutex::new(MinHeap::new()));
        let queried_flag = Arc::new(Mutex::new(false));

        let task = QueryTask::new(
            "v.0".to_string(),
            1_00,
            params.clone(),
            tickers.clone(),
            0,
            ContinuousKind::Volume,
            pool.clone(),
            Arc::clone(&heap),
            Arc::clone(&queried_flag),
        )
        .await?;

        let task2 = QueryTask::new(
            "v.1".to_string(),
            1_100,
            params,
            tickers,
            1,
            ContinuousKind::Volume,
            pool.clone(),
            Arc::clone(&heap),
            Arc::clone(&queried_flag),
        )
        .await?;

        // Validate
        assert!(task.symbol_map.map.keys().len() == 2);
        assert!(task2.symbol_map.map.keys().len() == 2);

        // Cleanup
        for id in ids {
            delete_instrument(id).await.expect("Error on delete");
        }

        Ok(())
    }

    #[sqlx::test]
    #[serial]
    // #[ignore]
    async fn test_get_records() -> anyhow::Result<()> {
        dotenv::dotenv().ok();
        let pool = init_db().await.unwrap();

        let ids = create_equities().await?;
        let records = create_equity_records(&ids).await?;

        // Test
        let params = RetrieveParams {
            symbols: vec!["AAPL".to_string(), "TSLA".to_string()],
            start_ts: 1704171600000000000,
            end_ts: 1704225600000000000,
            schema: Schema::Mbp1,
            dataset: Dataset::Equities,
            stype: Stype::Raw,
        };

        let heap = Arc::new(Mutex::new(MinHeap::new()));
        let queried_flag = Arc::new(Mutex::new(false));

        let mut task = QueryTask::new(
            "raw".to_string(),
            0,
            params.clone(),
            params.symbols.clone(),
            0,
            ContinuousKind::None,
            pool.clone(),
            Arc::clone(&heap),
            Arc::clone(&queried_flag),
        )
        .await?;

        task.process_records().await?;

        // Validate
        let record = heap.lock().await.pop().unwrap();
        assert_eq!(record.record, RecordEnum::Mbp1(records[0].clone()));

        let record2 = heap.lock().await.pop().unwrap();
        assert_eq!(record2.record, RecordEnum::Mbp1(records[1].clone()));

        // Cleanup
        for id in ids {
            delete_instrument(id).await.expect("Error on delete");
        }

        Ok(())
    }

    #[sqlx::test]
    #[serial]
    // #[ignore]
    async fn test_get_futures() -> anyhow::Result<()> {
        dotenv::dotenv().ok();
        let pool = init_db().await.unwrap();

        let ids = create_futures().await?;
        let records = create_future_records(&ids).await?;

        // Test
        let params = RetrieveParams {
            symbols: vec!["LEG4".to_string(), "HEG4".to_string()],
            start_ts: 1704171600000000000,
            end_ts: 1704225600000000000,
            schema: Schema::Mbp1,
            dataset: Dataset::Futures,
            stype: Stype::Raw,
        };

        let heap = Arc::new(Mutex::new(MinHeap::new()));
        let queried_flag = Arc::new(Mutex::new(false));

        let mut task = QueryTask::new(
            "raw".to_string(),
            0,
            params.clone(),
            params.symbols.clone(),
            0,
            ContinuousKind::None,
            pool.clone(),
            Arc::clone(&heap),
            Arc::clone(&queried_flag),
        )
        .await?;

        task.process_records().await?;

        // Validate
        let record0 = heap.lock().await.pop().unwrap();
        assert_eq!(record0.record, RecordEnum::Mbp1(records[0].clone()));

        // Cleanup
        for id in ids {
            delete_instrument(id).await.expect("Error on delete");
        }

        Ok(())
    }

    #[sqlx::test]
    #[serial]
    // #[ignore]
    async fn test_get_futures_calendar_continuous() -> anyhow::Result<()> {
        dotenv::dotenv().ok();
        let pool = init_db().await.unwrap();

        let ids = create_futures().await?;
        let records = create_future_records(&ids).await?;

        // Test
        let params = RetrieveParams {
            symbols: vec!["HE.c.0".to_string(), "LE.c.0".to_string()],
            start_ts: 1704171600000000000,
            end_ts: 1704225600000000000,
            schema: Schema::Mbp1,
            dataset: Dataset::Futures,
            stype: Stype::Continuous,
        };

        let heap = Arc::new(Mutex::new(MinHeap::new()));
        let queried_flag = Arc::new(Mutex::new(false));

        let mut task = QueryTask::new(
            "c.0".to_string(),
            1_000_000,
            params.clone(),
            vec!["HE.c.0".to_string(), "LE.c.0".to_string()],
            0,
            ContinuousKind::Calendar,
            pool.clone(),
            Arc::clone(&heap),
            Arc::clone(&queried_flag),
        )
        .await?;

        task.process_records().await?;

        // Validate
        let record = heap.lock().await.pop().unwrap();
        assert!(record.record != RecordEnum::Mbp1(records[1].clone()));

        // Cleanup
        for id in ids {
            delete_instrument(id).await.expect("Error on delete");
        }

        Ok(())
    }

    #[sqlx::test]
    #[serial]
    // #[ignore]
    async fn test_get_futures_volume_continuous() -> anyhow::Result<()> {
        // Tets works in that the HE.v.0 is the only one with a record in the retriev windwo , so
        // the secodns record msg is retunred as the only record beaceu the first record is th eday
        // in which it becoems the HE.v.0 so it start at the begiging of the next day
        dotenv::dotenv().ok();
        let pool = init_db().await.unwrap();

        let ids = create_futures().await?;
        let records = create_future_records(&ids).await?;

        // Test
        let params = RetrieveParams {
            symbols: vec!["HE.v.0".to_string(), "LE.v.0".to_string()],
            start_ts: 1704240000000000000,
            end_ts: 1704250000000000000,
            // end_ts: 1713117594000000000,
            schema: Schema::Mbp1,
            dataset: Dataset::Futures,
            stype: Stype::Continuous,
        };

        let heap = Arc::new(Mutex::new(MinHeap::new()));
        let queried_flag = Arc::new(Mutex::new(false));

        let mut task = QueryTask::new(
            "v.0".to_string(),
            1_000_000,
            params.clone(),
            vec!["HE.v.0".to_string(), "LE.v.0".to_string()],
            0,
            ContinuousKind::Volume,
            pool.clone(),
            Arc::clone(&heap),
            Arc::clone(&queried_flag),
        )
        .await?;

        task.process_records().await?;

        // Validate
        let record0 = heap.lock().await.pop();
        let record = record0.unwrap().record;
        assert!(record != RecordEnum::Mbp1(records[1].clone()));

        // Cleanup
        for id in ids {
            delete_instrument(id).await.expect("Error on delete");
        }

        Ok(())
    }
}
