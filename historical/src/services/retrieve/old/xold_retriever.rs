use super::heap::MinHeap;
use super::mutex_cursor::MutexCursor;
use crate::database::read::common::{RecordsQuery, RollingWindow};
use crate::database::read::rows::get_from_row_fn;
use crate::response::ApiResponse;
use crate::services::utils::query_symbols_map;
use crate::{Error, Result};
use async_stream::stream;
use axum::http::StatusCode;
use bytes::Bytes;
use futures::stream::Stream;
use futures::stream::StreamExt;
use mbinary::encode::AsyncRecordEncoder;
use mbinary::encode::MetadataEncoder;
use mbinary::enums::{RType, Stype};
use mbinary::metadata::Metadata;
use mbinary::params::RetrieveParams;
use mbinary::record_ref::RecordRef;
use mbinary::records::Record;
use mbinary::{record_enum::RecordEnum, symbols::SymbolMap};
use sqlx::{PgPool, Row};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::io::Cursor;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{error, info};

#[derive(Debug, PartialEq, Eq, Hash)]
pub enum ContinuousKind {
    Volume,
    Calendar,
}

impl ContinuousKind {
    fn from_str(s: &str) -> Result<Self> {
        match s {
            "c" => return Ok(ContinuousKind::Calendar),
            "v" => return Ok(ContinuousKind::Volume),
            _ => return Err(Error::CustomError("Invalid Continuous type. ".to_string())),
        }
    }

    pub fn as_str(&self) -> String {
        match self {
            ContinuousKind::Calendar => return "c".to_string(),
            ContinuousKind::Volume => return "v".to_string(),
        }
    }
}

impl std::fmt::Display for ContinuousKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ContinuousKind::Calendar => write!(f, "c"),
            ContinuousKind::Volume => write!(f, "v"),
        }
    }
}

pub struct Records {
    record: RecordEnum,
}

impl PartialEq for Records {
    fn eq(&self, other: &Self) -> bool {
        self.record.timestamp() == other.record.timestamp()
    }
}

impl Eq for Records {}

impl PartialOrd for Records {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Records {
    fn cmp(&self, other: &Self) -> Ordering {
        self.record.timestamp().cmp(&other.record.timestamp())
    }
}

pub struct RecordGetter {
    batch_size: i64,
    end_records: Arc<Mutex<bool>>,
    batch_counter: Arc<Mutex<i64>>,
    retrieve_params: Arc<Mutex<RetrieveParams>>,
    symbol_map: Arc<Mutex<SymbolMap>>,
    encoder: Arc<Mutex<AsyncRecordEncoder<MutexCursor>>>,
    heap: Arc<Mutex<MinHeap<Records>>>,
    id_map: Arc<Mutex<HashMap<String, u32>>>,
    type_map: Arc<Mutex<HashMap<ContinuousKind, HashMap<i32, Vec<String>>>>>,
    continuous_ticker_map: Arc<Mutex<HashMap<String, HashMap<String, Vec<RollingWindow>>>>>,
    cursor: Arc<Mutex<Cursor<Vec<u8>>>>,
    pool: PgPool,
}

impl RecordGetter {
    pub async fn new(batch_size: i64, params: RetrieveParams, pool: PgPool) -> Result<Self> {
        let cursor = Arc::new(Mutex::new(Cursor::new(Vec::new())));

        // Wrap the cursor in MutexCursor for compatibility with RecordEncoder
        let writer = MutexCursor::new(Arc::clone(&cursor));
        let encoder = AsyncRecordEncoder::new(writer);

        let mut getter = RecordGetter {
            batch_size,
            end_records: Arc::new(Mutex::new(false)),
            batch_counter: Arc::new(Mutex::new(0)),
            retrieve_params: Arc::new(Mutex::new(params)),
            symbol_map: Arc::new(Mutex::new(SymbolMap::new())),
            encoder: Arc::new(Mutex::new(encoder)), // Shared encoder
            heap: Arc::new(Mutex::new(MinHeap::new())),
            id_map: Arc::new(Mutex::new(HashMap::new())),
            type_map: Arc::new(Mutex::new(HashMap::new())),
            continuous_ticker_map: Arc::new(Mutex::new(HashMap::new())),
            cursor, // Shared cursor
            pool,
        };

        getter.initialize().await?;

        Ok(getter)
    }

    pub async fn initialize_task(&mut self) -> Result<()> {
        let params: RetrieveParams = self.retrieve_params.lock().await.clone();

        match params.stype {
            Stype::Raw => self.tasks.lock().await.insert()

        
        Ok(())
    }

    pub async fn initialize(&mut self) -> Result<()> {
        let params: RetrieveParams = self.retrieve_params.lock().await.clone();

        match params.stype {
            Stype::Raw => {
                let symbol_map =
                    query_symbols_map(&self.pool, &params.symbols, params.dataset).await?;
                *self.symbol_map.lock().await = symbol_map;
            }
            Stype::Continuous => {
                // Build type map and synthetic symbol map for continuous contracts
                self.build_type_map(params.symbols.clone()).await?;
                self.build_synthetic_symbol_map().await;

                // Build continuous ticker map
                self.build_continuous_map(params.start_ts, params.end_ts)
                    .await?;
            }
        }
        Ok(())
    }

    pub async fn build_type_map(&mut self, tickers: Vec<String>) -> Result<()> {
        let mut type_map = self.type_map.lock().await;

        for ticker in tickers {
            // Parse the ticker into components
            if let Some((_prefix, rest)) = ticker.split_once('.') {
                if let Some((kind, rank_str)) = rest.split_once('.') {
                    // Parse rank as integer
                    if let Ok(rank) = rank_str.parse::<i32>() {
                        // Insert into the nested HashMap
                        type_map
                            .entry(ContinuousKind::from_str(kind)?)
                            .or_insert_with(HashMap::new)
                            .entry(rank)
                            .or_insert_with(Vec::new)
                            .push(ticker);
                    }
                }
            }
        }
        Ok(())
    }

    /// Builds the synthetic symbol map and updates the `id_map`.
    async fn build_synthetic_symbol_map(&mut self) {
        let retrieve_params = self.retrieve_params.lock().await.clone();

        for (index, ticker) in retrieve_params.symbols.iter().enumerate() {
            let synthetic_id = 1_000_000 + index as u32; // Generate synthetic ID
            self.update_id_map(ticker, synthetic_id).await;
            self.symbol_map
                .lock()
                .await
                .add_instrument(&ticker, synthetic_id);
        }
    }

    /// Updates `id_map` with a given ticker and ID
    pub async fn update_id_map(&mut self, ticker: &str, id: u32) {
        let mut id_map = self.id_map.lock().await;
        id_map.insert(ticker.to_string(), id);
    }

    pub async fn build_continuous_map(&mut self, start: i64, end: i64) -> Result<()> {
        let type_map = self.type_map.lock().await;

        for (kind, rank_map) in type_map.iter() {
            for (rank, tickers) in rank_map {
                let symbols = tickers
                    .iter()
                    .map(|s| s.split('.').next().unwrap_or(s).to_string() + "%")
                    .collect();

                let key = format!("{}.{}", kind.as_str(), rank);
                let v = RollingWindow::retrieve_continuous_window(
                    &self.pool, start, end, *rank, symbols, &kind,
                )
                .await?;
                self.continuous_ticker_map.lock().await.insert(key, v);
            }
        }
        Ok(())
    }

    /// Retrieves the ID from `id_map` given a full ticker
    pub async fn get_id(&self, ticker: &str) -> Option<u32> {
        let id_map = self.id_map.lock().await;

        return id_map.get(ticker).copied();
    }

    // -- Raw symbols
    pub async fn process_metadata(&self) -> Result<Cursor<Vec<u8>>> {
        let mut metadata_cursor = Cursor::new(Vec::new());
        let mut metadata_encoder = MetadataEncoder::new(&mut metadata_cursor);

        let retrieve_params: RetrieveParams = self.retrieve_params.lock().await.clone();

        // let symbol_map = query_symbols_map(
        //     &self.pool,
        //     &retrieve_params.symbols,
        //     retrieve_params.dataset,
        // )
        // .await?;

        let metadata = Metadata::new(
            retrieve_params.schema,
            retrieve_params.dataset,
            retrieve_params.start_ts as u64,
            retrieve_params.end_ts as u64,
            self.symbol_map.lock().await.clone(),
        );
        metadata_encoder.encode_metadata(&metadata)?;

        Ok(metadata_cursor)
    }

    pub async fn get_records(&self, params: &RetrieveParams) -> Result<()> {
        let rtype = RType::from(params.rtype().unwrap());
        let from_row_fn = get_from_row_fn(rtype);

        let mut cursor =
            RecordEnum::retrieve_query(&self.pool, params, false, "".to_string()).await?;
        // info!("Processing queried records.");

        while let Some(row_result) = cursor.next().await {
            match row_result {
                Ok(row) => {
                    // Use the from_row_fn here
                    let record = from_row_fn(&row, None)?;

                    // Convert to RecordEnum and add to encoder
                    let record_ref = record.to_record_ref();
                    self.encoder
                        .lock()
                        .await
                        .encode_record(&record_ref)
                        .await
                        .unwrap();

                    // Increment the batch counter
                    *self.batch_counter.lock().await += 1;
                }
                Err(e) => {
                    error!("Error processing row: {:?}", e);
                    return Err(e.into());
                }
            }
        }
        Ok(())
    }

    pub async fn process_records(self: Arc<Self>) -> Result<()> {
        // Clone the parameters to avoid locking or mutability issues
        let mut params = self.retrieve_params.lock().await.clone();

        // Adjust start/ end to be star  end of repective interval
        let _ = params.interval_adjust_ts_start()?;
        let _ = params.interval_adjust_ts_end()?;
        let final_end = params.end_ts;

        while (final_end - params.start_ts) > 86_400_000_000_001 {
            params.end_ts = params.start_ts + 86_400_000_000_000;
            self.get_records(&params).await?;
            params.start_ts = params.end_ts;
        }

        params.end_ts = final_end;
        self.get_records(&params).await?;

        *self.end_records.lock().await = true;

        Ok(())
    }

    pub async fn stream_rawsymbols(
        self: Arc<Self>,
    ) -> Pin<Box<dyn Stream<Item = Result<Bytes>> + Send>> {
        let p_stream = stream! {
            // Stream metadata first
            match self.process_metadata().await {
                Ok(metadata_cursor) => {
                    let buffer_ref = metadata_cursor.get_ref();
                    let bytes = Bytes::copy_from_slice(buffer_ref);
                    yield Ok::<Bytes, Error>(bytes);
                }
                Err(e) => {
                    let response = ApiResponse::new(
                        "failed",
                        &format!("{:?}", e),
                        StatusCode::CONFLICT,
                        "".to_string(),
                    );
                    yield Ok(response.bytes());
                    return;
                }
            };

            // Spawn record processing
            let record_getter = Arc::clone(&self);
            let _records_processing = tokio::spawn(async move {
                record_getter.process_records().await
            });

            // Stream record batches while processing continues
            loop {
                let end_records = self.end_records.lock().await;
                if *end_records {
                    break;
                }

                let mut batch_counter = self.batch_counter.lock().await;
                if *batch_counter > self.batch_size {
                    let batch_bytes = {
                        self.encoder.lock()
                                .await
                                .flush()
                                .await
                                .unwrap();

                        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await; // Ensure async write settles
                        let cursor = self.cursor.lock().await;
                        Bytes::copy_from_slice(cursor.get_ref())
                    };

                    yield Ok::<Bytes, Error>(batch_bytes);

                    // Reset cursor and batch counter
                    {
                        let mut cursor = self.cursor.lock().await;
                        cursor.get_mut().clear();
                        cursor.set_position(0);
                    }
                    *batch_counter = 0;
                }

                drop(batch_counter);
                drop(end_records);

                tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;
            }

            // Send any remaining data that wasn't part of a full batch
            if !self.cursor.lock().await.get_ref().is_empty() {
                let remaining_bytes = Bytes::copy_from_slice(self.cursor.lock().await.get_ref());
                yield Ok::<Bytes, Error>(remaining_bytes);
            }

            info!("Finished streaming all batches");
            return;
        };

        Box::pin(p_stream)
    }

    // -- Continuous
    pub async fn get_records_continuous(&self, group: &str, params: RetrieveParams) -> Result<()> {
        let rtype = RType::from(params.rtype().unwrap());
        let from_row_fn = get_from_row_fn(rtype);

        let mut cursor =
            RecordEnum::retrieve_query(&self.pool, &params, true, group.to_string()).await?;

        while let Some(row_result) = cursor.next().await {
            match row_result {
                Ok(row) => {
                    let ticker: String = row.try_get("ticker")?;
                    let new_id = self.get_id(&ticker).await;

                    // Use the from_row_fn here
                    let record = Records {
                        record: from_row_fn(&row, new_id)?,
                    };
                    self.heap.lock().await.push(record);

                    // // Convert to RecordEnum and add to encoder
                    // let record_ref = record.to_record_ref();
                    // self.encoder
                    //     .lock()
                    //     .await
                    //     .encode_record(&record_ref)
                    //     .await
                    //     .unwrap();

                    // Increment the batch counter
                    // *self.batch_counter.lock().await += 1;
                }
                Err(e) => {
                    error!("Error processing row: {:?}", e);
                    return Err(e.into());
                }
            }
        }
        Ok(())
    }

    pub async fn task_continuous(
        &self,
        group: String,
        mut map: HashMap<String, Vec<RollingWindow>>,
    ) -> Result<()> {
        let mut params = self.retrieve_params.lock().await.clone();
        let final_end = params.end_ts;

        let mut current_tickers = HashMap::new();
        for (ticker, obj) in &mut map {
            if let Some(value) = obj.pop() {
                current_tickers.insert(ticker.clone(), value);
            }
        }

        // Wait for all tasks to finish
        while (final_end - params.start_ts) > 86_400_000_000_001 {
            params.end_ts = params.start_ts + 86_400_000_000_000;

            let mut to_remove = Vec::new();

            for (ticker, obj) in current_tickers.iter_mut() {
                if obj.end_time < params.start_ts {
                    if let Some(new_value) = map.get_mut(ticker).and_then(|v| v.pop()) {
                        *obj = new_value;
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
            params.symbols = current_tickers
                .values()
                .map(|obj| obj.ticker.clone()) // Extracts ticker field
                .collect::<Vec<String>>();

            self.get_records_continuous(&group, params.clone()).await?;

            params.start_ts = params.end_ts;
        }

        params.end_ts = final_end;
        self.get_records_continuous(&group, params.clone()).await?;

        *self.end_records.lock().await = true;

        Ok(())
    }

    pub async fn process_continuous_records(self: Arc<Self>) -> Result<()> {
        // Clone the parameters to avoid locking or mutability issues
        let mut params = self.retrieve_params.lock().await;
        let _ = params.interval_adjust_ts_start()?;
        let _ = params.interval_adjust_ts_end()?;
        drop(params);

        let map = self.continuous_ticker_map.lock().await.clone();

        // let rolling_schedule: Vec<RollingWindow> = RollingWindow::
        // let rtype = RType::from(params.rtype().unwrap());

        let mut tasks = Vec::new();

        for (group, map) in map {
            let clone_arc = Arc::clone(&self);
            // let heap_clone = heap.clone();
            tasks.push(tokio::spawn(async move {
                clone_arc.task_continuous(group, map).await;
            }));
        }

        // // Spawn heap processing task
        // let processing_task = task::spawn(async move {
        //     process_heap(heap.clone()).await;
        // });

        // Wait for all tasks to finish
        futures::future::join_all(tasks).await;
        // processing_task.await.unwrap();
        // let from_row_fn = get_from_row_fn(rtype);

        *self.end_records.lock().await = true;

        Ok(())
    }

    // pub async fn process_continuous_records(self: Arc<Self>) -> Result<()> {
    //     // Clone the parameters to avoid locking or mutability issues
    //     let mut params = self.retrieve_params.lock().await.clone();
    //     // let rolling_schedule: Vec<RollingWindow> = RollingWindow::
    //     // let rtype = RType::from(params.rtype().unwrap());
    //
    //     // let from_row_fn = get_from_row_fn(rtype);
    //
    //     let _ = params.interval_adjust_ts_start()?;
    //     let _ = params.interval_adjust_ts_end()?;
    //     let final_end = params.end_ts;
    //
    //     while (final_end - params.start_ts) > 86_400_000_000_001 {
    //         params.end_ts = params.start_ts + 86_400_000_000_000;
    //         self.get_records_continuous(params.clone()).await?;
    //         params.start_ts = params.end_ts;
    //     }
    //
    //     params.end_ts = final_end;
    //     self.get_records_continuous(params.clone()).await?;
    //
    //     *self.end_records.lock().await = true;
    //
    //     Ok(())
    // }

    pub async fn stream_continuous(
        self: Arc<Self>,
    ) -> Pin<Box<dyn Stream<Item = Result<Bytes>> + Send>> {
        let p_stream = stream! {
            // Pull the symbols for mutation

            // Stream metadata first
            match self.process_metadata().await {
                Ok(metadata_cursor) => {
                    let buffer_ref = metadata_cursor.get_ref();
                    let bytes = Bytes::copy_from_slice(buffer_ref);
                    yield Ok::<Bytes, Error>(bytes);
                }
                Err(e) => {
                    let response = ApiResponse::new(
                        "failed",
                        &format!("{:?}", e),
                        StatusCode::CONFLICT,
                        "".to_string(),
                    );
                    yield Ok(response.bytes());
                    return;
                }
            };

            // Spawn record processing
            let record_getter = Arc::clone(&self);
            let _records_processing = tokio::spawn(async move {
                record_getter.process_continuous_records().await
            });

            // Stream record batches while processing continues
            loop {
                let end_records = self.end_records.lock().await;
                if *end_records {
                    break;
                }

                let mut batch_counter = self.batch_counter.lock().await;
                if *batch_counter > self.batch_size {
                    let batch_bytes = {
                        self.encoder.lock()
                                .await
                                .flush()
                                .await
                                .unwrap();

                        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await; // Ensure async write settles


                        let cursor = self.cursor.lock().await;
                        Bytes::copy_from_slice(cursor.get_ref())
                    };

                    info!("Sending buffer, size: {:?}", batch_bytes.len());
                    yield Ok::<Bytes, Error>(batch_bytes);

                    // Reset cursor and batch counter
                    {
                        let mut cursor = self.cursor.lock().await;
                        cursor.get_mut().clear();
                        cursor.set_position(0);
                    }
                    *batch_counter = 0;
                }

                drop(batch_counter);
                drop(end_records);

                tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
            }

            // Send any remaining data that wasn't part of a full batch
            if !self.cursor.lock().await.get_ref().is_empty() {
                let remaining_bytes = Bytes::copy_from_slice(self.cursor.lock().await.get_ref());
                info!("Sending remaining buffer, size: {:?}", remaining_bytes.len());
                yield Ok::<Bytes, Error>(remaining_bytes);
            }

            info!("Finished streaming all batches");
            return;
        };

        Box::pin(p_stream)
    }

    pub async fn stream(self: Arc<Self>) -> Pin<Box<dyn Stream<Item = Result<Bytes>> + Send>> {
        if self.retrieve_params.lock().await.stype == Stype::Continuous {
            self.stream_continuous().await
        } else {
            self.stream_rawsymbols().await
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::database::init::init_db;
    use dotenv;
    use mbinary::decode::MetadataDecoder;
    use mbinary::enums::Dataset;
    use mbinary::enums::{Schema, Stype};
    use mbinary::symbols::Instrument;
    use mbinary::vendors::Vendors;
    use mbinary::vendors::{DatabentoData, VendorData};
    use serial_test::serial;
    use sqlx::postgres::PgPoolOptions;
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
            INSERT INTO {} (instrument_id, ticker, name, vendor,vendor_data, last_available, first_available, active)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
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

    #[sqlx::test]
    #[serial]
    async fn test_record_getter_process_metadata() -> anyhow::Result<()> {
        dotenv::dotenv().ok();
        let pool = init_db().await.unwrap();

        let schema = dbn::Schema::from_str("mbp-1")?;
        let dbn_dataset = dbn::Dataset::from_str("XNAS.ITCH")?;
        let stype = dbn::SType::from_str("raw_symbol")?;
        let vendor_data = VendorData::Databento(DatabentoData {
            schema,
            dataset: dbn_dataset,
            stype,
        });

        let mut ids = Vec::new();
        let tickers = vec![
            "ZC.n.0".to_string(),
            "GF.n.0".to_string(),
            "LE.n.0".to_string(),
            "ZS.n.0".to_string(),
            "ZL.n.0".to_string(),
            "ZM.n.0".to_string(),
            "HE.n.0".to_string(),
            "CL.n.0".to_string(),
            // "CU.n.0".to_string(),
        ];

        let dataset = Dataset::Equities;
        let name = "Apple Inc.";

        for ticker in &tickers {
            let instrument = Instrument::new(
                None,
                ticker,
                name,
                dataset.clone(),
                Vendors::Databento,
                vendor_data.encode(),
                1704672000000000000,
                1704672000000000000,
                0,
                true,
            );
            let id = create_instrument(instrument).await?;
            ids.push(id);
        }

        // Test
        let params = RetrieveParams {
            symbols: tickers,
            start_ts: 1704209103644092563,
            end_ts: 1704209903644092569,
            schema: Schema::Mbp1,
            dataset,
            stype: Stype::Raw,
        };
        let getter = RecordGetter::new(1000, params, pool.clone()).await?;
        let mut metadata_cursor = getter.process_metadata().await?;
        metadata_cursor.set_position(0);

        // Validate
        let mut decoded_metadata = MetadataDecoder::new(metadata_cursor);
        let metadata = decoded_metadata.decode()?.unwrap();
        assert_eq!(Schema::Mbp1, metadata.schema);
        assert_eq!(1704209103644092563, metadata.start);
        assert_eq!(1704209903644092569, metadata.end);
        assert_eq!(8, metadata.mappings.map.len());

        // Cleanup
        for id in ids {
            delete_instrument(id).await.expect("Error on delete");
        }

        Ok(())
    }
    //
    // #[sqlx::test]
    // #[serial]
    // async fn test_continuous_map_type_map() -> anyhow::Result<()> {
    //     dotenv::dotenv().ok();
    //
    //     let tickers = vec![
    //         "HE.c.1".to_string(),
    //         "HE.c.0".to_string(),
    //         "LE.c.1".to_string(),
    //         "LE.c.0".to_string(),
    //     ];
    //
    //     // Test
    //     let mut c_map = ContinuousMap::new();
    //     c_map.build_type_map(tickers);
    //
    //     // Validate
    //     let expected = HashMap::from([(
    //         // "c".to_string(),
    //         ContinuousKind::Calendar,
    //         HashMap::from([
    //             (1, vec!["HE.c.1".to_string(), "LE.c.1".to_string()]),
    //             (0, vec!["HE.c.0".to_string(), "LE.c.0".to_string()]),
    //         ]),
    //     )]);
    //     assert_eq!(expected, c_map.type_map);
    //
    //     Ok(())
    // }
    //
    // #[sqlx::test]
    // #[serial]
    // async fn test_continuous_update_id_map() -> anyhow::Result<()> {
    //     dotenv::dotenv().ok();
    //
    //     let tickers = vec![
    //         "HE.c.1".to_string(),
    //         "HE.c.0".to_string(),
    //         "LE.c.1".to_string(),
    //         "LE.c.0".to_string(),
    //     ];
    //
    //     // Test
    //     let mut c_map = ContinuousMap::new();
    //     c_map.build_type_map(tickers.clone());
    //
    //     // Dynamically generate synthetic symbol_map for continuous contracts
    //     for (index, ticker) in tickers.iter().enumerate() {
    //         let synthetic_id = 1_000_000 + index as u32; // Generate synthetic ID
    //         c_map.update_id_map(ticker, synthetic_id);
    //     }
    //
    //     // Validate
    //     let expected = HashMap::from([
    //         (
    //             1,
    //             HashMap::from([("HE".to_string(), 1000000), ("LE".to_string(), 1000002)]),
    //         ),
    //         (
    //             0,
    //             HashMap::from([("HE".to_string(), 1000001), ("LE".to_string(), 1000003)]),
    //         ),
    //     ]);
    //
    //     assert_eq!(expected, c_map.id_map);
    //
    //     Ok(())
    // }
    //
    // #[sqlx::test]
    // #[serial]
    //
    // async fn test_id_map() -> anyhow::Result<()> {
    //     let tickers = vec![
    //         "HE.c.1".to_string(),
    //         "HE.c.0".to_string(),
    //         "LE.c.1".to_string(),
    //         "LE.c.0".to_string(),
    //     ];
    //
    //     // Test
    //     let mut c_map = ContinuousMap::new();
    //     c_map.build_type_map(tickers.clone());
    //
    //     // Dynamically generate synthetic symbol_map for continuous contracts
    //     for (index, ticker) in tickers.iter().enumerate() {
    //         let synthetic_id = 1_000_000 + index as u32; // Generate synthetic ID
    //         c_map.update_id_map(ticker, synthetic_id);
    //     }
    //
    //     assert_eq!(c_map.get_id("HEG4", 0).unwrap(), 1000001);
    //     assert_eq!(c_map.get_id("LEG4", 1).unwrap(), 1000002);
    //
    //     Ok(())
    // }
}
