use super::heap::MinHeap;
use super::mutex_cursor::MutexCursor;
use super::query_task::QueryTask;
use crate::response::ApiResponse;
use crate::{Error, Result};
use async_stream::stream;
use axum::http::StatusCode;
use bytes::Bytes;
use futures::stream::Stream;
use mbinary::encode::AsyncRecordEncoder;
use mbinary::encode::MetadataEncoder;
use mbinary::enums::Stype;
use mbinary::metadata::Metadata;
use mbinary::params::RetrieveParams;
use mbinary::records::Record;
use mbinary::{record_enum::RecordEnum, symbols::SymbolMap};
use sqlx::PgPool;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::io::Cursor;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::info;

#[derive(Debug, PartialEq, Eq, Hash)]
pub enum ContinuousKind {
    Volume,
    Calendar,
    None,
}

impl ContinuousKind {
    fn from_str(s: &str) -> Result<Self> {
        match s {
            "c" => return Ok(ContinuousKind::Calendar),
            "v" => return Ok(ContinuousKind::Volume),
            "n" => return Ok(ContinuousKind::None),
            _ => return Err(Error::CustomError("Invalid Continuous type. ".to_string())),
        }
    }

    pub fn as_str(&self) -> String {
        match self {
            ContinuousKind::Calendar => return "c".to_string(),
            ContinuousKind::Volume => return "v".to_string(),
            ContinuousKind::None => return "v".to_string(),
        }
    }
}

impl std::fmt::Display for ContinuousKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ContinuousKind::Calendar => write!(f, "c"),
            ContinuousKind::Volume => write!(f, "v"),
            ContinuousKind::None => write!(f, "n"),
        }
    }
}

#[derive(Debug)]
pub struct Records {
    pub record: RecordEnum,
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
    end_tasks: Arc<Mutex<bool>>,
    batch_counter: Arc<Mutex<i64>>,
    retrieve_params: Arc<Mutex<RetrieveParams>>,
    symbol_map: Arc<Mutex<SymbolMap>>,
    encoder: Arc<Mutex<AsyncRecordEncoder<MutexCursor>>>,
    tasks: Arc<Mutex<HashMap<String, Arc<Mutex<QueryTask>>>>>,
    tasks_query_flag: Arc<Mutex<HashMap<String, Arc<Mutex<bool>>>>>,
    heap: Arc<Mutex<MinHeap<Records>>>,
    heap_process_flag: Arc<Mutex<bool>>,
    cursor: Arc<Mutex<Cursor<Vec<u8>>>>,
    pool: Arc<PgPool>,
}

impl RecordGetter {
    pub async fn new(batch_size: i64, params: RetrieveParams, pool: Arc<PgPool>) -> Result<Self> {
        let cursor = Arc::new(Mutex::new(Cursor::new(Vec::new())));

        // Wrap the cursor in MutexCursor for compatibility with RecordEncoder
        let writer = MutexCursor::new(Arc::clone(&cursor));
        let encoder = AsyncRecordEncoder::new(writer);

        let mut getter = RecordGetter {
            batch_size,
            end_records: Arc::new(Mutex::new(false)),
            end_tasks: Arc::new(Mutex::new(false)),
            batch_counter: Arc::new(Mutex::new(0)),
            retrieve_params: Arc::new(Mutex::new(params)),
            symbol_map: Arc::new(Mutex::new(SymbolMap::new())),
            encoder: Arc::new(Mutex::new(encoder)),
            tasks: Arc::new(Mutex::new(HashMap::new())),
            tasks_query_flag: Arc::new(Mutex::new(HashMap::new())),
            heap: Arc::new(Mutex::new(MinHeap::new())),
            heap_process_flag: Arc::new(Mutex::new(false)),
            cursor,
            pool,
        };

        getter.initialize_tasks().await?;

        Ok(getter)
    }

    pub async fn initialize_tasks(&mut self) -> Result<()> {
        let params = self.retrieve_params.lock().await.clone();
        let mut tasks = self.tasks.lock().await;
        let mut tasks_query_flag = self.tasks_query_flag.lock().await;

        match params.stype {
            Stype::Raw => {
                // Only one task for raw
                let task_id = "raw".to_string();
                let tickers = params.symbols.clone();
                let query_flag = Arc::new(Mutex::new(false));

                let task = QueryTask::new(
                    task_id.clone(),
                    0,
                    params.clone(),
                    tickers,
                    0,
                    ContinuousKind::None,
                    self.pool.clone(),
                    Arc::clone(&self.heap),
                    query_flag.clone(),
                )
                .await?;

                tasks.insert(task_id.clone(), Arc::new(Mutex::new(task)));
                tasks_query_flag.insert(task_id, query_flag);
            }
            Stype::Continuous => {
                let mut initial_continuous_id: usize = 1_000_000;
                let mut ticker_map: HashMap<String, Vec<String>> = HashMap::new();

                for ticker in &params.symbols {
                    if let Some((_base, task_id)) = ticker.split_once('.') {
                        // Group tickers by "c.X" (task_id)
                        ticker_map
                            .entry(task_id.to_string())
                            .or_insert_with(Vec::new)
                            .push(ticker.clone());
                    }
                }

                // Create QueryTasks for each c.X group
                for (task_id, tickers) in ticker_map {
                    if let Some((kind, rank_str)) = task_id.split_once('.') {
                        if let Ok(rank) = rank_str.parse::<i32>() {
                            let num_tickers = tickers.len();
                            let query_flag = Arc::new(Mutex::new(false));

                            // Insert into the nested HashMap
                            let task = QueryTask::new(
                                task_id.clone(),
                                initial_continuous_id as u32,
                                params.clone(),
                                tickers,
                                rank,
                                ContinuousKind::from_str(kind)?,
                                self.pool.clone(),
                                Arc::clone(&self.heap),
                                query_flag.clone(),
                            )
                            .await?;

                            tasks.insert(task_id.clone(), Arc::new(Mutex::new(task)));
                            tasks_query_flag.insert(task_id, query_flag);
                            initial_continuous_id += num_tickers;
                        }
                    }
                }
            }
        }

        Ok(())
    }

    pub async fn process_metadata(&self) -> Result<Cursor<Vec<u8>>> {
        let mut metadata_cursor = Cursor::new(Vec::new());
        let mut metadata_encoder = MetadataEncoder::new(&mut metadata_cursor);

        let retrieve_params: RetrieveParams = self.retrieve_params.lock().await.clone();

        let tasks = self.tasks.lock().await;

        for task in tasks.values() {
            let task = task.lock().await;

            // Merge symbol maps
            self.symbol_map.lock().await.merge(&task.symbol_map);
        }

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

    pub async fn process_heap(self: Arc<Self>) -> Result<()> {
        loop {
            if !*self.heap_process_flag.lock().await {
                tokio::task::yield_now().await;
            } else {
                let heap = self.heap.lock().await;

                if heap.is_empty() {
                    if *self.end_tasks.lock().await {
                        drop(heap);
                        *self.end_records.lock().await = true;
                        break;
                    } else {
                        let flags = self.tasks_query_flag.lock().await;

                        for (_, flag) in flags.iter() {
                            *flag.lock().await = false;
                        }
                        *self.heap_process_flag.lock().await = false;
                    }
                }
                drop(heap);

                // Pop the record with the smallest timestamp (heap behavior)
                let record = {
                    let mut heap = self.heap.lock().await;
                    heap.pop()
                };

                if let Some(item) = record {
                    let record_ref = item.record.to_record_ref();
                    self.encoder.lock().await.encode_record(&record_ref).await?;

                    // Increment batch counter
                    *self.batch_counter.lock().await += 1;
                }
            }
        }
        Ok(())
    }

    pub async fn stream(self: Arc<Self>) -> Pin<Box<dyn Stream<Item = Result<Bytes>> + Send>> {
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

            // Spawn task processes
            let mut handles = Vec::new();
            let tasks = self.tasks.lock().await;

            for task in tasks.values() {
                let task_arc = Arc::clone(task);

                let handle = tokio::spawn(async move {
                    let mut task = task_arc.lock().await;
                    let _= task.process_records().await;
                });
                handles.push(handle);
            }

            // Spawn query manager
            let query_handler = Arc::clone(&self);
            let _query_manager = tokio::spawn(async move {
                loop {
                    if !*query_handler.end_tasks.lock().await{
                        let all_queried = {
                            let flags = query_handler.tasks_query_flag.lock().await;
                            let mut done = true;
                            for (_, flag) in flags.iter() {
                                if !*flag.lock().await {
                                    done = false;
                                    break;
                                }
                            }
                            done
                        };

                        // If all tasks  haev queried, set heap_process_flag and process heap
                        if all_queried {
                            // Set heap_process_flag to true, allowing heap to process
                            *query_handler.heap_process_flag.lock().await = true;

                            // Wait until the heap has processed
                            while *query_handler.heap_process_flag.lock().await {
                                tokio::task::yield_now().await;
                            }
                        }
                    } else {
                        *query_handler.heap_process_flag.lock().await = true;
                        break;
                    }
                }
            });


            // Spawn task manager
            let task_handler = Arc::clone(&self);
            let _task_manager = tokio::spawn(async move {

                // Wait for all record-processing tasks
                for handle in handles {
                    let _ = handle.await;
                }

                // Signal that tasks are done
                *task_handler.end_tasks.lock().await = true;
            });

            // Spawn heap process
            let record_getter = Arc::clone(&self);
            let _records_processing = tokio::spawn(async move {
                record_getter.process_heap().await
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
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::historical::services::load::create_record;
    use crate::pool::DatabaseState;
    use crate::response::ApiResponse;
    use axum::response::IntoResponse;
    use axum::{Extension, Json};
    use dotenv;
    use futures::stream::StreamExt;
    use mbinary::decode::{Decoder, MetadataDecoder};
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
    use std::ops::Deref;
    use std::os::raw::c_char;
    use std::str::FromStr;

    async fn create_db_state() -> anyhow::Result<Arc<DatabaseState>> {
        let historical_db_url = std::env::var("HISTORICAL_DATABASE_URL")?;
        let trading_db_url = std::env::var("TRADING_DATABASE_URL")?;

        let state = DatabaseState::new(&historical_db_url, &trading_db_url).await?;
        Ok(Arc::new(state))
    }

    // -- Helper functions
    async fn create_instrument(instrument: Instrument) -> Result<i32> {
        let database_url = std::env::var("HISTORICAL_DATABASE_URL")?;
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
            INSERT INTO {} (instrument_id, ticker, name, vendor,vendor_data, last_available, first_available, expiration_date, active)
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
        let database_url = std::env::var("HISTORICAL_DATABASE_URL")?;
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

        let state = create_db_state().await?;
        let response = create_record(Extension(state.clone()), Json(buffer))
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
        sqlx::query(query)
            .execute(state.historical_pool.deref())
            .await?;

        let query = "REFRESH MATERIALIZED VIEW futures_continuous_volume_windows";
        sqlx::query(query)
            .execute(state.historical_pool.deref())
            .await?;

        Ok(records)
    }

    #[sqlx::test]
    #[serial]
    async fn test_initialize_tasks_continuous() -> anyhow::Result<()> {
        dotenv::dotenv().ok();
        let state = create_db_state().await?;
        let ids = create_futures().await?;

        // Test
        let params = RetrieveParams {
            symbols: vec!["HE.c.0".to_string(), "LE.c.1".to_string()],
            start_ts: 1704209103644092563,
            end_ts: 1704209903644092569,
            schema: Schema::Mbp1,
            dataset: Dataset::Futures,
            stype: Stype::Continuous,
        };
        let getter = RecordGetter::new(1000, params, state.historical_pool.clone()).await?;

        // Validate
        let tasks = getter.tasks.lock().await.keys().len();
        assert_eq!(tasks, 2);

        // Cleanup
        for id in ids {
            delete_instrument(id).await.expect("Error on delete");
        }

        Ok(())
    }

    #[sqlx::test]
    #[serial]
    async fn test_initialize_tasks_raw() -> anyhow::Result<()> {
        dotenv::dotenv().ok();
        let state = create_db_state().await?;
        let ids = create_futures().await?;

        // Test
        let params = RetrieveParams {
            symbols: vec!["HEG4".to_string(), "LEG4".to_string()],
            start_ts: 1704209103644092563,
            end_ts: 1704209903644092569,
            schema: Schema::Mbp1,
            dataset: Dataset::Futures,
            stype: Stype::Raw,
        };
        let getter = RecordGetter::new(1000, params, state.historical_pool.clone()).await?;

        // Validate
        let tasks = getter.tasks.lock().await.keys().len();
        assert_eq!(tasks, 1);

        // Cleanup
        for id in ids {
            delete_instrument(id).await.expect("Error on delete");
        }

        Ok(())
    }

    #[sqlx::test]
    #[serial]
    // #[ignore]
    async fn test_record_getter_process_metadata_continuous() -> anyhow::Result<()> {
        dotenv::dotenv().ok();
        let state = create_db_state().await?;
        let ids = create_futures().await?;
        let _records = create_future_records(&ids).await?;

        // Test
        let params = RetrieveParams {
            symbols: vec!["HE.c.0".to_string(), "Le.c.0".to_string()],
            start_ts: 1704209103644092563,
            end_ts: 1704209903644092569,
            schema: Schema::Mbp1,
            dataset: Dataset::Futures,
            stype: Stype::Continuous,
        };
        let getter = RecordGetter::new(1000, params, state.historical_pool.clone()).await?;

        let mut metadata_cursor = getter.process_metadata().await?;
        metadata_cursor.set_position(0);

        // Validate
        let mut decoded_metadata = MetadataDecoder::new(metadata_cursor);
        let metadata = decoded_metadata.decode()?.unwrap();
        assert_eq!(Schema::Mbp1, metadata.schema);
        assert_eq!(1704209103644092563, metadata.start);
        assert_eq!(1704209903644092569, metadata.end);
        assert_eq!(2, metadata.mappings.map.len());

        // Cleanup
        for id in ids {
            delete_instrument(id).await.expect("Error on delete");
        }

        Ok(())
    }

    #[sqlx::test]
    #[serial]
    // #[ignore]
    async fn test_record_getter_process_metadata_raw() -> anyhow::Result<()> {
        dotenv::dotenv().ok();
        let state = create_db_state().await?;
        let ids = create_futures().await?;
        let _records = create_future_records(&ids).await?;

        // Test
        let params = RetrieveParams {
            symbols: vec!["HEG4".to_string(), "LEG4".to_string()],
            start_ts: 1704209103644092563,
            end_ts: 1704209903644092569,
            schema: Schema::Mbp1,
            dataset: Dataset::Futures,
            stype: Stype::Raw,
        };
        let getter = RecordGetter::new(1000, params, state.historical_pool.clone()).await?;

        let mut metadata_cursor = getter.process_metadata().await?;
        metadata_cursor.set_position(0);

        // Validate
        let mut decoded_metadata = MetadataDecoder::new(metadata_cursor);
        let metadata = decoded_metadata.decode()?.unwrap();
        assert_eq!(Schema::Mbp1, metadata.schema);
        assert_eq!(1704209103644092563, metadata.start);
        assert_eq!(1704209903644092569, metadata.end);
        assert_eq!(2, metadata.mappings.map.len());

        // Cleanup
        for id in ids {
            delete_instrument(id).await.expect("Error on delete");
        }

        Ok(())
    }

    #[sqlx::test]
    #[serial]
    // #[ignore]
    async fn test_stream_records_raw() -> anyhow::Result<()> {
        dotenv::dotenv().ok();
        let state = create_db_state().await?;
        let ids = create_futures().await?;
        let _records = create_future_records(&ids).await?;

        // Test
        let params = RetrieveParams {
            symbols: vec!["HEG4".to_string(), "LEG4".to_string()],
            start_ts: 1704171600000000000,
            end_ts: 1704209903644092569,
            schema: Schema::Mbp1,
            dataset: Dataset::Futures,
            stype: Stype::Raw,
        };
        let loader =
            Arc::new(RecordGetter::new(1000, params, state.historical_pool.clone()).await?);
        let mut progress_stream = loader.stream().await;

        // Collect streamed response
        let mut buffer = Vec::new();

        while let Some(chunk) = progress_stream.next().await {
            match chunk {
                Ok(bytes) => {
                    buffer.extend_from_slice(&bytes);
                }
                Err(e) => panic!("Error while reading chunk: {:?}", e),
            }
        }

        // Validate
        let cursor = Cursor::new(buffer);
        let mut decoder = Decoder::new(cursor)?;
        let decoded_metadata = decoder.metadata().unwrap();
        let records = decoder.decode()?;
        assert_eq!(2, decoded_metadata.mappings.map.len());
        assert!(records.len() > 0);

        // Cleanup
        for id in ids {
            delete_instrument(id).await.expect("Error on delete");
        }

        Ok(())
    }

    #[sqlx::test]
    #[serial]
    // #[ignore]
    async fn test_stream_records_calendar_continuous() -> anyhow::Result<()> {
        dotenv::dotenv().ok();
        let state = create_db_state().await?;
        let ids = create_futures().await?;
        let _records = create_future_records(&ids).await?;

        // Test
        let params = RetrieveParams {
            symbols: vec!["HE.c.0".to_string(), "LE.c.0".to_string()],
            start_ts: 1704171600000000000,
            end_ts: 1704209903644092569,
            schema: Schema::Mbp1,
            dataset: Dataset::Futures,
            stype: Stype::Continuous,
        };
        let loader =
            Arc::new(RecordGetter::new(1000, params, state.historical_pool.clone()).await?);
        let mut progress_stream = loader.stream().await;

        // Collect streamed response
        let mut buffer = Vec::new();

        while let Some(chunk) = progress_stream.next().await {
            match chunk {
                Ok(bytes) => {
                    buffer.extend_from_slice(&bytes);
                }
                Err(e) => panic!("Error while reading chunk: {:?}", e),
            }
        }

        // Validate
        let cursor = Cursor::new(buffer);
        let mut decoder = Decoder::new(cursor)?;
        let decoded_metadata = decoder.metadata().unwrap();
        let records = decoder.decode()?;
        assert_eq!(2, decoded_metadata.mappings.map.len());
        assert!(records.len() > 0);

        // Cleanup
        for id in ids {
            delete_instrument(id).await.expect("Error on delete");
        }

        Ok(())
    }

    #[sqlx::test]
    #[serial]
    // #[ignore]
    async fn test_stream_records_volume_continuous() -> anyhow::Result<()> {
        dotenv::dotenv().ok();
        let state = create_db_state().await?;
        let ids = create_futures().await?;
        let _records = create_future_records(&ids).await?;

        // Test
        let params = RetrieveParams {
            symbols: vec!["HE.v.0".to_string(), "LE.v.0".to_string()],
            start_ts: 1704240000000000000,
            end_ts: 1704250000000000000,
            schema: Schema::Mbp1,
            dataset: Dataset::Futures,
            stype: Stype::Continuous,
        };
        let loader =
            Arc::new(RecordGetter::new(1000, params, state.historical_pool.clone()).await?);
        let mut progress_stream = loader.stream().await;

        // Collect streamed response
        let mut buffer = Vec::new();

        while let Some(chunk) = progress_stream.next().await {
            match chunk {
                Ok(bytes) => {
                    buffer.extend_from_slice(&bytes);
                }
                Err(e) => panic!("Error while reading chunk: {:?}", e),
            }
        }

        // Validate
        let cursor = Cursor::new(buffer);
        let mut decoder = Decoder::new(cursor)?;
        let decoded_metadata = decoder.metadata().unwrap();
        let records = decoder.decode()?;
        assert_eq!(2, decoded_metadata.mappings.map.len());
        assert!(records.len() > 0);

        // Cleanup
        for id in ids {
            delete_instrument(id).await.expect("Error on delete");
        }

        Ok(())
    }
}
