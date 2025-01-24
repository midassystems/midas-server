use crate::database::read::common::RecordsQuery;
use crate::database::read::rows::get_from_row_fn;
use crate::response::ApiResponse;
use crate::services::utils::query_symbols_map;
use crate::{Error, Result};
use async_stream::stream;
use axum::http::StatusCode;
use bytes::Bytes;
use futures::stream::Stream;
use futures::stream::StreamExt;
use mbn::encode::AsyncRecordEncoder;
use mbn::encode::MetadataEncoder;
use mbn::enums::{RType, Stype};
use mbn::metadata::Metadata;
use mbn::params::RetrieveParams;
use mbn::{record_enum::RecordEnum, symbols::SymbolMap};
use sqlx::{PgPool, Row};
use std::collections::HashMap;
use std::io::Cursor;
use std::io::{self};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::io::AsyncWrite;
use tokio::sync::Mutex;
use tracing::{error, info};

pub struct ContinuousMap {
    pub id_map: HashMap<String, u32>,
    pub type_map: HashMap<String, HashMap<i32, Vec<String>>>,
}

impl ContinuousMap {
    pub fn new() -> Self {
        Self {
            type_map: HashMap::new(),
            id_map: HashMap::new(),
        }
    }

    pub fn build_type_map(&mut self, tickers: Vec<String>) {
        for ticker in tickers {
            // Parse the ticker into components
            if let Some((_prefix, rest)) = ticker.split_once('.') {
                if let Some((kind, rank_str)) = rest.split_once('.') {
                    // Parse rank as integer
                    if let Ok(rank) = rank_str.parse::<i32>() {
                        // Insert into the nested HashMap
                        self.type_map
                            .entry(kind.to_string()) // "c" or "v"
                            .or_insert_with(HashMap::new)
                            .entry(rank) // rank (e.g., 1, 2, 3)
                            .or_insert_with(Vec::new)
                            .push(ticker);
                    }
                }
            }
        }
    }
}

pub struct MutexCursor {
    inner: Arc<Mutex<Cursor<Vec<u8>>>>,
}

impl MutexCursor {
    pub fn new(cursor: Arc<Mutex<Cursor<Vec<u8>>>>) -> Self {
        Self { inner: cursor }
    }
}

impl AsyncWrite for MutexCursor {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.inner.try_lock() {
            Ok(mut cursor) => Poll::Ready(std::io::Write::write(&mut *cursor, buf)), // Lock acquired successfully
            Err(_) => {
                // If the lock is not available, return Pending
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.inner.try_lock() {
            Ok(mut cursor) => {
                // Explicitly use the `std::io::Write` trait for `flush`
                Poll::Ready(std::io::Write::flush(&mut *cursor))
            } // Lock acquired successfully
            Err(_) => {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        // No additional shutdown behavior needed for Cursor
        Poll::Ready(Ok(()))
    }
}

pub struct RecordGetter {
    batch_size: i64,
    end_records: Arc<Mutex<bool>>,
    batch_counter: Arc<Mutex<i64>>,
    retrieve_params: Arc<Mutex<RetrieveParams>>,
    symbol_map: Arc<Mutex<SymbolMap>>,
    continuous_map: Arc<Mutex<ContinuousMap>>,
    cursor: Arc<Mutex<Cursor<Vec<u8>>>>,
    pool: PgPool,
    encoder: Arc<Mutex<AsyncRecordEncoder<MutexCursor>>>,
}

impl RecordGetter {
    pub async fn new(batch_size: i64, params: RetrieveParams, pool: PgPool) -> Result<Self> {
        let cursor = Arc::new(Mutex::new(Cursor::new(Vec::new()))); // Wrap in Arc<Mutex>

        // Wrap the cursor in MutexCursor for compatibility with RecordEncoder
        let writer = MutexCursor::new(Arc::clone(&cursor));
        let encoder = AsyncRecordEncoder::new(writer);

        Ok(RecordGetter {
            batch_size,
            end_records: Arc::new(Mutex::new(false)),
            batch_counter: Arc::new(Mutex::new(0)),
            retrieve_params: Arc::new(Mutex::new(params)),
            symbol_map: Arc::new(Mutex::new(SymbolMap::new())),
            continuous_map: Arc::new(Mutex::new(ContinuousMap::new())),
            cursor, // Shared cursor
            pool,
            encoder: Arc::new(Mutex::new(encoder)), // Shared encoder
        })
    }

    // -- Raw Symbols
    pub async fn process_metadata(&self) -> Result<Cursor<Vec<u8>>> {
        let mut metadata_cursor = Cursor::new(Vec::new());
        let mut metadata_encoder = MetadataEncoder::new(&mut metadata_cursor);

        let retrieve_params = self.retrieve_params.lock().await.clone();

        let symbol_map = query_symbols_map(
            &self.pool,
            &retrieve_params.symbols,
            retrieve_params.dataset,
        )
        .await?;

        let metadata = Metadata::new(
            retrieve_params.schema,
            retrieve_params.dataset,
            retrieve_params.start_ts as u64,
            retrieve_params.end_ts as u64,
            symbol_map,
        );
        metadata_encoder.encode_metadata(&metadata)?;

        Ok(metadata_cursor)
    }

    pub async fn process_records(self: Arc<Self>) -> Result<()> {
        // Clone the parameters to avoid locking or mutability issues
        let retrieve_params = self.retrieve_params.lock().await.clone();

        let rtype = RType::from(retrieve_params.rtype().unwrap());
        let from_row_fn = get_from_row_fn(rtype);

        let mut cursor = RecordEnum::retrieve_query(&self.pool, retrieve_params).await?;
        info!("Processing queried records.");

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

    // -- Continuous
    pub async fn process_continuous_metadata(&self) -> Result<Cursor<Vec<u8>>> {
        let mut metadata_cursor = Cursor::new(Vec::new());
        let mut metadata_encoder = MetadataEncoder::new(&mut metadata_cursor);

        let retrieve_params = self.retrieve_params.lock().await.clone();
        let mut symbol_map = self.symbol_map.lock().await;
        let mut continuous_map = self.continuous_map.lock().await;

        // Dynamically generate synthetic symbol_map for continuous contracts
        for (index, ticker) in retrieve_params.symbols.iter().enumerate() {
            let synthetic_id = 1_000_000 + index as u32; // Generate synthetic ID
            symbol_map.add_instrument(&ticker, synthetic_id);
            let prefix = ticker.get(..2).unwrap_or(""); // TODO: Should be an error
            continuous_map
                .id_map
                .insert(prefix.to_string(), synthetic_id);
        }

        // Construct metadata with the synthetic symbol_map
        let metadata = Metadata::new(
            retrieve_params.schema,
            retrieve_params.dataset,
            retrieve_params.start_ts as u64,
            retrieve_params.end_ts as u64,
            symbol_map.clone(), // Pass the synthetic map
        );

        metadata_encoder.encode_metadata(&metadata)?;

        // Return both metadata_cursor and the generated symbol_map
        Ok(metadata_cursor)
    }

    pub async fn process_continuous_records(self: Arc<Self>) -> Result<()> {
        // Clone the parameters to avoid locking or mutability issues
        let mut retrieve_params = self.retrieve_params.lock().await.clone();
        let rtype = RType::from(retrieve_params.rtype().unwrap());

        let from_row_fn = get_from_row_fn(rtype);
        let continuous_map = self.continuous_map.lock().await; // Clone to avoid holding the lock

        for (kind, rank_map) in continuous_map.type_map.iter() {
            let mut count = 0;
            for (rank, tickers) in rank_map {
                info!(
                    "Processing kind: {}, rank: {}, tickers: {:?}",
                    kind, rank, tickers
                );

                retrieve_params.symbols = tickers.clone();

                let mut cursor =
                    RecordEnum::retrieve_query(&self.pool, retrieve_params.clone()).await?;

                while let Some(row_result) = cursor.next().await {
                    match row_result {
                        Ok(row) => {
                            let ticker: String = row.try_get("ticker")?;
                            let prefix = ticker.get(..2).unwrap_or("");
                            let new_id = continuous_map.id_map.get(prefix).copied();

                            // Use the from_row_fn here
                            let record = from_row_fn(&row, new_id)?;

                            // Convert to RecordEnum and add to encoder
                            let record_ref = record.to_record_ref();
                            self.encoder
                                .lock()
                                .await
                                .encode_record(&record_ref)
                                .await
                                .unwrap();
                            count += 1;

                            // Increment the batch counter
                            *self.batch_counter.lock().await += 1;
                        }
                        Err(e) => {
                            error!("Error processing row: {:?}", e);
                            return Err(e.into());
                        }
                    }
                }
                info!("Count of records processed {}", count);
            }
        }
        *self.end_records.lock().await = true;

        Ok(())
    }

    pub async fn stream_continuous(
        self: Arc<Self>,
    ) -> Pin<Box<dyn Stream<Item = Result<Bytes>> + Send>> {
        let p_stream = stream! {
            // Pull the symbols for mutation
            let tickers: Vec<String> = self.retrieve_params.lock().await.symbols.clone();
            self.continuous_map.lock().await.build_type_map(tickers);

            // Stream metadata first
            match self.process_continuous_metadata().await {
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
    //
    // let p_stream = stream! {
    //     // Stream metadata first
    //     match self.process_metadata().await {
    //         Ok(metadata_cursor) => {
    //             let buffer_ref = metadata_cursor.get_ref();
    //             let bytes = Bytes::copy_from_slice(buffer_ref);
    //             yield Ok::<Bytes, Error>(bytes);
    //         }
    //         Err(e) => {
    //             let response = ApiResponse::new(
    //                 "failed",
    //                 &format!("{:?}", e),
    //                 StatusCode::CONFLICT,
    //                 "".to_string(),
    //             );
    //             yield Ok(response.bytes());
    //             return;
    //         }
    //     };
    //
    //     // Spawn record processing
    //     let record_getter = Arc::clone(&self);
    //     let _records_processing = tokio::spawn(async move {
    //         record_getter.process_records().await
    //     });
    //
    //     // Stream record batches while processing continues
    //     loop {
    //         let end_records = self.end_records.lock().await;
    //         if *end_records {
    //             break;
    //         }
    //
    //         let mut batch_counter = self.batch_counter.lock().await;
    //         if *batch_counter > self.batch_size {
    //             let batch_bytes = {
    //                 let cursor = self.cursor.lock().await;
    //                 Bytes::copy_from_slice(cursor.get_ref())
    //             };
    //
    //             info!("Sending buffer, size: {:?}", batch_bytes.len());
    //             yield Ok::<Bytes, Error>(batch_bytes);
    //
    //             // Reset cursor and batch counter
    //             {
    //                 let mut cursor = self.cursor.lock().await;
    //                 cursor.get_mut().clear();
    //                 cursor.set_position(0);
    //             }
    //             *batch_counter = 0;
    //         }
    //
    //         drop(batch_counter);
    //         drop(end_records);
    //
    //         tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    //     }
    //
    //     // Send any remaining data that wasn't part of a full batch
    //     if !self.cursor.lock().await.get_ref().is_empty() {
    //         let remaining_bytes = Bytes::copy_from_slice(self.cursor.lock().await.get_ref());
    //         info!("Sending remaining buffer, size: {:?}", remaining_bytes.len());
    //         yield Ok::<Bytes, Error>(remaining_bytes);
    //     }
    //
    //     info!("Finished streaming all batches");
    //     yield Ok(Bytes::from("Finished streaming all batches"));
    //     return;
    // };
    //
    // Box::pin(p_stream)
    // }
}

#[cfg(test)]
mod test {
    use std::str::FromStr;

    use super::*;
    use crate::database::init::init_db;
    use dotenv;
    use mbn::decode::MetadataDecoder;
    use mbn::enums::Dataset;
    use mbn::enums::{Schema, Stype};
    use mbn::symbols::Instrument;
    use mbn::vendors::Vendors;
    use mbn::vendors::{DatabentoData, VendorData};
    use serial_test::serial;
    use sqlx::postgres::PgPoolOptions;

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
}
