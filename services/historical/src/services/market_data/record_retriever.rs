use crate::database::market_data::read::{get_from_row_fn, RecordsQuery};
use crate::database::symbols::query_symbols_map;
use crate::response::ApiResponse;
use crate::Error;
use crate::{database::market_data::read::RetrieveParams, Result};
use async_stream::stream;
use axum::http::StatusCode;
use bytes::Bytes;
use futures::stream::Stream;
use futures::stream::StreamExt;
use mbn::encode::AsyncRecordEncoder;
use mbn::encode::MetadataEncoder;
use mbn::enums::{RType, Schema};
use mbn::metadata::Metadata;
use mbn::{record_enum::RecordEnum, symbols::SymbolMap};
use sqlx::{PgPool, Row};
use std::io::Cursor;
use std::io::{self};
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::io::AsyncWrite;
use tokio::sync::Mutex;
use tracing::{error, info};

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
            cursor, // Shared cursor
            pool,
            encoder: Arc::new(Mutex::new(encoder)), // Shared encoder
        })
    }

    pub async fn process_metadata(&self) -> Result<Cursor<Vec<u8>>> {
        let mut metadata_cursor = Cursor::new(Vec::new());
        let mut metadata_encoder = MetadataEncoder::new(&mut metadata_cursor);

        let retrieve_params = self.retrieve_params.lock().await.clone();

        let symbol_map = query_symbols_map(&self.pool, &retrieve_params.symbols).await?;

        let metadata = Metadata::new(
            Schema::from_str(&retrieve_params.schema).unwrap(),
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
                    let instrument_id = row.try_get::<i32, _>("instrument_id")? as u32;
                    let ticker: String = row.try_get("ticker")?;

                    // Use the from_row_fn here
                    let record = from_row_fn(&row)?;

                    // Convert to RecordEnum and add to encoder
                    let record_ref = record.to_record_ref();
                    self.encoder
                        .lock()
                        .await
                        .encode_record(&record_ref)
                        .await
                        .unwrap();

                    // Update symbol map
                    self.symbol_map
                        .lock()
                        .await
                        .add_instrument(&ticker, instrument_id);

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
            yield Ok(Bytes::from("Finished streaming all batches"));
            return;
        };

        Box::pin(p_stream)
    }
}
