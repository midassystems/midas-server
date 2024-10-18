use crate::database::market_data::{process_records, RecordInsertQueries, RetrieveParams};
use crate::response::ApiResponse;
use crate::{Error, Result};
use async_stream::stream;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{body::StreamBody, Extension, Json, Router};
use bytes::Bytes;
use mbn::decode::RecordDecoder;
use mbn::encode::MetadataEncoder;
use mbn::enums::Schema;
use mbn::metadata::Metadata;
use mbn::{record_enum::RecordEnum, symbols::SymbolMap};
use sqlx::PgPool;
use std::fs::File;
use std::io::BufReader;
use std::io::Cursor;
use std::path::PathBuf;
use std::str::FromStr;
use tracing::{error, info};

// Service
pub fn market_data_service() -> Router {
    Router::new()
        .route("/create", post(create_record))
        .route("/get", get(get_records))
        .route("/bulk_upload", post(bulk_upload))
}

// Handlers
pub async fn create_record(
    Extension(pool): Extension<PgPool>,
    Json(encoded_data): Json<Vec<u8>>,
) -> Result<ApiResponse<()>> {
    info!("Handling request to create records from binary data");

    // Decode received binary data
    let cursor = Cursor::new(encoded_data);
    let mut decoder = RecordDecoder::new(cursor);
    let records = match decoder.decode_to_owned() {
        Ok(records) => {
            info!("Successfully decoded {} records", records.len());
            records
        }
        Err(e) => {
            error!("Failed to decode records: {:?}", e);
            return Err(Error::from(e));
        }
    };

    // Insert decode records
    let mut tx = pool.begin().await?;
    for record in records {
        match record {
            RecordEnum::Mbp1(record) => {
                record.insert_query(&mut tx).await?;
            }
            _ => unimplemented!(),
        }
    }
    // Commit the transaction
    tx.commit().await?;
    info!("Successfully inserted all records.");

    Ok(ApiResponse::new(
        "success",
        "Successfully inserted records.",
        StatusCode::OK,
        None,
    ))
}

pub async fn bulk_upload(
    Extension(pool): Extension<PgPool>,
    Json(file_path): Json<String>,
) -> impl IntoResponse {
    const BATCH_SIZE: usize = 5000;
    let path = PathBuf::from(&file_path);

    if !path.is_file() {
        error!("Path is not a file: {}", file_path);
        return Err(Error::CustomError("Path is not a file.".to_string()));
    }

    info!("Preparing to stream load file : {}", file_path);

    // Create a stream to send updates
    let progress_stream = stream! {
        let mut records_in_batch = 0;
        let mut tx = pool.begin().await?;
        let mut decoder = RecordDecoder::<BufReader<File>>::from_file(&path)?;
        let mut decode_iter = decoder.decode_iterator();

        while let Some(record_result) = decode_iter.next() {
            // info!("Record {:?}", record_result); // uncomment when debugging
            match record_result {
                Ok(record) => {
                    match record {
                        RecordEnum::Mbp1(msg) => {
                            msg.insert_query(&mut tx).await?;
                            records_in_batch += 1;
                        }
                        _ => unimplemented!(),
                    }

                    if records_in_batch >= BATCH_SIZE {
                        info!("Committing batch of records.");
                        tx.commit().await?;
                        tx = pool.begin().await?;
                        let processed_records = records_in_batch.clone();
                        records_in_batch = 0; // Reset batch counter

                        // Yield progress update
                        yield Ok::<Bytes, Error>(Bytes::from(format!("Processed {} records.", processed_records)));
                    }
                }
                Err(e) => {
                    error!("Failed to decode record: {:?}", e);
                    yield Err(Error::CustomError(format!(
                        "Failed to decode record: {}",
                        e
                    )));
                    return;
                }
            }
        }

        // Commit remaining records
        if records_in_batch > 0 {
            info!("Committing final batch of {} records.", records_in_batch);
            tx.commit().await?;
            yield Ok(Bytes::from("Final batch committed."));
        }
        info!("Bulk upload completed.");
        yield Ok(Bytes::from("Bulk upload completed."));
    };

    // Return the stream
    Ok(StreamBody::new(progress_stream))
}

#[allow(unused_assignments)]
pub async fn get_records(
    Extension(pool): Extension<PgPool>,
    Json(params): Json<RetrieveParams>,
) -> impl IntoResponse {
    // Parameters
    const BATCH_SIZE: i64 = 5000;
    let mut end_records = false;
    let mut metadata_sent = false;
    let mut counter = 0;
    let mut clone_params = params.clone();
    let mut symbol_map = SymbolMap::new();

    // Record Encoder
    let record_buffer: Vec<u8> = Vec::new();
    let mut record_cursor = Cursor::new(record_buffer);

    let bytes_stream = stream! {
        let metadata_buffer: Vec<u8> = Vec::new();
        let mut metadata_cursor = Cursor::new(metadata_buffer);
        let mut metadata_encoder = MetadataEncoder::new(&mut metadata_cursor);

        match process_records(&pool, &mut clone_params, &mut counter, &mut record_cursor,  &mut symbol_map).await {
            Ok(_) => {
                info!("Retrieved all records");
                end_records = true;
            }
            Err(e) => {
                error!("Error during query: {:?}", e);
                yield Err(Error::from(e));
                return;
            }
        };

        // Ensure that all symbols are retrieved before proceeding
        while symbol_map.map.keys().len() < params.symbols.len() {
            // Keep waiting until the symbol_map has all requested symbols.
            tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        }

        // Encoding metadata once before streaming batches
        if !metadata_sent {
            let metadata = Metadata::new(
                Schema::from_str(&params.schema).unwrap(),
                params.start_ts as u64,
                params.end_ts as u64,
                symbol_map.clone(),
            );
            metadata_encoder.encode_metadata(&metadata)?;

           let batch_bytes = {
                let buffer_ref = metadata_cursor.get_ref();
                Bytes::copy_from_slice(buffer_ref)
            };

            // Send the data in the buffer to the client
            yield Ok::<Bytes, Error>(batch_bytes);

            // Clear the buffer and reset the cursor
            metadata_cursor.get_mut().clear();
            metadata_cursor.set_position(0);
            metadata_sent = true;
        }

        while metadata_sent && !end_records{
            if counter > BATCH_SIZE {
               // First, immutably borrow the buffer to create `Bytes`
                let batch_bytes = {
                    let buffer_ref = record_cursor.get_ref();
                    Bytes::copy_from_slice(buffer_ref)
                };

                // Send the data in the buffer to the client
                info!("Sending buffer, size: {:?}", batch_bytes.len());
                yield Ok::<Bytes, Error>(batch_bytes);

                // Clear the buffer and reset the cursor
                record_cursor.get_mut().clear();
                record_cursor.set_position(0);
                counter = 0;
            }

            // Check if end of records reached and set the flag appropriately
            if end_records {
                break;
            }

        }

        // Send any remaining data that wasn't part of a full batch
        if !record_cursor.get_ref().is_empty() {
            let remaining_bytes = Bytes::copy_from_slice(record_cursor.get_ref());
            info!("Sending remaining buffer, size: {:?}", remaining_bytes.len());
            yield Ok::<Bytes, Error>(remaining_bytes);
        }

        info!("Finished streaming all batches");
    };

    StreamBody::new(bytes_stream)
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::database::init::init_db;
    use crate::database::symbols::InstrumentsQueries;
    use hyper::body::HttpBody as _;
    use mbn::encode::RecordEncoder;
    use mbn::record_ref::RecordRef;
    use mbn::{
        decode::CombinedDecoder,
        enums::Schema,
        records::{BidAskPair, Mbp1Msg, RecordHeader},
        symbols::Instrument,
    };
    use serial_test::serial;

    #[sqlx::test]
    #[serial]
    async fn test_create_records() {
        dotenv::dotenv().ok();
        let pool = init_db().await.unwrap();
        let mut transaction = pool.begin().await.expect("Error settign up database.");

        // Create instrument
        let ticker = "AAPL";
        let name = "Apple Inc.";
        let instrument = Instrument::new(ticker, name, None);
        let id: i32 = instrument
            .insert_instrument(&mut transaction)
            .await
            .expect("Error inserting symbol.");
        let _ = transaction.commit().await;

        // Records
        let mbp_1 = Mbp1Msg {
            hd: { RecordHeader::new::<Mbp1Msg>(id as u32, 1704209103644092564) },
            price: 6770,
            size: 1,
            action: 1,
            side: 2,
            depth: 0,
            flags: 130,
            ts_recv: 1704209103644092564,
            ts_in_delta: 17493,
            sequence: 739763,
            levels: [BidAskPair {
                bid_px: 1,
                ask_px: 1,
                bid_sz: 1,
                ask_sz: 1,
                bid_ct: 10,
                ask_ct: 20,
            }],
        };
        let mbp_2 = Mbp1Msg {
            hd: { RecordHeader::new::<Mbp1Msg>(id as u32, 1704209103644092566) },
            price: 6870,
            size: 2,
            action: 1,
            side: 1,
            depth: 0,
            flags: 0,
            ts_recv: 1704209103644092566,
            ts_in_delta: 17493,
            sequence: 739763,
            levels: [BidAskPair {
                bid_px: 1,
                ask_px: 1,
                bid_sz: 1,
                ask_sz: 1,
                bid_ct: 10,
                ask_ct: 20,
            }],
        };

        // Test
        let record_ref1: RecordRef = (&mbp_1).into();
        let record_ref2: RecordRef = (&mbp_2).into();

        let mut buffer = Vec::new();
        let mut encoder = RecordEncoder::new(&mut buffer);
        encoder
            .encode_records(&[record_ref1, record_ref2])
            .expect("Encoding failed");

        let result = create_record(Extension(pool.clone()), Json(buffer))
            .await
            .expect("Error creating records.");

        // Validate
        assert_eq!(result.code, StatusCode::OK);

        // Cleanup
        let mut transaction = pool
            .begin()
            .await
            .expect("Error setting up test transaction.");
        Instrument::delete_instrument(&mut transaction, id)
            .await
            .expect("Error on delete.");
        let _ = transaction.commit().await;
    }

    #[sqlx::test]
    #[serial]
    async fn test_bulk_upload() -> anyhow::Result<()> {
        dotenv::dotenv().ok();
        let pool = init_db().await.unwrap();
        let mut transaction = pool.begin().await.expect("Error settign up database.");

        // Create instrument
        let ticker = "AAPL";
        let name = "Apple Inc.";
        let instrument = Instrument::new(ticker, name, None);
        let id: i32 = instrument
            .insert_instrument(&mut transaction)
            .await
            .expect("Error inserting symbol.");
        let _ = transaction.commit().await;

        // Records
        let mbp_1 = Mbp1Msg {
            hd: { RecordHeader::new::<Mbp1Msg>(id as u32, 1704209103644092564) },
            price: 6770,
            size: 1,
            action: 1,
            side: 2,
            depth: 0,
            flags: 130,
            ts_recv: 1704209103644092564,
            ts_in_delta: 17493,
            sequence: 739763,
            levels: [BidAskPair {
                bid_px: 1,
                ask_px: 1,
                bid_sz: 1,
                ask_sz: 1,
                bid_ct: 10,
                ask_ct: 20,
            }],
        };
        let mbp_2 = Mbp1Msg {
            hd: { RecordHeader::new::<Mbp1Msg>(id as u32, 1704209103644092566) },
            price: 6870,
            size: 2,
            action: 1,
            side: 1,
            depth: 0,
            flags: 0,
            ts_recv: 1704209103644092566,
            ts_in_delta: 17493,
            sequence: 739763,
            levels: [BidAskPair {
                bid_px: 1,
                ask_px: 1,
                bid_sz: 1,
                ask_sz: 1,
                bid_ct: 10,
                ask_ct: 20,
            }],
        };

        let record_ref1: RecordRef = (&mbp_1).into();
        let record_ref2: RecordRef = (&mbp_2).into();

        let mut buffer = Vec::new();
        let mut encoder = RecordEncoder::new(&mut buffer);
        encoder
            .encode_records(&[record_ref1, record_ref2])
            .expect("Encoding failed");

        let file = "tests/data/test_bulk_upload.bin";
        let path = PathBuf::from(file);
        let _ = encoder.write_to_file(&path);

        // Test
        let result = bulk_upload(Extension(pool.clone()), Json(file.to_string()))
            .await
            .into_response();

        // Extract the body (which is a stream)
        let mut stream = result.into_body();

        // Collect streamed response
        let mut all_bytes = Vec::new();
        while let Some(chunk) = stream.data().await {
            match chunk {
                Ok(bytes) => all_bytes.extend_from_slice(&bytes),
                Err(e) => panic!("Error while reading chunk: {:?}", e),
            }
        }

        // Validate
        let params = RetrieveParams {
            symbols: vec!["AAPL".to_string()],
            start_ts: 1704209103644092563,
            end_ts: 1704209903644092569,
            schema: Schema::Mbp1.to_string(),
        };
        let response = get_records(Extension(pool.clone()), Json(params))
            .await
            .into_response();

        let mut body = response.into_body();

        // Collect streamed response
        let mut all_bytes = Vec::new();
        while let Some(chunk) = body.data().await {
            match chunk {
                Ok(bytes) => all_bytes.extend_from_slice(&bytes),
                Err(e) => panic!("Error while reading chunk: {:?}", e),
            }
        }

        assert!(!all_bytes.is_empty(), "Streamed data should not be empty");

        // Cleanup
        let mut transaction = pool
            .begin()
            .await
            .expect("Error setting up test transaction.");
        Instrument::delete_instrument(&mut transaction, id)
            .await
            .expect("Error on delete.");
        let _ = transaction.commit().await;

        Ok(())
    }

    #[sqlx::test]
    #[serial]
    async fn test_get_record() {
        dotenv::dotenv().ok();
        let pool = init_db().await.unwrap();
        let mut transaction = pool.begin().await.expect("Error settign up database.");

        // Create instrument
        let ticker = "AAPL";
        let name = "Apple Inc.";
        let instrument = Instrument::new(ticker, name, None);
        let id: i32 = instrument
            .insert_instrument(&mut transaction)
            .await
            .expect("Error inserting symbol.");
        let _ = transaction.commit().await;

        // Records
        let mbp_1 = Mbp1Msg {
            hd: { RecordHeader::new::<Mbp1Msg>(id as u32, 1704209103644092564) },
            price: 6770,
            size: 1,
            action: 1,
            side: 2,
            depth: 0,
            flags: 10,
            ts_recv: 1704209103644092564,
            ts_in_delta: 17493,
            sequence: 739763,
            levels: [BidAskPair {
                bid_px: 1,
                ask_px: 1,
                bid_sz: 1,
                ask_sz: 1,
                bid_ct: 10,
                ask_ct: 20,
            }],
        };
        let mbp_2 = Mbp1Msg {
            hd: { RecordHeader::new::<Mbp1Msg>(id as u32, 1704209103644092565) },
            price: 6870,
            size: 2,
            action: 1,
            side: 1,
            depth: 0,
            flags: 0,
            ts_recv: 1704209103644092565,
            ts_in_delta: 17493,
            sequence: 739763,
            levels: [BidAskPair {
                bid_px: 1,
                ask_px: 1,
                bid_sz: 1,
                ask_sz: 1,
                bid_ct: 10,
                ask_ct: 20,
            }],
        };

        let record_ref1: RecordRef = (&mbp_1).into();
        let record_ref2: RecordRef = (&mbp_2).into();

        let mut buffer = Vec::new();
        let mut encoder = RecordEncoder::new(&mut buffer);
        encoder
            .encode_records(&[record_ref1, record_ref2])
            .expect("Encoding failed");

        let result = create_record(Extension(pool.clone()), Json(buffer))
            .await
            .expect("Error creating records.");

        assert_eq!(result.code, StatusCode::OK);

        // Test
        let params = RetrieveParams {
            symbols: vec!["AAPL".to_string()],
            start_ts: 1704209103644092563,
            end_ts: 1704209903644092569,
            schema: Schema::Mbp1.to_string(),
        };

        let response = get_records(Extension(pool.clone()), Json(params))
            .await
            .into_response();

        let mut body = response.into_body();

        // Collect streamed response
        let mut buffer = Vec::new();
        while let Some(chunk) = body.data().await {
            match chunk {
                Ok(bytes) => buffer.extend_from_slice(&bytes),
                Err(e) => panic!("Error while reading chunk: {:?}", e),
            }
        }

        let cursor = Cursor::new(buffer);
        let mut decoder = CombinedDecoder::new(cursor);
        let (metadata, records) = decoder.decode().expect("Error decoding metadata.");

        // Validate
        // println!("Metadata {:?}", metadata);
        // println!("Records {:?}", records);
        assert!(!records.is_empty(), "Streamed data should not be empty");

        // Cleanup
        let mut transaction = pool
            .begin()
            .await
            .expect("Error setting up test transaction.");
        Instrument::delete_instrument(&mut transaction, id)
            .await
            .expect("Error on delete.");
        let _ = transaction.commit().await;
    }
}
