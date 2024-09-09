use crate::database::market_data::{RecordInsertQueries, RecordRetrieveQueries, RetrieveParams};
use crate::mbn::decode::decoder_from_file;
use crate::mbn::record_enum::RecordEnum;
use crate::mbn::symbols::SymbolMap;
use crate::response::ApiResponse;
use crate::{Error, Result};
use async_stream::stream;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{body::StreamBody, Extension, Json, Router};
use bytes::Bytes;
use mbn::decode::RecordDecoder;
use mbn::encode::CombinedEncoder;
use mbn::enums::Schema;
use mbn::metadata::Metadata;
use sqlx::PgPool;
use std::io::Cursor;
use std::path::Path;
use std::str::FromStr;
use tracing::info;

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
    // Decode received binary data
    let cursor = Cursor::new(encoded_data);
    let mut decoder = RecordDecoder::new(cursor);
    let records = decoder.decode_to_owned()?;

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
    tx.commit().await?;

    Ok(ApiResponse::new(
        "success",
        "Successfully inserted records.",
        StatusCode::OK,
        None,
    ))
}

pub async fn bulk_upload(
    Extension(pool): Extension<PgPool>,
    Json(file_name): Json<String>,
) -> impl IntoResponse {
    const BATCH_SIZE: usize = 5000;

    let bulk_data_dir = "/app/bulk_data/";
    let file_path = Path::new(bulk_data_dir).join(file_name);
    let file_path_str = match file_path.to_str() {
        Some(path) => path.to_string(),
        None => return Err(Error::CustomError("Invalid file path".to_string())),
    };

    info!("File found, preparing to stream load data.");

    // Create a stream to send updates
    let progress_stream = stream! {
        let mut records_in_batch = 0;
        let mut tx = pool.begin().await?;
        let mut decoder = decoder_from_file(&file_path_str)?;
        let mut decode_iter = decoder.decode_iterator();

        while let Some(record_result) = decode_iter.next() {
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
            tx.commit().await?;
            yield Ok(Bytes::from("Final batch committed."));
        }

        yield Ok(Bytes::from("Bulk upload completed."));
    };

    // Return the stream
    Ok(StreamBody::new(progress_stream))
}

pub async fn get_records(
    Extension(pool): Extension<PgPool>,
    Json(params): Json<RetrieveParams>,
) -> impl IntoResponse {
    const BATCH_SIZE: i64 = 86400000000000; // One day in nanoseconds

    let bytes_stream = stream! {
        let mut buffer = Vec::new();
        let mut metadata_sent = false;
        let mut clone_params = params.clone();
        let mut accumulated_symbol_map = SymbolMap::new();

        loop {
            let (records, map) = match RecordEnum::retrieve_query(&pool, &mut clone_params, BATCH_SIZE).await {
                Ok((records, map)) => (records, map),
                Err(e) => {
                    println!("Error during query: {:?}", e);
                    yield Err(Error::from(e));
                    return;
                }
            };
            // Merge the new symbol map with the accumulated one
            accumulated_symbol_map.merge(&map);

            if accumulated_symbol_map.map.keys().len() == params.symbols.len(){


            {
                let mut encoder = CombinedEncoder::new(&mut buffer);

                if !metadata_sent {
                    let metadata = Metadata::new(
                        Schema::from_str(&params.schema).unwrap(),
                        params.start_ts as u64,
                        params.end_ts as u64,
                        map.clone(),
                    );

                    encoder.encode_metadata(&metadata).unwrap();
                    metadata_sent = true;
                }

                for record in records {
                    let record_ref = record.to_record_ref();
                    encoder.encode_record(&record_ref).unwrap();
                }
            }

            let batch_bytes = Bytes::from(buffer.clone());
            buffer.clear();

            println!("Sending buffer, currently: {:?}", buffer);
            yield Ok::<Bytes, Error>(batch_bytes);

            }
            if clone_params.start_ts >= clone_params.end_ts{
                break;
            }
        }

        println!("Finished streaming all batches");
    };
    StreamBody::new(bytes_stream)
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::database::init::init_quest_db;
    use crate::database::symbols::InstrumentsQueries;
    use crate::mbn::encode::RecordEncoder;
    use crate::mbn::record_ref::RecordRef;
    use crate::mbn::{
        enums::Schema,
        records::{BidAskPair, Mbp1Msg, RecordHeader},
        symbols::Instrument,
    };
    use hyper::body::HttpBody as _;
    use serial_test::serial;

    #[sqlx::test]
    #[serial]
    async fn test_create_records() {
        dotenv::dotenv().ok();
        let pool = init_quest_db().await.unwrap();
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
    async fn test_get_record() {
        dotenv::dotenv().ok();
        let pool = init_quest_db().await.unwrap();
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
        let mut all_bytes = Vec::new();
        while let Some(chunk) = body.data().await {
            match chunk {
                Ok(bytes) => all_bytes.extend_from_slice(&bytes),
                Err(e) => panic!("Error while reading chunk: {:?}", e),
            }
        }

        // Validate
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
    }
}
