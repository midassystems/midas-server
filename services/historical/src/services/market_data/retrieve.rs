use crate::database::market_data::read::{process_records, RetrieveParams};
use crate::Error;
use async_stream::stream;
use axum::response::IntoResponse;
use axum::{body::StreamBody, Extension, Json};
use bytes::Bytes;
use mbn::encode::MetadataEncoder;
use mbn::enums::Schema;
use mbn::metadata::Metadata;
use mbn::symbols::SymbolMap;
use sqlx::PgPool;
use std::io::Cursor;
use std::str::FromStr;
use tracing::{error, info};

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
    use crate::database::market_data::read::RetrieveParams;
    use crate::database::symbols::InstrumentsQueries;
    use crate::response::ApiResponse;
    use crate::services::market_data::load::create_record;
    // use axum::http::StatusCode;
    use axum::response::IntoResponse;
    use axum::{Extension, Json};
    // use hyper::body::to_bytes;
    use hyper::body::HttpBody as _;
    use mbn::encode::RecordEncoder;
    use mbn::record_ref::RecordRef;
    use mbn::symbols::Vendors;
    use mbn::{
        decode::Decoder,
        enums::Schema,
        records::{BidAskPair, Mbp1Msg, RecordHeader},
        symbols::Instrument,
    };
    // use serde::de::DeserializeOwned;
    use serial_test::serial;
    use std::io::Cursor;

    // async fn parse_response<T: DeserializeOwned>(
    //     response: axum::response::Response,
    // ) -> anyhow::Result<ApiResponse<T>> {
    //     // Extract the body as bytes
    //     let body_bytes = to_bytes(response.into_body()).await.unwrap();
    //     let body_text = String::from_utf8(body_bytes.to_vec()).unwrap();
    //
    //     // Deserialize the response body to ApiResponse for further assertions
    //     let api_response: ApiResponse<T> = serde_json::from_str(&body_text).unwrap();
    //     Ok(api_response)
    // }

    #[sqlx::test]
    #[serial]
    // #[ignore]
    async fn test_get_record() -> anyhow::Result<()> {
        dotenv::dotenv().ok();
        let pool = init_db().await.unwrap();
        let mut transaction = pool.begin().await.expect("Error settign up database.");

        // Create instrument
        let ticker = "AAPL";
        let name = "Apple Inc.";
        let instrument = Instrument::new(
            None,
            ticker,
            name,
            Vendors::Databento,
            Some("continuous".to_string()),
            Some("GLBX.MDP3".to_string()),
            1704672000000000000,
            1704672000000000000,
            true,
        );
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

        let response = create_record(Extension(pool.clone()), Json(buffer))
            .await
            .expect("Error creating records.")
            .into_response();
        let mut stream = response.into_body();

        // Collect streamed responses
        while let Some(chunk) = stream.data().await {
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
        let mut decoder = Decoder::new(cursor)?;
        let records = decoder.decode()?; //.expect("Error decoding metadata.");

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

        Ok(())
    }
}
