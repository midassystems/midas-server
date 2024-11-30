use crate::database::market_data::create::{merge_staging, InsertBatch, RecordInsertQueries};
use crate::response::ApiResponse;
use crate::{Error, Result};
use async_stream::stream;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::{body::StreamBody, Extension, Json};
use bytes::Bytes;
use mbn::decode::RecordDecoder;
use mbn::record_enum::RecordEnum;
use sqlx::PgPool;
use std::fs::File;
use std::io::BufReader;
use std::io::Cursor;
use std::path::PathBuf;
use tracing::{error, info};

// Handlers
/// For testing purposes ONLY, inserts directly to the productions tables(mbp, bid_ask).
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

    // Merge staging into production
    let mut tx = pool.begin().await?;

    // Merge staging into production
    if let Err(e) = merge_staging(&mut tx).await {
        if let Err(rollback_err) = tx.rollback().await {
            error!("Failed to rollback transaction: {:?}", rollback_err);
        }
        error!("Failed to merge staging into production: {:?}", e);
        return Err(Error::from(e)); // Return your custom error here
    }

    tx.commit().await?;
    info!("Successfully merged all records.");

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
    let path;
    if cfg!(test) {
        path = PathBuf::from(&file_path);
    } else {
        path = PathBuf::from("data/processed_data").join(&file_path);
    }

    if !path.is_file() {
        error!("Path is not a file: {}", &file_path);
        return Err(Error::CustomError("Path is not a file.".to_string()));
    }

    info!("Preparing to stream load file : {}", &file_path);

    const BATCH_SIZE: usize = 20000;
    let mut insert_batch = InsertBatch::new();

    // Create a stream to send updates
    let progress_stream = stream! {
        let mut records_in_batch = 0;
        let mut decoder = RecordDecoder::<BufReader<File>>::from_file(&path)?;
        let mut decode_iter = decoder.decode_iterator();

        while let Some(record_result) = decode_iter.next() {
            // info!("Record {:?}", record_result); // uncomment when debugging
            match record_result {
                Ok(record) => {
                    match record {
                        RecordEnum::Mbp1(msg) => {
                            insert_batch.process(&msg).await?;
                            records_in_batch += 1;
                        }
                        _ => unimplemented!(),
                    }

                    if records_in_batch >= BATCH_SIZE {
                        let mut tx = pool.begin().await?;
                        insert_batch.execute(&mut tx).await?;

                        tx.commit().await?;
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
            let mut tx = pool.begin().await?;
            insert_batch.execute(&mut tx).await?;
            tx.commit().await?;
            yield Ok(Bytes::from("Final batch committed."));
        }

        // Merge staging into production
        let mut tx = pool.begin().await?;
        if let Err(e) = merge_staging(&mut tx).await {
            tx.rollback().await?;
            error!("Failed to merge staging into production: {:?}", e);
            yield Err(Error::CustomError(format!(
                "Failed to merge staging into production: {:?}",
                e
            )));
            return;
        }
        tx.commit().await?;

        info!("Bulk upload and merge completed successfully.");
        yield Ok(Bytes::from("Bulk upload and merge completed successfully."));
    };

    Ok(StreamBody::new(progress_stream))
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::database::init::init_db;
    use crate::database::market_data::read::RetrieveParams;
    use crate::database::symbols::InstrumentsQueries;
    use crate::services::market_data::get_records;
    use hyper::body::HttpBody as _;
    use mbn::encode::RecordEncoder;
    use mbn::record_ref::RecordRef;
    use mbn::symbols::Vendors;
    use mbn::{
        enums::Schema,
        records::{BidAskPair, Mbp1Msg, RecordHeader},
        symbols::Instrument,
    };
    use serial_test::serial;

    #[sqlx::test]
    #[serial]
    // #[ignore]
    async fn test_create_records() {
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
    // #[ignore]
    async fn test_bulk_upload() -> anyhow::Result<()> {
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

        // Cleanup
        if path.exists() {
            std::fs::remove_file(&path).expect("Failed to delete the test file.");
        }

        Ok(())
    }
}
