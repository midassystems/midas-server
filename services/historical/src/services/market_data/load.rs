use crate::database::market_data::create::{
    clear_staging, merge_staging, InsertBatch, RecordInsertQueries,
};
use crate::response::ApiResponse;
use crate::{Error, Result};
use async_stream::stream;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::{body::StreamBody, Extension, Json};
use bytes::Bytes;
use mbn::decode::RecordDecoder;
use mbn::record_enum::RecordEnum;
use sqlx::{PgPool, Postgres, Transaction};
use std::fs::File;
use std::io::BufReader;
use std::io::Cursor;
use std::path::PathBuf;
use tracing::{error, info};

// Helpers
async fn execute_and_commit_batch(
    insert_batch: &mut InsertBatch,
    tx: &mut Transaction<'_, Postgres>,
    batch_size: usize,
) -> std::result::Result<ApiResponse<String>, ApiResponse<String>> {
    match insert_batch.execute(tx).await {
        Ok(_) => {
            // Create a success response
            Ok(ApiResponse::new(
                "success",
                &format!("Processed {} records.", batch_size),
                StatusCode::OK,
                "".to_string(),
            ))
        }
        Err(e) => {
            error!("Error during batch execution: {:?}", e);

            // Create an error response
            Err(ApiResponse::new(
                "failed",
                &format!("Error during batch execution: {:?}", e),
                StatusCode::CONFLICT,
                "".to_string(),
            ))
        }
    }
}

pub async fn merge(pool: &PgPool) -> Result<()> {
    let mut tx = pool.begin().await?;
    if let Err(e) = merge_staging(&mut tx).await {
        error!("Failed to merge staging into production: {:?}", e);

        // Rollback merge
        if let Err(rollback_err) = tx.rollback().await {
            error!("Failed to rollback transaction: {:?}", rollback_err);
        }

        // Clear the staging tables
        let mut tx = pool.begin().await?;
        if let Err(clear_err) = clear_staging(&mut tx).await {
            error!("Failed to clear staging tables: {:?}", clear_err);
        }

        return Err(Error::CustomError(format!(
            "Failed to merge staging into production: {:?}",
            e
        )));
    }

    tx.commit().await?;
    Ok(())
}

pub fn check_file(file_path: String) -> Result<PathBuf> {
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
    Ok(path)
}

// Handlers
/// For smaller inserts, mainly using bulk_upload.
pub async fn create_record(
    Extension(pool): Extension<PgPool>,
    Json(encoded_data): Json<Vec<u8>>,
) -> Result<impl IntoResponse> {
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
    merge(&pool).await?;
    info!("Successfully merged all records.");

    Ok(ApiResponse::new(
        "success",
        "Successfully inserted records.",
        StatusCode::OK,
        "".to_string(),
    ))
}

pub async fn bulk_upload(
    Extension(pool): Extension<PgPool>,
    Json(file_path): Json<String>,
) -> Result<impl IntoResponse> {
    const BATCH_SIZE: usize = 20000;
    let path = check_file(file_path)?;
    let mut insert_batch = InsertBatch::new();
    info!("Preparing to stream load file : {}", &path.display());

    // Create a stream to send updates
    let progress_stream = stream! {
        let mut records_in_batch = 0;
        let mut decoder = RecordDecoder::<BufReader<File>>::from_file(&path)?;
        let mut decode_iter = decoder.decode_iterator();

        while let Some(record_result) = decode_iter.next() {
            // info!("Record {:?}", record_result); // uncomment when debugging
            match record_result {
                Ok(record) => {
                    // Add record to batch
                    match record {
                        RecordEnum::Mbp1(msg) => {
                            insert_batch.process(&msg).await?;
                            records_in_batch += 1;
                        }
                        _ => unimplemented!(),
                    }

                    // Insert batch
                    if records_in_batch >= BATCH_SIZE {
                        let mut tx = pool.begin().await?;

                        match execute_and_commit_batch(&mut insert_batch, &mut tx, records_in_batch).await {
                            Ok(response) => {
                                tx.commit().await?;
                                yield Ok(Bytes::from(serde_json::to_string(&response).unwrap()));
                                records_in_batch = 0;
                            }
                            Err(response) => {
                                tx.rollback().await?;
                                yield Ok(Bytes::from(serde_json::to_string(&response).unwrap()));
                                return;
                            }
                        }
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

        // Insert remaining records

        if records_in_batch > 0 {
            let mut tx = pool.begin().await?;
                match execute_and_commit_batch(&mut insert_batch, &mut tx, records_in_batch).await {
                    Ok(response) => {
                        tx.commit().await?;
                        yield Ok(Bytes::from(serde_json::to_string(&response).unwrap()));
                    }
                    Err(response) => {
                        tx.rollback().await?;
                        yield Ok(Bytes::from(serde_json::to_string(&response).unwrap()));
                        return;
                    }
                }
        }

        // Merge staging into production
        merge(&pool).await?;
        info!("Bulk upload and merge completed successfully.");

        // Final response
        let response: ApiResponse<String> = ApiResponse::new(
            "success",
            "Successfully inserted records.",
            StatusCode::OK,
            "".to_string(),
        );

        yield Ok(Bytes::from(serde_json::to_string(&response).unwrap()));
    };

    // Returned to establish stream
    Ok(StreamBody::new(progress_stream))
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::database::init::init_db;
    use crate::database::market_data::read::RetrieveParams;
    use crate::database::symbols::InstrumentsQueries;
    use crate::services::market_data::get_records;
    use hyper::body::to_bytes;
    use hyper::body::HttpBody as _;
    use mbn::encode::RecordEncoder;
    use mbn::record_ref::RecordRef;
    use mbn::symbols::Vendors;
    use mbn::{
        enums::Schema,
        records::{BidAskPair, Mbp1Msg, RecordHeader},
        symbols::Instrument,
    };
    use serde::de::DeserializeOwned;
    use serial_test::serial;

    async fn parse_response<T: DeserializeOwned>(
        response: axum::response::Response,
    ) -> anyhow::Result<ApiResponse<T>> {
        // Extract the body as bytes
        let body_bytes = to_bytes(response.into_body()).await.unwrap();
        let body_text = String::from_utf8(body_bytes.to_vec()).unwrap();

        // Deserialize the response body to ApiResponse for further assertions
        let api_response: ApiResponse<T> = serde_json::from_str(&body_text).unwrap();
        Ok(api_response)
    }

    #[sqlx::test]
    #[serial]
    async fn test_create_records() -> anyhow::Result<()> {
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

        let response = create_record(Extension(pool.clone()), Json(buffer))
            .await
            .expect("Error creating records.")
            .into_response();

        // Validate
        let api_response: ApiResponse<String> = parse_response(response)
            .await
            .expect("Error parsing response");
        assert_eq!(api_response.code, StatusCode::OK);

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

        // Vectors to store success and error responses
        let mut success_responses = Vec::new();
        let mut error_responses = Vec::new();

        // Collect streamed responses
        while let Some(chunk) = stream.data().await {
            match chunk {
                Ok(bytes) => {
                    let bytes_str = String::from_utf8_lossy(&bytes);

                    match serde_json::from_str::<ApiResponse<String>>(&bytes_str) {
                        Ok(response) => {
                            if response.status == "success" {
                                success_responses.push(response); // Store success response
                            } else {
                                error_responses.push(response); // Store error response
                            }
                        }
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

        // Log stored responses (optional)
        assert!(success_responses.len() > 0);
        assert!(error_responses.len() == 0);

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

    #[sqlx::test]
    #[serial]
    async fn test_bulk_upload_error() -> anyhow::Result<()> {
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

        // Vectors to store success and error responses
        let mut success_responses = Vec::new();
        let mut error_responses = Vec::new();

        // Collect streamed responses
        while let Some(chunk) = stream.data().await {
            match chunk {
                Ok(bytes) => {
                    let bytes_str = String::from_utf8_lossy(&bytes);

                    match serde_json::from_str::<ApiResponse<String>>(&bytes_str) {
                        Ok(response) => {
                            if response.status == "success" {
                                success_responses.push(response); // Store success response
                            } else {
                                error_responses.push(response); // Store error response
                            }
                        }
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

        // Log stored responses (optional)
        assert!(success_responses.len() == 0);
        assert!(error_responses.len() > 0);

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
