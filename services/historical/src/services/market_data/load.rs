use crate::services::market_data::record_loader::RecordLoader;
use crate::{Error, Result};
use axum::response::IntoResponse;
use axum::{body::StreamBody, Extension, Json};
use mbn::decode::RecordDecoder;
use sqlx::PgPool;
use std::fs::File;
use std::io::BufReader;
use std::io::Cursor;
use std::path::PathBuf;
use tracing::{error, info};

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
    let decoder = RecordDecoder::new(cursor);

    // Initialize the loader
    let loader = RecordLoader::new(1000, pool).await?;
    let progress_stream = loader.process_records(decoder).await;

    Ok(StreamBody::new(progress_stream))
}

pub async fn bulk_upload(
    Extension(pool): Extension<PgPool>,
    Json(file_path): Json<String>,
) -> crate::Result<impl IntoResponse> {
    let path = check_file(file_path)?;
    info!("Preparing to stream load file: {}", &path.display());

    // Initialize the decoder
    let decoder = RecordDecoder::<BufReader<File>>::from_file(&path)?;

    // Initialize the loader
    let loader = RecordLoader::new(20_000, pool).await?;
    let progress_stream = loader.process_records(decoder).await;

    Ok(StreamBody::new(progress_stream))
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::database::init::init_db;
    use crate::database::market_data::read::RetrieveParams;
    use crate::database::symbols::InstrumentsQueries;
    use crate::response::ApiResponse;
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
            discriminator: 0,

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
            discriminator: 0,

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
        let mut stream = response.into_body();

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
        assert!(success_responses.len() > 0);
        assert!(error_responses.len() == 0);

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
            discriminator: 0,

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
            discriminator: 0,

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
        let _ = encoder.write_to_file(&path, false);

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
    // #[ignore]
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
            discriminator: 0,
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
            discriminator: 0,
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
        let _ = encoder.write_to_file(&path, false);

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
        assert!(success_responses.len() == 2); // Successful rollback of all batches in process
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
