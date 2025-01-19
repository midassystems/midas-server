pub mod loader;

use crate::{Error, Result};
use axum::response::IntoResponse;
use axum::{body::StreamBody, Extension, Json};
use axum::{routing::post, Router};
use loader::RecordLoader;
use mbn::decode::Decoder;
use sqlx::PgPool;
use std::fs::File;
use std::io::BufReader;
use std::io::Cursor;
use std::path::PathBuf;
use tracing::{error, info};

// Service
pub fn create_service() -> Router {
    Router::new()
        .route("/stream", post(create_record))
        .route("/bulk", post(bulk_upload))
}

pub fn check_file(file_path: String) -> Result<PathBuf> {
    let path;
    if cfg!(test) {
        path = PathBuf::from(&file_path);
    } else if std::env::var("CARGO_MANIFEST_DIR").is_ok() {
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
    let mut decoder = Decoder::new(cursor)?;

    let metadata = decoder
        .metadata()
        .ok_or_else(|| Error::CustomError("Invalid metadata.".into()))?;

    // Initialize the loader
    let loader = RecordLoader::new(1000, metadata.dataset, pool).await?;
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
    let mut decoder = Decoder::<BufReader<File>>::from_file(&path)?;

    let metadata = decoder
        .metadata()
        .ok_or_else(|| Error::CustomError("Invalid metadata.".into()))?;

    // Initialize the loader
    let loader = RecordLoader::new(20_000, metadata.dataset, pool).await?;
    let progress_stream = loader.process_records(decoder).await;

    Ok(StreamBody::new(progress_stream))
}

#[cfg(test)]
mod test {
    use std::str::FromStr;

    use super::*;
    use crate::database::init::init_db;
    use crate::response::ApiResponse;
    use crate::services::retrieve::get_records;
    use hyper::body::HttpBody as _;
    use mbn::encode::CombinedEncoder;
    use mbn::metadata::Metadata;
    use mbn::params::RetrieveParams;
    use mbn::record_ref::RecordRef;
    use mbn::symbols::SymbolMap;
    use mbn::vendors::Vendors;
    use mbn::vendors::{DatabentoData, VendorData};
    use mbn::{
        enums::{Dataset, Schema},
        records::{BidAskPair, Mbp1Msg, RecordHeader},
        symbols::Instrument,
    };
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
            INSERT INTO {} (instrument_id, ticker, name, vendor, vendor_data, last_available, first_available, active)
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
    // #[ignore]
    async fn test_create_records() -> anyhow::Result<()> {
        dotenv::dotenv().ok();
        let pool = init_db().await.unwrap();
        let transaction = pool.begin().await.expect("Error settign up database.");

        // Create instrument
        let schema = dbn::Schema::from_str("mbp-1")?;
        let dbn_dataset = dbn::Dataset::from_str("XNAS.ITCH")?;
        let stype = dbn::SType::from_str("raw_symbol")?;
        let vendor_data = VendorData::Databento(DatabentoData {
            schema,
            dataset: dbn_dataset,
            stype,
        });

        let dataset = Dataset::Equities;
        let ticker = "AAPL";
        let name = "Apple Inc.";
        let instrument = Instrument::new(
            None,
            ticker,
            name,
            dataset.clone(),
            Vendors::Databento,
            vendor_data.encode(),
            1704672000000000000,
            1704672000000000000,
            true,
        );

        let id = create_instrument(instrument)
            .await
            .expect("Error creating instrument.");
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

        let metadata = Metadata::new(
            Schema::Mbp1,
            dataset,
            1704209103644092564,
            1704209103644092566,
            SymbolMap::new(),
        );

        let mut buffer = Vec::new();
        let mut encoder = CombinedEncoder::new(&mut buffer);
        encoder.encode_metadata(&metadata)?;
        // let mut encoder = RecordEncoder::new(&mut buffer);
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
        delete_instrument(id).await.expect("Error on delete");

        Ok(())
    }

    #[sqlx::test]
    #[serial]
    // #[ignore]
    async fn test_bulk_upload() -> anyhow::Result<()> {
        dotenv::dotenv().ok();
        let pool = init_db().await.unwrap();
        let transaction = pool.begin().await.expect("Error settign up database.");

        // Create instrument
        let schema = dbn::Schema::from_str("mbp-1")?;
        let dbn_dataset = dbn::Dataset::from_str("XNAS.ITCH")?;
        let stype = dbn::SType::from_str("raw_symbol")?;
        let vendor_data = VendorData::Databento(DatabentoData {
            schema,
            dataset: dbn_dataset,
            stype,
        });
        let dataset = Dataset::Equities;
        let ticker = "AAPL";
        let name = "Apple Inc.";
        let instrument = Instrument::new(
            None,
            ticker,
            name,
            dataset.clone(),
            Vendors::Databento,
            vendor_data.encode(),
            1704672000000000000,
            1704672000000000000,
            true,
        );

        let id = create_instrument(instrument)
            .await
            .expect("Error creating instrument.");
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

        let metadata = Metadata::new(
            Schema::Mbp1,
            dataset,
            1704209103644092564,
            1704209103644092566,
            SymbolMap::new(),
        );

        let mut buffer = Vec::new();
        let mut encoder = CombinedEncoder::new(&mut buffer);
        encoder.encode_metadata(&metadata)?;
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
            schema: Schema::Mbp1,
            dataset,
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
        delete_instrument(id).await.expect("Error on delete");

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
        let transaction = pool.begin().await.expect("Error settign up database.");

        // Create instrument
        let schema = dbn::Schema::from_str("mbp-1")?;
        let dbn_dataset = dbn::Dataset::from_str("XNAS.ITCH")?;
        let stype = dbn::SType::from_str("raw_symbol")?;
        let vendor_data = VendorData::Databento(DatabentoData {
            schema,
            dataset: dbn_dataset,
            stype,
        });

        let dataset = Dataset::Equities;
        let ticker = "AAPL";
        let name = "Apple Inc.";
        let instrument = Instrument::new(
            None,
            ticker,
            name,
            dataset.clone(),
            Vendors::Databento,
            vendor_data.encode(),
            1704672000000000000,
            1704672000000000000,
            true,
        );

        let id = create_instrument(instrument)
            .await
            .expect("Error creating instrument.");
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

        let metadata = Metadata::new(
            Schema::Mbp1,
            dataset,
            1704209103644092564,
            1704209103644092566,
            SymbolMap::new(),
        );

        let mut buffer = Vec::new();

        let mut encoder = CombinedEncoder::new(&mut buffer);
        encoder.encode_metadata(&metadata)?;
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
                            // println!("{:?}", response);
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
        assert!(error_responses.len() == 1);

        // Cleanup
        delete_instrument(id).await.expect("Error on delete");

        // Cleanup
        if path.exists() {
            std::fs::remove_file(&path).expect("Failed to delete the test file.");
        }

        Ok(())
    }
}
