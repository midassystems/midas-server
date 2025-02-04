use anyhow::Result;
use axum::Router;
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use historical::{database::init::init_db, response::ApiResponse, router::router};
use hyper::body::to_bytes;
use hyper::body::HttpBody as _;
use mbinary::encode::CombinedEncoder;
use mbinary::metadata::Metadata;
use mbinary::params::RetrieveParams;
use mbinary::symbols::SymbolMap;
use mbinary::vendors::Vendors;
use mbinary::vendors::{DatabentoData, VendorData};
use mbinary::{
    enums::{Dataset, Schema, Stype},
    symbols::Instrument,
};
use mbinary::{
    record_ref::RecordRef,
    records::{BidAskPair, Mbp1Msg, RecordHeader},
};
use serde::de::DeserializeOwned;
use serde_json::json;
use serial_test::serial;
use sqlx::postgres::PgPoolOptions;
use std::convert::Infallible;
use std::path::PathBuf;
use std::str::FromStr;
use tower::ServiceExt;

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

async fn create_app() -> Router {
    // Initialize the app with the test router
    dotenv::dotenv().ok();
    let pool = init_db().await.expect("Error on init_db pool.");
    let app = router(pool);
    app
}

#[allow(dead_code)]
async fn parse_response<T: DeserializeOwned>(
    response: axum::response::Response,
) -> Result<ApiResponse<T>, Infallible> {
    // Extract the body as bytes
    let body_bytes = to_bytes(response.into_body()).await.unwrap();
    let body_text = String::from_utf8(body_bytes.to_vec()).unwrap();

    // Deserialize the response body to ApiResponse for further assertions
    let api_response: ApiResponse<T> = serde_json::from_str(&body_text).unwrap();
    Ok(api_response)
}

// -- Market Data --
#[tokio::test]
#[serial]
// #[ignore]
async fn test_records_create() -> Result<()> {
    let mut ids: Vec<i32> = Vec::new();

    // Create first instrument
    let schema = dbn::Schema::from_str("mbp-1")?;
    let dbn_dataset = dbn::Dataset::from_str("XNAS.ITCH")?;
    let stype = dbn::SType::from_str("raw_symbol")?;
    let vendor_data = VendorData::Databento(DatabentoData {
        schema,
        dataset: dbn_dataset,
        stype,
    });

    let instrument = Instrument::new(
        None,
        "AAPL",
        "Apple tester",
        Dataset::Equities,
        Vendors::Databento,
        vendor_data.encode(),
        1704672000000000000,
        1704672000000000001,
        0,
        true,
    );
    let id = create_instrument(instrument).await?;
    ids.push(id);

    // Records
    let mbp_1 = Mbp1Msg {
        hd: { RecordHeader::new::<Mbp1Msg>(id as u32, 1704209103644092564, 0) },
        price: 6770,
        size: 1,
        action: 1,
        side: 2,
        depth: 0,
        flags: 0,
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
        hd: { RecordHeader::new::<Mbp1Msg>(id as u32, 1704209103644092564, 0) },
        price: 6870,
        size: 2,
        action: 1,
        side: 1,
        depth: 0,
        flags: 0,
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
    let record_ref1: RecordRef = (&mbp_1).into();
    let record_ref2: RecordRef = (&mbp_2).into();

    let metadata = Metadata::new(
        Schema::Mbp1,
        Dataset::Equities,
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

    // Create JSON body
    let json_body = json!(buffer);

    // Test
    let request = Request::builder()
        .method("POST")
        .uri("/historical/mbp/create/stream")
        .header("content-type", "application/json")
        .body(Body::from(json_body.to_string()))
        .unwrap();

    let app = create_app().await;
    let response = app.oneshot(request).await?;

    // Validate
    // Stream and parse the response body
    let mut body_stream = response.into_body();

    // Collect the response body as bytes
    let mut responses = Vec::new();
    while let Some(chunk) = body_stream.data().await {
        match chunk {
            Ok(bytes) => {
                let bytes_str = String::from_utf8_lossy(&bytes);
                let api_response: ApiResponse<String> =
                    serde_json::from_str::<ApiResponse<String>>(&bytes_str)?;
                responses.push(api_response);
            }
            Err(e) => {
                panic!("Error while reading stream: {:?}", e);
            }
        }
    }

    assert_eq!(responses[responses.len() - 1].code, StatusCode::OK);

    // Cleanup
    delete_instrument(id).await.expect("Error on delete");

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_records_get() -> Result<()> {
    let mut ids: Vec<i32> = Vec::new();

    // Create first instrument
    let schema = dbn::Schema::from_str("mbp-1")?;
    let dbn_dataset = dbn::Dataset::from_str("XNAS.ITCH")?;
    let stype = dbn::SType::from_str("raw_symbol")?;
    let vendor_data = VendorData::Databento(DatabentoData {
        schema,
        dataset: dbn_dataset,
        stype,
    });

    let instrument = Instrument::new(
        None,
        "AAPL",
        "Apple tester",
        Dataset::Equities,
        Vendors::Databento,
        vendor_data.encode(),
        1704672000000000000,
        1704672000000000001,
        0,
        true,
    );
    let id = create_instrument(instrument).await?;
    ids.push(id);

    // Records
    let mbp_1 = Mbp1Msg {
        hd: { RecordHeader::new::<Mbp1Msg>(id as u32, 1704209103644092564, 0) },
        price: 6770,
        size: 1,
        action: 1,
        side: 2,
        depth: 0,
        flags: 0,
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
        hd: { RecordHeader::new::<Mbp1Msg>(id as u32, 1704209103644092564, 0) },
        price: 6870,
        size: 2,
        action: 1,
        side: 1,
        depth: 0,
        flags: 0,
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
    let record_ref1: RecordRef = (&mbp_1).into();
    let record_ref2: RecordRef = (&mbp_2).into();

    let metadata = Metadata::new(
        Schema::Mbp1,
        Dataset::Equities,
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

    let json_body = json!(buffer);
    let request = Request::builder()
        .method("POST")
        .uri("/historical/mbp/create/stream")
        .header("content-type", "application/json")
        .body(Body::from(json_body.to_string()))
        .unwrap();

    let app = create_app().await;
    let response = app.oneshot(request).await.unwrap();

    // Stream and parse the response body
    let mut body_stream = response.into_body();

    // Collect the response body as bytes
    let mut responses = Vec::new();
    while let Some(chunk) = body_stream.data().await {
        match chunk {
            Ok(bytes) => {
                let bytes_str = String::from_utf8_lossy(&bytes);
                let api_response: ApiResponse<String> =
                    serde_json::from_str::<ApiResponse<String>>(&bytes_str)?;
                responses.push(api_response);
            }
            Err(e) => {
                panic!("Error while reading stream: {:?}", e);
            }
        }
    }

    // Test
    let params = RetrieveParams {
        symbols: vec!["TSLA".to_string()],
        start_ts: 1704209103644092563,
        end_ts: 1704209903644092564,
        schema: Schema::Mbp1,
        dataset: Dataset::Equities,
        stype: Stype::Raw,
    };
    let json_body = json!(params);

    let request = Request::builder()
        .method("GET")
        .uri("/historical/mbp/get/stream")
        .header("content-type", "application/json")
        .body(Body::from(json_body.to_string()))
        .unwrap();

    let app = create_app().await;
    let response = app.oneshot(request).await.unwrap();

    // Validate
    let body = response.into_body();

    // Convert the body to bytes
    let bytes = to_bytes(body).await.expect("Failed to read response body");

    // Convert bytes to Vec<u8>
    let all_bytes: Vec<u8> = bytes.to_vec();
    assert!(!all_bytes.is_empty(), "Streamed data should not be empty");

    // Cleanup
    delete_instrument(id).await.expect("Error on delete");

    Ok(())
}

#[tokio::test]
#[serial]
// #[ignore]
async fn test_records_create_bulk() -> Result<()> {
    let mut ids: Vec<i32> = Vec::new();

    // Create first instrument
    let schema = dbn::Schema::from_str("mbp-1")?;
    let dbn_dataset = dbn::Dataset::from_str("XNAS.ITCH")?;
    let stype = dbn::SType::from_str("raw_symbol")?;
    let vendor_data = VendorData::Databento(DatabentoData {
        schema,
        dataset: dbn_dataset,
        stype,
    });

    let instrument = Instrument::new(
        None,
        "AAPL",
        "Apple tester",
        Dataset::Equities,
        Vendors::Databento,
        vendor_data.encode(),
        1704672000000000000,
        1704672000000000001,
        0,
        true,
    );
    let id = create_instrument(instrument).await?;
    ids.push(id);

    // Records
    let mbp_1 = Mbp1Msg {
        hd: { RecordHeader::new::<Mbp1Msg>(id as u32, 1704209103644092564, 0) },
        price: 6770,
        size: 1,
        action: 1,
        side: 2,
        depth: 0,
        flags: 0,
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
        hd: { RecordHeader::new::<Mbp1Msg>(id as u32, 1704209103644092564, 0) },
        price: 6870,
        size: 2,
        action: 1,
        side: 1,
        depth: 0,
        flags: 0,
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
    let record_ref1: RecordRef = (&mbp_1).into();
    let record_ref2: RecordRef = (&mbp_2).into();

    let metadata = Metadata::new(
        Schema::Mbp1,
        Dataset::Equities,
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

    // Create records file
    let file = "../data/processed_data/test_bulk_integration.bin";
    let path = PathBuf::from(file);
    let _ = encoder.write_to_file(&path, false)?;

    // Create JSON body
    let json_body = json!(file); //"test_bulk_instegration.bin");

    // Test
    let request = Request::builder()
        .method("POST")
        .uri("/historical/mbp/create/bulk")
        .header("content-type", "application/json")
        .body(Body::from(json_body.to_string()))
        .unwrap();

    let app = create_app().await;
    let response = app.oneshot(request).await?;

    // Validate
    // Stream and parse the response body
    let mut body_stream = response.into_body();

    // Collect the response body as bytes
    let mut responses = Vec::new();
    while let Some(chunk) = body_stream.data().await {
        match chunk {
            Ok(bytes) => {
                let bytes_str = String::from_utf8_lossy(&bytes);
                let api_response: ApiResponse<String> =
                    serde_json::from_str::<ApiResponse<String>>(&bytes_str)?;
                responses.push(api_response);
            }
            Err(e) => {
                panic!("Error while reading stream: {:?}", e);
            }
        }
    }

    assert_eq!(responses[responses.len() - 1].code, StatusCode::OK);

    // Cleanup
    delete_instrument(id).await.expect("Error on delete");
    let _ = tokio::fs::remove_file(path).await;

    Ok(())
}

#[tokio::test]
#[serial]
// #[ignore]
async fn test_records_create_bulk_duplicate_error() -> Result<()> {
    let mut ids: Vec<i32> = Vec::new();

    // Create first instrument
    let schema = dbn::Schema::from_str("mbp-1")?;
    let dbn_dataset = dbn::Dataset::from_str("XNAS.ITCH")?;
    let stype = dbn::SType::from_str("raw_symbol")?;
    let vendor_data = VendorData::Databento(DatabentoData {
        schema,
        dataset: dbn_dataset,
        stype,
    });

    let instrument = Instrument::new(
        None,
        "AAPL",
        "Apple tester",
        Dataset::Equities,
        Vendors::Databento,
        vendor_data.encode(),
        1704672000000000000,
        1704672000000000001,
        0,
        true,
    );
    let id = create_instrument(instrument).await?;
    ids.push(id);

    // Records
    let mbp_1 = Mbp1Msg {
        hd: { RecordHeader::new::<Mbp1Msg>(id as u32, 1704209103644092564, 0) },
        price: 6770,
        size: 1,
        action: 1,
        side: 2,
        depth: 0,
        flags: 0,
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

    let record_ref1: RecordRef = (&mbp_1).into();
    let record_ref2: RecordRef = (&mbp_1).into();

    let metadata = Metadata::new(
        Schema::Mbp1,
        Dataset::Equities,
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

    // Create records file
    let file = "../data/processed_data/test_bulk_integration.bin";
    let path = PathBuf::from(file);
    let _ = encoder.write_to_file(&path, false)?;

    // Create JSON body
    let json_body = json!(file);

    // Test
    let request = Request::builder()
        .method("POST")
        .uri("/historical/mbp/create/bulk")
        .header("content-type", "application/json")
        .body(Body::from(json_body.to_string()))
        .unwrap();

    let app = create_app().await;
    let response = app.oneshot(request).await?;

    // Validate
    // Stream and parse the response body
    let mut body_stream = response.into_body();

    // Collect the response body as bytes
    let mut responses = Vec::new();
    while let Some(chunk) = body_stream.data().await {
        match chunk {
            Ok(bytes) => {
                let bytes_str = String::from_utf8_lossy(&bytes);
                let api_response: ApiResponse<String> =
                    serde_json::from_str::<ApiResponse<String>>(&bytes_str)?;
                responses.push(api_response);
            }
            Err(e) => {
                panic!("Error while reading stream: {:?}", e);
            }
        }
    }
    assert_eq!(responses[0].code, StatusCode::OK);
    assert_eq!(responses[1].code, 409);
    assert_eq!(responses[2].code, StatusCode::OK);

    // Cleanup
    delete_instrument(id).await.expect("Error on delete");
    let _ = tokio::fs::remove_file(path).await;

    Ok(())
}
