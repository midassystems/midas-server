use anyhow::Result;
use application::pool::DatabaseState;
use application::response::ApiResponse;
use application::router::router;
use axum::body::to_bytes;
use axum::Router;
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use futures::stream::StreamExt;
use mbinary::backtest::BacktestData;
use mbinary::backtest_encode::BacktestEncoder;
use mbinary::encode::CombinedEncoder;
use mbinary::enums::{Schema, Stype};
use mbinary::metadata::Metadata;
use mbinary::params::RetrieveParams;
use mbinary::symbols::SymbolMap;
use mbinary::vendors::{DatabentoData, VendorData};
use mbinary::{self};
use mbinary::{enums::Dataset, symbols::Instrument, vendors::Vendors};
use mbinary::{
    record_ref::RecordRef,
    records::{BidAskPair, Mbp1Msg, RecordHeader},
};
use serde::de::DeserializeOwned;
use serde_json::json;
use serial_test::serial;
use sqlx::postgres::PgPoolOptions;
use std::convert::Infallible;
use std::fs;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use tower::ServiceExt;

async fn create_app() -> anyhow::Result<Router> {
    // Initialize the app with the test router
    dotenv::dotenv().ok();
    let historical_db_url = std::env::var("HISTORICAL_DATABASE_URL")?;
    let trading_db_url = std::env::var("TRADING_DATABASE_URL")?;

    // Initialize the database and obtain a connection pool
    let db_state = DatabaseState::new(&historical_db_url, &trading_db_url).await?;
    let state = Arc::new(db_state);

    // let pool = init_db().await.expect("Error on init_db pool.");
    let app = router(state);
    Ok(app)
}

async fn parse_response<T: DeserializeOwned>(
    response: axum::response::Response,
) -> Result<ApiResponse<T>, Infallible> {
    // Extract the body as bytes
    let body_bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
    let body_text = String::from_utf8(body_bytes.to_vec()).unwrap();

    // Deserialize the response body to ApiResponse for further assertions
    let api_response: ApiResponse<T> = serde_json::from_str(&body_text).unwrap();
    Ok(api_response)
}

// -- Helper functions
async fn create_instrument(instrument: Instrument) -> Result<i32> {
    let database_url = std::env::var("HISTORICAL_DATABASE_URL")?;
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
    let database_url = std::env::var("HISTORICAL_DATABASE_URL")?;
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

// -- Backtest --
#[tokio::test]
#[serial]
async fn test_backtest_create() -> Result<()> {
    // Initialize the app with the test router
    let app = create_app().await?;

    // Mock Data
    let mock_data =
        fs::read_to_string("tests/data/test_data.backtest.json").expect("Unable to read file");

    let backtest_data: BacktestData =
        serde_json::from_str(&mock_data).expect("JSON was not well-formatted");

    // Encode
    let mut bytes = Vec::new();
    let mut encoder = BacktestEncoder::new(&mut bytes);
    encoder.encode_metadata(&backtest_data.metadata);
    encoder.encode_timeseries(&backtest_data.period_timeseries_stats);
    encoder.encode_timeseries(&backtest_data.daily_timeseries_stats);
    encoder.encode_trades(&backtest_data.trades);
    encoder.encode_signals(&backtest_data.signals);

    // Test
    let request = Request::builder()
        .method("POST")
        .uri("/trading/backtest/create")
        .header("content-type", "application/json")
        .body(Body::from(bytes))
        .unwrap();

    let response = app.clone().oneshot(request).await.unwrap();
    let mut body_stream = response.into_body().into_data_stream();

    // Validate
    let mut responses = Vec::new();
    while let Some(chunk) = body_stream.next().await {
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
    let last_response = &responses[responses.len() - 1];
    let id: i32 = last_response.data.parse().unwrap();
    assert!(id > 0);
    assert_eq!(last_response.code, StatusCode::OK);

    // Cleanup
    let request = Request::builder()
        .method("DELETE")
        .uri("/trading/backtest/delete")
        .header("content-type", "application/json")
        .body(Body::from(id.to_string()))
        .unwrap();

    let _ = app.oneshot(request).await.unwrap();

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_backtest_get() -> Result<()> {
    // Initialize the app with the test router
    let app = create_app().await?;

    // Mock Data
    let mock_data =
        fs::read_to_string("tests/data/test_data.backtest.json").expect("Unable to read file");
    let backtest_data: BacktestData =
        serde_json::from_str(&mock_data).expect("JSON was not well-formatted");

    // Encode
    let mut bytes = Vec::new();
    let mut encoder = BacktestEncoder::new(&mut bytes);
    encoder.encode_metadata(&backtest_data.metadata);
    encoder.encode_timeseries(&backtest_data.period_timeseries_stats);
    encoder.encode_timeseries(&backtest_data.daily_timeseries_stats);
    encoder.encode_trades(&backtest_data.trades);
    encoder.encode_signals(&backtest_data.signals);

    // Test
    let request = Request::builder()
        .method("POST")
        .uri("/trading/backtest/create")
        .header("content-type", "application/json")
        .body(Body::from(bytes))
        .unwrap();

    let response = app.clone().oneshot(request).await.unwrap();
    let mut body_stream = response.into_body().into_data_stream();

    // Validate
    let mut responses = Vec::new();
    while let Some(chunk) = body_stream.next().await {
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
    let last_response = &responses[responses.len() - 1];
    let id: i32 = last_response.data.parse().unwrap();

    // Test
    let request = Request::builder()
        .method("GET")
        .uri(format!("/trading/backtest/get?id={}", id)) // Include the query parameter in the URL
        .header("content-type", "application/json")
        .body(Body::empty()) // No need to include the id in the body
        .unwrap();

    let response = app.clone().oneshot(request).await.unwrap();
    let api_response: ApiResponse<Vec<BacktestData>> = parse_response(response).await.unwrap();

    // Validate
    assert_eq!(api_response.code, StatusCode::OK);

    // Cleanup
    let request = Request::builder()
        .method("DELETE")
        .uri("/trading/backtest/delete")
        .header("content-type", "application/json")
        .body(Body::from(id.to_string()))
        .unwrap();

    let _ = app.clone().oneshot(request).await.unwrap();

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_backtest_get_by_name() -> Result<()> {
    // Initialize the app with the test router
    let app = create_app().await?;

    // Mock Data
    let mock_data =
        fs::read_to_string("tests/data/test_data.backtest.json").expect("Unable to read file");

    let backtest_data: BacktestData =
        serde_json::from_str(&mock_data).expect("JSON was not well-formatted");

    // Encode
    let mut bytes = Vec::new();
    let mut encoder = BacktestEncoder::new(&mut bytes);
    encoder.encode_metadata(&backtest_data.metadata);
    encoder.encode_timeseries(&backtest_data.period_timeseries_stats);
    encoder.encode_timeseries(&backtest_data.daily_timeseries_stats);
    encoder.encode_trades(&backtest_data.trades);
    encoder.encode_signals(&backtest_data.signals);

    // Test
    let request = Request::builder()
        .method("POST")
        .uri("/trading/backtest/create")
        .header("content-type", "application/json")
        .body(Body::from(bytes))
        .unwrap();

    let response = app.clone().oneshot(request).await.unwrap();
    let mut body_stream = response.into_body().into_data_stream();

    // Validate
    let mut responses = Vec::new();
    while let Some(chunk) = body_stream.next().await {
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
    let last_response = &responses[responses.len() - 1];
    let id: i32 = last_response.data.parse().unwrap();

    // Test
    let request = Request::builder()
        .method("GET")
        .uri(format!("/trading/backtest/get?name={}", "testing123")) // Include the query parameter in the URL
        .header("content-type", "application/json")
        .body(Body::empty()) // No need to include the id in the body
        .unwrap();

    let response = app.clone().oneshot(request).await.unwrap();
    let api_response: ApiResponse<Vec<BacktestData>> = parse_response(response).await.unwrap();

    // Validate
    assert_eq!(api_response.code, StatusCode::OK);

    // Cleanup
    let request = Request::builder()
        .method("DELETE")
        .uri("/trading/backtest/delete")
        .header("content-type", "application/json")
        .body(Body::from(id.to_string()))
        .unwrap();

    let _ = app.oneshot(request).await.unwrap();

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_backtest_get_none() -> Result<()> {
    // Test
    let request = Request::builder()
        .method("GET")
        .uri(format!("/trading/backtest/get?id={}", 1)) // Include the query parameter in the URL
        .header("content-type", "application/json")
        .body(Body::empty()) // No need to include the id in the body
        .unwrap();

    let app = create_app().await?;
    let response = app.oneshot(request).await.unwrap();
    let api_response: ApiResponse<String> = parse_response(response).await.unwrap();

    // Validate
    assert_eq!(api_response.status, "failed");

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_backtest_get_by_name_none() -> Result<()> {
    // Test
    let request = Request::builder()
        .method("GET")
        .uri(format!("/trading/backtest/get?name={}", "none")) // Include the query parameter in the URL
        .header("content-type", "application/json")
        .body(Body::empty()) // No need to include the id in the body
        .unwrap();

    let app = create_app().await?;
    let response = app.oneshot(request).await.unwrap();
    let api_response: ApiResponse<String> = parse_response(response).await.unwrap();

    // Validate
    assert_eq!(api_response.status, "failed");

    Ok(())
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
        false,
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

    let app = create_app().await?;
    let response = app.oneshot(request).await?;

    // Validate
    // Stream and parse the response body
    let mut body_stream = response.into_body().into_data_stream();

    // Collect the response body as bytes
    let mut responses = Vec::new();
    while let Some(chunk) = body_stream.next().await {
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
        false,
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

    let app = create_app().await?;
    let response = app.clone().oneshot(request).await.unwrap();

    // Stream and parse the response body
    let mut body_stream = response.into_body().into_data_stream();

    // Collect the response body as bytes
    let mut responses = Vec::new();
    while let Some(chunk) = body_stream.next().await {
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

    let response = app.oneshot(request).await.unwrap();

    // Validate
    let body = response.into_body();

    // Convert the body to bytes
    let bytes = to_bytes(body, 1024 * 1024)
        .await
        .expect("Failed to read response body");

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
        false,
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

    let app = create_app().await?;
    let response = app.clone().oneshot(request).await?;

    // Validate
    // Stream and parse the response body
    let mut body_stream = response.into_body().into_data_stream();

    // Collect the response body as bytes
    let mut responses = Vec::new();
    while let Some(chunk) = body_stream.next().await {
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
        false,
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

    let app = create_app().await?;
    let response = app.clone().oneshot(request).await?;

    // Validate
    // Stream and parse the response body
    let mut body_stream = response.into_body().into_data_stream();

    // Collect the response body as bytes
    let mut responses = Vec::new();
    while let Some(chunk) = body_stream.next().await {
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

// -- Instruments --
#[tokio::test]
#[serial]
async fn test_instrument_create() -> anyhow::Result<()> {
    // Initialize the app with the test router
    let app = create_app().await?;

    let schema = dbn::Schema::from_str("mbp-1")?;
    let dataset = dbn::Dataset::from_str("GLBX.MDP3")?;
    let stype = dbn::SType::from_str("raw_symbol")?;
    let vendor_data = VendorData::Databento(DatabentoData {
        schema,
        dataset,
        stype,
    });

    // Test
    let instrument = Instrument::new(
        None,
        "AAPL11",
        "Apple tester",
        Dataset::Equities,
        Vendors::Databento,
        vendor_data.encode(),
        1704672000000000000,
        1704672000000000001,
        0,
        false,
        true,
    );
    let json = json!(instrument);
    let request = Request::builder()
        .method("POST")
        .uri("/instruments/create")
        .header("content-type", "application/json")
        .body(Body::from(json.to_string()))
        .unwrap();

    let response = app.oneshot(request).await.unwrap();

    // Validate
    let api_response: ApiResponse<i32> = parse_response(response).await.unwrap();
    let id = api_response.data;
    assert!(id > 0);
    assert_eq!(api_response.code, StatusCode::OK);

    // Cleanup
    let request = Request::builder()
        .method("DELETE")
        .uri("/instruments/delete")
        .header("content-type", "application/json")
        .body(Body::from(id.to_string()))
        .unwrap();

    let app = create_app().await?;
    let _ = app.oneshot(request).await.unwrap();

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_instrument_get() -> Result<()> {
    // Initialize the app with the test router
    let app = create_app().await?;

    let schema = dbn::Schema::from_str("mbp-1")?;
    let dataset = dbn::Dataset::from_str("GLBX.MDP3")?;
    let stype = dbn::SType::from_str("raw_symbol")?;
    let vendor_data = VendorData::Databento(DatabentoData {
        schema,
        dataset,
        stype,
    });
    // Create instrument
    let instrument = Instrument::new(
        None,
        "AAPL11",
        "Apple tester",
        Dataset::Equities,
        Vendors::Databento,
        vendor_data.encode(),
        1704672000000000000,
        1704672000000000001,
        0,
        false,
        true,
    );
    let json = json!(instrument);
    let request = Request::builder()
        .method("POST")
        .uri("/instruments/create")
        .header("content-type", "application/json")
        .body(Body::from(json.to_string()))
        .unwrap();

    let response = app.oneshot(request).await.unwrap();
    let api_response: ApiResponse<i32> = parse_response(response).await.unwrap();
    let id = api_response.data;

    // Test
    let request = Request::builder()
        .method("GET")
        .uri("/instruments/get")
        .header("content-type", "application/json")
        .body(Body::from(
            serde_json::to_string(&(String::from("AAPL11"), Dataset::Equities)).unwrap(),
        )) // JSON body
        .unwrap();

    let app = create_app().await?;
    let response = app.oneshot(request).await?;

    // Validate
    let api_response: ApiResponse<Vec<Instrument>> = parse_response(response).await.unwrap();

    assert!(api_response.data.len() > 0);
    assert_eq!(api_response.code, StatusCode::OK);

    // Cleanup
    let request = Request::builder()
        .method("DELETE")
        .uri("/instruments/delete")
        .header("content-type", "application/json")
        .body(Body::from(id.to_string()))
        .unwrap();

    let app = create_app().await?;
    let _ = app.oneshot(request).await.unwrap();

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_instrument_update() -> anyhow::Result<()> {
    let mut ids: Vec<i32> = Vec::new();

    let schema = dbn::Schema::from_str("mbp-1")?;
    let dataset = dbn::Dataset::from_str("GLBX.MDP3")?;
    let stype = dbn::SType::from_str("raw_symbol")?;
    let vendor_data = VendorData::Databento(DatabentoData {
        schema,
        dataset,
        stype,
    });

    // Create instrument
    let app = create_app().await?;
    let instrument = Instrument::new(
        None,
        "AAPL11",
        "Apple tester",
        Dataset::Equities,
        Vendors::Databento,
        vendor_data.encode(),
        1704672000000000000,
        1704672000000000001,
        0,
        false,
        true,
    );
    let json = json!(instrument);

    let request = Request::builder()
        .method("POST")
        .uri("/instruments/create")
        .header("content-type", "application/json")
        .body(Body::from(json.to_string()))
        .unwrap();

    let response = app.clone().oneshot(request).await.unwrap();

    let api_response: ApiResponse<i32> = parse_response(response).await.unwrap();
    let id = api_response.data;
    ids.push(id);

    // Test
    let instrument2 = Instrument::new(
        Some(id as u32),
        "F2",
        "tesla",
        Dataset::Equities,
        Vendors::Databento,
        vendor_data.encode(),
        1704672000000000000,
        1704672000000000001,
        0,
        false,
        true,
    );

    let json = json!(instrument2);

    // let update_payload = json!([intstrument_json, id]);
    let request = Request::builder()
        .method("PUT")
        .uri("/instruments/update")
        .header("content-type", "application/json")
        .body(Body::from(json.to_string()))
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    let api_response: ApiResponse<String> = parse_response(response).await?;

    // Validate
    assert_eq!(api_response.code, StatusCode::OK);

    // Cleanup
    for id in ids {
        let request = Request::builder()
            .method("DELETE")
            .uri("/instruments/delete")
            .header("content-type", "application/json")
            .body(Body::from(id.to_string()))
            .unwrap();

        // let pool = init_db().await?;
        // let app = router(pool);
        let _ = app.clone().oneshot(request).await.unwrap();
    }

    Ok(())
}
#[tokio::test]
#[serial]
async fn test_instrument_get_none() -> Result<()> {
    // Test
    let request = Request::builder()
        .method("GET")
        .uri("/instruments/get")
        .header("content-type", "application/json")
        .body(Body::from(
            serde_json::to_string(&(String::from("AAPL11"), Dataset::Equities)).unwrap(),
        )) // JSON body
        .unwrap();

    let app = create_app().await?;
    let response = app.clone().oneshot(request).await?;

    // Validate
    let api_response: ApiResponse<Vec<Instrument>> = parse_response(response).await.unwrap();
    let data: Vec<Instrument> = api_response.data;

    assert!(data.len() == 0);
    assert_eq!(api_response.code, StatusCode::NOT_FOUND);

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_instrument_list_dataset() -> anyhow::Result<()> {
    let mut ids: Vec<i32> = Vec::new();

    let schema = dbn::Schema::from_str("mbp-1")?;
    let dataset = dbn::Dataset::from_str("GLBX.MDP3")?;
    let stype = dbn::SType::from_str("raw_symbol")?;
    let vendor_data = VendorData::Databento(DatabentoData {
        schema,
        dataset,
        stype,
    });

    // Create first instrument
    let app = create_app().await?;
    let instrument = Instrument::new(
        None,
        "AAPL11",
        "Apple tester",
        Dataset::Equities,
        Vendors::Databento,
        vendor_data.encode(),
        1704672000000000000,
        1704672000000000001,
        0,
        false,
        true,
    );
    let json = json!(instrument);
    let request = Request::builder()
        .method("POST")
        .uri("/instruments/create")
        .header("content-type", "application/json")
        .body(Body::from(json.to_string()))
        .unwrap();

    let response = app.clone().oneshot(request).await.unwrap();

    let api_response: ApiResponse<i32> = parse_response(response).await.unwrap();
    let id = api_response.data;
    ids.push(id);

    // Create second instrument
    let instrument2 = Instrument::new(
        None,
        "AAPL12",
        "Apple tester",
        Dataset::Equities,
        Vendors::Databento,
        vendor_data.encode(),
        1704672000000000000,
        1704672000000000001,
        0,
        false,
        true,
    );
    let json = json!(instrument2);
    let request = Request::builder()
        .method("POST")
        .uri("/instruments/create")
        .header("content-type", "application/json")
        .body(Body::from(json.to_string()))
        .unwrap();

    let response = app.clone().oneshot(request).await.unwrap();
    let api_response: ApiResponse<i32> = parse_response(response).await.unwrap();
    let id = api_response.data;
    ids.push(id);

    // Test
    let request = Request::builder()
        .method("GET")
        .uri("/instruments/list_dataset")
        .header("content-type", "application/json")
        .body(Body::from(
            serde_json::to_string(&Dataset::Equities).unwrap(),
        ))
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    let api_response: ApiResponse<Vec<Instrument>> = parse_response(response).await.unwrap();

    // Validate
    assert!(api_response.data.len() > 0);
    assert_eq!(api_response.code, StatusCode::OK);

    // Cleanup
    for id in ids {
        let request = Request::builder()
            .method("DELETE")
            .uri("/instruments/delete")
            .header("content-type", "application/json")
            .body(Body::from(id.to_string()))
            .unwrap();

        let _ = app.clone().oneshot(request).await.unwrap();
    }

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_vendor_list_instruments() -> anyhow::Result<()> {
    let mut ids: Vec<i32> = Vec::new();

    let schema = dbn::Schema::from_str("mbp-1")?;
    let dataset = dbn::Dataset::from_str("GLBX.MDP3")?;
    let stype = dbn::SType::from_str("raw_symbol")?;
    let vendor_data = VendorData::Databento(DatabentoData {
        schema,
        dataset,
        stype,
    });

    // Create first instrument
    let app = create_app().await?;
    let instrument = Instrument::new(
        None,
        "AAPL11",
        "Apple tester",
        Dataset::Equities,
        Vendors::Databento,
        vendor_data.encode(),
        1704672000000000000,
        1704672000000000001,
        0,
        false,
        true,
    );
    let json = json!(instrument);
    let request = Request::builder()
        .method("POST")
        .uri("/instruments/create")
        .header("content-type", "application/json")
        .body(Body::from(json.to_string()))
        .unwrap();

    let response = app.clone().oneshot(request).await.unwrap();

    let api_response: ApiResponse<i32> = parse_response(response).await.unwrap();
    let id = api_response.data;
    ids.push(id);

    // Create second instrument
    let instrument = Instrument::new(
        None,
        "AAPL12",
        "Apple tester",
        Dataset::Equities,
        Vendors::Databento,
        vendor_data.encode(),
        1704672000000000000,
        1704672000000000001,
        0,
        false,
        true,
    );
    let json = json!(instrument);

    let request = Request::builder()
        .method("POST")
        .uri("/instruments/create")
        .header("content-type", "application/json")
        .body(Body::from(json.to_string()))
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    let api_response: ApiResponse<i32> = parse_response(response).await.unwrap();
    let id = api_response.data;
    ids.push(id);

    // Test
    let request = Request::builder()
        .method("GET")
        .uri("/instruments/list_vendor")
        .header("content-type", "application/json")
        .body(Body::from(
            serde_json::to_string(&(Vendors::Databento, Dataset::Equities)).unwrap(),
        ))
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    let api_response: ApiResponse<Vec<Instrument>> = parse_response(response).await.unwrap();

    // Validate
    assert!(api_response.data.len() > 0);
    assert_eq!(api_response.code, StatusCode::OK);

    // Cleanup
    for id in ids {
        let request = Request::builder()
            .method("DELETE")
            .uri("/instruments/delete")
            .header("content-type", "application/json")
            .body(Body::from(id.to_string()))
            .unwrap();

        let _ = app.clone().oneshot(request).await.unwrap();
    }

    Ok(())
}
