use anyhow::Result;
use axum::Router;
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use hyper::body::to_bytes;
use mbn::enums::Schema;
use mbn::{
    backtest::BacktestData,
    encode::RecordEncoder,
    record_ref::RecordRef,
    records::{BidAskPair, Mbp1Msg, RecordHeader},
    symbols::Instrument,
};
use midasbackend::database::init::{init_pg_db, init_quest_db};
use midasbackend::database::market_data::RetrieveParams;
use midasbackend::response::ApiResponse;
use midasbackend::router;
use regex::Regex;
use serde::de::DeserializeOwned;
use serde_json::json;
use serial_test::serial;
use std::convert::Infallible;
use std::fs;
use tower::ServiceExt;

fn get_id_from_string(message: &str) -> Option<i32> {
    let re = Regex::new(r"\d+$").unwrap();

    if let Some(captures) = re.captures(message) {
        if let Some(matched) = captures.get(0) {
            let number: i32 = matched.as_str().parse().unwrap();
            return Some(number);
        }
    }
    None
}

async fn create_app() -> Router {
    // Initialize the app with the test router
    dotenv::dotenv().ok();
    let pg_pool = init_pg_db().await.expect("Error on trading_db pool.");
    let quest_pool = init_quest_db().await.expect("Error on market_data pool.");
    let app = router(pg_pool, quest_pool);
    app
}

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

// -- Instruments --
#[tokio::test]
#[serial]
async fn test_instrument_create() -> Result<()> {
    // Initialize the app with the test router
    let app = create_app().await;

    // Test
    let intstrument_json = json!({"ticker": "AAPL11", "name": "Apple tester"});
    let request = Request::builder()
        .method("POST")
        .uri("/market_data/instruments/create")
        .header("content-type", "application/json")
        .body(Body::from(intstrument_json.to_string()))
        .unwrap();

    let response = app.oneshot(request).await.unwrap();

    // Validate
    let api_response: ApiResponse<i32> = parse_response(response).await.unwrap();
    let id = api_response.data.unwrap();
    // let id = get_id_from_string(&api_response.message).unwrap();
    assert!(id > 0);
    assert_eq!(api_response.code, StatusCode::OK);

    // Cleanup
    let request = Request::builder()
        .method("DELETE")
        .uri("/market_data/instruments/delete")
        .header("content-type", "application/json")
        .body(Body::from(id.to_string()))
        .unwrap();

    let app = create_app().await;
    let _ = app.oneshot(request).await.unwrap();

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_instrument_get() -> Result<()> {
    let mut ids: Vec<i32> = Vec::new();

    // Create first instrument
    let app = create_app().await;
    let intstrument_json = json!({"ticker": "TSLA5", "name": "Tesla"});
    let request = Request::builder()
        .method("POST")
        .uri("/market_data/instruments/create")
        .header("content-type", "application/json")
        .body(Body::from(intstrument_json.to_string()))
        .unwrap();

    let response = app.oneshot(request).await.unwrap();

    let api_response: ApiResponse<i32> = parse_response(response).await.unwrap();
    let id = api_response.data.unwrap();
    // let id = get_id_from_string(&api_response.message).unwrap();
    ids.push(id);

    // Create second instrument
    let app = create_app().await;
    let intstrument_json = json!({"ticker": "MSFT5", "name": "Microsoft"});
    let request = Request::builder()
        .method("POST")
        .uri("/market_data/instruments/create")
        .header("content-type", "application/json")
        .body(Body::from(intstrument_json.to_string()))
        .unwrap();
    let response = app.oneshot(request).await.unwrap();
    let api_response: ApiResponse<i32> = parse_response(response).await.unwrap();
    let id = api_response.data.unwrap();
    ids.push(id);

    // Test
    let app = create_app().await;
    let request = Request::builder()
        .method("GET")
        .uri("/market_data/instruments/list")
        .header("content-type", "application/json")
        .body(Body::from(intstrument_json.to_string()))
        .unwrap();
    let response = app.oneshot(request).await.unwrap();
    let api_response: ApiResponse<Vec<Instrument>> = parse_response(response).await.unwrap();

    // Validate
    assert!(api_response.data.unwrap().len() > 0);
    assert_eq!(api_response.code, StatusCode::OK);

    // Cleanup
    for id in ids {
        let request = Request::builder()
            .method("DELETE")
            .uri("/market_data/instruments/delete")
            .header("content-type", "application/json")
            .body(Body::from(id.to_string()))
            .unwrap();

        let pg_pool = init_pg_db().await?;
        let quest_pool = init_quest_db().await?;
        let app = router(pg_pool, quest_pool);
        let _ = app.oneshot(request).await.unwrap();
    }

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_instrument_update() -> Result<()> {
    let mut ids: Vec<i32> = Vec::new();

    // Create instrument
    let app = create_app().await;
    let intstrument_json = json!({"ticker": "TSLA10", "name": "Tesla"});
    let request = Request::builder()
        .method("POST")
        .uri("/market_data/instruments/create")
        .header("content-type", "application/json")
        .body(Body::from(intstrument_json.to_string()))
        .unwrap();

    let response = app.oneshot(request).await.unwrap();

    let api_response: ApiResponse<i32> = parse_response(response).await.unwrap();
    let id = api_response.data.unwrap();
    ids.push(id);

    // Test
    let app = create_app().await;
    let intstrument_json = json!({"ticker": "F2", "name": "Telsa"});
    let update_payload = json!([intstrument_json, id]);
    let request = Request::builder()
        .method("PUT")
        .uri("/market_data/instruments/update")
        .header("content-type", "application/json")
        .body(Body::from(update_payload.to_string()))
        .unwrap();
    let response = app.oneshot(request).await.unwrap();
    let api_response: ApiResponse<Vec<Instrument>> = parse_response(response).await?;

    // Validate
    assert_eq!(api_response.code, StatusCode::OK);

    // Cleanup
    for id in ids {
        let request = Request::builder()
            .method("DELETE")
            .uri("/market_data/instruments/delete")
            .header("content-type", "application/json")
            .body(Body::from(id.to_string()))
            .unwrap();

        let pg_pool = init_pg_db().await?;
        let quest_pool = init_quest_db().await?;
        let app = router(pg_pool, quest_pool);
        let _ = app.oneshot(request).await.unwrap();
    }

    Ok(())
}

// -- Backtest --
#[tokio::test]
#[serial]
async fn test_backtest_create() -> Result<()> {
    // Initialize the app with the test router
    let app = create_app().await;

    // Mock Data
    let mock_data =
        fs::read_to_string("tests/data/test_data.backtest.json").expect("Unable to read file");

    // Test
    let request = Request::builder()
        .method("POST")
        .uri("/trading/backtest/create")
        .header("content-type", "application/json")
        .body(Body::from(mock_data.to_string()))
        .unwrap();

    let response = app.oneshot(request).await.unwrap();

    // Validate
    let api_response: ApiResponse<i32> = parse_response(response).await.unwrap();
    let id = api_response.data.unwrap();
    assert!(id > 0);
    assert_eq!(api_response.code, StatusCode::OK);

    // Cleanup
    let request = Request::builder()
        .method("DELETE")
        .uri("/trading/backtest/delete")
        .header("content-type", "application/json")
        .body(Body::from(id.to_string()))
        .unwrap();

    let app = create_app().await;
    let _ = app.oneshot(request).await.unwrap();

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_backtest_get() -> Result<()> {
    // Initialize the app with the test router
    let app = create_app().await;

    // Mock Data
    let mock_data =
        fs::read_to_string("tests/data/test_data.backtest.json").expect("Unable to read file");

    let request = Request::builder()
        .method("POST")
        .uri("/trading/backtest/create")
        .header("content-type", "application/json")
        .body(Body::from(mock_data.to_string()))
        .unwrap();

    let response = app.oneshot(request).await.unwrap();
    let api_response: ApiResponse<i32> = parse_response(response).await.unwrap();
    let id = api_response.data.unwrap();

    // Test
    let request = Request::builder()
        .method("GET")
        .uri("/trading/backtest/get")
        .header("content-type", "application/json")
        .body(Body::from(id.to_string()))
        .unwrap();

    let app = create_app().await;
    let response = app.oneshot(request).await.unwrap();
    let api_response: ApiResponse<BacktestData> = parse_response(response).await.unwrap();

    // Validate
    assert_eq!(api_response.code, StatusCode::OK);

    // Cleanup
    let request = Request::builder()
        .method("DELETE")
        .uri("/trading/backtest/delete")
        .header("content-type", "application/json")
        .body(Body::from(id.to_string()))
        .unwrap();

    let app = create_app().await;
    let _ = app.oneshot(request).await.unwrap();

    Ok(())
}

// -- Market Data --
#[tokio::test]
#[serial]
async fn test_records_create() -> Result<()> {
    let mut ids: Vec<i32> = Vec::new();

    // Create first instrument
    let app = create_app().await;
    let intstrument_json = json!({"ticker": "TSLA5", "name": "Tesla"});
    let request = Request::builder()
        .method("POST")
        .uri("/market_data/instruments/create")
        .header("content-type", "application/json")
        .body(Body::from(intstrument_json.to_string()))
        .unwrap();

    let response = app.oneshot(request).await.unwrap();

    let api_response: ApiResponse<i32> = parse_response(response).await.unwrap();
    let id = api_response.data.unwrap();
    ids.push(id);

    // Records
    let mbp_1 = Mbp1Msg {
        hd: { RecordHeader::new::<Mbp1Msg>(id as u32, 1704209103644092564) },
        price: 6770,
        size: 1,
        action: 1,
        side: 2,
        depth: 0,
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
        hd: { RecordHeader::new::<Mbp1Msg>(id as u32, 1704209103644092564) },
        price: 6870,
        size: 2,
        action: 1,
        side: 1,
        depth: 0,
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
    let record_ref1: RecordRef = (&mbp_1).into();
    let record_ref2: RecordRef = (&mbp_2).into();

    let mut buffer = Vec::new();
    let mut encoder = RecordEncoder::new(&mut buffer);
    encoder
        .encode_records(&[record_ref1, record_ref2])
        .expect("Encoding failed");

    // Create JSON body
    let json_body = json!(buffer);

    // Test
    let request = Request::builder()
        .method("POST")
        .uri("/market_data/mbp/create")
        .header("content-type", "application/json")
        .body(Body::from(json_body.to_string()))
        .unwrap();

    let app = create_app().await;
    let response = app.oneshot(request).await.unwrap();

    // Validate
    let api_response: ApiResponse<()> = parse_response(response).await.unwrap();
    assert_eq!(api_response.code, StatusCode::OK);

    // Cleanup
    let request = Request::builder()
        .method("DELETE")
        .uri("/market_data/instruments/delete")
        .header("content-type", "application/json")
        .body(Body::from(id.to_string()))
        .unwrap();

    let app = create_app().await;
    let _ = app.oneshot(request).await.unwrap();

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_records_get() -> Result<()> {
    let mut ids: Vec<i32> = Vec::new();

    // Create first instrument
    let app = create_app().await;
    let intstrument_json = json!({"ticker": "TSLA", "name": "Tesla"});
    let request = Request::builder()
        .method("POST")
        .uri("/market_data/instruments/create")
        .header("content-type", "application/json")
        .body(Body::from(intstrument_json.to_string()))
        .unwrap();

    let response = app.oneshot(request).await.unwrap();

    let api_response: ApiResponse<i32> = parse_response(response).await.unwrap();
    let id = api_response.data.unwrap();
    ids.push(id);

    // Records
    let mbp_1 = Mbp1Msg {
        hd: { RecordHeader::new::<Mbp1Msg>(id as u32, 1704209103644092564) },
        price: 6770,
        size: 1,
        action: 1,
        side: 2,
        depth: 0,
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
        hd: { RecordHeader::new::<Mbp1Msg>(id as u32, 1704209103644092564) },
        price: 6870,
        size: 2,
        action: 1,
        side: 1,
        depth: 0,
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
    let record_ref1: RecordRef = (&mbp_1).into();
    let record_ref2: RecordRef = (&mbp_2).into();

    let mut buffer = Vec::new();
    let mut encoder = RecordEncoder::new(&mut buffer);
    encoder
        .encode_records(&[record_ref1, record_ref2])
        .expect("Encoding failed");

    let json_body = json!(buffer);
    let request = Request::builder()
        .method("POST")
        .uri("/market_data/mbp/create")
        .header("content-type", "application/json")
        .body(Body::from(json_body.to_string()))
        .unwrap();

    let app = create_app().await;
    let _ = app.oneshot(request).await.unwrap();

    // Test
    let params = RetrieveParams {
        symbols: vec!["TSLA".to_string()],
        start_ts: 1704209103644092563,
        end_ts: 1704209903644092564,
        schema: Schema::Mbp1.to_string(),
    };
    let json_body = json!(params);

    let request = Request::builder()
        .method("GET")
        .uri("/market_data/mbp/get")
        .header("content-type", "application/json")
        .body(Body::from(json_body.to_string()))
        .unwrap();

    let app = create_app().await;
    let response = app.oneshot(request).await.unwrap();

    // Validate
    let api_response: ApiResponse<Vec<u8>> = parse_response(response).await.unwrap();
    assert_eq!(api_response.code, StatusCode::OK);

    // // Decode data
    // let mut data = api_response.data.unwrap();
    // let cursor = Cursor::new(data);
    // let mut decoder = RecordDecoder::new(cursor);
    // let decoded = decoder.decode_to_owned().expect("Error decoding metadata.");

    // Cleanup
    let request = Request::builder()
        .method("DELETE")
        .uri("/market_data/instruments/delete")
        .header("content-type", "application/json")
        .body(Body::from(id.to_string()))
        .unwrap();

    let app = create_app().await;
    let _ = app.oneshot(request).await.unwrap();

    Ok(())
}
