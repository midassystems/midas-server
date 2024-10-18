use anyhow::Result;
use axum::Router;
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use historical::{
    database::{init::init_db, market_data::RetrieveParams},
    response::ApiResponse,
    router::router,
};
use hyper::body::to_bytes;
use mbn::enums::Schema;
use mbn::{
    encode::RecordEncoder,
    record_ref::RecordRef,
    records::{BidAskPair, Mbp1Msg, RecordHeader},
    symbols::Instrument,
};
use serde::de::DeserializeOwned;
use serde_json::json;
use serial_test::serial;
use std::convert::Infallible;
use tower::ServiceExt;

async fn create_app() -> Router {
    // Initialize the app with the test router
    dotenv::dotenv().ok();
    let pool = init_db().await.expect("Error on init_db pool.");
    let app = router(pool);
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
        .uri("/historical/instruments/create")
        .header("content-type", "application/json")
        .body(Body::from(intstrument_json.to_string()))
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
        .uri("/historical/instruments/delete")
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
    // Initialize the app with the test router
    let app = create_app().await;

    // Create instrument
    let instrument_json = json!({"ticker": "AAPL11", "name": "Apple tester"});
    let request = Request::builder()
        .method("POST")
        .uri("/historical/instruments/create")
        .header("content-type", "application/json")
        .body(Body::from(instrument_json.to_string()))
        .unwrap();

    let response = app.oneshot(request).await.unwrap();
    let api_response: ApiResponse<i32> = parse_response(response).await.unwrap();
    let id = api_response.data.unwrap();

    // Test
    let request = Request::builder()
        .method("GET")
        .uri("/historical/instruments/get")
        .header("content-type", "application/json")
        .body(Body::from(json!(String::from("AAPL11")).to_string())) // JSON body
        .unwrap();

    let app = create_app().await;
    let response = app.oneshot(request).await?;

    // Validate
    let api_response: ApiResponse<i32> = parse_response(response).await.unwrap();
    assert!(api_response.data.unwrap() > 0);
    assert_eq!(api_response.code, StatusCode::OK);

    // Cleanup
    let request = Request::builder()
        .method("DELETE")
        .uri("/historical/instruments/delete")
        .header("content-type", "application/json")
        .body(Body::from(id.to_string()))
        .unwrap();

    let app = create_app().await;
    let _ = app.oneshot(request).await.unwrap();

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_instrument_get_none() -> Result<()> {
    // Test
    let request = Request::builder()
        .method("GET")
        .uri("/historical/instruments/get")
        .header("content-type", "application/json")
        .body(Body::from(json!(String::from("AAPL11")).to_string())) // JSON body
        .unwrap();

    let app = create_app().await;
    let response = app.oneshot(request).await?;

    // Validate
    let api_response: ApiResponse<i32> = parse_response(response).await.unwrap();
    let id: Option<i32> = api_response.data;

    assert!(id == None);
    assert_eq!(api_response.code, StatusCode::NOT_FOUND);

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_instrument_list() -> Result<()> {
    let mut ids: Vec<i32> = Vec::new();

    // Create first instrument
    let app = create_app().await;
    let intstrument_json = json!({"ticker": "TSLA5", "name": "Tesla"});
    let request = Request::builder()
        .method("POST")
        .uri("/historical/instruments/create")
        .header("content-type", "application/json")
        .body(Body::from(intstrument_json.to_string()))
        .unwrap();

    let response = app.oneshot(request).await.unwrap();

    let api_response: ApiResponse<i32> = parse_response(response).await.unwrap();
    let id = api_response.data.unwrap();
    ids.push(id);

    // Create second instrument
    let app = create_app().await;
    let intstrument_json = json!({"ticker": "MSFT5", "name": "Microsoft"});
    let request = Request::builder()
        .method("POST")
        .uri("/historical/instruments/create")
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
        .uri("/historical/instruments/list")
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
            .uri("/historical/instruments/delete")
            .header("content-type", "application/json")
            .body(Body::from(id.to_string()))
            .unwrap();

        let pool = init_db().await?;
        let app = router(pool);
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
        .uri("/historical/instruments/create")
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
        .uri("/historical/instruments/update")
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
            .uri("/historical/instruments/delete")
            .header("content-type", "application/json")
            .body(Body::from(id.to_string()))
            .unwrap();

        let pool = init_db().await?;
        let app = router(pool);
        let _ = app.oneshot(request).await.unwrap();
    }

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
        .uri("/historical/instruments/create")
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
        flags: 0,
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
        flags: 0,
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
        .uri("/historical/mbp/create")
        .header("content-type", "application/json")
        .body(Body::from(json_body.to_string()))
        .unwrap();

    let app = create_app().await;
    let response = app.oneshot(request).await?;

    // Validate
    let api_response: ApiResponse<()> = parse_response(response).await.unwrap();
    assert_eq!(api_response.code, StatusCode::OK);

    // Cleanup
    let request = Request::builder()
        .method("DELETE")
        .uri("/historical/instruments/delete")
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
        .uri("/historical/instruments/create")
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
        flags: 0,
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
        flags: 0,
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
        .uri("/historical/mbp/create")
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
        .uri("/historical/mbp/get")
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
    let request = Request::builder()
        .method("DELETE")
        .uri("/historical/instruments/delete")
        .header("content-type", "application/json")
        .body(Body::from(id.to_string()))
        .unwrap();

    let app = create_app().await;
    let _ = app.oneshot(request).await.unwrap();

    Ok(())
}
