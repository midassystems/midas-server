use anyhow::Result;
use axum::body::to_bytes;
use axum::Router;
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use futures::stream::StreamExt;
use mbinary::backtest::BacktestData;
use mbinary::backtest_encode::BacktestEncoder;
use serde::de::DeserializeOwned;
use serial_test::serial;
use std::convert::Infallible;
use std::fs;
use tower::ServiceExt;
use trading::{database::init::init_db, response::ApiResponse, router::router};

async fn create_app() -> Router {
    // Initialize the app with the test router
    dotenv::dotenv().ok();
    let pool = init_db().await.expect("Error on trading_db pool.");
    let app = router(pool);
    app
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

// -- Backtest --
#[tokio::test]
#[serial]
async fn test_backtest_create() -> Result<()> {
    // Initialize the app with the test router
    let app = create_app().await;

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

    let response = app.oneshot(request).await.unwrap();
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

    let response = app.oneshot(request).await.unwrap();
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

    let app = create_app().await;
    let response = app.oneshot(request).await.unwrap();
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

    let app = create_app().await;
    let _ = app.oneshot(request).await.unwrap();

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_backtest_get_by_name() -> Result<()> {
    // Initialize the app with the test router
    let app = create_app().await;

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

    let response = app.oneshot(request).await.unwrap();
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

    let app = create_app().await;
    let response = app.oneshot(request).await.unwrap();
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

    let app = create_app().await;
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

    let app = create_app().await;
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

    let app = create_app().await;
    let response = app.oneshot(request).await.unwrap();
    let api_response: ApiResponse<String> = parse_response(response).await.unwrap();

    // Validate
    assert_eq!(api_response.status, "failed");

    Ok(())
}
