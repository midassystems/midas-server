use anyhow::Result;
use axum::Router;
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use hyper::body::to_bytes;
use mbn::backtest::BacktestData;
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
    let body_bytes = to_bytes(response.into_body()).await.unwrap();
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
    let id = api_response.data;
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
    let id = api_response.data;

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
