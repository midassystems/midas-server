use anyhow::Result;
use axum::Router;
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use hyper::body::to_bytes;
use instrument::database::init::init_db;
use instrument::response::ApiResponse;
use instrument::router::router;
use mbn::vendors::{DatabentoData, VendorData};
use mbn::{enums::Dataset, symbols::Instrument, vendors::Vendors};
use serde::de::DeserializeOwned;
use serde_json::json;
use serial_test::serial;
use std::convert::Infallible;
use std::str::FromStr;
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
async fn test_instrument_create() -> anyhow::Result<()> {
    // Initialize the app with the test router
    let app = create_app().await;

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

    let app = create_app().await;
    let _ = app.oneshot(request).await.unwrap();

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_instrument_get() -> Result<()> {
    // Initialize the app with the test router
    let app = create_app().await;

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

    let app = create_app().await;
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

    let app = create_app().await;
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
    let app = create_app().await;
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
    ids.push(id);

    // Test
    let app = create_app().await;
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
    let response = app.oneshot(request).await.unwrap();
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

        let pool = init_db().await?;
        let app = router(pool);
        let _ = app.oneshot(request).await.unwrap();
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

    let app = create_app().await;
    let response = app.oneshot(request).await?;

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
    let app = create_app().await;
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
    ids.push(id);

    // Create second instrument
    let app = create_app().await;
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
        true,
    );
    let json = json!(instrument2);
    let request = Request::builder()
        .method("POST")
        .uri("/instruments/create")
        .header("content-type", "application/json")
        .body(Body::from(json.to_string()))
        .unwrap();

    let response = app.oneshot(request).await.unwrap();
    let api_response: ApiResponse<i32> = parse_response(response).await.unwrap();
    let id = api_response.data;
    ids.push(id);

    // Test
    let app = create_app().await;
    let request = Request::builder()
        .method("GET")
        .uri("/instruments/list_dataset")
        .header("content-type", "application/json")
        .body(Body::from(
            serde_json::to_string(&Dataset::Equities).unwrap(),
        ))
        .unwrap();
    let response = app.oneshot(request).await.unwrap();
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

        let pool = init_db().await?;
        let app = router(pool);
        let _ = app.oneshot(request).await.unwrap();
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
    let app = create_app().await;
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
    ids.push(id);

    // Create second instrument
    let app = create_app().await;
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
    ids.push(id);

    // Test
    let app = create_app().await;
    let request = Request::builder()
        .method("GET")
        .uri("/instruments/list_vendor")
        .header("content-type", "application/json")
        .body(Body::from(
            serde_json::to_string(&(Vendors::Databento, Dataset::Equities)).unwrap(),
        ))
        .unwrap();
    let response = app.oneshot(request).await.unwrap();
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

        let pool = init_db().await?;
        let app = router(pool);
        let _ = app.oneshot(request).await.unwrap();
    }

    Ok(())
}
