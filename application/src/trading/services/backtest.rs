pub mod loader;
pub mod retriever;

use super::utils::start_transaction;
use crate::error::Result;
use crate::pool::DatabaseState;
use crate::response::ApiResponse;
use crate::trading::database::backtest::BacktestDataQueries;
use crate::Error;
use axum::body::Body;
use axum::extract::Query;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{delete, get, post};
use axum::{Extension, Json, Router};
use futures::stream::StreamExt;
use loader::BacktestLoader;
use mbinary::backtest::BacktestData;
use mbinary::backtest_decoder::BacktestDecoder;
use retriever::BacktestRetriever;
use std::collections::hash_map::HashMap;
use std::io::Cursor;
use std::sync::Arc;
use tracing::{error, info};

// Service
pub fn backtest_service() -> Router {
    Router::new()
        .route("/create", post(create_backtest))
        .route("/delete", delete(delete_backtest))
        .route("/get", get(retrieve_backtest))
        .route("/list", get(list_backtest))
}

// Handlers
pub async fn create_backtest(
    Extension(state): Extension<Arc<DatabaseState>>,
    stream: Body,
    // stream: StreamBody<Vec<u8>>,
    // Json(encoded_data): Json<Vec<u8>>,
) -> Result<impl IntoResponse> {
    info!("Handling request to create backtest from binary data");
    let mut stream = stream.into_data_stream();
    let mut encoded_data = Vec::new();

    // Read chunks from the stream
    while let Some(chunk) = stream.next().await {
        match chunk {
            Ok(bytes) => {
                encoded_data.extend_from_slice(&bytes);
            }
            Err(e) => {
                error!("Failed to list backtests: {:?}", e);
                return Err(e.into());
            }
        }
    }

    // Decode received binary data
    let cursor = Cursor::new(encoded_data);
    let decoder = BacktestDecoder::new(cursor);

    // Initialize the loader
    let pool = state.trading_pool.clone();
    let loader = BacktestLoader::new(pool).await?;
    let progress_stream = loader.process_stream(decoder).await;

    Ok(Body::from_stream(progress_stream))
}

pub async fn list_backtest(
    Extension(state): Extension<Arc<DatabaseState>>,
) -> Result<impl IntoResponse> {
    info!("Handling request to list backtests");

    match BacktestData::retrieve_list_query(&state.trading_pool).await {
        Ok(data) => {
            info!("Successfully retrieved backtest list");
            Ok(ApiResponse::new(
                "success",
                "Successfully retrieved backtest names.",
                StatusCode::OK,
                data,
            ))
        }
        Err(e) => {
            error!("Failed to list backtests: {:?}", e);
            Err(e.into())
        }
    }
}

pub async fn retrieve_backtest(
    Extension(state): Extension<Arc<DatabaseState>>,

    // Extension(pool): Extension<PgPool>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<impl IntoResponse> {
    info!("Handling request to retrieve backtest");

    let id: i32;
    if let Some(name) = params.get("name") {
        id = match BacktestData::retrieve_id_query(&state.trading_pool, name).await {
            Ok(val) => val,
            Err(e) => {
                error!("Failed to retrieve backtest id : {:?}", e);
                return Err(e.into());
            }
        };
    } else {
        id = params
            .get("id")
            .and_then(|id_str| id_str.parse().ok())
            .ok_or_else(|| Error::GeneralError("Invalid id parameter".into()))?;
    }
    let retriever = BacktestRetriever::new(state.trading_pool.clone(), id);

    match retriever.retrieve_backtest().await {
        Ok(backtest) => {
            info!("Successfully retrieved backtest with id {}", id);
            Ok(ApiResponse::new(
                "success",
                "",
                StatusCode::OK,
                vec![backtest],
            ))
        }
        Err(e) => {
            error!("Failed to retrieve backtest with id {}: {:?}", id, e);
            Err(e.into())
        }
    }
}

pub async fn delete_backtest(
    Extension(state): Extension<Arc<DatabaseState>>,
    Json(id): Json<i32>,
) -> Result<impl IntoResponse> {
    info!("Handling request to delete backtest with id {}", id);

    let mut transaction = start_transaction(&state.trading_pool).await?;
    // let mut transaction = state.trading_pool.begin().await?;
    match BacktestData::delete_query(&mut transaction, id).await {
        Ok(()) => {
            let _ = transaction.commit().await;

            info!("Successfully deleted backtest with id {}", id);
            Ok(ApiResponse::new(
                "success",
                &format!("Successfully deleted backtest with id {}.", id),
                StatusCode::OK,
                "".to_string(),
            ))
        }
        Err(e) => {
            error!("Failed to delete backtest with id {}: {:?}", id, e);
            let _ = transaction.rollback().await;
            Err(e.into())
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use axum::body::to_bytes;
    use futures::stream::StreamExt;
    use mbinary::backtest_encode::BacktestEncoder;
    use serde::de::DeserializeOwned;
    use serial_test::serial;
    use std::fs;

    async fn create_db_state() -> anyhow::Result<Arc<DatabaseState>> {
        let historical_db_url = std::env::var("HISTORICAL_DATABASE_URL")?;
        let trading_db_url = std::env::var("TRADING_DATABASE_URL")?;

        let state = DatabaseState::new(&historical_db_url, &trading_db_url).await?;
        Ok(Arc::new(state))
    }

    async fn parse_response<T: DeserializeOwned>(
        response: axum::response::Response,
    ) -> anyhow::Result<ApiResponse<T>> {
        // Extract the body as bytes
        let body_bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        let body_text = String::from_utf8(body_bytes.to_vec()).unwrap();

        // Deserialize the response body to ApiResponse for further assertions
        let api_response: ApiResponse<T> = serde_json::from_str(&body_text).unwrap();
        Ok(api_response)
    }

    #[sqlx::test]
    #[serial]
    async fn test_create_backtest() -> anyhow::Result<()> {
        dotenv::dotenv().ok();
        let state = create_db_state().await?;
        // let pool = init_db().await.unwrap();

        // Pull test data
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

        let body = Body::from(bytes);

        // Test
        let result = create_backtest(Extension(state.clone()), body)
            .await
            .unwrap()
            .into_response();

        // Validate
        let mut stream = result.into_body().into_data_stream();

        // Vectors to store success and error responses
        let mut success_responses = Vec::new();
        let mut error_responses = Vec::new();

        // Collect streamed responses
        while let Some(chunk) = stream.next().await {
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

        // Validate
        assert!(success_responses.len() > 0);
        assert!(error_responses.len() == 0);

        // Cleanup
        let id: i32 = success_responses[0].data.parse().unwrap();
        let _ = delete_backtest(Extension(state.clone()), Json(id)).await;

        Ok(())
    }

    #[sqlx::test]
    #[serial]
    // #[ignore]
    async fn test_list_backtest() -> anyhow::Result<()> {
        dotenv::dotenv().ok();
        let state = create_db_state().await?;

        // let pool = init_db().await.unwrap();

        // Pull test data
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
        let body = Body::from(bytes);
        let result = create_backtest(Extension(state.clone()), body)
            .await
            .unwrap()
            .into_response();

        let mut stream = result.into_body().into_data_stream();

        // Vectors to store success and error responses
        let mut success_responses = Vec::new();
        let mut error_responses = Vec::new();

        // Collect streamed responses
        while let Some(chunk) = stream.next().await {
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
        let backtest_id: i32 = success_responses[0].data.parse().unwrap();

        // Test
        let backtests = list_backtest(Extension(state.clone()))
            .await
            .unwrap()
            .into_response();

        // Validate
        let api_result: ApiResponse<Vec<(i32, String)>> = parse_response(backtests)
            .await
            .expect("Error parsing response");

        assert!(api_result.data.len() > 0);

        // Cleanup
        let _ = delete_backtest(Extension(state.clone()), Json(backtest_id)).await;

        Ok(())
    }

    #[sqlx::test]
    #[serial]
    // #[ignore]
    async fn test_retrieve_backtest() -> anyhow::Result<()> {
        dotenv::dotenv().ok();
        // let pool = init_db().await.unwrap();
        let state = create_db_state().await?;

        // Pull test data
        let mock_data =
            fs::read_to_string("tests/data/test_data.backtest.json").expect("Unable to read file");
        let mut backtest_data: BacktestData =
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
        let body = Body::from(bytes);
        let result = create_backtest(Extension(state.clone()), body)
            .await
            .unwrap()
            .into_response();

        let mut stream = result.into_body().into_data_stream();

        // Vectors to store success and error responses
        let mut success_responses = Vec::new();
        let mut error_responses = Vec::new();

        // Collect streamed responses
        while let Some(chunk) = stream.next().await {
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
        let backtest_id: i32 = success_responses[0].data.parse().unwrap();

        // Test
        let mut params = HashMap::new();
        params.insert("id".to_string(), backtest_id.to_string());

        let ret_backtest_data = retrieve_backtest(Extension(state.clone()), Query(params))
            .await
            .unwrap()
            .into_response();

        // Validate
        let api_result: ApiResponse<Vec<BacktestData>> = parse_response(ret_backtest_data)
            .await
            .expect("Error parsing response");

        backtest_data.metadata.backtest_id = backtest_id as u16;
        assert_eq!(api_result.data[0], backtest_data);

        // Cleanup
        let _ = delete_backtest(Extension(state.clone()), Json(backtest_id)).await;

        Ok(())
    }

    #[sqlx::test]
    #[serial]
    // #[ignore]
    async fn test_retrieve_backtest_by_name() -> anyhow::Result<()> {
        dotenv::dotenv().ok();
        // let pool = init_db().await.unwrap();
        let state = create_db_state().await?;

        // Pull test data
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
        let body = Body::from(bytes);
        let result = create_backtest(Extension(state.clone()), body)
            .await
            .unwrap()
            .into_response();

        let mut stream = result.into_body().into_data_stream();

        // Vectors to store success and error responses
        let mut success_responses = Vec::new();
        let mut error_responses = Vec::new();

        // Collect streamed responses
        while let Some(chunk) = stream.next().await {
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
        let backtest_id: i32 = success_responses[0].data.parse().unwrap();

        // Test
        let mut params = HashMap::new();
        params.insert("name".to_string(), "testing123".to_string());

        // Test
        let ret_backtest_data = retrieve_backtest(Extension(state.clone()), Query(params))
            .await
            .unwrap()
            .into_response();

        // Validate
        let api_result: ApiResponse<Vec<BacktestData>> = parse_response(ret_backtest_data)
            .await
            .expect("Error parsing response");

        assert_eq!(
            api_result.data[0].metadata.parameters,
            backtest_data.metadata.parameters
        );

        // Cleanup
        let _ = delete_backtest(Extension(state.clone()), Json(backtest_id)).await;
        Ok(())
    }

    #[sqlx::test]
    #[serial]
    // #[ignore]
    async fn test_delete_backtest() -> anyhow::Result<()> {
        dotenv::dotenv().ok();
        // let pool = init_db().await.unwrap();
        let state = create_db_state().await?;

        // Test
        let result = delete_backtest(Extension(state.clone()), Json(63))
            .await
            .unwrap()
            .into_response();

        // Validate
        let api_result: ApiResponse<String> = parse_response(result)
            .await
            .expect("Error parsing response");

        assert_eq!(api_result.code, StatusCode::OK);
        Ok(())
    }
}
