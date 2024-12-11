use super::utils::start_transaction;
use crate::database::backtest::{
    create_backtest_related, retrieve_backtest_related, BacktestDataQueries,
};
use crate::error::Result;
use crate::response::ApiResponse;
use crate::Error;
use axum::extract::Query;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{delete, get, post};
use axum::{Extension, Json, Router};
use mbn::backtest::BacktestData;
use sqlx::PgPool;
use std::collections::hash_map::HashMap;
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
    Extension(pool): Extension<PgPool>,
    Json(backtest_data): Json<BacktestData>,
) -> Result<impl IntoResponse> {
    info!("Handling request to create backtest");

    let mut transaction = start_transaction(&pool).await?;

    match create_backtest_related(&mut transaction, &backtest_data).await {
        Ok(id) => {
            let _ = transaction.commit().await;

            info!("Successfully created backtest with id {}", id);
            Ok(ApiResponse::new(
                "success",
                &format!("Successfully created backtest with id {}", id),
                StatusCode::OK,
                id,
            ))
        }
        Err(e) => {
            error!("Failed to create backtest: {:?}", e);
            let _ = transaction.rollback().await;
            Err(e.into())
        }
    }
}

pub async fn list_backtest(Extension(pool): Extension<PgPool>) -> Result<impl IntoResponse> {
    info!("Handling request to list backtests");

    match BacktestData::retrieve_list_query(&pool).await {
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
    Extension(pool): Extension<PgPool>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<impl IntoResponse> {
    info!("Handling request to retrieve backtest");

    let id: i32;
    if let Some(name) = params.get("name") {
        id = match BacktestData::retrieve_id_query(&pool, name).await {
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

    match retrieve_backtest_related(&pool, id).await {
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
    Extension(pool): Extension<PgPool>,
    Json(id): Json<i32>,
) -> Result<impl IntoResponse> {
    info!("Handling request to delete backtest with id {}", id);

    let mut transaction = start_transaction(&pool).await?;
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
    use crate::database::init::init_db;
    use hyper::body::to_bytes;
    use regex::Regex;
    use serde::de::DeserializeOwned;
    use serial_test::serial;
    use std::fs;

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

    async fn parse_response<T: DeserializeOwned>(
        response: axum::response::Response,
    ) -> anyhow::Result<ApiResponse<T>> {
        // Extract the body as bytes
        let body_bytes = to_bytes(response.into_body()).await.unwrap();
        let body_text = String::from_utf8(body_bytes.to_vec()).unwrap();
        // println!("{:?}", body_text);

        // Deserialize the response body to ApiResponse for further assertions
        let api_response: ApiResponse<T> = serde_json::from_str(&body_text).unwrap();
        Ok(api_response)
    }

    #[sqlx::test]
    #[serial]
    async fn test_create_backtest() {
        dotenv::dotenv().ok();
        let pool = init_db().await.unwrap();

        // Pull test data
        let mock_data =
            fs::read_to_string("tests/data/test_data.backtest.json").expect("Unable to read file");
        let backtest_data: BacktestData =
            serde_json::from_str(&mock_data).expect("JSON was not well-formatted");

        // Test
        let result = create_backtest(Extension(pool.clone()), Json(backtest_data))
            .await
            .unwrap()
            .into_response();

        // Validate
        let api_result: ApiResponse<i32> = parse_response(result)
            .await
            .expect("Error parsing response");

        assert_eq!(api_result.code, StatusCode::OK);
        assert!(api_result
            .message
            .contains("Successfully created backtest with id"));

        // Cleanup
        let number = get_id_from_string(&api_result.message);
        if number.is_some() {
            let _ = delete_backtest(Extension(pool.clone()), Json(number.unwrap())).await;
        }
    }

    #[sqlx::test]
    #[serial]
    async fn test_list_backtest() {
        dotenv::dotenv().ok();
        let pool = init_db().await.unwrap();

        // Pull test data
        let mock_data =
            fs::read_to_string("tests/data/test_data.backtest.json").expect("Unable to read file");
        let backtest_data: BacktestData =
            serde_json::from_str(&mock_data).expect("JSON was not well-formatted");

        let result = create_backtest(Extension(pool.clone()), Json(backtest_data))
            .await
            .unwrap()
            .into_response();

        let api_result: ApiResponse<i32> = parse_response(result)
            .await
            .expect("Error parsing response");

        let number = get_id_from_string(&api_result.message);
        let backtest_id = number.clone();

        // Test
        let backtests = list_backtest(Extension(pool.clone()))
            .await
            .unwrap()
            .into_response();

        // Validate
        let api_result: ApiResponse<Vec<(i32, String)>> = parse_response(backtests)
            .await
            .expect("Error parsing response");

        assert!(api_result.data.len() > 0);

        // Cleanup
        if backtest_id.is_some() {
            let _ = delete_backtest(Extension(pool.clone()), Json(number.unwrap())).await;
        }
    }

    #[sqlx::test]
    #[serial]
    async fn test_retrieve_backtest() {
        dotenv::dotenv().ok();
        let pool = init_db().await.unwrap();

        // Pull test data
        let mock_data =
            fs::read_to_string("tests/data/test_data.backtest.json").expect("Unable to read file");
        let backtest_data_obj: BacktestData =
            serde_json::from_str(&mock_data).expect("JSON was not well-formatted");

        let result = create_backtest(Extension(pool.clone()), Json(backtest_data_obj.clone()))
            .await
            .unwrap()
            .into_response();

        let api_result: ApiResponse<i32> = parse_response(result)
            .await
            .expect("Error parsing response");

        let number = get_id_from_string(&api_result.message);
        let backtest_id = number.clone();

        // Test
        let mut params = HashMap::new();
        params.insert("id".to_string(), number.unwrap().to_string());

        // Test
        let backtest_data = retrieve_backtest(Extension(pool.clone()), Query(params))
            .await
            .unwrap()
            .into_response();

        // Validate
        let api_result: ApiResponse<Vec<BacktestData>> = parse_response(backtest_data)
            .await
            .expect("Error parsing response");

        assert_eq!(api_result.data[0].parameters, backtest_data_obj.parameters);

        // Cleanup
        if backtest_id.is_some() {
            let _ = delete_backtest(Extension(pool.clone()), Json(number.unwrap())).await;
        }
    }

    #[sqlx::test]
    #[serial]
    async fn test_retrieve_backtest_by_name() {
        dotenv::dotenv().ok();
        let pool = init_db().await.unwrap();

        // Pull test data
        let mock_data =
            fs::read_to_string("tests/data/test_data.backtest.json").expect("Unable to read file");
        let backtest_data_obj: BacktestData =
            serde_json::from_str(&mock_data).expect("JSON was not well-formatted");

        let result = create_backtest(Extension(pool.clone()), Json(backtest_data_obj.clone()))
            .await
            .unwrap()
            .into_response();

        let api_result: ApiResponse<i32> = parse_response(result)
            .await
            .expect("Error parsing response");

        let number = get_id_from_string(&api_result.message);
        let backtest_id = number.clone();

        // Test
        let mut params = HashMap::new();
        params.insert("name".to_string(), "testing123".to_string());

        // Test
        let backtest_data = retrieve_backtest(Extension(pool.clone()), Query(params))
            .await
            .unwrap()
            .into_response();

        // Validate
        let api_result: ApiResponse<Vec<BacktestData>> = parse_response(backtest_data)
            .await
            .expect("Error parsing response");

        assert_eq!(api_result.data[0].parameters, backtest_data_obj.parameters);

        // Cleanup
        if backtest_id.is_some() {
            let _ = delete_backtest(Extension(pool.clone()), Json(number.unwrap())).await;
        }
    }

    #[sqlx::test]
    #[serial]
    async fn test_delete_backtest() {
        dotenv::dotenv().ok();
        let pool = init_db().await.unwrap();

        // Test
        let result = delete_backtest(Extension(pool.clone()), Json(63))
            .await
            .unwrap()
            .into_response();

        // Validate
        let api_result: ApiResponse<String> = parse_response(result)
            .await
            .expect("Error parsing response");

        assert_eq!(api_result.code, StatusCode::OK);
    }
}
