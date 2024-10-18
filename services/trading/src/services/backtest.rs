use super::utils::start_transaction;
use crate::database::backtest::{
    create_backtest_related, retrieve_backtest_related, BacktestDataQueries,
};
use crate::error::Result;
use crate::response::ApiResponse;
use crate::Error;
use axum::extract::Query;
use axum::http::StatusCode;
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
) -> Result<ApiResponse<i32>> {
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
                Some(id),
            ))
        }
        Err(e) => {
            error!("Failed to create backtest: {:?}", e);
            let _ = transaction.rollback().await;
            Err(e.into())
        }
    }
}

pub async fn list_backtest(
    Extension(pool): Extension<PgPool>,
) -> Result<ApiResponse<Vec<(i32, String)>>> {
    info!("Handling request to list backtests");

    match BacktestData::retrieve_list_query(&pool).await {
        Ok(data) => {
            info!("Successfully retrieved backtest list");
            Ok(ApiResponse::new(
                "success",
                "Successfully retrieved backtest names.",
                StatusCode::OK,
                Some(data),
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
) -> Result<ApiResponse<BacktestData>> {
    info!("Handling request to retrieve backtest");

    let id: i32 = params
        .get("id")
        .and_then(|id_str| id_str.parse().ok())
        .ok_or_else(|| Error::GeneralError("Invalid id parameter".into()))?;

    match retrieve_backtest_related(&pool, id).await {
        Ok(backtest) => {
            info!("Successfully retrieved backtest with id {}", id);
            Ok(ApiResponse::new(
                "success",
                "",
                StatusCode::OK,
                Some(backtest),
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
) -> Result<ApiResponse<()>> {
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
                None,
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
    use regex::Regex;
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
            .unwrap();

        // Validate
        assert_eq!(result.code, StatusCode::OK);
        assert!(result
            .message
            .contains("Successfully created backtest with id"));

        // Cleanup
        let number = get_id_from_string(&result.message);
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
            .unwrap();

        let number = get_id_from_string(&result.message);
        let backtest_id = number.clone();

        // Test
        let backtests = list_backtest(Extension(pool.clone())).await.unwrap();

        // Validate
        assert!(backtests.data.unwrap().len() > 0);

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
            .unwrap();

        let number = get_id_from_string(&result.message);
        let backtest_id = number.clone();

        // Test
        let mut params = HashMap::new();
        params.insert("id".to_string(), number.unwrap().to_string());

        // Test
        let backtest_data = retrieve_backtest(Extension(pool.clone()), Query(params))
            .await
            .unwrap();

        // Validate
        assert_eq!(
            backtest_data.data.unwrap().parameters,
            backtest_data_obj.parameters
        );

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
            .unwrap();

        // Validate
        assert_eq!(result.code, StatusCode::OK);
    }
}
