use super::utils::start_transaction;
use crate::database::backtest::{
    create_backtest_related, retrieve_backtest_related, BacktestDataQueries,
};
use crate::error::Result;
use crate::response::ApiResponse;
use axum::http::StatusCode;
use axum::routing::{delete, get, post};
use axum::{Extension, Json, Router};
use mbn::backtest::BacktestData;
use sqlx::PgPool;

// Service
pub fn backtest_service() -> Router {
    Router::new()
        .route("/create", post(create_backtest))
        .route("/delete", delete(delete_backtest))
        .route("/get", get(retrieve_backtest))
}

// Handlers
pub async fn create_backtest(
    Extension(pool): Extension<PgPool>,
    Json(backtest_data): Json<BacktestData>,
) -> Result<ApiResponse<i32>> {
    let mut transaction = start_transaction(&pool).await?;

    match create_backtest_related(&mut transaction, &backtest_data).await {
        Ok(id) => {
            let _ = transaction.commit().await;
            Ok(ApiResponse::new(
                "success",
                &format!("Successfully created backtest with id {}", id),
                StatusCode::OK,
                Some(id),
            ))
        }
        Err(e) => {
            let _ = transaction.rollback().await;
            Err(e.into())
        }
    }
}

pub async fn retrieve_backtest(
    Extension(pool): Extension<PgPool>,
    Json(id): Json<i32>,
) -> Result<ApiResponse<BacktestData>> {
    match retrieve_backtest_related(&pool, id).await {
        Ok(backtest) => Ok(ApiResponse::new(
            "success",
            "",
            StatusCode::OK,
            Some(backtest),
        )),
        Err(e) => Err(e.into()),
    }
}

pub async fn delete_backtest(
    Extension(pool): Extension<PgPool>,
    Json(id): Json<i32>,
) -> Result<ApiResponse<()>> {
    let mut transaction = start_transaction(&pool).await?;
    match BacktestData::delete_query(&mut transaction, id).await {
        Ok(()) => {
            let _ = transaction.commit().await;
            Ok(ApiResponse::new(
                "success",
                &format!("Successfully deleted backtest with id {}.", id),
                StatusCode::OK,
                None,
            ))
        }
        Err(e) => {
            let _ = transaction.rollback().await;
            Err(e.into())
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::database::init::init_pg_db;
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
        let pool = init_pg_db().await.unwrap();

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
    async fn test_retrieve_backtest() {
        dotenv::dotenv().ok();
        let pool = init_pg_db().await.unwrap();

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
        let _ = retrieve_backtest(Extension(pool.clone()), Json(number.unwrap()))
            .await
            .unwrap();

        // Cleanup
        if backtest_id.is_some() {
            let _ = delete_backtest(Extension(pool.clone()), Json(number.unwrap())).await;
        }
    }

    #[sqlx::test]
    #[serial]
    async fn test_delete_backtest() {
        dotenv::dotenv().ok();
        let pool = init_pg_db().await.unwrap();

        // Test
        let result = delete_backtest(Extension(pool.clone()), Json(63))
            .await
            .unwrap();

        // Validate
        assert_eq!(result.code, StatusCode::OK);
    }
}
