use super::utils::start_transaction;
use crate::database::live::{create_live_related, retrieve_live_related, LiveDataQueries};
use crate::error::Result;
use crate::response::ApiResponse;
use crate::Error;
use axum::extract::Query;
use axum::http::StatusCode;
use axum::routing::{delete, get, post};
use axum::{Extension, Json, Router};
use mbn::live::LiveData;
use sqlx::PgPool;
use std::collections::hash_map::HashMap;
use tracing::{error, info};

// Service
pub fn live_service() -> Router {
    Router::new()
        .route("/create", post(create_live))
        .route("/delete", delete(delete_live))
        .route("/get", get(retrieve_live))
        .route("/list", get(list_live))
}

// Handlers
pub async fn create_live(
    Extension(pool): Extension<PgPool>,
    Json(live_data): Json<LiveData>,
) -> Result<ApiResponse<i32>> {
    info!("Handling request to create live");

    let mut transaction = start_transaction(&pool).await?;

    match create_live_related(&mut transaction, &live_data).await {
        Ok(id) => {
            let _ = transaction.commit().await;

            info!("Successfully created live with id {}", id);
            Ok(ApiResponse::new(
                "success",
                &format!("Successfully created live with id {}", id),
                StatusCode::OK,
                Some(id),
            ))
        }
        Err(e) => {
            error!("Failed to create live: {:?}", e);
            let _ = transaction.rollback().await;
            Err(e.into())
        }
    }
}

pub async fn list_live(
    Extension(pool): Extension<PgPool>,
) -> Result<ApiResponse<Vec<(i32, String)>>> {
    info!("Handling request to list lives");

    match LiveData::retrieve_list_query(&pool).await {
        Ok(data) => {
            info!("Successfully retrieved live list");
            Ok(ApiResponse::new(
                "success",
                "Successfully retrieved live names.",
                StatusCode::OK,
                Some(data),
            ))
        }
        Err(e) => {
            error!("Failed to list lives: {:?}", e);
            Err(e.into())
        }
    }
}

pub async fn retrieve_live(
    Extension(pool): Extension<PgPool>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<ApiResponse<LiveData>> {
    info!("Handling request to retrieve live");

    let id: i32 = params
        .get("id")
        .and_then(|id_str| id_str.parse().ok())
        .ok_or_else(|| Error::GeneralError("Invalid id parameter".into()))?;

    match retrieve_live_related(&pool, id).await {
        Ok(live) => {
            info!("Successfully retrieved live with id {}", id);
            Ok(ApiResponse::new("success", "", StatusCode::OK, Some(live)))
        }
        Err(e) => {
            error!("Failed to retrieve live with id {}: {:?}", id, e);
            Err(e.into())
        }
    }
}

pub async fn delete_live(
    Extension(pool): Extension<PgPool>,
    Json(id): Json<i32>,
) -> Result<ApiResponse<()>> {
    info!("Handling request to delete live with id {}", id);

    let mut transaction = start_transaction(&pool).await?;
    match LiveData::delete_query(&mut transaction, id).await {
        Ok(()) => {
            let _ = transaction.commit().await;

            info!("Successfully deleted live with id {}", id);
            Ok(ApiResponse::new(
                "success",
                &format!("Successfully deleted live with id {}.", id),
                StatusCode::OK,
                None,
            ))
        }
        Err(e) => {
            error!("Failed to delete live with id {}: {:?}", id, e);
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
    async fn test_create_live() {
        dotenv::dotenv().ok();
        let pool = init_db().await.unwrap();

        // Pull test data
        let mock_data =
            fs::read_to_string("tests/data/test_data.live.json").expect("Unable to read file");
        let live_data: LiveData =
            serde_json::from_str(&mock_data).expect("JSON was not well-formatted");

        // Test
        let result = create_live(Extension(pool.clone()), Json(live_data))
            .await
            .unwrap();

        // Validate
        assert_eq!(result.code, StatusCode::OK);
        assert!(result.message.contains("Successfully created live with id"));

        // Cleanup
        let number = get_id_from_string(&result.message);
        if number.is_some() {
            let _ = delete_live(Extension(pool.clone()), Json(number.unwrap())).await;
        }
    }

    #[sqlx::test]
    #[serial]
    async fn test_list_live() {
        dotenv::dotenv().ok();
        let pool = init_db().await.unwrap();

        // Pull test data
        let mock_data =
            fs::read_to_string("tests/data/test_data.live.json").expect("Unable to read file");
        let live_data: LiveData =
            serde_json::from_str(&mock_data).expect("JSON was not well-formatted");

        let result = create_live(Extension(pool.clone()), Json(live_data))
            .await
            .unwrap();

        let number = get_id_from_string(&result.message);
        let live_id = number.clone();

        // Test
        let lives = list_live(Extension(pool.clone())).await.unwrap();

        // Validate
        assert!(lives.data.unwrap().len() > 0);

        // Cleanup
        if live_id.is_some() {
            let _ = delete_live(Extension(pool.clone()), Json(number.unwrap())).await;
        }
    }

    #[sqlx::test]
    #[serial]
    async fn test_retrieve_live() {
        dotenv::dotenv().ok();
        let pool = init_db().await.unwrap();

        // Pull test data
        let mock_data =
            fs::read_to_string("tests/data/test_data.live.json").expect("Unable to read file");
        let live_data_obj: LiveData =
            serde_json::from_str(&mock_data).expect("JSON was not well-formatted");

        let result = create_live(Extension(pool.clone()), Json(live_data_obj.clone()))
            .await
            .unwrap();

        let number = get_id_from_string(&result.message);
        let live_id = number.clone();

        // Test
        let mut params = HashMap::new();
        params.insert("id".to_string(), number.unwrap().to_string());

        // Test
        let live_data = retrieve_live(Extension(pool.clone()), Query(params))
            .await
            .unwrap();

        // Validate
        assert_eq!(live_data.data.unwrap().parameters, live_data_obj.parameters);

        // Cleanup
        if live_id.is_some() {
            let _ = delete_live(Extension(pool.clone()), Json(number.unwrap())).await;
        }
    }

    #[sqlx::test]
    #[serial]
    async fn test_delete_live() {
        dotenv::dotenv().ok();
        let pool = init_db().await.unwrap();

        // Test
        let result = delete_live(Extension(pool.clone()), Json(63))
            .await
            .unwrap();

        // Validate
        assert_eq!(result.code, StatusCode::OK);
    }
}
