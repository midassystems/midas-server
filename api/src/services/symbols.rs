use super::utils::start_transaction;
use crate::database::symbols::InstrumentsQueries;
use crate::error::Result;
use crate::response::ApiResponse;
use axum::http::StatusCode;
use axum::routing::{delete, get, post, put};
use axum::{Extension, Json, Router};
use mbn::symbols::Instrument;
use sqlx::PgPool;

// Service
pub fn instrument_service() -> Router {
    Router::new()
        .route("/create", post(create_instrument))
        .route("/delete", delete(delete_instrument))
        .route("/list", get(list_instruments))
        .route("/update", put(update_instrument_id))
        .route("/get", get(get_instrument))
}

// Handlers
pub async fn create_instrument(
    Extension(pool): Extension<PgPool>,
    Json(instrument): Json<Instrument>,
) -> Result<ApiResponse<i32>> {
    let mut tx = start_transaction(&pool).await?;

    match instrument.insert_instrument(&mut tx).await {
        Ok(id) => {
            let _ = tx.commit().await;
            Ok(ApiResponse::new(
                "success",
                &format!("Successfully created instrument with id {}", id),
                StatusCode::OK,
                Some(id),
            ))
        }
        Err(e) => {
            let _ = tx.rollback().await;
            Err(e.into())
        }
    }
}

pub async fn get_instrument(
    Extension(pool): Extension<PgPool>,
    Json(ticker): Json<String>,
) -> Result<ApiResponse<Option<i32>>> {
    match Instrument::get_instrument_id(&pool, &ticker).await {
        Ok(Some(id)) => Ok(ApiResponse::new(
            "success",
            &format!("Successfully retrieved instrument id {}", id),
            StatusCode::OK,
            Some(Some(id)),
        )),
        Ok(None) => {
            // The ticker was not found, return a successful response with None
            Ok(ApiResponse::new(
                "success",
                &format!("No instrument found for ticker {}", ticker),
                StatusCode::NOT_FOUND,
                None,
            ))
        }
        Err(e) => {
            // Handle the error case
            Err(e.into())
        }
    }
}

pub async fn delete_instrument(
    Extension(pool): Extension<PgPool>,
    Json(id): Json<i32>,
) -> Result<ApiResponse<()>> {
    let mut tx = start_transaction(&pool).await?;

    match Instrument::delete_instrument(&mut tx, id).await {
        Ok(()) => {
            let _ = tx.commit().await;
            Ok(ApiResponse::<()>::new(
                "success",
                &format!("Successfully deleted instrument with id {}", id),
                StatusCode::OK,
                None,
            ))
        }
        Err(e) => {
            let _ = tx.rollback().await;
            Err(e.into())
        }
    }
}

pub async fn list_instruments(
    Extension(pool): Extension<PgPool>,
) -> Result<ApiResponse<Vec<Instrument>>> {
    match Instrument::list_instruments(&pool).await {
        Ok(instruments) => Ok(ApiResponse::new(
            "success",
            "Successfully retrieved list of instruments.",
            StatusCode::OK,
            Some(instruments),
        )),
        Err(e) => Err(e.into()),
    }
}

pub async fn update_instrument_id(
    Extension(pool): Extension<PgPool>,
    Json((instrument, id)): Json<(Instrument, i32)>,
) -> Result<ApiResponse<()>> {
    let mut tx = start_transaction(&pool).await?;

    match instrument.update_instrument(&mut tx, id).await {
        Ok(()) => {
            let _ = tx.commit().await;
            Ok(ApiResponse::new(
                "success",
                &format!("Successfully updated instrument with id {}", id),
                StatusCode::OK,
                None,
            ))
        }
        Err(e) => {
            let _ = tx.rollback().await;
            Err(e.into())
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::database::init::init_market_db;
    use regex::Regex;
    use serial_test::serial;

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
    async fn test_create_instrument() {
        dotenv::dotenv().ok();
        let pool = init_market_db().await.unwrap();

        let instrument = Instrument::new("AAPL", "Apple Inc.", None);

        // Test
        let result = create_instrument(Extension(pool.clone()), Json(instrument))
            .await
            .unwrap();

        // Validate
        assert_eq!(result.code, StatusCode::OK);
        assert!(result
            .message
            .contains("Successfully created instrument with id"));

        // Cleanup
        let number = get_id_from_string(&result.message);
        if number.is_some() {
            let _ = delete_instrument(Extension(pool.clone()), Json(number.unwrap())).await;
        }
    }

    #[sqlx::test]
    #[serial]
    async fn test_get_instrument_id() {
        dotenv::dotenv().ok();
        let pool = init_market_db().await.unwrap();

        let instrument = Instrument::new("AAPL", "Apple Inc.", None);

        let result = create_instrument(Extension(pool.clone()), Json(instrument))
            .await
            .unwrap();

        // Test
        let response = get_instrument(Extension(pool.clone()), Json("AAPL".to_string()))
            .await
            .unwrap();

        // Validate
        assert_eq!(response.code, StatusCode::OK);
        assert!(response
            .message
            .contains("Successfully retrieved instrument id"));

        // Cleanup
        let number = get_id_from_string(&result.message);
        if number.is_some() {
            let _ = delete_instrument(Extension(pool.clone()), Json(number.unwrap())).await;
        }
    }

    #[sqlx::test]
    #[serial]
    async fn test_get_instrument_id_none() {
        dotenv::dotenv().ok();
        let pool = init_market_db().await.unwrap();

        // Test
        let response = get_instrument(Extension(pool.clone()), Json("AAPL".to_string()))
            .await
            .unwrap();

        // Validate
        assert_eq!(response.code, StatusCode::NOT_FOUND);
    }

    #[sqlx::test]
    #[serial]
    async fn test_list_instruments() {
        dotenv::dotenv().ok();
        let pool = init_market_db().await.unwrap();
        let mut transaction = pool.begin().await.expect("Error settign up database.");

        // Create Instruments
        let mut ids: Vec<i32> = vec![];

        let ticker = "AAPL";
        let name = "Apple Inc.";
        let instrument = Instrument::new(ticker, name, None);
        let id = instrument
            .insert_instrument(&mut transaction)
            .await
            .expect("Error inserting symbol.");
        ids.push(id);

        let ticker = "TSLA";
        let name = "Tesle Inc.";
        let instrument = Instrument::new(ticker, name, None);
        let id2 = instrument
            .insert_instrument(&mut transaction)
            .await
            .expect("Error inserting symbol.");
        ids.push(id2);

        let _ = transaction.commit().await;

        // Test
        let result = list_instruments(Extension(pool.clone())).await.unwrap();

        // Validate
        assert_eq!(result.code, StatusCode::OK);
        assert!(result.data.unwrap().len() > 0);

        // Clean up
        let mut transaction = pool
            .begin()
            .await
            .expect("Error setting up test transaction.");
        for id in ids {
            Instrument::delete_instrument(&mut transaction, id)
                .await
                .expect("Error on delete.");
        }

        let _ = transaction.commit().await;
    }

    #[sqlx::test]
    #[serial]
    async fn test_update_instrument() {
        dotenv::dotenv().ok();
        let pool = init_market_db().await.unwrap();
        let mut transaction = pool.begin().await.expect("Error settign up database.");

        // Create Instrument
        let ticker = "AAPL";
        let name = "Apple Inc.";
        let instrument = Instrument::new(ticker, name, None);
        let id = instrument
            .insert_instrument(&mut transaction)
            .await
            .expect("Error inserting symbol.");
        let _ = transaction.commit().await;

        // Test
        let new_ticker = "TSLA";
        let new_name = "Tesla Inc.";
        let new_instrument = Instrument::new(new_ticker, new_name, None);

        let result = update_instrument_id(Extension(pool.clone()), Json((new_instrument, id)))
            .await
            .expect("Error on updating instrument.");

        // Validate
        assert_eq!(result.code, StatusCode::OK);

        // Cleanup
        let mut transaction = pool
            .begin()
            .await
            .expect("Error setting up test transaction.");
        Instrument::delete_instrument(&mut transaction, id)
            .await
            .expect("Error on delete.");
        let _ = transaction.commit().await;
    }
}
