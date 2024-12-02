use super::utils::start_transaction;
use crate::database::symbols::InstrumentsQueries;
use crate::error::Result;
use crate::response::ApiResponse;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{delete, get, post, put};
use axum::{Extension, Json, Router};
use mbn::symbols::Instrument;
use sqlx::PgPool;
use tracing::{error, info};

// TODO: Update time tracing
// let span = tracing::info_span!("create_instrument", instrument_name = %instrument.name);
//  let _enter = span.enter();

// Service
pub fn instrument_service() -> Router {
    Router::new()
        .route("/create", post(create_instrument))
        .route("/delete", delete(delete_instrument))
        .route("/list", get(list_instruments))
        .route("/vendor_list", get(vendor_list_instruments))
        .route("/update", put(update_instrument_id))
        .route("/get", get(get_instrument))
}

// Handlers
pub async fn create_instrument(
    Extension(pool): Extension<PgPool>,
    Json(instrument): Json<Instrument>,
) -> Result<impl IntoResponse> {
    info!("Handling request to create an instrument {:?}", instrument);

    // Start the transaction
    let mut tx = start_transaction(&pool).await?;

    match instrument.insert_instrument(&mut tx).await {
        Ok(id) => {
            if let Err(commit_err) = tx.commit().await {
                error!("Failed to commit transaction: {:?}", commit_err);
                return Err(commit_err.into());
            }

            info!("Successfully created instrument with id {}", id);
            Ok(ApiResponse::new(
                "success",
                &format!("Successfully created instrument with id {}", id),
                StatusCode::OK,
                id,
            ))
        }
        Err(e) => {
            error!("Failed to create instrument: {:?}", e);
            let _ = tx.rollback().await;
            Err(e.into())
        }
    }
}

pub async fn get_instrument(
    Extension(pool): Extension<PgPool>,
    Json(ticker): Json<String>,
) -> Result<impl IntoResponse> {
    info!("Handling request to get instrument {}", ticker);

    match Instrument::get_instrument_id(&pool, &ticker).await {
        Ok(Some(id)) => Ok(ApiResponse::new(
            "success",
            &format!("Successfully retrieved instrument id {}", id),
            StatusCode::OK,
            id,
        )),
        Ok(None) => {
            info!("No instrument found for ticker {}", ticker);
            // The ticker was not found, return a successful response with None
            Ok(ApiResponse::new(
                "success",
                &format!("No instrument found for ticker {}", ticker),
                StatusCode::NOT_FOUND,
                0,
            ))
        }
        Err(e) => {
            error!("Failed to retrieve instrument: {:?}", e);
            // Handle the error case
            Err(e.into())
        }
    }
}

pub async fn delete_instrument(
    Extension(pool): Extension<PgPool>,
    Json(id): Json<i32>,
) -> Result<impl IntoResponse> {
    info!("Handling request to delete instrument with id {}", id);

    let mut tx = start_transaction(&pool).await?;

    match Instrument::delete_instrument(&mut tx, id).await {
        Ok(()) => {
            // Commit the transaction upon success
            if let Err(commit_err) = tx.commit().await {
                error!("Failed to commit transaction: {:?}", commit_err);
                return Err(commit_err.into());
            }

            info!("Successfully deleted instrument with id {}", id);
            Ok(ApiResponse::<String>::new(
                "success",
                &format!("Successfully deleted instrument with id {}", id),
                StatusCode::OK,
                "".to_string(),
            ))
        }
        Err(e) => {
            // Rollback on error and log the failure
            error!("Failed to delete instrument: {:?}", e);
            let _ = tx.rollback().await;
            Err(e.into())
        }
    }
}

pub async fn list_instruments(Extension(pool): Extension<PgPool>) -> Result<impl IntoResponse> {
    info!("Handling request to list instruments");

    match Instrument::list_instruments(&pool).await {
        Ok(instruments) => Ok(ApiResponse::new(
            "success",
            "Successfully retrieved list of instruments.",
            StatusCode::OK,
            instruments,
        )),
        Err(e) => {
            error!("Failed to retrieve instrument list : {:?}", e);
            Err(e.into())
        }
    }
}

pub async fn vendor_list_instruments(
    Extension(pool): Extension<PgPool>,
    Json(vendor): Json<String>,
) -> Result<impl IntoResponse> {
    info!("Handling request to list {} instruments", vendor);

    match Instrument::vendor_list_instruments(&pool, &vendor).await {
        Ok(instruments) => Ok(ApiResponse::new(
            "success",
            "Successfully retrieved vendor list of instruments.",
            StatusCode::OK,
            instruments,
        )),
        Err(e) => {
            error!("Failed to retrieve instrument list : {:?}", e);
            Err(e.into())
        }
    }
}

pub async fn update_instrument_id(
    Extension(pool): Extension<PgPool>,
    Json((instrument, id)): Json<(Instrument, i32)>,
) -> Result<impl IntoResponse> {
    info!(
        "Handling request to update instrument with id {} with {:?}",
        id, instrument
    );

    let mut tx = start_transaction(&pool).await?;

    match instrument.update_instrument(&mut tx, id).await {
        Ok(()) => {
            // Commit the transaction upon success
            if let Err(commit_err) = tx.commit().await {
                error!("Failed to commit transaction: {:?}", commit_err);
                return Err(commit_err.into());
            }

            info!("Successfully udpated instrument with id {}", id);
            Ok(ApiResponse::new(
                "success",
                &format!("Successfully updated instrument with id {}", id),
                StatusCode::OK,
                "".to_string(),
            ))
        }
        Err(e) => {
            // Rollback on error and log the failure
            error!("Failed to update instrument: {:?}", e);
            let _ = tx.rollback().await;
            Err(e.into())
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::database::init::init_db;
    use hyper::body::to_bytes;
    use mbn::symbols::Vendors;
    use regex::Regex;
    use serde::de::DeserializeOwned;
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

    async fn parse_response<T: DeserializeOwned>(
        response: axum::response::Response,
    ) -> anyhow::Result<ApiResponse<T>> {
        // Extract the body as bytes
        let body_bytes = to_bytes(response.into_body()).await.unwrap();
        let body_text = String::from_utf8(body_bytes.to_vec()).unwrap();

        // Deserialize the response body to ApiResponse for further assertions
        let api_response: ApiResponse<T> = serde_json::from_str(&body_text).unwrap();
        Ok(api_response)
    }

    #[sqlx::test]
    #[serial]
    async fn test_create_instrument() {
        dotenv::dotenv().ok();
        let pool = init_db().await.unwrap();

        let instrument = Instrument::new(
            None,
            "AAPL",
            "Apple Inc.",
            Vendors::Databento,
            Some("continuous".to_string()),
            Some("GLBX.MDP3".to_string()),
            1704672000000000000,
            1704672000000000000,
            true,
        );

        // Test
        let result = create_instrument(Extension(pool.clone()), Json(instrument))
            .await
            .unwrap()
            .into_response();

        // Validate
        let api_response: ApiResponse<i32> = parse_response(result)
            .await
            .expect("Error parsing response");

        assert_eq!(api_response.code, StatusCode::OK);

        // Cleanup
        let number = get_id_from_string(&api_response.message);
        if number.is_some() {
            let _ = delete_instrument(Extension(pool.clone()), Json(number.unwrap())).await;
        }
    }

    #[sqlx::test]
    #[serial]
    async fn test_get_instrument_id() {
        dotenv::dotenv().ok();
        let pool = init_db().await.unwrap();

        let instrument = Instrument::new(
            None,
            "AAPL",
            "Apple Inc.",
            Vendors::Databento,
            Some("continuous".to_string()),
            Some("GLBX.MDP3".to_string()),
            1704672000000000000,
            1704672000000000000,
            true,
        );

        let result = create_instrument(Extension(pool.clone()), Json(instrument))
            .await
            .unwrap()
            .into_response();

        // Test
        let response = get_instrument(Extension(pool.clone()), Json("AAPL".to_string()))
            .await
            .unwrap()
            .into_response();

        // Validate
        let api_response: ApiResponse<i32> = parse_response(response)
            .await
            .expect("Error parsing response");

        assert_eq!(api_response.code, StatusCode::OK);
        assert!(api_response
            .message
            .contains("Successfully retrieved instrument id"));

        // Cleanup
        let api_result: ApiResponse<i32> = parse_response(result)
            .await
            .expect("Error parsing response");
        let number = get_id_from_string(&api_result.message);
        if number.is_some() {
            let _ = delete_instrument(Extension(pool.clone()), Json(number.unwrap())).await;
        }
    }

    #[sqlx::test]
    #[serial]
    async fn test_get_instrument_id_none() {
        dotenv::dotenv().ok();
        let pool = init_db().await.unwrap();

        // Test
        let result = get_instrument(Extension(pool.clone()), Json("AAPL".to_string()))
            .await
            .unwrap()
            .into_response();

        // Validate
        let api_response: ApiResponse<i32> = parse_response(result)
            .await
            .expect("Error parsing response");

        assert_eq!(api_response.code, StatusCode::NOT_FOUND);
    }

    #[sqlx::test]
    #[serial]
    async fn test_list_instruments() {
        dotenv::dotenv().ok();
        let pool = init_db().await.unwrap();
        let mut transaction = pool.begin().await.expect("Error settign up database.");

        // Create Instruments
        let mut ids: Vec<i32> = vec![];

        let ticker = "AAPL";
        let name = "Apple Inc.";
        let instrument = Instrument::new(
            None,
            ticker,
            name,
            Vendors::Databento,
            Some("continuous".to_string()),
            Some("GLBX.MDP3".to_string()),
            1704672000000000000,
            1704672000000000000,
            true,
        );
        let id = instrument
            .insert_instrument(&mut transaction)
            .await
            .expect("Error inserting symbol.");
        ids.push(id);

        let ticker = "TSLA";
        let name = "Tesle Inc.";
        let instrument = Instrument::new(
            None,
            ticker,
            name,
            Vendors::Databento,
            Some("continuous".to_string()),
            Some("GLBX.MDP3".to_string()),
            1704672000000000000,
            1704672000000000000,
            true,
        );
        let id2 = instrument
            .insert_instrument(&mut transaction)
            .await
            .expect("Error inserting symbol.");
        ids.push(id2);

        let _ = transaction.commit().await;

        // Test
        let result = list_instruments(Extension(pool.clone()))
            .await
            .unwrap()
            .into_response();

        // Validate
        let api_response: ApiResponse<Vec<Instrument>> = parse_response(result)
            .await
            .expect("Error parsing response");

        assert_eq!(api_response.code, StatusCode::OK);
        assert!(api_response.data.len() > 0);

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
    async fn test_vendor_list_instruments() {
        dotenv::dotenv().ok();
        let pool = init_db().await.unwrap();
        let mut transaction = pool.begin().await.expect("Error settign up database.");

        // Create Instruments
        let mut ids: Vec<i32> = vec![];

        let ticker = "AAPL";
        let name = "Apple Inc.";
        let instrument = Instrument::new(
            None,
            ticker,
            name,
            Vendors::Databento,
            Some("continuous".to_string()),
            Some("GLBX.MDP3".to_string()),
            1704672000000000000,
            1704672000000000000,
            true,
        );
        let id = instrument
            .insert_instrument(&mut transaction)
            .await
            .expect("Error inserting symbol.");
        ids.push(id);

        let ticker = "TSLA";
        let name = "Tesle Inc.";
        let instrument = Instrument::new(
            None,
            ticker,
            name,
            Vendors::Yfinance,
            Some("continuous".to_string()),
            Some("GLBX.MDP3".to_string()),
            1704672000000000000,
            1704672000000000000,
            true,
        );
        let id2 = instrument
            .insert_instrument(&mut transaction)
            .await
            .expect("Error inserting symbol.");
        ids.push(id2);

        let _ = transaction.commit().await;

        // Test
        let result = vendor_list_instruments(Extension(pool.clone()), Json("yfinance".to_string()))
            .await
            .unwrap()
            .into_response();

        // Validate
        let api_response: ApiResponse<Vec<Instrument>> = parse_response(result)
            .await
            .expect("Error parsing response");

        assert_eq!(api_response.code, StatusCode::OK);
        assert!(api_response.data.len() > 0);

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
        let pool = init_db().await.unwrap();
        let mut transaction = pool.begin().await.expect("Error settign up database.");

        // Create Instrument
        let ticker = "AAPL";
        let name = "Apple Inc.";
        let instrument = Instrument::new(
            None,
            ticker,
            name,
            Vendors::Databento,
            Some("continuous".to_string()),
            Some("GLBX.MDP3".to_string()),
            1704672000000000000,
            1704672000000000000,
            true,
        );
        let id = instrument
            .insert_instrument(&mut transaction)
            .await
            .expect("Error inserting symbol.");
        let _ = transaction.commit().await;

        // Test
        let new_ticker = "TSLA";
        let new_name = "Tesla Inc.";
        let new_instrument = Instrument::new(
            None,
            new_ticker,
            new_name,
            Vendors::Databento,
            Some("continuous".to_string()),
            Some("GLBX.MDP3".to_string()),
            1704672000000000000,
            1704672000000000000,
            true,
        );

        let result = update_instrument_id(Extension(pool.clone()), Json((new_instrument, id)))
            .await
            .expect("Error on updating instrument.")
            .into_response();

        // Validate
        let api_response: ApiResponse<String> = parse_response(result)
            .await
            .expect("Error parsing response");

        assert_eq!(api_response.code, StatusCode::OK);

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
