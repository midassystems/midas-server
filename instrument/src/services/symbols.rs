use super::utils::start_transaction;
use crate::database::symbols::{
    dataset_list_instruments, vendor_list_instruments, InstrumentsQueries,
};
use crate::error::Result;
use crate::response::ApiResponse;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{delete, get, post, put};
use axum::{Extension, Json, Router};
use mbn::enums::Dataset;
use mbn::symbols::Instrument;
use mbn::vendors::Vendors;
use sqlx::PgPool;
use tracing::{error, info};

// TODO: Update time tracing
// let span = tracing::info_span!("create_instrument", instrument_name = %instrument.name);
//  let _enter = span.enter();

// Service
pub fn instrument_service() -> Router {
    Router::new()
        .route("/create", post(create_instrument))
        .route("/get", get(get_instrument))
        .route("/update", put(update_instrument))
        .route("/delete", delete(delete_instrument))
        .route("/list_vendor", get(list_instruments_vendor))
        .route("/list_dataset", get(list_instruments_dataset))
}

// Handlers
pub async fn create_instrument(
    Extension(pool): Extension<PgPool>,
    Json(instrument): Json<Instrument>,
) -> Result<impl IntoResponse> {
    info!("Handling request to create an instrument {:?}", instrument);

    // Start the transaction
    let mut tx = start_transaction(&pool).await?;

    match instrument.create(&mut tx).await {
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
    Json((ticker, dataset)): Json<(String, Dataset)>,
) -> Result<impl IntoResponse> {
    info!("Handling request to get instrument {}", ticker);

    match Instrument::read(&pool, &ticker, dataset).await {
        Ok(vec) => {
            if vec.len() > 0 {
                Ok(ApiResponse::new(
                    "success",
                    &format!("Successfully retrieved instrument."),
                    StatusCode::OK,
                    vec,
                ))
            } else {
                info!("No instrument found for ticker {}", ticker);
                // The ticker was not found, return a successful response with None
                Ok(ApiResponse::new(
                    "success",
                    &format!("No instrument found for ticker {}", ticker),
                    StatusCode::NOT_FOUND,
                    vec![],
                ))
            }
        }
        Err(e) => {
            error!("Failed to retrieve instrument: {:?}", e);
            // Handle the error case
            Err(e.into())
        }
    }
}

pub async fn update_instrument(
    Extension(pool): Extension<PgPool>,
    Json(instrument): Json<Instrument>,
) -> Result<impl IntoResponse> {
    info!("Handling request to update instrument {:?}", instrument);

    let mut tx = start_transaction(&pool).await?;

    match instrument.update(&mut tx).await {
        Ok(()) => {
            // Commit the transaction upon success
            if let Err(commit_err) = tx.commit().await {
                error!("Failed to commit transaction: {:?}", commit_err);
                return Err(commit_err.into());
            }

            info!("Successfully udpated instrument.");
            Ok(ApiResponse::new(
                "success",
                &format!("Successfully updated instrument."),
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

pub async fn delete_instrument(
    Extension(pool): Extension<PgPool>,
    Json(id): Json<i32>,
) -> Result<impl IntoResponse> {
    info!("Handling request to delete instrument with id {}", id);

    let mut tx = start_transaction(&pool).await?;

    match Instrument::delete(&mut tx, id).await {
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

pub async fn list_instruments_vendor(
    Extension(pool): Extension<PgPool>,
    Json((vendor, dataset)): Json<(Vendors, Dataset)>,
) -> Result<impl IntoResponse> {
    info!("Handling request to list instruments");

    match vendor_list_instruments(&pool, vendor, dataset).await {
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

pub async fn list_instruments_dataset(
    Extension(pool): Extension<PgPool>,
    Json(dataset): Json<Dataset>,
) -> Result<impl IntoResponse> {
    info!("Handling request to list {} instruments", dataset);

    match dataset_list_instruments(&pool, dataset).await {
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

#[cfg(test)]
mod test {
    use std::str::FromStr;

    use super::*;
    use crate::database::init::init_db;
    use hyper::body::to_bytes;
    use mbn::vendors::{DatabentoData, VendorData};
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
    async fn test_create_instrument() -> anyhow::Result<()> {
        dotenv::dotenv().ok();
        let pool = init_db().await.unwrap();
        let schema = dbn::Schema::from_str("mbp-1")?;
        let dataset = dbn::Dataset::from_str("GLBX.MDP3")?;
        let stype = dbn::SType::from_str("raw_symbol")?;
        let vendor_data = VendorData::Databento(DatabentoData {
            schema,
            dataset,
            stype,
        });

        let instrument = Instrument::new(
            None,
            "AAPL",
            "Apple Inc.",
            Dataset::Equities,
            Vendors::Databento,
            vendor_data.encode(),
            1704672000000000000,
            1704672000000000000,
            0,
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

        Ok(())
    }

    #[sqlx::test]
    #[serial]
    async fn test_get_instrument() -> anyhow::Result<()> {
        dotenv::dotenv().ok();
        let pool = init_db().await.unwrap();

        let schema = dbn::Schema::from_str("mbp-1")?;
        let dataset = dbn::Dataset::from_str("GLBX.MDP3")?;
        let stype = dbn::SType::from_str("raw_symbol")?;
        let vendor_data = VendorData::Databento(DatabentoData {
            schema,
            dataset,
            stype,
        });

        let instrument = Instrument::new(
            None,
            "AAPL",
            "Apple Inc.",
            Dataset::Equities,
            Vendors::Databento,
            vendor_data.encode(),
            1704672000000000000,
            1704672000000000000,
            0,
            true,
        );
        let result = create_instrument(Extension(pool.clone()), Json(instrument))
            .await
            .unwrap()
            .into_response();

        // Test
        let response = get_instrument(
            Extension(pool.clone()),
            Json(("AAPL".to_string(), Dataset::Equities)),
        )
        .await
        .unwrap()
        .into_response();

        // Validate
        let api_response: ApiResponse<Vec<Instrument>> = parse_response(response)
            .await
            .expect("Error parsing response");

        assert_eq!(api_response.code, StatusCode::OK);
        assert!(api_response.data.len() > 0);

        // Cleanup
        let api_result: ApiResponse<i32> = parse_response(result)
            .await
            .expect("Error parsing response");
        let number = get_id_from_string(&api_result.message);
        if number.is_some() {
            let _ = delete_instrument(Extension(pool.clone()), Json(number.unwrap())).await;
        }
        Ok(())
    }

    #[sqlx::test]
    #[serial]
    // #[ignore]
    async fn test_get_instrument_none() {
        dotenv::dotenv().ok();
        let pool = init_db().await.unwrap();

        // Test
        let result = get_instrument(
            Extension(pool.clone()),
            Json(("AAPL".to_string(), Dataset::Equities)),
        )
        .await
        .unwrap()
        .into_response();

        // Validate
        let api_response: ApiResponse<Vec<Instrument>> = parse_response(result)
            .await
            .expect("Error parsing response");

        assert_eq!(api_response.code, StatusCode::NOT_FOUND);
    }

    #[sqlx::test]
    #[serial]
    async fn test_update_instrument() -> anyhow::Result<()> {
        dotenv::dotenv().ok();
        let pool = init_db().await.unwrap();
        let mut transaction = pool.begin().await.expect("Error settign up database.");

        let schema = dbn::Schema::from_str("mbp-1")?;
        let dataset = dbn::Dataset::from_str("GLBX.MDP3")?;
        let stype = dbn::SType::from_str("raw_symbol")?;
        let vendor_data = VendorData::Databento(DatabentoData {
            schema,
            dataset,
            stype,
        });
        // Create Instrument
        let ticker = "AAPL";
        let name = "Apple Inc.";
        let instrument = Instrument::new(
            None,
            ticker,
            name,
            Dataset::Equities,
            Vendors::Databento,
            vendor_data.encode(),
            1704672000000000000,
            1704672000000000000,
            0,
            true,
        );
        let id = instrument
            .create(&mut transaction)
            .await
            .expect("Error inserting symbol.");
        let _ = transaction.commit().await;

        // Test
        let new_ticker = "TSLA";
        let new_name = "Tesla Inc.";
        let new_instrument = Instrument::new(
            Some(id as u32),
            new_ticker,
            new_name,
            Dataset::Equities,
            Vendors::Databento,
            vendor_data.encode(),
            1704672000000000000,
            1704672000000000000,
            0,
            true,
        );

        let result = update_instrument(Extension(pool.clone()), Json(new_instrument))
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
        Instrument::delete(&mut transaction, id)
            .await
            .expect("Error on delete.");
        let _ = transaction.commit().await;

        Ok(())
    }

    #[sqlx::test]
    #[serial]
    async fn test_list_instruments_vendor() -> anyhow::Result<()> {
        dotenv::dotenv().ok();
        let pool = init_db().await.unwrap();
        let mut transaction = pool.begin().await.expect("Error settign up database.");

        let schema = dbn::Schema::from_str("mbp-1")?;
        let dataset = dbn::Dataset::from_str("GLBX.MDP3")?;
        let stype = dbn::SType::from_str("raw_symbol")?;
        let vendor_data = VendorData::Databento(DatabentoData {
            schema,
            dataset,
            stype,
        });

        // Create Instruments
        let mut ids: Vec<i32> = vec![];

        let ticker = "AAPL";
        let name = "Apple Inc.";
        let instrument = Instrument::new(
            None,
            ticker,
            name,
            Dataset::Equities,
            Vendors::Databento,
            vendor_data.encode(),
            1704672000000000000,
            1704672000000000000,
            0,
            true,
        );
        let id = instrument
            .create(&mut transaction)
            .await
            .expect("Error inserting symbol.");
        ids.push(id);

        let ticker = "TSLA";
        let name = "Tesle Inc.";
        let instrument = Instrument::new(
            None,
            ticker,
            name,
            Dataset::Equities,
            Vendors::Databento,
            vendor_data.encode(),
            1704672000000000000,
            1704672000000000000,
            0,
            true,
        );
        let id2 = instrument
            .create(&mut transaction)
            .await
            .expect("Error inserting symbol.");
        ids.push(id2);

        let _ = transaction.commit().await;

        // Test
        let result = list_instruments_vendor(
            Extension(pool.clone()),
            Json((Vendors::Databento, Dataset::Equities)),
        )
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
            Instrument::delete(&mut transaction, id)
                .await
                .expect("Error on delete.");
        }

        let _ = transaction.commit().await;

        Ok(())
    }

    #[sqlx::test]
    #[serial]
    async fn test_list_instruments_dataset() -> anyhow::Result<()> {
        dotenv::dotenv().ok();
        let pool = init_db().await.unwrap();
        let mut transaction = pool.begin().await.expect("Error settign up database.");

        let schema = dbn::Schema::from_str("mbp-1")?;
        let dataset = dbn::Dataset::from_str("GLBX.MDP3")?;
        let stype = dbn::SType::from_str("raw_symbol")?;
        let vendor_data = VendorData::Databento(DatabentoData {
            schema,
            dataset,
            stype,
        });

        // Create Instruments
        let mut ids: Vec<i32> = vec![];

        let ticker = "AAPL";
        let name = "Apple Inc.";
        let instrument = Instrument::new(
            None,
            ticker,
            name,
            Dataset::Equities,
            Vendors::Databento,
            vendor_data.encode(),
            1704672000000000000,
            1704672000000000000,
            0,
            true,
        );
        let id = instrument
            .create(&mut transaction)
            .await
            .expect("Error inserting symbol.");
        ids.push(id);

        let ticker = "TSLA";
        let name = "Tesle Inc.";
        let instrument = Instrument::new(
            None,
            ticker,
            name,
            Dataset::Equities,
            Vendors::Databento,
            vendor_data.encode(),
            1704672000000000000,
            1704672000000000000,
            0,
            true,
        );
        let id2 = instrument
            .create(&mut transaction)
            .await
            .expect("Error inserting symbol.");
        ids.push(id2);

        let _ = transaction.commit().await;

        // Test
        let result = list_instruments_dataset(Extension(pool.clone()), Json(Dataset::Equities))
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
            Instrument::delete(&mut transaction, id)
                .await
                .expect("Error on delete.");
        }

        let _ = transaction.commit().await;

        Ok(())
    }
}
