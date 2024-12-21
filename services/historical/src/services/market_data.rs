pub mod load;
pub mod loader;
pub mod retrieve;
pub mod retriever;

use crate::services::market_data::load::{bulk_upload, create_record};
use crate::services::market_data::retrieve::get_records;
use axum::{
    routing::{get, post},
    Router,
};

// Service
pub fn market_data_service() -> Router {
    Router::new()
        .route("/create", post(create_record))
        .route("/get", get(get_records))
        .route("/bulk_upload", post(bulk_upload))
}
