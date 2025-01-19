use crate::services::symbols::instrument_service;
use axum::{extract::Extension, Router};
use dotenv::dotenv;
use sqlx::PgPool;

pub fn router(pool: PgPool) -> Router {
    // Load environment variables from .env if available
    dotenv().ok();

    Router::new().nest(
        "/instruments",
        instrument_service().layer(Extension(pool.clone())),
    )
}
