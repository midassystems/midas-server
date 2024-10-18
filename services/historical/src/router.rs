use crate::services::{market_data::market_data_service, symbols::instrument_service};
use axum::{extract::Extension, Router};
use dotenv::dotenv;
use sqlx::PgPool;

pub fn router(pool: PgPool) -> Router {
    // Load environment variables from .env if available
    dotenv().ok();

    Router::new().nest(
        "/historical",
        Router::new()
            .nest(
                "/instruments",
                instrument_service().layer(Extension(pool.clone())),
            )
            .nest("/mbp", market_data_service().layer(Extension(pool.clone()))),
    )
}
