use crate::services::backtest::backtest_service;
use crate::services::live::live_service;
use axum::{extract::Extension, Router};
use dotenv::dotenv;
use sqlx::PgPool;

pub fn router(pool: PgPool) -> Router {
    // Load environment variables from .env if available
    dotenv().ok();

    Router::new().nest(
        "/trading",
        Router::new()
            .nest(
                "/backtest",
                backtest_service().layer(Extension(pool.clone())),
            )
            .nest("/live", live_service().layer(Extension(pool.clone()))), // .layer(cors.clone()),
    )
}
