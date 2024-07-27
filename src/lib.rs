mod api;
pub mod database;
pub mod error;
pub mod logger;
mod mbn;
pub mod response;
pub use self::error::{Error, Result};
use crate::api::{
    backtest::backtest_service, market_data::market_data_service, symbols::instrument_service,
};
use axum::{extract::Extension, Router};
use sqlx::PgPool;

pub fn router(pg_pool: PgPool, quest_pool: PgPool) -> Router {
    Router::new()
        .nest(
            "/market_data",
            Router::new()
                .nest(
                    "/instruments",
                    instrument_service().layer(Extension(quest_pool.clone())),
                )
                .nest(
                    "/mbp",
                    market_data_service().layer(Extension(quest_pool.clone())),
                ),
            // Router::new()
            //     .nest("/instruments", instrument_service())
            //     .nest("/mbp", market_data_service())
            //     .layer(Extension(quest_pool)),
        )
        .nest(
            "/trading",
            Router::new()
                .nest("/backtest", backtest_service())
                .layer(Extension(pg_pool)),
        )
}
