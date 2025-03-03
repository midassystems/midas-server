use std::sync::Arc;

use super::services::backtest::backtest_service;
use super::services::live::live_service;
use crate::pool::DatabaseState;
use axum::{extract::Extension, Router};

pub fn router(state: Arc<DatabaseState>) -> Router {
    Router::new()
        .nest(
            "/backtest",
            backtest_service().layer(Extension(state.clone())),
        )
        .nest("/live", live_service().layer(Extension(state.clone())))
}
