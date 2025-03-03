use super::services::symbols::instrument_service;
use crate::pool::DatabaseState;
use axum::{extract::Extension, Router};
use std::sync::Arc;

pub fn router(state: Arc<DatabaseState>) -> Router {
    Router::new()
        .merge(instrument_service())
        .layer(Extension(state.clone()))
}
