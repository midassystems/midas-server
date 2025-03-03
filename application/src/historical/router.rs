use super::services::load::create_service;
use super::services::retrieve::get_service;
use crate::pool::DatabaseState;
use axum::{extract::Extension, Router};
use std::sync::Arc;

pub fn router(state: Arc<DatabaseState>) -> Router {
    Router::new().nest(
        "/mbp",
        Router::new()
            .nest("/create", create_service().layer(Extension(state.clone())))
            .nest("/get", get_service().layer(Extension(state.clone()))),
    )
}
