use crate::services::load::create_service;
use crate::services::retrieve::get_service;
use axum::{extract::Extension, Router};
use dotenv::dotenv;
use sqlx::PgPool;

pub fn router(pool: PgPool) -> Router {
    // Load environment variables from .env if available
    dotenv().ok();

    Router::new().nest(
        "/historical",
        Router::new().nest(
            "/mbp",
            Router::new()
                .nest("/create", create_service().layer(Extension(pool.clone())))
                .nest("/get", get_service().layer(Extension(pool.clone()))),
        ),
    )
}
