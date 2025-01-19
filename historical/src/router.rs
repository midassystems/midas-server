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

// params::RetrieveParams;
// use crate::services::market_data::market_data_service; //, symbols::instrument_service};
// use axum::extract::{Extension, Query};
// use axum::routing::{get, post};
// use axum::Router;
// use dotenv::dotenv;
// use sqlx::PgPool;
//
// pub fn router(pool: PgPool) -> Router {
//     // Load environment variables from .env if available
//     dotenv().ok();
//
//     Router::new().nest(
//         "/historical",
//         Router::new().nest(
//             "/mbp",
//             get(
//                 |pool: Extension<PgPool>, params: Query<RetrieveParams>| async move {
//                     let dataset = params.dataset.as_str();
//                     Router::new().nest(
//                         "/futures",
//                         market_data_service().layer(Extension(pool.clone())),
//                     )
//                 },
//             ), // .nest(
//                //     "/instruments",
//                //     instrument_service().layer(Extension(pool.clone())),
//                // )
//         ),
//     )
// }
//
// pub fn dataset_router() -> Router {
//     Router::new()
//         .route("/futures", futures_service()) //post(create_record))
//         .route("/equities", equity_service()) //post(create_record))
//         .route("/futures", option_service()) //post(create_record))
//
//     // .route("/get", get(get_records))
//     // .route("/bulk_upload", post(bulk_upload))
// }

// pub fn router(pool: PgPool) -> Router {
//     // Load environment variables from .env if available
//     dotenv().ok();
//
//     Router::new().nest(
//         "/historical",
//         Router::new().route(
//             "/mbp",
//             get(
//                 |pool: Extension<PgPool>, params: Query<RetrieveParams>| async move {
//                     let dataset = params.dataset.as_str();
//                     match dataset {
//                         "futures" => Router::new().nest(
//                             "/futures",
//                             market_data_service().layer(Extension(pool.clone())),
//                         ),
//                         "equities" => Router::new().nest(
//                             "/equities",
//                             market_data_service().layer(Extension(pool.clone())),
//                         ),
//                         "option" => Router::new().nest(
//                             "/option",
//                             market_data_service().layer(Extension(pool.clone())),
//                         ),
//                         //         //     "/instruments",
//                         //         //     instrument_service().layer(Extension(pool.clone())),
//                         //         // ) handle_futures_mbp(pool.0, params).await,
//                         // "equities" => handle_equities_mbp(pool.0, params).await,
//                         // "option" => handle_options_mbp(pool.0, params).await,
//                         _ => Err(StatusCode::BAD_REQUEST.into_response()),
//                     }
//                 },
//             ),
//         ),
//     )
// }
