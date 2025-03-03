use std::sync::Arc;

use crate::historical::router::router as historical_router;
use crate::instrument::router::router as instrument_router;
use crate::pool::DatabaseState;
use crate::trading::router::router as trading_router;
use axum::Router;

pub fn router(state: Arc<DatabaseState>) -> Router {
    Router::new()
        .nest("/historical", historical_router(state.clone()))
        .nest("/trading", trading_router(state.clone()))
        .nest("/instruments", instrument_router(state.clone()))
}
