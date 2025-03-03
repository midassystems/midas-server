use std::sync::Arc;

use crate::error::{Error, Result};
use sqlx::{PgPool, Postgres, Transaction};

pub async fn start_transaction(pool: &Arc<PgPool>) -> Result<Transaction<'_, Postgres>> {
    pool.begin()
        .await
        .map_err(|_| Error::CustomError("Failed to connect to database.".into()))
}
