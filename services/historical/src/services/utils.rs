use crate::error::{Error, Result};
use sqlx::{PgPool, Postgres, Transaction};

pub async fn start_transaction(pool: &PgPool) -> Result<Transaction<'_, Postgres>> {
    pool.begin()
        .await
        .map_err(|_| Error::CustomError("Failed to connect to database.".into()))
}
