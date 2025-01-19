use anyhow::Result;
use sqlx::{postgres::PgPoolOptions, PgPool};

// Init Databases
pub async fn init_db() -> Result<PgPool> {
    let database_url = std::env::var("INSTRUMENT_DATABASE_URL")?;

    let db_pool = PgPoolOptions::new()
        .max_connections(10)
        .connect(&database_url)
        .await?;
    Ok(db_pool)
}
