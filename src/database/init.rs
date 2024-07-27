use anyhow::Result;
use sqlx::{postgres::PgPoolOptions, PgPool};

// Init Databases
pub async fn init_pg_db() -> Result<PgPool> {
    let database_url = std::env::var("TRADING_DATABASE_URL")?;
    let postgres_pool = PgPoolOptions::new()
        .max_connections(10)
        .connect(&database_url)
        .await?;
    Ok(postgres_pool)
}

pub async fn init_quest_db() -> Result<PgPool> {
    let database_url = std::env::var("MARKET_DATABASE_URL")?;
    let questdb_pool = PgPoolOptions::new()
        .max_connections(10)
        .connect(&database_url)
        .await?;
    Ok(questdb_pool)
}
