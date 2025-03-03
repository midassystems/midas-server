use crate::Result;
use sqlx::postgres::PgPoolOptions;
use sqlx::postgres::{PgConnectOptions, PgPool};
use sqlx::ConnectOptions;
use std::sync::Arc;
use std::time::Duration;

pub struct DatabaseState {
    pub historical_pool: Arc<PgPool>,
    pub trading_pool: Arc<PgPool>,
}

impl DatabaseState {
    pub async fn new(historical_url: &str, trading_url: &str) -> Result<Self> {
        Ok(DatabaseState {
            historical_pool: Arc::new(init_db(historical_url.to_string()).await?),
            trading_pool: Arc::new(init_db(trading_url.to_string()).await?),
        })
    }
}

// Init Databases
pub async fn init_db(database_url: String) -> Result<PgPool> {
    // let database_url = std::env::var("HISTORICAL_DATABASE_URL")?;

    // URL connection string
    let mut opts: PgConnectOptions = database_url.parse()?;
    opts = opts.log_slow_statements(log::LevelFilter::Debug, Duration::from_secs(1));

    let db_pool = PgPoolOptions::new()
        .max_connections(100)
        .connect_with(opts)
        .await?;
    Ok(db_pool)
}
