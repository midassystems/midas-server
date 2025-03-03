use application::logger::system_logger;
use application::pool::DatabaseState;
use application::router::router;
use application::Result;
use dotenv::dotenv;
use std::env;
use std::sync::Arc;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<()> {
    // Load environment variables from .env if available
    dotenv().ok();
    let historical_db_url = std::env::var("HISTORICAL_DATABASE_URL")?;
    let trading_db_url = std::env::var("TRADING_DATABASE_URL")?;
    let log_file = env::var("LOG_FILE").unwrap_or_else(|_| "app/logs/midas-api.log".to_string());
    let log_level = env::var("LOG_LEVEL").unwrap_or_else(|_| "info".to_string());

    // Setup Logging
    let _ = system_logger(log_file, log_level)?;

    // Initialize the database and obtain a connection pool
    let db_state = DatabaseState::new(&historical_db_url, &trading_db_url).await?;
    let state = Arc::new(db_state);

    // Initialize the Axum routing service
    let app = router(state);

    // Define the address to bind to
    let port: u16 = env::var("API_PORT")
        .expect("PORT environment variable is not set.") // Error if PORT is not set
        .parse()
        .expect("PORT environment variable is not a valid u16 integer."); // Error if PORT is not valid

    // Path
    let path = format!("0.0.0.0:{}", port);
    let listener = TcpListener::bind(path.clone()).await.unwrap();
    tracing::info!("Listening on {}", path);

    // Run the server
    axum::serve(listener, app).await.unwrap();

    Ok(())
}
