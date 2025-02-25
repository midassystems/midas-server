use dotenv::dotenv;
use historical::database::init::init_db;
use historical::logger::system_logger;
use historical::router::router;
use historical::Result;
use std::env;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<()> {
    // Load environment variables from .env if available
    dotenv().ok();

    // Setup Logging
    let _ = system_logger()?;

    // Initialize the database and obtain a connection pool
    let pool = init_db().await.expect("Error on market_data pool.");

    // Initialize the Axum routing service
    let app = router(pool);

    // Define the address to bind to
    let port: u16 = env::var("HISTORICAL_PORT")
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
