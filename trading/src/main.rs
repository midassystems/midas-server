use dotenv::dotenv;
use std::env;
use tokio::net::TcpListener;
use trading::database::init::init_db;
use trading::logger::system_logger;
use trading::router::router;
use trading::Result;

#[tokio::main]
async fn main() -> Result<()> {
    // Load environment variables from .env if available
    dotenv().ok();

    // Setup Logging
    let _ = system_logger()?;

    // Initialize the database and obtain a connection pool
    let pool = init_db().await.expect("Error on trading_db pool.");

    // Initialize the Axum routing service
    let app = router(pool);

    // Define the address to bind to
    let port: u16 = env::var("TRADING_PORT")
        .expect("PORT environment variable is not set.") // Error if PORT is not set
        .parse()
        .expect("PORT environment variable is not a valid u16 integer."); // Error if PORT is not valid

    // let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    let path = format!("0.0.0.0:{}", port);
    let listener = TcpListener::bind(path.clone()).await.unwrap();
    tracing::info!("Listening on {}", path);

    // Run the server
    axum::serve(listener, app).await.unwrap();
    // axum::serve(listener, app);

    Ok(())
}
