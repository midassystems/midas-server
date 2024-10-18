use dotenv::dotenv;
use historical::database::init::init_db;
use historical::logger::system_logger;
use historical::router::router;
use historical::Result;
use std::env;
use std::net::SocketAddr;

#[tokio::main]
async fn main() -> Result<()> {
    // Load environment variables from .env if available
    dotenv().ok();

    // Setup Logging
    /*     let file_path = env::var("API_LOG_FILE")?; */
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

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    tracing::info!("Listening on {}", addr);

    // Run the server
    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await
        .expect("error on connection.");

    Ok(())
}
