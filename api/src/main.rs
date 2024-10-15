use api::database::init::{init_market_db, init_trading_db};
use api::logger::system_logger;
use api::router::router;
use api::Result;
use dotenv::dotenv;
use std::env;
use std::net::SocketAddr;

#[tokio::main]
async fn main() -> Result<()> {
    // Load environment variables from .env if available
    dotenv().ok();

    // Setup Tracing
    let file_path = env::var("API_LOG_FILE")?;
    let _ = system_logger(&file_path, "info");

    // Initialize the database and obtain a connection pool
    let trading_pool = init_trading_db().await.expect("Error on trading_db pool.");
    let market_pool = init_market_db().await.expect("Error on market_data pool.");

    // Initialize the Axum routing service
    let app = router(trading_pool, market_pool);

    // Define the address to bind to
    // let port: u32 = std::env::var("PORT")?;
    let addr = SocketAddr::from(([0, 0, 0, 0], 8080));

    println!("Listening on {}", addr);

    // Run the server
    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await
        .expect("error on connection.");

    Ok(())
}
