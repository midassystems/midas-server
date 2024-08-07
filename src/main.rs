use dotenv::dotenv;
use midasbackend::database::init::{init_pg_db, init_quest_db};
use midasbackend::error::Result;
use midasbackend::logger::system_logger;
use midasbackend::router;
use std::env;
use std::net::SocketAddr;
mod logger;
// use crate::logger::system_logger;

#[tokio::main]
async fn main() -> Result<()> {
    // Load environment variables from .env if available
    dotenv().ok();

    // Setup Tracing
    let file_path = env::var("LOG_FILE")?;
    let _ = system_logger(&file_path, "info");

    // Initialize the database and obtain a connection pool
    let pg_pool = init_pg_db().await.expect("Error on trading_db pool.");
    let quest_pool = init_quest_db().await.expect("Error on market_data pool.");

    // Initialize the Axum routing service
    let app = router(pg_pool, quest_pool);

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
