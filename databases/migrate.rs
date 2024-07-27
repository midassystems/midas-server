use sqlx::PgPool;
use std::env;
use std::fs;
use tokio;

#[tokio::main]
async fn main() -> Result<(), sqlx::Error> {
    let args: Vec<String> = env::args().collect();
    if args.len() != 3 {
        eprintln!("Usage: {} <DATABASE_URL> <MIGRATION_FILE>", args[0]);
        std::process::exit(1);
    }

    let database_url = &args[1];
    let migration_file = &args[2];

    let pool = PgPool::connect(database_url).await?;

    let migration_sql = fs::read_to_string(migration_file).expect("Unable to read file");
    sqlx::query(&migration_sql).execute(&pool).await?;

    println!("Migrations applied successfully!");
    Ok(())
}

