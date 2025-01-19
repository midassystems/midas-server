use crate::Result;
use mbn::enums::Dataset;
use sqlx::Column;
use sqlx::{PgPool, Row};

pub async fn get_lastest_mbp_id(pool: &PgPool, dataset: &Dataset) -> Result<i32> {
    let query = format!("SELECT COALESCE(MAX(id), 0) FROM {}_mbp", dataset.as_str());
    let last_id: i32 = sqlx::query_scalar(&query) //"SELECT COALESCE(MAX(id), 0) FROM mbp")
        .fetch_one(pool)
        .await?;
    Ok(last_id)
}

/// Used for debugging purposes.
#[allow(dead_code)]
pub fn print_pg_row(row: &sqlx::postgres::PgRow) -> anyhow::Result<()> {
    for column in row.columns() {
        let column_name = column.name();

        // Attempt to get the value as a string for debugging purposes
        let value: anyhow::Result<String, sqlx::Error> = row.try_get(column_name);
        match value {
            Ok(val) => println!("{:?}: {:?}", column_name, val),
            Err(err) => println!("{}: Error fetching value - {:?}", column_name, err),
        }
    }
    Ok(())
}
