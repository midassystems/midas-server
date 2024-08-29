use anyhow::Result as anyResult;
use sqlx::Column;
use sqlx::Row;

/// Used for debugging purposes.
#[allow(dead_code)]
pub fn print_pg_row(row: &sqlx::postgres::PgRow) -> anyResult<()> {
    for column in row.columns() {
        let column_name = column.name();

        // Attempt to get the value as a string for debugging purposes
        let value: anyResult<String, sqlx::Error> = row.try_get(column_name);
        match value {
            Ok(val) => println!("{:?}: {:?}", column_name, val),
            Err(err) => println!("{}: Error fetching value - {:?}", column_name, err),
        }
    }
    Ok(())
}
