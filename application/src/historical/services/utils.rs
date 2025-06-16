use crate::error::{Error, Result};
use mbinary::enums::Dataset;
use mbinary::symbols::SymbolMap;
use sqlx::{PgPool, Postgres, Row, Transaction};

pub async fn start_transaction(pool: &PgPool) -> Result<Transaction<'_, Postgres>> {
    pool.begin()
        .await
        .map_err(|_| Error::CustomError("Failed to connect to database.".into()))
}

pub async fn query_symbols_map(
    pool: &PgPool,
    tickers: &Vec<String>,
    dataset: Dataset,
) -> Result<SymbolMap> {
    let query = format!(
        r#"
        SELECT instrument_id, ticker 
        FROM {}
        WHERE ticker = ANY($1)
        "#,
        dataset.as_str()
    );

    let rows = sqlx::query(&query)
        .bind(tickers)
        .fetch_all(pool) // Use fetch_optional instead of fetch_one
        .await?;

    let mut map = SymbolMap::new();

    for row in rows {
        let id: i32 = row.try_get("instrument_id")?;
        let ticker: String = row.try_get("ticker")?;
        map.add_instrument(&ticker, id as u32);
    }

    Ok(map)
}

#[cfg(test)]
mod test {
    use std::str::FromStr;

    use super::*;
    use anyhow::Result;
    use dotenv;
    use mbinary::symbols::Instrument;
    use mbinary::symbols::SymbolMap;
    use mbinary::vendors::Vendors;
    use mbinary::vendors::{DatabentoData, VendorData};
    use serial_test::serial;
    use sqlx::postgres::PgPoolOptions;
    use sqlx::postgres::{PgConnectOptions, PgPool};
    use sqlx::ConnectOptions;
    use std::time::Duration;

    // -- Helper functions
    pub async fn init_db() -> Result<PgPool> {
        let database_url = std::env::var("HISTORICAL_DATABASE_URL")?;

        // URL connection string
        let mut opts: PgConnectOptions = database_url.parse()?;
        opts = opts.log_slow_statements(log::LevelFilter::Debug, Duration::from_secs(1));

        let db_pool = PgPoolOptions::new()
            .max_connections(100)
            .connect_with(opts)
            .await?;
        Ok(db_pool)
    }
    async fn create_instrument(instrument: Instrument) -> Result<i32> {
        let database_url = std::env::var("HISTORICAL_DATABASE_URL")?;
        let pool = PgPoolOptions::new()
            .max_connections(10)
            .connect(&database_url)
            .await?;

        let mut tx = pool.begin().await.expect("Error settign up database.");

        // Insert dataset into the instrument table and fetch the ID
        let instrument_id: i32 = sqlx::query_scalar(
            r#"
            INSERT INTO instrument (dataset)
            VALUES ($1)
            RETURNING id
            "#,
        )
        .bind(instrument.dataset.clone() as i16)
        .fetch_one(&mut *tx) // Borrow tx mutably
        .await?;

        let query = format!(
            r#"
            INSERT INTO {} (instrument_id, ticker, name, vendor, vendor_data, last_available, first_available, active)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            RETURNING id
            "#,
            instrument.dataset.as_str()
        );

        let _ = sqlx::query(&query)
            .bind(instrument_id)
            .bind(instrument.ticker)
            .bind(instrument.name)
            .bind(instrument.vendor.as_str())
            .bind(instrument.vendor_data as i64)
            .bind(instrument.last_available as i64)
            .bind(instrument.first_available as i64)
            .bind(instrument.active)
            .execute(&mut *tx) // Borrow tx mutably
            .await?;

        let _ = tx.commit().await;

        Ok(instrument_id)
    }

    async fn delete_instrument(id: i32) -> Result<()> {
        let database_url = std::env::var("HISTORICAL_DATABASE_URL")?;
        let pool = PgPoolOptions::new()
            .max_connections(10)
            .connect(&database_url)
            .await?;

        let mut tx = pool.begin().await.expect("Error settign up database.");

        let _ = sqlx::query(
            r#"
            DELETE FROM instrument WHERE id = $1
            "#,
        )
        .bind(id)
        .execute(&mut *tx)
        .await?;
        let _ = tx.commit().await;

        Ok(())
    }

    #[sqlx::test]
    #[serial]
    // #[ignore]
    async fn test_query_symbol_map() -> anyhow::Result<()> {
        dotenv::dotenv().ok();
        let pool = init_db().await.unwrap();
        let mut ids = Vec::new();
        let tickers = vec![
            "ZC.n.0".to_string(),
            "GF.n.0".to_string(),
            "LE.n.0".to_string(),
            "ZS.n.0".to_string(),
            "ZL.n.0".to_string(),
            "ZM.n.0".to_string(),
            "HE.n.0".to_string(),
            "CL.n.0".to_string(),
            "CU.n.0".to_string(),
        ];

        let schema = dbn::Schema::from_str("mbp-1")?;
        let dbn_dataset = dbn::Dataset::from_str("XNAS.ITCH")?;
        let stype = dbn::SType::from_str("raw_symbol")?;
        let vendor_data = VendorData::Databento(DatabentoData {
            schema,
            dataset: dbn_dataset,
            stype,
        });
        let dataset = Dataset::Equities;
        let name = "Apple Inc.";

        for ticker in &tickers {
            let instrument = Instrument::new(
                None,
                ticker,
                name,
                dataset.clone(),
                Vendors::Databento,
                vendor_data.encode(),
                1704672000000000000,
                1704672000000000000,
                0,
                false,
                true,
            );
            let id = create_instrument(instrument).await?;
            ids.push(id);
        }

        // Test
        let map: SymbolMap = query_symbols_map(&pool, &tickers, dataset).await?;

        // Validate
        assert_eq!(9, map.map.len());

        // Cleanup
        for id in ids {
            delete_instrument(id).await.expect("Error on delete");
        }

        Ok(())
    }

    #[sqlx::test]
    #[serial]
    // #[ignore]
    async fn test_get_symbol_map_partial() -> anyhow::Result<()> {
        dotenv::dotenv().ok();
        let pool = init_db().await.unwrap();
        let mut ids = Vec::new();
        let tickers = vec!["AAPL".to_string(), "AAPL1".to_string(), "AAPL2".to_string()];

        // Create instrument
        let schema = dbn::Schema::from_str("mbp-1")?;
        let dbn_dataset = dbn::Dataset::from_str("XNAS.ITCH")?;
        let stype = dbn::SType::from_str("raw_symbol")?;
        let vendor_data = VendorData::Databento(DatabentoData {
            schema,
            dataset: dbn_dataset,
            stype,
        });
        let dataset = Dataset::Equities;
        let ticker = "AAPL";
        let name = "Apple Inc.";
        let instrument = Instrument::new(
            None,
            ticker,
            name,
            dataset.clone(),
            Vendors::Databento,
            vendor_data.encode(),
            1704672000000000000,
            1704672000000000000,
            0,
            false,
            true,
        );

        let id = create_instrument(instrument)
            .await
            .expect("Error creating instrument.");
        ids.push(id);

        // Create instrument
        let dataset = Dataset::Equities;
        let ticker = "AAPL1";
        let name = "Apple Inc.";
        let instrument = Instrument::new(
            None,
            ticker,
            name,
            dataset.clone(),
            Vendors::Databento,
            vendor_data.encode(),
            1704672000000000000,
            1704672000000000000,
            0,
            false,
            true,
        );

        let id = create_instrument(instrument)
            .await
            .expect("Error creating instrument.");
        ids.push(id);

        // Test
        let map = query_symbols_map(&pool, &tickers, dataset).await?;

        // Validate
        assert_eq!(2, map.map.len());

        // Cleanup
        for id in ids {
            delete_instrument(id).await.expect("Error on delete");
        }

        Ok(())
    }

    #[sqlx::test]
    #[serial]
    // #[ignore]
    async fn test_query_symbol_map_none() -> anyhow::Result<()> {
        dotenv::dotenv().ok();
        let pool = init_db().await.unwrap();
        let tickers = vec!["AAPL".to_string(), "AAPL1".to_string(), "AAPL2".to_string()];

        // Test
        let map = query_symbols_map(&pool, &tickers, Dataset::Futures).await?;

        // Validate
        assert_eq!(0, map.map.len());

        Ok(())
    }
}
