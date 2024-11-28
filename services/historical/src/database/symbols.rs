use crate::Result;
use async_trait::async_trait;
use mbn::symbols::Instrument;
use sqlx::{PgPool, Postgres, Row, Transaction};
use tracing::info;
struct InstrumentWrapper(Instrument);

impl sqlx::FromRow<'_, sqlx::postgres::PgRow> for InstrumentWrapper {
    fn from_row(row: &sqlx::postgres::PgRow) -> std::result::Result<Self, sqlx::Error> {
        Ok(InstrumentWrapper(Instrument {
            instrument_id: row.try_get::<Option<i32>, _>("id")?.map(|id| id as u32),
            ticker: row.try_get::<String, _>("ticker")?,
            name: row.try_get::<String, _>("name")?,
            vendor: row.try_get::<String, _>("vendor")?,
            stype: row.try_get::<Option<String>, _>("stype")?,
            dataset: row.try_get::<Option<String>, _>("dataset")?,
            last_available: row.try_get::<i64, _>("last_available")?.try_into().unwrap(),
            first_available: row
                .try_get::<i64, _>("first_available")?
                .try_into()
                .unwrap(),
            active: row.try_get::<bool, _>("active")?,
        }))
    }
}

impl From<InstrumentWrapper> for Instrument {
    fn from(wrapper: InstrumentWrapper) -> Self {
        wrapper.0
    }
}

#[async_trait]
pub trait InstrumentsQueries: Sized {
    async fn insert_instrument(&self, tx: &mut Transaction<'_, Postgres>) -> Result<i32>;
    async fn get_instrument_id(pool: &PgPool, ticker: &str) -> Result<Option<i32>>;
    async fn delete_instrument(tx: &mut Transaction<'_, Postgres>, id: i32) -> Result<()>;
    async fn list_instruments(pool: &PgPool) -> Result<Vec<Instrument>>;
    async fn vendor_list_instruments(pool: &PgPool, vendor: &str) -> Result<Vec<Instrument>>;

    async fn update_instrument(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        instrument_id: i32,
    ) -> Result<()>;
}

#[async_trait]
impl InstrumentsQueries for Instrument {
    async fn insert_instrument(&self, tx: &mut Transaction<'_, Postgres>) -> Result<i32> {
        info!("Inserting new instrument {:?}", self);
        let result = sqlx::query(
            r#"
            INSERT INTO instrument (ticker, name, vendor, stype, dataset, last_available, first_available, active)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            RETURNING id
            "#,
        )
        .bind(&self.ticker)
        .bind(&self.name)
        .bind(&self.vendor)
        .bind(&self.stype)
        .bind(&self.dataset)
        .bind(self.last_available as i64)
        .bind(self.first_available as i64)
        .bind(&self.active)
        .fetch_one(tx)
        .await?;

        let id: i32 = result.try_get("id")?;

        info!("Successfully inserted instrument with id {}", id);
        Ok(id)
    }

    async fn get_instrument_id(pool: &PgPool, ticker: &str) -> Result<Option<i32>> {
        info!("Fetching instrument id for ticker: {}", ticker);
        let result = sqlx::query(
            r#"
            SELECT id FROM instrument
            WHERE ticker = $1
            "#,
        )
        .bind(ticker)
        .fetch_optional(pool) // Use fetch_optional instead of fetch_one
        .await?;

        // If result is Some, extract the id, otherwise return None
        let id = result.map(|row| row.try_get("id")).transpose()?;

        match id {
            Some(id) => info!("Found instrument id: {}", id),
            None => info!("No instrument found for ticker: {}", ticker),
        }

        Ok(id)
    }

    async fn list_instruments(pool: &PgPool) -> Result<Vec<Instrument>> {
        info!("Fetching list of all instruments");
        let rows: Vec<InstrumentWrapper> = sqlx::query_as(
            r#"
            SELECT * FROM instrument
            "#,
        )
        .fetch_all(pool)
        .await?;

        let instruments: Vec<Instrument> = rows.into_iter().map(Instrument::from).collect();
        info!("Successfully fetched {} instruments", instruments.len());

        Ok(instruments)
    }
    async fn vendor_list_instruments(pool: &PgPool, vendor: &str) -> Result<Vec<Instrument>> {
        info!("Fetching list of {} instruments", vendor);
        let rows: Vec<InstrumentWrapper> = sqlx::query_as(
            r#"
            SELECT * FROM instrument WHERE vendor = $1
            "#,
        )
        .bind(vendor)
        .fetch_all(pool)
        .await?;

        let instruments: Vec<Instrument> = rows.into_iter().map(Instrument::from).collect();
        info!("Successfully fetched {} instruments", vendor);

        Ok(instruments)
    }

    async fn delete_instrument(tx: &mut Transaction<'_, Postgres>, id: i32) -> Result<()> {
        info!("Deleting instrument with id {}", id);
        let _ = sqlx::query(
            r#"
            DELETE FROM instrument WHERE id = $1
            "#,
        )
        .bind(&id)
        .execute(tx)
        .await?;

        info!("Successfully deleted instrument with id {}", id);

        Ok(())
    }

    async fn update_instrument(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        instrument_id: i32,
    ) -> Result<()> {
        info!(
            "Updating instrument with id {} to new values: ticker = {}, name = {}",
            instrument_id, self.ticker, self.name
        );
        let _ = sqlx::query(
            r#"
            UPDATE instrument
            SET ticker = $1, name =$2
            WHERE id = $3
            "#,
        )
        .bind(&self.ticker)
        .bind(&self.name)
        .bind(instrument_id)
        .execute(tx)
        .await?;

        info!("Successfully updated instrument with id {}", instrument_id);

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::database::init::init_db;
    use dotenv;
    use mbn::symbols::SymbolMap;
    use mbn::symbols::Vendors;
    use serial_test::serial;

    pub async fn get_symbols_map(pool: &PgPool, symbols: &Vec<&str>) -> Result<SymbolMap> {
        let mut map = SymbolMap::new();

        for ticker in symbols {
            let id: i32 = Instrument::get_instrument_id(pool, ticker).await?.unwrap();
            map.add_instrument(ticker, id as u32);
        }

        Ok(map)
    }

    #[sqlx::test]
    #[serial]
    async fn test_insert_instrument() {
        dotenv::dotenv().ok();
        let pool = init_db().await.expect("Error on creating pool");
        let mut transaction = pool.begin().await.expect("Error settign up database.");

        let ticker = "AAPL";
        let name = "Apple Inc.";
        let instrument = Instrument::new(
            None,
            ticker,
            name,
            Vendors::Databento,
            Some("continuous".to_string()),
            Some("GLBX.MDP3".to_string()),
            1704672000000000000,
            1704672000000000000,
            true,
        );

        // Test
        let result = instrument
            .insert_instrument(&mut transaction)
            .await
            .expect("Error inserting symbol.");

        // Validate
        assert!(result > 0);
    }

    #[sqlx::test]
    #[serial]
    async fn test_get_instrument_id() {
        dotenv::dotenv().ok();
        let pool = init_db().await.unwrap();
        let mut transaction = pool.begin().await.expect("Error settign up database.");

        let ticker = "AAPL";
        let name = "Apple Inc.";
        let instrument = Instrument::new(
            None,
            ticker,
            name,
            Vendors::Databento,
            Some("continuous".to_string()),
            Some("GLBX.MDP3".to_string()),
            1704672000000000000,
            1704672000000000000,
            true,
        );
        let id = instrument
            .insert_instrument(&mut transaction)
            .await
            .expect("Error inserting symbol.");
        let _ = transaction.commit().await;

        // Test
        let ret_id: Option<i32> = Instrument::get_instrument_id(&pool, ticker)
            .await
            .expect("Error getting symbols map.");

        // Validate
        assert_eq!(id, ret_id.unwrap());

        // Cleanup
        let mut transaction = pool
            .begin()
            .await
            .expect("Error setting up test transaction.");
        Instrument::delete_instrument(&mut transaction, id)
            .await
            .expect("Error on delete.");
        let _ = transaction.commit().await;
    }

    #[sqlx::test]
    #[serial]
    async fn update_instrument() {
        dotenv::dotenv().ok();
        let pool = init_db().await.unwrap();
        let mut transaction = pool.begin().await.expect("Error settign up database.");

        let ticker = "AAPL";
        let name = "Apple Inc.";
        let instrument = Instrument::new(
            None,
            ticker,
            name,
            Vendors::Databento,
            Some("continuous".to_string()),
            Some("GLBX.MDP3".to_string()),
            1704672000000000000,
            1704672000000000000,
            true,
        );
        let id = instrument
            .insert_instrument(&mut transaction)
            .await
            .expect("Error inserting symbol.");
        let _ = transaction.commit().await;

        // Test
        let new_ticker = "TSLA";
        let new_name = "Tesla Inc.";
        let new_instrument = Instrument::new(
            None,
            new_ticker,
            new_name,
            Vendors::Databento,
            Some("continuous".to_string()),
            Some("GLBX.MDP3".to_string()),
            1704672000000000001,
            1704672000000000000,
            true,
        );
        let mut transaction = pool.begin().await.expect("Error settign up database.");
        let result = new_instrument
            .update_instrument(&mut transaction, id)
            .await
            .expect("Error updating instrument.");
        let _ = transaction.commit().await;

        // Validate
        assert_eq!(result, ());

        // Cleanup
        let mut transaction = pool
            .begin()
            .await
            .expect("Error setting up test transaction.");
        Instrument::delete_instrument(&mut transaction, id)
            .await
            .expect("Error on delete.");
        let _ = transaction.commit().await;
    }

    #[sqlx::test]
    #[serial]
    async fn test_list_instruments() {
        dotenv::dotenv().ok();
        let pool = init_db().await.unwrap();
        let mut transaction = pool.begin().await.expect("Error settign up database.");

        // Create Instruments
        let mut ids: Vec<i32> = vec![];

        let ticker = "AAPL";
        let name = "Apple Inc.";
        let instrument = Instrument::new(
            None,
            ticker,
            name,
            Vendors::Databento,
            Some("continuous".to_string()),
            Some("GLBX.MDP3".to_string()),
            1704672000000000000,
            1704672000000000000,
            true,
        );
        let id = instrument
            .insert_instrument(&mut transaction)
            .await
            .expect("Error inserting symbol.");
        ids.push(id);

        let ticker = "TSLA";
        let name = "Tesle Inc.";
        let instrument = Instrument::new(
            None,
            ticker,
            name,
            Vendors::Databento,
            Some("continuous".to_string()),
            Some("GLBX.MDP3".to_string()),
            1704672000000000000,
            1704672000000000000,
            true,
        );
        let id2 = instrument
            .insert_instrument(&mut transaction)
            .await
            .expect("Error inserting symbol.");
        ids.push(id2);

        let _ = transaction.commit().await;

        // Test
        let vec = Instrument::list_instruments(&pool)
            .await
            .expect("Error getting list of instruments.");

        // Validate
        assert!(vec.len() >= 2);

        // Clean up
        let mut transaction = pool
            .begin()
            .await
            .expect("Error setting up test transaction.");
        for id in ids {
            Instrument::delete_instrument(&mut transaction, id)
                .await
                .expect("Error on delete.");
        }

        let _ = transaction.commit().await;
    }

    #[sqlx::test]
    #[serial]
    async fn test_vendor_list_instruments() {
        dotenv::dotenv().ok();
        let pool = init_db().await.unwrap();
        let mut transaction = pool.begin().await.expect("Error settign up database.");

        // Create Instruments
        let mut ids: Vec<i32> = vec![];

        let ticker = "AAPL";
        let name = "Apple Inc.";
        let instrument = Instrument::new(
            None,
            ticker,
            name,
            Vendors::Databento,
            Some("continuous".to_string()),
            Some("GLBX.MDP3".to_string()),
            1704672000000000000,
            1704672000000000000,
            true,
        );
        let id = instrument
            .insert_instrument(&mut transaction)
            .await
            .expect("Error inserting symbol.");
        ids.push(id);

        let ticker = "TSLA";
        let name = "Tesle Inc.";
        let instrument = Instrument::new(
            None,
            ticker,
            name,
            Vendors::Yfinance,
            Some("continuous".to_string()),
            Some("GLBX.MDP3".to_string()),
            1704672000000000000,
            1704672000000000000,
            true,
        );
        let id2 = instrument
            .insert_instrument(&mut transaction)
            .await
            .expect("Error inserting symbol.");
        ids.push(id2);

        let _ = transaction.commit().await;

        // Test
        let vec = Instrument::vendor_list_instruments(&pool, "databento")
            .await
            .expect("Error getting list of instruments.");

        // Validate
        assert!(vec.len() == 1);

        // Clean up
        let mut transaction = pool
            .begin()
            .await
            .expect("Error setting up test transaction.");
        for id in ids {
            Instrument::delete_instrument(&mut transaction, id)
                .await
                .expect("Error on delete.");
        }

        let _ = transaction.commit().await;
    }

    #[sqlx::test]
    #[serial]
    async fn test_get_symbols_map() {
        dotenv::dotenv().ok();
        let pool = init_db().await.unwrap();
        let mut transaction = pool.begin().await.expect("Error settign up database.");

        // Create Instruments
        let mut ids: Vec<i32> = vec![];

        let ticker = "AAPL";
        let name = "Apple Inc.";
        let instrument = Instrument::new(
            None,
            ticker,
            name,
            Vendors::Databento,
            Some("continuous".to_string()),
            Some("GLBX.MDP3".to_string()),
            1704672000000000000,
            1704672000000000000,
            true,
        );
        let id = instrument
            .insert_instrument(&mut transaction)
            .await
            .expect("Error inserting symbol.");
        ids.push(id);

        let ticker2 = "TSLA";
        let name = "Tesle Inc.";
        let instrument = Instrument::new(
            None,
            ticker2,
            name,
            Vendors::Databento,
            Some("continuous".to_string()),
            Some("GLBX.MDP3".to_string()),
            1704672000000000000,
            1704672000000000000,
            true,
        );
        let id2 = instrument
            .insert_instrument(&mut transaction)
            .await
            .expect("Error inserting symbol.");
        ids.push(id2);

        let _ = transaction.commit().await;

        // Test
        let symbols = vec![ticker, ticker2];

        let result = get_symbols_map(&pool, &symbols)
            .await
            .expect("Error gettign symbols map.");

        // Validate
        assert_eq!(result.map[&(id as u32)], ticker);

        // Clean up
        let mut transaction = pool
            .begin()
            .await
            .expect("Error setting up test transaction.");
        for id in ids {
            Instrument::delete_instrument(&mut transaction, id)
                .await
                .expect("Error on delete.");
        }

        let _ = transaction.commit().await;
    }
}
