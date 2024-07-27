use crate::mbn::symbols::{Instrument, SymbolMap};
use crate::Result;
use async_trait::async_trait;
use sqlx::{PgPool, Postgres, Row, Transaction};

#[async_trait]
pub trait InstrumentsQueries: Sized {
    async fn insert_instrument(&self, tx: &mut Transaction<'_, Postgres>) -> Result<i32>;
    async fn get_instrument_id(pool: &PgPool, ticker: &str) -> Result<i32>;
    async fn delete_instrument(tx: &mut Transaction<'_, Postgres>, id: i32) -> Result<()>;
    async fn list_instruments(pool: &PgPool) -> Result<Vec<Instrument>>;
    async fn update_instrument(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        instrument_id: i32,
    ) -> Result<()>;
}

#[async_trait]
impl InstrumentsQueries for Instrument {
    async fn insert_instrument(&self, tx: &mut Transaction<'_, Postgres>) -> Result<i32> {
        let result = sqlx::query(
            r#"
            INSERT INTO instrument (ticker, name)
            VALUES ($1, $2)
            RETURNING id
            "#,
        )
        .bind(&self.ticker)
        .bind(&self.name)
        .fetch_one(tx)
        .await?;

        let id: i32 = result.try_get("id")?;

        Ok(id)
    }

    async fn get_instrument_id(pool: &PgPool, ticker: &str) -> Result<i32> {
        let result = sqlx::query(
            r#"
            SELECT id, ticker, name FROM instrument
            WHERE ticker = $1
            "#,
        )
        .bind(&ticker)
        .fetch_one(pool)
        .await?;

        let id: i32 = result.try_get("id")?;

        Ok(id)
    }

    async fn list_instruments(pool: &PgPool) -> Result<Vec<Instrument>> {
        let result: Vec<Instrument> = sqlx::query_as(
            r#"
            SELECT * FROM instrument;
            "#,
        )
        .fetch_all(pool)
        .await?;

        Ok(result)
    }

    async fn delete_instrument(tx: &mut Transaction<'_, Postgres>, id: i32) -> Result<()> {
        let _ = sqlx::query(
            r#"
            DELETE FROM instrument WHERE id = $1
            "#,
        )
        .bind(&id)
        .execute(tx)
        .await?;

        Ok(())
    }

    async fn update_instrument(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        instrument_id: i32,
    ) -> Result<()> {
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

        Ok(())
    }
}

pub async fn get_symbols_map(pool: &PgPool, symbols: &Vec<&str>) -> Result<SymbolMap> {
    let mut map = SymbolMap::new();

    for ticker in symbols {
        let id: i32 = Instrument::get_instrument_id(pool, ticker).await?;
        map.add_instrument(ticker, id as u32);
    }

    Ok(map)
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::database::init::init_quest_db;
    use dotenv;
    use serial_test::serial;

    #[sqlx::test]
    #[serial]
    async fn test_insert_instrument() {
        dotenv::dotenv().ok();
        let pool = init_quest_db().await.expect("Error on creating pool");
        let mut transaction = pool.begin().await.expect("Error settign up database.");

        let ticker = "AAPL";
        let name = "Apple Inc.";
        let instrument = Instrument::new(ticker, name);

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
        let pool = init_quest_db().await.unwrap();
        let mut transaction = pool.begin().await.expect("Error settign up database.");

        let ticker = "AAPL";
        let name = "Apple Inc.";
        let instrument = Instrument::new(ticker, name);
        let id = instrument
            .insert_instrument(&mut transaction)
            .await
            .expect("Error inserting symbol.");
        let _ = transaction.commit().await;

        // Test
        let ret_id: i32 = Instrument::get_instrument_id(&pool, ticker)
            .await
            .expect("Error getting symbols map.");

        // Validate
        // assert_eq!(result.map[&id], ticker);
        assert_eq!(id, ret_id);

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
        let pool = init_quest_db().await.unwrap();
        let mut transaction = pool.begin().await.expect("Error settign up database.");

        let ticker = "AAPL";
        let name = "Apple Inc.";
        let instrument = Instrument::new(ticker, name);
        let id = instrument
            .insert_instrument(&mut transaction)
            .await
            .expect("Error inserting symbol.");
        let _ = transaction.commit().await;

        // Test
        let new_ticker = "TSLA";
        let new_name = "Tesla Inc.";
        let new_instrument = Instrument::new(new_ticker, new_name);
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
        let pool = init_quest_db().await.unwrap();
        let mut transaction = pool.begin().await.expect("Error settign up database.");

        // Create Instruments
        let mut ids: Vec<i32> = vec![];

        let ticker = "AAPL";
        let name = "Apple Inc.";
        let instrument = Instrument::new(ticker, name);
        let id = instrument
            .insert_instrument(&mut transaction)
            .await
            .expect("Error inserting symbol.");
        ids.push(id);

        let ticker = "TSLA";
        let name = "Tesle Inc.";
        let instrument = Instrument::new(ticker, name);
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
        assert_eq!(vec.len(), 2);

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
        let pool = init_quest_db().await.unwrap();
        let mut transaction = pool.begin().await.expect("Error settign up database.");

        // Create Instruments
        let mut ids: Vec<i32> = vec![];

        let ticker = "AAPL";
        let name = "Apple Inc.";
        let instrument = Instrument::new(ticker, name);
        let id = instrument
            .insert_instrument(&mut transaction)
            .await
            .expect("Error inserting symbol.");
        ids.push(id);

        let ticker2 = "TSLA";
        let name = "Tesle Inc.";
        let instrument = Instrument::new(ticker2, name);
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
