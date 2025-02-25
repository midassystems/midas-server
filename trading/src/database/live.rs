use crate::Result;
use async_trait::async_trait;
use mbinary::live::{AccountSummary, LiveData};
use mbinary::backtest::{Parameters,   Signals, Trades};
use crate::database::backtest::{ParametersQueries, SignalQueries, SignalInstructionsQueries, TradesQueries};
use sqlx::{PgPool, Postgres, Row, Transaction};
use tracing::info;
use std::ops::DerefMut;

#[async_trait]
pub trait LiveDataQueries {
    async fn insert_query(&self, tx: &mut Transaction<'_, Postgres>) -> Result<i32>;
    async fn delete_query(tx: &mut Transaction<'_, Postgres>, live_id: i32) -> Result<()>;
    // async fn retrieve_query(pool: &PgPool, id: i32) -> Result<String>;
    async fn retrieve_list_query(pool: &PgPool) -> Result<Vec<(i32, String)>>;
}

#[async_trait]
impl LiveDataQueries for LiveData {
    async fn insert_query(&self, tx: &mut Transaction<'_, Postgres>) -> Result<i32> {
        // info!("Inserting live data for live name: {}", &self.live_name);

        let live_result = sqlx::query(
            r#"
            INSERT INTO Live (created_at)
            VALUES (NOW())
            RETURNING id
            "#,
        )
        .fetch_one(tx.deref_mut())
        .await?;

        // Extract the id directly from the row
        let id: i32 = live_result.try_get("id")?;
        info!("Successfully inserted live with id {}", id);

        Ok(id)
    }

    async fn delete_query(tx: &mut Transaction<'_, Postgres>, live_id: i32) -> Result<()> {
        info!("Deleting live with id {}", live_id);

        sqlx::query(
            r#"
            DELETE FROM Live WHERE id = $1
            "#,
        )
        .bind(live_id)
        .execute(tx.deref_mut())
        .await?;

        info!("Successfully deleted live with id {}", live_id);

        Ok(())
    }

    async fn retrieve_list_query(pool: &PgPool) -> Result<Vec<(i32, String)>> {
        info!("Retrieving list of all lives");

        let rows: Vec<(i32, String)> = sqlx::query_as(
            r#"
            SELECT id, created_at::TEXT
            FROM Live
            "#,
        )
        .fetch_all(pool)
        .await?;

        info!("Successfully retrieved list of {} lives", rows.len());
        Ok(rows)
    }
}

#[async_trait]
pub trait AccountSummaryQueries {
    async fn insert_query(&self, tx: &mut Transaction<'_, Postgres>, live_id: i32) -> Result<()>;
    async fn retrieve_query(pool: &PgPool, live_id: i32) -> Result<Self>
    where
        Self: Sized;
}

#[async_trait]
impl AccountSummaryQueries for AccountSummary {
    async fn insert_query(&self, tx: &mut Transaction<'_, Postgres>, live_id: i32) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO lv_AccountSummary (
            live_id, 
            currency,
            start_timestamp,
            start_buying_power,
            start_excess_liquidity,
            start_full_available_funds,
            start_full_init_margin_req,
            start_full_maint_margin_req,
            start_futures_pnl,
            start_net_liquidation,
            start_total_cash_balance,
            start_unrealized_pnl,
            end_timestamp,
            end_buying_power,
            end_excess_liquidity,
            end_full_available_funds,
            end_full_init_margin_req,
            end_full_maint_margin_req,
            end_futures_pnl,
            end_net_liquidation,
            end_total_cash_balance, 
            end_unrealized_pnl)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22)
            "#,
        )
        .bind(&live_id)
        .bind(&self.currency)
        .bind(&self.start_timestamp)
        .bind(&self.start_buying_power)
        .bind(&self.start_excess_liquidity)
        .bind(&self.start_full_available_funds)
        .bind(&self.start_full_init_margin_req)
        .bind(&self.start_full_maint_margin_req)
        .bind(&self.start_futures_pnl)
        .bind(&self.start_net_liquidation)
        .bind(&self.start_total_cash_balance)
        .bind(&self.start_unrealized_pnl)
        .bind(&self.end_timestamp)
        .bind(&self.end_buying_power)
        .bind(&self.end_excess_liquidity)
        .bind(&self.end_full_available_funds)
        .bind(&self.end_full_init_margin_req)
        .bind(&self.end_full_maint_margin_req)
        .bind(&self.end_futures_pnl)
        .bind(&self.end_net_liquidation)
        .bind(&self.end_total_cash_balance) 
        .bind(&self.end_unrealized_pnl)
        .execute(tx.deref_mut())
        .await?;
        Ok(())
    }

    async fn retrieve_query(pool: &PgPool, live_id: i32) -> Result<Self> {
        let result : AccountSummary =  sqlx::query_as(
            r#"
            SELECT  live_id, currency, start_timestamp, start_buying_power, start_excess_liquidity, start_full_available_funds, start_full_init_margin_req, start_full_maint_margin_req, start_futures_pnl, start_net_liquidation, start_total_cash_balance, start_unrealized_pnl, end_timestamp, end_buying_power, end_excess_liquidity, end_full_available_funds, end_full_init_margin_req, end_full_maint_margin_req, end_futures_pnl, end_net_liquidation, end_total_cash_balance, end_unrealized_pnl
            FROM lv_AccountSummary
            WHERE live_id = $1
            "#
        )
        .bind(live_id)
        .fetch_one(pool)
        .await?;

        Ok(result)
    }
}

pub async fn create_live_related(
    tx: &mut Transaction<'_, Postgres>,
    live_data: &LiveData,
) -> Result<i32> {
    info!(
        "Creating live related data."
    );

    // Insert into Backtest table
    let live_id = live_data.insert_query(tx).await?;

    // Insert Parameters
    live_data.parameters.insert_query(tx, live_id, false).await?;
    info!("Successfully inserted parameters for live id {}", live_id);

    // Account Summary
    live_data.account.insert_query(tx, live_id).await?;
    info!("Successfully inserted account for live id {}", live_id);

    // Insert Trade
    for trade in &live_data.trades {
        trade.insert_query(tx, live_id, false).await?;
    }
    info!("Successfully inserted trades for live id {}", live_id);

    // Insert Signal
    for signal in &live_data.signals {
        let signal_id = signal.insert_query(tx, live_id, false).await.unwrap();
        for instruction in &signal.trade_instructions {
            instruction
                .insert_query(tx, live_id, signal_id, false)
                .await
                .unwrap();
        }
    }
    info!("Successfully inserted signals for live id {}", live_id);

    Ok(live_id)
}

pub async fn retrieve_live_related(pool: &PgPool, live_id: i32) -> Result<LiveData> {
    info!("Retrieving full live data for id {}", live_id);

    // Retrieve components
    // let live_name = LiveData::retrieve_query(pool, live_id).await?;
    let parameters = Parameters::retrieve_query(pool, live_id, false).await?;
    let account = AccountSummary::retrieve_query(pool, live_id).await?;
    let trades = Trades::retrieve_query(pool, live_id, false).await?;
    let signals = Signals::retrieve_query(pool, live_id, false).await?;
    info!(
        "Successfully retrieved all related data for live id {}",
        live_id
    );

    Ok(LiveData {
        live_id: Some(live_id as u16),
        parameters,
        trades,
        signals,
        account,
    })
}



#[cfg(test)]
mod test {
    use super::*;
    use crate::database::init::init_db;
    use serial_test::serial;
    use std::fs;


     #[sqlx::test]
    #[serial]
    async fn test_create_live_related() {
        dotenv::dotenv().ok();
        let pool = init_db().await.unwrap();
        let mut transaction = pool
            .begin()
            .await
            .expect("Error setting up test transaction.");

        // Pull test data
        let mock_data =
            fs::read_to_string("tests/data/test_data.live.json").expect("Unable to read file");
        let data: LiveData =
            serde_json::from_str(&mock_data).expect("JSON was not well-formatted");

        // Test
        let id = create_live_related(&mut transaction, &data)
            .await
            .expect("Error on creating backtest and related.");

        // Validate
        assert!(id > 0);
    }

    #[sqlx::test]
    #[serial]
    async fn test_retrieve_live_related() {
        dotenv::dotenv().ok();
        let pool = init_db().await.unwrap();
        let mut transaction = pool
            .begin()
            .await
            .expect("Error setting up test transaction.");

        // Pull test data
        let mock_data =
            fs::read_to_string("tests/data/test_data.live.json").expect("Unable to read file");
        let backtest_data: LiveData =
            serde_json::from_str(&mock_data).expect("JSON was not well-formatted");

        // Create backtest
        let backtest_id = create_live_related(&mut transaction, &backtest_data)
            .await
            .expect("Error on creating backtest and related.");

        let _ = transaction.commit().await;

        // Test
        let result: LiveData = retrieve_live_related(&pool, backtest_id)
            .await
            .expect("Error while retrieving backtest.");

        // Validate
        assert!(result.live_id.unwrap() > 0); //, result.backtest_name);

        // Cleanup
        let mut transaction = pool
            .begin()
            .await
            .expect("Error setting up test transaction.");
        LiveData::delete_query(&mut transaction, backtest_id)
            .await
            .expect("Error on delete.");
        let _ = transaction.commit().await;
    }
}
