use crate::Result;
use async_trait::async_trait;
use mbn::backtest::{
    BacktestData, Parameters, SignalInstructions, Signals, StaticStats, TimeseriesStats, Trades,
};
use sqlx::{PgPool, Postgres, Row, Transaction};
use tracing::info;

#[async_trait]
pub trait BacktestDataQueries {
    async fn insert_query(&self, tx: &mut Transaction<'_, Postgres>) -> Result<i32>;
    async fn delete_query(tx: &mut Transaction<'_, Postgres>, backtest_id: i32) -> Result<()>;
    async fn retrieve_query(pool: &PgPool, id: i32) -> Result<String>;
    async fn retrieve_list_query(pool: &PgPool) -> Result<Vec<(i32, String)>>;
}

#[async_trait]
impl BacktestDataQueries for BacktestData {
    async fn insert_query(&self, tx: &mut Transaction<'_, Postgres>) -> Result<i32> {
        info!(
            "Inserting backtest data for backtest name: {}",
            &self.backtest_name
        );

        let backtest_result = sqlx::query(
            r#"
            INSERT INTO Backtest (backtest_name, created_at)
            VALUES ($1, NOW())
            RETURNING id
            "#,
        )
        .bind(&self.backtest_name)
        .fetch_one(tx)
        .await?;

        // Extract the id directly from the row
        let id: i32 = backtest_result.try_get("id")?;
        info!("Successfully inserted backtest with id {}", id);

        Ok(id)
    }

    async fn delete_query(tx: &mut Transaction<'_, Postgres>, backtest_id: i32) -> Result<()> {
        info!("Deleting backtest with id {}", backtest_id);

        sqlx::query(
            r#"
            DELETE FROM Backtest WHERE id = $1
            "#,
        )
        .bind(backtest_id)
        .execute(tx)
        .await?;

        info!("Successfully deleted backtest with id {}", backtest_id);

        Ok(())
    }
    async fn retrieve_query(pool: &PgPool, id: i32) -> Result<String> {
        info!("Retrieving backtest name for id {}", id);

        let result = sqlx::query(
            r#"
            SELECT backtest_name
            FROM Backtest
            WHERE id = $1
            "#,
        )
        .bind(id)
        .fetch_one(pool)
        .await?;

        let backtest_name: String = result.try_get("backtest_name")?;
        info!("Successfully retrieved backtest name: {}", backtest_name);

        Ok(backtest_name)
    }
    async fn retrieve_list_query(pool: &PgPool) -> Result<Vec<(i32, String)>> {
        info!("Retrieving list of all backtests");

        let rows: Vec<(i32, String)> = sqlx::query_as(
            r#"
            SELECT id, backtest_name
            FROM Backtest
            "#,
        )
        .fetch_all(pool)
        .await?;

        info!("Successfully retrieved list of {} backtests", rows.len());
        Ok(rows)
    }
}

#[async_trait]
pub trait ParametersQueries {
    async fn insert_query(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        backtest_id: i32,
    ) -> Result<()>;
    async fn retrieve_query(pool: &PgPool, backtest_id: i32) -> Result<Self>
    where
        Self: Sized;
}

#[async_trait]
impl ParametersQueries for Parameters {
    async fn insert_query(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        backtest_id: i32,
    ) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO bt_Parameters (backtest_id, strategy_name, capital, data_type, schema, train_start, train_end, test_start, test_end, tickers)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            "#
        )
        .bind(&backtest_id)
        .bind(&self.strategy_name)
        .bind(&self.capital)
        .bind(&self.data_type)
        .bind(&self.schema)
        .bind(&self.train_start)
        .bind(&self.train_end)
        .bind(&self.test_start)
        .bind(&self.test_end)
        .bind(&self.tickers)
        .execute(tx)
        .await?;
        Ok(())
    }

    async fn retrieve_query(pool: &PgPool, backtest_id: i32) -> Result<Self> {
        let result : Parameters = sqlx::query_as(
            r#"
            SELECT strategy_name, capital, schema, data_type, train_start, train_end, test_start, test_end, tickers
            FROM bt_Parameters
            WHERE backtest_id = $1
            "#
        )
        .bind(backtest_id)
        .fetch_one(pool)
        .await?;

        Ok(result)
    }
}

#[async_trait]
pub trait StaticStatsQueries {
    async fn insert_query(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        backtest_id: i32,
    ) -> Result<()>;
    async fn retrieve_query(pool: &PgPool, backtest_id: i32) -> Result<Self>
    where
        Self: Sized;
}

#[async_trait]
impl StaticStatsQueries for StaticStats {
    async fn insert_query(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        backtest_id: i32,
    ) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO bt_StaticStats (
            backtest_id,
            total_trades,
            total_winning_trades,
            total_losing_trades,
            avg_profit,
            avg_profit_percent,
            avg_gain,
            avg_gain_percent,
            avg_loss,
            avg_loss_percent,
            profitability_ratio,
            profit_factor,
            profit_and_loss_ratio,
            total_fees,
            net_profit,
            beginning_equity,
            ending_equity,
            total_return,
            daily_standard_deviation_percentage,
            annual_standard_deviation_percentage,
            max_drawdown_percentage_period,
            max_drawdown_percentage_daily,
            sharpe_ratio,
            sortino_ratio)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24)
            "#,
        )
        .bind(&backtest_id)
        .bind(&self.total_trades)
        .bind(&self.total_winning_trades)
        .bind(&self.total_losing_trades)
        .bind(&self.avg_profit)
        .bind(&self.avg_profit_percent)
        .bind(&self.avg_gain)
        .bind(&self.avg_gain_percent)
        .bind(&self.avg_loss)
        .bind(&self.avg_loss_percent)
        .bind(&self.profitability_ratio)
        .bind(&self.profit_factor)
        .bind(&self.profit_and_loss_ratio)
        .bind(&self.total_fees)
        .bind(&self.net_profit)
        .bind(&self.beginning_equity)
        .bind(&self.ending_equity)
        .bind(&self.total_return)
        .bind(&self.daily_standard_deviation_percentage)
        .bind(&self.annual_standard_deviation_percentage)
        .bind(&self.max_drawdown_percentage_period)
        .bind(&self.max_drawdown_percentage_daily)
        .bind(&self.sharpe_ratio)
        .bind(&self.sortino_ratio)
        .execute(tx)
        .await?;
        Ok(())
    }

    async fn retrieve_query(pool: &PgPool, backtest_id: i32) -> Result<Self> {
        let row: StaticStats = sqlx::query_as(
            r#"
            SELECT  total_trades,
                total_winning_trades,
                total_losing_trades,
                avg_profit,
                avg_profit_percent,
                avg_gain,
                avg_gain_percent,
                avg_loss,
                avg_loss_percent,
                profitability_ratio,
                profit_factor,
                profit_and_loss_ratio,
                total_fees,
                net_profit,
                beginning_equity,
                ending_equity,
                total_return,
                daily_standard_deviation_percentage,
                annual_standard_deviation_percentage,
                max_drawdown_percentage_period,
                max_drawdown_percentage_daily,
                sharpe_ratio,
            sortino_ratio            
            FROM bt_StaticStats
            WHERE backtest_id = $1
            "#,
        )
        .bind(backtest_id)
        .fetch_one(pool)
        .await?;

        Ok(row)
    }
}

pub enum TimeseriesTypes {
    DAILY,
    PERIOD,
}

impl TimeseriesTypes {
    fn to_table(&self) -> String {
        match self {
            TimeseriesTypes::DAILY => "bt_PeriodTimeseriesStats".to_string(),
            TimeseriesTypes::PERIOD => "bt_DailyTimeseriesStats".to_string(),
        }
    }
}

#[async_trait]
pub trait TimeseriesQueries {
    async fn insert_query(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        timeseries_type: TimeseriesTypes,
        backtest_id: i32,
    ) -> Result<()>;
    async fn retrieve_query(
        pool: &PgPool,
        timeseries_type: TimeseriesTypes,
        backtest_id: i32,
    ) -> Result<Vec<Self>>
    where
        Self: Sized;
}

#[async_trait]
impl TimeseriesQueries for TimeseriesStats {
    async fn insert_query(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        timeseries_type: TimeseriesTypes,
        backtest_id: i32,
    ) -> Result<()> {
        let table = timeseries_type.to_table();

        sqlx::query(format!(
            r#"
            INSERT INTO {} (backtest_id, timestamp, equity_value, percent_drawdown, cumulative_return, period_return) 
            VALUES ($1, $2, $3, $4, $5, $6)
            "#
        , &table).as_str())
        .bind(&backtest_id)
        .bind(&self.timestamp)
        .bind(&self.equity_value)
        .bind(&self.percent_drawdown)
        .bind(&self.cumulative_return)
        .bind(&self.period_return)
        .execute(tx)
        .await?;
        Ok(())
    }

    async fn retrieve_query(
        pool: &PgPool,
        timeseries_type: TimeseriesTypes,
        backtest_id: i32,
    ) -> Result<Vec<Self>> {
        let table = timeseries_type.to_table();

        let result : Vec<TimeseriesStats> = sqlx::query_as(format!(
            r#"
            SELECT backtest_id, timestamp, equity_value, percent_drawdown, cumulative_return, period_return
            FROM {}
            WHERE backtest_id = $1
            "#
        , &table).as_str())
        .bind(backtest_id)
        .fetch_all(pool)
        .await?;

        Ok(result)
    }
}

#[async_trait]
pub trait TradesQueries {
    async fn insert_query(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        backtest_id: i32,
    ) -> Result<()>;
    async fn retrieve_query(pool: &PgPool, backtest_id: i32) -> Result<Vec<Self>>
    where
        Self: Sized;
}

#[async_trait]
impl TradesQueries for Trades {
    async fn insert_query(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        backtest_id: i32,
    ) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO bt_Trade (backtest_id, trade_id, leg_id, timestamp, ticker, quantity, avg_price, trade_value, action, fees)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            "#
        )
        .bind(&backtest_id)
        .bind(&self.trade_id)
        .bind(&self.leg_id)
        .bind(&self.timestamp)
        .bind(&self.ticker)
        .bind(&self.quantity)
        .bind(&self.avg_price)
        .bind(&self.trade_value)
        .bind(&self.action)
        .bind(&self.fees)
        .execute(tx)
        .await?;
        Ok(())
    }

    async fn retrieve_query(pool: &PgPool, backtest_id: i32) -> Result<Vec<Self>> {
        let result : Vec<Trades> = sqlx::query_as(
            r#"
            SELECT backtest_id, trade_id, leg_id, timestamp, ticker, quantity, avg_price, trade_value, action, fees
            FROM bt_Trade
            WHERE backtest_id = $1
            "#
        )
        .bind(backtest_id)
        .fetch_all(pool)
        .await?;

        Ok(result)
    }
}

#[async_trait]
pub trait SignalInstructionsQueries {
    async fn insert_query(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        backtest_id: i32,
        signal_id: i32,
    ) -> Result<()>;
    async fn retrieve_query(pool: &PgPool, backtest_id: i32, signal_id: i32) -> Result<Vec<Self>>
    where
        Self: Sized;
}

#[async_trait]
impl SignalInstructionsQueries for SignalInstructions {
    async fn insert_query(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        backtest_id: i32,
        signal_id: i32,
    ) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO bt_SignalInstructions (backtest_id, signal_id, ticker, order_type, action, trade_id, leg_id, weight, quantity, limit_price, aux_price)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            "#
        )
        .bind(&backtest_id)
        .bind(&signal_id)
        .bind(&self.ticker)
        .bind(&self.order_type)
        .bind(&self.action)
        .bind(&self.trade_id)
        .bind(&self.leg_id)
        .bind(&self.weight)
        .bind(&self.quantity)
        .bind(&self.limit_price)
        .bind(&self.aux_price)
        .execute(tx)
        .await?;

        Ok(())
    }

    async fn retrieve_query(pool: &PgPool, backtest_id: i32, signal_id: i32) -> Result<Vec<Self>> {
        let result : Vec<SignalInstructions> = sqlx::query_as(
            r#"
            SELECT ticker, order_type, action, trade_id, leg_id, weight, quantity, limit_price, aux_price
            FROM bt_SignalInstructions
            WHERE backtest_id = $1 AND signal_id = $2
            "#
        )
        .bind(backtest_id)
        .bind(signal_id)
        .fetch_all(pool)
        .await?;

        Ok(result)
    }
}

#[async_trait]
pub trait SignalQueries {
    async fn insert_query(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        backtest_id: i32,
    ) -> Result<i32>;
    async fn retrieve_query(pool: &PgPool, backtest_id: i32) -> Result<Vec<Self>>
    where
        Self: Sized;
}

#[async_trait]
impl SignalQueries for Signals {
    async fn insert_query(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        backtest_id: i32,
    ) -> Result<i32> {
        let result = sqlx::query(
            r#"
            INSERT INTO bt_Signal (backtest_id, timestamp)
            VALUES ($1, $2)
            RETURNING id
            "#,
        )
        .bind(&backtest_id)
        .bind(&self.timestamp)
        .fetch_one(tx)
        .await?;

        // Extract the id directly from the row
        let id: i32 = result.try_get("id")?;

        Ok(id)
    }

    async fn retrieve_query(pool: &PgPool, backtest_id: i32) -> Result<Vec<Self>> {
        let results = sqlx::query(
            r#"
            SELECT id, timestamp 
            FROM bt_Signal
            WHERE backtest_id = $1
            "#,
        )
        .bind(backtest_id)
        .fetch_all(pool)
        .await?;

        let mut signals = Vec::new();

        for row in results {
            let timestamp = row.try_get::<i64, _>("timestamp")?;
            let signal_id = row.try_get::<i32, _>("id")?;
            let trade_instructions =
                SignalInstructions::retrieve_query(pool, backtest_id, signal_id).await?;

            let signal = Signals {
                timestamp,
                trade_instructions,
            };
            signals.push(signal);
        }
        Ok(signals)
    }
}

pub async fn create_backtest_related(
    tx: &mut Transaction<'_, Postgres>,
    backtest_data: &BacktestData,
) -> Result<i32> {
    info!(
        "Creating backtest related data for backtest name: {}",
        backtest_data.backtest_name
    );

    // Insert into Backtest table
    let backtest_id = backtest_data.insert_query(tx).await?;

    // Insert Parameters
    backtest_data
        .parameters
        .insert_query(tx, backtest_id)
        .await?;
    info!(
        "Successfully inserted parameters for backtest id {}",
        backtest_id
    );
    // Insert StaticStats
    backtest_data
        .static_stats
        .insert_query(tx, backtest_id)
        .await?;
    info!(
        "Successfully inserted static stats for backtest id {}",
        backtest_id
    );
    // Insert Period Timeseries Stats
    for period_stat in &backtest_data.period_timeseries_stats {
        period_stat
            .insert_query(tx, TimeseriesTypes::PERIOD, backtest_id)
            .await?;
    }
    info!(
        "Successfully inserted period timeseries stats for backtest id {}",
        backtest_id
    );

    // Insert Daily Timeseries Stats
    for daily_stat in &backtest_data.daily_timeseries_stats {
        daily_stat
            .insert_query(tx, TimeseriesTypes::DAILY, backtest_id)
            .await?;
    }
    info!(
        "Successfully inserted daily timeseries stats for backtest id {}",
        backtest_id
    );

    // Insert Trade
    for trade in &backtest_data.trades {
        trade.insert_query(tx, backtest_id).await?;
    }
    info!(
        "Successfully inserted trades for backtest id {}",
        backtest_id
    );

    // Insert Signal
    for signal in &backtest_data.signals {
        let signal_id = signal.insert_query(tx, backtest_id).await.unwrap();
        for instruction in &signal.trade_instructions {
            instruction
                .insert_query(tx, backtest_id, signal_id)
                .await
                .unwrap();
        }
    }
    info!(
        "Successfully inserted signals for backtest id {}",
        backtest_id
    );

    Ok(backtest_id)
}

pub async fn retrieve_backtest_related(pool: &PgPool, backtest_id: i32) -> Result<BacktestData> {
    info!("Retrieving full backtest data for id {}", backtest_id);

    // Retrieve components
    let backtest_name = BacktestData::retrieve_query(pool, backtest_id).await?;
    let parameters = Parameters::retrieve_query(pool, backtest_id).await?;
    let static_stats = StaticStats::retrieve_query(pool, backtest_id).await?;
    let period_timeseries_stats =
        TimeseriesStats::retrieve_query(pool, TimeseriesTypes::PERIOD, backtest_id).await?;
    let daily_timeseries_stats =
        TimeseriesStats::retrieve_query(pool, TimeseriesTypes::DAILY, backtest_id).await?;
    let trades = Trades::retrieve_query(pool, backtest_id).await?;
    let signals = Signals::retrieve_query(pool, backtest_id).await?;
    info!(
        "Successfully retrieved all related data for backtest id {}",
        backtest_id
    );

    Ok(BacktestData {
        backtest_id: Some(backtest_id as u16),
        backtest_name,
        parameters,
        static_stats,
        period_timeseries_stats,
        daily_timeseries_stats,
        trades,
        signals,
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
    async fn test_insert_backtest() {
        dotenv::dotenv().ok();
        let pool = init_db().await.unwrap();
        let mut transaction = pool
            .begin()
            .await
            .expect("Error setting up test transaction.");

        // Pull test data
        let mock_data =
            fs::read_to_string("tests/data/test_data.backtest.json").expect("Unable to read file");
        let backtest_data: BacktestData =
            serde_json::from_str(&mock_data).expect("JSON was not well-formatted");

        // Test
        let backtest_id = backtest_data
            .insert_query(&mut transaction)
            .await
            .expect("Error on insert.");

        // Validate
        assert!(
            backtest_id > 0,
            "Expected a valid backtest ID greater than 0"
        );
    }

    #[sqlx::test]
    #[serial]
    async fn test_retrieve_backtest_list() {
        dotenv::dotenv().ok();
        let pool = init_db().await.unwrap();
        let mut transaction = pool
            .begin()
            .await
            .expect("Error setting up test transaction.");

        // Pull test data
        let mock_data =
            fs::read_to_string("tests/data/test_data.backtest.json").expect("Unable to read file");
        let backtest_data: BacktestData =
            serde_json::from_str(&mock_data).expect("JSON was not well-formatted");

        let backtest_id = backtest_data
            .insert_query(&mut transaction)
            .await
            .expect("Error on insert.");
        let _ = transaction.commit().await;

        // Test
        let result = BacktestData::retrieve_list_query(&pool)
            .await
            .expect("Error geting backtest list.");

        // Validate
        assert!(result.len() > 0);

        // Cleanup
        let mut transaction = pool
            .begin()
            .await
            .expect("Error setting up test transaction.");
        BacktestData::delete_query(&mut transaction, backtest_id)
            .await
            .expect("Error on delete.");
        let _ = transaction.commit().await;
    }

    #[sqlx::test]
    #[serial]
    async fn test_retrieve_backtest() {
        dotenv::dotenv().ok();
        let pool = init_db().await.unwrap();
        let mut transaction = pool
            .begin()
            .await
            .expect("Error setting up test transaction.");

        // Pull test data
        let mock_data =
            fs::read_to_string("tests/data/test_data.backtest.json").expect("Unable to read file");
        let backtest_data: BacktestData =
            serde_json::from_str(&mock_data).expect("JSON was not well-formatted");

        // Create backtest
        let backtest_id = backtest_data
            .insert_query(&mut transaction)
            .await
            .expect("Error on insert.");
        let _ = transaction.commit().await;

        // Test
        let result: String = BacktestData::retrieve_query(&pool, backtest_id)
            .await
            .expect("Error retrieving parameters.");

        // Validate
        assert_eq!(result, backtest_data.backtest_name);

        // Cleanup
        let mut transaction = pool
            .begin()
            .await
            .expect("Error setting up test transaction.");
        BacktestData::delete_query(&mut transaction, backtest_id)
            .await
            .expect("Error on delete.");
        let _ = transaction.commit().await;
    }

    #[sqlx::test]
    #[serial]
    async fn create_parameters() {
        dotenv::dotenv().ok();
        let pool = init_db().await.unwrap();
        let mut transaction = pool
            .begin()
            .await
            .expect("Error setting up test transaction.");

        // Pull test data
        let mock_data =
            fs::read_to_string("tests/data/test_data.backtest.json").expect("Unable to read file");
        let backtest_data: BacktestData =
            serde_json::from_str(&mock_data).expect("JSON was not well-formatted");

        let backtest_id = backtest_data
            .insert_query(&mut transaction)
            .await
            .expect("Error on insert.");

        // Test
        let result = backtest_data
            .parameters
            .insert_query(&mut transaction, backtest_id)
            .await
            .expect("Error on parameters insert test.");

        // Validate
        assert_eq!(result, ());
    }

    #[sqlx::test]
    #[serial]
    async fn retrieve_parameters() {
        dotenv::dotenv().ok();
        let pool = init_db().await.unwrap();
        let mut transaction = pool
            .begin()
            .await
            .expect("Error setting up test transaction.");

        // Pull test data
        let mock_data =
            fs::read_to_string("tests/data/test_data.backtest.json").expect("Unable to read file");
        let backtest_data: BacktestData =
            serde_json::from_str(&mock_data).expect("JSON was not well-formatted");

        // Create
        let backtest_id = backtest_data
            .insert_query(&mut transaction)
            .await
            .expect("Error on insert.");

        let _ = backtest_data
            .parameters
            .insert_query(&mut transaction, backtest_id)
            .await
            .expect("Error on parameters insert test.");

        let _ = transaction.commit().await;

        // Test
        let result: Parameters = Parameters::retrieve_query(&pool, backtest_id)
            .await
            .expect("Error retrieving parameters.");

        // Validate
        assert_eq!(result.strategy_name, backtest_data.parameters.strategy_name);

        // Cleanup
        let mut transaction = pool
            .begin()
            .await
            .expect("Error setting up test transaction.");
        BacktestData::delete_query(&mut transaction, backtest_id)
            .await
            .expect("Error on delete.");
        let _ = transaction.commit().await;
    }

    #[sqlx::test]
    #[serial]
    async fn create_staticstats() {
        dotenv::dotenv().ok();
        let pool = init_db().await.unwrap();
        let mut transaction = pool
            .begin()
            .await
            .expect("Error setting up test transaction.");

        // Pull test data
        let mock_data =
            fs::read_to_string("tests/data/test_data.backtest.json").expect("Unable to read file");
        let backtest_data: BacktestData =
            serde_json::from_str(&mock_data).expect("JSON was not well-formatted");

        let backtest_id = backtest_data
            .insert_query(&mut transaction)
            .await
            .expect("Error on insert.");

        // Test
        let result = backtest_data
            .static_stats
            .insert_query(&mut transaction, backtest_id)
            .await
            .expect("Error on parameters insert test.");

        // Validate
        assert_eq!(result, ());
    }

    #[sqlx::test]
    #[serial]
    async fn retrieve_staticstats() {
        dotenv::dotenv().ok();
        let pool = init_db().await.unwrap();
        let mut transaction = pool
            .begin()
            .await
            .expect("Error setting up test transaction.");

        // Pull test data
        let mock_data =
            fs::read_to_string("tests/data/test_data.backtest.json").expect("Unable to read file");
        let backtest_data: BacktestData =
            serde_json::from_str(&mock_data).expect("JSON was not well-formatted");

        // Create
        let backtest_id = backtest_data
            .insert_query(&mut transaction)
            .await
            .expect("Error on insert.");

        let _ = backtest_data
            .static_stats
            .insert_query(&mut transaction, backtest_id)
            .await
            .expect("Error on parameters insert test.");

        let _ = transaction.commit().await;

        // Test
        let result: StaticStats = StaticStats::retrieve_query(&pool, backtest_id)
            .await
            .expect("Error retriving static stats.");

        // Validate
        assert_eq!(result.net_profit, backtest_data.static_stats.net_profit);

        // Cleanup
        let mut transaction = pool
            .begin()
            .await
            .expect("Error setting up test transaction.");
        BacktestData::delete_query(&mut transaction, backtest_id)
            .await
            .expect("Error on delete.");
        let _ = transaction.commit().await;
    }

    #[sqlx::test]
    #[serial]
    async fn create_periodtimeseriesstats() {
        dotenv::dotenv().ok();
        let pool = init_db().await.unwrap();
        let mut transaction = pool
            .begin()
            .await
            .expect("Error setting up test transaction.");

        // Pull test data
        let mock_data =
            fs::read_to_string("tests/data/test_data.backtest.json").expect("Unable to read file");
        let backtest_data: BacktestData =
            serde_json::from_str(&mock_data).expect("JSON was not well-formatted");

        let backtest_id = backtest_data
            .insert_query(&mut transaction)
            .await
            .expect("Error on insert.");

        // Test
        for i in backtest_data.period_timeseries_stats {
            let result = i
                .insert_query(&mut transaction, TimeseriesTypes::PERIOD, backtest_id)
                .await
                .expect("Error on insert.");

            // Validate
            assert_eq!(result, ());
        }
    }

    #[sqlx::test]
    #[serial]
    async fn retrieve_periodtimeseriesstats() {
        dotenv::dotenv().ok();
        let pool = init_db().await.unwrap();
        let mut transaction = pool
            .begin()
            .await
            .expect("Error setting up test transaction.");

        // Pull test data
        let mock_data =
            fs::read_to_string("tests/data/test_data.backtest.json").expect("Unable to read file");
        let backtest_data: BacktestData =
            serde_json::from_str(&mock_data).expect("JSON was not well-formatted");

        // Create
        let backtest_id = backtest_data
            .insert_query(&mut transaction)
            .await
            .expect("Error on insert.");

        for i in &backtest_data.period_timeseries_stats {
            let _ = i
                .insert_query(&mut transaction, TimeseriesTypes::PERIOD, backtest_id)
                .await
                .expect("Error on insert.");
        }

        let _ = transaction.commit().await;

        // Test
        let result: Vec<TimeseriesStats> =
            TimeseriesStats::retrieve_query(&pool, TimeseriesTypes::PERIOD, backtest_id)
                .await
                .expect("Error retriving static stats.");

        // Validate
        assert_eq!(
            result[0].equity_value,
            backtest_data.period_timeseries_stats[0].equity_value
        );

        // Cleanup
        let mut transaction = pool
            .begin()
            .await
            .expect("Error setting up test transaction.");
        BacktestData::delete_query(&mut transaction, backtest_id)
            .await
            .expect("Error on delete.");
        let _ = transaction.commit().await;
    }

    #[sqlx::test]
    #[serial]
    async fn create_dailytimeseriesstats() {
        dotenv::dotenv().ok();
        let pool = init_db().await.unwrap();
        let mut transaction = pool
            .begin()
            .await
            .expect("Error setting up test transaction.");

        // Pull test data
        let mock_data =
            fs::read_to_string("tests/data/test_data.backtest.json").expect("Unable to read file");
        let backtest_data: BacktestData =
            serde_json::from_str(&mock_data).expect("JSON was not well-formatted");

        let backtest_id = backtest_data
            .insert_query(&mut transaction)
            .await
            .expect("Error on insert.");

        // Test
        for i in backtest_data.daily_timeseries_stats {
            let result = i
                .insert_query(&mut transaction, TimeseriesTypes::DAILY, backtest_id)
                .await
                .expect("Error in daily timeseries.");

            // Validate
            assert_eq!(result, ());
        }
    }

    #[sqlx::test]
    #[serial]
    async fn retrieve_dailytimeseriesstats() {
        dotenv::dotenv().ok();
        let pool = init_db().await.unwrap();
        let mut transaction = pool
            .begin()
            .await
            .expect("Error setting up test transaction.");

        // Pull test data
        let mock_data =
            fs::read_to_string("tests/data/test_data.backtest.json").expect("Unable to read file");
        let backtest_data: BacktestData =
            serde_json::from_str(&mock_data).expect("JSON was not well-formatted");

        // Create
        let backtest_id = backtest_data
            .insert_query(&mut transaction)
            .await
            .expect("Error on insert.");

        for i in &backtest_data.daily_timeseries_stats {
            let _ = i
                .insert_query(&mut transaction, TimeseriesTypes::DAILY, backtest_id)
                .await
                .expect("Error on insert.");
        }

        let _ = transaction.commit().await;

        // Test
        let result: Vec<TimeseriesStats> =
            TimeseriesStats::retrieve_query(&pool, TimeseriesTypes::DAILY, backtest_id)
                .await
                .expect("Error retriving static stats.");

        // Validate
        assert_eq!(
            result[0].equity_value,
            backtest_data.daily_timeseries_stats[0].equity_value
        );

        // Cleanup
        let mut transaction = pool
            .begin()
            .await
            .expect("Error setting up test transaction.");
        BacktestData::delete_query(&mut transaction, backtest_id)
            .await
            .expect("Error on delete.");
        let _ = transaction.commit().await;
    }

    #[sqlx::test]
    #[serial]
    async fn create_trades() {
        dotenv::dotenv().ok();
        let pool = init_db().await.unwrap();
        let mut transaction = pool
            .begin()
            .await
            .expect("Error setting up test transaction.");

        // Pull test data
        let mock_data =
            fs::read_to_string("tests/data/test_data.backtest.json").expect("Unable to read file");
        let backtest_data: BacktestData =
            serde_json::from_str(&mock_data).expect("JSON was not well-formatted");

        let backtest_id = backtest_data
            .insert_query(&mut transaction)
            .await
            .expect("Error on insert.");

        // Test
        for i in backtest_data.trades {
            let result = i
                .insert_query(&mut transaction, backtest_id)
                .await
                .expect("Error on insert.");

            // Validate
            assert_eq!(result, ());
        }
    }

    #[sqlx::test]
    #[serial]
    async fn retrieve_trades() {
        dotenv::dotenv().ok();
        let pool = init_db().await.unwrap();
        let mut transaction = pool
            .begin()
            .await
            .expect("Error setting up test transaction.");

        // Pull test data
        let mock_data =
            fs::read_to_string("tests/data/test_data.backtest.json").expect("Unable to read file");
        let backtest_data: BacktestData =
            serde_json::from_str(&mock_data).expect("JSON was not well-formatted");

        // Create
        let backtest_id = backtest_data
            .insert_query(&mut transaction)
            .await
            .expect("Error on insert.");

        for i in &backtest_data.trades {
            let _ = i
                .insert_query(&mut transaction, backtest_id)
                .await
                .expect("Error on insert.");
        }

        let _ = transaction.commit().await;

        // Test
        let result: Vec<Trades> = Trades::retrieve_query(&pool, backtest_id)
            .await
            .expect("Error retriving static stats.");

        // Validate
        assert_eq!(result[0].avg_price, backtest_data.trades[0].avg_price);

        // Cleanup
        let mut transaction = pool
            .begin()
            .await
            .expect("Error setting up test transaction.");
        BacktestData::delete_query(&mut transaction, backtest_id)
            .await
            .expect("Error on delete.");
        let _ = transaction.commit().await;
    }

    #[sqlx::test]
    async fn create_signals() -> anyhow::Result<()> {
        dotenv::dotenv().ok();
        let pool = init_db().await.unwrap();
        let mut transaction = pool
            .begin()
            .await
            .expect("Error setting up test transaction.");

        // Pull test data
        let mock_data =
            fs::read_to_string("tests/data/test_data.backtest.json").expect("Unable to read file");
        let backtest_data: BacktestData =
            serde_json::from_str(&mock_data).expect("JSON was not well-formatted");

        let backtest_id = backtest_data
            .insert_query(&mut transaction)
            .await
            .expect("Error on insert.");

        // Test
        for signal in &backtest_data.signals {
            let signal_id = signal.insert_query(&mut transaction, backtest_id).await?;

            for instruction in &signal.trade_instructions {
                let result = instruction
                    .insert_query(&mut transaction, backtest_id, signal_id)
                    .await
                    .unwrap();

                // Validate
                assert_eq!(result, ());
            }
        }
        Ok(())
    }

    #[sqlx::test]
    #[serial]
    async fn retrieve_signals() {
        dotenv::dotenv().ok();
        let pool = init_db().await.unwrap();
        let mut transaction = pool
            .begin()
            .await
            .expect("Error setting up test transaction.");

        // Pull test data
        let mock_data =
            fs::read_to_string("tests/data/test_data.backtest.json").expect("Unable to read file");
        let backtest_data: BacktestData =
            serde_json::from_str(&mock_data).expect("JSON was not well-formatted");

        // Create
        let backtest_id = backtest_data
            .insert_query(&mut transaction)
            .await
            .expect("Error on insert.");

        for i in &backtest_data.signals {
            let _ = i
                .insert_query(&mut transaction, backtest_id)
                .await
                .expect("Error on insert.");
        }

        let _ = transaction.commit().await;

        // Test
        let result: Vec<Signals> = Signals::retrieve_query(&pool, backtest_id)
            .await
            .expect("Error retriving static stats.");

        // Validate
        assert_eq!(result[0].timestamp, backtest_data.signals[0].timestamp);

        // Cleanup
        let mut transaction = pool
            .begin()
            .await
            .expect("Error setting up test transaction.");
        BacktestData::delete_query(&mut transaction, backtest_id)
            .await
            .expect("Error on delete.");
        let _ = transaction.commit().await;
    }

    #[sqlx::test]
    #[serial]
    async fn test_create_backtest_related() {
        dotenv::dotenv().ok();
        let pool = init_db().await.unwrap();
        let mut transaction = pool
            .begin()
            .await
            .expect("Error setting up test transaction.");

        // Pull test data
        let mock_data =
            fs::read_to_string("tests/data/test_data.backtest.json").expect("Unable to read file");
        let backtest_data: BacktestData =
            serde_json::from_str(&mock_data).expect("JSON was not well-formatted");

        // Test
        let id = create_backtest_related(&mut transaction, &backtest_data)
            .await
            .expect("Error on creating backtest and related.");

        // Validate
        assert!(id > 0);
    }

    #[sqlx::test]
    #[serial]
    async fn test_retrieve_backtest_related() {
        dotenv::dotenv().ok();
        let pool = init_db().await.unwrap();
        let mut transaction = pool
            .begin()
            .await
            .expect("Error setting up test transaction.");

        // Pull test data
        let mock_data =
            fs::read_to_string("tests/data/test_data.backtest.json").expect("Unable to read file");
        let backtest_data: BacktestData =
            serde_json::from_str(&mock_data).expect("JSON was not well-formatted");

        // Create backtest
        let backtest_id = create_backtest_related(&mut transaction, &backtest_data)
            .await
            .expect("Error on creating backtest and related.");

        let _ = transaction.commit().await;

        // Test
        let result: BacktestData = retrieve_backtest_related(&pool, backtest_id)
            .await
            .expect("Error while retrieving backtest.");

        // Validate
        assert_eq!(backtest_data.backtest_name, result.backtest_name);

        // Cleanup
        let mut transaction = pool
            .begin()
            .await
            .expect("Error setting up test transaction.");
        BacktestData::delete_query(&mut transaction, backtest_id)
            .await
            .expect("Error on delete.");
        let _ = transaction.commit().await;
    }
}
