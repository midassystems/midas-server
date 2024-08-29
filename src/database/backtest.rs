use crate::Result;
use async_trait::async_trait;
use mbn::backtest::{
    BacktestData, DailyTimeseriesStats, Parameters, PeriodTimeseriesStats, Signals, StaticStats,
    Trades,
};
use sqlx::{PgPool, Postgres, Row, Transaction};

#[async_trait]
pub trait BacktestDataQueries {
    async fn retrieve_backtest_names(pool: &PgPool) -> Result<Vec<(i32, String)>>;
    async fn insert_query(&self, tx: &mut Transaction<'_, Postgres>) -> Result<i32>;
    async fn delete_query(tx: &mut Transaction<'_, Postgres>, backtest_id: i32) -> Result<()>;
    async fn retrieve_query(pool: &PgPool, id: i32) -> Result<String>;
}

#[async_trait]
impl BacktestDataQueries for BacktestData {
    async fn retrieve_backtest_names(pool: &PgPool) -> Result<Vec<(i32, String)>> {
        let rows: Vec<(i32, String)> = sqlx::query_as(
            r#"
            SELECT id, backtest_name
            FROM Backtest
            "#,
        )
        .fetch_all(pool)
        .await?;

        Ok(rows)
    }

    async fn insert_query(&self, tx: &mut Transaction<'_, Postgres>) -> Result<i32> {
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

        Ok(id)
    }

    async fn delete_query(tx: &mut Transaction<'_, Postgres>, backtest_id: i32) -> Result<()> {
        sqlx::query(
            r#"
            DELETE FROM Backtest WHERE id = $1
            "#,
        )
        .bind(backtest_id)
        .execute(tx)
        .await?;

        Ok(())
    }

    async fn retrieve_query(pool: &PgPool, id: i32) -> Result<String> {
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

        Ok(backtest_name)
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
            INSERT INTO Parameters (backtest_id, strategy_name, capital, data_type, train_start, train_end, test_start, test_end, tickers)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            "#
        )
        .bind(&backtest_id)
        .bind(&self.strategy_name)
        .bind(&self.capital)
        .bind(&self.data_type)
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
            SELECT strategy_name, capital, data_type, train_start, train_end, test_start, test_end, tickers
            FROM Parameters
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
            INSERT INTO backtest_StaticStats (
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
            FROM backtest_StaticStats
            WHERE backtest_id = $1
            "#,
        )
        .bind(backtest_id)
        .fetch_one(pool)
        .await?;

        Ok(row)
    }
}

#[async_trait]
pub trait TimeseriesQueries {
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
impl TimeseriesQueries for PeriodTimeseriesStats {
    async fn insert_query(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        backtest_id: i32,
    ) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO backtest_PeriodTimeseriesStats (backtest_id, timestamp, equity_value, percent_drawdown, cumulative_return, period_return) 
            VALUES ($1, $2, $3, $4, $5, $6)
            "#
        )
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

    async fn retrieve_query(pool: &PgPool, backtest_id: i32) -> Result<Vec<Self>> {
        let result : Vec<PeriodTimeseriesStats> = sqlx::query_as(
            r#"
            SELECT backtest_id, timestamp, equity_value, percent_drawdown, cumulative_return, period_return
            FROM backtest_PeriodTimeseriesStats
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
impl TimeseriesQueries for DailyTimeseriesStats {
    async fn insert_query(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        backtest_id: i32,
    ) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO backtest_DailyTimeseriesStats (backtest_id, timestamp, equity_value, percent_drawdown, cumulative_return, period_return)
            VALUES ($1, $2, $3, $4, $5, $6)
            "#
        )
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

    async fn retrieve_query(pool: &PgPool, backtest_id: i32) -> Result<Vec<Self>> {
        let result : Vec<DailyTimeseriesStats> = sqlx::query_as(
            r#"
            SELECT backtest_id, timestamp, equity_value, percent_drawdown, cumulative_return, period_return
            FROM backtest_DailyTimeseriesStats
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
            INSERT INTO backtest_Trade (backtest_id, trade_id, leg_id, timestamp, ticker, quantity, avg_price, trade_value, action, fees)
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
            FROM backtest_Trade
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
pub trait SignalQueries {
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
impl SignalQueries for Signals {
    async fn insert_query(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        backtest_id: i32,
    ) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO backtest_Signal (backtest_id, timestamp, trade_instructions)
            VALUES ($1, $2, $3)
            "#,
        )
        .bind(&backtest_id)
        .bind(&self.timestamp)
        .bind(&self.trade_instructions)
        .execute(tx)
        .await?;
        Ok(())
    }

    async fn retrieve_query(pool: &PgPool, backtest_id: i32) -> Result<Vec<Self>> {
        let results: Vec<Signals> = sqlx::query_as(
            r#"
            SELECT id, timestamp, trade_instructions 
            FROM backtest_Signal
            WHERE backtest_id = $1
            "#,
        )
        .bind(backtest_id)
        .fetch_all(pool)
        .await?;

        Ok(results)
    }
}

pub async fn create_backtest_related(
    tx: &mut Transaction<'_, Postgres>,
    backtest_data: &BacktestData,
) -> Result<i32> {
    // Insert into Backtest table
    let backtest_id = backtest_data.insert_query(tx).await?;

    // Insert Parameters
    backtest_data
        .parameters
        .insert_query(tx, backtest_id)
        .await?;

    // Insert StaticStats
    backtest_data
        .static_stats
        .insert_query(tx, backtest_id)
        .await?;

    // Insert Period Timeseries Stats
    for period_stat in &backtest_data.period_timeseries_stats {
        period_stat.insert_query(tx, backtest_id).await?;
    }

    // Insert Daily Timeseries Stats
    for daily_stat in &backtest_data.daily_timeseries_stats {
        daily_stat.insert_query(tx, backtest_id).await?;
    }

    // Insert Trade
    for trade in &backtest_data.trades {
        trade.insert_query(tx, backtest_id).await?;
    }

    // Insert Signal
    for signal in &backtest_data.signals {
        signal.insert_query(tx, backtest_id).await.unwrap();
    }

    Ok(backtest_id)
}

pub async fn retrieve_backtest_related(pool: &PgPool, backtest_id: i32) -> Result<BacktestData> {
    let backtest_name = BacktestData::retrieve_query(pool, backtest_id).await?;
    let parameters = Parameters::retrieve_query(pool, backtest_id).await?;
    let static_stats = StaticStats::retrieve_query(pool, backtest_id).await?;
    let period_timeseries_stats = PeriodTimeseriesStats::retrieve_query(pool, backtest_id).await?;
    let daily_timeseries_stats = DailyTimeseriesStats::retrieve_query(pool, backtest_id).await?;
    let trades = Trades::retrieve_query(pool, backtest_id).await?;
    let signals = Signals::retrieve_query(pool, backtest_id).await?;

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
    use crate::database::init::init_pg_db;
    use serial_test::serial;
    use std::fs;

    #[sqlx::test]
    #[serial]
    async fn test_create_backtest() {
        dotenv::dotenv().ok();
        let pool = init_pg_db().await.unwrap();
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
    async fn test_get_backtest_list() {
        dotenv::dotenv().ok();
        let pool = init_pg_db().await.unwrap();
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
        let _ = BacktestData::retrieve_backtest_names(&pool)
            .await
            .expect("Error geting backtest list.");

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
        let pool = init_pg_db().await.unwrap();
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
        let pool = init_pg_db().await.unwrap();
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
        let pool = init_pg_db().await.unwrap();
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
        let pool = init_pg_db().await.unwrap();
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
        let pool = init_pg_db().await.unwrap();
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
        let pool = init_pg_db().await.unwrap();
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
                .insert_query(&mut transaction, backtest_id)
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
        let pool = init_pg_db().await.unwrap();
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
                .insert_query(&mut transaction, backtest_id)
                .await
                .expect("Error on insert.");
        }

        let _ = transaction.commit().await;

        // Test
        let result: Vec<PeriodTimeseriesStats> =
            PeriodTimeseriesStats::retrieve_query(&pool, backtest_id)
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
        let pool = init_pg_db().await.unwrap();
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
                .insert_query(&mut transaction, backtest_id)
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
        let pool = init_pg_db().await.unwrap();
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
                .insert_query(&mut transaction, backtest_id)
                .await
                .expect("Error on insert.");
        }

        let _ = transaction.commit().await;

        // Test
        let result: Vec<DailyTimeseriesStats> =
            DailyTimeseriesStats::retrieve_query(&pool, backtest_id)
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
        let pool = init_pg_db().await.unwrap();
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
        let pool = init_pg_db().await.unwrap();
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
    async fn create_signals() {
        dotenv::dotenv().ok();
        let pool = init_pg_db().await.unwrap();
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
        for i in backtest_data.signals {
            let result = i
                .insert_query(&mut transaction, backtest_id)
                .await
                .expect("Error on signal create.");

            // Validate
            assert_eq!(result, ());
        }
    }

    #[sqlx::test]
    #[serial]
    async fn retrieve_signals() {
        dotenv::dotenv().ok();
        let pool = init_pg_db().await.unwrap();
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
        let pool = init_pg_db().await.unwrap();
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
        let pool = init_pg_db().await.unwrap();
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
