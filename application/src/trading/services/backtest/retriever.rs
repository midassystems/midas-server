use std::sync::Arc;

use crate::trading::database::backtest::{
    BacktestDataQueries, ParametersQueries, SignalQueries, StaticStatsQueries, TimeseriesQueries,
    TimeseriesTypes, TradesQueries,
};
use crate::Result;
use mbinary::backtest::{
    BacktestData, BacktestMetaData, Parameters, Signals, StaticStats, TimeseriesStats, Trades,
};
use sqlx::PgPool;
use tracing::info;

pub struct BacktestRetriever {
    pool: Arc<PgPool>,
    backtest_id: i32,
}

impl BacktestRetriever {
    pub fn new(pool: Arc<PgPool>, backtest_id: i32) -> Self {
        BacktestRetriever { pool, backtest_id }
    }

    pub async fn retrieve_backtest(&self) -> Result<BacktestData> {
        info!("Retrieving full backtest data for id {}", self.backtest_id);

        // Retrieve components
        let backtest_name = BacktestData::retrieve_query(&self.pool, self.backtest_id).await?;
        let parameters = Parameters::retrieve_query(&self.pool, self.backtest_id, true).await?;
        let static_stats = StaticStats::retrieve_query(&self.pool, self.backtest_id).await?;
        let period_timeseries_stats =
            TimeseriesStats::retrieve_query(&self.pool, TimeseriesTypes::PERIOD, self.backtest_id)
                .await?;
        let daily_timeseries_stats =
            TimeseriesStats::retrieve_query(&self.pool, TimeseriesTypes::DAILY, self.backtest_id)
                .await?;
        let trades = Trades::retrieve_query(&self.pool, self.backtest_id, true).await?;
        let signals = Signals::retrieve_query(&self.pool, self.backtest_id, true).await?;
        info!(
            "Successfully retrieved data for backtest: {}",
            self.backtest_id
        );

        let metadata = BacktestMetaData::new(
            Some(self.backtest_id as u16),
            &backtest_name,
            parameters,
            static_stats,
        );

        Ok(BacktestData {
            metadata,
            period_timeseries_stats,
            daily_timeseries_stats,
            trades,
            signals,
        })
    }
}
