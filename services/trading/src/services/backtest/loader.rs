use crate::database::backtest::{
    create_backtest_query, BacktestDataQueries, ParametersQueries, SignalInstructionsQueries,
    SignalQueries, StaticStatsQueries, TimeseriesQueries, TimeseriesTypes, TradesQueries,
};
use crate::response::ApiResponse;
use crate::Result;
use async_stream::stream;
use axum::http::StatusCode;
use bytes::Bytes;
use futures::stream::Stream;
use mbn::backtest::{BacktestData, BacktestMetaData, Signals, TimeseriesStats, Trades};
use mbn::backtest_decoder::BacktestDecoder;
use sqlx::PgPool;
use std::pin::Pin;
use tracing::error;

pub struct BacktestLoader {
    pool: PgPool,
    backtest_id: Option<i32>,
}

impl BacktestLoader {
    pub async fn new(pool: PgPool) -> Result<Self> {
        Ok(BacktestLoader {
            pool,
            backtest_id: None,
        })
    }

    pub async fn create_metadata(&mut self, metadata: &BacktestMetaData) -> Result<()> {
        let mut tx = self.pool.begin().await?;

        // Insert into Backtest table
        self.backtest_id = Some(create_backtest_query(&metadata.backtest_name, &mut tx).await?);

        // Insert Parameters
        metadata
            .parameters
            .insert_query(&mut tx, self.backtest_id.unwrap(), true)
            .await?;

        // Insert StaticStats
        metadata
            .static_stats
            .insert_query(&mut tx, self.backtest_id.unwrap())
            .await?;

        tx.commit().await?;

        Ok(())
    }

    pub async fn create_timeseries_stats(
        &self,
        data: &Vec<TimeseriesStats>,
        data_type: TimeseriesTypes,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;

        for i in data {
            i.insert_query(&mut tx, data_type, self.backtest_id.unwrap())
                .await?;
        }
        tx.commit().await?;

        Ok(())
    }

    pub async fn create_trade(&self, data: &Vec<Trades>) -> Result<()> {
        let mut tx = self.pool.begin().await?;

        for i in data {
            i.insert_query(&mut tx, self.backtest_id.unwrap(), true)
                .await?;
        }
        tx.commit().await?;

        Ok(())
    }

    pub async fn create_signals(&self, data: &Vec<Signals>) -> Result<()> {
        let mut tx = self.pool.begin().await?;

        // Insert Signal
        for signal in data {
            let signal_id = signal
                .insert_query(&mut tx, self.backtest_id.unwrap(), true)
                .await
                .unwrap();
            for instruction in &signal.trade_instructions {
                instruction
                    .insert_query(&mut tx, self.backtest_id.unwrap(), signal_id, true)
                    .await
                    .unwrap();
            }
        }
        tx.commit().await?;

        Ok(())
    }

    pub async fn cleanup(&self) -> Result<ApiResponse<String>> {
        let mut tx = self.pool.begin().await?;
        match BacktestData::delete_query(&mut tx, self.backtest_id.unwrap()).await {
            Ok(_) => {
                tx.commit().await?;
                let response = ApiResponse::new(
                    "success",
                    "Removed all commits from this batch process.",
                    StatusCode::OK,
                    "".to_string(),
                );
                Ok(response)
            }
            Err(e) => {
                tx.rollback().await?;
                error!("Fialed to remove all batched from current process: {:?}", e);
                let response = ApiResponse::new(
                    "failed",
                    &format!("Fialed to remove all batched from current process: {:?}", e),
                    StatusCode::CONFLICT,
                    "".to_string(),
                );
                Ok(response)
            }
        }
    }

    pub async fn process_stream<R>(
        mut self,
        mut decoder: BacktestDecoder<R>,
    ) -> Pin<Box<dyn Stream<Item = Result<Bytes>> + Send>>
    where
        R: std::io::Read + Send + 'static,
    {
        let p_stream = stream! {
                let metadata = decoder.decode_metadata()?;
                self.create_metadata(&metadata).await?;

                let period_stats = decoder.decode_timeseries()?;
                self.create_timeseries_stats(&period_stats, TimeseriesTypes::PERIOD).await?;


                let daily_stats = decoder.decode_timeseries()?;
                self.create_timeseries_stats(&daily_stats, TimeseriesTypes::DAILY).await?;

                let trades = decoder.decode_trades()?;
                self.create_trade(&trades).await?;

                let signals = decoder.decode_signals()?;
                self.create_signals(&signals).await?;

            // Success response
            let response = ApiResponse::new(
                "success",
                &format!("Successfully created backest {} with id {}", metadata.backtest_name, self.backtest_id.unwrap()),
                StatusCode::OK,
                format!("{}", self.backtest_id.unwrap()),
            );
            yield Ok(response.bytes());

        };
        Box::pin(p_stream) // Explicitly pin the stream and return it
    }
}
