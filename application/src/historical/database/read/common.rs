use super::{
    equities::{
        EQUITIES_BBO_QUERY, EQUITIES_MBP1_QUERY, EQUITIES_OHLCV_QUERY, EQUITIES_TRADE_QUERY,
    },
    futures::{
        FUTURES_BBO_QUERY, FUTURES_CALENDAR_ROLLING_WINDOW, FUTURES_MBP1_QUERY,
        FUTURES_OHLCV_QUERY, FUTURES_TRADE_QUERY, FUTURES_VOLUME_ROLLING_WINDOW,
    },
    option::{OPTION_BBO_QUERY, OPTION_MBP1_QUERY, OPTION_OHLCV_QUERY, OPTION_TRADE_QUERY},
};
use crate::Error;
use crate::{historical::services::retrieve::retriever::ContinuousKind, Result};
use async_trait::async_trait;
use futures::Stream;
use mbinary::enums::{Dataset, RType, Schema};
use mbinary::params::RetrieveParams;
use mbinary::record_enum::RecordEnum;
use mbinary::records::{BboMsg, Mbp1Msg, OhlcvMsg, TbboMsg, TradeMsg};
use sqlx::{prelude::FromRow, PgPool};
use std::{collections::HashMap, pin::Pin};

pub enum QueryType {
    Mbp,
    Trade,
    Ohlcv,
    Bbo,
}

impl QueryType {
    fn get_query(self, dataset: Dataset) -> &'static str {
        match self {
            QueryType::Mbp => match dataset {
                Dataset::Futures => FUTURES_MBP1_QUERY,
                Dataset::Equities => EQUITIES_MBP1_QUERY,
                Dataset::Option => OPTION_MBP1_QUERY,
            },
            QueryType::Trade => match dataset {
                Dataset::Futures => FUTURES_TRADE_QUERY,
                Dataset::Equities => EQUITIES_TRADE_QUERY,
                Dataset::Option => OPTION_TRADE_QUERY,
            },
            QueryType::Ohlcv => match dataset {
                Dataset::Futures => FUTURES_OHLCV_QUERY,
                Dataset::Equities => EQUITIES_OHLCV_QUERY,
                Dataset::Option => OPTION_OHLCV_QUERY,
            },
            QueryType::Bbo => match dataset {
                Dataset::Futures => FUTURES_BBO_QUERY,
                Dataset::Equities => EQUITIES_BBO_QUERY,
                Dataset::Option => OPTION_BBO_QUERY,
            },
        }
    }
}

#[derive(FromRow, Debug, Clone)]
pub struct RollingWindow {
    pub ticker: String,
    pub instrument_id: i32,
    pub continuous_ticker: String,
    pub start_time: i64,
    pub end_time: i64,
}

impl RollingWindow {
    pub async fn adjust_end_ts(start_ts: i64, end_ts: &mut i64) -> Result<()> {
        let interval_ns = 86_400_000_000_000;
        let start_day_end = start_ts + (interval_ns - (start_ts % interval_ns));

        if *end_ts > start_day_end {
            *end_ts = start_day_end;
        }
        Ok(())
    }

    pub async fn retrieve_continuous_window(
        pool: &PgPool,
        start_ts: i64,
        end_ts: i64,
        rank: i32,
        symbols: Vec<String>,
        continuous_kind: &ContinuousKind,
    ) -> Result<HashMap<String, Vec<RollingWindow>>> {
        let query = match continuous_kind {
            ContinuousKind::Volume => FUTURES_VOLUME_ROLLING_WINDOW,
            ContinuousKind::Calendar => FUTURES_CALENDAR_ROLLING_WINDOW,
            ContinuousKind::None => {
                return Err(Error::CustomError("Invalid continous type".to_string()))
            }
        };

        // Create a HashMap to store the results
        let mut result_map: HashMap<String, Vec<RollingWindow>> = HashMap::new();

        // Execute continuous query
        let objs: Vec<RollingWindow> = sqlx::query_as(query)
            .bind(start_ts)
            .bind(end_ts - 1)
            .bind(&symbols)
            .bind(rank)
            .fetch_all(pool)
            .await?;

        // Final result: Collapse inner HashMaps into Vec<RollingWindow>
        for obj in objs {
            // Use the continuous_ticker as the key
            result_map
                .entry(obj.continuous_ticker.clone())
                .or_insert_with(Vec::new)
                .push(obj);
        }

        // Sort each Vec<RollingWindow> by start_ts
        for vec in result_map.values_mut() {
            vec.sort_by_key(|w| std::cmp::Reverse(w.start_time));
        }

        Ok(result_map)
    }
}

#[async_trait]
pub trait RecordsQuery {
    async fn retrieve_query(
        pool: &PgPool,
        params: &RetrieveParams,
        continuous_flag: bool,
        continuous_suffix: String,
    ) -> Result<
        Pin<Box<dyn Stream<Item = std::result::Result<sqlx::postgres::PgRow, sqlx::Error>> + Send>>,
    >;
}

#[async_trait]
impl RecordsQuery for Mbp1Msg {
    async fn retrieve_query(
        pool: &PgPool,
        params: &RetrieveParams,
        continuous_flag: bool,
        continuous_suffix: String,
    ) -> Result<
        Pin<Box<dyn Stream<Item = std::result::Result<sqlx::postgres::PgRow, sqlx::Error>> + Send>>,
    > {
        // Parameters
        let tbbo_flag = params.schema == Schema::Tbbo;

        // Execute continuous query
        let cursor = sqlx::query(QueryType::Mbp.get_query(params.dataset))
            .bind(params.start_ts)
            .bind(params.end_ts - 1)
            .bind(params.symbols.clone())
            .bind(tbbo_flag)
            .bind(continuous_flag)
            .bind(continuous_suffix)
            .fetch(pool);

        Ok(cursor)
    }
}

#[async_trait]
impl RecordsQuery for TradeMsg {
    async fn retrieve_query(
        pool: &PgPool,
        params: &RetrieveParams,
        continuous_flag: bool,
        continuous_suffix: String,
    ) -> Result<
        Pin<Box<dyn Stream<Item = std::result::Result<sqlx::postgres::PgRow, sqlx::Error>> + Send>>,
    > {
        // Parameters

        // Execute the query with parameters, including LIMIT and OFFSET
        let cursor = sqlx::query(QueryType::Trade.get_query(params.dataset))
            .bind(params.start_ts)
            .bind(params.end_ts - 1)
            .bind(params.symbols.clone())
            .bind(continuous_flag)
            .bind(continuous_suffix)
            .fetch(pool);

        Ok(cursor)
    }
}

#[async_trait]
impl RecordsQuery for BboMsg {
    async fn retrieve_query(
        pool: &PgPool,
        params: &RetrieveParams,
        continuous_flag: bool,
        continuous_suffix: String,
    ) -> Result<
        Pin<Box<dyn Stream<Item = std::result::Result<sqlx::postgres::PgRow, sqlx::Error>> + Send>>,
    > {
        // Parameters
        let interval_ns = params.schema_interval()?;

        // Construct the SQL query with a join and additional filtering by symbols
        let cursor = sqlx::query(QueryType::Bbo.get_query(params.dataset))
            .bind(params.start_ts)
            .bind(params.end_ts)
            .bind(interval_ns)
            .bind(params.symbols.clone())
            .bind(continuous_flag)
            .bind(continuous_suffix)
            .fetch(pool);

        Ok(cursor)
    }
}

#[async_trait]
impl RecordsQuery for OhlcvMsg {
    async fn retrieve_query(
        pool: &PgPool,
        params: &RetrieveParams,
        continuous_flag: bool,
        continuous_suffix: String,
    ) -> Result<
        Pin<Box<dyn Stream<Item = std::result::Result<sqlx::postgres::PgRow, sqlx::Error>> + Send>>,
    > {
        // Parameters
        let interval_ns = params.schema_interval()?;

        let cursor = sqlx::query(QueryType::Ohlcv.get_query(params.dataset))
            .bind(params.start_ts)
            .bind(params.end_ts)
            .bind(interval_ns)
            .bind(params.symbols.clone())
            .bind(continuous_flag)
            .bind(continuous_suffix)
            .fetch(pool);

        Ok(cursor)
    }
}
#[async_trait]
impl RecordsQuery for RecordEnum {
    async fn retrieve_query(
        pool: &PgPool,
        params: &RetrieveParams,
        continuous_flag: bool,
        continuous_suffix: String,
    ) -> Result<
        Pin<Box<dyn Stream<Item = std::result::Result<sqlx::postgres::PgRow, sqlx::Error>> + Send>>,
    > {
        match RType::from(params.rtype().unwrap()) {
            RType::Mbp1 => {
                Ok(
                    Mbp1Msg::retrieve_query(pool, params, continuous_flag, continuous_suffix)
                        .await?,
                )
            }
            RType::Trades => {
                Ok(
                    TradeMsg::retrieve_query(pool, params, continuous_flag, continuous_suffix)
                        .await?,
                )
            }
            RType::Ohlcv => {
                Ok(
                    OhlcvMsg::retrieve_query(pool, params, continuous_flag, continuous_suffix)
                        .await?,
                )
            }
            RType::Bbo => {
                Ok(
                    BboMsg::retrieve_query(pool, params, continuous_flag, continuous_suffix)
                        .await?,
                )
            }
            RType::Tbbo => {
                Ok(
                    TbboMsg::retrieve_query(pool, params, continuous_flag, continuous_suffix)
                        .await?,
                )
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::historical::database::create::RecordInsertQueries;
    use crate::historical::database::read::rows::FromRow;
    use crate::pool::DatabaseState;
    use dbn;
    use futures::stream::StreamExt;
    use mbinary::enums::{Action, Dataset, Side, Stype};
    use mbinary::records::{BidAskPair, RecordHeader};
    use mbinary::symbols::Instrument;
    use mbinary::vendors::Vendors;
    use mbinary::vendors::{DatabentoData, VendorData};
    use serial_test::serial;
    use sqlx::{postgres::PgPoolOptions, Postgres, Transaction};
    use std::ops::Deref;
    use std::os::raw::c_char;
    use std::str::FromStr;
    use std::sync::Arc;
    use tracing::error;

    async fn create_db_state() -> anyhow::Result<Arc<DatabaseState>> {
        let historical_db_url = std::env::var("HISTORICAL_DATABASE_URL")?;
        let trading_db_url = std::env::var("TRADING_DATABASE_URL")?;

        let state = DatabaseState::new(&historical_db_url, &trading_db_url).await?;
        Ok(Arc::new(state))
    }
    // -- Helper functions
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
            INSERT INTO {} (instrument_id, ticker, name, vendor,vendor_data, last_available, first_available, expiration_date, active)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
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
            .bind(instrument.expiration_date as i64)
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

    // -- Helper functions
    async fn create_futures() -> anyhow::Result<Vec<i32>> {
        //  Vendor data
        let schema = dbn::Schema::from_str("mbp-1")?;
        let dbn_dataset = dbn::Dataset::from_str("GLBX.MDP3")?;
        let stype = dbn::SType::from_str("raw_symbol")?;
        let vendor_data = VendorData::Databento(DatabentoData {
            schema,
            dataset: dbn_dataset,
            stype,
        });

        // Create instrument
        let dataset = Dataset::Futures;
        let mut instruments = Vec::new();

        // LEG4
        instruments.push(Instrument::new(
            None,
            "HEG4",
            "LeanHogs-0224",
            dataset,
            Vendors::Databento,
            vendor_data.encode(),
            1707937194000000000,
            1704067200000000000,
            1707937194000000000,
            true,
        ));

        instruments.push(Instrument::new(
            None,
            "HEJ4",
            "LeanHogs-0424",
            dataset,
            Vendors::Databento,
            vendor_data.encode(),
            1712941200000000000,
            1704067200000000000,
            1712941200000000000,
            true,
        ));

        instruments.push(Instrument::new(
            None,
            "LEG4",
            "LeanHogs-0224",
            dataset,
            Vendors::Databento,
            vendor_data.encode(),
            1707937194000000000,
            1704067200000000000,
            1707937194000000000,
            true,
        ));

        instruments.push(Instrument::new(
            None,
            "LEJ4",
            "LeanHogs-0424",
            dataset,
            Vendors::Databento,
            vendor_data.encode(),
            1712941200000000000,
            1704067200000000000,
            1713326400000000000,
            true,
        ));

        let mut ids = Vec::new();
        for i in instruments {
            let id = create_instrument(i).await?;
            ids.push(id);
        }

        Ok(ids)
    }

    async fn create_equities() -> anyhow::Result<Vec<i32>> {
        //  Vendor data
        let dataset = Dataset::Equities;
        let schema = dbn::Schema::from_str("mbp-1")?;
        let dbn_dataset = dbn::Dataset::from_str("XNAS.ITCH")?;
        let stype = dbn::SType::from_str("raw_symbol")?;
        let vendor_data = VendorData::Databento(DatabentoData {
            schema,
            dataset: dbn_dataset,
            stype,
        });

        // Create instrument
        let instruments = vec![
            Instrument::new(
                None,
                "AAPL",
                "aaple",
                dataset.clone(),
                Vendors::Databento,
                vendor_data.encode(),
                1704672000000000000,
                1704672000000000000,
                0,
                true,
            ),
            Instrument::new(
                None,
                "TSLA",
                "tesla",
                dataset.clone(),
                Vendors::Databento,
                vendor_data.encode(),
                1704672000000000000,
                1704672000000000000,
                0,
                true,
            ),
        ];

        let mut ids = Vec::new();
        for i in instruments {
            let id = create_instrument(i).await?;
            ids.push(id);
        }

        Ok(ids)
    }

    async fn create_options() -> anyhow::Result<Vec<i32>> {
        //  Vendor data
        let dataset = Dataset::Option;
        let schema = dbn::Schema::from_str("mbp-1")?;
        let dbn_dataset = dbn::Dataset::from_str("OPRA.PILLAR")?;
        let stype = dbn::SType::from_str("raw_symbol")?;
        let vendor_data = VendorData::Databento(DatabentoData {
            schema,
            dataset: dbn_dataset,
            stype,
        });

        let instruments = vec![Instrument::new(
            None,
            "APPLP12345",
            "Apple Put",
            dataset.clone(),
            Vendors::Databento,
            vendor_data.encode(),
            1704672000000000000,
            1704672000000000000,
            0,
            true,
        )];

        let mut ids = Vec::new();

        for i in instruments {
            let id = create_instrument(i).await?;
            ids.push(id);
        }

        Ok(ids)
    }

    async fn create_option_records(ids: &Vec<i32>) -> anyhow::Result<Vec<Mbp1Msg>> {
        let records = vec![
            Mbp1Msg {
                hd: { RecordHeader::new::<Mbp1Msg>(ids[0] as u32, 1704209103644092564, 0) },
                price: 6770,
                size: 1,
                action: Action::Trade as c_char,
                side: Side::Bid as c_char,
                depth: 0,
                flags: 0,
                ts_recv: 1704209103644092564,
                ts_in_delta: 17493,
                sequence: 739763,
                discriminator: 0,
                levels: [BidAskPair {
                    bid_px: 1,
                    ask_px: 1,
                    bid_sz: 1,
                    ask_sz: 1,
                    bid_ct: 10,
                    ask_ct: 20,
                }],
            },
            Mbp1Msg {
                hd: { RecordHeader::new::<Mbp1Msg>(ids[0] as u32, 1704209103644092565, 0) },
                price: 6870,
                size: 2,
                action: Action::Add as c_char,
                side: Side::Bid as c_char,
                depth: 0,
                flags: 0,
                ts_recv: 1704209103644092565,
                ts_in_delta: 17493,
                sequence: 739763,
                discriminator: 0,
                levels: [BidAskPair {
                    bid_px: 1,
                    ask_px: 1,
                    bid_sz: 1,
                    ask_sz: 1,
                    bid_ct: 10,
                    ask_ct: 20,
                }],
            },
        ];

        // Create Records
        dotenv::dotenv().ok();
        let dataset = Dataset::Option;
        let state = create_db_state().await?;

        let mut transaction = state
            .historical_pool
            .begin()
            .await
            .expect("Error setting up test transaction.");

        let _ = insert_records(&mut transaction, records.clone(), dataset.clone())
            .await
            .expect("Error inserting records.");
        let _ = transaction.commit().await;

        Ok(records)
    }
    async fn create_equity_records(ids: &Vec<i32>) -> anyhow::Result<Vec<Mbp1Msg>> {
        let records = vec![
            Mbp1Msg {
                hd: { RecordHeader::new::<Mbp1Msg>(ids[0] as u32, 1704209103644092564, 0) },
                price: 6770,
                size: 1,
                action: Action::Trade as c_char,
                side: 2,
                depth: 0,
                flags: 10,
                ts_recv: 1704209103644092564,
                ts_in_delta: 17493,
                sequence: 739763,
                discriminator: 0,
                levels: [BidAskPair {
                    bid_px: 1,
                    ask_px: 1,
                    bid_sz: 1,
                    ask_sz: 1,
                    bid_ct: 10,
                    ask_ct: 20,
                }],
            },
            Mbp1Msg {
                hd: { RecordHeader::new::<Mbp1Msg>(ids[1] as u32, 1704209103644092565, 0) },
                price: 6870,
                size: 2,
                action: Action::Trade as c_char,
                side: 1,
                depth: 0,
                flags: 0,
                ts_recv: 1704209103644092565,
                ts_in_delta: 17493,
                sequence: 739763,
                discriminator: 0,
                levels: [BidAskPair {
                    bid_px: 1,
                    ask_px: 1,
                    bid_sz: 1,
                    ask_sz: 1,
                    bid_ct: 10,
                    ask_ct: 20,
                }],
            },
        ];

        // Create Records
        dotenv::dotenv().ok();
        let dataset = Dataset::Equities;
        let state = create_db_state().await?;

        // let pool = init_db().await.unwrap();

        let mut transaction = state
            .historical_pool
            .begin()
            .await
            .expect("Error setting up test transaction.");

        let _ = insert_records(&mut transaction, records.clone(), dataset.clone())
            .await
            .expect("Error inserting records.");
        let _ = transaction.commit().await;

        Ok(records)
    }

    async fn create_future_records(ids: &Vec<i32>) -> anyhow::Result<Vec<Mbp1Msg>> {
        let records = vec![
            Mbp1Msg {
                hd: { RecordHeader::new::<Mbp1Msg>(ids[0] as u32, 1704209103644092562, 0) },
                price: 500,
                size: 1,
                action: Action::Trade as c_char,
                side: Side::Bid as c_char,
                depth: 0,
                flags: 0,
                ts_recv: 1704209103644092562,
                ts_in_delta: 17493,
                sequence: 739763,
                discriminator: 0,
                levels: [BidAskPair {
                    bid_px: 1,
                    ask_px: 1,
                    bid_sz: 1,
                    ask_sz: 1,
                    bid_ct: 10,
                    ask_ct: 20,
                }],
            },
            Mbp1Msg {
                hd: { RecordHeader::new::<Mbp1Msg>(ids[0] as u32, 1704240000000000001, 0) },
                price: 500,
                size: 1,
                action: Action::Trade as c_char,
                side: Side::Bid as c_char,
                depth: 0,
                flags: 0,
                ts_recv: 1704240000000000001,
                ts_in_delta: 17493,
                sequence: 739763,
                discriminator: 0,
                levels: [BidAskPair {
                    bid_px: 1,
                    ask_px: 1,
                    bid_sz: 1,
                    ask_sz: 1,
                    bid_ct: 10,
                    ask_ct: 20,
                }],
            },
            Mbp1Msg {
                hd: { RecordHeader::new::<Mbp1Msg>(ids[1] as u32, 1707117590000000000, 0) },
                price: 6770,
                size: 1,
                action: Action::Trade as c_char,
                side: 2,
                depth: 0,
                flags: 0,
                ts_recv: 1707117590000000000,
                ts_in_delta: 17493,
                sequence: 739763,
                discriminator: 0,

                levels: [BidAskPair {
                    bid_px: 1,
                    ask_px: 1,
                    bid_sz: 1,
                    ask_sz: 1,
                    bid_ct: 10,
                    ask_ct: 20,
                }],
            },
            Mbp1Msg {
                hd: { RecordHeader::new::<Mbp1Msg>(ids[2] as u32, 1704295503644092562, 0) },
                price: 6870,
                size: 2,
                action: Action::Trade as c_char,
                side: 2,
                depth: 0,
                flags: 0,
                ts_recv: 1704295503644092562,
                ts_in_delta: 17493,
                sequence: 739763,
                discriminator: 0,

                levels: [BidAskPair {
                    bid_px: 1,
                    ask_px: 1,
                    bid_sz: 1,
                    ask_sz: 1,
                    bid_ct: 10,
                    ask_ct: 20,
                }],
            },
            Mbp1Msg {
                hd: { RecordHeader::new::<Mbp1Msg>(ids[3] as u32, 1707117590000000000, 0) },
                price: 6870,
                size: 2,
                action: Action::Trade as c_char,
                side: 2,
                depth: 0,
                flags: 0,
                ts_recv: 1707117590000000000,
                ts_in_delta: 17493,
                sequence: 739763,
                discriminator: 0,

                levels: [BidAskPair {
                    bid_px: 1,
                    ask_px: 1,
                    bid_sz: 1,
                    ask_sz: 1,
                    bid_ct: 10,
                    ask_ct: 20,
                }],
            },
        ];

        // Create Records
        dotenv::dotenv().ok();
        let dataset = Dataset::Futures;
        let state = create_db_state().await?;

        let mut transaction = state
            .historical_pool
            .begin()
            .await
            .expect("Error setting up test transaction.");

        let _ = insert_records(&mut transaction, records.clone(), dataset.clone())
            .await
            .expect("Error inserting records.");
        let _ = transaction.commit().await;

        let query = "REFRESH MATERIALIZED VIEW futures_continuous_calendar_windows";
        sqlx::query(query)
            .execute(state.historical_pool.deref())
            .await?;

        let query = "REFRESH MATERIALIZED VIEW futures_continuous_volume_windows";
        sqlx::query(query)
            .execute(state.historical_pool.deref())
            .await?;

        Ok(records)
    }

    async fn insert_records(
        tx: &mut Transaction<'_, Postgres>,
        records: Vec<Mbp1Msg>,
        dataset: Dataset,
    ) -> Result<()> {
        for record in records {
            record.insert_query(tx, &dataset).await?;
        }
        Ok(())
    }

    // -- Tests
    #[sqlx::test]
    #[serial]
    // #[ignore]
    async fn test_retrieve_mbp1() -> anyhow::Result<()> {
        dotenv::dotenv().ok();
        let state = create_db_state().await?;
        let ids = create_equities().await?;
        let _records = create_equity_records(&ids).await?;

        // Test
        let query_params = RetrieveParams {
            symbols: vec!["AAPL".to_string()],
            start_ts: 1704209103644092563,
            end_ts: 1704209903644092567,
            schema: Schema::Mbp1,
            dataset: Dataset::Equities,
            stype: Stype::Raw,
        };

        let mut cursor = Mbp1Msg::retrieve_query(
            state.historical_pool.deref(),
            &query_params,
            false,
            "".to_string(),
        )
        .await
        .expect("Error on retrieve records.");

        // Validate
        let mut query: Vec<RecordEnum> = vec![];
        while let Some(row_result) = cursor.next().await {
            match row_result {
                Ok(row) => {
                    let record = Mbp1Msg::from_row(&row, None, None)?;

                    // Convert to RecordEnum and add to encoder
                    let record_enum = RecordEnum::Mbp1(record);
                    query.push(record_enum);
                }
                Err(e) => {
                    error!("Error processing row: {:?}", e);
                    return Err(e.into());
                }
            }
        }

        assert_eq!(1, query.len());

        // Cleanup
        for id in ids {
            delete_instrument(id).await.expect("Error on delete");
        }

        Ok(())
    }

    #[sqlx::test]
    #[serial]
    // #[ignore]
    async fn test_retrieve_tbbo() -> anyhow::Result<()> {
        dotenv::dotenv().ok();
        let state = create_db_state().await?;
        let ids = create_equities().await?;
        let _records = create_equity_records(&ids).await?;

        // Test
        let query_params = RetrieveParams {
            symbols: vec!["AAPL".to_string()],
            start_ts: 1704209103644092563,
            end_ts: 1704209903644092567,
            schema: Schema::Tbbo,
            dataset: Dataset::Equities,
            stype: Stype::Raw,
        };

        let mut cursor = TbboMsg::retrieve_query(
            state.historical_pool.deref(),
            &query_params,
            false,
            "".to_string(),
        )
        .await
        .expect("Error on retrieve records.");

        // Validate
        let mut query: Vec<RecordEnum> = vec![];
        while let Some(row_result) = cursor.next().await {
            match row_result {
                Ok(row) => {
                    let record = TbboMsg::from_row(&row, None, None)?;

                    // Convert to RecordEnum and add to encoder
                    let record_enum = RecordEnum::Tbbo(record);
                    query.push(record_enum);
                }
                Err(e) => {
                    error!("Error processing row: {:?}", e);
                    return Err(e.into());
                }
            }
        }

        assert_eq!(query.len(), 1);

        // Cleanup
        for id in ids {
            delete_instrument(id).await.expect("Error on delete");
        }

        Ok(())
    }

    #[sqlx::test]
    #[serial]
    // #[ignore]
    async fn test_retrieve_trade() -> anyhow::Result<()> {
        dotenv::dotenv().ok();
        let state = create_db_state().await?;
        let ids = create_equities().await?;
        let _records = create_equity_records(&ids).await?;

        // Test
        let query_params = RetrieveParams {
            symbols: vec!["AAPL".to_string()],
            start_ts: 1704209103644092563,
            end_ts: 1704209903644092567,
            schema: Schema::Trades,
            dataset: Dataset::Equities,
            stype: Stype::Raw,
        };

        let mut cursor =
            TradeMsg::retrieve_query(&state.historical_pool, &query_params, false, "".to_string())
                .await
                .expect("Error on retrieve records.");

        // Validate
        let mut query: Vec<RecordEnum> = vec![];
        while let Some(row_result) = cursor.next().await {
            match row_result {
                Ok(row) => {
                    let record = TradeMsg::from_row(&row, None, None)?;

                    // Convert to RecordEnum and add to encoder
                    let record_enum = RecordEnum::Trade(record);
                    query.push(record_enum);
                }
                Err(e) => {
                    error!("Error processing row: {:?}", e);
                    return Err(e.into());
                }
            }
        }

        assert!(query.len() == 1);

        // Cleanup
        for id in ids {
            delete_instrument(id).await.expect("Error on delete");
        }

        Ok(())
    }

    #[sqlx::test]
    #[serial]
    // #[ignore]
    async fn test_retrieve_bbo() -> anyhow::Result<()> {
        dotenv::dotenv().ok();
        let state = create_db_state().await?;
        let ids = create_equities().await?;
        let _records = create_equity_records(&ids).await?;

        // Test
        let query_params = RetrieveParams {
            symbols: vec!["AAPL".to_string()],
            start_ts: 1704209103644092563,
            end_ts: 1704209903644092567,
            schema: Schema::Bbo1S,
            dataset: Dataset::Equities,
            stype: Stype::Raw,
        };

        let mut cursor =
            BboMsg::retrieve_query(&state.historical_pool, &query_params, false, "".to_string())
                .await
                .expect("Error on retrieve records.");

        // Validate
        let mut query: Vec<RecordEnum> = vec![];
        while let Some(row_result) = cursor.next().await {
            match row_result {
                Ok(row) => {
                    let record = BboMsg::from_row(&row, None, None)?;

                    // Convert to RecordEnum and add to encoder
                    let record_enum = RecordEnum::Bbo(record);
                    query.push(record_enum);
                }
                Err(e) => {
                    error!("Error processing row: {:?}", e);
                    return Err(e.into());
                }
            }
        }

        assert!(query.len() == 1);

        // Cleanup
        for id in ids {
            delete_instrument(id).await.expect("Error on delete");
        }

        Ok(())
    }

    #[sqlx::test]
    #[serial]
    // #[ignore]
    async fn test_retrieve_ohlcv() -> anyhow::Result<()> {
        dotenv::dotenv().ok();
        // let pool = init_db().await.unwrap();
        let state = create_db_state().await?;

        let ids = create_equities().await?;
        let _records = create_equity_records(&ids).await?;

        // Test
        let query_params = RetrieveParams {
            symbols: vec!["AAPL".to_string()],
            start_ts: 1704209103644092562,
            end_ts: 1704295503654092563,
            schema: Schema::Ohlcv1D,
            dataset: Dataset::Equities,
            stype: Stype::Raw,
        };

        let mut cursor =
            OhlcvMsg::retrieve_query(&state.historical_pool, &query_params, false, "".to_string())
                .await
                .expect("Error on retrieve records.");

        // Validate
        let mut query: Vec<RecordEnum> = vec![];
        while let Some(row_result) = cursor.next().await {
            match row_result {
                Ok(row) => {
                    let record = OhlcvMsg::from_row(&row, None, None)?;

                    // Convert to RecordEnum and add to encoder
                    let record_enum = RecordEnum::Ohlcv(record);
                    query.push(record_enum);
                }
                Err(e) => {
                    error!("Error processing row: {:?}", e);
                    return Err(e.into());
                }
            }
        }

        // Validate
        assert_eq!(1, query.len());

        // Cleanup
        for id in ids {
            delete_instrument(id).await.expect("Error on delete");
        }

        assert!(query.len() > 0);

        Ok(())
    }

    // -- Futures
    #[sqlx::test]
    #[serial]
    // #[ignore]
    async fn test_retrieve_mbp1_futures() -> anyhow::Result<()> {
        dotenv::dotenv().ok();
        let state = create_db_state().await?;

        // let pool = init_db().await.unwrap();
        let ids = create_futures().await?;
        let _records = create_future_records(&ids).await?;

        // Test
        let query_params = RetrieveParams {
            symbols: vec!["HEG4".to_string()],
            start_ts: 1704171600000000000,
            end_ts: 1704225600000000000,
            schema: Schema::Mbp1,
            dataset: Dataset::Futures,
            stype: Stype::Raw,
        };

        let mut cursor =
            Mbp1Msg::retrieve_query(&state.historical_pool, &query_params, false, "".to_string())
                .await
                .expect("Error on retrieve records.");

        // Validate
        let mut query: Vec<RecordEnum> = vec![];
        while let Some(row_result) = cursor.next().await {
            match row_result {
                Ok(row) => {
                    let record = Mbp1Msg::from_row(&row, None, None)?;

                    // Convert to RecordEnum and add to encoder
                    let record_enum = RecordEnum::Mbp1(record);
                    query.push(record_enum);
                }
                Err(e) => {
                    error!("Error processing row: {:?}", e);
                    return Err(e.into());
                }
            }
        }
        assert_eq!(query.len(), 1);

        // Cleanup
        for id in ids {
            delete_instrument(id).await.expect("Error on delete");
        }

        Ok(())
    }

    #[sqlx::test]
    #[serial]
    // #[ignore]
    async fn test_retrieve_tbbo_futures() -> anyhow::Result<()> {
        dotenv::dotenv().ok();
        let state = create_db_state().await?;
        let ids = create_futures().await?;
        let _records = create_future_records(&ids).await?;

        // Test
        let query_params = RetrieveParams {
            symbols: vec!["HEG4".to_string()],
            start_ts: 1704171600000000000,
            end_ts: 1704225600000000000,
            schema: Schema::Tbbo,
            dataset: Dataset::Futures,
            stype: Stype::Raw,
        };

        let mut cursor =
            TbboMsg::retrieve_query(&state.historical_pool, &query_params, false, "".to_string())
                .await
                .expect("Error on retrieve records.");

        // Validate
        let mut query: Vec<RecordEnum> = vec![];
        while let Some(row_result) = cursor.next().await {
            match row_result {
                Ok(row) => {
                    let record = TbboMsg::from_row(&row, None, None)?;

                    // Convert to RecordEnum and add to encoder
                    let record_enum = RecordEnum::Tbbo(record);
                    query.push(record_enum);
                }
                Err(e) => {
                    error!("Error processing row: {:?}", e);
                    return Err(e.into());
                }
            }
        }
        assert_eq!(query.len(), 1);

        // Cleanup
        for id in ids {
            delete_instrument(id).await.expect("Error on delete");
        }

        Ok(())
    }

    #[sqlx::test]
    #[serial]
    // #[ignore]
    async fn test_retrieve_trade_futures() -> anyhow::Result<()> {
        dotenv::dotenv().ok();
        let state = create_db_state().await?;
        let ids = create_futures().await?;
        let _records = create_future_records(&ids).await?;

        // Test
        let query_params = RetrieveParams {
            symbols: vec!["HEG4".to_string()],
            start_ts: 1704171600000000000,
            end_ts: 1704225600000000000,
            schema: Schema::Trades,
            dataset: Dataset::Futures,
            stype: Stype::Raw,
        };

        let mut cursor =
            TradeMsg::retrieve_query(&state.historical_pool, &query_params, false, "".to_string())
                .await
                .expect("Error on retrieve records.");

        // Validate
        let mut query: Vec<RecordEnum> = vec![];
        while let Some(row_result) = cursor.next().await {
            match row_result {
                Ok(row) => {
                    let record = TradeMsg::from_row(&row, None, None)?;

                    // Convert to RecordEnum and add to encoder
                    let record_enum = RecordEnum::Trade(record);
                    query.push(record_enum);
                }
                Err(e) => {
                    error!("Error processing row: {:?}", e);
                    return Err(e.into());
                }
            }
        }
        assert!(query.len() == 1);

        // Cleanup
        for id in ids {
            delete_instrument(id).await.expect("Error on delete");
        }

        Ok(())
    }

    #[sqlx::test]
    #[serial]
    // #[ignore]
    async fn test_retrieve_bbo_futures() -> anyhow::Result<()> {
        dotenv::dotenv().ok();
        let state = create_db_state().await?;
        let ids = create_futures().await?;
        let _records = create_future_records(&ids).await?;

        // Test
        let query_params = RetrieveParams {
            symbols: vec!["HEG4".to_string()],
            start_ts: 1704171600000000000,
            end_ts: 1704225600000000000,
            schema: Schema::Bbo1S,
            dataset: Dataset::Futures,
            stype: Stype::Raw,
        };

        let mut cursor =
            BboMsg::retrieve_query(&state.historical_pool, &query_params, false, "".to_string())
                .await
                .expect("Error on retrieve records.");

        // Validate
        let mut query: Vec<RecordEnum> = vec![];
        while let Some(row_result) = cursor.next().await {
            match row_result {
                Ok(row) => {
                    let record = BboMsg::from_row(&row, None, None)?;

                    // Convert to RecordEnum and add to encoder
                    let record_enum = RecordEnum::Bbo(record);
                    query.push(record_enum);
                }
                Err(e) => {
                    error!("Error processing row: {:?}", e);
                    return Err(e.into());
                }
            }
        }
        assert!(query.len() > 0);

        // Cleanup
        for id in ids {
            delete_instrument(id).await.expect("Error on delete");
        }

        Ok(())
    }

    #[sqlx::test]
    #[serial]
    // #[ignore]
    async fn test_retrieve_ohlcv_futures() -> anyhow::Result<()> {
        dotenv::dotenv().ok();
        let state = create_db_state().await?;
        let ids = create_futures().await?;
        let _records = create_future_records(&ids).await?;

        // Test
        let query_params = RetrieveParams {
            symbols: vec!["HEG4".to_string()],
            start_ts: 1704171600000000000,
            end_ts: 1704225600000000000,
            schema: Schema::Ohlcv1D,
            dataset: Dataset::Futures,
            stype: Stype::Raw,
        };

        let mut cursor =
            OhlcvMsg::retrieve_query(&state.historical_pool, &query_params, false, "".to_string())
                .await
                .expect("Error on retrieve records.");

        // Validate
        let mut query: Vec<RecordEnum> = vec![];
        while let Some(row_result) = cursor.next().await {
            match row_result {
                Ok(row) => {
                    let record = OhlcvMsg::from_row(&row, None, None)?;

                    // Convert to RecordEnum and add to encoder
                    let record_enum = RecordEnum::Ohlcv(record);
                    query.push(record_enum);
                }
                Err(e) => {
                    error!("Error processing row: {:?}", e);
                    return Err(e.into());
                }
            }
        }
        // Validate
        assert!(query.len() > 0);

        // Cleanup
        for id in ids {
            delete_instrument(id).await.expect("Error on delete");
        }

        Ok(())
    }

    // -- Option
    #[sqlx::test]
    #[serial]
    // #[ignore]
    async fn test_retrieve_mbp1_option() -> anyhow::Result<()> {
        dotenv::dotenv().ok();
        let state = create_db_state().await?;
        let ids = create_options().await?;
        let _records = create_option_records(&ids).await?;

        // Test
        let query_params = RetrieveParams {
            symbols: vec!["APPLP12345".to_string()],
            start_ts: 1704209103644092563,
            end_ts: 1704209903644092567,
            schema: Schema::Mbp1,
            dataset: Dataset::Option,
            stype: Stype::Raw,
        };

        let mut cursor =
            Mbp1Msg::retrieve_query(&state.historical_pool, &query_params, false, "".to_string())
                .await
                .expect("Error on retrieve records.");

        // Validate
        let mut query: Vec<RecordEnum> = vec![];
        while let Some(row_result) = cursor.next().await {
            match row_result {
                Ok(row) => {
                    let record = Mbp1Msg::from_row(&row, None, None)?;

                    // Convert to RecordEnum and add to encoder
                    let record_enum = RecordEnum::Mbp1(record);
                    query.push(record_enum);
                }
                Err(e) => {
                    error!("Error processing row: {:?}", e);
                    return Err(e.into());
                }
            }
        }

        assert_eq!(2, query.len());

        // Cleanup
        for id in ids {
            delete_instrument(id).await.expect("Error on delete");
        }

        Ok(())
    }

    #[sqlx::test]
    #[serial]
    // #[ignore]
    async fn test_retrieve_tbbo_option() -> anyhow::Result<()> {
        dotenv::dotenv().ok();
        let state = create_db_state().await?;
        let ids = create_options().await?;
        let _records = create_option_records(&ids).await?;

        // Test
        let query_params = RetrieveParams {
            symbols: vec!["APPLP12345".to_string()],
            start_ts: 1704209103644092563,
            end_ts: 1704209903644092567,
            schema: Schema::Tbbo,
            dataset: Dataset::Option,
            stype: Stype::Raw,
        };

        let mut cursor =
            TbboMsg::retrieve_query(&state.historical_pool, &query_params, false, "".to_string())
                .await
                .expect("Error on retrieve records.");

        // Validate
        let mut query: Vec<RecordEnum> = vec![];
        while let Some(row_result) = cursor.next().await {
            match row_result {
                Ok(row) => {
                    let record = TbboMsg::from_row(&row, None, None)?;

                    // Convert to RecordEnum and add to encoder
                    let record_enum = RecordEnum::Tbbo(record);
                    query.push(record_enum);
                }
                Err(e) => {
                    error!("Error processing row: {:?}", e);
                    return Err(e.into());
                }
            }
        }
        assert_eq!(query.len(), 1);

        // Cleanup
        for id in ids {
            delete_instrument(id).await.expect("Error on delete");
        }

        Ok(())
    }

    #[sqlx::test]
    #[serial]
    // #[ignore]
    async fn test_retrieve_trade_option() -> anyhow::Result<()> {
        dotenv::dotenv().ok();
        let state = create_db_state().await?;
        let ids = create_options().await?;
        let _records = create_option_records(&ids).await?;

        // Test
        let query_params = RetrieveParams {
            symbols: vec!["APPLP12345".to_string()],
            start_ts: 1704209103644092563,
            end_ts: 1704209903644092567,
            schema: Schema::Trades,
            dataset: Dataset::Option,
            stype: Stype::Raw,
        };

        let mut cursor =
            TradeMsg::retrieve_query(&state.historical_pool, &query_params, false, "".to_string())
                .await
                .expect("Error on retrieve records.");

        // Validate
        let mut query: Vec<RecordEnum> = vec![];
        while let Some(row_result) = cursor.next().await {
            match row_result {
                Ok(row) => {
                    let record = TradeMsg::from_row(&row, None, None)?;

                    // Convert to RecordEnum and add to encoder
                    let record_enum = RecordEnum::Trade(record);
                    query.push(record_enum);
                }
                Err(e) => {
                    error!("Error processing row: {:?}", e);
                    return Err(e.into());
                }
            }
        }
        assert!(query.len() == 1);

        // Cleanup
        for id in ids {
            delete_instrument(id).await.expect("Error on delete");
        }

        Ok(())
    }

    #[sqlx::test]
    #[serial]
    // #[ignore]
    async fn test_retrieve_bbo_option() -> anyhow::Result<()> {
        dotenv::dotenv().ok();
        let state = create_db_state().await?;
        let ids = create_options().await?;
        let _records = create_option_records(&ids).await?;

        // Test
        let query_params = RetrieveParams {
            symbols: vec!["APPLP12345".to_string()],
            start_ts: 1704209103644092563,
            end_ts: 1704209903644092567,
            schema: Schema::Bbo1S,
            dataset: Dataset::Option,
            stype: Stype::Raw,
        };

        let mut cursor =
            BboMsg::retrieve_query(&state.historical_pool, &query_params, false, "".to_string())
                .await
                .expect("Error on retrieve records.");

        // Validate
        let mut query: Vec<RecordEnum> = vec![];
        while let Some(row_result) = cursor.next().await {
            match row_result {
                Ok(row) => {
                    let record = BboMsg::from_row(&row, None, None)?;

                    // Convert to RecordEnum and add to encoder
                    let record_enum = RecordEnum::Bbo(record);
                    query.push(record_enum);
                }
                Err(e) => {
                    error!("Error processing row: {:?}", e);
                    return Err(e.into());
                }
            }
        }
        assert!(query.len() > 0);

        // Cleanup
        for id in ids {
            delete_instrument(id).await.expect("Error on delete");
        }

        Ok(())
    }

    #[sqlx::test]
    #[serial]
    // #[ignore]
    async fn test_retrieve_ohlcv_option() -> anyhow::Result<()> {
        dotenv::dotenv().ok();
        let state = create_db_state().await?;
        let ids = create_options().await?;
        let _records = create_option_records(&ids).await?;

        // Test
        let query_params = RetrieveParams {
            symbols: vec!["APPLP12345".to_string()],
            start_ts: 1704209103644092562,
            end_ts: 1704295503654092563,
            schema: Schema::Ohlcv1D,
            dataset: Dataset::Option,
            stype: Stype::Raw,
        };

        let mut cursor =
            OhlcvMsg::retrieve_query(&state.historical_pool, &query_params, false, "".to_string())
                .await
                .expect("Error on retrieve records.");

        // Validate
        let mut query: Vec<RecordEnum> = vec![];
        while let Some(row_result) = cursor.next().await {
            match row_result {
                Ok(row) => {
                    let record = OhlcvMsg::from_row(&row, None, None)?;

                    // Convert to RecordEnum and add to encoder
                    let record_enum = RecordEnum::Ohlcv(record);
                    query.push(record_enum);
                }
                Err(e) => {
                    error!("Error processing row: {:?}", e);
                    return Err(e.into());
                }
            }
        }

        // Validate
        assert!(query.len() > 0);

        // Cleanup
        for id in ids {
            delete_instrument(id).await.expect("Error on delete");
        }

        Ok(())
    }

    #[sqlx::test]
    #[serial]
    // #[ignore]
    async fn test_retrieve_calendar_rolling_window() -> anyhow::Result<()> {
        dotenv::dotenv().ok();
        let state = create_db_state().await?;
        let ids = create_futures().await?;
        let _records = create_future_records(&ids).await?;

        // Test
        let query_params = RetrieveParams {
            symbols: vec!["HE.c.0".to_string(), "LE.c.0".to_string()],
            start_ts: 1704209103644092562,
            end_ts: 1713117594000000000,
            schema: Schema::Mbp1,
            dataset: Dataset::Futures,
            stype: Stype::Continuous,
        };

        let vec = RollingWindow::retrieve_continuous_window(
            &state.historical_pool,
            query_params.start_ts,
            query_params.end_ts,
            0,
            vec!["HE%".to_string(), "LE%".to_string()],
            &ContinuousKind::Calendar,
        )
        .await
        .expect("Error on retrieve records.");

        //HE Validation
        let timestamp: i64 = 1707937194000000000;
        let nanos_per_day: i64 = 86_400_000_000_000;
        let start_of_day = (timestamp / nanos_per_day) * nanos_per_day;
        let end_of_day = start_of_day + nanos_per_day - 1;

        let first_hog = vec["HE.c.0"][1].clone();
        assert_eq!(first_hog.ticker, "HEG4");
        assert_eq!(first_hog.start_time, 1704209103644092562); // start of requested range
        assert_eq!(first_hog.end_time, end_of_day); //  eod on expiration date

        let second_hog = vec["HE.c.0"][0].clone();
        let beg_end_day = (1712941200000000000 / nanos_per_day) * nanos_per_day;
        let end_end_day = beg_end_day + nanos_per_day - 1;
        assert_eq!(second_hog.ticker, "HEJ4");
        assert_eq!(second_hog.start_time, end_of_day + 1); // start of requested range
        assert_eq!(second_hog.end_time, end_end_day); // end of requested b/c before expiration date

        // LE Validation
        let start_of_day = (1707937194000000000 / nanos_per_day) * nanos_per_day;
        let end_of_day = start_of_day + nanos_per_day - 1;

        let first_cattle = vec["LE.c.0"][1].clone();
        assert_eq!(first_cattle.ticker, "LEG4");
        assert_eq!(first_cattle.start_time, 1704209103644092562); // start range
        assert_eq!(first_cattle.end_time, end_of_day);

        let second_cattle = vec["LE.c.0"][0].clone();
        assert_eq!(second_cattle.ticker, "LEJ4");
        assert_eq!(second_cattle.start_time, end_of_day + 1); // start of day after exp
        assert_eq!(second_cattle.end_time, 1713117594000000000 - 1); // end of range

        // Cleanup
        for id in ids {
            delete_instrument(id).await.expect("Error on delete");
        }

        Ok(())
    }

    #[sqlx::test]
    #[serial]
    // #[ignore]
    async fn test_retrieve_volume_rolling_window() -> anyhow::Result<()> {
        dotenv::dotenv().ok();
        let state = create_db_state().await?;
        let ids = create_futures().await?;
        let _records = create_future_records(&ids).await?;

        // Test
        let query_params = RetrieveParams {
            symbols: vec!["HE.v.0".to_string(), "LE.v.0".to_string()],
            start_ts: 1704209103644092562,
            end_ts: 1713117594000000000,
            schema: Schema::Mbp1,
            dataset: Dataset::Futures,
            stype: Stype::Continuous,
        };

        let vec = RollingWindow::retrieve_continuous_window(
            &state.historical_pool,
            query_params.start_ts,
            query_params.end_ts,
            0,
            vec!["HE%".to_string(), "LE%".to_string()],
            &ContinuousKind::Volume,
        )
        .await
        .expect("Error on retrieve records.");

        //Validate (they are in reverse order for easy pop off)
        let nanos_per_day: i64 = 86_400_000_000_000;
        let first_hog = vec["HE.v.0"][1].clone();
        assert_eq!(first_hog.ticker, "HEG4");

        let start_of_day = (1704209103644092562 / nanos_per_day) * nanos_per_day;
        let end_of_day = start_of_day + nanos_per_day;
        assert_eq!(first_hog.start_time, end_of_day); // start of day after highest volume

        let start_of_day = (1707117590000000000 / nanos_per_day) * nanos_per_day;
        let end_of_day = start_of_day + nanos_per_day - 1;
        assert_eq!(first_hog.end_time, end_of_day); // end of day after passed as highest volume

        let second_hog = vec["HE.v.0"][0].clone();
        assert_eq!(second_hog.ticker, "HEJ4");
        assert_eq!(second_hog.start_time, end_of_day + 1);
        let start_of_day = (1712941200000000000 / nanos_per_day) * nanos_per_day;
        let end_of_day = start_of_day + nanos_per_day - 1;
        assert_eq!(second_hog.end_time, end_of_day); // end of day expires, since range end greater

        // LE validation
        let first_cattle = vec["LE.v.0"][1].clone();
        assert_eq!(first_cattle.ticker, "LEG4");

        let start_of_day = (1704295503644092562 / nanos_per_day) * nanos_per_day;
        let end_of_day = start_of_day + nanos_per_day;
        assert_eq!(first_cattle.start_time, end_of_day); // start of day after day with volume

        let start_of_day = (1707117590000000000 / nanos_per_day) * nanos_per_day;
        let end_of_day = start_of_day + nanos_per_day - 1;
        assert_eq!(first_cattle.end_time, end_of_day);

        let second_cattle = vec["LE.v.0"][0].clone();
        assert_eq!(second_cattle.ticker, "LEJ4");
        assert_eq!(second_cattle.start_time, end_of_day + 1);
        assert_eq!(second_cattle.end_time, 1713117594000000000 - 1); // end of range

        // Cleanup
        for id in ids {
            delete_instrument(id).await.expect("Error on delete");
        }

        Ok(())
    }
}
