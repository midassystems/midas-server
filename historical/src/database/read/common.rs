use super::{
    equities::{
        EQUITIES_BBO_QUERY, EQUITIES_MBP1_QUERY, EQUITIES_OHLCV_QUERY, EQUITIES_TRADE_QUERY,
    },
    futures::{
        FUTURES_BBO_CONTINUOUS_QUERY, FUTURES_BBO_CONTINUOUS_VOLUME_QUERY, FUTURES_BBO_QUERY,
        FUTURES_MBP1_CONTINUOUS_QUERY, FUTURES_MBP1_CONTINUOUS_VOLUME_QUERY, FUTURES_MBP1_QUERY,
        FUTURES_OHLCV_CONTINUOUS_QUERY, FUTURES_OHLCV_CONTINUOUS_VOLUME_QUERY, FUTURES_OHLCV_QUERY,
        FUTURES_TRADE_CONTINUOUS_QUERY, FUTURES_TRADE_CONTINUOUS_VOLUME_QUERY, FUTURES_TRADE_QUERY,
    },
    option::{OPTION_BBO_QUERY, OPTION_MBP1_QUERY, OPTION_OHLCV_QUERY, OPTION_TRADE_QUERY},
};
use crate::{services::retrieve::retriever::ContinuousKind, Result};
use async_trait::async_trait;
use futures::Stream;
use mbinary::enums::{Dataset, RType, Schema, Stype};
use mbinary::params::RetrieveParams;
use mbinary::record_enum::RecordEnum;
use mbinary::records::{BboMsg, Mbp1Msg, OhlcvMsg, TbboMsg, TradeMsg};
use sqlx::PgPool;
use std::pin::Pin;

pub enum QueryType {
    Mbp,
    Trade,
    Ohlcv,
    Bbo,
}

impl QueryType {
    fn get_query(
        self,
        dataset: Dataset,
        stype: Stype,
        continuous_kind: &ContinuousKind,
    ) -> &'static str {
        match self {
            QueryType::Mbp => match dataset {
                Dataset::Futures => match stype {
                    Stype::Continuous => match continuous_kind {
                        ContinuousKind::Calendar => FUTURES_MBP1_CONTINUOUS_QUERY,
                        ContinuousKind::Volume => FUTURES_MBP1_CONTINUOUS_VOLUME_QUERY,
                        ContinuousKind::None => FUTURES_MBP1_CONTINUOUS_QUERY,
                    },
                    Stype::Raw => FUTURES_MBP1_QUERY,
                },
                Dataset::Equities => EQUITIES_MBP1_QUERY,
                Dataset::Option => OPTION_MBP1_QUERY,
            },
            QueryType::Trade => match dataset {
                Dataset::Futures => match stype {
                    Stype::Continuous => match continuous_kind {
                        ContinuousKind::Calendar => FUTURES_TRADE_CONTINUOUS_QUERY,
                        ContinuousKind::Volume => FUTURES_TRADE_CONTINUOUS_VOLUME_QUERY,
                        ContinuousKind::None => FUTURES_TRADE_CONTINUOUS_QUERY,
                    },
                    Stype::Raw => FUTURES_TRADE_QUERY,
                },
                Dataset::Equities => EQUITIES_TRADE_QUERY,
                Dataset::Option => OPTION_TRADE_QUERY,
            },
            QueryType::Ohlcv => match dataset {
                Dataset::Futures => match stype {
                    Stype::Continuous => match continuous_kind {
                        ContinuousKind::Calendar => FUTURES_OHLCV_CONTINUOUS_QUERY,
                        ContinuousKind::Volume => FUTURES_OHLCV_CONTINUOUS_VOLUME_QUERY,
                        ContinuousKind::None => FUTURES_OHLCV_CONTINUOUS_QUERY,
                    },
                    Stype::Raw => FUTURES_OHLCV_QUERY,
                },
                Dataset::Equities => EQUITIES_OHLCV_QUERY,
                Dataset::Option => OPTION_OHLCV_QUERY,
            },
            QueryType::Bbo => match dataset {
                Dataset::Futures => match stype {
                    Stype::Continuous => match continuous_kind {
                        ContinuousKind::Calendar => FUTURES_BBO_CONTINUOUS_QUERY,
                        ContinuousKind::Volume => FUTURES_BBO_CONTINUOUS_VOLUME_QUERY,
                        ContinuousKind::None => FUTURES_BBO_CONTINUOUS_QUERY,
                    },
                    Stype::Raw => FUTURES_BBO_QUERY,
                },
                Dataset::Equities => EQUITIES_BBO_QUERY,
                Dataset::Option => OPTION_BBO_QUERY,
            },
        }
    }
}

pub trait QueryParams {
    fn symbols_array(&self) -> Vec<String>;
    fn rank(&self) -> i32;
}

impl QueryParams for RetrieveParams {
    fn symbols_array(&self) -> Vec<String> {
        match self.stype {
            Stype::Raw => self.symbols.iter().map(|s| s.clone()).collect(),
            Stype::Continuous => self
                .symbols
                .iter()
                .map(|s| s.split('.').next().unwrap_or(s).to_string() + "%")
                .collect(),
        }
    }
    fn rank(&self) -> i32 {
        self.symbols
            .first()
            .and_then(|symbol| symbol.split('.').last())
            .and_then(|rank_str| rank_str.parse::<i32>().ok())
            .unwrap_or(0) // Default to 0 if any step fails
    }
}

#[async_trait]
pub trait RecordsQuery {
    async fn retrieve_query(
        pool: &PgPool,
        params: &RetrieveParams,
        continuous_kind: &ContinuousKind,
    ) -> Result<
        Pin<Box<dyn Stream<Item = std::result::Result<sqlx::postgres::PgRow, sqlx::Error>> + Send>>,
    >;
}

#[async_trait]
impl RecordsQuery for Mbp1Msg {
    async fn retrieve_query(
        pool: &PgPool,
        params: &RetrieveParams,
        continuous_kind: &ContinuousKind,
    ) -> Result<
        Pin<Box<dyn Stream<Item = std::result::Result<sqlx::postgres::PgRow, sqlx::Error>> + Send>>,
    > {
        // Parameters
        let tbbo_flag = params.schema == Schema::Tbbo;
        let symbol_array = params.symbols_array();
        let rank = params.rank();

        // Execute continuous query
        let cursor =
            sqlx::query(QueryType::Mbp.get_query(params.dataset, params.stype, continuous_kind))
                .bind(params.start_ts)
                .bind(params.end_ts - 1)
                .bind(symbol_array)
                .bind(tbbo_flag)
                .bind(rank)
                .fetch(pool);

        Ok(cursor)
    }
}

#[async_trait]
impl RecordsQuery for TradeMsg {
    async fn retrieve_query(
        pool: &PgPool,
        params: &RetrieveParams,
        continuous_kind: &ContinuousKind,
    ) -> Result<
        Pin<Box<dyn Stream<Item = std::result::Result<sqlx::postgres::PgRow, sqlx::Error>> + Send>>,
    > {
        // Parameters
        let symbol_array = params.symbols_array();
        let rank = params.rank();

        // Execute the query with parameters, including LIMIT and OFFSET
        let cursor =
            sqlx::query(QueryType::Trade.get_query(params.dataset, params.stype, continuous_kind))
                .bind(params.start_ts)
                .bind(params.end_ts - 1)
                .bind(symbol_array)
                .bind(rank)
                .fetch(pool);

        Ok(cursor)
    }
}

#[async_trait]
impl RecordsQuery for BboMsg {
    async fn retrieve_query(
        pool: &PgPool,
        params: &RetrieveParams,
        continuous_kind: &ContinuousKind,
    ) -> Result<
        Pin<Box<dyn Stream<Item = std::result::Result<sqlx::postgres::PgRow, sqlx::Error>> + Send>>,
    > {
        // Parameters
        let interval_ns = params.schema_interval()?;
        let symbol_array = params.symbols_array();
        let rank = params.rank();

        // Construct the SQL query with a join and additional filtering by symbols
        let cursor =
            sqlx::query(QueryType::Bbo.get_query(params.dataset, params.stype, continuous_kind))
                .bind(params.start_ts)
                .bind(params.end_ts)
                .bind(interval_ns)
                .bind(symbol_array)
                .bind(rank)
                .fetch(pool);

        Ok(cursor)
    }
}

#[async_trait]
impl RecordsQuery for OhlcvMsg {
    async fn retrieve_query(
        pool: &PgPool,
        params: &RetrieveParams,
        continuous_kind: &ContinuousKind,
    ) -> Result<
        Pin<Box<dyn Stream<Item = std::result::Result<sqlx::postgres::PgRow, sqlx::Error>> + Send>>,
    > {
        // Parameters
        let interval_ns = params.schema_interval()?;
        let symbol_array = params.symbols_array();
        let rank = params.rank();

        let cursor =
            sqlx::query(QueryType::Ohlcv.get_query(params.dataset, params.stype, continuous_kind))
                .bind(params.start_ts)
                .bind(params.end_ts)
                .bind(interval_ns)
                .bind(symbol_array)
                .bind(rank)
                .fetch(pool);

        Ok(cursor)
    }
}
#[async_trait]
impl RecordsQuery for RecordEnum {
    async fn retrieve_query(
        pool: &PgPool,
        params: &RetrieveParams,
        continuous_kind: &ContinuousKind,
    ) -> Result<
        Pin<Box<dyn Stream<Item = std::result::Result<sqlx::postgres::PgRow, sqlx::Error>> + Send>>,
    > {
        match RType::from(params.rtype().unwrap()) {
            RType::Mbp1 => Ok(Mbp1Msg::retrieve_query(pool, params, continuous_kind).await?),
            RType::Trades => Ok(TradeMsg::retrieve_query(pool, params, continuous_kind).await?),
            RType::Ohlcv => Ok(OhlcvMsg::retrieve_query(pool, params, continuous_kind).await?),
            RType::Bbo => Ok(BboMsg::retrieve_query(pool, params, continuous_kind).await?),
            RType::Tbbo => Ok(TbboMsg::retrieve_query(pool, params, continuous_kind).await?),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::database::create::RecordInsertQueries;
    use crate::database::init::init_db;
    use crate::database::read::rows::FromRow;
    use dbn;
    use futures::stream::StreamExt;
    use mbinary::enums::{Action, Dataset, Side, Stype};
    use mbinary::records::{BidAskPair, RecordHeader};
    use mbinary::symbols::Instrument;
    use mbinary::vendors::Vendors;
    use mbinary::vendors::{DatabentoData, VendorData};
    use serial_test::serial;
    use sqlx::{postgres::PgPoolOptions, Postgres, Transaction};
    use std::os::raw::c_char;
    use std::str::FromStr;
    use tracing::error;

    // -- Helper functions
    async fn create_instrument(instrument: Instrument) -> Result<i32> {
        let database_url = std::env::var("INSTRUMENT_DATABASE_URL")?;
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
        let database_url = std::env::var("INSTRUMENT_DATABASE_URL")?;
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
        let pool = init_db().await.unwrap();

        let schema = dbn::Schema::from_str("mbp-1")?;
        let dbn_dataset = dbn::Dataset::from_str("XNAS.ITCH")?;
        let stype = dbn::SType::from_str("raw_symbol")?;
        let vendor_data = VendorData::Databento(DatabentoData {
            schema,
            dataset: dbn_dataset,
            stype,
        });

        let dataset = Dataset::Equities;
        let instrument = Instrument::new(
            None,
            "AAPL",
            "Apple",
            dataset.clone(),
            Vendors::Databento,
            vendor_data.encode(),
            1704672000000000000,
            1704672000000000000,
            0,
            true,
        );

        let instrument_id = create_instrument(instrument)
            .await
            .expect("Error creating instrument.");

        let mut transaction = pool
            .begin()
            .await
            .expect("Error setting up test transaction.");

        // Mock data
        let records = vec![
            Mbp1Msg {
                hd: { RecordHeader::new::<Mbp1Msg>(instrument_id as u32, 1704209103644092564, 0) },
                price: 6770,
                size: 1,
                action: Action::Add as c_char,
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
                hd: { RecordHeader::new::<Mbp1Msg>(instrument_id as u32, 1704209103644092565, 0) },
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

        let _ = insert_records(&mut transaction, records.clone(), dataset.clone())
            .await
            .expect("Error inserting records.");
        let _ = transaction.commit().await;

        // Test
        let query_params = RetrieveParams {
            symbols: vec!["AAPL".to_string()],
            start_ts: 1704209103644092563,
            end_ts: 1704209903644092567,
            schema: Schema::Mbp1,
            dataset: dataset.clone(),
            stype: Stype::Raw,
        };

        let mut cursor = Mbp1Msg::retrieve_query(&pool, &query_params, &ContinuousKind::None)
            .await
            .expect("Error on retrieve records.");

        // Validate
        let mut query: Vec<RecordEnum> = vec![];
        while let Some(row_result) = cursor.next().await {
            match row_result {
                Ok(row) => {
                    let record = Mbp1Msg::from_row(&row, None)?;

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

        assert_eq!(records.len(), query.len());

        // Cleanup
        delete_instrument(instrument_id)
            .await
            .expect("Error on delete");

        Ok(())
    }

    #[sqlx::test]
    #[serial]
    // #[ignore]
    async fn test_retrieve_tbbo() -> anyhow::Result<()> {
        dotenv::dotenv().ok();
        let pool = init_db().await.unwrap();

        let schema = dbn::Schema::from_str("mbp-1")?;
        let dbn_dataset = dbn::Dataset::from_str("XNAS.ITCH")?;
        let stype = dbn::SType::from_str("raw_symbol")?;
        let vendor_data = VendorData::Databento(DatabentoData {
            schema,
            dataset: dbn_dataset,
            stype,
        });

        let dataset = Dataset::Equities;
        let instrument = Instrument::new(
            None,
            "AAPL",
            "Apple",
            dataset.clone(),
            Vendors::Databento,
            vendor_data.encode(),
            1704672000000000000,
            1704672000000000000,
            0,
            true,
        );

        let instrument_id = create_instrument(instrument)
            .await
            .expect("Error creating instrument.");

        let mut transaction = pool
            .begin()
            .await
            .expect("Error setting up test transaction.");

        // Mock data
        let records = vec![
            Mbp1Msg {
                hd: { RecordHeader::new::<Mbp1Msg>(instrument_id as u32, 1704209103644092564, 0) },
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
                hd: { RecordHeader::new::<Mbp1Msg>(instrument_id as u32, 1704209103644092565, 0) },
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

        let _ = insert_records(&mut transaction, records.clone(), dataset.clone())
            .await
            .expect("Error inserting records.");
        let _ = transaction.commit().await;

        // Test
        let query_params = RetrieveParams {
            symbols: vec!["AAPL".to_string()],
            start_ts: 1704209103644092563,
            end_ts: 1704209903644092567,
            schema: Schema::Tbbo,
            dataset: dataset.clone(),
            stype: Stype::Raw,
        };

        let mut cursor = TbboMsg::retrieve_query(&pool, &query_params, &ContinuousKind::None)
            .await
            .expect("Error on retrieve records.");

        // Validate
        let mut query: Vec<RecordEnum> = vec![];
        while let Some(row_result) = cursor.next().await {
            match row_result {
                Ok(row) => {
                    let record = TbboMsg::from_row(&row, None)?;

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
        delete_instrument(instrument_id)
            .await
            .expect("Error on delete");

        Ok(())
    }

    #[sqlx::test]
    #[serial]
    // #[ignore]
    async fn test_retrieve_trade() -> anyhow::Result<()> {
        dotenv::dotenv().ok();
        let pool = init_db().await.unwrap();

        let schema = dbn::Schema::from_str("mbp-1")?;
        let dbn_dataset = dbn::Dataset::from_str("XNAS.ITCH")?;
        let stype = dbn::SType::from_str("raw_symbol")?;
        let vendor_data = VendorData::Databento(DatabentoData {
            schema,
            dataset: dbn_dataset,
            stype,
        });

        let dataset = Dataset::Equities;
        let instrument = Instrument::new(
            None,
            "AAPL",
            "Apple",
            dataset.clone(),
            Vendors::Databento,
            vendor_data.encode(),
            1704672000000000000,
            1704672000000000000,
            0,
            true,
        );

        let instrument_id = create_instrument(instrument)
            .await
            .expect("Error creating instrument.");

        let mut transaction = pool
            .begin()
            .await
            .expect("Error setting up test transaction.");

        // Mock data
        let records = vec![
            Mbp1Msg {
                hd: { RecordHeader::new::<Mbp1Msg>(instrument_id as u32, 1704209103644092564, 0) },
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
                hd: { RecordHeader::new::<Mbp1Msg>(instrument_id as u32, 1704209103644092565, 0) },
                price: 6870,
                size: 2,
                action: Action::Trade as c_char,
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

        let _ = insert_records(&mut transaction, records.clone(), dataset.clone())
            .await
            .expect("Error inserting records.");
        let _ = transaction.commit().await;

        // Test
        let query_params = RetrieveParams {
            symbols: vec!["AAPL".to_string()],
            start_ts: 1704209103644092563,
            end_ts: 1704209903644092567,
            schema: Schema::Trades,
            dataset,
            stype: Stype::Raw,
        };

        let mut cursor = TradeMsg::retrieve_query(&pool, &query_params, &ContinuousKind::None)
            .await
            .expect("Error on retrieve records.");

        // Validate
        let mut query: Vec<RecordEnum> = vec![];
        while let Some(row_result) = cursor.next().await {
            match row_result {
                Ok(row) => {
                    let record = TradeMsg::from_row(&row, None)?;

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

        assert!(query.len() == 2);

        // Cleanup
        delete_instrument(instrument_id)
            .await
            .expect("Error on delete");

        Ok(())
    }

    #[sqlx::test]
    #[serial]
    // #[ignore]
    async fn test_retrieve_bbo() -> anyhow::Result<()> {
        dotenv::dotenv().ok();
        let pool = init_db().await.unwrap();

        let schema = dbn::Schema::from_str("mbp-1")?;
        let dbn_dataset = dbn::Dataset::from_str("XNAS.ITCH")?;
        let stype = dbn::SType::from_str("raw_symbol")?;
        let vendor_data = VendorData::Databento(DatabentoData {
            schema,
            dataset: dbn_dataset,
            stype,
        });

        let dataset = Dataset::Equities;
        let instrument = Instrument::new(
            None,
            "AAPL",
            "Apple",
            dataset.clone(),
            Vendors::Databento,
            vendor_data.encode(),
            1704672000000000000,
            1704672000000000000,
            0,
            true,
        );

        let instrument_id = create_instrument(instrument)
            .await
            .expect("Error creating instrument.");

        let mut transaction = pool
            .begin()
            .await
            .expect("Error setting up test transaction.");

        // Mock data
        let records = vec![
            Mbp1Msg {
                hd: { RecordHeader::new::<Mbp1Msg>(instrument_id as u32, 1704209103644092564, 0) },
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
                hd: { RecordHeader::new::<Mbp1Msg>(instrument_id as u32, 1704209103644092565, 0) },
                price: 6870,
                size: 2,
                action: Action::Trade as c_char,
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

        let _ = insert_records(&mut transaction, records.clone(), dataset.clone())
            .await
            .expect("Error inserting records.");
        let _ = transaction.commit().await;

        // Test
        let query_params = RetrieveParams {
            symbols: vec!["AAPL".to_string()],
            start_ts: 1704209103644092563,
            end_ts: 1704209903644092567,
            schema: Schema::Bbo1S,
            dataset,
            stype: Stype::Raw,
        };

        let mut cursor = BboMsg::retrieve_query(&pool, &query_params, &ContinuousKind::None)
            .await
            .expect("Error on retrieve records.");

        // Validate
        let mut query: Vec<RecordEnum> = vec![];
        while let Some(row_result) = cursor.next().await {
            match row_result {
                Ok(row) => {
                    let record = BboMsg::from_row(&row, None)?;

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
        delete_instrument(instrument_id)
            .await
            .expect("Error on delete");

        Ok(())
    }

    #[sqlx::test]
    #[serial]
    // #[ignore]
    async fn test_retrieve_ohlcv() -> anyhow::Result<()> {
        dotenv::dotenv().ok();
        let pool = init_db().await.unwrap();

        let schema = dbn::Schema::from_str("mbp-1")?;
        let dbn_dataset = dbn::Dataset::from_str("XNAS.ITCH")?;
        let stype = dbn::SType::from_str("raw_symbol")?;
        let vendor_data = VendorData::Databento(DatabentoData {
            schema,
            dataset: dbn_dataset,
            stype,
        });

        let dataset = Dataset::Equities;
        let instrument = Instrument::new(
            None,
            "AAPL",
            "Apple",
            dataset.clone(),
            Vendors::Databento,
            vendor_data.encode(),
            1704672000000000000,
            1704672000000000000,
            0,
            true,
        );

        let instrument_id = create_instrument(instrument)
            .await
            .expect("Error creating instrument.");

        let mut transaction = pool
            .begin()
            .await
            .expect("Error setting up test transaction.");

        // Mock data
        let records = vec![
            Mbp1Msg {
                hd: { RecordHeader::new::<Mbp1Msg>(instrument_id as u32, 1704209103644092562, 0) },
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
                hd: { RecordHeader::new::<Mbp1Msg>(instrument_id as u32, 1704209103644092564, 0) },
                price: 6770,
                size: 1,
                action: Action::Trade as c_char,
                side: 2,
                depth: 0,
                flags: 0,
                ts_recv: 1704209104645092564,
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
                hd: { RecordHeader::new::<Mbp1Msg>(instrument_id as u32, 1704295503644092562, 0) },
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
        ];

        let _ = insert_records(&mut transaction, records.clone(), dataset.clone())
            .await
            .expect("Error inserting records.");
        let _ = transaction.commit().await;

        // Test
        let query_params = RetrieveParams {
            symbols: vec!["AAPL".to_string()],
            start_ts: 1704209103644092562,
            end_ts: 1704295503654092563,
            schema: Schema::Ohlcv1D,
            dataset,
            stype: Stype::Raw,
        };

        let mut cursor = OhlcvMsg::retrieve_query(&pool, &query_params, &ContinuousKind::None)
            .await
            .expect("Error on retrieve records.");

        // Validate
        let mut query: Vec<RecordEnum> = vec![];
        while let Some(row_result) = cursor.next().await {
            match row_result {
                Ok(row) => {
                    let record = OhlcvMsg::from_row(&row, None)?;

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
        delete_instrument(instrument_id)
            .await
            .expect("Error on delete");

        Ok(())
    }

    // -- Futures
    #[sqlx::test]
    #[serial]
    // #[ignore]
    async fn test_retrieve_mbp1_futures() -> anyhow::Result<()> {
        dotenv::dotenv().ok();
        let pool = init_db().await.unwrap();

        let schema = dbn::Schema::from_str("mbp-1")?;
        let dbn_dataset = dbn::Dataset::from_str("GLBX.MDP3")?;
        let stype = dbn::SType::from_str("raw_symbol")?;
        let vendor_data = VendorData::Databento(DatabentoData {
            schema,
            dataset: dbn_dataset,
            stype,
        });

        let dataset = Dataset::Futures;
        let instrument = Instrument::new(
            None,
            "HEJ4",
            "Lean hogs",
            dataset.clone(),
            Vendors::Databento,
            vendor_data.encode(),
            1704672000000000000,
            1704672000000000000,
            0,
            true,
        );

        let instrument_id = create_instrument(instrument)
            .await
            .expect("Error creating instrument.");

        let mut transaction = pool
            .begin()
            .await
            .expect("Error setting up test transaction.");

        // Mock data
        let records = vec![
            Mbp1Msg {
                hd: { RecordHeader::new::<Mbp1Msg>(instrument_id as u32, 1704209103644092564, 0) },
                price: 6770,
                size: 1,
                action: Action::Add as c_char,
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
                hd: { RecordHeader::new::<Mbp1Msg>(instrument_id as u32, 1704209103644092565, 0) },
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

        let _ = insert_records(&mut transaction, records.clone(), dataset.clone())
            .await
            .expect("Error inserting records.");
        let _ = transaction.commit().await;

        // Test
        let query_params = RetrieveParams {
            symbols: vec!["HEJ4".to_string()],
            start_ts: 1704209103644092563,
            end_ts: 1704209903644092567,
            schema: Schema::Mbp1,
            dataset: dataset.clone(),
            stype: Stype::Raw,
        };

        let mut cursor = Mbp1Msg::retrieve_query(&pool, &query_params, &ContinuousKind::None)
            .await
            .expect("Error on retrieve records.");

        // Validate
        let mut query: Vec<RecordEnum> = vec![];
        while let Some(row_result) = cursor.next().await {
            match row_result {
                Ok(row) => {
                    let record = Mbp1Msg::from_row(&row, None)?;

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
        assert_eq!(records.len(), query.len());

        // Cleanup
        delete_instrument(instrument_id)
            .await
            .expect("Error on delete");

        Ok(())
    }

    #[sqlx::test]
    #[serial]
    // #[ignore]
    async fn test_retrieve_tbbo_futures() -> anyhow::Result<()> {
        dotenv::dotenv().ok();
        let pool = init_db().await.unwrap();

        let schema = dbn::Schema::from_str("mbp-1")?;
        let dbn_dataset = dbn::Dataset::from_str("GLBX.MDP3")?;
        let stype = dbn::SType::from_str("raw_symbol")?;
        let vendor_data = VendorData::Databento(DatabentoData {
            schema,
            dataset: dbn_dataset,
            stype,
        });

        let dataset = Dataset::Futures;
        let instrument = Instrument::new(
            None,
            "HEJ4",
            "Lean hogs",
            dataset.clone(),
            Vendors::Databento,
            vendor_data.encode(),
            1704672000000000000,
            1704672000000000000,
            0,
            true,
        );

        let instrument_id = create_instrument(instrument)
            .await
            .expect("Error creating instrument.");

        let mut transaction = pool
            .begin()
            .await
            .expect("Error setting up test transaction.");

        // Mock data
        let records = vec![
            Mbp1Msg {
                hd: { RecordHeader::new::<Mbp1Msg>(instrument_id as u32, 1704209103644092564, 0) },
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
                hd: { RecordHeader::new::<Mbp1Msg>(instrument_id as u32, 1704209103644092565, 0) },
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

        let _ = insert_records(&mut transaction, records.clone(), dataset.clone())
            .await
            .expect("Error inserting records.");
        let _ = transaction.commit().await;

        // Test
        let query_params = RetrieveParams {
            symbols: vec!["HEJ4".to_string()],
            start_ts: 1704209103644092563,
            end_ts: 1704209903644092567,
            schema: Schema::Tbbo,
            dataset: dataset.clone(),
            stype: Stype::Raw,
        };

        let mut cursor = TbboMsg::retrieve_query(&pool, &query_params, &ContinuousKind::None)
            .await
            .expect("Error on retrieve records.");

        // Validate
        let mut query: Vec<RecordEnum> = vec![];
        while let Some(row_result) = cursor.next().await {
            match row_result {
                Ok(row) => {
                    let record = TbboMsg::from_row(&row, None)?;

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
        delete_instrument(instrument_id)
            .await
            .expect("Error on delete");

        Ok(())
    }

    #[sqlx::test]
    #[serial]
    // #[ignore]
    async fn test_retrieve_trade_futures() -> anyhow::Result<()> {
        dotenv::dotenv().ok();
        let pool = init_db().await.unwrap();

        let schema = dbn::Schema::from_str("mbp-1")?;
        let dbn_dataset = dbn::Dataset::from_str("GLBX.MDP3")?;
        let stype = dbn::SType::from_str("raw_symbol")?;
        let vendor_data = VendorData::Databento(DatabentoData {
            schema,
            dataset: dbn_dataset,
            stype,
        });

        let dataset = Dataset::Futures;
        let instrument = Instrument::new(
            None,
            "HEJ4",
            "Lean hogs",
            dataset.clone(),
            Vendors::Databento,
            vendor_data.encode(),
            1704672000000000000,
            1704672000000000000,
            0,
            true,
        );

        let instrument_id = create_instrument(instrument)
            .await
            .expect("Error creating instrument.");

        let mut transaction = pool
            .begin()
            .await
            .expect("Error setting up test transaction.");

        // Mock data
        let records = vec![
            Mbp1Msg {
                hd: { RecordHeader::new::<Mbp1Msg>(instrument_id as u32, 1704209103644092564, 0) },
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
                hd: { RecordHeader::new::<Mbp1Msg>(instrument_id as u32, 1704209103644092565, 0) },
                price: 6870,
                size: 2,
                action: Action::Trade as c_char,
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

        let _ = insert_records(&mut transaction, records.clone(), dataset.clone())
            .await
            .expect("Error inserting records.");
        let _ = transaction.commit().await;

        // Test
        let query_params = RetrieveParams {
            symbols: vec!["HEJ4".to_string()],
            start_ts: 1704209103644092563,
            end_ts: 1704209903644092567,
            schema: Schema::Trades,
            dataset,
            stype: Stype::Raw,
        };

        let mut cursor = TradeMsg::retrieve_query(&pool, &query_params, &ContinuousKind::None)
            .await
            .expect("Error on retrieve records.");

        // Validate
        let mut query: Vec<RecordEnum> = vec![];
        while let Some(row_result) = cursor.next().await {
            match row_result {
                Ok(row) => {
                    let record = TradeMsg::from_row(&row, None)?;

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
        assert!(query.len() == 2);

        // Cleanup
        delete_instrument(instrument_id)
            .await
            .expect("Error on delete");

        Ok(())
    }

    #[sqlx::test]
    #[serial]
    // #[ignore]
    async fn test_retrieve_bbo_futures() -> anyhow::Result<()> {
        dotenv::dotenv().ok();
        let pool = init_db().await.unwrap();

        let schema = dbn::Schema::from_str("mbp-1")?;
        let dbn_dataset = dbn::Dataset::from_str("GLBX.MDP3")?;
        let stype = dbn::SType::from_str("raw_symbol")?;
        let vendor_data = VendorData::Databento(DatabentoData {
            schema,
            dataset: dbn_dataset,
            stype,
        });

        let dataset = Dataset::Futures;
        let instrument = Instrument::new(
            None,
            "HEJ4",
            "Lean hogs",
            dataset.clone(),
            Vendors::Databento,
            vendor_data.encode(),
            1704672000000000000,
            1704672000000000000,
            0,
            true,
        );

        let instrument_id = create_instrument(instrument)
            .await
            .expect("Error creating instrument.");

        let mut transaction = pool
            .begin()
            .await
            .expect("Error setting up test transaction.");

        // Mock data
        let records = vec![
            Mbp1Msg {
                hd: { RecordHeader::new::<Mbp1Msg>(instrument_id as u32, 1704209103644092564, 0) },
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
                hd: { RecordHeader::new::<Mbp1Msg>(instrument_id as u32, 1704209103644092565, 0) },
                price: 6870,
                size: 2,
                action: Action::Trade as c_char,
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

        let _ = insert_records(&mut transaction, records.clone(), dataset.clone())
            .await
            .expect("Error inserting records.");
        let _ = transaction.commit().await;

        // Test
        let query_params = RetrieveParams {
            symbols: vec!["HEJ4".to_string()],
            start_ts: 1704209103644092563,
            end_ts: 1704209903644092567,
            schema: Schema::Bbo1S,
            dataset,
            stype: Stype::Raw,
        };

        let mut cursor = BboMsg::retrieve_query(&pool, &query_params, &ContinuousKind::None)
            .await
            .expect("Error on retrieve records.");

        // Validate
        let mut query: Vec<RecordEnum> = vec![];
        while let Some(row_result) = cursor.next().await {
            match row_result {
                Ok(row) => {
                    let record = BboMsg::from_row(&row, None)?;

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
        delete_instrument(instrument_id)
            .await
            .expect("Error on delete");

        Ok(())
    }

    #[sqlx::test]
    #[serial]
    // #[ignore]
    async fn test_retrieve_ohlcv_futures() -> anyhow::Result<()> {
        dotenv::dotenv().ok();
        let pool = init_db().await.unwrap();

        let schema = dbn::Schema::from_str("mbp-1")?;
        let dbn_dataset = dbn::Dataset::from_str("GLBX.MDP3")?;
        let stype = dbn::SType::from_str("raw_symbol")?;
        let vendor_data = VendorData::Databento(DatabentoData {
            schema,
            dataset: dbn_dataset,
            stype,
        });

        let dataset = Dataset::Futures;
        let instrument = Instrument::new(
            None,
            "HEJ4",
            "Lean hogs",
            dataset.clone(),
            Vendors::Databento,
            vendor_data.encode(),
            1704672000000000000,
            1704672000000000000,
            0,
            true,
        );

        let instrument_id = create_instrument(instrument)
            .await
            .expect("Error creating instrument.");

        let mut transaction = pool
            .begin()
            .await
            .expect("Error setting up test transaction.");

        // Mock data
        let records = vec![
            Mbp1Msg {
                hd: { RecordHeader::new::<Mbp1Msg>(instrument_id as u32, 1704209103644092562, 0) },
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
                hd: { RecordHeader::new::<Mbp1Msg>(instrument_id as u32, 1704209103644092564, 0) },
                price: 6770,
                size: 1,
                action: Action::Trade as c_char,
                side: 2,
                depth: 0,
                flags: 0,
                ts_recv: 1704209104645092564,
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
                hd: { RecordHeader::new::<Mbp1Msg>(instrument_id as u32, 1704295503644092562, 0) },
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
        ];

        let _ = insert_records(&mut transaction, records.clone(), dataset.clone())
            .await
            .expect("Error inserting records.");
        let _ = transaction.commit().await;

        // Test
        let query_params = RetrieveParams {
            symbols: vec!["HEJ4".to_string()],
            start_ts: 1704209103644092562,
            end_ts: 1704295503654092563,
            schema: Schema::Ohlcv1D,
            dataset,
            stype: Stype::Raw,
        };

        let mut cursor = OhlcvMsg::retrieve_query(&pool, &query_params, &ContinuousKind::None)
            .await
            .expect("Error on retrieve records.");

        // Validate
        let mut query: Vec<RecordEnum> = vec![];
        while let Some(row_result) = cursor.next().await {
            match row_result {
                Ok(row) => {
                    let record = OhlcvMsg::from_row(&row, None)?;

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
        delete_instrument(instrument_id)
            .await
            .expect("Error on delete");

        Ok(())
    }

    // -- Option
    #[sqlx::test]
    #[serial]
    // #[ignore]
    async fn test_retrieve_mbp1_option() -> anyhow::Result<()> {
        dotenv::dotenv().ok();
        let pool = init_db().await.unwrap();

        let schema = dbn::Schema::from_str("mbp-1")?;
        let dbn_dataset = dbn::Dataset::from_str("OPRA.PILLAR")?;
        let stype = dbn::SType::from_str("raw_symbol")?;
        let vendor_data = VendorData::Databento(DatabentoData {
            schema,
            dataset: dbn_dataset,
            stype,
        });

        let dataset = Dataset::Option;
        let instrument = Instrument::new(
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
        );

        let instrument_id = create_instrument(instrument)
            .await
            .expect("Error creating instrument.");

        let mut transaction = pool
            .begin()
            .await
            .expect("Error setting up test transaction.");

        // Mock data
        let records = vec![
            Mbp1Msg {
                hd: { RecordHeader::new::<Mbp1Msg>(instrument_id as u32, 1704209103644092564, 0) },
                price: 6770,
                size: 1,
                action: Action::Add as c_char,
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
                hd: { RecordHeader::new::<Mbp1Msg>(instrument_id as u32, 1704209103644092565, 0) },
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

        let _ = insert_records(&mut transaction, records.clone(), dataset.clone())
            .await
            .expect("Error inserting records.");
        let _ = transaction.commit().await;

        // Test
        let query_params = RetrieveParams {
            symbols: vec!["APPLP12345".to_string()],
            start_ts: 1704209103644092563,
            end_ts: 1704209903644092567,
            schema: Schema::Mbp1,
            dataset: dataset.clone(),
            stype: Stype::Raw,
        };

        let mut cursor = Mbp1Msg::retrieve_query(&pool, &query_params, &ContinuousKind::None)
            .await
            .expect("Error on retrieve records.");

        // Validate
        let mut query: Vec<RecordEnum> = vec![];
        while let Some(row_result) = cursor.next().await {
            match row_result {
                Ok(row) => {
                    let record = Mbp1Msg::from_row(&row, None)?;

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
        assert_eq!(records.len(), query.len());

        // Cleanup
        delete_instrument(instrument_id)
            .await
            .expect("Error on delete");

        Ok(())
    }

    #[sqlx::test]
    #[serial]
    // #[ignore]
    async fn test_retrieve_tbbo_option() -> anyhow::Result<()> {
        dotenv::dotenv().ok();
        let pool = init_db().await.unwrap();

        let schema = dbn::Schema::from_str("mbp-1")?;
        let dbn_dataset = dbn::Dataset::from_str("OPRA.PILLAR")?;
        let stype = dbn::SType::from_str("raw_symbol")?;
        let vendor_data = VendorData::Databento(DatabentoData {
            schema,
            dataset: dbn_dataset,
            stype,
        });

        let dataset = Dataset::Option;
        let instrument = Instrument::new(
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
        );

        let instrument_id = create_instrument(instrument)
            .await
            .expect("Error creating instrument.");

        let mut transaction = pool
            .begin()
            .await
            .expect("Error setting up test transaction.");

        // Mock data
        let records = vec![
            Mbp1Msg {
                hd: { RecordHeader::new::<Mbp1Msg>(instrument_id as u32, 1704209103644092564, 0) },
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
                hd: { RecordHeader::new::<Mbp1Msg>(instrument_id as u32, 1704209103644092565, 0) },
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

        let _ = insert_records(&mut transaction, records.clone(), dataset.clone())
            .await
            .expect("Error inserting records.");
        let _ = transaction.commit().await;

        // Test
        let query_params = RetrieveParams {
            symbols: vec!["APPLP12345".to_string()],
            start_ts: 1704209103644092563,
            end_ts: 1704209903644092567,
            schema: Schema::Tbbo,
            dataset: dataset.clone(),
            stype: Stype::Raw,
        };

        let mut cursor = TbboMsg::retrieve_query(&pool, &query_params, &ContinuousKind::None)
            .await
            .expect("Error on retrieve records.");

        // Validate
        let mut query: Vec<RecordEnum> = vec![];
        while let Some(row_result) = cursor.next().await {
            match row_result {
                Ok(row) => {
                    let record = TbboMsg::from_row(&row, None)?;

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
        delete_instrument(instrument_id)
            .await
            .expect("Error on delete");

        Ok(())
    }

    #[sqlx::test]
    #[serial]
    // #[ignore]
    async fn test_retrieve_trade_option() -> anyhow::Result<()> {
        dotenv::dotenv().ok();
        let pool = init_db().await.unwrap();

        let schema = dbn::Schema::from_str("mbp-1")?;
        let dbn_dataset = dbn::Dataset::from_str("OPRA.PILLAR")?;
        let stype = dbn::SType::from_str("raw_symbol")?;
        let vendor_data = VendorData::Databento(DatabentoData {
            schema,
            dataset: dbn_dataset,
            stype,
        });

        let dataset = Dataset::Option;
        let instrument = Instrument::new(
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
        );

        let instrument_id = create_instrument(instrument)
            .await
            .expect("Error creating instrument.");

        let mut transaction = pool
            .begin()
            .await
            .expect("Error setting up test transaction.");

        // Mock data
        let records = vec![
            Mbp1Msg {
                hd: { RecordHeader::new::<Mbp1Msg>(instrument_id as u32, 1704209103644092564, 0) },
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
                hd: { RecordHeader::new::<Mbp1Msg>(instrument_id as u32, 1704209103644092565, 0) },
                price: 6870,
                size: 2,
                action: Action::Trade as c_char,
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

        let _ = insert_records(&mut transaction, records.clone(), dataset.clone())
            .await
            .expect("Error inserting records.");
        let _ = transaction.commit().await;

        // Test
        let query_params = RetrieveParams {
            symbols: vec!["APPLP12345".to_string()],
            start_ts: 1704209103644092563,
            end_ts: 1704209903644092567,
            schema: Schema::Trades,
            dataset,
            stype: Stype::Raw,
        };

        let mut cursor = TradeMsg::retrieve_query(&pool, &query_params, &ContinuousKind::None)
            .await
            .expect("Error on retrieve records.");

        // Validate
        let mut query: Vec<RecordEnum> = vec![];
        while let Some(row_result) = cursor.next().await {
            match row_result {
                Ok(row) => {
                    let record = TradeMsg::from_row(&row, None)?;

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
        assert!(query.len() == 2);

        // Cleanup
        delete_instrument(instrument_id)
            .await
            .expect("Error on delete");

        Ok(())
    }

    #[sqlx::test]
    #[serial]
    // #[ignore]
    async fn test_retrieve_bbo_option() -> anyhow::Result<()> {
        dotenv::dotenv().ok();
        let pool = init_db().await.unwrap();

        let schema = dbn::Schema::from_str("mbp-1")?;
        let dbn_dataset = dbn::Dataset::from_str("OPRA.PILLAR")?;
        let stype = dbn::SType::from_str("raw_symbol")?;
        let vendor_data = VendorData::Databento(DatabentoData {
            schema,
            dataset: dbn_dataset,
            stype,
        });

        let dataset = Dataset::Option;
        let instrument = Instrument::new(
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
        );

        let instrument_id = create_instrument(instrument)
            .await
            .expect("Error creating instrument.");

        let mut transaction = pool
            .begin()
            .await
            .expect("Error setting up test transaction.");

        // Mock data
        let records = vec![
            Mbp1Msg {
                hd: { RecordHeader::new::<Mbp1Msg>(instrument_id as u32, 1704209103644092564, 0) },
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
                hd: { RecordHeader::new::<Mbp1Msg>(instrument_id as u32, 1704209103644092565, 0) },
                price: 6870,
                size: 2,
                action: Action::Trade as c_char,
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

        let _ = insert_records(&mut transaction, records.clone(), dataset.clone())
            .await
            .expect("Error inserting records.");
        let _ = transaction.commit().await;

        // Test
        let query_params = RetrieveParams {
            symbols: vec!["APPLP12345".to_string()],
            start_ts: 1704209103644092563,
            end_ts: 1704209903644092567,
            schema: Schema::Bbo1S,
            dataset,
            stype: Stype::Raw,
        };

        let mut cursor = BboMsg::retrieve_query(&pool, &query_params, &ContinuousKind::None)
            .await
            .expect("Error on retrieve records.");

        // Validate
        let mut query: Vec<RecordEnum> = vec![];
        while let Some(row_result) = cursor.next().await {
            match row_result {
                Ok(row) => {
                    let record = BboMsg::from_row(&row, None)?;

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
        delete_instrument(instrument_id)
            .await
            .expect("Error on delete");

        Ok(())
    }

    #[sqlx::test]
    #[serial]
    // #[ignore]
    async fn test_retrieve_ohlcv_option() -> anyhow::Result<()> {
        dotenv::dotenv().ok();
        let pool = init_db().await.unwrap();

        let schema = dbn::Schema::from_str("mbp-1")?;
        let dbn_dataset = dbn::Dataset::from_str("OPRA.PILLAR")?;
        let stype = dbn::SType::from_str("raw_symbol")?;
        let vendor_data = VendorData::Databento(DatabentoData {
            schema,
            dataset: dbn_dataset,
            stype,
        });

        let dataset = Dataset::Option;
        let instrument = Instrument::new(
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
        );

        let instrument_id = create_instrument(instrument)
            .await
            .expect("Error creating instrument.");

        let mut transaction = pool
            .begin()
            .await
            .expect("Error setting up test transaction.");

        // Mock data
        let records = vec![
            Mbp1Msg {
                hd: { RecordHeader::new::<Mbp1Msg>(instrument_id as u32, 1704209103644092562, 0) },
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
                hd: { RecordHeader::new::<Mbp1Msg>(instrument_id as u32, 1704209103644092564, 0) },
                price: 6770,
                size: 1,
                action: Action::Trade as c_char,
                side: 2,
                depth: 0,
                flags: 0,
                ts_recv: 1704209104645092564,
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
                hd: { RecordHeader::new::<Mbp1Msg>(instrument_id as u32, 1704295503644092562, 0) },
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
        ];

        let _ = insert_records(&mut transaction, records.clone(), dataset.clone())
            .await
            .expect("Error inserting records.");
        let _ = transaction.commit().await;

        // Test
        let query_params = RetrieveParams {
            symbols: vec!["APPLP12345".to_string()],
            start_ts: 1704209103644092562,
            end_ts: 1704295503654092563,
            schema: Schema::Ohlcv1D,
            dataset,
            stype: Stype::Raw,
        };

        let mut cursor = OhlcvMsg::retrieve_query(&pool, &query_params, &ContinuousKind::None)
            .await
            .expect("Error on retrieve records.");

        // Validate
        let mut query: Vec<RecordEnum> = vec![];
        while let Some(row_result) = cursor.next().await {
            match row_result {
                Ok(row) => {
                    let record = OhlcvMsg::from_row(&row, None)?;

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
        delete_instrument(instrument_id)
            .await
            .expect("Error on delete");

        Ok(())
    }

    // Continuous futures
    #[sqlx::test]
    #[serial]
    // #[ignore]
    async fn test_retrieve_mbp1_futures_continuous() -> anyhow::Result<()> {
        dotenv::dotenv().ok();
        let pool = init_db().await.unwrap();

        let schema = dbn::Schema::from_str("mbp-1")?;
        let dbn_dataset = dbn::Dataset::from_str("GLBX.MDP3")?;
        let stype = dbn::SType::from_str("raw_symbol")?;
        let vendor_data = VendorData::Databento(DatabentoData {
            schema,
            dataset: dbn_dataset,
            stype,
        });

        let dataset = Dataset::Futures;
        let instrument = Instrument::new(
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
        );

        let instrument_id = create_instrument(instrument)
            .await
            .expect("Error creating instrument.");

        let mut transaction = pool
            .begin()
            .await
            .expect("Error setting up test transaction.");

        // Mock data
        let records = vec![
            Mbp1Msg {
                hd: { RecordHeader::new::<Mbp1Msg>(instrument_id as u32, 1704209103644092564, 0) },
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
                hd: { RecordHeader::new::<Mbp1Msg>(instrument_id as u32, 1704209103644092565, 0) },
                price: 6870,
                size: 2,
                action: Action::Trade as c_char,
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

        let _ = insert_records(&mut transaction, records.clone(), dataset.clone())
            .await
            .expect("Error inserting records.");
        let _ = transaction.commit().await;

        // Test
        let query_params = RetrieveParams {
            symbols: vec!["HE.c.0".to_string()],
            start_ts: 1704209103644092563,
            end_ts: 1704209903644092567,
            schema: Schema::Mbp1,
            dataset: dataset.clone(),
            stype: Stype::Continuous,
        };

        let mut cursor = Mbp1Msg::retrieve_query(&pool, &query_params, &ContinuousKind::Calendar)
            .await
            .expect("Error on retrieve records.");

        // Validate
        let mut query: Vec<RecordEnum> = vec![];
        while let Some(row_result) = cursor.next().await {
            match row_result {
                Ok(row) => {
                    let record = Mbp1Msg::from_row(&row, None)?;

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
        assert_eq!(records.len(), query.len());

        // Cleanup
        delete_instrument(instrument_id)
            .await
            .expect("Error on delete");

        Ok(())
    }

    #[sqlx::test]
    #[serial]
    // #[ignore]
    async fn test_retrieve_mbp1_futures_continuous_volume() -> anyhow::Result<()> {
        dotenv::dotenv().ok();
        let pool = init_db().await.unwrap();

        let schema = dbn::Schema::from_str("mbp-1")?;
        let dbn_dataset = dbn::Dataset::from_str("GLBX.MDP3")?;
        let stype = dbn::SType::from_str("raw_symbol")?;
        let vendor_data = VendorData::Databento(DatabentoData {
            schema,
            dataset: dbn_dataset,
            stype,
        });

        let dataset = Dataset::Futures;
        let instrument = Instrument::new(
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
        );

        let instrument_id = create_instrument(instrument)
            .await
            .expect("Error creating instrument.");

        let mut transaction = pool
            .begin()
            .await
            .expect("Error setting up test transaction.");

        // Mock data
        let records = vec![
            Mbp1Msg {
                hd: { RecordHeader::new::<Mbp1Msg>(instrument_id as u32, 1704209103644092564, 0) },
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
                hd: { RecordHeader::new::<Mbp1Msg>(instrument_id as u32, 1704209103644092565, 0) },
                price: 6870,
                size: 2,
                action: Action::Trade as c_char,
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

        let _ = insert_records(&mut transaction, records.clone(), dataset.clone())
            .await
            .expect("Error inserting records.");
        let _ = transaction.commit().await;

        // Test
        let query_params = RetrieveParams {
            symbols: vec!["HE.v.0".to_string()],
            start_ts: 1704209103644092563,
            end_ts: 1704209903644092567,
            schema: Schema::Mbp1,
            dataset: dataset.clone(),
            stype: Stype::Continuous,
        };

        let mut cursor = Mbp1Msg::retrieve_query(&pool, &query_params, &ContinuousKind::Volume)
            .await
            .expect("Error on retrieve records.");

        // Validate
        let mut query: Vec<RecordEnum> = vec![];
        while let Some(row_result) = cursor.next().await {
            match row_result {
                Ok(row) => {
                    let record = Mbp1Msg::from_row(&row, None)?;

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
        assert_eq!(records.len(), query.len());

        // Cleanup
        delete_instrument(instrument_id)
            .await
            .expect("Error on delete");

        Ok(())
    }
}
