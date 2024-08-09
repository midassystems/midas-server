use crate::mbn::enums::{RType, Schema};
use crate::mbn::record_enum::RecordEnum;
use crate::mbn::records::{Mbp1Msg, RecordHeader};
use crate::{Error, Result};
use async_trait::async_trait;
use mbn::records::{BidAskPair, OhlcvMsg};
use mbn::symbols::SymbolMap;
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, Postgres, Row, Transaction};
use std::os::raw::c_char;
use std::str::FromStr;

#[derive(Debug, Serialize, Deserialize)]
pub struct RetrieveParams {
    pub symbols: Vec<String>,
    pub start_ts: i64,
    pub end_ts: i64,
    pub schema: String,
}

impl RetrieveParams {
    fn schema_interval(&self) -> Result<i64> {
        let schema = Schema::from_str(&self.schema)?;
        match schema {
            Schema::Ohlcv1S => Ok(1),
            Schema::Ohlcv1M => Ok(60),
            Schema::Ohlcv1H => Ok(3600),
            Schema::Ohlcv1D => Ok(86400),
            _ => Err(Error::SqlError(format!(
                "No interval for Schema value: '{}'",
                schema
            ))),
        }
    }
}

impl RetrieveParams {
    fn rtype(&self) -> Result<RType> {
        let schema = Schema::from_str(&self.schema)?;
        Ok(RType::from(schema))
    }
}

#[async_trait]
pub trait RecordInsertQueries {
    async fn insert_query(&self, tx: &mut Transaction<'_, Postgres>) -> Result<()>;
}

#[async_trait]
impl RecordInsertQueries for Mbp1Msg {
    async fn insert_query(&self, tx: &mut Transaction<'_, Postgres>) -> Result<()> {
        println!("{:?}", self.action);
        println!("{:?}", self.action as i8);
        // Insert into mbp table
        let mbp_id: i32 = sqlx::query_scalar(
            r#"
            INSERT INTO mbp (instrument_id, ts_event, price, size, action, side, ts_recv, ts_in_delta, sequence)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            RETURNING id
            "#
        )
        .bind(self.hd.instrument_id as i32)
        .bind(self.hd.ts_event as i64)
        .bind(self.price)
        .bind(self.size as i32)
        .bind(self.action as i32)
        .bind(self.side as i32)
        .bind(self.ts_recv as i64)
        .bind(self.ts_in_delta)
        .bind(self.sequence as i32)
        .fetch_one(&mut *tx)
        .await?;

        // Insert into bid_ask table
        for (depth, level) in self.levels.iter().enumerate() {
            let _ = sqlx::query(
                r#"
                INSERT INTO bid_ask (mbp_id, depth, bid_px, bid_sz, bid_ct, ask_px, ask_sz, ask_ct)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                "#,
            )
            .bind(mbp_id)
            .bind(depth as i32) // Using the index as depth
            .bind(level.bid_px)
            .bind(level.bid_sz as i32)
            .bind(level.bid_ct as i32)
            .bind(level.ask_px)
            .bind(level.ask_sz as i32)
            .bind(level.ask_ct as i32)
            .execute(&mut *tx)
            .await?;
        }

        Ok(())
    }
}

pub async fn insert_records(
    tx: &mut Transaction<'_, Postgres>,
    records: Vec<Mbp1Msg>,
) -> Result<()> {
    for record in records {
        record.insert_query(tx).await?;
    }

    Ok(())
}

#[async_trait]
pub trait RecordRetrieveQueries: Sized {
    async fn retrieve_query(
        pool: &PgPool,
        params: &RetrieveParams,
    ) -> Result<(Vec<Self>, SymbolMap)>;
}

pub trait FromRow: Sized {
    fn from_row(row: sqlx::postgres::PgRow) -> Result<Self>;
}

#[async_trait]
impl RecordRetrieveQueries for Mbp1Msg {
    async fn retrieve_query(
        pool: &PgPool,
        params: &RetrieveParams,
    ) -> Result<(Vec<Self>, SymbolMap)> {
        // Construct the SQL query with a join and additional filtering by symbols
        let query = r#"
            SELECT m.instrument_id, m.ts_event, m.price, m.size, m.action, m.side, m.ts_recv, m.ts_in_delta, m.sequence, i.ticker,
                   b.bid_px, b.bid_sz, b.bid_ct, b.ask_px, b.ask_sz, b.ask_ct
            FROM mbp m
            INNER JOIN instrument i ON m.instrument_id = i.id
            LEFT JOIN bid_ask b ON m.id = b.mbp_id AND b.depth = 0
            WHERE m.ts_event BETWEEN $1 AND $2
            AND i.ticker = ANY($3)
        "#;

        // Convert the Vec<String> symbols to an array for binding
        let symbol_array: Vec<&str> = params.symbols.iter().map(AsRef::as_ref).collect();

        let rows = sqlx::query(query)
            .bind(params.start_ts)
            .bind(params.end_ts)
            .bind(&symbol_array)
            .fetch_all(pool)
            .await?;

        let mut records = Vec::new();
        let mut symbol_map = SymbolMap::new();

        for row in rows {
            let instrument_id = row.try_get::<i32, _>("instrument_id")? as u32;
            let ticker: String = row.try_get("ticker")?;
            let record = Mbp1Msg::from_row(row)?;
            println!("got row.");

            records.push(record);
            symbol_map.add_instrument(&ticker, instrument_id);
        }

        Ok((records, symbol_map))
    }
}

impl FromRow for Mbp1Msg {
    fn from_row(row: sqlx::postgres::PgRow) -> Result<Self> {
        println!("getting the row.");

        Ok(Mbp1Msg {
            hd: RecordHeader::new::<Mbp1Msg>(
                row.try_get::<i32, _>("instrument_id")? as u32,
                row.try_get::<i64, _>("ts_event")? as u64,
            ),
            price: row.try_get::<i64, _>("price")?,
            size: row.try_get::<i32, _>("size")? as u32,
            action: row.try_get::<i32, _>("action")? as c_char,
            side: row.try_get::<i32, _>("side")? as c_char,
            depth: 0 as u8, // Always top of book

            // flags: row.try_get::<i32, _>("flags")? as u8,
            ts_recv: row.try_get::<i64, _>("ts_recv")? as u64,
            ts_in_delta: row.try_get::<i32, _>("ts_in_delta")?,
            sequence: row.try_get::<i32, _>("sequence")? as u32,
            levels: [BidAskPair {
                bid_px: row.try_get::<i64, _>("bid_px")?,
                ask_px: row.try_get::<i64, _>("ask_px")?,
                bid_sz: row.try_get::<i32, _>("bid_sz")? as u32,
                ask_sz: row.try_get::<i32, _>("ask_sz")? as u32,
                bid_ct: row.try_get::<i32, _>("bid_ct")? as u32,
                ask_ct: row.try_get::<i32, _>("ask_ct")? as u32,
            }],
        })
    }
}

#[async_trait]
impl RecordRetrieveQueries for OhlcvMsg {
    async fn retrieve_query(
        pool: &PgPool,
        params: &RetrieveParams,
    ) -> Result<(Vec<Self>, SymbolMap)> {
        // Convert the Vec<String> symbols to an array for binding
        let symbol_array: Vec<&str> = params.symbols.iter().map(AsRef::as_ref).collect();

        let rows = sqlx::query(
            r#"
            WITH ordered_data AS (
              SELECT
                m.instrument_id,
                m.ts_event,
                m.price,
                m.size,
                row_number() OVER (PARTITION BY floor(m.ts_event / 1000000000 / $3) * $3 ORDER BY m.ts_event ASC) AS first_row,
                row_number() OVER (PARTITION BY floor(m.ts_event / 1000000000 / $3) * $3 ORDER BY m.ts_event DESC) AS last_row
              FROM mbp m
              INNER JOIN instrument i ON m.instrument_id = i.id
              WHERE m.ts_event BETWEEN $1 AND $2
              AND i.ticker = ANY($4)
            ),
            aggregated_data AS (
              SELECT
                instrument_id,
                floor(ts_event / 1000000000 / $3) * $3 * 1000000000 AS ts_event, -- Maintain nanoseconds
                MIN(price) FILTER (WHERE first_row = 1) AS open,
                MIN(price) FILTER (WHERE last_row = 1) AS close,
                MIN(price) AS low,
                MAX(price) AS high,
                SUM(size) AS volume
              FROM ordered_data
              GROUP BY
                instrument_id,
                floor(ts_event / 1000000000 / $3) * $3
            )
            SELECT
              a.instrument_id,
              CAST(a.ts_event AS BIGINT), -- Keep as nanoseconds
              a.open,
              a.close,
              a.low,
              a.high,
              a.volume,
              i.ticker
            FROM aggregated_data a
            INNER JOIN instrument i ON a.instrument_id = i.id
            ORDER BY a.ts_event
            "#
        )
        .bind(params.start_ts as i64)
        .bind(params.end_ts as i64)
        .bind(params.schema_interval()?)
        .bind(&symbol_array)
        .fetch_all(pool)
        .await?;

        let mut records = Vec::new();
        let mut symbol_map = SymbolMap::new();

        for row in rows {
            let instrument_id = row.try_get::<i32, _>("instrument_id")? as u32;
            let ticker: String = row.try_get("ticker")?;
            let record = OhlcvMsg::from_row(row)?;

            records.push(record);
            symbol_map.add_instrument(&ticker, instrument_id);
        }

        Ok((records, symbol_map))
    }
}

impl FromRow for OhlcvMsg {
    fn from_row(row: sqlx::postgres::PgRow) -> Result<Self> {
        Ok(OhlcvMsg {
            hd: RecordHeader::new::<OhlcvMsg>(
                row.try_get::<i32, _>("instrument_id")? as u32,
                row.try_get::<i64, _>("ts_event")? as u64,
            ),
            open: row.try_get::<i64, _>("open")?,
            close: row.try_get::<i64, _>("close")?,
            low: row.try_get::<i64, _>("low")?,
            high: row.try_get::<i64, _>("high")?,
            volume: row.try_get::<i64, _>("volume")? as u64,
        })
    }
}

#[async_trait]
impl RecordRetrieveQueries for RecordEnum {
    async fn retrieve_query(
        pool: &PgPool,
        params: &RetrieveParams,
    ) -> Result<(Vec<Self>, SymbolMap)> {
        // tracing::info!("{}", params.rtype().unwrap());
        let (records, map) = match RType::from(params.rtype().unwrap()) {
            RType::Mbp1 => {
                let (records, map) = Mbp1Msg::retrieve_query(pool, params).await?;
                (records.into_iter().map(RecordEnum::Mbp1).collect(), map)
                // Ok((rec_enum, hash_map))
            }
            RType::Ohlcv => {
                let (records, map) = OhlcvMsg::retrieve_query(pool, params).await?;
                // records.into_iter().map(RecordEnum::Ohlcv).collect()
                (records.into_iter().map(RecordEnum::Ohlcv).collect(), map)

                // Ok((rec_enum, hash_map))
            }
        };
        Ok((records, map))
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::database::init::init_quest_db;
    use crate::database::symbols::*;
    use mbn::enums::{Action, Side};
    use mbn::symbols::Instrument;
    use serial_test::serial;

    async fn create_instrument(pool: &PgPool) -> Result<i32> {
        let mut transaction = pool
            .begin()
            .await
            .expect("Error setting up test transaction.");

        let ticker = "AAPL";
        let name = "Apple Inc.";
        let instrument = Instrument::new(ticker, name, None);
        let id = instrument
            .insert_instrument(&mut transaction)
            .await
            .expect("Error inserting symbol.");
        let _ = transaction.commit().await?;
        Ok(id)
    }

    #[sqlx::test]
    #[serial]
    #[ignore]
    async fn test_create_record() {
        dotenv::dotenv().ok();
        let pool = init_quest_db().await.unwrap();

        let instrument_id = create_instrument(&pool)
            .await
            .expect("Error creating instrument.");

        let mut transaction = pool
            .begin()
            .await
            .expect("Error setting up test transaction.");

        // Mock data
        let records = vec![
            Mbp1Msg {
                hd: { RecordHeader::new::<Mbp1Msg>(instrument_id as u32, 1704209103644092564) },
                price: 6770,
                size: 1,
                action: Action::Add as c_char,
                side: Side::Bid as c_char,
                depth: 0,
                ts_recv: 1704209103644092564,
                ts_in_delta: 17493,
                sequence: 739763,
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
                hd: { RecordHeader::new::<Mbp1Msg>(instrument_id as u32, 1704209103644092564) },
                price: 6870,
                size: 2,
                action: Action::Add as c_char,
                side: Side::Bid as c_char,
                depth: 0,
                ts_recv: 1704209103644092564,
                ts_in_delta: 17493,
                sequence: 739763,
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

        // Test
        let result = insert_records(&mut transaction, records)
            .await
            .expect("Error inserting records.");

        // transaction.commit().await.expect("testing");

        // Validate
        assert_eq!(result, ());

        // Cleanup
        Instrument::delete_instrument(&mut transaction, instrument_id)
            .await
            .expect("Error on delete.");

        let _ = transaction.commit().await;
    }

    #[sqlx::test]
    #[serial]
    // #[ignore]
    async fn test_retrieve_mbp1() {
        dotenv::dotenv().ok();
        let pool = init_quest_db().await.unwrap();

        let instrument_id = create_instrument(&pool)
            .await
            .expect("Error creating instrument.");

        let mut transaction = pool
            .begin()
            .await
            .expect("Error setting up test transaction.");

        // Mock data
        let records = vec![
            Mbp1Msg {
                hd: { RecordHeader::new::<Mbp1Msg>(instrument_id as u32, 1704209103644092564) },
                price: 6770,
                size: 1,
                action: Action::Add as c_char,
                side: Side::Bid as c_char,
                depth: 0,
                ts_recv: 1704209103644092564,
                ts_in_delta: 17493,
                sequence: 739763,
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
                hd: { RecordHeader::new::<Mbp1Msg>(instrument_id as u32, 1704209103644092564) },
                price: 6870,
                size: 2,
                action: Action::Add as c_char,
                side: Side::Bid as c_char,
                depth: 0,
                ts_recv: 1704209103644092564,
                ts_in_delta: 17493,
                sequence: 739763,
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

        let _ = insert_records(&mut transaction, records)
            .await
            .expect("Error inserting records.");
        let _ = transaction.commit().await;

        // Test
        let query_params = RetrieveParams {
            symbols: vec!["AAPL".to_string()],
            start_ts: 1704209103644092563,
            end_ts: 1704209903644092564,
            schema: String::from("mbp_1"),
        };

        println!("{:?}", query_params);

        let (records, hash_map) = Mbp1Msg::retrieve_query(&pool, &query_params)
            .await
            .expect("Error on retrieve records.");
        println!("{:?}", records);
        println!("{:?}", hash_map);

        // Validate
        assert!(records.len() > 0);

        // Cleanup
        let mut transaction = pool
            .begin()
            .await
            .expect("Error setting up test transaction.");

        Instrument::delete_instrument(&mut transaction, instrument_id)
            .await
            .expect("Error on delete.");

        let _ = transaction.commit().await;
    }

    #[sqlx::test]
    #[serial]
    #[ignore]
    async fn test_retrieve_ohlcv() {
        dotenv::dotenv().ok();
        let pool = init_quest_db().await.unwrap();

        let instrument_id = create_instrument(&pool)
            .await
            .expect("Error creating instrument.");

        let mut transaction = pool
            .begin()
            .await
            .expect("Error setting up test transaction.");

        // Mock data
        let records = vec![
            Mbp1Msg {
                hd: { RecordHeader::new::<Mbp1Msg>(instrument_id as u32, 1704209103644092564) },
                price: 6770,
                size: 1,
                action: 1,
                side: 2,
                depth: 0,
                ts_recv: 1704209103644092564,
                ts_in_delta: 17493,
                sequence: 739763,
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
                hd: { RecordHeader::new::<Mbp1Msg>(instrument_id as u32, 1709209107644092564) },
                price: 6870,
                size: 2,
                action: 1,
                side: 2,
                depth: 0,
                ts_recv: 1704209103644092564,
                ts_in_delta: 17493,
                sequence: 739763,
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

        let _ = insert_records(&mut transaction, records)
            .await
            .expect("Error inserting records.");
        let _ = transaction.commit().await;

        // Test
        let query_params = RetrieveParams {
            symbols: vec!["AAPL".to_string()],
            start_ts: 1704209103644092563,
            end_ts: 1704209903644092564,
            schema: String::from("ohlcv-1d"),
        };

        let (result, hash_map) = OhlcvMsg::retrieve_query(&pool, &query_params)
            .await
            .expect("Error on retrieve records.");

        println!("{:?}, {:?}", result, hash_map);
        // Validate
        assert!(result.len() > 0);

        // Cleanup
        let mut transaction = pool
            .begin()
            .await
            .expect("Error setting up test transaction.");

        Instrument::delete_instrument(&mut transaction, instrument_id)
            .await
            .expect("Error on delete.");

        let _ = transaction.commit().await;
    }
}
